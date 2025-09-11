# adapters/sql_adapter.py
import polars as pl
import pyarrow as pa
from sqlalchemy import create_engine, inspect, text
from typing import Iterator, Dict, Any, Optional
from .base import DataSourceAdapter
from utils.schema_helpers import infer_nullability_streaming_from_lazy, schema_from_sample

class SQLAdapter(DataSourceAdapter):
    """
    Adapter for SQL databases using SQLAlchemy + Arrow + Polars.
    Attempts to use DB metadata for nullability; falls back to streaming inference when requested.
    """

    def __init__(self, connection_string: str, table_name: str, schema: Optional[str] = None):
        self.engine = create_engine(connection_string)
        self.table_name = table_name
        self.schema = schema
        self._nullable_map: Optional[Dict[str, bool]] = None

    def get_schema(self, stream_infer_nulls: bool = False, sample_size: int = 5, null_chunk_size: int = 100_000, null_max_chunks: Optional[int] = None) -> Dict[str, Any]:
        # Try metadata first
        insp = inspect(self.engine)
        try:
            cols_meta = insp.get_columns(self.table_name, schema=self.schema)
            # Build sample structure from metadata (we still need dtypes for polars sampling)
            col_names = [c["name"] for c in cols_meta]
            nullable_meta = {c["name"]: bool(c.get("nullable", False)) for c in cols_meta}
        except Exception:
            # If metadata not available, fall back to scanning
            col_names = None
            nullable_meta = {}

        # If we have metadata and no streaming requested, we can use metadata for nullability,
        # but we still want a polars sample for dtype discovery.
        # Build a simple lazy query to fetch sample rows
        query = text(f"SELECT * FROM {self.table_name} LIMIT :limit")
        with self.engine.connect() as conn:
            try:
                result = conn.execute(query, {"limit": sample_size})
                rows = [dict(r._mapping) for r in result]
                sample_df = pl.from_dicts(rows) if rows else pl.DataFrame()
            except Exception:
                # Fallback: empty sample
                sample_df = pl.DataFrame()

        # If no columns discovered from sample, try to get column names from metadata
        if not sample_df.columns and col_names:
            # create empty dataframe with column names (no types)
            sample_df = pl.DataFrame({name: [] for name in col_names})

        # Determine nullable map
        if not stream_infer_nulls:
            # if metadata has info use it; else infer from sample
            if nullable_meta:
                nullable_map = {c: nullable_meta.get(c, False) for c in sample_df.columns}
            else:
                nullable_map = {c: (sample_df[c].null_count() > 0 if c in sample_df.columns else False) for c in sample_df.columns}
        else:
            # streaming inference: build a lazyframe by selecting the table via SQLAlchemy streaming
            if getattr(self, "_nullable_map", None) is None:
                # We'll stream rows in batches using plain execute and build polars frames
                nullable_map = {c: False for c in sample_df.columns}
                batch = []
                batch_count = 0
                chunk = []
                select_query = text(f"SELECT * FROM {self.table_name}")
                with self.engine.connect() as conn:
                    result = conn.execution_options(stream_results=True).execute(select_query)
                    for row in result:
                        batch.append(dict(row._mapping))
                        if len(batch) >= null_chunk_size:
                            arrow_table = pa.Table.from_pylist(batch)
                            pldf = pl.from_arrow(arrow_table)
                            for c in pldf.columns:
                                if not nullable_map.get(c, False) and pldf[c].null_count() > 0:
                                    nullable_map[c] = True
                            batch = []
                            batch_count += 1
                            if all(nullable_map.values()):
                                break
                            if null_max_chunks is not None and batch_count >= null_max_chunks:
                                break
                    # final partial batch
                    if batch and not all(nullable_map.values()):
                        arrow_table = pa.Table.from_pylist(batch)
                        pldf = pl.from_arrow(arrow_table)
                        for c in pldf.columns:
                            if not nullable_map.get(c, False) and pldf[c].null_count() > 0:
                                nullable_map[c] = True
                self._nullable_map = nullable_map
            nullable_map = self._nullable_map

        return schema_from_sample(sample_df, nullable_map)

    def get_data_iterator(self, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
        query = text(f"SELECT * FROM {self.table_name}")
        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(query)
            batch = []
            for row in result:
                batch.append(dict(row._mapping))
                if len(batch) >= chunk_size:
                    arrow_table = pa.Table.from_pylist(batch)
                    yield pl.from_arrow(arrow_table)
                    batch = []
            if batch:
                arrow_table = pa.Table.from_pylist(batch)
                yield pl.from_arrow(arrow_table)

    def get_row_count(self) -> int:
        query = text(f"SELECT COUNT(*) FROM {self.table_name}")
        with self.engine.connect() as conn:
            result = conn.execute(query).scalar()
        return int(result)