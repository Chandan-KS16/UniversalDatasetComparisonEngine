import polars as pl
import pyarrow as pa
from sqlalchemy import create_engine, inspect, text
from typing import Iterator, Dict, Any
from .base import DataSourceAdapter


class SQLAdapter(DataSourceAdapter):
    """Adapter for SQL databases using SQLAlchemy + Arrow."""

    def __init__(self, connection_string: str, table_name: str):
        self.engine = create_engine(connection_string)
        self.table_name = table_name

    def get_schema(self) -> Dict[str, Any]:
        insp = inspect(self.engine)
        columns = []
        for col in insp.get_columns(self.table_name):
            columns.append({
                "name": col["name"],
                "dtype": str(col["type"]),
                "nullable": col.get("nullable", "unknown")
            })
        return {"columns": columns}

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
