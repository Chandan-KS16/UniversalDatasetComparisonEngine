# utils/schema_helpers.py
from typing import Dict, Optional
import polars as pl

def infer_nullability_streaming_from_lazy(lf: pl.LazyFrame, chunk_size: int = 100_000, max_chunks: Optional[int] = None) -> Dict[str, bool]:
    """
    Stream the LazyFrame in chunks and return a map {col_name: bool} indicating
    whether any null was seen for that column.
    - max_chunks: if provided, limits the number of chunks to inspect (early stop).
    """
    nullable: Dict[str, bool] = {}
    cols = None
    chunks = 0

    # collect(streaming=True) produces a lazy execution that yields slices
    for batch in lf.collect(streaming=True).iter_slices(n_rows=chunk_size):
        if cols is None:
            cols = batch.columns
            nullable = {c: False for c in cols}
        chunks += 1
        # Check nulls in this chunk
        for c in cols:
            if not nullable[c] and batch[c].null_count() > 0:
                nullable[c] = True
        # If all columns already have been observed as nullable, we can stop early
        if all(nullable.values()):
            break
        if max_chunks is not None and chunks >= max_chunks:
            break

    # Ensure flags exist for all columns (False if not seen nullable)
    if cols:
        for c in cols:
            nullable.setdefault(c, False)
    return nullable


def schema_from_sample(df: pl.DataFrame, nullable_map: Dict[str, bool]) -> Dict[str, object]:
    """
    Build schema dict from a polars DataFrame sample and a nullable_map.
    Returns: {"columns": [ {"name":..., "dtype":..., "nullable": bool}, ... ] }
    """
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        cols.append({
            "name": col,
            "dtype": str(dtype),
            "nullable": bool(nullable_map.get(col, False))
        })
    return {"columns": cols}
