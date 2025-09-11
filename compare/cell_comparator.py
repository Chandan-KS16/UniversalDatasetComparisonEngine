import polars as pl
from typing import List, Dict, Any, Optional
from adapters.base import DataSourceAdapter

def compare_cells(
    adapter_a: DataSourceAdapter,
    adapter_b: DataSourceAdapter,
    pk_cols: List[str],
    differing_pks: List[str],
    chunk_size: int = 10_000,
) -> List[Dict[str, Any]]:
    """
    Deep comparison of rows with same PK but differing row hashes.
    Returns a list of diffs:
      [{"pk": ..., "differences": [{"column":..., "a":..., "b":...}, ...]}, ...]
    """

    diffs: List[Dict[str, Any]] = []

    # Create lookup set for quick filtering
    pk_set = set(differing_pks)

    # Iterate both datasets
    map_a: Dict[str, Dict[str, Any]] = {}
    for df in adapter_a.get_data_iterator(chunk_size=chunk_size):
        for row in df.to_dicts():
            pk_val = "|".join([str(row[c]) if row[c] is not None else "NULL" for c in pk_cols])
            if pk_val in pk_set:
                map_a[pk_val] = row

    map_b: Dict[str, Dict[str, Any]] = {}
    for df in adapter_b.get_data_iterator(chunk_size=chunk_size):
        for row in df.to_dicts():
            pk_val = "|".join([str(row[c]) if row[c] is not None else "NULL" for c in pk_cols])
            if pk_val in pk_set:
                map_b[pk_val] = row

    # Compare row-by-row
    for pk in differing_pks:
        row_a = map_a.get(pk)
        row_b = map_b.get(pk)
        if row_a is None or row_b is None:
            continue

        row_diffs = []
        for col in row_a.keys():
            val_a = row_a.get(col)
            val_b = row_b.get(col)
            if val_a != val_b:
                row_diffs.append({"column": col, "a": val_a, "b": val_b})

        if row_diffs:
            diffs.append({"pk": pk, "differences": row_diffs})

    return diffs
