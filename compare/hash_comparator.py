import polars as pl
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from adapters.base import DataSourceAdapter

def _make_row_hash(row: Dict[str, Any]) -> str:
    """
    Compute a null-safe hash for a row dictionary.
    Ensures same values across systems hash identically.
    """
    vals = []
    for k in sorted(row.keys()):
        v = row[k]
        if v is None:
            vals.append("NULL")
        else:
            vals.append(str(v))
    concat = "|".join(vals)
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


def _candidate_pk_detection(df: pl.DataFrame) -> Optional[List[str]]:
    """
    Try to detect a unique column or combination of columns.
    Limited to first N rows to avoid heavy compute.
    """
    n = min(len(df), 10000)
    sub = df[:n]

    # Check single column uniqueness
    for col in df.columns:
        if sub[col].n_unique() == n:
            return [col]

    # Check two-column combos (lightweight)
    cols = df.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            combined = sub.select([cols[i], cols[j]]).unique()
            if len(combined) == n:
                return [cols[i], cols[j]]

    return None


def _prepare_pk_and_hash(
    adapter: DataSourceAdapter, pk_cols: Optional[List[str]] = None, chunk_size: int = 100_000
) -> Tuple[Dict[str, str], int]:
    """
    Build {PK: row_hash} for the dataset.
    Returns (mapping, row_count).
    """
    mapping: Dict[str, str] = {}
    total_rows = 0

    for df in adapter.get_data_iterator(chunk_size=chunk_size):
        # Detect PK if not given
        if pk_cols is None:
            pk_cols = _candidate_pk_detection(df)
            if pk_cols is None:
                # fallback: surrogate PK = row_hash
                for row in df.to_dicts():
                    rh = _make_row_hash(row)
                    mapping[rh] = rh
                    total_rows += 1
                continue

        # Build PK-based mapping
        for row in df.to_dicts():
            pk_val = "|".join([str(row[c]) if row[c] is not None else "NULL" for c in pk_cols])
            rh = _make_row_hash(row)
            mapping[pk_val] = rh
            total_rows += 1

    return mapping, total_rows


def compare_datasets(
    adapter_a: DataSourceAdapter,
    adapter_b: DataSourceAdapter,
    pk_cols: Optional[List[str]] = None,
    chunk_size: int = 100_000,
) -> Dict[str, Any]:
    """
    Compare two datasets row-by-row using PK or surrogate PK.
    """

    # Build mappings
    map_a, rows_a = _prepare_pk_and_hash(adapter_a, pk_cols, chunk_size)
    map_b, rows_b = _prepare_pk_and_hash(adapter_b, pk_cols, chunk_size)

    # Compare PK sets
    keys_a = set(map_a.keys())
    keys_b = set(map_b.keys())

    missing_in_b = sorted(keys_a - keys_b)
    extra_in_b = sorted(keys_b - keys_a)

    # Compare shared rows by row_hash
    changed = [k for k in keys_a & keys_b if map_a[k] != map_b[k]]

    summary = {
        "rows_a": rows_a,
        "rows_b": rows_b,
        "missing_in_b": len(missing_in_b),
        "extra_in_b": len(extra_in_b),
        "changed": len(changed),
    }

    details = {
        "missing_in_b": missing_in_b[:100],  # sample only
        "extra_in_b": extra_in_b[:100],
        "changed": changed[:100],
    }

    return {"summary": summary, "details": details}
