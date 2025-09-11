# backend/compare/schema_comparator.py
from typing import Dict, Any, Iterable, Mapping, List, Tuple, Optional
from collections import Counter

from normalize.schema_normalizer import SchemaNormalizer
from .value_comparator import compare_values


_norm = SchemaNormalizer()


def _is_normalized(schema: Dict[str, Any]) -> bool:
    cols = schema.get("columns", [])
    return bool(cols) and isinstance(cols[0], dict) and "type" in cols[0]


def compare_schemas(
    schema_a: Dict[str, Any],
    schema_b: Dict[str, Any],
    source_a: str = "generic",
    source_b: str = "generic",
    label_a: str = "A",
    label_b: str = "B",
) -> Dict[str, Any]:
    """
    Compare two schemas (raw or normalized).

    Returns a dict:
      {
        "normalized_a": {...},
        "normalized_b": {...},
        "warnings": {"a": [...], "b": [...]},
        "findings": [...],          # issues from validation (severity, code, message, column)
        "summary": {counts...},     # simple counts and severity breakdown
      }
    """

    # normalize if needed
    if _is_normalized(schema_a):
        norm_a = {"columns": schema_a.get("columns", []), "warnings": []}
    else:
        norm_a = _norm.normalize_schema(schema_a, source=source_a)

    if _is_normalized(schema_b):
        norm_b = {"columns": schema_b.get("columns", []), "warnings": []}
    else:
        norm_b = _norm.normalize_schema(schema_b, source=source_b)

    # collect warnings from normalization
    warnings = {"a": norm_a.get("warnings", []), "b": norm_b.get("warnings", [])}

    # validation/findings (leverages normalizer logic)
    validation = _norm.validate_migration(norm_a, norm_b, src_label=label_a, dst_label=label_b)
    findings = validation.get("findings", [])

    # summary metrics
    total_cols_a = len(norm_a.get("columns", []))
    total_cols_b = len(norm_b.get("columns", []))
    severity_counts = Counter([f.get("severity", "info") for f in findings])
    code_counts = Counter([f.get("code", "UNKNOWN") for f in findings])

    summary = {
        "total_columns_a": total_cols_a,
        "total_columns_b": total_cols_b,
        "total_findings": len(findings),
        "severity_counts": dict(severity_counts),
        "top_issue_counts": dict(code_counts.most_common(10)),
    }

    return {
        "normalized_a": norm_a,
        "normalized_b": norm_b,
        "warnings": warnings,
        "findings": findings,
        "summary": summary,
    }


def compare_rows(
    rows_a: Iterable[Mapping[str, Any]],
    rows_b: Iterable[Mapping[str, Any]],
    key: Optional[str] = None,
    float_tol: float = 1e-9,
) -> Dict[str, Any]:
    """
    Compare two iterables of row mappings (list of dicts, DataFrame.to_dict('records'), etc).

    - If key is provided, rows are matched by that key (stable even if ordering differs).
    - If no key, rows are compared positionally (zip).
    Returns:
      {
        "total_cells": int,
        "match_count": int,
        "coerced_matches": int,
        "overall_pct": float,
        "mismatches": [ {row_id, column, type, expected, actual, details/match_type, coerced(bool)}, ... ],
        "changed_row_ids": [ ... ]
      }
    """
    # materialize as lists of dicts
    a_list = list(rows_a)
    b_list = list(rows_b)

    # build row pairs
    pairs: List[Tuple[Any, Mapping[str, Any], Mapping[str, Any]]] = []
    if key:
        # map by key value
        a_map = {r.get(key): r for r in a_list}
        b_map = {r.get(key): r for r in b_list}
        all_keys = sorted(set(a_map.keys()) | set(b_map.keys()))
        for k in all_keys:
            pairs.append((k, a_map.get(k, {}), b_map.get(k, {})))
    else:
        # positional zip; row_id is index
        n = max(len(a_list), len(b_list))
        for i in range(n):
            pairs.append((i, a_list[i] if i < len(a_list) else {}, b_list[i] if i < len(b_list) else {}))

    match_count = 0
    total_cells = 0
    coerced_matches = 0
    mismatches: List[Dict[str, Any]] = []
    changed_row_ids: List[Any] = []

    # decide column universe: union of columns across both sides for each row
    for row_id, a_row, b_row in pairs:
        row_has_change = False
        cols = sorted(set(list(a_row.keys()) + list(b_row.keys())))
        for col in cols:
            expected = a_row.get(col)
            actual = b_row.get(col)

            # call the helper you already imported
            is_match, has_type_mismatch, match_type = compare_values(expected, actual, float_tol=float_tol)

            total_cells += 1
            if is_match:
                match_count += 1
                if has_type_mismatch:
                    # logical match but types differ => coerced match entry (still indexed)
                    coerced_matches += 1
                    row_has_change = True
                    mismatches.append({
                        "row_id": row_id,
                        "column": col,
                        "type": "datatype_mismatch",
                        "expected": expected,
                        "actual": actual,
                        "match_type": match_type,
                        "coerced": True,
                    })
            else:
                # real value mismatch
                row_has_change = True
                mismatches.append({
                    "row_id": row_id,
                    "column": col,
                    "type": "value_mismatch",
                    "expected": expected,
                    "actual": actual,
                    "details": match_type,
                    "coerced": False,
                })
        if row_has_change:
            # ensure row_id is serializable as string for outputs (safe)
            changed_row_ids.append(str(row_id))

    overall_pct = round(100.0 * match_count / total_cells, 6) if total_cells else 100.0
    return {
        "total_cells": total_cells,
        "match_count": match_count,
        "coerced_matches": coerced_matches,
        "overall_pct": overall_pct,
        "mismatches": mismatches,
        "changed_row_ids": changed_row_ids,
    }
