# value_comparator.py
"""
Tiny standalone comparator used for ephemeral (non-destructive) value comparison.
Provides a single function `compare_values(expected, actual, float_tol=1e-9)`
that returns (is_match: bool, has_type_mismatch: bool, match_type: str).
"""

from datetime import datetime
from typing import Tuple, Any

__all__ = ["compare_values"]


def compare_values(expected: Any, actual: Any, float_tol: float = 1e-9) -> Tuple[bool, bool, str]:
    def _is_null(x):
        return x is None

    def _try_num(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        if isinstance(v, str):
            s = v.strip()
            if s == "":
                return None
            try:
                return float(s.replace(",", ""))
            except Exception:
                return None
        return None

    def _try_bool(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            if v == 0:
                return False
            if v == 1:
                return True
            return None
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "t", "yes", "1"):
                return True
            if s in ("false", "f", "no", "0"):
                return False
        return None

    def _try_date_iso(v):
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                # conservative ISO-only parsing
                return datetime.fromisoformat(s)
            except Exception:
                return None
        return None

    # both null
    if _is_null(expected) and _is_null(actual):
        return True, False, "both_null"

    # detect lightweight types
    t_exp = ("bool" if _try_bool(expected) is not None else
             "number" if _try_num(expected) is not None else
             "datetime" if _try_date_iso(expected) is not None else
             "string" if isinstance(expected, str) else type(expected).__name__.lower())

    t_act = ("bool" if _try_bool(actual) is not None else
             "number" if _try_num(actual) is not None else
             "datetime" if _try_date_iso(actual) is not None else
             "string" if isinstance(actual, str) else type(actual).__name__.lower())

    has_type_mismatch = (t_exp != t_act)

    # quick exact equality (covers many coerced-equals too)
    try:
        if expected == actual:
            return True, has_type_mismatch, ("exact" if not has_type_mismatch else "coerced_exact")
    except Exception:
        pass

    # numeric handling (both sides numeric-like)
    n_exp = _try_num(expected)
    n_act = _try_num(actual)
    if (n_exp is not None) and (n_act is not None):
        denom = max(abs(n_exp), abs(n_act), 1.0)
        rel = abs(n_exp - n_act) / denom
        if rel <= float_tol:
            return True, has_type_mismatch, "numeric_tol"
        return False, has_type_mismatch, f"numeric_rel={1.0 - rel:.4f}"

    # bool <-> number equivalence (0/1)
    b_exp = _try_bool(expected)
    b_act = _try_bool(actual)
    if (b_exp is not None) and (n_act in (0, 1) if n_act is not None else False):
        if (b_exp and n_act == 1) or (not b_exp and n_act == 0):
            return True, True, "coerced_bool_number"
    if (b_act is not None) and (n_exp in (0, 1) if n_exp is not None else False):
        if (b_act and n_exp == 1) or (not b_act and n_exp == 0):
            return True, True, "coerced_bool_number"

    # simple case-insensitive string equality
    if isinstance(expected, str) and isinstance(actual, str):
        if expected.strip().lower() == actual.strip().lower():
            return True, has_type_mismatch, "case_insensitive_exact"
        return False, has_type_mismatch, "string_mismatch"

    # datetime exact or same-day (if ISO parsed)
    d_exp = _try_date_iso(expected)
    d_act = _try_date_iso(actual)
    if (d_exp is not None) and (d_act is not None):
        if d_exp == d_act:
            return True, has_type_mismatch, "datetime_exact"
        if d_exp.date() == d_act.date():
            return True, has_type_mismatch, "datetime_same_day"
        return False, has_type_mismatch, "datetime_mismatch"

    # fallback: not a match
    return False, has_type_mismatch, "mismatch"


if __name__ == "__main__":
    # tiny smoke tests (useful for presentation/demo)
    assert compare_values("30", 30)[0] is True
    assert compare_values(None, None) == (True, False, "both_null")
    assert compare_values(True, 1)[:2] == (True, True)
    assert compare_values("Hello", " hello ")[0] is True
    print("value_comparator smoke checks ok")
