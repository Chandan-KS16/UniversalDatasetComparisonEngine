import re
from typing import Dict, Any, List, Optional, Tuple

CanonicalType = str


class SchemaNormalizer:
    """
    Lightweight schema normalizer with minimal source-awareness.
    Preserves precision, scale, length, nullability, and detects risks:
      - string truncation
      - decimal precision/scale loss
      - integer width reduction
      - nullable drift
      - timezone differences
    """

    # -----------------------------
    # Public API
    # -----------------------------

    def normalize_schema(
        self, schema: Dict[str, Any], source: str = "generic"
    ) -> Dict[str, Any]:
        """Normalize adapter schema into canonical form."""
        cols_in = schema.get("columns", [])
        normalized, warnings = [], []
        for col in cols_in:
            norm, warns = self._normalize_column(col, source)
            normalized.append(norm)
            warnings.extend(warns)
        return {"columns": normalized, "warnings": warnings}

    def validate_migration(
        self,
        src_norm: Dict[str, Any],
        dst_norm: Dict[str, Any],
        src_label: str = "source",
        dst_label: str = "target",
    ) -> Dict[str, Any]:
        """Compare normalized schemas and detect migration risks."""
        src_map = {c["name"]: c for c in src_norm["columns"]}
        dst_map = {c["name"]: c for c in dst_norm["columns"]}
        findings = []

        # Missing/extra columns
        for c in src_map.keys() - dst_map.keys():
            findings.append(self._msg("error", "COLUMN_MISSING", f"{c} missing in {dst_label}", c))
        for c in dst_map.keys() - src_map.keys():
            findings.append(self._msg("warning", "EXTRA_COLUMN", f"{c} extra in {dst_label}", c))

        # Shared columns
        for c in src_map.keys() & dst_map.keys():
            findings.extend(self._compare_columns(src_map[c], dst_map[c], src_label, dst_label))

        return {"findings": findings}

    # -----------------------------
    # Internal: column normalization
    # -----------------------------

    def _normalize_column(
        self, col: Dict[str, Any], source: str
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        name = col.get("name")
        raw = col.get("dtype") or col.get("type") or "UNKNOWN"
        nullable = col.get("nullable", "unknown")
        if isinstance(nullable, str):
            nullable = {"true": True, "false": False}.get(nullable.lower(), "unknown")

        ctype, meta = self._parse_type(raw, source)
        normalized = {
            "name": name,
            "type": ctype,
            "nullable": nullable,
            "metadata": meta,
            "original": str(raw),
        }
        return normalized, []

    # -----------------------------
    # Type parsing
    # -----------------------------

    def _parse_type(self, raw: str, source: str) -> Tuple[CanonicalType, Dict[str, Any]]:
        t = raw.upper().strip()
        meta: Dict[str, Any] = {}

        # DECIMAL / NUMERIC(p,s)
        m = re.match(r"(DECIMAL|NUMERIC|NUMBER)\((\d+),\s*(\d+)\)", t)
        if m:
            return "DECIMAL", {"precision": int(m[2]), "scale": int(m[3])}
        if t.startswith(("DECIMAL", "NUMERIC", "NUMBER")):
            return "DECIMAL", {"precision": None, "scale": None}

        # VARCHAR/CHAR(n)
        m = re.match(r"(VARCHAR|CHAR|CHARACTER)\((\d+)\)", t)
        if m:
            return "STRING", {"length": int(m[2])}
        if any(x in t for x in ["STRING", "TEXT", "VARCHAR", "CHAR"]):
            return "STRING", {"length": None}

        # INT types
        if "BIGINT" in t:
            return "INT", {"bits": 64}
        if "SMALLINT" in t:
            return "INT", {"bits": 16}
        if "TINYINT" in t:
            return "INT", {"bits": 8}
        if "INT" in t:
            return "INT", {"bits": 32}

        # FLOAT/DOUBLE/REAL
        if "DOUBLE" in t or "FLOAT64" in t:
            return "FLOAT", {"bits": 64}
        if "FLOAT" in t or "REAL" in t:
            return "FLOAT", {"bits": 32}

        # Boolean
        if "BOOL" in t or "BOOLEAN" in t:
            return "BOOLEAN", {}

        # Dates/Times
        if "TIMESTAMP" in t:
            tz = "with" if "TZ" in t else "none"
            return "TIMESTAMP", {"timezone": tz}
        if "DATE" == t:
            return "DATE", {}
        if "TIME" == t:
            return "TIME", {}

        # JSON/STRUCT/ARRAY
        if "JSON" in t:
            return "JSON", {}
        if "STRUCT" in t or "OBJECT" in t or "MAP" in t:
            return "OBJECT", {}
        if "ARRAY" in t:
            return "ARRAY", {}

        # Binary
        if any(x in t for x in ["BINARY", "BYTEA", "BYTES"]):
            return "BINARY", {}

        return "UNKNOWN", {"original_hint": t}

    # -----------------------------
    # Validation checks
    # -----------------------------

    def _compare_columns(
        self, a: Dict[str, Any], b: Dict[str, Any], src: str, dst: str
    ) -> List[Dict[str, Any]]:
        out = []
        name, t1, t2 = a["name"], a["type"], b["type"]
        m1, m2 = a.get("metadata", {}), b.get("metadata", {})

        # Type mismatch
        if t1 != t2:
            out.append(self._msg("error", "TYPE_MISMATCH", f"{name}: {t1} vs {t2}", name))
            return out

        # Nullable drift
        if a["nullable"] != b["nullable"]:
            sev = "error" if a["nullable"] and not b["nullable"] else "warning"
            out.append(self._msg(sev, "NULLABILITY_CHANGE", f"{name} nullability {a['nullable']} -> {b['nullable']}", name))

        # STRING truncation
        if t1 == "STRING":
            if m1.get("length") and m2.get("length") and m2["length"] < m1["length"]:
                out.append(self._msg("error", "STRING_TRUNCATION", f"{name} length {m1['length']} -> {m2['length']}", name))

        # DECIMAL precision/scale loss
        if t1 == "DECIMAL":
            if (m1.get("precision") and m2.get("precision")) and m2["precision"] < m1["precision"]:
                out.append(self._msg("error", "DECIMAL_PRECISION_LOSS", f"{name} precision {m1['precision']} -> {m2['precision']}", name))
            if (m1.get("scale") and m2.get("scale")) and m2["scale"] < m1["scale"]:
                out.append(self._msg("error", "DECIMAL_SCALE_LOSS", f"{name} scale {m1['scale']} -> {m2['scale']}", name))

        # INT width reduction
        if t1 == "INT":
            if (m1.get("bits") and m2.get("bits")) and m2["bits"] < m1["bits"]:
                out.append(self._msg("error", "INT_WIDTH_LOSS", f"{name} {m1['bits']}-bit -> {m2['bits']}-bit", name))

        # TIMESTAMP timezone
        if t1 == "TIMESTAMP" and m1.get("timezone") != m2.get("timezone"):
            out.append(self._msg("warning", "TIMEZONE_CHANGE", f"{name} tz {m1.get('timezone')} -> {m2.get('timezone')}", name))

        return out

    # -----------------------------
    # Helper
    # -----------------------------

    def _msg(self, severity, code, msg, col):
        return {"severity": severity, "code": code, "message": msg, "column": col}
