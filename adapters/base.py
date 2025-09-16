# adapters/base.py

from abc import ABC, abstractmethod
from logging import config
from typing import Iterator, Dict, Any
import polars as pl


class DataSourceAdapter(ABC):
    """Abstract base class for all dataset adapters."""

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Return schema as dict with columns, types, nullability."""
        pass

    @abstractmethod
    def get_data_iterator(self, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
        """Yield data in Polars DataFrame chunks."""
        pass

    @abstractmethod
    def get_row_count(self) -> int:
        """Return total row count if available, else -1."""
        pass


# -------------------------------
# Adapter Factory
# -------------------------------

def adapter_factory(config: Dict[str, Any]) -> DataSourceAdapter:
    """
    Factory to create the right adapter based on source_type in config.

    Accepts both "canonical" CompareRequest style:
      {"source_type":"file","config":{"path": "...", "encoding":"utf8"}}

    And legacy/flat style:
      {"source_type":"file","path":"...", "encoding":"utf8"}

    For SQL:
      canonical: {"source_type":"sql","config":{"connection_string":"...","table":"..."}}
      legacy:    {"source_type":"sql","connection_string":"...","table":"..."}

    For cloud:
      canonical: {"source_type":"cloud","config":{"path":"s3://bucket/key"}}
      legacy:    {"source_type":"cloud","path":"s3://bucket/key"}
    """
    
        # normalize config key for backward compatibility
    if "source_type" in config and "type" not in config:
        config["type"] = config["source_type"]

        
    if not isinstance(config, dict):
        raise ValueError("adapter_factory: config must be a dict")

    source_type = config.get("source_type")
    if not source_type:
        raise ValueError("Config must include 'source_type'")

    # prefer nested config dict if present
    nested = config.get("config") if isinstance(config.get("config"), dict) else {}

    def get(key: str, default=None):
        """Prefer nested[key] then top-level config[key]."""
        return nested.get(key, config.get(key, default))

    if source_type == "sql":
        from .sql_adapter import SQLAdapter
        conn = get("connection_string")
        table = get("table")
        if not conn or not table:
            raise ValueError("SQL config requires 'connection_string' and 'table'")
        return SQLAdapter(conn, table)

    elif source_type == "file":
        from .file_adapter import FileAdapter
        path = get("path")
        encoding = get("encoding", "utf8")
        # Also support passing raw bytes via key 'file_obj' (legacy upload endpoints)
        file_obj = get("file_obj", None)
        if not path and file_obj is None:
            raise ValueError("File config requires 'path' or 'file_obj'")
        # FileAdapter signature: FileAdapter(path: Optional[str]=None, file_obj: Optional[bytes]=None, encoding: str="utf8")
        return FileAdapter(path=path, file_obj=file_obj, encoding=encoding)

    elif source_type == "cloud":
        from .cloud_adapter import CloudAdapter
        path = get("path") or get("uri") or get("url")
        encoding = get("encoding", "utf8")
        if not path:
            raise ValueError("Cloud config requires 'path' (or 'uri'/'url')")
        return CloudAdapter(path=path, encoding=encoding)
    
    elif config["type"] == "snowflake":
        from adapters.snowflake_adapter import SnowflakeAdapter
        adapter = SnowflakeAdapter()
        adapter.config.update(config.get("config", {}))  # merge runtime config (e.g. table)
        return adapter


    else:
        raise ValueError(f"Unsupported source_type: {source_type}")
