import os
from typing import Iterator, Dict, Any, List
import snowflake.connector
import polars as pl

from adapters.base import DataSourceAdapter


class SnowflakeAdapter(DataSourceAdapter):
    def __init__(self):
        """
        Reads Snowflake configuration from environment variables.
        These can later be injected via Helm values.yaml or Kubernetes secrets.
        """
        self.conn = None
        self.config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "chunk_size": int(os.getenv("SNOWFLAKE_CHUNK_SIZE", "1000")),
            "table": None,  # runtime injected
        }

    def connect(self):
        if not self.conn:
            self.conn = snowflake.connector.connect(
                user=self.config["user"],
                password=self.config["password"],
                account=self.config["account"],
                warehouse=self.config["warehouse"],
                database=self.config.get("database"),
                schema=self.config.get("schema"),
                role=self.config.get("role"),
            )

    def get_schema(self, stream_infer_nulls: bool = False) -> Dict[str, Any]:
        """
        Fetch schema details for the configured table from INFORMATION_SCHEMA.COLUMNS.
        Returns dict with "columns" and "warnings" keys to match FileAdapter/SQLAdapter.
        """
        self.connect()
        database = self.config.get("database") or os.getenv("SNOWFLAKE_DATABASE")
        schema = self.config.get("schema") or os.getenv("SNOWFLAKE_SCHEMA")
        table_name = self.config.get("table")

        if not database or not schema or not table_name:
            raise ValueError("SnowflakeAdapter requires 'database', 'schema', and 'table' in config (or .env).")

        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM {database}.information_schema.columns
        WHERE table_schema = '{schema.upper()}'
          AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        return {
            "columns": [
                {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
                for row in results
            ],
            "warnings": []
        }

    def get_row_count(self, **kwargs) -> int:
        """Return total row count for the configured table."""
        self.connect()
        database = self.config.get("database") or os.getenv("SNOWFLAKE_DATABASE")
        schema = self.config.get("schema") or os.getenv("SNOWFLAKE_SCHEMA")
        table_name = self.config.get("table")

        if not database or not schema or not table_name:
            raise ValueError("SnowflakeAdapter requires 'database', 'schema', and 'table' in config (or .env).")

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_data_iterator(self, **kwargs) -> Iterator["pl.DataFrame"]:
        """Stream rows in Polars DataFrames for the configured table."""
        self.connect()
        database = self.config.get("database") or os.getenv("SNOWFLAKE_DATABASE")
        schema = self.config.get("schema") or os.getenv("SNOWFLAKE_SCHEMA")
        table_name = self.config.get("table")

        if not database or not schema or not table_name:
            raise ValueError("SnowflakeAdapter requires 'database', 'schema', and 'table' in config (or .env).")

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {database}.{schema}.{table_name}")
        col_names = [desc[0] for desc in cursor.description]  # fetch column names once

        while True:
            rows = cursor.fetchmany(self.config["chunk_size"])
            if not rows:
                break
            # convert rows (tuples) into Polars DataFrame
            df = pl.DataFrame(rows, schema=col_names, orient="row")
            yield df

        cursor.close()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _fetch_metadata(self, query: str, index: int = 1):
        """Helper to run SHOW queries and extract a specific column."""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = [row[index] for row in cursor.fetchall()]
        cursor.close()
        return results

    def list_databases(self):
        return self._fetch_metadata("SHOW DATABASES", index=1)

    def list_schemas(self, database: str):
        return self._fetch_metadata(f"SHOW SCHEMAS IN DATABASE {database}", index=1)

    def list_tables(self, database: str, schema: str):
        return self._fetch_metadata(f"SHOW TABLES IN SCHEMA {database}.{schema}", index=1)
