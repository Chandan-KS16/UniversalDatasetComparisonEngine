import polars as pl
from typing import Iterator, Dict, Any
from .base import DataSourceAdapter


class CloudAdapter(DataSourceAdapter):
    """Adapter for cloud storage (S3, GCS, Azure Blob) using fsspec + Polars."""

    def __init__(self, path: str, encoding: str = "utf8"):
        # e.g., s3://bucket/file.csv, gs://bucket/file.parquet, abfss://container/file.json
        self.path = path
        self.encoding = encoding
        self.extension = path.split(".")[-1].lower()

    def get_schema(self) -> Dict[str, Any]:
        if self.extension == "csv":
            lf = pl.scan_csv(self.path, encoding=self.encoding)
        elif self.extension == "parquet":
            lf = pl.scan_parquet(self.path)
        elif self.extension == "json":
            lf = pl.scan_ndjson(self.path, encoding=self.encoding)
        else:
            raise ValueError(f"Unsupported file type: {self.extension}")

        df = lf.fetch(5)
        schema = []
        for col, dtype in zip(df.columns, df.dtypes):
            schema.append({
                "name": col,
                "dtype": str(dtype),
                "nullable": "unknown"  # object storage files don’t store nullability
            })
        return {"columns": schema}

    def get_data_iterator(self, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
        if self.extension == "csv":
            lf = pl.scan_csv(self.path, encoding=self.encoding)
        elif self.extension == "parquet":
            lf = pl.scan_parquet(self.path)
        elif self.extension == "json":
            lf = pl.scan_ndjson(self.path, encoding=self.encoding)
        else:
            raise ValueError(f"Unsupported file type: {self.extension}")

        for batch in lf.collect(streaming=True).iter_slices(n_rows=chunk_size):
            yield batch

    def get_row_count(self) -> int:
        if self.extension == "csv":
            lf = pl.scan_csv(self.path, encoding=self.encoding)
        elif self.extension == "parquet":
            lf = pl.scan_parquet(self.path)
        elif self.extension == "json":
            lf = pl.scan_ndjson(self.path, encoding=self.encoding)
        else:
            raise ValueError(f"Unsupported file type: {self.extension}")
        return lf.select(pl.len()).collect().item()
