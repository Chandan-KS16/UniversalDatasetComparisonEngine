import os
import uuid
import polars as pl
import fsspec
from typing import Iterator, Dict, Any, Optional
from .base import DataSourceAdapter


UPLOADS_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)


class FileAdapter(DataSourceAdapter):
    """
    Adapter for handling:
      - Local files (CSV, Parquet, JSON)
      - Uploaded files (saved to ./uploads/)
      - Cloud files (s3://, gcs://, az://) via fsspec
    """

    def __init__(self, path: Optional[str] = None, file_obj: Optional[bytes] = None, encoding: str = "utf8"):
        """
        Args:
            path: str → path to local/cloud file
            file_obj: bytes → raw file content (e.g. from FastAPI UploadFile.file.read())
            encoding: str → text encoding
        """
        if file_obj is not None:
            # Derive extension (default to .csv if missing)
            ext = os.path.splitext(path or "")[1].lower()
            if not ext:
                ext = ".csv"

            fname = f"{uuid.uuid4().hex}{ext}"
            save_path = os.path.join(UPLOADS_DIR, fname)
            with open(save_path, "wb") as f:
                f.write(file_obj)
            self.path = save_path
        elif path:
            self.path = path
        else:
            raise ValueError("Either path or file_obj must be provided")

        self.encoding = encoding
        self.extension = os.path.splitext(self.path)[1].lower()

    def _lazyframe(self) -> pl.LazyFrame:
        """Return a Polars LazyFrame depending on file type."""
        if self.extension == ".csv":
            return pl.scan_csv(self.path, encoding=self.encoding)
        elif self.extension == ".parquet":
            return pl.scan_parquet(self.path)
        elif self.extension in (".json", ".ndjson"):
            # Try NDJSON first (fast + streaming)
            try:
                return pl.scan_ndjson(self.path, encoding=self.encoding)
            except Exception:
                # Fallback: normal JSON array (non-streaming)
                return pl.read_json(self.path).lazy()
        else:
            raise ValueError(f"Unsupported file type: {self.extension}")

    def get_schema(self) -> Dict[str, Any]:
        lf = self._lazyframe()
        df = lf.fetch(5)  # small sample
        schema = []
        for col, dtype in zip(df.columns, df.dtypes):
            schema.append({
                "name": col,
                "dtype": str(dtype),
                "nullable": "unknown"
            })
        return {"columns": schema}

    def get_data_iterator(self, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
        lf = self._lazyframe()
        for batch in lf.collect(streaming=True).iter_slices(n_rows=chunk_size):
            yield batch

    def get_row_count(self) -> int:
        lf = self._lazyframe()
        return lf.select(pl.len()).collect().item()
