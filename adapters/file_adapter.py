# adapters/file_adapter.py
import os
import uuid
import polars as pl
from typing import Iterator, Dict, Any, Optional
from .base import DataSourceAdapter
from utils.schema_helpers import infer_nullability_streaming_from_lazy, schema_from_sample

UPLOADS_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)


class FileAdapter(DataSourceAdapter):
    """
    Adapter for handling:
      - Local files (CSV, Parquet, JSON)
      - Uploaded files (saved to ./uploads/)
    """

    def __init__(self, path: Optional[str] = None, file_obj: Optional[bytes] = None, encoding: str = "utf8"):
        if file_obj is not None:
            ext = os.path.splitext(path or "")[1].lower() or ".csv"
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
        self._nullable_map: Optional[Dict[str, bool]] = None

    def _lazyframe(self) -> pl.LazyFrame:
        if self.extension == ".csv":
            return pl.scan_csv(self.path, encoding=self.encoding)
        elif self.extension == ".parquet":
            return pl.scan_parquet(self.path)
        elif self.extension in (".json", ".ndjson"):
            try:
                return pl.scan_ndjson(self.path, encoding=self.encoding)
            except Exception:
                return pl.read_json(self.path).lazy()
        else:
            raise ValueError(f"Unsupported file type: {self.extension}")

    def get_schema(self, stream_infer_nulls: bool = False, sample_size: int = 5, null_chunk_size: int = 100_000, null_max_chunks: Optional[int] = None) -> Dict[str, Any]:
        """
        Return schema with dtype and nullable (True/False).
        If stream_infer_nulls is True, will stream the dataset to detect nulls exactly (cached).
        """
        lf = self._lazyframe()
        # sample for dtype discovery
        sample_df = lf.fetch(sample_size)
        # fast (sample) null detection by default
        if not stream_infer_nulls:
            nullable_map = {c: sample_df[c].null_count() > 0 for c in sample_df.columns}
        else:
            # use cached if available
            if getattr(self, "_nullable_map", None) is None:
                self._nullable_map = infer_nullability_streaming_from_lazy(lf, chunk_size=null_chunk_size, max_chunks=null_max_chunks)
            nullable_map = self._nullable_map

        return schema_from_sample(sample_df, nullable_map)

    def get_data_iterator(self, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
        lf = self._lazyframe()
        for batch in lf.collect(streaming=True).iter_slices(n_rows=chunk_size):
            yield batch

    def get_row_count(self) -> int:
        lf = self._lazyframe()
        return lf.select(pl.len()).collect().item()
