# backend/report/report_models.py
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class SchemaComparison(BaseModel):
    status: str
    missing_columns: List[str]
    extra_columns: List[str]
    type_mismatches: Dict[str, Any]
    nullable_mismatches: Dict[str, Any]
    order_difference: bool

class DataComparison(BaseModel):
    total_rows_a: Optional[int]
    total_rows_b: Optional[int]
    total_rows_compared: int
    match_percentage: float
    missing_rows_in_b: int
    extra_rows_in_b: int
    value_mismatches: List[Dict[str, Any]]

class FinalReport(BaseModel):
    job_id: str
    start_time: float
    end_time: float
    schema_comparison: SchemaComparison
    data_comparison: DataComparison
    summary: str
    artifacts: Optional[Dict[str, str]]
