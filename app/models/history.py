from sqlalchemy import Column, DateTime, JSON, Text, ForeignKey
from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime
from app.database import Base

class HistoryRecord(Base):
    __tablename__ = "history"

    id = Column(CHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    dataset_a = Column(JSON, nullable=False)
    dataset_b = Column(JSON, nullable=False)

    json_report = Column(JSON, nullable=False)
    markup_report = Column(Text, nullable=True)
    visual_report = Column(JSON, nullable=True)

    rerun_parent_id = Column(CHAR(36), ForeignKey("history.id"), nullable=True)
    reruns = relationship("HistoryRecord", remote_side=[id])

    tags = Column(JSON, default=[])

def _summary_from_json(report):
    """Extract minimal summary from json_report for version cards."""
    if not report or not isinstance(report, dict):
        return {}
    for k in ("summary", "stats", "mismatches"):
        if k in report and isinstance(report[k], dict):
            return report[k]
    return {k: v for k, v in report.items() if isinstance(v, (int, float))}

def history_to_version(self, include_result: bool = False):
    return {
        "version_id": self.id,
        "timestamp": self.timestamp,
        "is_rerun": bool(self.rerun_parent_id),
        "summary": _summary_from_json(self.json_report),
        **({"result": self.json_report} if include_result else {})
    }

def history_to_entry(self, include_results: bool = False):
    versions = [self.history_to_version(include_result=include_results)]
    versions += [r.history_to_version(include_result=include_results) for r in sorted(self.reruns, key=lambda x: x.timestamp)]
    return {
        "id": self.id,
        "dataset_a": self.dataset_a,
        "dataset_b": self.dataset_b,
        "versions": versions,
    }

# Attach helpers
HistoryRecord.history_to_version = history_to_version
HistoryRecord.history_to_entry = history_to_entry
