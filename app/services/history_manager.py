from sqlalchemy.orm import Session
from app.models.history import HistoryRecord
import uuid

def save_history(db: Session, dataset_a, dataset_b, json_report, markup_report=None, visual_report=None, tags=None):
    record = HistoryRecord(
        id=str(uuid.uuid4()),
        dataset_a=dataset_a,
        dataset_b=dataset_b,
        json_report=json_report,
        markup_report=markup_report,
        visual_report=visual_report,
        tags=tags or ["original"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record

def get_history(db: Session, skip=0, limit=50):
    return db.query(HistoryRecord).offset(skip).limit(limit).all()

def get_history_by_id(db: Session, history_id: str):
    return db.query(HistoryRecord).filter(HistoryRecord.id == history_id).first()

def rerun_history(db: Session, history_id: str, comparison_fn):
    old_record = get_history_by_id(db, history_id)
    if not old_record:
        return None

    # Call comparison function with old dataset metadata
    result_json, result_markup, result_visual = comparison_fn(
        old_record.dataset_a, old_record.dataset_b
    )

    new_record = HistoryRecord(
        dataset_a=old_record.dataset_a,
        dataset_b=old_record.dataset_b,
        json_report=result_json,
        markup_report=result_markup,
        visual_report=result_visual,
        rerun_parent_id=old_record.id,
        tags=["rerun"]
    )

    db.add(new_record)
    db.commit()
    db.refresh(new_record)

    return new_record


def compute_diff(old_json, new_json):
    # placeholder: implement a JSON diff
    return {"changed_keys": [], "differences": []}
