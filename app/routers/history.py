from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.services import history_manager

router = APIRouter(prefix="/history", tags=["History"])

@router.get("/")
def get_history(skip: int = 0, limit: int = 50, db: Session = Depends(get_db)):
    records = history_manager.get_history(db, skip, limit)
    return [r.history_to_entry(include_results=False) for r in records]


@router.get("/{history_id}")
def get_history_record(history_id: str, db: Session = Depends(get_db)):
    record = history_manager.get_history_by_id(db, history_id)
    if not record:
        raise HTTPException(status_code=404, detail="History record not found")
    return record.history_to_entry(include_results=True)

@router.post("/{history_id}/rerun")
def rerun_history(history_id: str, db: Session = Depends(get_db)):
    result = history_manager.rerun_history(db, history_id, comparison_fn=run_comparison)
    if not result:
        raise HTTPException(status_code=404, detail="History record not found")
    return result.history_to_version(include_result=False)


# Dummy function for now — replace with real comparison
def run_comparison(dataset_a, dataset_b):
    return {}, "<p>markup</p>", {"chart": []}

