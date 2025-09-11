import os, json, time
from typing import Dict, Any, List

REPORTS_DIR = os.path.join(os.getcwd(), "reports")
HISTORY_FILE = os.path.join(REPORTS_DIR, "history.json")
RETENTION_DAYS = 60  # keep reports for 60 days

os.makedirs(REPORTS_DIR, exist_ok=True)


def _now_epoch() -> int:
    return int(time.time())


def _load_history() -> List[Dict[str, Any]]:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _write_history(history: List[Dict[str, Any]]):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def _prune(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cutoff = _now_epoch() - RETENTION_DAYS * 86400
    keep = []
    for e in history:
        ts = e.get("timestamp", 0)
        try:
            ts = int(ts)
        except Exception:
            ts = 0
        if ts >= cutoff:
            keep.append(e)
        else:
            jid = e.get("job_id")
            if jid:
                p = os.path.join(REPORTS_DIR, f"report_{jid}.json")
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
    return keep


def save_history(entry: Dict[str, Any]):
    """Append entry (timestamp should be epoch int) and prune old entries."""
    history = _load_history()
    history.append(entry)
    history = _prune(history)
    _write_history(history)


def load_history() -> List[Dict[str, Any]]:
    return _load_history()
