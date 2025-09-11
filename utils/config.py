# backend/utils/config.py
import os
from dotenv import load_dotenv

load_dotenv()

def load_config():
    """
    Minimal config loader: loads env vars as needed.
    """
    return {
        "DEFAULT_CHUNK_SIZE": int(os.getenv("DEFAULT_CHUNK_SIZE", "100000")),
        "REPORT_DIR": os.getenv("REPORT_DIR", "reports")
    }
