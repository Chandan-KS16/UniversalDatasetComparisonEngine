import os
import time
import uuid
import json
import csv
import traceback
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Adapters and comparison
from adapters.base import adapter_factory
from adapters.file_adapter import FileAdapter
from normalize.schema_normalizer import SchemaNormalizer
from compare.schema_comparator import compare_schemas
from compare.hash_comparator import compare_datasets
from compare.cell_comparator import compare_cells
from compare.schema_comparator import compare_rows
from compare.schema_comparator import compare_schemas

# Value comparator for coercion enrichment
from compare.value_comparator import compare_values

# History manager (summaries)
from report.history_manager import save_history, load_history

# ------------------- FastAPI setup -------------------
app = FastAPI(title="Universal Dataset Comparison Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust later for frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------- Directories -------------------
UPLOADS_DIR = os.path.join(os.getcwd(), "uploads")
REPORTS_DIR = os.path.join(os.getcwd(), "reports")
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# ------------------- Minimal Safe Synthetic ID -------------------
def ensure_pk(path_a: str, path_b: str, pk_list: Optional[List[str]]):
    """Minimal safe synthetic ID generation"""
    if pk_list: return path_a, path_b, pk_list
    
    # Store original file states for integrity check
    original_size_a = os.path.getsize(path_a)
    original_size_b = os.path.getsize(path_b)

        # Preserve original file extensions
    ext_a = '.csv' if path_a.endswith('.csv') else '.json'
    ext_b = '.csv' if path_b.endswith('.csv') else '.json'

    new_a = os.path.join(UPLOADS_DIR, f"{uuid.uuid4().hex}_a{ext_a}")
    new_b = os.path.join(UPLOADS_DIR, f"{uuid.uuid4().hex}_b{ext_b}")

    def _safe_inject(in_path, out_path):
        if in_path.endswith(".csv"):
            with open(in_path, newline="", encoding="utf-8") as fr, \
                 open(out_path, "w", newline="", encoding="utf-8") as fw:
                rows = list(csv.reader(fr))
                if rows and "Id" not in rows[0]:
                    csv.writer(fw).writerow(["Id"] + rows[0])
                    for i, row in enumerate(rows[1:], 1):
                        csv.writer(fw).writerow([i] + row)
                else:
                    csv.writer(fw).writerows(rows)
        else:
            with open(in_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            for i, obj in enumerate(data, 1):
                if isinstance(obj, dict) and "Id" not in obj:
                    obj["Id"] = i
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
    
    # Safe injection with basic integrity validation
    try:
        _safe_inject(path_a, new_a)
        _safe_inject(path_b, new_b)
        
        # Quick integrity check: ensure original files unchanged
        if (os.path.getsize(path_a) != original_size_a or 
            os.path.getsize(path_b) != original_size_b):
            raise Exception("Original files were modified during processing")
            
    except Exception:
        # Cleanup on any error
        for f in [new_a, new_b]:
            try: os.remove(f)
            except: pass
        raise
        
    return new_a, new_b, ["Id"]

# ------------------- Request Models -------------------
class DataSourceConfig(BaseModel):
    source_type: str
    config: Dict[str, Any] = {}

class CompareRequest(BaseModel):
    dataset_a: DataSourceConfig
    dataset_b: DataSourceConfig
    primary_key: Optional[List[str]] = None
    options: Optional[Dict[str, Any]] = {}

# ------------------- Endpoints -------------------
@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/upload/a")
async def upload_dataset_a(file: UploadFile = File(...)):
    try:
        file_bytes = await file.read()
        adapter = FileAdapter(path=file.filename, file_obj=file_bytes)
        return {
            "message": "Dataset A uploaded successfully",
            "original_name": file.filename,
            "stored_path": adapter.path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload A failed: {e}")


@app.post("/upload/b")
async def upload_dataset_b(file: UploadFile = File(...)):
    try:
        file_bytes = await file.read()
        adapter = FileAdapter(path=file.filename, file_obj=file_bytes)
        return {
            "message": "Dataset B uploaded successfully",
            "original_name": file.filename,
            "stored_path": adapter.path,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload B failed: {e}")


@app.post("/compare")
def compare(request: CompareRequest):
    start_time = time.time()
    job_id = str(uuid.uuid4())

    # right before adapter_factory(...) lines in compare(...)
    print("DEBUG dataset_a dict:", request.dataset_a.dict())
    print("DEBUG dataset_b dict:", request.dataset_b.dict())


    # Build adapters (robust to adapters that expect path at different places)
    try:
        a_dict = request.dataset_a.dict()
        b_dict = request.dataset_b.dict()
        try:
            adapter_a = adapter_factory(a_dict)
            adapter_b = adapter_factory(b_dict)
        except KeyError as ke:
            # If adapter raised KeyError('path'), try to inject path from config and retry once
            if str(ke) in ("'path'", "path"):
                # make shallow copies so we don't mutate Pydantic internals
                a_fixed = dict(a_dict)
                b_fixed = dict(b_dict)
                if "config" in a_dict and isinstance(a_dict["config"], dict) and "path" in a_dict["config"]:
                    a_fixed.setdefault("path", a_dict["config"]["path"])
                if "config" in b_dict and isinstance(b_dict["config"], dict) and "path" in b_dict["config"]:
                    b_fixed.setdefault("path", b_dict["config"]["path"])
                # try again
                adapter_a = adapter_factory(a_fixed)
                adapter_b = adapter_factory(b_fixed)
            else:
                raise
    except HTTPException:
        raise
    except Exception as e:
        # keep behavior consistent: wrap other errors as adapter creation failure
        raise HTTPException(status_code=400, detail=f"Adapter creation failed: {e}")


    # Schema comparison
    # ---------- SCHEMA COMPARISON WITH NULLABILITY OPTIONS ----------
    try:
        # options: {"check_nullability": "none"|"sample"|"stream"}
        check_nulls = (request.options or {}).get("check_nullability", "sample")

        def _get_schema(adapter):
            if check_nulls == "none":
                # no null inference, just dtype sample
                raw = adapter.get_schema(stream_infer_nulls=False)
                # override all nullables as False (ignore nullability)
                for col in raw["columns"]:
                    col["nullable"] = False
                return raw
            elif check_nulls == "stream":
                return adapter.get_schema(stream_infer_nulls=True)
            else:  # "sample" or fallback
                return adapter.get_schema(stream_infer_nulls=False)

        schema_a = _get_schema(adapter_a)
        schema_b = _get_schema(adapter_b)

        schema_report = compare_schemas(
            schema_a, schema_b,
            source_a=request.dataset_a.source_type,
            source_b=request.dataset_b.source_type,
            label_a="A", label_b="B",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema comparison failed: {e}")
# ---------- END SCHEMA COMPARISON ----------


    # ---------- PRIMARY KEY VALIDATION (conservative) ----------
    # normalize primary_key to a list (supports string or list)
    pk_list = None
    if request.primary_key:
        if isinstance(request.primary_key, str):
            # allow comma-separated or single string
            pk_list = [p.strip() for p in request.primary_key.split(",") if p.strip()]
        elif isinstance(request.primary_key, list):
            pk_list = [str(p) for p in request.primary_key]
        else:
            # fallback: try to coerce
            try:
                pk_list = list(request.primary_key)
            except Exception:
                pk_list = None

    # If primary key is required by your flow, enforce it
    if not pk_list:
        raise HTTPException(status_code=400, detail="primary_key is required (provide column name(s) as string or JSON list)")

    # extract column names from schemas (your get_schema returns {"columns":[{"name":...},...]})
    cols_a = [c.get("name") for c in schema_a.get("columns", []) if c.get("name")]
    cols_b = [c.get("name") for c in schema_b.get("columns", []) if c.get("name")]

    missing_in_a = [c for c in pk_list if c not in cols_a]
    missing_in_b = [c for c in pk_list if c not in cols_b]

    if missing_in_a or missing_in_b:
        # provide helpful sample of available columns (limit to 20 to avoid huge responses)
        sample_a = cols_a[:20]
        sample_b = cols_b[:20]
        raise HTTPException(
            status_code=400,
            detail={
                "msg": "Primary key column(s) not found",
                "missing_in_a": missing_in_a,
                "missing_in_b": missing_in_b,
                "available_a_sample": sample_a,
                "available_b_sample": sample_b
            }
        )

    # pass the normalized pk_list into downstream call
    request.primary_key = pk_list
    # ---------- END PK VALIDATION ----------

    # Hash comparison
    try:
        data_report = compare_datasets(adapter_a, adapter_b, pk_cols=request.primary_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data comparison failed: {e}")

    # Cell comparison (optional)
    cell_report = []
    if data_report["summary"]["changed"] > 0 and request.primary_key:
        try:
            cell_report = compare_cells(
                adapter_a, adapter_b,
                pk_cols=request.primary_key,
                differing_pks=data_report["details"]["changed"],
            )
        except Exception:
            pass

    # ---- Coercion enrichment ----
    coerced_matches, cell_level_mismatches = 0, []
    for entry in (cell_report or []):
        pk = entry.get("pk")
        for diff in entry.get("differences", []):
            try:
                is_match, has_type_mismatch, match_type = compare_values(diff.get("a"), diff.get("b"))
            except Exception:
                is_match, has_type_mismatch, match_type = False, False, "error"
            if is_match and has_type_mismatch:
                coerced_matches += 1
                cell_level_mismatches.append({
                    "row_id": str(pk),
                    "column": diff.get("column"),
                    "expected": diff.get("a"),
                    "actual": diff.get("b"),
                    "match_type": match_type,
                    "coerced": True,
                })
    # ---- end enrichment ----

    # Final report
    report = {
        "job_id": job_id,
        "start_time": start_time,
        "end_time": time.time(),
        "schema_comparison": schema_report,
        "data_comparison": {
            **data_report,
            "summary": {
                **data_report.get("summary", {}),
                "coerced_matches": coerced_matches,
            },
            "details": {
                **data_report.get("details", {}),
                "cell_level_mismatches": cell_level_mismatches,
            },
        },
        "cell_comparison": cell_report,
        "summary": {
            "schema_findings": len(schema_report["findings"]),
            "rows_a": data_report["summary"]["rows_a"],
            "rows_b": data_report["summary"]["rows_b"],
            "changed_rows": data_report["summary"]["changed"],
            "coerced_matches": coerced_matches,
        },
        "datasets": {
            "dataset_a": request.dataset_a.config.get("path"),
            "dataset_b": request.dataset_b.config.get("path"),
        },
    }

    # Save full report to file
    report_path = os.path.join(REPORTS_DIR, f"report_{job_id}.json")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save report: {e}")

    # Save summary to history.json
    history_entry = {
        "job_id": report["job_id"],
        "timestamp": int(time.time()),  # epoch seconds
        "dataset_a": report["datasets"]["dataset_a"],
        "dataset_b": report["datasets"]["dataset_b"],
        "rows_a": report["summary"]["rows_a"],
        "rows_b": report["summary"]["rows_b"],
        "changed_rows": report["summary"]["changed_rows"],
        "schema_findings": report["summary"]["schema_findings"],
    }
    save_history(history_entry)

    return report

@app.post("/compare/files")
async def compare_files(
    dataset_a: UploadFile = File(...),
    dataset_b: UploadFile = File(...),
    primary_key: Optional[str] = Form(None),  # accepts 'CustomerID' or '["CustomerID"]' or 'id1,id2'
    check_nullability: Optional[str] = Form("sample")  # "none" | "sample" | "stream"
):
    """
    Accept two uploaded files and forward them into the canonical compare pipeline.
    Saves files to uploads/, builds CompareRequest, and calls existing compare().
    """
    async def _save(upload: UploadFile) -> str:
        safe_name = f"{int(time.time())}_{uuid.uuid4().hex}_{os.path.basename(upload.filename or 'upload')}"
        dest = os.path.join(UPLOADS_DIR, safe_name)
        contents = await upload.read()
        with open(dest, "wb") as f:
            f.write(contents)
        return dest

    path_a = await _save(dataset_a)
    path_b = await _save(dataset_b)

    # ----- replace primary_key parsing in /compare/files with placeholder rejection -----
    pk_list = None
    if primary_key:
        # quick check for obvious placeholder strings (raw input)
        placeholder_values = {"string", "String", "<string>", "<primary_key>", "<pk>", "primary_key"}
        if isinstance(primary_key, str) and primary_key.strip() in placeholder_values:
            raise HTTPException(
                status_code=400,
                detail="primary_key appears to be a placeholder. Provide a real column name (e.g. CustomerID) or leave blank."
            )

        # try JSON first (e.g. '["CustomerID"]' or '"CustomerID"')
        try:
            parsed = json.loads(primary_key)
            if isinstance(parsed, list):
                pk_list = [str(x).strip() for x in parsed if str(x).strip()]
            else:
                pk_list = [str(parsed).strip()] if str(parsed).strip() else None
        except Exception:
            # fallback: treat as comma-separated list or single name
            pk_list = [p.strip() for p in primary_key.split(",") if p.strip()]

        # final safety: if parsed list contains placeholder values, reject
        if pk_list and any(p in placeholder_values for p in pk_list):
            raise HTTPException(
                status_code=400,
                detail="primary_key contains placeholder values (e.g. 'string'). Provide real column name(s) or leave blank."
            )
    # -------------------------------------------------------------------------------

    # Ensure pk is usable (auto-add Id if missing)
    path_a, path_b, pk_list = ensure_pk(path_a, path_b, pk_list)

    # ----- validate nullability option -----
    allowed_null_opts = {"none", "sample", "stream"}
    if check_nullability not in allowed_null_opts:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid check_nullability value '{check_nullability}'. Allowed: {sorted(allowed_null_opts)}"
        )

    # Build CompareRequest with options
    req = CompareRequest(
        dataset_a=DataSourceConfig(source_type="file", config={"path": path_a}),
        dataset_b=DataSourceConfig(source_type="file", config={"path": path_b}),
        primary_key=pk_list,
        options={"check_nullability": check_nullability}
    )

    try:
        result = compare(req)
        return result
    except Exception as e:
        tb = traceback.format_exc()
        print("COMPARE FILES ERROR:", e)
        print(tb)
        raise HTTPException(status_code=500, detail={
            "error": str(e),
            "traceback": tb.splitlines()[-30:]  # last 30 lines of the trace
        })


@app.get("/history")
def get_history():
    """Return list of past comparison summaries."""
    return load_history()


@app.get("/history/{job_id}")
def get_history_entry(job_id: str):
    """Return the full detailed report for a given job_id."""
    report_path = os.path.join(REPORTS_DIR, f"report_{job_id}.json")
    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Report not found")
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read report: {e}")