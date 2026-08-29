import asyncio
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

# ---------------------------------------------------------
# Configuration & Constants (Koyeb Free Tier Optimized)
# ---------------------------------------------------------
# Concurrency limits kept low to prevent Koyeb 512MB RAM OOM crashes
PARALLELISM = int(os.environ.get("PARALLELISM", "2"))
THREADS_PER_CONN = int(os.environ.get("THREADS_PER_CONN", "1"))
DUPLICATE_CAP = int(os.environ.get("DUPLICATE_CAP", "2"))
HF_TOKEN = os.environ.get("HF_TOKEN", "")

# Hugging Face Bucket Base URL
BASE_URL = "https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve"

# Remote Parquet Files
PARQUET_FILES = [f"{BASE_URL}/part1.parquet", f"{BASE_URL}/part2a.parquet", f"{BASE_URL}/part2b_new.parquet"]
IDX_PHONE = f"{BASE_URL}/idx_phone.parquet"
IDX_AADHAR = f"{BASE_URL}/idx_aadhar.parquet"
IDX_NAME = f"{BASE_URL}/idx_name.parquet"

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source"
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]

# ---------------------------------------------------------
# App Initialization & Thread-Local Connection Pool
# ---------------------------------------------------------
app = FastAPI(
    title="ICMR + HITEK Remote Search API",
    description="DuckDB HTTPFS search over 2.5B records (Koyeb Free Tier Optimized)",
    version="2.1.0"
)

_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duckdb_worker")

def _get_auth_url(base_path: str) -> str:
    """Appends HF Token for authentication to bypass 403 Forbidden errors."""
    if HF_TOKEN:
        return f"{base_path}?download=true&token={HF_TOKEN}"
    return f"{base_path}?download=true"

def _create_connection() -> duckdb.DuckDBPyConnection:
    """Creates a configured DuckDB connection with strict memory limits."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # CRITICAL: Strict limits for Koyeb Free Tier (512MB total system RAM)
    con.execute(f"SET max_memory = '256MB';")
    con.execute(f"SET threads = {THREADS_PER_CONN};")
    con.execute("SET enable_http_metadata_cache = true;")
    con.execute("SET http_keep_alive = true;")

    # Register raw files view
    files_str = ", ".join(f"'{_get_auth_url(f)}'" for f in PARQUET_FILES)
    con.execute(f"CREATE OR REPLACE VIEW people AS SELECT * FROM read_parquet([{files_str}])")

    # Register sorted index views for zone-map pruning
    con.execute(f"CREATE OR REPLACE VIEW people_phone AS SELECT * FROM read_parquet('{_get_auth_url(IDX_PHONE)}')")
    con.execute(f"CREATE OR REPLACE VIEW people_aadhar AS SELECT * FROM read_parquet('{_get_auth_url(IDX_AADHAR)}')")
    con.execute(f"CREATE OR REPLACE VIEW people_name AS SELECT * FROM read_parquet('{_get_auth_url(IDX_NAME)}')")

    return con

def _get_worker_conn() -> duckdb.DuckDBPyConnection:
    """Retrieves or creates a thread-local DuckDB connection."""
    tid = getattr(_thread_local, "id", None)
    if tid is None:
        with _conns_lock:
            tid = len(_conns)
            _thread_local.id = tid
            _conns.append(_create_connection())
    return _conns[tid]

# ---------------------------------------------------------
# Deduplication & Processing Helpers
# ---------------------------------------------------------
def _generate_identity_key(row: dict[str, Any]) -> tuple[str, ...]:
    ph = str(row.get("phoneNumber") or "").strip()
    ad = str(row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (str(row.get("name") or "").strip(), str(row.get("fathersName") or "").strip())

def _apply_deduplication(rows: list[dict[str, Any]], cap: int = DUPLICATE_CAP) -> list[dict[str, Any]]:
    seen_counts: dict[tuple[str, ...], int] = {}
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        key = _generate_identity_key(row)
        count = seen_counts.get(key, 0)
        if count < cap:
            seen_counts[key] = count + 1
            unique_rows.append(row)
    return unique_rows

def _sanitize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned = []
    for r in rows:
        cleaned.append({k: ("" if v is None or str(v).lower() == "nan" else str(v)) for k, v in r.items()})
    return cleaned

# ---------------------------------------------------------
# Query Execution Engine
# ---------------------------------------------------------
def _execute_field_query(field: str, value: str, mode: str = "contains", limit: int = 10, source: str | None = None) -> dict[str, Any]:
    if field not in SEARCH_FIELDS:
        raise ValueError(f"Invalid search field: {field}")

    sanitized_val = value.replace("'", "''")
    target_view = "people"

    if mode == "exact":
        if field == "phoneNumber": target_view = "people_phone"
        elif field == "aadharNumber": target_view = "people_aadhar"
        elif field == "name": target_view = "people_name"
        where_clause = f"WHERE CAST({field} AS VARCHAR) = '{sanitized_val}'"
    elif mode == "contains":
        if field in ("name", "fathersName"): target_view = "people_name"
        escaped_pattern = sanitized_val.replace("%", r"\%").replace("_", r"\_")
        where_clause = f"WHERE {field} ILIKE '%{escaped_pattern}%' ESCAPE '\\'"
    else:
        raise ValueError(f"Invalid mode: {mode}")

    if source and source in ("icmr", "hitek", "inddata"):
        src = "hitek" if source == "hitek" else source
        where_clause += f" AND source = '{src}'"

    sql = f"SELECT * FROM {target_view} {where_clause} LIMIT {limit * DUPLICATE_CAP + 20}"

    con = _get_worker_conn()
    cursor = con.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    raw_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    deduped = _apply_deduplication(raw_rows, DUPLICATE_CAP)[:limit]
    results = _sanitize_rows(deduped)

    return {"field": field, "value": value, "mode": mode, "count": len(results), "results": results}

async def _unified_search(q: str, limit: int, source: str | None = None) -> dict[str, Any]:
    cleaned_q = q.strip()
    is_numeric = cleaned_q.isdigit() and len(cleaned_q) >= 8
    loop = asyncio.get_running_loop()

    if is_numeric:
        rows: list[dict[str, Any]] = []
        searched_fields: list[str] = []

        for fld in ["phoneNumber", "aadharNumber", "otherNumber"]:
            if not rows:
                res = await loop.run_in_executor(pool, _execute_field_query, fld, cleaned_q, "exact", limit, source)
                rows.extend(res["results"])
                searched_fields.append(fld)

        final_results = _apply_deduplication(rows, DUPLICATE_CAP)[:limit]
        return {"query": cleaned_q, "searched_fields": searched_fields or NUMBER_FIELDS, "dedup_cap": DUPLICATE_CAP, "count": len(final_results), "results": final_results}
    else:
        tasks = [
            loop.run_in_executor(pool, _execute_field_query, "name", cleaned_q, "contains", limit, source),
            loop.run_in_executor(pool, _execute_field_query, "fathersName", cleaned_q, "contains", limit, source)
        ]
        executed_tasks = await asyncio.gather(*tasks)
        combined_rows: list[dict[str, Any]] = []
        for task_res in executed_tasks:
            combined_rows.extend(task_res["results"])

        final_results = _apply_deduplication(combined_rows, DUPLICATE_CAP)[:limit]
        return {"query": cleaned_q, "searched_fields": ["name", "fathersName"], "dedup_cap": DUPLICATE_CAP, "count": len(final_results), "results": final_results}

# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
class BatchQueryItem(BaseModel):
    field: str = "name"
    value: str
    mode: str = "contains"
    limit: int = 10

class BatchRequest(BaseModel):
    queries: list[BatchQueryItem]
    limit: int = 10

@app.get("/")
def root():
    return {"status": "online", "service": "ICMR + HITEK Remote Parquet API", "parallelism": PARALLELISM, "memory_limit": "256MB"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/search")
async def search(
    q: str = Query(..., description="Query term (Name, Phone, ID, Address)"),
    field: str | None = Query(None, description=f"Optional field target: {SEARCH_FIELDS}"),
    mode: str = Query("contains", pattern="^(exact|contains)$"),
    limit: int = Query(10, ge=1, le=100),
    source: str | None = Query(None, description="Optional source filter tag"),
    pretty: bool = Query(False, description="Format output JSON with indentation")
):
    if field:
        try:
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(pool, _execute_field_query, field, q, mode, limit, source)
            data = {"query": q, "field": field, "mode": mode, "dedup_cap": DUPLICATE_CAP, "count": res["count"], "results": res["results"]}
        except ValueError as err:
            raise HTTPException(status_code=400, detail=str(err))
    else:
        data = await _unified_search(q, limit, source)

    if pretty:
        return Response(content=json.dumps(data, indent=2, ensure_ascii=False), media_type="application/json")
    return data

@app.post("/search/batch")
async def search_batch(req: BatchRequest):
    if not req.queries:
        raise HTTPException(status_code=400, detail="Query list must not be empty.")
    if len(req.queries) > 10:
        raise HTTPException(status_code=400, detail="Exceeded maximum of 10 batch queries on Free Tier.")

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(pool, _execute_field_query, item.field, item.value, item.mode, item.limit or req.limit, None) for item in req.queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    serialized_results = [{"error": str(r)} if isinstance(r, Exception) else r for r in results]

    return {"batch_size": len(req.queries), "results": serialized_results}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)

