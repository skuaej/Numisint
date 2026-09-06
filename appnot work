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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR") or (
    os.path.join(BASE_DIR, "data") if os.path.isdir(os.path.join(BASE_DIR, "data")) else BASE_DIR
)

PARALLELISM = int(os.environ.get("PARALLELISM", "2"))
THREADS_PER_CONN = int(os.environ.get("THREADS_PER_CONN", "1"))
DUPLICATE_CAP = int(os.environ.get("DUPLICATE_CAP", "2"))
HF_TOKEN = os.environ.get("HF_TOKEN", "")

# Correct Hugging Face Datasets Base URL
BASE_URL = "https://huggingface.co/datasets/CutehackX/hitek-data-bucket/resolve/main"

PARQUET_FILES = [
    f"{BASE_URL}/part1.parquet",
    f"{BASE_URL}/part2a.parquet",
    f"{BASE_URL}/part2b_new.parquet"
]
IDX_PHONE = f"{BASE_URL}/idx_phone.parquet"
IDX_ID = f"{BASE_URL}/idx_aadhar.parquet"
IDX_NAME = f"{BASE_URL}/idx_name.parquet"

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source"
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]

# ---------------------------------------------------------
# FastAPI App Initialization
# ---------------------------------------------------------
app = FastAPI(
    title="High-Performance Parquet Gateway",
    description="DuckDB-backed search API optimized for cloud containers",
    version="2.3.0"
)

# ---------------------------------------------------------
# Safe Thread-Local Connection Pool
# ---------------------------------------------------------
_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duckdb_worker")


def _get_auth_url(base_path: str) -> str:
    """Appends authentication parameters to Hugging Face URLs."""
    if HF_TOKEN:
        return f"{base_path}?download=true&token={HF_TOKEN}"
    return f"{base_path}?download=true"


def _create_connection() -> duckdb.DuckDBPyConnection:
    """Instantiates a DuckDB connection with memory bounds and remote views."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET max_memory = '256MB';")
    con.execute(f"SET threads = {THREADS_PER_CONN};")
    con.execute("SET enable_http_metadata_cache = true;")
    con.execute("SET http_keep_alive = true;")

    # Register raw files view
    files_str = ", ".join(f"'{_get_auth_url(f)}'" for f in PARQUET_FILES)
    con.execute(f"CREATE OR REPLACE VIEW people AS SELECT * FROM read_parquet([{files_str}])")

    # Register sorted index views
    con.execute(f"CREATE OR REPLACE VIEW people_phone AS SELECT * FROM read_parquet('{_get_auth_url(IDX_PHONE)}')")
    con.execute(f"CREATE OR REPLACE VIEW people_aadhar AS SELECT * FROM read_parquet('{_get_auth_url(IDX_ID)}')")
    con.execute(f"CREATE OR REPLACE VIEW people_name AS SELECT * FROM read_parquet('{_get_auth_url(IDX_NAME)}')")

    return con


def _get_worker_conn() -> duckdb.DuckDBPyConnection:
    """Thread-safe connection retriever that avoids index out of range races."""
    tid = getattr(_thread_local, "id", None)
    
    # Fast path: check if thread already has a valid connection
    if tid is not None and tid < len(_conns):
        return _conns[tid]

    with _conns_lock:
        # Double-check inside the lock
        tid = getattr(_thread_local, "id", None)
        if tid is not None and tid < len(_conns):
            return _conns[tid]

        # Initialize connection before registering thread index
        new_conn = _create_connection()
        new_tid = len(_conns)
        _conns.append(new_conn)
        _thread_local.id = new_tid
        return _conns[new_tid]


# ---------------------------------------------------------
# Deduplication & Processing Helpers
# ---------------------------------------------------------
def _generate_identity_key(row: dict[str, Any]) -> tuple[str, ...]:
    ph = str(row.get("phoneNumber") or "").strip()
    id_val = str(row.get("aadharNumber") or row.get("idNumber") or "").strip()
    if ph or id_val:
        return (ph, id_val)
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
        cleaned.append({
            k: ("" if v is None or str(v).lower() == "nan" else str(v))
            for k, v in r.items()
        })
    return cleaned


# ---------------------------------------------------------
# Query Execution Engine
# ---------------------------------------------------------
def _execute_field_query(
    field: str,
    value: str,
    mode: str = "contains",
    limit: int = 10,
    source: str | None = None
) -> dict[str, Any]:
    if field not in SEARCH_FIELDS:
        raise ValueError(f"Invalid search field: {field}")

    sanitized_val = value.replace("'", "''")
    target_view = "people"

    if mode == "exact":
        if field == "phoneNumber":
            target_view = "people_phone"
        elif field in ("aadharNumber", "idNumber"):
            target_view = "people_aadhar"
            field = "aadharNumber"
        elif field == "name":
            target_view = "people_name"
        
        where_clause = f"WHERE CAST({field} AS VARCHAR) = '{sanitized_val}'"
    elif mode == "contains":
        if field in ("name", "fathersName"):
            target_view = "people_name"
        
        escaped_pattern = sanitized_val.replace("%", r"\%").replace("_", r"\_")
        where_clause = f"WHERE CAST({field} AS VARCHAR) ILIKE '%{escaped_pattern}%' ESCAPE '\\'"
    else:
        raise ValueError(f"Invalid mode: {mode}")

    if source and source in ("icmr", "hitek", "inddata"):
        src = "hitek" if source == "hitek" else source
        where_clause += f" AND source = '{src}'"

    sql = f"SELECT * FROM {target_view} {where_clause} LIMIT {limit * DUPLICATE_CAP + 20}"

    try:
        con = _get_worker_conn()
        cursor = con.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        deduped = _apply_deduplication(raw_rows, DUPLICATE_CAP)[:limit]
        results = _sanitize_rows(deduped)

        return {
            "field": field,
            "value": value,
            "mode": mode,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        return {
            "field": field,
            "value": value,
            "mode": mode,
            "count": 0,
            "results": [],
            "error_message": str(e)
        }


async def _unified_search(q: str, limit: int, source: str | None = None) -> dict[str, Any]:
    cleaned_q = q.strip()
    is_numeric = cleaned_q.isdigit() and len(cleaned_q) >= 8
    loop = asyncio.get_running_loop()

    if is_numeric:
        rows: list[dict[str, Any]] = []
        searched_fields: list[str] = []

        for fld in ["phoneNumber", "aadharNumber", "otherNumber"]:
            if not rows:
                res = await loop.run_in_executor(
                    pool, _execute_field_query, fld, cleaned_q, "exact", limit, source
                )
                if res.get("results"):
                    rows.extend(res["results"])
                    searched_fields.append(fld)
                elif res.get("error_message"):
                    # Bubble up the connection error if the first query fails
                    return {"query": cleaned_q, "error": res["error_message"]}

        final_results = _apply_deduplication(rows, DUPLICATE_CAP)[:limit]
        return {
            "query": cleaned_q,
            "searched_fields": searched_fields or NUMBER_FIELDS,
            "dedup_cap": DUPLICATE_CAP,
            "count": len(final_results),
            "results": final_results
        }
    else:
        tasks = [
            loop.run_in_executor(
                pool, _execute_field_query, "name", cleaned_q, "contains", limit, source
            ),
            loop.run_in_executor(
                pool, _execute_field_query, "fathersName", cleaned_q, "contains", limit, source
            )
        ]
        executed_tasks = await asyncio.gather(*tasks)
        combined_rows: list[dict[str, Any]] = []
        
        for task_res in executed_tasks:
            if task_res.get("error_message"):
                return {"query": cleaned_q, "error": task_res["error_message"]}
            combined_rows.extend(task_res.get("results", []))

        final_results = _apply_deduplication(combined_rows, DUPLICATE_CAP)[:limit]
        return {
            "query": cleaned_q,
            "searched_fields": ["name", "fathersName"],
            "dedup_cap": DUPLICATE_CAP,
            "count": len(final_results),
            "results": final_results
        }


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
    return {
        "status": "online",
        "service": "ICMR + HITEK Remote Parquet Gateway",
        "parallelism": PARALLELISM,
        "max_memory": "256MB",
        "docs": "/docs"
    }


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
            res = await loop.run_in_executor(
                pool, _execute_field_query, field, q, mode, limit, source
            )
            data = {
                "query": q,
                "field": field,
                "mode": mode,
                "dedup_cap": DUPLICATE_CAP,
                "count": res.get("count", 0),
                "results": res.get("results", []),
                "error": res.get("error_message")
            }
        except ValueError as err:
            raise HTTPException(status_code=400, detail=str(err))
    else:
        data = await _unified_search(q, limit, source)

    if pretty:
        return Response(
            content=json.dumps(data, indent=2, ensure_ascii=False),
            media_type="application/json"
        )
    return data


@app.get("/FetchData")
async def fetch_data(Number: str = Query(None)):
    """Legacy endpoint support."""
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return Response(
            content=json.dumps({
                "status": "rejected",
                "message": "Invalid parameter. Use /FetchData?Number=XXXXXXXXXX"
            }),
            media_type="application/json",
            status_code=400
        )
    
    data = await _unified_search(Number, limit=10)
    return data


@app.post("/search/batch")
async def search_batch(req: BatchRequest):
    if not req.queries:
        raise HTTPException(status_code=400, detail="Query list must not be empty.")
    if len(req.queries) > 10:
        raise HTTPException(status_code=400, detail="Exceeded maximum of 10 batch queries on Free Tier.")

    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(
            pool,
            _execute_field_query,
            item.field,
            item.value,
            item.mode,
            item.limit or req.limit,
            None
        )
        for item in req.queries
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    serialized_results = [
        {"error": str(r)} if isinstance(r, Exception) else r
        for r in results
    ]

    return {
        "batch_size": len(req.queries),
        "results": serialized_results
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
