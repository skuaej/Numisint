import asyncio
import glob
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
# Configuration & Constants
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR") or (
    os.path.join(BASE_DIR, "data") if os.path.isdir(os.path.join(BASE_DIR, "data")) else BASE_DIR
)

# Raw data and partitioned index file paths
PARQUET_FILES = [
    os.path.join(DATA_DIR, f)
    for f in ["part1.parquet", "part2a.parquet", "part2b.parquet"]
    if os.path.exists(os.path.join(DATA_DIR, f))
]

IDX_PHONE = os.path.join(DATA_DIR, "idx_phone.parquet")
IDX_ID = os.path.join(DATA_DIR, "idx_id.parquet")
IDX_NAME = os.path.join(DATA_DIR, "idx_name.parquet")

PARALLELISM = int(os.environ.get("PARALLELISM", "15"))
THREADS_PER_CONN = int(os.environ.get("THREADS_PER_CONN", "4"))
DUPLICATE_CAP = int(os.environ.get("DUPLICATE_CAP", "2"))

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "idNumber", "otherNumber",
    "address", "district", "pincode", "state", "town", "source"
]
NUMBER_FIELDS = ["phoneNumber", "idNumber", "otherNumber"]

# ---------------------------------------------------------
# App Initialization & Thread-Local Connection Pool
# ---------------------------------------------------------
app = FastAPI(
    title="High-Performance Parquet Gateway",
    description="DuckDB-backed zero-copy search over partitioned Parquet datasets",
    version="2.0.0"
)

_conns: list[duckdb.DuckDBPyConnection] = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duckdb_worker")


def _get_index_files(path: str) -> list[str]:
    """Resolves index files whether stored as a single file or partitioned parts."""
    base, ext = os.path.splitext(path)
    parts = sorted(glob.glob(f"{base}.*{ext}"))
    if parts and all(os.path.exists(p) and os.path.getsize(p) > 0 for p in parts):
        return parts
    return [path] if os.path.exists(path) and os.path.getsize(path) > 0 else []


def _is_index_ready(path: str) -> bool:
    """Checks if index exists and is not currently mid-write."""
    files = _get_index_files(path)
    if not files:
        return False
    
    log_path = os.path.join(BASE_DIR, "build_index.log")
    if os.path.exists(log_path):
        try:
            with open(log_path, encoding="utf-8", errors="ignore") as f:
                log = f.read()
            name = os.path.basename(path)
            if f"START {name}" in log and f"DONE {name}" not in log:
                return False
        except OSError:
            pass
    return True


def _create_connection() -> duckdb.DuckDBPyConnection:
    """Creates a configured DuckDB connection with pre-registered views."""
    con = duckdb.connect()
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute(f"SET threads = {THREADS_PER_CONN};")
    con.execute("SET enable_http_metadata_cache = true;")

    # Register raw files view if present
    if PARQUET_FILES:
        files_str = ", ".join(f"'{f}'" for f in PARQUET_FILES)
        con.execute(f"CREATE OR REPLACE VIEW records AS SELECT * FROM read_parquet([{files_str}])")

    # Register sorted index views for zone-map pruning
    index_views = [
        (IDX_PHONE, "idx_phone_view"),
        (IDX_ID, "idx_id_view"),
        (IDX_NAME, "idx_name_view"),
    ]
    for idx_path, view_name in index_views:
        files = _get_index_files(idx_path)
        if files and _is_index_ready(idx_path):
            files_str = ", ".join(f"'{f}'" for f in files)
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet([{files_str}])")

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
    """Generates composite deduplication key."""
    ph = str(row.get("phoneNumber") or "").strip()
    id_val = str(row.get("idNumber") or "").strip()
    if ph or id_val:
        return (ph, id_val)
    return (str(row.get("name") or "").strip(), str(row.get("fathersName") or "").strip())


def _apply_deduplication(rows: list[dict[str, Any]], cap: int = DUPLICATE_CAP) -> list[dict[str, Any]]:
    """Limits the number of duplicate identity entries returned."""
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
    """Converts NaNs and None objects into clean JSON-serializable strings."""
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
    target_view = "records"

    # Select optimized index view when available
    if mode == "exact":
        if field == "phoneNumber" and _is_index_ready(IDX_PHONE):
            target_view = "idx_phone_view"
        elif field == "idNumber" and _is_index_ready(IDX_ID):
            target_view = "idx_id_view"
        elif field == "name" and _is_index_ready(IDX_NAME):
            target_view = "idx_name_view"
        
        where_clause = f"WHERE CAST({field} AS VARCHAR) = '{sanitized_val}'"
    elif mode == "contains":
        if field in ("name", "fathersName") and _is_index_ready(IDX_NAME):
            target_view = "idx_name_view"
        
        escaped_pattern = sanitized_val.replace("%", r"\%").replace("_", r"\_")
        where_clause = f"WHERE {field} ILIKE '%{escaped_pattern}%' ESCAPE '\\'"
    else:
        raise ValueError(f"Invalid mode: {mode}")

    if source:
        src = source.replace("'", "''")
        where_clause += f" AND source = '{src}'"

    sql = f"SELECT * FROM {target_view} {where_clause} LIMIT {limit * DUPLICATE_CAP + 20}"

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


async def _unified_search(q: str, limit: int, source: str | None = None) -> dict[str, Any]:
    """Dispatches search based on query characteristics."""
    cleaned_q = q.strip()
    is_numeric = cleaned_q.isdigit() and len(cleaned_q) >= 8
    loop = asyncio.get_running_loop()

    if is_numeric:
        # Numeric routing priority: phone -> ID -> secondary number
        rows: list[dict[str, Any]] = []
        searched_fields: list[str] = []

        if _is_index_ready(IDX_PHONE):
            res = await loop.run_in_executor(
                pool, _execute_field_query, "phoneNumber", cleaned_q, "exact", limit, source
            )
            rows.extend(res["results"])
            searched_fields.append("phoneNumber")

        if not rows and _is_index_ready(IDX_ID):
            res = await loop.run_in_executor(
                pool, _execute_field_query, "idNumber", cleaned_q, "exact", limit, source
            )
            rows.extend(res["results"])
            searched_fields.append("idNumber")

        if not rows and _is_index_ready(IDX_NAME):
            res = await loop.run_in_executor(
                pool, _execute_field_query, "otherNumber", cleaned_q, "exact", limit, source
            )
            rows.extend(res["results"])
            searched_fields.append("otherNumber")

        final_results = _apply_deduplication(rows, DUPLICATE_CAP)[:limit]
        return {
            "query": cleaned_q,
            "searched_fields": searched_fields or NUMBER_FIELDS,
            "dedup_cap": DUPLICATE_CAP,
            "count": len(final_results),
            "results": final_results
        }
    else:
        # Textual routing: Name prefix / pattern matching
        tasks = [
            loop.run_in_executor(
                pool, _execute_field_query, "name", cleaned_q, "contains", limit, source
            )
        ]
        if _is_index_ready(IDX_NAME):
            tasks.append(
                loop.run_in_executor(
                    pool, _execute_field_query, "fathersName", cleaned_q, "contains", limit, source
                )
            )

        executed_tasks = await asyncio.gather(*tasks)
        combined_rows: list[dict[str, Any]] = []
        for task_res in executed_tasks:
            combined_rows.extend(task_res["results"])

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
        "service": "High-Performance Parquet Gateway",
        "parallelism": PARALLELISM,
        "threads_per_conn": THREADS_PER_CONN,
        "indexes": {
            "phone": _is_index_ready(IDX_PHONE),
            "id": _is_index_ready(IDX_ID),
            "name": _is_index_ready(IDX_NAME)
        }
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "raw_files_available": len(PARQUET_FILES),
        "indexes_ready": {
            "phone": _is_index_ready(IDX_PHONE),
            "id": _is_index_ready(IDX_ID),
            "name": _is_index_ready(IDX_NAME)
        }
    }


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
                "count": res["count"],
                "results": res["results"]
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


@app.post("/search/batch")
async def search_batch(req: BatchRequest):
    if not req.queries:
        raise HTTPException(status_code=400, detail="Query list must not be empty.")
    if len(req.queries) > 50:
        raise HTTPException(status_code=400, detail="Exceeded maximum of 50 batch queries.")

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
