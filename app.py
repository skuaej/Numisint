from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from huggingface_hub import hf_hub_download
import pandas as pd
import os
import tempfile
import uvicorn

app = FastAPI(title="Hitek Data Gateway")

LANDING_PAGE_HTML = """<!DOCTYPE html>
<html>
<head><title>Hitek Data Gateway</title></head>
<body style="background:#050505;color:#00ffcc;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh;">
  <div style="text-align:center;border:1px solid #00ffcc;padding:30px;border-radius:8px;">
    <h2>SYSTEM ONLINE (HF HUB)</h2>
    <p>Use: <code>/FetchData?Number=XXXXXXXXXX</code></p>
  </div>
</body>
</html>"""

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "rejected", "message": "Invalid endpoint."},
    )

@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

def load_parquet_from_hf(filename: str) -> pd.DataFrame:
    """Download a single parquet file from the public bucket and return as DataFrame."""
    local_path = hf_hub_download(
        repo_id="CutehackX/hitek-data-bucket",
        filename=filename,
        repo_type="bucket",          # important for Storage Buckets
        token=None,                  # public – no token needed
    )
    return pd.read_parquet(local_path)

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or not (10 <= len(Number) <= 15):
        return JSONResponse(
            status_code=400,
            content={"status": "rejected", "message": "Invalid parameter."},
        )

    last_digit = Number[-1]
    main_file = f"final_master_shard_{last_digit}.parquet"
    alt_file  = f"alt_master_shard_{last_digit}.parquet"

    main_records = []
    alt_records  = []

    # Main records
    try:
        df_main = load_parquet_from_hf(main_file)
        matched = df_main[df_main["mobile"].astype(str) == Number]
        if not matched.empty:
            main_records = matched.fillna("").astype(str).to_dict(orient="records")
    except Exception as e:
        print(f"Main DB Error: {e}")

    # Alt records
    try:
        df_alt = load_parquet_from_hf(alt_file)
        matched = df_alt[df_alt["alt"].astype(str) == Number]
        if not matched.empty:
            alt_records = matched.fillna("").astype(str).to_dict(orient="records")
    except Exception as e:
        print(f"Alt DB Error: {e}")

    if not main_records and not alt_records:
        return JSONResponse(
            status_code=404,
            content={"status": "not_found", "phone": Number},
        )

    return {
        "status": "success",
        "Data": {
            "Main_Records": main_records,
            "Alt_Records": alt_records,
        },
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
