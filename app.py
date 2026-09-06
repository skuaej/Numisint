from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import pandas as pd
import requests
import tempfile
import os
import uvicorn

app = FastAPI(title="Hitek Data Gateway")

LANDING_PAGE_HTML = """<!DOCTYPE html>
<html>
<head><title>Hitek Data Gateway</title></head>
<body style="background:#050505;color:#00ffcc;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh;">
  <div style="text-align:center;border:1px solid #00ffcc;padding:30px;border-radius:8px;">
    <h2>SYSTEM ONLINE (DEBUG)</h2>
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

def load_parquet(url: str) -> pd.DataFrame:
    """Download parquet file and return as DataFrame."""
    response = requests.get(url, timeout=90)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name

    try:
        df = pd.read_parquet(tmp_path)
    finally:
        os.unlink(tmp_path)

    return df

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or not (10 <= len(Number) <= 15):
        return JSONResponse(
            status_code=400,
            content={"status": "rejected", "message": "Invalid parameter."},
        )

    last_digit = Number[-1]
    primary_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_{last_digit}.parquet"
    alt_url     = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/alt_master_shard_{last_digit}.parquet"

    debug_info = {}

    # ---------- Main file ----------
    try:
        df_main = load_parquet(primary_url)
        debug_info["main_columns"] = list(df_main.columns)
        debug_info["main_total_rows"] = len(df_main)
        debug_info["main_sample"] = df_main.head(3).fillna("").astype(str).to_dict(orient="records")

        possible_cols = ["mobile", "Mobile", "phone", "Phone", "number", "Number", "msisdn", "msisdn_number"]
        matched = pd.DataFrame()
        for col in possible_cols:
            if col in df_main.columns:
                matched = df_main[df_main[col].astype(str).str.contains(Number, na=False)]
                if not matched.empty:
                    debug_info["matched_column"] = col
                    break

        main_records = matched.fillna("").astype(str).to_dict(orient="records") if not matched.empty else []
    except Exception as e:
        debug_info["main_error"] = str(e)
        main_records = []

    # ---------- Alt file ----------
    try:
        df_alt = load_parquet(alt_url)
        debug_info["alt_columns"] = list(df_alt.columns)
        debug_info["alt_total_rows"] = len(df_alt)
        debug_info["alt_sample"] = df_alt.head(3).fillna("").astype(str).to_dict(orient="records")

        possible_cols = ["alt", "Alt", "alternate", "Alternate", "mobile", "phone", "number"]
        matched = pd.DataFrame()
        for col in possible_cols:
            if col in df_alt.columns:
                matched = df_alt[df_alt[col].astype(str).str.contains(Number, na=False)]
                if not matched.empty:
                    debug_info["alt_matched_column"] = col
                    break

        alt_records = matched.fillna("").astype(str).to_dict(orient="records") if not matched.empty else []
    except Exception as e:
        debug_info["alt_error"] = str(e)
        alt_records = []

    return {
        "status": "debug",
        "phone": Number,
        "main_found": len(main_records),
        "alt_found": len(alt_records),
        "debug": debug_info,
        "Main_Records": main_records[:2],
        "Alt_Records": alt_records[:2]
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
