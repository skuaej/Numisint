from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb
import os
import uvicorn

app = FastAPI(title="Hitek Data Gateway")

# ---------- DuckDB + Hugging Face S3 ----------
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("SET enable_http_metadata_cache=true;")

HF_S3_KEY    = os.getenv("HF_S3_KEY")
HF_S3_SECRET = os.getenv("HF_S3_SECRET")

if HF_S3_KEY and HF_S3_SECRET:
    con.execute(f"""
        CREATE OR REPLACE SECRET hf_s3 (
            TYPE s3,
            KEY_ID '{HF_S3_KEY}',
            SECRET '{HF_S3_SECRET}',
            ENDPOINT 's3.hf.co/usaomega1',
            URL_STYLE 'path',
            REGION 'us-east-1'
        );
    """)
    print("Hugging Face S3 secret created successfully")
else:
    print("WARNING: HF_S3_KEY or HF_S3_SECRET missing!")
# ----------------------------------------------

LANDING_PAGE_HTML = """<!DOCTYPE html>
<html>
<head><title>Hitek Data Gateway</title></head>
<body style="background:#050505;color:#00ffcc;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh;">
  <div style="text-align:center;border:1px solid #00ffcc;padding:30px;border-radius:8px;">
    <h2>SYSTEM ONLINE (KOYEB S3)</h2>
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

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or not (10 <= len(Number) <= 15):
        return JSONResponse(
            status_code=400,
            content={"status": "rejected", "message": "Invalid parameter."},
        )

    last_digit = Number[-1]

    # S3 paths for the bucket
    primary_url = f"s3://hitek-data-bucket/final_master_shard_{last_digit}.parquet"
    alt_url     = f"s3://hitek-data-bucket/alt_master_shard_{last_digit}.parquet"

    main_records = []
    alt_records  = []

    try:
        df_main = con.execute(
            f"SELECT * FROM read_parquet('{primary_url}') WHERE mobile = '{Number}' LIMIT 1"
        ).df()
        if not df_main.empty:
            main_records = df_main.fillna("").astype(str).to_dict(orient="records")
    except Exception as e:
        print(f"Main DB Error: {e}")

    try:
        df_alt = con.execute(
            f"SELECT * FROM read_parquet('{alt_url}') WHERE alt = '{Number}' LIMIT 1"
        ).df()
        if not df_alt.empty:
            alt_records = df_alt.fillna("").astype(str).to_dict(orient="records")
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
