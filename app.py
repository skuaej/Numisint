import os
import duckdb
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from huggingface_hub import HfFileSystem

# 1. Initialize FastAPI
app = FastAPI(
    title="Hitek Data Gateway",
    description="High-Speed Partitioned Parquet Lookup API",
    version="1.0.0"
)

# 2. Authenticate Hugging Face FileSystem
HF_TOKEN = os.getenv("HF_TOKEN", "")
fs = HfFileSystem(token=HF_TOKEN if HF_TOKEN else None)

# 3. Initialize In-Memory DuckDB
con = duckdb.connect()

LANDING_PAGE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hitek Data Gateway</title>
    <style>
        body {
            margin: 0;
            background-color: #080808;
            color: #00ffcc;
            font-family: 'Courier New', Courier, monospace;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
        }
        .container {
            border: 1px solid #00ffcc;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 0 15px rgba(0, 255, 204, 0.2);
            text-align: center;
            max-width: 480px;
            width: 90%;
        }
        h1 { margin-top: 0; font-size: 22px; }
        p { color: #aaaaaa; font-size: 14px; }
        code { background: #161616; padding: 4px 8px; border-radius: 4px; color: #ff007f; }
    </style>
</head>
<body>
    <div class="container">
        <h1>SYSTEM ONLINE</h1>
        <p>DuckDB Parquet Gateway is active.</p>
        <p><strong>Query Format:</strong><br><code>/FetchData?Number=XXXXXXXXXX</code></p>
    </div>
</body>
</html>
"""

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return JSONResponse(
            status_code=404,
            content={
                "status": "rejected",
                "message": "Invalid endpoint. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@Maybechx"
            }
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "Developer": "@Maybechx"}
    )

@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

@app.get("/FetchData")
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return JSONResponse(
            status_code=400,
            content={
                "status": "rejected",
                "message": "Invalid parameter. STRICTLY use /FetchData?Number=XXXXXXXXXX",
                "Developer": "@Maybechx"
            }
        )
    
    last_digit = Number[-1]
    
    # Bucket paths on Hugging Face
    primary_path = f"CutehackX/hitek-data-bucket/final_master_shard_{last_digit}.parquet"
    alt_path = f"CutehackX/hitek-data-bucket/alt_master_shard_{last_digit}.parquet"
    
    main_records = []
    alt_records = []
    errors = []
    
    # Query Main Shard via HfFileSystem
    try:
        with fs.open(primary_path, "rb") as f:
            df_main = con.execute(
                f"SELECT * FROM read_parquet(f) WHERE CAST(mobile AS VARCHAR) = '{Number}'"
            ).df()
            main_records = df_main.to_dict(orient="records")
    except Exception as e:
        print(f"[ERROR] Main Shard Query Failed for {Number}: {e}")
        errors.append(f"Main shard: {str(e)}")

    # Query Alt Shard via HfFileSystem
    try:
        with fs.open(alt_path, "rb") as f:
            df_alt = con.execute(
                f"SELECT * FROM read_parquet(f) WHERE CAST(alt AS VARCHAR) = '{Number}'"
            ).df()
            alt_records = df_alt.to_dict(orient="records")
    except Exception as e:
        print(f"[ERROR] Alt Shard Query Failed for {Number}: {e}")
        errors.append(f"Alt shard: {str(e)}")

    # Sanitize NaN/None values for JSON serialization
    def sanitize(records):
        cleaned = []
        for row in records:
            cleaned.append({k: ("" if v is None or str(v).lower() == "nan" else str(v)) for k, v in row.items()})
        return cleaned

    main_records = sanitize(main_records)
    alt_records = sanitize(alt_records)

    if not main_records and not alt_records:
        return JSONResponse(
            status_code=404,
            content={
                "status": "not_found",
                "phone": Number,
                "errors": errors if errors else None,
                "Developer": "@Maybechx"
            }
        )
        
    return {
        "status": "success",
        "Data": {
            "Main_Records": main_records,
            "Alt_Records": alt_records
        },
        "Developer": "@Maybechx"
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
