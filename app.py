import os
import duckdb
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

app = FastAPI(
    title="Hitek Data Gateway",
    description="High-Speed Partitioned Parquet Lookup API",
    version="1.0.0"
)

# Initialize DuckDB and load HTTP Parquet support
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

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
    primary_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_{last_digit}.parquet?download=true"
    alt_url = f"https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/alt_master_shard_{last_digit}.parquet?download=true"
    
    main_records = []
    alt_records = []
    
    try:
        query_main = f"SELECT * FROM read_parquet('{primary_url}') WHERE mobile = '{Number}'"
        main_records = con.execute(query_main).df().to_dict(orient="records")
    except Exception:
        pass
    
    try:
        query_alt = f"SELECT * FROM read_parquet('{alt_url}') WHERE alt = '{Number}'"
        alt_records = con.execute(query_alt).df().to_dict(orient="records")
    except Exception:
        pass

    if not main_records and not alt_records:
        return JSONResponse(
            status_code=404,
            content={
                "status": "not_found",
                "phone": Number,
                "Developer": "@none"
            }
        )
        
    return {
        "status": "success",
        "Data": {
            "Main_Records": main_records,
            "Alt_Records": alt_records
        },
        "Developer": "@none"
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
