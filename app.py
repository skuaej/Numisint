import os
import random
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb
import uvicorn

app = FastAPI(title='Hitek Data Gateway')

# Aapke tested proxies
PROXIES = [
    "http://13.125.44.24:80",
    "http://3.10.170.234:3128",
    "http://15.235.21.254:8080"
]

LANDING_PAGE_HTML = """<!DOCTYPE html>
<html>
<head><title>Hitek Data Gateway</title></head>
<body style="background:#050505;color:#00ffcc;font-family:monospace;display:flex;justify-content:center;align-items:center;height:100vh;">
  <div style="text-align:center;border:1px solid #00ffcc;padding:30px;border-radius:8px;">
    <h2>SYSTEM ONLINE (PRO MODE)</h2>
    <p>Use: <code>/FetchData?Number=XXXXXXXXXX</code></p>
  </div>
</body>
</html>"""

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(status_code=exc.status_code, content={'status': 'rejected', 'message': 'Invalid endpoint.'})

@app.get('/', response_class=HTMLResponse)
def root():
    return HTMLResponse(content=LANDING_PAGE_HTML, status_code=200)

@app.get('/FetchData')
def fetch_data(Number: str = Query(None)):
    if not Number or not Number.isdigit() or len(Number) < 10 or len(Number) > 15:
        return JSONResponse(status_code=400, content={'status': 'rejected', 'message': 'Invalid parameter.'})
    
    last_digit = Number[-1]
    primary_url = f'https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/final_master_shard_{last_digit}.parquet'
    alt_url = f'https://huggingface.co/buckets/CutehackX/hitek-data-bucket/resolve/alt_master_shard_{last_digit}.parquet'
    
    main_records = []
    alt_records = []
    success = False
    last_error = ""
    
    proxies_to_try = PROXIES.copy()
    random.shuffle(proxies_to_try)

    for proxy in proxies_to_try:
        try:
            # [FIX]: DuckDB cURL use karta hai, isliye hum Proxy directly OS environment me set kar rahe hain
            os.environ['HTTP_PROXY'] = proxy
            os.environ['HTTPS_PROXY'] = proxy
            
            con = duckdb.connect()
            con.execute('INSTALL httpfs;')
            con.execute('LOAD httpfs;')
            con.execute('SET enable_http_metadata_cache=true;')
            
            # Ab hum secret me sirf User-Agent bhejenge (jo perfectly kaam kar raha tha)
            secret_query = """
            CREATE OR REPLACE SECRET hf_headers (
                TYPE HTTP,
                EXTRA_HTTP_HEADERS MAP {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
            );
            """
            con.execute(secret_query)
            
            try:
                df_main = con.execute(f"SELECT * FROM read_parquet('{primary_url}') WHERE mobile = '{Number}' LIMIT 1").df()
                if not df_main.empty:
                    main_records = df_main.fillna('').astype(str).to_dict(orient='records')
            except Exception as e:
                raise Exception(f"Main DB Error via {proxy}: {e}")
            
            try:
                df_alt = con.execute(f"SELECT * FROM read_parquet('{alt_url}') WHERE alt = '{Number}' LIMIT 1").df()
                if not df_alt.empty:
                    alt_records = df_alt.fillna('').astype(str).to_dict(orient='records')
            except Exception as e:
                raise Exception(f"Alt DB Error via {proxy}: {e}")

            success = True
            con.close()
            break 

        except Exception as e:
            last_error = str(e)
            print(f"Proxy Failed: {last_error}")
            try:
                con.close()
            except:
                pass
            continue 

    if not success:
        return JSONResponse(status_code=502, content={'status': 'error', 'message': 'All proxies failed or blocked.', 'details': last_error})

    if not main_records and not alt_records:
        return JSONResponse(status_code=404, content={'status': 'not_found', 'phone': Number})
        
    return {'status': 'success', 'Data': {'Main_Records': main_records, 'Alt_Records': alt_records}}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    uvicorn.run(app, host='0.0.0.0', port=port)

