import os
import random
import requests
import urllib3
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb
import uvicorn

# SSL warnings ko chupa dega
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title='Hitek Data Gateway')

con = duckdb.connect()
con.execute('INSTALL httpfs;')
con.execute('LOAD httpfs;')
con.execute('SET enable_http_metadata_cache=true;')

def get_fresh_proxies():
    """Internet se automatically fresh aur working proxies download karega"""
    print("Fetching fresh proxies from API...")
    try:
        r = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=anonymous", timeout=5)
        if r.status_code == 200:
            raw_proxies = r.text.strip().split('\r\n')
            valid_proxies = [f"http://{p}" for p in raw_proxies if p]
            if valid_proxies:
                # 10 random fresh proxies select karega
                return random.sample(valid_proxies, min(10, len(valid_proxies)))
    except Exception as e:
        print(f"Failed to fetch proxies dynamically: {e}")
    
    # Agar API fail ho jaye toh yeh backup proxies use honge
    return [
        "http://13.125.44.24:80",
        "http://3.10.170.234:3128"
    ]

def get_direct_aws_url(hf_url, proxy=None):
    """Final AWS S3/CDN URL nikalega"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*'
    }
    proxies = {'http': proxy, 'https': proxy} if proxy else None
    
    try:
        # allow_redirects=True seedha final AWS URL par le jayega
        resp = requests.get(
            hf_url, 
            headers=headers, 
            proxies=proxies, 
            allow_redirects=True, 
            timeout=8, 
            verify=False,
            stream=True
        )
        final_url = str(resp.url)
        resp.close()
        
        # Check karega ki URL actual mein change ho kar AWS/CDN ka bana ya nahi
        if final_url != hf_url and ("aws" in final_url or "cdn" in final_url or "xet" in final_url):
            return final_url
        else:
            print(f"[{proxy or 'DIRECT'}] Failed. URL didn't change: {final_url}")
            
    except Exception as e:
        print(f"[{proxy or 'DIRECT'}] Error: {type(e).__name__}")
    return None

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
    
    primary_aws_url, alt_aws_url = None, None
    
    # 1. Sabse pehle BINA PROXY try karega (Fastest method)
    print("Trying DIRECT connection...")
    primary_aws_url = get_direct_aws_url(primary_url, proxy=None)
    alt_aws_url = get_direct_aws_url(alt_url, proxy=None)
    
    # 2. Agar direct fail ho jaye (Railway IP Blocked ho), tab fresh Proxies mangwayega
    if not primary_aws_url or not alt_aws_url:
        print("Direct connection failed! Trying with Fresh Dynamic Proxies...")
        fresh_proxies = get_fresh_proxies()
        
        for proxy in fresh_proxies:
            print(f"Testing Proxy: {proxy}")
            if not primary_aws_url:
                primary_aws_url = get_direct_aws_url(primary_url, proxy)
            if not alt_aws_url:
                alt_aws_url = get_direct_aws_url(alt_url, proxy)
                
            if primary_aws_url and alt_aws_url:
                print("Success! Got AWS Links via proxy.")
                break

    if not primary_aws_url or not alt_aws_url:
        return JSONResponse(status_code=502, content={'status': 'error', 'message': 'Unable to resolve Hugging Face CDN. All proxies failed.'})
    
    safe_primary_aws_url = primary_aws_url.replace("'", "''")
    safe_alt_aws_url = alt_aws_url.replace("'", "''")
    
    main_records = []
    alt_records = []
    
    try:
        df_main = con.execute(f"SELECT * FROM read_parquet('{safe_primary_aws_url}') WHERE mobile = '{Number}' LIMIT 1").df()
        if not df_main.empty:
            main_records = df_main.fillna('').astype(str).to_dict(orient='records')
    except Exception as e:
        print(f'Main DB Error: {e}')
    
    try:
        df_alt = con.execute(f"SELECT * FROM read_parquet('{safe_alt_aws_url}') WHERE alt = '{Number}' LIMIT 1").df()
        if not df_alt.empty:
            alt_records = df_alt.fillna('').astype(str).to_dict(orient='records')
    except Exception as e:
        print(f'Alt DB Error: {e}')

    if not main_records and not alt_records:
        return JSONResponse(status_code=404, content={'status': 'not_found', 'phone': Number})
        
    return {'status': 'success', 'Data': {'Main_Records': main_records, 'Alt_Records': alt_records}}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    uvicorn.run(app, host='0.0.0.0', port=port)
import os
import random
import requests
import urllib3
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import duckdb
import uvicorn

# SSL warnings ko chupa dega
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title='Hitek Data Gateway')

con = duckdb.connect()
con.execute('INSTALL httpfs;')
con.execute('LOAD httpfs;')
con.execute('SET enable_http_metadata_cache=true;')

def get_fresh_proxies():
    """Internet se automatically fresh aur working proxies download karega"""
    print("Fetching fresh proxies from API...")
    try:
        r = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=anonymous", timeout=5)
        if r.status_code == 200:
            raw_proxies = r.text.strip().split('\r\n')
            valid_proxies = [f"http://{p}" for p in raw_proxies if p]
            if valid_proxies:
                # 10 random fresh proxies select karega
                return random.sample(valid_proxies, min(10, len(valid_proxies)))
    except Exception as e:
        print(f"Failed to fetch proxies dynamically: {e}")
    
    # Agar API fail ho jaye toh yeh backup proxies use honge
    return [
        "http://13.125.44.24:80",
        "http://3.10.170.234:3128"
    ]

def get_direct_aws_url(hf_url, proxy=None):
    """Final AWS S3/CDN URL nikalega"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*'
    }
    proxies = {'http': proxy, 'https': proxy} if proxy else None
    
    try:
        # allow_redirects=True seedha final AWS URL par le jayega
        resp = requests.get(
            hf_url, 
            headers=headers, 
            proxies=proxies, 
            allow_redirects=True, 
            timeout=8, 
            verify=False,
            stream=True
        )
        final_url = str(resp.url)
        resp.close()
        
        # Check karega ki URL actual mein change ho kar AWS/CDN ka bana ya nahi
        if final_url != hf_url and ("aws" in final_url or "cdn" in final_url or "xet" in final_url):
            return final_url
        else:
            print(f"[{proxy or 'DIRECT'}] Failed. URL didn't change: {final_url}")
            
    except Exception as e:
        print(f"[{proxy or 'DIRECT'}] Error: {type(e).__name__}")
    return None

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
    
    primary_aws_url, alt_aws_url = None, None
    
    # 1. Sabse pehle BINA PROXY try karega (Fastest method)
    print("Trying DIRECT connection...")
    primary_aws_url = get_direct_aws_url(primary_url, proxy=None)
    alt_aws_url = get_direct_aws_url(alt_url, proxy=None)
    
    # 2. Agar direct fail ho jaye (Railway IP Blocked ho), tab fresh Proxies mangwayega
    if not primary_aws_url or not alt_aws_url:
        print("Direct connection failed! Trying with Fresh Dynamic Proxies...")
        fresh_proxies = get_fresh_proxies()
        
        for proxy in fresh_proxies:
            print(f"Testing Proxy: {proxy}")
            if not primary_aws_url:
                primary_aws_url = get_direct_aws_url(primary_url, proxy)
            if not alt_aws_url:
                alt_aws_url = get_direct_aws_url(alt_url, proxy)
                
            if primary_aws_url and alt_aws_url:
                print("Success! Got AWS Links via proxy.")
                break

    if not primary_aws_url or not alt_aws_url:
        return JSONResponse(status_code=502, content={'status': 'error', 'message': 'Unable to resolve Hugging Face CDN. All proxies failed.'})
    
    safe_primary_aws_url = primary_aws_url.replace("'", "''")
    safe_alt_aws_url = alt_aws_url.replace("'", "''")
    
    main_records = []
    alt_records = []
    
    try:
        df_main = con.execute(f"SELECT * FROM read_parquet('{safe_primary_aws_url}') WHERE mobile = '{Number}' LIMIT 1").df()
        if not df_main.empty:
            main_records = df_main.fillna('').astype(str).to_dict(orient='records')
    except Exception as e:
        print(f'Main DB Error: {e}')
    
    try:
        df_alt = con.execute(f"SELECT * FROM read_parquet('{safe_alt_aws_url}') WHERE alt = '{Number}' LIMIT 1").df()
        if not df_alt.empty:
            alt_records = df_alt.fillna('').astype(str).to_dict(orient='records')
    except Exception as e:
        print(f'Alt DB Error: {e}')

    if not main_records and not alt_records:
        return JSONResponse(status_code=404, content={'status': 'not_found', 'phone': Number})
        
    return {'status': 'success', 'Data': {'Main_Records': main_records, 'Alt_Records': alt_records}}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    uvicorn.run(app, host='0.0.0.0', port=port)

