# 1. Base image set karein (Lightweight Python)
FROM python:3.10-slim

# 2. Container ke andar working directory banayein
WORKDIR /app

# 3. Pehle requirements file copy karein aur libraries install karein
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Apna main Python code (app.py) copy karein
COPY app.py .

# 5. Port expose karein
EXPOSE 8000

# 6. Server start karne ka final command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
