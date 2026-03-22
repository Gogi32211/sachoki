FROM python:3.11-slim

WORKDIR /app

# Build context is repo root (Railway uses root as context with dockerfilePath).
# Explicitly copy backend requirements so we get fastapi/uvicorn/etc.
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the backend source files into /app
COPY backend/ .

EXPOSE 8080
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
