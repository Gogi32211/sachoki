# ── Stage 1: Build React frontend ────────────────────────────────────────────
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend

# package-lock.json is tracked since d342eaa, so the install is deterministic. The glob matters:
# copying only package.json would leave the lockfile invisible and npm ci would refuse to run.
COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build

# vite.config.js has had `outDir: '../backend/static'` since 2026-07-26 (b4176a5), so the build
# lands at /app/backend/static — NOT /app/frontend/dist, which vite stopped producing. This
# Dockerfile kept copying the old path for three months. Fail here, loudly, rather than letting
# stage 2 ship an empty or stale ./static.
RUN test -f /app/backend/static/index.html \
    && test -n "$(ls /app/backend/static/assets/*.js 2>/dev/null)" \
    && echo "frontend build verified: $(ls /app/backend/static/assets | wc -l) assets"

# ── Stage 2: Python runtime ───────────────────────────────────────────────────
FROM python:3.11-slim
WORKDIR /app

COPY backend/requirements.txt backend/*.py ./
# ALL of the backend's packages, not a subset. Only analyzers/ and tz_intelligence/ were copied
# before, so studio_api could not import studio.* inside the image — main.py caught that, tried to
# log a warning, and died on an unrelated NameError instead (see the note in the commit). Seven
# packages carry __init__.py; the image needs every one of them, or the app is not in the image.
COPY backend/studio/ ./studio/
COPY backend/brain/ ./brain/
COPY backend/qlib_lab/ ./qlib_lab/
COPY backend/ai_journal/ ./ai_journal/
COPY backend/company_graph/ ./company_graph/
COPY backend/analyzers/ ./analyzers/
COPY backend/tz_intelligence/ ./tz_intelligence/
COPY tz_intelligence_package/ ./tz_intelligence_package/
RUN pip install --no-cache-dir -r requirements.txt

# Copy built React app into ./static (served by FastAPI StaticFiles)
COPY --from=frontend-build /app/backend/static ./static

ENV DB_PATH=/tmp/scanner.db

EXPOSE 8080

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8080"]
