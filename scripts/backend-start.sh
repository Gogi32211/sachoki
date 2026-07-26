#!/bin/bash
# Sachoki backend auto-start — run by the macOS LaunchAgent (com.sachoki.backend).
# Self-contained: everything (code + venv + .env + data/) lives under the project root.
# NOTE: /bin/bash and the venv python need Full Disk Access (Desktop is TCC-protected).

PROJECT="/Users/sachoki/Desktop/sachoki-desktop"
BACKEND="$PROJECT/backend"

cd "$BACKEND" || exit 1

# Data dir defaults to $PROJECT/data (see studio/paths.py); export only to be explicit.
export SACHOKI_DATA_DIR="$PROJECT/data"

# Load .env API keys (MASSIVE_API_KEY, ANTHROPIC_API_KEY, …)
if [ -f "$BACKEND/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$BACKEND/.env"
    set +a
fi

exec "$BACKEND/.venv/bin/uvicorn" main:app --host 127.0.0.1 --port 8080 --log-level info
