#!/bin/bash
# Sachoki backend auto-start script — used by macOS LaunchAgent
# Sources .env for API keys, then starts uvicorn on port 8080.

cd /Users/sachoki/Desktop/sachoki-desktop/backend

# Load environment variables from .env (ignore missing keys / comments)
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

exec .venv/bin/uvicorn main:app --host 127.0.0.1 --port 8080 --log-level info
