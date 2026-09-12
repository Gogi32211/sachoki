#!/bin/bash
# Wait for the capability process to exit, then seal provenance. The closure is written by a
# SEPARATE process because the running one carries the pre-patch code and would emit a result
# payload with no ledger binding.
set -e
cd /Users/sachoki/Desktop/sachoki-desktop/backend
while kill -0 58567 2>/dev/null; do sleep 60; done
echo "=== capability process exited ==="
.venv/bin/python t5_15m_closure.py
