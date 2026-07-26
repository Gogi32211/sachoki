"""
qlib_lab/jobs.py — tiny in-memory job registry for the QLIB tab.

Build + train can take a while on the 8M-row table, so the API kicks them off in
a background task and the UI polls for status. Single-process, single-user local
app, so an in-memory dict behind a lock is plenty (no external queue).
"""

from __future__ import annotations

import threading
import time
import traceback
import uuid

_LOCK = threading.Lock()
_JOBS: dict[str, dict] = {}
_MAX_LOG = 400


def create_job(kind: str, params: dict) -> str:
    job_id = uuid.uuid4().hex[:12]
    with _LOCK:
        _JOBS[job_id] = {
            "id": job_id,
            "kind": kind,           # "build" | "train"
            "status": "queued",     # queued | running | done | error
            "params": params,
            "log": [],
            "result": None,
            "error": None,
            "created": time.time(),
            "started": None,
            "finished": None,
        }
    return job_id


def _append_log(job_id: str, msg: str):
    with _LOCK:
        j = _JOBS.get(job_id)
        if j is None:
            return
        j["log"].append(f"[{time.strftime('%H:%M:%S')}] {msg}")
        if len(j["log"]) > _MAX_LOG:
            j["log"] = j["log"][-_MAX_LOG:]


def log_fn_for(job_id: str):
    return lambda msg: _append_log(job_id, msg)


def set_running(job_id: str):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id]["status"] = "running"
            _JOBS[job_id]["started"] = time.time()


def set_done(job_id: str, result):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id].update(status="done", result=result, finished=time.time())


def set_error(job_id: str, exc: Exception):
    with _LOCK:
        if job_id in _JOBS:
            _JOBS[job_id].update(
                status="error",
                error=f"{type(exc).__name__}: {exc}",
                finished=time.time(),
            )
            _JOBS[job_id]["log"].append("TRACEBACK:\n" + traceback.format_exc())


def get_job(job_id: str) -> dict | None:
    with _LOCK:
        j = _JOBS.get(job_id)
        return dict(j) if j else None


def run_job(job_id: str, fn, *args, **kwargs):
    """Execute fn in the current (background) thread, recording status/result."""
    set_running(job_id)
    try:
        result = fn(*args, log_fn=log_fn_for(job_id), **kwargs)
        set_done(job_id, result)
    except Exception as exc:  # noqa: BLE001 — surface everything to the UI
        _append_log(job_id, f"FAILED: {type(exc).__name__}: {exc}")
        set_error(job_id, exc)
