"""
ultra_scan_routes.py — DB-backed Ultra Scan endpoints.

Provides persistent scan snapshots that survive deploy/restart.
Separate from the ULTRA Pump Research archive (ultra_pump_routes.py).

Endpoints:
  GET  /api/ultra/latest            — latest completed scan metadata
  GET  /api/ultra/latest/candidates — candidates from latest scan
  GET  /api/ultra/scan/status       — live scan status (same as /ultra-scan/status)
  POST /api/ultra/scan              — trigger a new scan
  DELETE /api/ultra/latest          — delete latest scan snapshot only
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ultra", tags=["ultra-scan-db"])

# ── Simple cache to avoid hammering DB on every dashboard refresh ─────────────
_meta_cache:   dict[tuple, tuple[float, Any]] = {}  # key → (ts, payload)
_CACHE_TTL = 30  # seconds


def _cache_get(key: tuple) -> Any | None:
    entry = _meta_cache.get(key)
    if entry and time.time() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: tuple, val: Any) -> Any:
    _meta_cache[key] = (time.time(), val)
    return val


def _cache_invalidate_universe(universe: str, tf: str, nb: str = "") -> None:
    for k in list(_meta_cache.keys()):
        if k[:3] == (universe, tf, nb or ""):
            _meta_cache.pop(k, None)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_latest_run(universe: str, tf: str, nb: str) -> dict | None:
    """Return the is_latest=1 run row for (universe, tf, nb), or None."""
    try:
        from db import get_db
        with get_db() as db:
            db.execute(
                """SELECT id, universe, tf, nasdaq_batch, status, is_latest,
                          total_candidates, last_turbo_scan, sources_json,
                          warnings_json, started_at, finished_at, created_at
                   FROM ultra_scan_runs
                   WHERE universe=? AND tf=? AND nasdaq_batch=? AND is_latest=1 AND status='completed'
                   ORDER BY id DESC LIMIT 1""",
                (universe, tf, nb),
            )
            return db.fetchone()
    except Exception as exc:
        log.warning("ultra/latest DB error: %s", exc)
        return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/latest")
def ultra_latest(
    universe: str = Query("sp500"),
    tf: str       = Query("1d"),
    nasdaq_batch: str = Query(""),
):
    """Latest completed scan metadata (no candidate rows)."""
    nb = nasdaq_batch or ""
    cache_key = ("latest_meta", universe, tf, nb)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    run = _get_latest_run(universe, tf, nb)
    if not run:
        # Also report live scan status so UI can show "scan running" instead of "no data"
        try:
            from ultra_orchestrator import get_ultra_status
            st = get_ultra_status()
            running = st.get("running", False)
        except Exception:
            running = False
        return _cache_set(cache_key, {
            "run": None,
            "scan_running": running,
            "message": "No completed scan yet. Trigger a scan to populate results.",
        })

    sources  = json.loads(run.get("sources_json") or "{}")
    warnings = json.loads(run.get("warnings_json") or "[]")

    try:
        from ultra_orchestrator import get_ultra_status
        st = get_ultra_status()
        running = st.get("running", False)
    except Exception:
        running = False

    payload = {
        "run": {
            "id":               run["id"],
            "universe":         run["universe"],
            "tf":               run["tf"],
            "nasdaq_batch":     run.get("nasdaq_batch", ""),
            "status":           run["status"],
            "total_candidates": run.get("total_candidates", 0),
            "last_turbo_scan":  run.get("last_turbo_scan"),
            "sources":          sources,
            "warnings":         warnings,
            "started_at":       run.get("started_at"),
            "finished_at":      run.get("finished_at"),
            "created_at":       run.get("created_at"),
        },
        "scan_running": running,
    }
    return _cache_set(cache_key, payload)


@router.get("/latest/candidates")
def ultra_latest_candidates(
    universe: str  = Query("sp500"),
    tf: str        = Query("1d"),
    nasdaq_batch: str = Query(""),
    limit: int     = Query(0, ge=0, le=10000),   # 0 = all
    min_score: float = Query(0.0),
    offset: int    = Query(0, ge=0),
):
    """Candidate rows from the latest completed scan, ordered by ultra_score desc."""
    nb = nasdaq_batch or ""
    cache_key = ("candidates", universe, tf, nb, limit, min_score, offset)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    run = _get_latest_run(universe, tf, nb)
    if not run:
        return _cache_set(cache_key, {
            "run_id": None, "universe": universe, "tf": tf,
            "total": 0, "candidates": [],
            "message": "No scan data in DB yet.",
        })

    run_id = run["id"]
    try:
        from db import get_db
        with get_db() as db:
            if limit > 0:
                db.execute(
                    """SELECT ticker, ultra_score, row_json
                       FROM ultra_scan_candidates
                       WHERE scan_run_id=? AND ultra_score>=?
                       ORDER BY ultra_score DESC
                       LIMIT ? OFFSET ?""",
                    (run_id, min_score, limit, offset),
                )
            else:
                db.execute(
                    """SELECT ticker, ultra_score, row_json
                       FROM ultra_scan_candidates
                       WHERE scan_run_id=? AND ultra_score>=?
                       ORDER BY ultra_score DESC
                       LIMIT -1 OFFSET ?""",
                    (run_id, min_score, offset),
                )
            raw_rows = db.fetchall()
    except Exception as exc:
        log.error("ultra/latest/candidates DB error: %s", exc)
        raise HTTPException(status_code=500, detail="DB error fetching candidates")

    candidates = []
    for r in raw_rows:
        try:
            row_data = json.loads(r["row_json"])
        except Exception:
            row_data = {"ticker": r["ticker"], "ultra_score": r["ultra_score"]}
        candidates.append(row_data)

    payload = {
        "run_id":    run_id,
        "universe":  universe,
        "tf":        tf,
        "total":     len(candidates),
        "candidates": candidates,
        "last_turbo_scan": run.get("last_turbo_scan"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    return _cache_set(cache_key, payload)


@router.get("/scan/status")
def ultra_scan_status_db():
    """Live scan status — same data as /api/ultra-scan/status."""
    try:
        from ultra_orchestrator import get_ultra_status
        return get_ultra_status()
    except Exception as exc:
        log.warning("ultra/scan/status error: %s", exc)
        return {"running": False, "error": str(exc)}


class UltraScanTriggerBody(BaseModel):
    universe: str    = "sp500"
    tf: str          = "1d"
    nasdaq_batch: str = ""
    lookback_n: int  = 5
    min_volume: float = 0.0
    min_store_score: float = 5.0
    min_price: float = 0.0
    max_price: float = 1e9
    partial_day: bool = False


@router.post("/scan")
def ultra_scan_trigger_db(body: UltraScanTriggerBody):
    """Trigger a new ULTRA scan. Runs Stage 1 (Turbo) in the background;
    results are persisted to DB on completion. Call GET /api/ultra/scan/status
    to poll progress."""
    from fastapi import BackgroundTasks  # noqa: F401 — imported for type hint only
    import threading

    try:
        from ultra_orchestrator import get_ultra_status, run_ultra_scan_job
        st = get_ultra_status()
        if st.get("running"):
            return {"queued": False, "message": "A scan is already running."}

        def _run():
            try:
                run_ultra_scan_job(
                    universe=body.universe,
                    tf=body.tf,
                    nasdaq_batch=body.nasdaq_batch,
                    lookback_n=body.lookback_n,
                    partial_day=body.partial_day,
                    min_volume=body.min_volume,
                    min_store_score=body.min_store_score,
                    min_price=body.min_price,
                    max_price=body.max_price,
                )
                _cache_invalidate_universe(body.universe, body.tf, body.nasdaq_batch)
            except Exception as exc:
                log.error("ultra/scan background job error: %s", exc)

        t = threading.Thread(target=_run, daemon=True, name="ultra-scan-db-trigger")
        t.start()
        return {"queued": True, "message": f"Scan started for {body.universe}/{body.tf}."}
    except Exception as exc:
        log.error("ultra/scan trigger error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/latest")
def ultra_delete_latest(
    universe: str = Query("sp500"),
    tf: str       = Query("1d"),
    nasdaq_batch: str = Query(""),
):
    """Delete the latest scan snapshot for (universe, tf). Does NOT touch
    the ULTRA Pump Research archive (ultra_pump_runs / episodes / bundles)."""
    nb = nasdaq_batch or ""
    try:
        from db import get_db
        with get_db() as db:
            db.execute(
                """SELECT id FROM ultra_scan_runs
                   WHERE universe=? AND tf=? AND nasdaq_batch=? AND is_latest=1""",
                (universe, tf, nb),
            )
            run_row = db.fetchone()
            if not run_row:
                return {"deleted": False, "message": "No latest scan found."}

            run_id = run_row["id"]
            db.execute("DELETE FROM ultra_scan_runs WHERE id=?", (run_id,))
            db.commit()

        _cache_invalidate_universe(universe, tf, nb)
        # Also evict from memory cache
        try:
            from ultra_orchestrator import _ultra_results_cache, _ultra_lock, _cache_key
            key = _cache_key(universe, tf, nb)
            with _ultra_lock:
                _ultra_results_cache.pop(key, None)
        except Exception:
            pass

        return {"deleted": True, "run_id": run_id}
    except Exception as exc:
        log.error("ultra/latest DELETE error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
