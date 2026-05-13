"""
signal_replay_routes.py — FastAPI endpoints for the Signal Replay engine.

Phase 1 endpoints:
  POST   /api/signal-replay/run
  GET    /api/signal-replay/status
  GET    /api/signal-replay/history
  GET    /api/signal-replay/{run_id}
  GET    /api/signal-replay/{run_id}/events
  GET    /api/signal-replay/{run_id}/outcomes
  GET    /api/signal-replay/{run_id}/signal-statistics
  DELETE /api/signal-replay/{run_id}

Phase 2 will add pattern-statistics / filter-impact / research-bundle / export.
"""
from __future__ import annotations
import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from db import get_db, USE_PG
from signal_replay_engine import (
    get_state as _get_state,
    run_signal_replay as _run_signal_replay,
    _insert_run as _insert_run_row,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/signal-replay", tags=["signal_replay"])


# ─── Request models ──────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    universe: str = Field(..., pattern="^(sp500|nasdaq|nasdaq_gt5|split|all_us)$")
    mode:     str = Field(..., pattern="^(single_day|date_range)$")
    as_of_date: str | None = None
    start_date: str | None = None
    end_date:   str | None = None
    benchmark_symbol: str = "QQQ"
    event_scope: str = "all_signals"
    min_price:         float | None = None
    min_volume:        int   | None = None
    min_dollar_volume: float | None = None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ph() -> str:
    return "%s" if USE_PG else "?"


def _query(sql: str, params: list | tuple = ()) -> list[dict]:
    with get_db() as db:
        rows = db.fetchall(sql, params)
    return [dict(r) for r in rows] if rows else []


def _query_one(sql: str, params: list | tuple = ()) -> dict | None:
    with get_db() as db:
        row = db.fetchone(sql, params)
    return dict(row) if row else None


# ─── Endpoints ───────────────────────────────────────────────────────────────

@router.post("/run")
def start_run(req: RunRequest, background_tasks: BackgroundTasks) -> dict:
    state = _get_state()
    if state.get("running"):
        raise HTTPException(
            status_code=409,
            detail=f"Signal Replay already running (run_id={state.get('run_id')}). "
                   f"Wait or DELETE the active run.",
        )
    payload = req.model_dump()
    # MVP enforcement: always 1d
    payload["timeframe"] = "1d"

    if payload["mode"] == "single_day" and not payload.get("as_of_date"):
        raise HTTPException(status_code=400, detail="as_of_date required for single_day")
    if payload["mode"] == "date_range" and not (payload.get("start_date") and payload.get("end_date")):
        raise HTTPException(status_code=400, detail="start_date and end_date required for date_range")

    run_id = _insert_run_row(payload)
    background_tasks.add_task(_run_signal_replay, run_id, payload)
    return {"run_id": run_id, "status": "running", "message": "Signal replay started"}


@router.get("/status")
def status() -> dict:
    return _get_state()


@router.get("/history")
def history(limit: int = 50) -> list[dict]:
    limit = max(1, min(limit, 500))
    ph = _ph()
    sql = (f"SELECT id, status, mode, universe, as_of_date, start_date, end_date, "
           f"total_events, total_outcomes, total_statistics_rows, "
           f"started_at, finished_at, error_message "
           f"FROM signal_replay_runs ORDER BY id DESC LIMIT {limit}")
    return _query(sql)


@router.get("/{run_id}")
def get_run(run_id: int) -> dict:
    ph = _ph()
    row = _query_one(f"SELECT * FROM signal_replay_runs WHERE id={ph}", [run_id])
    if not row:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")
    return row


@router.get("/{run_id}/events")
def list_events(
    run_id: int,
    symbol: str | None = None,
    event_signal: str | None = None,
    event_signal_family: str | None = None,
    event_signal_type: str | None = None,
    sequence_4bar: str | None = None,
    abr_category: str | None = None,
    wlnbb_bucket: str | None = None,
    ema50_state: str | None = None,
    role: str | None = None,
    limit: int = 200,
    offset: int = 0,
    sort_by: str = "id",
    sort_dir: str = "desc",
) -> dict:
    limit = max(1, min(limit, 2000))
    offset = max(0, offset)
    ph = _ph()
    where = [f"replay_run_id={ph}"]
    params: list[Any] = [run_id]
    for col, val in (
        ("symbol", symbol), ("event_signal", event_signal),
        ("event_signal_family", event_signal_family),
        ("event_signal_type", event_signal_type),
        ("sequence_4bar", sequence_4bar),
        ("abr_category", abr_category), ("wlnbb_bucket", wlnbb_bucket),
        ("ema50_state", ema50_state), ("role", role),
    ):
        if val is not None:
            where.append(f"{col}={ph}")
            params.append(val)

    sort_col = sort_by if sort_by in {
        "id", "scan_date", "symbol", "event_signal", "score", "close", "volume",
    } else "id"
    sort_dir_sql = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    sql = (f"SELECT * FROM replay_signal_events WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} {sort_dir_sql} LIMIT {limit} OFFSET {offset}")
    rows = _query(sql, params)
    cnt = _query_one(
        f"SELECT COUNT(*) AS n FROM replay_signal_events WHERE {' AND '.join(where)}",
        params,
    )
    total = (cnt or {}).get("n", 0)
    return {"total": total, "limit": limit, "offset": offset, "rows": rows}


@router.get("/{run_id}/outcomes")
def list_outcomes(
    run_id: int,
    symbol: str | None = None,
    horizon: str | None = None,
    outcome_label: str | None = None,
    min_return: float | None = None,
    min_max_gain: float | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    limit = max(1, min(limit, 2000))
    ph = _ph()
    where = [f"replay_run_id={ph}"]
    params: list[Any] = [run_id]
    for col, val in (("symbol", symbol), ("horizon", horizon), ("outcome_label", outcome_label)):
        if val is not None:
            where.append(f"{col}={ph}")
            params.append(val)
    if min_return is not None:
        where.append(f"return_pct >= {ph}")
        params.append(min_return)
    if min_max_gain is not None:
        where.append(f"max_gain_pct >= {ph}")
        params.append(min_max_gain)
    sql = (f"SELECT * FROM replay_signal_outcomes WHERE {' AND '.join(where)} "
           f"ORDER BY id DESC LIMIT {limit} OFFSET {offset}")
    rows = _query(sql, params)
    cnt = _query_one(
        f"SELECT COUNT(*) AS n FROM replay_signal_outcomes WHERE {' AND '.join(where)}",
        params,
    )
    total = (cnt or {}).get("n", 0)
    return {"total": total, "limit": limit, "offset": offset, "rows": rows}


@router.get("/{run_id}/signal-statistics")
def list_signal_statistics(
    run_id: int,
    horizon: str | None = "10d",
    event_signal_family: str | None = None,
    verdict: str | None = None,
    stat_type: str | None = None,
    min_sample_size: int = 0,
    sort_by: str = "median_return",
    sort_dir: str = "desc",
    limit: int = 500,
) -> list[dict]:
    limit = max(1, min(limit, 5000))
    ph = _ph()
    where = [f"replay_run_id={ph}"]
    params: list[Any] = [run_id]
    if horizon:
        where.append(f"horizon={ph}"); params.append(horizon)
    if event_signal_family:
        where.append(f"event_signal_family={ph}"); params.append(event_signal_family)
    if verdict:
        where.append(f"verdict={ph}"); params.append(verdict)
    if stat_type:
        where.append(f"stat_type={ph}"); params.append(stat_type)
    if min_sample_size:
        where.append(f"sample_size >= {ph}"); params.append(min_sample_size)

    sort_col_allowed = {
        "sample_size", "avg_return", "median_return", "win_rate",
        "hit_10pct_rate", "fail_10pct_rate", "expectancy",
        "confidence_score", "stat_key",
    }
    sort_col = sort_by if sort_by in sort_col_allowed else "median_return"
    sort_dir_sql = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    sql = (f"SELECT * FROM replay_signal_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} {sort_dir_sql} NULLS LAST LIMIT {limit}"
           if USE_PG else
           f"SELECT * FROM replay_signal_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} IS NULL, {sort_col} {sort_dir_sql} LIMIT {limit}")
    return _query(sql, params)


@router.delete("/{run_id}")
def delete_run(run_id: int) -> dict:
    state = _get_state()
    if state.get("running") and state.get("run_id") == run_id:
        raise HTTPException(status_code=409, detail="Cannot delete the active run")
    ph = _ph()
    try:
        with get_db() as db:
            for tbl in ("replay_signal_statistics", "replay_signal_outcomes",
                        "replay_signal_events", "signal_replay_runs"):
                db.execute(f"DELETE FROM {tbl} WHERE "
                           f"{'id' if tbl == 'signal_replay_runs' else 'replay_run_id'}={ph}",
                           [run_id])
            db.commit()
        return {"deleted": True, "run_id": run_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"delete failed: {exc}")
