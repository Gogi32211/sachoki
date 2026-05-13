"""
signal_replay_routes.py — FastAPI endpoints for the Signal Replay engine.

Endpoints:
  POST   /api/signal-replay/run
  POST   /api/signal-replay/stop
  POST   /api/signal-replay/pause
  POST   /api/signal-replay/resume
  GET    /api/signal-replay/status
  GET    /api/signal-replay/history
  GET    /api/signal-replay/{run_id}
  GET    /api/signal-replay/{run_id}/events
  GET    /api/signal-replay/{run_id}/outcomes
  GET    /api/signal-replay/{run_id}/signal-statistics
  GET    /api/signal-replay/{run_id}/pattern-statistics
  GET    /api/signal-replay/{run_id}/filter-impact
  GET    /api/signal-replay/{run_id}/export
  DELETE /api/signal-replay/{run_id}
"""
from __future__ import annotations
import datetime
import json
import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from db import get_db, USE_PG
from signal_replay_engine import (
    get_state as _get_state,
    run_signal_replay as _run_signal_replay,
    _insert_run as _insert_run_row,
    request_stop as _request_stop,
    request_pause as _request_pause,
    request_resume as _request_resume,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/signal-replay", tags=["signal_replay"])


# ─── Request models ──────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    universe: str = Field(..., pattern="^(sp500|nasdaq|nasdaq_gt5|split|all_us)$")
    mode:     str = Field(..., pattern="^(single_day|date_range|last_n_days|ytd)$")
    as_of_date:    str | None = None
    start_date:    str | None = None
    end_date:      str | None = None
    lookback_days: int | None = None   # for last_n_days mode
    benchmark_symbol: str = "QQQ"
    event_scope: str = "all_signals"
    min_price:         float | None = None
    min_volume:        int   | None = None
    min_dollar_volume: float | None = None
    lookback_bars:     int   | None = None   # bars to fetch per ticker: 500/1000/1500/2000


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ph() -> str:
    return "%s" if USE_PG else "?"


def _query(sql: str, params: list | tuple = ()) -> list[dict]:
    with get_db() as db:
        db.execute(sql, params)
        rows = db.fetchall()
    return rows if rows else []


def _query_one(sql: str, params: list | tuple = ()) -> dict | None:
    with get_db() as db:
        db.execute(sql, params)
        row = db.fetchone()
    return row


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

    mode = payload["mode"]
    if mode == "single_day" and not payload.get("as_of_date"):
        raise HTTPException(status_code=400, detail="as_of_date required for single_day")
    if mode == "date_range" and not (payload.get("start_date") and payload.get("end_date")):
        raise HTTPException(status_code=400, detail="start_date and end_date required for date_range")
    if mode == "last_n_days" and not payload.get("lookback_days"):
        raise HTTPException(status_code=400, detail="lookback_days required for last_n_days")

    run_id = _insert_run_row(payload)
    background_tasks.add_task(_run_signal_replay, run_id, payload)
    return {"run_id": run_id, "status": "running", "message": "Signal replay started"}


@router.get("/status")
def status() -> dict:
    return _get_state()


@router.post("/stop")
def stop_run() -> dict:
    state = _get_state()
    if not state.get("running"):
        raise HTTPException(status_code=409, detail="No active run to stop")
    _request_stop()
    return {"message": "Stop requested", "run_id": state.get("run_id")}


@router.post("/pause")
def pause_run() -> dict:
    state = _get_state()
    if not state.get("running"):
        raise HTTPException(status_code=409, detail="No active run to pause")
    if state.get("pause_requested"):
        raise HTTPException(status_code=409, detail="Already paused")
    _request_pause()
    return {"message": "Paused", "run_id": state.get("run_id")}


@router.post("/resume")
def resume_run() -> dict:
    state = _get_state()
    if not state.get("running"):
        raise HTTPException(status_code=409, detail="No active run to resume")
    _request_resume()
    return {"message": "Resumed", "run_id": state.get("run_id")}


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


@router.get("/{run_id}/pattern-statistics")
def list_pattern_statistics(
    run_id: int,
    horizon: str | None = "10d",
    pattern_type: str | None = None,
    terminal_signal: str | None = None,
    min_sample_size: int = 3,
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
    if pattern_type:
        where.append(f"pattern_type={ph}"); params.append(pattern_type)
    if terminal_signal:
        where.append(f"terminal_signal={ph}"); params.append(terminal_signal)
    if min_sample_size:
        where.append(f"sample_size >= {ph}"); params.append(min_sample_size)

    sort_col_allowed = {
        "sample_size", "median_return", "avg_return", "win_rate",
        "hit_10pct_rate", "fail_10pct_rate", "expectancy",
        "confidence_score", "stat_key", "pattern_value",
    }
    sort_col = sort_by if sort_by in sort_col_allowed else "median_return"
    sort_dir_sql = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    sql = (f"SELECT * FROM replay_pattern_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} {sort_dir_sql} NULLS LAST LIMIT {limit}"
           if USE_PG else
           f"SELECT * FROM replay_pattern_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} IS NULL, {sort_col} {sort_dir_sql} LIMIT {limit}")
    return _query(sql, params)


@router.get("/{run_id}/filter-impact")
def list_filter_impact(
    run_id: int,
    horizon: str | None = "10d",
    base_signal: str | None = None,
    filter_name: str | None = None,
    filter_value: str | None = None,
    min_sample_size: int = 5,
    sort_by: str = "lift_median_return",
    sort_dir: str = "desc",
    limit: int = 500,
) -> list[dict]:
    limit = max(1, min(limit, 5000))
    ph = _ph()
    where = [f"replay_run_id={ph}"]
    params: list[Any] = [run_id]
    if horizon:
        where.append(f"horizon={ph}"); params.append(horizon)
    if base_signal:
        where.append(f"base_signal={ph}"); params.append(base_signal)
    if filter_name:
        where.append(f"filter_name={ph}"); params.append(filter_name)
    if filter_value:
        where.append(f"filter_value={ph}"); params.append(filter_value)
    if min_sample_size:
        where.append(f"sample_size >= {ph}"); params.append(min_sample_size)

    sort_col_allowed = {
        "sample_size", "median_return", "avg_return", "win_rate",
        "hit_10pct_rate", "fail_10pct_rate",
        "lift_median_return", "lift_hit_10pct", "confidence_score",
    }
    sort_col = sort_by if sort_by in sort_col_allowed else "lift_median_return"
    sort_dir_sql = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
    sql = (f"SELECT * FROM replay_filter_impact_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} {sort_dir_sql} NULLS LAST LIMIT {limit}"
           if USE_PG else
           f"SELECT * FROM replay_filter_impact_statistics WHERE {' AND '.join(where)} "
           f"ORDER BY {sort_col} IS NULL, {sort_col} {sort_dir_sql} LIMIT {limit}")
    return _query(sql, params)


@router.get("/{run_id}/export")
def export_run(
    run_id: int,
    part: str = "all",
    offset: int = 0,
    limit: int = 50000,
) -> Response:
    """Export run data as a downloadable JSON file.

    `part` selects which slice to export — useful when the full export is too
    large for analytics tooling. Valid values:
      run           — run metadata only
      signal_stats  — replay_signal_statistics rows
      pattern_stats — replay_pattern_statistics rows
      filter_impact — replay_filter_impact_statistics rows
      events        — replay_signal_events (paginated via offset/limit)
      outcomes      — replay_signal_outcomes (paginated via offset/limit)
      all           — everything in one file (legacy behaviour)
    """
    ph = _ph()
    run = _query_one(f"SELECT * FROM signal_replay_runs WHERE id={ph}", [run_id])
    if not run:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")

    valid_parts = {"run", "signal_stats", "pattern_stats", "filter_impact",
                   "events", "outcomes", "all"}
    if part not in valid_parts:
        raise HTTPException(status_code=400,
                            detail=f"part must be one of {sorted(valid_parts)}")

    offset = max(0, int(offset))
    limit  = max(1, min(int(limit), 500_000))

    meta = {
        "run_id":      run_id,
        "exported_at": datetime.datetime.utcnow().isoformat() + "Z",
        "version":     "phase3",
        "part":        part,
    }

    body: dict[str, Any] = {"meta": meta, "run": run}

    if part in ("run", "all"):
        # run already included
        pass

    if part in ("signal_stats", "all"):
        rows = _query(
            f"SELECT * FROM replay_signal_statistics WHERE replay_run_id={ph}",
            [run_id],
        )
        body["signal_statistics"] = rows
        meta["signal_stats_count"] = len(rows)

    if part in ("pattern_stats", "all"):
        rows = _query(
            f"SELECT * FROM replay_pattern_statistics WHERE replay_run_id={ph}",
            [run_id],
        )
        body["pattern_statistics"] = rows
        meta["pattern_stats_count"] = len(rows)

    if part in ("filter_impact", "all"):
        rows = _query(
            f"SELECT * FROM replay_filter_impact_statistics WHERE replay_run_id={ph}",
            [run_id],
        )
        body["filter_impact_statistics"] = rows
        meta["filter_impact_count"] = len(rows)

    if part in ("events", "all"):
        sql = (f"SELECT * FROM replay_signal_events WHERE replay_run_id={ph} "
               f"ORDER BY id LIMIT {limit} OFFSET {offset}")
        rows = _query(sql, [run_id])
        body["events"] = rows
        meta["events_count"]  = len(rows)
        meta["events_offset"] = offset
        meta["events_limit"]  = limit

    if part in ("outcomes", "all"):
        sql = (f"SELECT * FROM replay_signal_outcomes WHERE replay_run_id={ph} "
               f"ORDER BY id LIMIT {limit} OFFSET {offset}")
        rows = _query(sql, [run_id])
        body["outcomes"] = rows
        meta["outcomes_count"]  = len(rows)
        meta["outcomes_offset"] = offset
        meta["outcomes_limit"]  = limit

    filename = (f"replay_{run_id}_export.json" if part == "all"
                else f"replay_{run_id}_{part}_{offset}-{offset + limit}.json"
                     if part in ("events", "outcomes")
                else f"replay_{run_id}_{part}.json")

    content = json.dumps(body, default=str, indent=2)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


_HEAVY_TABLES   = ("replay_signal_outcomes", "replay_signal_events")
_LIGHT_TABLES   = ("replay_filter_impact_statistics", "replay_pattern_statistics",
                   "replay_signal_statistics")
_DELETE_CHUNK   = 5000  # rows per chunked delete (heavy tables)


def _delete_run_chunked(run_id: int) -> dict:
    """Delete one run's data using small per-chunk transactions.

    Heavy tables are deleted in 5k-row batches to keep PG WAL growth bounded
    and survive low-disk conditions. Light tables and the run row are deleted
    in single statements.
    """
    ph = _ph()
    deleted_counts: dict[str, int] = {}

    for tbl in _HEAVY_TABLES:
        total = 0
        while True:
            with get_db() as db:
                if USE_PG:
                    sql = (f"DELETE FROM {tbl} WHERE ctid IN ("
                           f"SELECT ctid FROM {tbl} WHERE replay_run_id={ph} "
                           f"LIMIT {_DELETE_CHUNK})")
                    db.execute(sql, [run_id])
                    rc = getattr(db, "rowcount", 0) or 0
                else:
                    db.execute(
                        f"DELETE FROM {tbl} WHERE id IN ("
                        f"SELECT id FROM {tbl} WHERE replay_run_id={ph} "
                        f"LIMIT {_DELETE_CHUNK})",
                        [run_id],
                    )
                    rc = getattr(db, "rowcount", 0) or 0
                db.commit()
            total += rc
            if rc < _DELETE_CHUNK:
                break
        deleted_counts[tbl] = total

    for tbl in _LIGHT_TABLES:
        with get_db() as db:
            db.execute(f"DELETE FROM {tbl} WHERE replay_run_id={ph}", [run_id])
            rc = getattr(db, "rowcount", 0) or 0
            db.commit()
        deleted_counts[tbl] = rc

    with get_db() as db:
        db.execute(f"DELETE FROM signal_replay_runs WHERE id={ph}", [run_id])
        db.commit()
    deleted_counts["signal_replay_runs"] = 1
    return deleted_counts


@router.delete("/{run_id}")
def delete_run(run_id: int) -> dict:
    state = _get_state()
    if state.get("running") and state.get("run_id") == run_id:
        raise HTTPException(status_code=409, detail="Cannot delete the active run")
    try:
        counts = _delete_run_chunked(run_id)
        return {"deleted": True, "run_id": run_id, "rows_deleted": counts}
    except Exception as exc:
        log.exception("delete_run failed for run %s", run_id)
        raise HTTPException(status_code=500, detail=f"delete failed: {exc}")


@router.post("/purge-all")
def purge_all_runs(confirm: str | None = None) -> dict:
    """Nuke ALL replay data via TRUNCATE. Requires confirm=YES to execute.

    Use this when DELETE fails due to disk pressure or accumulated runs.
    Live scanner tables are untouched. Stop any active run before calling.
    """
    state = _get_state()
    if state.get("running"):
        raise HTTPException(status_code=409,
                            detail="Active run in progress — stop it before purging")
    if confirm != "YES":
        raise HTTPException(status_code=400,
                            detail="Pass ?confirm=YES to confirm full purge of all replay data")

    tables = list(_HEAVY_TABLES) + list(_LIGHT_TABLES) + ["signal_replay_runs"]
    try:
        with get_db() as db:
            if USE_PG:
                # CASCADE not needed (no FKs) — RESTART IDENTITY resets serial seqs
                db.execute(
                    f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY"
                )
            else:
                for t in tables:
                    db.execute(f"DELETE FROM {t}")
            db.commit()
        return {"purged": True, "tables": tables}
    except Exception as exc:
        log.exception("purge_all_runs failed")
        raise HTTPException(status_code=500, detail=f"purge failed: {exc}")
