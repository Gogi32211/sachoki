"""
ultra_pump_routes.py — FastAPI endpoints for the ULTRA Pump Research Engine.

Endpoints (all under /api/ultra-pump):
  POST   /run
  POST   /stop
  POST   /pause
  POST   /resume
  GET    /status
  GET    /history
  GET    /{run_id}
  GET    /{run_id}/episodes
  GET    /{run_id}/caught
  GET    /{run_id}/missed
  GET    /{run_id}/ultra-patterns
  GET    /{run_id}/pattern-lift
  GET    /{run_id}/split-impact
  GET    /{run_id}/diagnostics
  GET    /{run_id}/recommendations
  GET    /{run_id}/research-bundle
  GET    /{run_id}/export-manifest
  GET    /{run_id}/export?part=...
  POST   /{run_id}/rebuild
  POST   /{run_id}/recalculate
  POST   /{run_id}/full-rescan
  DELETE /{run_id}
"""
from __future__ import annotations
import csv
import datetime
import io
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator

from db import get_db, USE_PG
from ultra_pump_research_engine import (
    get_state as _get_state,
    run_ultra_pump_research as _run_engine,
    _insert_run as _insert_run_row,
    request_stop as _request_stop,
    request_pause as _request_pause,
    request_resume as _request_resume,
    ALLOWED_HORIZONS, ALLOWED_PRE_PUMP_WINDOW, ALLOWED_SCANNER_WINDOW,
    ALLOWED_LOOKBACK_BARS, ALLOWED_SPLIT_IMPACT_WINDOW,
    DEFAULT_PUMP_HORIZON, DEFAULT_PRE_PUMP_WINDOW_BARS,
    DEFAULT_SCANNER_WINDOW_BARS, DEFAULT_DETECTION_REFERENCE,
    DEFAULT_LOOKBACK_BARS, DEFAULT_SPLIT_IMPACT_WINDOW,
)
from replay_storage import (
    run_dir, artifact_path, query_parquet, count_parquet,
    delete_run_directory,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ultra-pump", tags=["ultra_pump"])


# ── Pydantic models ──────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    universe: str = Field("all_us", pattern="^(sp500|nasdaq|nasdaq_gt5|split|all_us)$")
    mode:     str = Field("date_range", pattern="^(single_day|date_range|last_n_days|ytd)$")
    pump_target: str = Field("X2_TO_X4", pattern="^(X2_TO_X4|X4_PLUS|BOTH)$")
    start_date: str | None = None
    end_date:   str | None = None
    lookback_days: int | None = None

    pump_horizon:                  int | None = None
    pre_pump_window_bars:          int | None = None
    scanner_detection_window_bars: int | None = None
    detection_reference:           str | None = None
    lookback_bars:                 int | None = None
    split_impact_window_days:      int | None = None

    research_mode:    str = "standard"
    event_scope:      str = "pumps_only"
    min_price:         float | None = None
    min_volume:        int   | None = None
    min_dollar_volume: float | None = None
    benchmark_symbol:  str = "QQQ"

    @field_validator("pump_horizon", mode="before")
    @classmethod
    def _v_horizon(cls, v):
        if v is None:
            return None
        v = int(v)
        if v not in ALLOWED_HORIZONS:
            raise ValueError(f"pump_horizon must be one of {sorted(ALLOWED_HORIZONS)}")
        return v

    @field_validator("pre_pump_window_bars", mode="before")
    @classmethod
    def _v_pp(cls, v):
        if v is None:
            return None
        v = int(v)
        if v not in ALLOWED_PRE_PUMP_WINDOW:
            raise ValueError(f"pre_pump_window_bars must be one of {sorted(ALLOWED_PRE_PUMP_WINDOW)}")
        return v

    @field_validator("scanner_detection_window_bars", mode="before")
    @classmethod
    def _v_sw(cls, v):
        if v is None:
            return None
        v = int(v)
        if v not in ALLOWED_SCANNER_WINDOW:
            raise ValueError(f"scanner_detection_window_bars must be one of {sorted(ALLOWED_SCANNER_WINDOW)}")
        return v

    @field_validator("lookback_bars", mode="before")
    @classmethod
    def _v_lb(cls, v):
        if v is None:
            return None
        v = int(v)
        if v not in ALLOWED_LOOKBACK_BARS:
            raise ValueError(f"lookback_bars must be one of {sorted(ALLOWED_LOOKBACK_BARS)}")
        return v

    @field_validator("split_impact_window_days", mode="before")
    @classmethod
    def _v_si(cls, v):
        if v is None:
            return None
        v = int(v)
        if v not in ALLOWED_SPLIT_IMPACT_WINDOW:
            raise ValueError(f"split_impact_window_days must be one of {sorted(ALLOWED_SPLIT_IMPACT_WINDOW)}")
        return v


# ── Helpers ──────────────────────────────────────────────────────────────────

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


def _artifact_path_if_ready(run_id: int, slot: str) -> Path | None:
    p = artifact_path(run_id, slot, "parquet")
    if not p.exists() or p.stat().st_size == 0:
        return None
    return p


def _csv_neutralize_value(v: Any) -> Any:
    """Prefix any cell starting with = + - @ with a single quote (CSV-injection guard)."""
    if isinstance(v, str) and v and v[0] in ("=", "+", "-", "@"):
        return "'" + v
    return v


# ── Endpoints: lifecycle ─────────────────────────────────────────────────────

@router.post("/run")
def start_run(req: RunRequest, background_tasks: BackgroundTasks) -> dict:
    state = _get_state()
    if state.get("running"):
        raise HTTPException(
            status_code=409,
            detail=(f"ULTRA Pump Research already running (run_id={state.get('run_id')}). "
                    "Stop or delete the active run first."),
        )
    payload = req.model_dump()

    mode = payload["mode"]
    if mode == "single_day" and not payload.get("as_of_date" if False else "start_date"):
        # single_day uses start_date as the anchor
        if not payload.get("start_date"):
            raise HTTPException(status_code=400, detail="start_date required for single_day")
    if mode == "date_range" and not (payload.get("start_date") and payload.get("end_date")):
        raise HTTPException(status_code=400, detail="start_date and end_date required for date_range")
    if mode == "last_n_days" and not payload.get("lookback_days"):
        raise HTTPException(status_code=400, detail="lookback_days required for last_n_days")

    run_id = _insert_run_row(payload)
    background_tasks.add_task(_run_engine, run_id, payload)
    return {"run_id": run_id, "status": "running", "message": "ULTRA Pump Research started"}


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
    sql = (f"SELECT id, status, universe, mode, pump_target, start_date, end_date, "
           f"pump_horizon, pre_pump_window_bars, scanner_detection_window_bars, "
           f"detection_reference, lookback_bars, split_impact_window_days, "
           f"total_episodes, total_caught, total_missed, symbols_total, symbols_completed, "
           f"summary_json, artifact_manifest_json, error_message, "
           f"started_at, finished_at "
           f"FROM ultra_pump_runs ORDER BY id DESC LIMIT {limit}")
    runs = _query(sql)
    for run in runs:
        rid = run.get("id")
        if rid:
            rdir = run_dir(rid)
            run["artifacts_on_disk"] = rdir.exists()
            if rdir.exists():
                run["disk_bytes"] = sum(
                    f.stat().st_size for f in rdir.rglob("*") if f.is_file()
                )
    return runs


@router.get("/{run_id}")
def get_run(run_id: int) -> dict:
    ph = _ph()
    row = _query_one(f"SELECT * FROM ultra_pump_runs WHERE id={ph}", [run_id])
    if not row:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")
    artifacts = _query(
        f"SELECT artifact_type, file_path, format, row_count, size_bytes, created_at "
        f"FROM replay_artifacts WHERE run_id={ph} ORDER BY artifact_type",
        [run_id],
    )
    row = dict(row)
    row["artifacts"] = artifacts
    # Attach manifest JSON if present
    em_path = run_dir(run_id) / "export_manifest.json"
    if em_path.exists():
        try:
            row["export_manifest"] = json.loads(em_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return row


@router.delete("/{run_id}")
def delete_run(run_id: int) -> dict:
    state = _get_state()
    if state.get("running") and state.get("run_id") == run_id:
        raise HTTPException(status_code=409, detail="Cannot delete the active run")
    ph = _ph()
    run = _query_one(f"SELECT id FROM ultra_pump_runs WHERE id={ph}", [run_id])
    if not run:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")
    try:
        disk_deleted = delete_run_directory(run_id)
        with get_db() as db:
            db.execute(f"DELETE FROM replay_artifacts WHERE run_id={ph}", [run_id])
            db.execute(f"DELETE FROM ultra_pump_runs WHERE id={ph}", [run_id])
            db.commit()
        return {"deleted": True, "run_id": run_id, "disk_directory_removed": disk_deleted}
    except Exception as exc:
        log.exception("ultra_pump delete failed for run %s", run_id)
        raise HTTPException(status_code=500, detail=f"delete failed: {exc}")


# ── Endpoints: artifact listing (read parquet via DuckDB) ───────────────────

def _list_artifact_rows(
    run_id: int,
    slot: str,
    *,
    conditions: list[tuple[str, str, Any]] | None = None,
    sort_col: str = "anchor_date",
    sort_dir: str = "DESC",
    limit: int = 500,
    offset: int = 0,
) -> dict:
    p = _artifact_path_if_ready(run_id, slot)
    if p is None:
        return {"total": 0, "limit": limit, "offset": offset, "rows": []}
    try:
        rows = query_parquet(p, conditions or [], sort_col=sort_col,
                             sort_dir=sort_dir, limit=limit, offset=offset)
        total = count_parquet(p, conditions or [])
    except Exception as exc:
        log.warning("ultra_pump list %s/%s failed: %s", run_id, slot, exc)
        return {"total": 0, "limit": limit, "offset": offset, "rows": []}
    return {"total": total, "limit": limit, "offset": offset, "rows": rows}


@router.get("/{run_id}/episodes")
def list_episodes(
    run_id: int,
    category: str | None = None,
    symbol: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> dict:
    conds: list[tuple[str, str, Any]] = []
    if category:
        conds.append(("category", "=", category))
    if symbol:
        conds.append(("symbol", "=", symbol))
    return _list_artifact_rows(run_id, "pump_episodes", conditions=conds,
                               sort_col="anchor_date", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/caught")
def list_caught(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "scanner_caught_pumps",
                               sort_col="anchor_date", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/missed")
def list_missed(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "missed_pumps",
                               sort_col="anchor_date", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/ultra-patterns")
def list_ultra_patterns(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "ultra_pattern_stats",
                               sort_col="lift_vs_baseline", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/pattern-lift")
def list_pattern_lift(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "ultra_pattern_lift_stats",
                               sort_col="lift_all", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/split-impact")
def list_split_impact(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "split_impact_stats",
                               sort_col="lift", sort_dir="DESC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


@router.get("/{run_id}/diagnostics")
def list_diagnostics(run_id: int, limit: int = 500, offset: int = 0) -> dict:
    return _list_artifact_rows(run_id, "missed_diagnostics",
                               sort_col="episode_id", sort_dir="ASC",
                               limit=max(1, min(limit, 5000)), offset=max(0, offset))


# ── JSON artifact endpoints ──────────────────────────────────────────────────

def _read_json_artifact(run_id: int, name: str) -> dict | None:
    p = run_dir(run_id) / f"{name}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


@router.get("/{run_id}/recommendations")
def get_recommendations(run_id: int) -> dict:
    body = _read_json_artifact(run_id, "ultra_recommendations")
    if body is None:
        raise HTTPException(status_code=404, detail="recommendations not yet written")
    return body


@router.get("/{run_id}/research-bundle")
def get_research_bundle(run_id: int) -> dict:
    body = _read_json_artifact(run_id, "research_bundle")
    if body is None:
        raise HTTPException(status_code=404, detail="research_bundle not yet written")
    return body


@router.get("/{run_id}/export-manifest")
def get_export_manifest(run_id: int) -> dict:
    body = _read_json_artifact(run_id, "export_manifest")
    if body is None:
        raise HTTPException(status_code=404, detail="export_manifest not yet written")
    return body


# ── Control endpoints (Phase 5 fills behavior; Phase 1 = stubs) ─────────────

@router.post("/{run_id}/rebuild")
def rebuild_run(run_id: int, background_tasks: BackgroundTasks) -> dict:
    """Re-derive downstream artifacts (patterns/lift/splits/recs) from existing
    episodes + pre_pump_ultra_bars. Phase 5 implements the full rebuild path."""
    return {
        "run_id": run_id,
        "queued": False,
        "phase": "phase_1_foundation",
        "message": "Rebuild logic is implemented in Phase 5. Phase 1 only stores the foundation.",
    }


@router.post("/{run_id}/recalculate")
def recalculate_run(run_id: int, background_tasks: BackgroundTasks) -> dict:
    return {
        "run_id": run_id,
        "queued": False,
        "phase": "phase_1_foundation",
        "message": "Recalculate logic is implemented in Phase 5.",
    }


@router.post("/{run_id}/full-rescan")
def full_rescan_run(run_id: int, background_tasks: BackgroundTasks) -> dict:
    return {
        "run_id": run_id,
        "queued": False,
        "phase": "phase_1_foundation",
        "message": "Full rescan logic is implemented in Phase 5. Start a new run for a clean re-detection.",
    }


# ── Export ───────────────────────────────────────────────────────────────────

_EXPORT_PARTS_PARQUET = {
    "pump_episodes", "x2_to_x4_episodes", "x4_plus_episodes",
    "scanner_caught_pumps", "missed_pumps",
    "pre_pump_ultra_bars", "pre_pump_ultra_signals", "pre_pump_ultra_combinations",
    "missed_diagnostics",
    "ultra_pattern_stats", "ultra_pattern_lift_stats", "ultra_timing_stats",
    "baseline_windows", "baseline_ultra_patterns", "baseline_pattern_stats",
    "split_impact_stats", "split_related_pumps",
    "clean_non_split_pumps", "post_reverse_split_pumps",
}
_EXPORT_PARTS_JSON = {"run", "research_bundle", "export_manifest", "ultra_recommendations", "warnings"}
_EXPORT_PARTS_SPECIAL = {"all_non_empty_zip"}


@router.get("/{run_id}/export")
def export_run(
    run_id: int,
    part: str = Query("pump_episodes"),
    fmt: str = "json",
    offset: int = 0,
    limit: int = 100_000,
) -> Response:
    ph = _ph()
    run = _query_one(f"SELECT * FROM ultra_pump_runs WHERE id={ph}", [run_id])
    if not run:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")

    valid = _EXPORT_PARTS_PARQUET | _EXPORT_PARTS_JSON | _EXPORT_PARTS_SPECIAL
    if part not in valid:
        raise HTTPException(status_code=400, detail=f"part must be one of {sorted(valid)}")

    run_status = run["status"] if isinstance(run, dict) else getattr(run, "status", None)
    if run_status in ("running", "paused") and part != "run":
        raise HTTPException(
            status_code=409,
            detail=(f"Run {run_id} is still {run_status}. Artifacts are finalized after the "
                    "run completes — only the 'run' metadata is available now."),
        )

    use_csv = str(fmt).lower() == "csv"

    # ── JSON-only parts ──
    if part in _EXPORT_PARTS_JSON:
        if part == "run":
            content = json.dumps(dict(run), default=str, indent=2)
        else:
            p = run_dir(run_id) / f"{part}.json"
            if not p.exists():
                content = json.dumps({"run_id": run_id, "status": "no_data",
                                      "message": f"{part}.json not yet written"}, indent=2)
            else:
                content = p.read_text(encoding="utf-8")
        return Response(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_{part}.json"'},
        )

    # ── all_non_empty_zip ──
    if part == "all_non_empty_zip":
        import zipfile
        rdir = run_dir(run_id)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            if not rdir.exists():
                zf.writestr("EMPTY.txt", f"run {run_id} has no artifacts on disk")
            else:
                for f in sorted(rdir.iterdir()):
                    if not f.is_file():
                        continue
                    if f.suffix == ".parquet" and f.stat().st_size <= 1024:
                        # parquet with only schema (empty) — skip from "non_empty" zip
                        # but only if the parquet has 0 rows
                        try:
                            n = count_parquet(f, [])
                        except Exception:
                            n = 0
                        if n == 0:
                            continue
                    zf.write(f, arcname=f.name)
        return Response(
            content=buf.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_all_non_empty.zip"'},
        )

    # ── Parquet-backed parts → JSON or CSV ──
    p = artifact_path(run_id, part, "parquet")
    if not p.exists() or p.stat().st_size == 0:
        # Empty / missing → still respond with an empty body and the reason
        em = _read_json_artifact(run_id, "export_manifest") or {}
        reason = None
        for a in em.get("artifacts", []):
            if a.get("artifact_name") == part:
                reason = a.get("reason_if_empty")
                break
        if use_csv:
            return Response(
                content=f"# no rows for {part}: {reason or 'artifact missing'}\n",
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_{part}.csv"'},
            )
        return Response(
            content=json.dumps({"run_id": run_id, "part": part, "rows": [],
                                "reason_if_empty": reason}, default=str, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_{part}.json"'},
        )

    rows = query_parquet(p, [], sort_col="anchor_date" if part.endswith("episodes") else "episode_id",
                         sort_dir="DESC", limit=max(1, min(limit, 1_000_000)),
                         offset=max(0, offset))

    if use_csv:
        buf = io.StringIO()
        if rows:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: _csv_neutralize_value(v) for k, v in r.items()})
        else:
            buf.write(f"# no rows for {part}\n")
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_{part}.csv"'},
        )

    body = {
        "run_id": run_id,
        "exported_at": datetime.datetime.utcnow().isoformat() + "Z",
        "part": part,
        "offset": offset,
        "limit": limit,
        "row_count": len(rows),
        "rows": rows,
    }
    return Response(
        content=json.dumps(body, default=str, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="ultra_pump_{run_id}_{part}.json"'},
    )
