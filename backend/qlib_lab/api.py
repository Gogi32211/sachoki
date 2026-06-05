"""
qlib_lab/api.py — FastAPI router for the QLIB tab.

Mount in main.py:
    from qlib_lab.api import router as qlib_router
    app.include_router(qlib_router)

Endpoints (prefix /api/qlib):
    GET  /columns?universe=sp500   selectable feature columns (forbidden excluded)
    GET  /models                   available model engines
    POST /build                    {universe, features, date_from, date_to, min_bars} -> job
    POST /train                    {universe, features, model, splits, ...}            -> job
    GET  /job/{job_id}             status / log / result polling
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from .columns import list_columns, validate_features
from .models import available_models
from .data import build_dataset, VALID_UNIVERSES
from .train import run_training, DEFAULT_SPLITS
from .search import run_search
from . import jobs

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/qlib", tags=["qlib"])


class BuildRequest(BaseModel):
    universe: str = "sp500"
    features: list[str] = Field(default_factory=list)
    date_from: str | None = None
    date_to: str | None = None
    min_bars: int = 250
    horizon: int = 1
    lookback: int = 0


class TrainRequest(BaseModel):
    universe: str = "sp500"
    features: list[str] = Field(default_factory=list)
    model: str = "lightgbm"
    splits: dict | None = None
    date_from: str | None = None
    date_to: str | None = None
    min_bars: int = 250
    horizon: int = 1
    lookback: int = 0


class SearchRequest(BaseModel):
    universe: str = "sp500"
    features: list[str] = Field(default_factory=list)
    model: str = "lightgbm"
    splits: dict | None = None
    date_from: str | None = None
    date_to: str | None = None
    min_bars: int = 250
    horizon: int = 1
    lookback: int = 0
    max_features: int = 6


@router.get("/columns")
def qlib_columns(universe: str = Query("sp500")):
    if universe not in VALID_UNIVERSES:
        raise HTTPException(400, f"unknown universe {universe!r}")
    return list_columns(universe)


@router.get("/models")
def qlib_models():
    return {"models": available_models(), "default": "lightgbm",
            "default_splits": DEFAULT_SPLITS}


@router.post("/build")
def qlib_build(req: BuildRequest, background_tasks: BackgroundTasks):
    try:
        feats = validate_features(req.features)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not feats:
        raise HTTPException(400, "select at least one feature")
    job_id = jobs.create_job("build", req.model_dump())
    background_tasks.add_task(
        jobs.run_job, job_id, build_dataset,
        req.universe, feats, req.date_from, req.date_to, req.min_bars, req.horizon, req.lookback,
    )
    return {"job_id": job_id, "status": "started"}


@router.post("/train")
def qlib_train(req: TrainRequest, background_tasks: BackgroundTasks):
    try:
        feats = validate_features(req.features)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not feats:
        raise HTTPException(400, "select at least one feature")
    job_id = jobs.create_job("train", req.model_dump())
    background_tasks.add_task(
        jobs.run_job, job_id, run_training,
        req.universe, feats, req.model, req.splits,
        req.date_from, req.date_to, req.min_bars, req.horizon, req.lookback,
    )
    return {"job_id": job_id, "status": "started"}


@router.post("/search")
def qlib_search(req: SearchRequest, background_tasks: BackgroundTasks):
    try:
        feats = validate_features(req.features)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if len(feats) < 2:
        raise HTTPException(400, "select at least 2 features to search combinations")
    job_id = jobs.create_job("search", req.model_dump())
    background_tasks.add_task(
        jobs.run_job, job_id, run_search,
        req.universe, feats, req.model, req.splits,
        req.date_from, req.date_to, req.min_bars, req.horizon, req.lookback, req.max_features,
    )
    return {"job_id": job_id, "status": "started"}


@router.get("/job/{job_id}")
def qlib_job(job_id: str):
    j = jobs.get_job(job_id)
    if j is None:
        raise HTTPException(404, "unknown job id")
    return j


# ── Combo Lab (walk-forward combo discovery + exit optimization) ─────────────

class ComboDiscoverRequest(BaseModel):
    sizes: list[int] = Field(default_factory=lambda: [1, 2, 3])


@router.post("/combo/discover")
def combo_discover(req: ComboDiscoverRequest, background_tasks: BackgroundTasks):
    """Enumerate + walk-forward backtest all combos. Heavy — runs as a job."""
    from .combo_lab.backtest import run_walk_forward
    job_id = jobs.create_job("combo_discover")
    def _run():
        try:
            res = run_walk_forward(sizes=tuple(req.sizes))
            jobs.finish_job(job_id, result={k: v for k, v in res.items() if k != "top"})
        except Exception as e:
            jobs.fail_job(job_id, str(e))
    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "queued"}


@router.post("/combo/optimize-exits")
def combo_optimize_exits(limit: int = 30, background_tasks: BackgroundTasks = None):
    """Grid-search (stop/target/hold) for top-N passed combos. Slower."""
    from .combo_lab.exits import optimize_passed_combos
    job_id = jobs.create_job("combo_exits")
    def _run():
        try:
            res = optimize_passed_combos(limit=limit)
            jobs.finish_job(job_id, result=res)
        except Exception as e:
            jobs.fail_job(job_id, str(e))
    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "queued"}


@router.get("/combo/catalog")
def combo_catalog(status: str | None = None, limit: int = 200):
    """Return the combo catalog rows for the UI table."""
    from ai_journal.db import get_journal_conn, ensure_schema
    ensure_schema()
    c = get_journal_conn()
    try:
        rows = c.execute(f"""
            SELECT combo_id, predicates, size, n_train, n_oos,
                   round(train_hh5*100,1) train_hh5, round(oos_hh5*100,1) oos_hh5,
                   round(train_hh_edge,1) train_edge, round(oos_hh_edge,1) oos_edge,
                   p_value, bonferroni_p, status, pass_reason,
                   best_stop_atr, best_target_atr, best_hold_days,
                   realized_win, realized_avg
            FROM combo_catalog
            {"WHERE status = ?" if status else ""}
            ORDER BY (CASE WHEN status='passed' THEN 0 ELSE 1 END), oos_hh_edge DESC NULLS LAST
            LIMIT ?
        """, ([status, limit] if status else [limit])).fetchall()
        cols = ["combo_id", "predicates", "size", "n_train", "n_oos",
                "train_hh5", "oos_hh5", "train_edge", "oos_edge",
                "p_value", "bonferroni_p", "status", "pass_reason",
                "best_stop_atr", "best_target_atr", "best_hold_days",
                "realized_win", "realized_avg"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}
    finally:
        c.close()
