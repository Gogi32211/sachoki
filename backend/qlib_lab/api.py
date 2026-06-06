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
    job_id = jobs.create_job("combo_discover", req.dict())
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
    job_id = jobs.create_job("combo_exits", {"limit": limit})
    def _run():
        try:
            res = optimize_passed_combos(limit=limit)
            jobs.finish_job(job_id, result=res)
        except Exception as e:
            jobs.fail_job(job_id, str(e))
    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "queued"}


class ComboPnlDiscoverRequest(BaseModel):
    horizon: int = 10
    beam: int = 40
    depth_max: int = 5


@router.post("/combo/discover-pnl")
def combo_discover_pnl(req: ComboPnlDiscoverRequest, background_tasks: BackgroundTasks):
    """Greedy beam search on realized P&L edge for a given horizon (H=1/3/5/10)."""
    from .combo_lab.enumerate_greedy import run_greedy
    job_id = jobs.create_job(f"combo_pnl_h{req.horizon}", req.dict())
    def _run():
        try:
            res = run_greedy(horizon=req.horizon, depth_max=req.depth_max, beam=req.beam)
            jobs.finish_job(job_id, result={k: v for k, v in res.items() if k != "top"})
        except Exception as e:
            jobs.fail_job(job_id, str(e))
    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "queued"}


@router.get("/combo/catalog-pnl")
def combo_catalog_pnl(horizon: int = 10, status: str | None = None, limit: int = 400):
    """P&L-based combo catalog (greedy beam search) for a given horizon."""
    from ai_journal.db import get_journal_conn, ensure_schema
    ensure_schema()
    c = get_journal_conn()
    try:
        # Ensure table exists (may be missing if discover never ran)
        c.execute("""CREATE TABLE IF NOT EXISTS combo_catalog_pnl (
            combo_id VARCHAR, predicates VARCHAR, size INTEGER, horizon INTEGER,
            n_train BIGINT, train_avg_clip DOUBLE, train_win DOUBLE,
            base_avg_train DOUBLE, train_edge_avg DOUBLE,
            n_oos BIGINT, oos_avg_clip DOUBLE, oos_win DOUBLE,
            base_avg_oos DOUBLE, oos_edge_avg DOUBLE,
            p_value DOUBLE, bonferroni_p DOUBLE, status VARCHAR,
            grown_from VARCHAR, discovered_at TIMESTAMP,
            PRIMARY KEY (combo_id, horizon))""")
        where = ["horizon = ?"]; params = [horizon]
        if status:
            where.append("status = ?"); params.append(status)
        rows = c.execute(f"""
            SELECT combo_id, predicates, size, horizon,
                   n_train, round(train_avg_clip,3) train_avg, round(train_win*100,1) train_win,
                   round(train_edge_avg,3) train_edge,
                   n_oos, round(oos_avg_clip,3) oos_avg, round(oos_win*100,1) oos_win,
                   round(oos_edge_avg,3) oos_edge, p_value, bonferroni_p,
                   status, grown_from
            FROM combo_catalog_pnl WHERE {' AND '.join(where)}
            ORDER BY (CASE WHEN status='passed' THEN 0 ELSE 1 END), oos_edge_avg DESC NULLS LAST
            LIMIT ?""", params + [limit]).fetchall()
        cols = ["combo_id", "predicates", "size", "horizon", "n_train", "train_avg",
                "train_win", "train_edge", "n_oos", "oos_avg", "oos_win", "oos_edge",
                "p_value", "bonferroni_p", "status", "grown_from"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows),
                "horizon": horizon}
    finally:
        c.close()


@router.get("/combo/active")
def combo_active(predicates: str, as_of: str | None = None, limit: int = 100):
    """Tickers that satisfy a combo's full predicate set on the latest bar
    (or `as_of`). Atoms come as a comma-separated string of predicate names —
    we look them up in the ATOMS catalog and AND their SQLs.
    Joined with ticker_meta for sector/mcap/name."""
    import duckdb
    from ai_journal.db import ANALYTICS_DB_PATH, get_journal_conn, ensure_schema
    from .combo_lab.enumerate import ATOMS
    ensure_schema()
    atoms = [a.strip() for a in predicates.split(",") if a.strip()]
    if not atoms:
        return {"rows": [], "predicates": []}
    unknown = [a for a in atoms if a not in ATOMS]
    if unknown:
        raise HTTPException(400, f"unknown predicate(s): {unknown}")
    sql_cond = " AND ".join(f"({ATOMS[a]})" for a in atoms)
    # meta first
    j = get_journal_conn()
    try:
        meta = {r[0]: {"name": r[1], "sector": r[2], "mcap_bucket": r[3],
                       "market_cap": r[4]}
                for r in j.execute("SELECT ticker,name,sector,mcap_bucket,market_cap FROM ticker_meta").fetchall()}
    finally:
        j.close()
    a = duckdb.connect(ANALYTICS_DB_PATH, read_only=True)
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute(f"""
            SELECT ticker, universe, close, change_pct, rsi_14, atr_14,
                   prebreak_v3, prebreak_v3_reasons, t_sig, z_sig, rtb_phase, vol_bucket
            FROM bars WHERE date = ? AND ({sql_cond})
            ORDER BY prebreak_v3 DESC NULLS LAST LIMIT ?""", [as_of, limit]).fetchdf()
    finally:
        a.close()
    out = []
    seen = set()
    for _, r in rows.iterrows():
        tk = r["ticker"]
        if tk in seen: continue
        seen.add(tk)
        m = meta.get(tk, {})
        out.append({
            "ticker": tk, "universe": r["universe"],
            "name": m.get("name") or "", "sector": m.get("sector") or "",
            "mcap_bucket": m.get("mcap_bucket") or "unknown",
            "close": float(r["close"]) if r["close"] is not None else None,
            "change_pct": float(r["change_pct"]) if r["change_pct"] is not None else None,
            "rsi": int(round(float(r["rsi_14"]))) if r["rsi_14"] is not None else None,
            "v3": int(r["prebreak_v3"]) if r["prebreak_v3"] is not None else 0,
            "reasons": r["prebreak_v3_reasons"] or "",
            "tz": r["t_sig"] or r["z_sig"] or "",
            "phase": r["rtb_phase"] or "", "vol": r["vol_bucket"] or "",
        })
    return {"as_of": as_of, "predicates": atoms, "rows": out, "count": len(out)}


@router.get("/combo/pnl-summary")
def combo_pnl_summary():
    """Summary across all horizons: how many combos passed at each depth & H."""
    from ai_journal.db import get_journal_conn
    c = get_journal_conn()
    try:
        rows = c.execute("""SELECT horizon, size, status, count(*) AS n
                            FROM combo_catalog_pnl GROUP BY 1,2,3 ORDER BY 1,2,3""").fetchall()
    finally:
        c.close()
    return {"breakdown": [dict(zip(["horizon", "size", "status", "n"], r)) for r in rows]}


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
