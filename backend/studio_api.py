"""
studio_api.py — FastAPI router for Analytic Studio endpoints.

Mount in main.py:
    from studio_api import router as studio_router
    app.include_router(studio_router)
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Optional

import math
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from studio.db import ensure_schema, get_stats, STUDIO_DB_PATH, UNIVERSE_PRIORITY_SQL
from studio.importer import import_all, UNIVERSE_CSV_MAP, PROGRESS_FILE
from studio.event_detector import (
    detect_events, get_events_summary, list_events,
    EventFilter, PRESET_EVENTS,
)
from studio.pattern_miner import mine_patterns
from studio.miss_analyzer import analyze_misses, analyze_false_positives
from studio.scoring_lab import define_score, backtest_score, list_scores
from studio.signal_stats import (
    query_combo, rank_signals, get_available_filters,
    query_tz_sequence, query_confluence_sequence,
    query_exact_sequence,
    SORT_METRICS,
)
from studio.enricher import enrich_universe as _enrich_universe, get_progress as _enrich_progress
# NOTE: the legacy studio.incremental module is deprecated and intentionally NOT
# imported here — the live path is studio.incremental_delta (see _run_incremental).
# Keeping it unimported prevents accidental use of the old 40-column write path.
from studio.edge_scanner import (
    run_edge_scan as _run_edge_scan,
    get_progress as _edge_progress,
    get_cached_results as _edge_cached_results,
)
from studio.ultra_db_scan import run_ultra_db_scan as _run_ultra_db_scan
from studio.acc_exit_miner import (
    mine_lifts as _mine_lifts,
    get_progress as _mine_progress,
    get_cached_lifts as _mine_cached,
)
from studio.per_ticker_calibration import (
    calibrate_per_ticker as _calibrate_per_ticker,
    get_progress as _calib_progress,
    get_ticker_lifts as _ticker_lifts,
)
from studio.bar_describer import (
    get_bar_description, get_pre_narrative, generate_bar_description
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/studio", tags=["studio"])


def _safe_records(df) -> list[dict]:
    """Convert DataFrame to list of dicts, replacing NaN/Inf with None."""
    import numpy as np
    records = []
    for rec in df.to_dict("records"):
        clean = {}
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                clean[k] = None
            elif isinstance(v, (np.floating,)):
                fv = float(v)
                clean[k] = None if (math.isnan(fv) or math.isinf(fv)) else fv
            elif hasattr(v, 'item'):   # numpy int/bool scalars
                clean[k] = v.item()
            else:
                clean[k] = v
        records.append(clean)
    return records

# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────────────────────

class ImportRequest(BaseModel):
    universes: list[str] = Field(default=["sp500", "nasdaq"])
    force: bool = False   # override the regression guard (full rebuild)

class EventDetectRequest(BaseModel):
    event_type: str = "BULL_2X_60D"
    universes: list[str] = ["sp500", "nasdaq"]
    date_from: Optional[str] = None
    date_to:   Optional[str] = None
    # Custom overrides
    custom_name:   Optional[str]   = None
    mfe_col:       Optional[str]   = None
    mfe_min:       Optional[float] = None
    mae_col:       Optional[str]   = None
    mae_max:       Optional[float] = None
    fwd_col:       Optional[str]   = None
    fwd_min:       Optional[float] = None
    fwd_max:       Optional[float] = None
    turbo_min:     Optional[float] = None
    turbo_max:     Optional[float] = None
    price_min:     Optional[float] = None
    price_max:     Optional[float] = None
    volume_min:    Optional[int]   = None
    clear_existing: bool = True

class PatternMineRequest(BaseModel):
    event_type:    str         = "BULL_2X_60D"
    pre_window:    int         = 20
    min_lift:      float       = 2.0
    min_n:         int         = 15
    combo_depth:   int         = 3
    include_seqs:  bool        = True
    universes:     Optional[list[str]] = None

class MissRequest(BaseModel):
    event_type: str         = "BULL_2X_60D"
    turbo_max:  float       = 15.0
    universes:  Optional[list[str]] = None
    pre_window: int         = 20
    top_n:      int         = 20

class FPRequest(BaseModel):
    turbo_min:  float       = 50.0
    fwd_max:    float       = -10.0
    fwd_col:    str         = "fwd_10d"
    universes:  Optional[list[str]] = None
    pre_window: int         = 5
    top_n:      int         = 20

class DefineScoreRequest(BaseModel):
    name:         str
    weights:      dict[str, float]
    hard_filters: Optional[list[dict]] = None
    threshold:    int = 45

class BacktestRequest(BaseModel):
    score_id:   str
    event_type: str               = "BULL_2X_60D"
    date_from:  Optional[str]     = None
    date_to:    Optional[str]     = None
    universes:  Optional[list[str]] = None

class TZSequenceRequest(BaseModel):
    sequence:  list[str | None] = []   # signal names or null for wildcard
    universe:  Optional[str]    = None
    regime:    Optional[str]    = None
    min_n:     int              = 5

class ConfluenceSequenceRequest(BaseModel):
    bars:      list[str | None] = []   # T/Z signal per bar (None = wildcard); bars[0]=oldest
    universe:  Optional[str]    = None

class SignalStatsRequest(BaseModel):
    signals:    list[str]         = []
    universe:   Optional[str]     = None
    regime:     Optional[str]     = None
    date_from:  Optional[str]     = None
    date_to:    Optional[str]     = None
    turbo_min:  Optional[float]   = None
    turbo_max:  Optional[float]   = None
    min_n:      int               = 5

class SignalRankRequest(BaseModel):
    universe:   Optional[str]     = None
    regime:     Optional[str]     = None
    date_from:  Optional[str]     = None
    date_to:    Optional[str]     = None
    turbo_min:  Optional[float]   = None
    turbo_max:  Optional[float]   = None
    sort_by:    str               = "win_5d"
    min_n:      int               = 30
    top_n:      int               = 60


# ─────────────────────────────────────────────────────────────────────────────
# Import state (in-memory for current import run)
# ─────────────────────────────────────────────────────────────────────────────
_import_thread: threading.Thread | None = None
_import_results: list = []
_import_running = False


def _run_import(universes: list[str], force: bool = False) -> None:
    global _import_results, _import_running
    _import_running = True
    try:
        _import_results = import_all(universes, force=force)
    finally:
        _import_running = False


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/import")
def trigger_import(req: ImportRequest, background_tasks: BackgroundTasks):
    """Start importing bulk_export CSVs into DuckDB. Runs in background."""
    global _import_thread, _import_running, _import_results
    if _import_running:
        return {"status": "already_running", "message": "Import already in progress"}

    # Check CSVs exist
    missing = []
    for univ in req.universes:
        path = UNIVERSE_CSV_MAP.get(univ, "")
        if not path or not os.path.exists(path):
            missing.append({"universe": univ, "path": path})
    if missing:
        raise HTTPException(400, detail={"message": "Some CSV files not found", "missing": missing})

    ensure_schema()
    _import_results = []
    _import_thread = threading.Thread(
        target=_run_import, args=(req.universes, req.force), daemon=True
    )
    _import_thread.start()
    return {
        "status": "started",
        "universes": req.universes,
        "force": req.force,
        "progress_file": PROGRESS_FILE,
    }


@router.get("/import/status")
def import_status():
    """Poll import progress."""
    running = _import_running
    prog = {}
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                prog = json.load(f)
        except Exception:
            pass
    return {
        "running":  running,
        "results":  _import_results,
        "progress": prog,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bar Enrichment — compute & store derived columns (suffix, body/wick, gap/range,
# line5, Williams pivots 3-3 + 5-5, HL/HH outcomes, L digit flags).
# ─────────────────────────────────────────────────────────────────────────────
_enrich_running = False
_enrich_results: dict = {}


class EnrichRequest(BaseModel):
    universe:    str = "sp500"
    max_workers: int = 4


def _run_enrich(universe: str, max_workers: int) -> None:
    global _enrich_running, _enrich_results
    _enrich_running = True
    try:
        from studio.db import ensure_schema
        ensure_schema()      # safety — make sure new columns exist
        result = _enrich_universe(universe=universe, max_workers=max_workers)
        _enrich_results = result
    except Exception as exc:
        log.exception("enrich failed")
        _enrich_results = {"error": str(exc)}
    finally:
        _enrich_running = False


@router.post("/enrich")
def trigger_enrich(req: EnrichRequest, background_tasks: BackgroundTasks):
    """Trigger bar enrichment in background. Idempotent — safe to re-run."""
    global _enrich_running
    if _enrich_running:
        return {"status": "already_running"}
    background_tasks.add_task(_run_enrich, req.universe, req.max_workers)
    return {"status": "started", "universe": req.universe, "workers": req.max_workers}


@router.get("/enrich/status")
def enrich_status():
    """Poll enrichment progress."""
    return {
        "running":  _enrich_running,
        "results":  _enrich_results,
        "progress": _enrich_progress(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Incremental Daily Refresh — add latest bars to existing tickers without
# re-scanning history. Designed for daily run after market close.
# ─────────────────────────────────────────────────────────────────────────────
_incremental_running = False
_incremental_results: dict = {}


class IncrementalRequest(BaseModel):
    universes: list[str] = ["sp500", "nasdaq"]


def _run_incremental(universes: list[str]) -> None:
    """Run the new delta-append refresh (studio.incremental_delta).

    Replaces the legacy `_incremental_refresh` path which inserted rows with
    id=NULL and missed Pine score columns. The new path matches bulk_export
    output 100% on Pine cols (validated SP500 sample).
    """
    global _incremental_running, _incremental_results
    _incremental_running = True
    try:
        from studio.db import ensure_schema
        from studio.incremental_delta import incremental_delta_refresh
        ensure_schema()
        result = incremental_delta_refresh(universes=universes)
        # Backfill forward-return labels for the trailing bars whose future has
        # now arrived (otherwise they keep NULL fwd_* — a silent backtest gap).
        try:
            from studio.backfill_fwd import backfill_forward_returns
            result["forward_backfill"] = backfill_forward_returns()
        except Exception:
            log.exception("forward-return backfill failed (non-fatal)")
            result["forward_backfill"] = {"error": "backfill failed — see logs"}
        _incremental_results = result
    except Exception as exc:
        log.exception("incremental delta refresh failed")
        _incremental_results = {"error": str(exc)}
    finally:
        _incremental_running = False


@router.post("/incremental-update")
def trigger_incremental(req: IncrementalRequest, background_tasks: BackgroundTasks):
    """Trigger incremental delta refresh — fetches the latest bars for every
    ticker and appends them to the unified `bars` table (no separate DB)."""
    global _incremental_running
    if _incremental_running:
        return {"status": "already_running"}
    background_tasks.add_task(_run_incremental, req.universes)
    return {"status": "started", "universes": req.universes}


@router.get("/seq-lab")
def seq_lab_endpoint(
    universe:  Optional[str] = Query(None),
    n_bars:    int  = Query(4,   ge=2, le=6),
    mode:      str  = Query("color"),
    horizon:   str  = Query("fwd_1d"),
    min_occ:   int  = Query(500, ge=20, le=200000),
    wyc_phase: Optional[str] = Query(None),
    prefix:    Optional[str] = Query(None),
    sort:      str  = Query("win"),
    limit:     int  = Query(25, ge=1, le=100),
    by_phase:  bool = Query(False),
    evaluate:  bool = Query(False),
    cost:      float = Query(0.5, ge=0.0, le=10.0),
):
    """TZ Sequence Lab — rank N-bar T/Z sequences by forward outcome vs baseline.

    When evaluate=true, each row also gets a `verdict` (backtest-expert skill):
    Deploy / Refine / Abandon after the significance, Bonferroni (vs n_candidates)
    and net-edge-after-`cost` gates — so a mirage (significant only because n is
    huge, but below cost) is flagged in the UI."""
    from studio.seq_lab import seq_lab
    try:
        res = seq_lab(
            universe=universe, n_bars=n_bars, mode=mode, horizon=horizon,
            min_occ=min_occ, wyc_phase=wyc_phase, prefix=prefix, sort=sort,
            limit=limit, by_phase=by_phase,
        )
        if evaluate:
            try:
                from studio.eval_sequence import annotate_seq_lab
                res = annotate_seq_lab(res, cost_per_trade_pct=cost)
            except Exception:
                log.exception("seq-lab verdict annotation failed (rows returned unannotated)")
        return res
    except Exception as e:
        log.exception("seq-lab failed")
        raise HTTPException(500, detail=str(e))


@router.get("/seq-backtest")
def seq_backtest_endpoint(
    signals:    str = Query(...),                 # comma-separated signal flag columns
    universe:   Optional[str] = Query(None),
    wyc_phase:  Optional[str] = Query(None),
    target_pct: float = Query(10.0, ge=0.5, le=200),
    stop_pct:   float = Query(5.0,  ge=0.5, le=100),
    max_hold:   int   = Query(20,   ge=1,  le=120),
    side:       str   = Query("long"),
):
    """Realised backtest of an entry condition (entry next open, target/stop/time exit)."""
    from studio.seq_backtest import backtest
    flags = [s for s in (signals or "").split(",") if s.strip()]
    try:
        return backtest(signals=flags, universe=universe, wyc_phase=wyc_phase,
                        target_pct=target_pct, stop_pct=stop_pct, max_hold=max_hold, side=side)
    except Exception as e:
        log.exception("seq-backtest failed")
        raise HTTPException(500, detail=str(e))


@router.get("/playbook")
def playbook_endpoint(
    universe:   str   = Query("sp500", description="single universe: sp500 | nasdaq | russell2k"),
    min_trades: int   = Query(30,      ge=1,   le=100000),
    min_price:  float = Query(5.0,     ge=0),
    min_volume: int   = Query(100_000, ge=0),
    max_live:   int   = Query(40,      ge=1,   le=500),
):
    """Build the Playbook — run every predefined setup through the realised-backtest
    gate (expectancy>0 & PF>1 & positive in BOTH halves & enough trades) and attach
    today's live tickers for the survivors. One universe per call."""
    from studio.playbook import build_playbook
    try:
        return _sanitize_for_json(build_playbook(
            universe=universe, min_trades=min_trades,
            min_price=min_price, min_volume=min_volume, max_live=max_live,
        ))
    except Exception as e:
        log.exception("playbook failed")
        raise HTTPException(500, detail=str(e))


@router.post("/backfill-forward")
def trigger_backfill_forward(lookback_days: int = Query(150, ge=10, le=400)):
    """Recompute forward-return labels (fwd_*/mfe_*/mae_*/hit_*/drop_*) for recent
    bars whose future has since arrived but were imported with NULL labels.
    Purely additive — only previously-NULL rows are filled; existing values untouched."""
    from studio.backfill_fwd import backfill_forward_returns
    try:
        return backfill_forward_returns(lookback_days=lookback_days)
    except Exception as e:
        log.exception("backfill-forward failed")
        raise HTTPException(500, detail=str(e))


@router.get("/incremental-update/status")
def incremental_status():
    """Poll incremental refresh progress.

    Reads from the new delta module's progress file when present, falls
    back to the legacy module otherwise.
    """
    try:
        from studio.incremental_delta import get_progress as _delta_progress
        progress = _delta_progress()
        if progress.get("ts"):
            return {
                "running":  _incremental_running,
                "results":  _incremental_results,
                "progress": progress,
            }
    except Exception:
        pass
    # Fallback (before any run has produced progress data)
    try:
        from studio.incremental_delta import get_progress as _delta_progress0
        _prog = _delta_progress0()
    except Exception:
        _prog = {}
    return {
        "running":  _incremental_running,
        "results":  _incremental_results,
        "progress": _prog,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Edge Scanner — "Today's Best Setups" across universe
# ─────────────────────────────────────────────────────────────────────────────
_edge_running = False
_edge_results: dict = {}


class EdgeScanRequest(BaseModel):
    universes:    list[str] = ["sp500", "nasdaq"]
    n_bars:       int       = 3
    min_matches:  int       = 20
    pivot_lr:     int       = 3
    min_price:    float     = 15.0
    min_volume:   int       = 100_000
    strictness:   Optional[dict] = None


def _run_edge(req: EdgeScanRequest) -> None:
    global _edge_running, _edge_results
    _edge_running = True
    try:
        result = _run_edge_scan(
            universes   = req.universes,
            strictness  = req.strictness,
            n_bars      = req.n_bars,
            min_matches = req.min_matches,
            pivot_lr    = req.pivot_lr,
            min_price   = req.min_price,
            min_volume  = req.min_volume,
        )
        _edge_results = {
            "scanned_at":     result["scanned_at"],
            "qualifying":     result["qualifying"],
            "total_tickers":  result["total_tickers"],
            "duration_sec":   result["duration_sec"],
            "cached_seqs":    result["cached_seqs"],
        }
    except Exception as exc:
        log.exception("edge scan failed")
        _edge_results = {"error": str(exc)}
    finally:
        _edge_running = False


@router.post("/edge-scan/trigger")
def trigger_edge_scan(req: EdgeScanRequest, background_tasks: BackgroundTasks):
    """Kick off the edge scanner. Results saved to disk + cached for /results."""
    global _edge_running
    if _edge_running:
        return {"status": "already_running"}
    background_tasks.add_task(_run_edge, req)
    return {"status": "started", "universes": req.universes}


@router.get("/edge-scan/status")
def edge_scan_status():
    return {
        "running":  _edge_running,
        "summary":  _edge_results,
        "progress": _edge_progress(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ULTRA-from-DB Scan — DB-backed replacement for the slow live scan.
# Returns same row shape as /api/ultra-scan/results (compatible with the
# existing UltraScanPanel client-side filters).
# ─────────────────────────────────────────────────────────────────────────────
class UltraDBScanRequest(BaseModel):
    universes:    list[str]       = ["sp500", "nasdaq"]
    min_price:    Optional[float] = None
    min_volume:   Optional[int]   = None
    age_signals:  Optional[list[str]] = None  # which sigs to compute sig_ages JSON for
    age_lookback: int             = 20


# ─────────────────────────────────────────────────────────────────────────────
# Accumulation Exit (Breakout Hunter) — lift mining + Exit Hunter ranked list
# ─────────────────────────────────────────────────────────────────────────────
_acc_mine_running = False
_acc_mine_results: dict = {}


def _run_acc_mine() -> None:
    global _acc_mine_running, _acc_mine_results
    _acc_mine_running = True
    try:
        _acc_mine_results = _mine_lifts(min_samples=100)
    except Exception as exc:
        log.exception("acc_exit lift mining failed")
        _acc_mine_results = {"error": str(exc)}
    finally:
        _acc_mine_running = False


@router.post("/acc-exit/mine-lifts")
def trigger_acc_mine(background_tasks: BackgroundTasks):
    """Kick off ACC_TR exit lift mining job. Outputs to acc_exit_lift_v1 table."""
    global _acc_mine_running
    if _acc_mine_running:
        return {"status": "already_running"}
    background_tasks.add_task(_run_acc_mine)
    return {"status": "started"}


@router.get("/acc-exit/status")
def acc_mine_status():
    return {
        "running":  _acc_mine_running,
        "summary":  _acc_mine_results,
        "progress": _mine_progress(),
    }


@router.get("/acc-exit/lifts")
def acc_mine_lifts():
    """Get the cached mined lift table."""
    rows = _mine_cached()
    return {"rows": rows, "count": len(rows)}


# Per-ticker calibration job
_calib_running = False
_calib_results: dict = {}


def _run_calib() -> None:
    global _calib_running, _calib_results
    _calib_running = True
    try:
        _calib_results = _calibrate_per_ticker()
        # Reset enricher's AES caches so next enrich picks up new ticker lifts
        try:
            from studio.enricher import reset_aes_caches
            reset_aes_caches()
        except Exception:
            pass
    except Exception as exc:
        log.exception("per-ticker calibration failed")
        _calib_results = {"error": str(exc)}
    finally:
        _calib_running = False


@router.post("/acc-exit/calibrate-per-ticker")
def trigger_calib(background_tasks: BackgroundTasks):
    """Run per-ticker AES calibration. Stores results in ticker_signal_lift_v1."""
    global _calib_running
    if _calib_running:
        return {"status": "already_running"}
    background_tasks.add_task(_run_calib)
    return {"status": "started"}


@router.get("/acc-exit/calibrate-status")
def calib_status():
    return {
        "running":  _calib_running,
        "summary":  _calib_results,
        "progress": _calib_progress(),
    }


@router.get("/acc-exit/ticker-lifts/{ticker}")
def get_ticker_lifts_api(ticker: str, universe: Optional[str] = Query(None)):
    """Get per-ticker calibrated lifts for a specific ticker."""
    rows = _ticker_lifts(ticker, universe)
    return {"ticker": ticker, "universe": universe, "rows": rows, "count": len(rows)}


@router.get("/acc-exit/hunter")
def acc_exit_hunter(
    universe:     Optional[str] = Query(None),
    min_aes:      float = Query(10.0),
    min_price:    float = Query(5.0),
    min_volume:   int   = Query(50_000),
    stage:        Optional[str] = Query(None, description="ACC | READY | PRIME★★ | SPRING★ | SOS★ | MARKUP — filter by aes_stage"),
    pre_bo_only:  bool  = Query(True, description="Only show ACC_TR / SPRING phase bars (true pre-BO candidates)"),
    limit:        int   = Query(100, ge=1, le=500),
):
    """Rank tickers by AES (accumulation-exit score). Returns latest bar per ticker
    in (typically) ACC_TR phase, sorted by AES descending.
    """
    from studio.db import get_conn
    conn = get_conn(read_only=True)
    try:
        where_clauses = []
        params: list = []
        if universe:
            where_clauses.append("universe = ?"); params.append(universe)
        # Latest bar per (ticker, universe) — using ROW_NUMBER window
        sql = """
            WITH ranked AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, universe ORDER BY date DESC) AS rn
              FROM bars
            )
            SELECT ticker, universe, date, close, volume, avg_vol_20d,
                   wyc_phase, aes_stage, aes_score, aes_leading, aes_trend_5d,
                   acc_exit_class, acc_exit_in_n,
                   pb_lvbo, wyc_spring, ad_fresh, ad_cluster,
                   prebreak_prime, prebreak_ready, prebreak_watch,
                   pb_wvf_confirm, pb_stop_cause, wyc_sos,
                   t_sig, z_sig, l_sig, turbo_score, final_bull_score,
                   rsi_14, cci_20, change_pct, rtb_phase, profile_category, sector,
                   composite_full_suffix, bar_body_wick, bar_gap_range, bar_line5
            FROM ranked
            WHERE rn = 1
              AND close >= ?
              AND avg_vol_20d >= ?
              AND aes_score >= ?
        """
        params2 = [min_price, min_volume, min_aes] + params
        sql += (" AND universe = ?") if universe else ""
        if pre_bo_only:
            # Wide pre-BO candidates: ACC, springs, neutrals, plus RECENT MARKUP
            # (just broke out in last 3 bars — retest opportunity)
            sql += """ AND (
                wyc_phase IN ('ACC_TR', 'SPRING', 'SOS', 'NEUTRAL')
                OR (wyc_phase = 'MARKUP' AND acc_exit_class IN ('BO_NOW'))
            )"""
        if stage:
            sql += " AND aes_stage = ?"
            params2.append(stage)
        sql += f" ORDER BY aes_score DESC, aes_leading DESC, turbo_score DESC LIMIT {limit}"
        df = conn.execute(sql, params2).fetchdf()

        # Sanitize NaN/Inf
        records = []
        for _, row in df.iterrows():
            d = {}
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    d[k] = None
                else:
                    d[k] = v if not isinstance(v, pd.Timestamp) else str(v.date())
            records.append(d)

        return _sanitize_for_json({
            "rows":  records,
            "count": len(records),
            "filters": {
                "universe": universe, "min_aes": min_aes, "min_price": min_price,
                "min_volume": min_volume, "stage": stage, "limit": limit,
            },
        })
    finally:
        conn.close()


@router.post("/ultra-from-db")
def ultra_from_db(req: UltraDBScanRequest):
    """Run DB-backed ULTRA scan. Returns ~1-2 sec for 3700+ tickers."""
    try:
        result = _run_ultra_db_scan(
            universes    = req.universes,
            min_price    = req.min_price,
            min_volume   = req.min_volume,
            age_signals  = req.age_signals,
            age_lookback = req.age_lookback,
        )
        return _sanitize_for_json(result)
    except Exception as exc:
        log.exception("ultra_from_db failed")
        raise HTTPException(500, detail=str(exc))


def _sanitize_for_json(obj):
    """Replace NaN / Inf floats with None recursively (FastAPI strict JSON)."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


@router.get("/edge-scan/results")
def edge_scan_results(
    tab:        str  = Query("top_buys", description="top_buys | top_sells | by_quality | all"),
    limit:      int  = Query(50, ge=1, le=2000),
    universe:   Optional[str] = Query(None),
    min_n:      int  = Query(0),
    min_pqs:    Optional[float] = Query(None),
):
    """Fetch cached edge scan results, optionally filtered."""
    cached = _edge_cached_results()
    if not cached:
        return {"error": "No edge scan results yet — run /edge-scan/trigger first"}

    key_map = {
        "top_buys":   "top_buys",
        "top_sells":  "top_sells",
        "by_quality": "by_quality",
        "all":        "all_results",
    }
    src = cached.get(key_map.get(tab, "top_buys"), [])

    def keep(r):
        if universe and r.get("universe") != universe:    return False
        if min_n and (r.get("matches") or 0) < min_n:     return False
        if min_pqs is not None and (r.get("pqs") or 0) < min_pqs: return False
        return True

    filtered = [r for r in src if keep(r)][:limit]

    payload = {
        "scanned_at":    cached.get("scanned_at"),
        "universes":     cached.get("universes"),
        "strictness":    cached.get("strictness"),
        "n_bars":        cached.get("n_bars"),
        "pivot_lr":      cached.get("pivot_lr"),
        "qualifying":    cached.get("qualifying"),
        "total_tickers": cached.get("total_tickers"),
        "last_data_date": cached.get("last_data_date"),
        "tab":           tab,
        "rows":          filtered,
        "total_rows":    len(src),
    }
    return _sanitize_for_json(payload)


@router.get("/stats")
def stats():
    """Overall DB stats: row count, ticker count, date range, events."""
    try:
        ensure_schema()
        return get_stats()
    except Exception as e:
        return {"error": str(e), "db_path": STUDIO_DB_PATH}


@router.get("/presets")
def get_presets():
    """List all preset event types."""
    return [{"key": k, **v} for k, v in PRESET_EVENTS.items()]


# ─────────────────────────────────────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/events/detect")
def events_detect(req: EventDetectRequest):
    """Detect events matching the given criteria. Writes to events table."""
    f = EventFilter(
        event_type   = req.event_type,
        universes    = req.universes,
        date_from    = req.date_from,
        date_to      = req.date_to,
        custom_name  = req.custom_name,
        mfe_col      = req.mfe_col,
        mfe_min      = req.mfe_min,
        mae_col      = req.mae_col,
        mae_max      = req.mae_max,
        fwd_col      = req.fwd_col,
        fwd_min      = req.fwd_min,
        fwd_max      = req.fwd_max,
        turbo_min    = req.turbo_min,
        turbo_max    = req.turbo_max,
        price_min    = req.price_min,
        price_max    = req.price_max,
        volume_min   = req.volume_min,
    )
    return detect_events(f, clear_existing=req.clear_existing)


@router.get("/events/summary")
def events_summary():
    return get_events_summary()


@router.get("/events/list")
def events_list(
    event_type: Optional[str] = Query(None),
    universe:   Optional[str] = Query(None),
    limit:      int = Query(100, ge=1, le=1000),
    offset:     int = Query(0, ge=0),
):
    return list_events(event_type, universe, limit, offset)


# ─────────────────────────────────────────────────────────────────────────────
# Pattern Mining
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/patterns/mine")
def patterns_mine(req: PatternMineRequest):
    """Mine pre-event signal patterns with lift scores."""
    return mine_patterns(
        event_type   = req.event_type,
        pre_window   = req.pre_window,
        min_lift     = req.min_lift,
        min_n        = req.min_n,
        combo_depth  = req.combo_depth,
        include_seqs = req.include_seqs,
        universes    = req.universes,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Miss & False Positive Analysis
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/analysis/miss")
def analysis_miss(req: MissRequest):
    """Analyze missed opportunities (big move, no signal)."""
    return analyze_misses(
        event_type = req.event_type,
        turbo_max  = req.turbo_max,
        universes  = req.universes,
        pre_window = req.pre_window,
        top_n      = req.top_n,
    )


@router.post("/analysis/false-pos")
def analysis_fp(req: FPRequest):
    """Analyze false positives (signal fired, price dropped)."""
    return analyze_false_positives(
        turbo_min  = req.turbo_min,
        fwd_max    = req.fwd_max,
        fwd_col    = req.fwd_col,
        universes  = req.universes,
        pre_window = req.pre_window,
        top_n      = req.top_n,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scoring Lab
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/scoring-lab/define")
def scoring_lab_define(req: DefineScoreRequest):
    """Save a custom score definition."""
    score_id = define_score(
        name         = req.name,
        weights      = req.weights,
        hard_filters = req.hard_filters,
        threshold    = req.threshold,
    )
    return {"score_id": score_id, "name": req.name}


@router.get("/scoring-lab/list")
def scoring_lab_list():
    return list_scores()


@router.post("/scoring-lab/backtest")
def scoring_lab_backtest(req: BacktestRequest):
    """Run backtest for a custom score. Returns comparison vs turbo_score."""
    return backtest_score(
        score_id   = req.score_id,
        event_type = req.event_type,
        date_from  = req.date_from,
        date_to    = req.date_to,
        universes  = req.universes,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Bar Descriptions & Narratives
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/bars/{ticker}")
def get_ticker_bars(
    ticker: str,
    limit:  int = Query(120, ge=1, le=2500),
    offset: int = Query(0, ge=0),
):
    """Get all bars for a ticker with forward returns + full 6-line chart codes
    (TZ / L / suffix / body-wick / gap-range / line5) straight from the DB so a
    Studio chart can render exactly what the Sequence Builder matches."""
    from studio.db import get_conn
    conn = get_conn(read_only=True)
    try:
        rows = conn.execute(
            f"""SELECT ticker, date, open, high, low, close, volume,
                      turbo_score, vol_bucket, gog_tier, swing_type,
                      fwd_1d, fwd_5d, fwd_10d, fwd_20d, fwd_60d,
                      mfe_20d, mfe_60d, mae_20d,
                      hit_2x_60d, hit_50pct_20d, drop_20pct_10d,
                      all_signals_text, t_sig, z_sig, l_sig,
                      composite_full_suffix, full_suffix,
                      bar_body_wick, bar_gap_range, bar_line5,
                      swing_type_3, is_pivot_low_3, is_pivot_high_3
               FROM bars WHERE ticker = ?
               -- a ticker can live in >1 universe (e.g. RGTI/CYCU in nasdaq AND
               -- russell2k) with DIVERGENT bars on the same date. Keep ONE row
               -- per date, choosing the canonical universe by priority
               -- (sp500 > nasdaq > russell2k) so the chart matches the scan.
               QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
               ORDER BY date DESC LIMIT ? OFFSET ?""",
            [ticker.upper(), limit, offset],
        ).fetchdf()
        return _safe_records(rows)
    finally:
        conn.close()


@router.get("/narrative/{ticker}/{date}/bar")
def bar_narrative(ticker: str, date: str):
    """Get description for one bar."""
    desc = get_bar_description(ticker.upper(), date)
    if desc is None:
        raise HTTPException(404, detail="Bar not found")
    return {"ticker": ticker, "date": date, "description": desc}


@router.get("/narrative/{ticker}/{date}/pre")
def pre_move_narrative(
    ticker: str,
    date:   str,
    window: int = Query(20, ge=5, le=60),
):
    """Get pre-move narrative for N bars before event date."""
    narrative = get_pre_narrative(ticker.upper(), date, window)
    return {"ticker": ticker, "event_date": date, "pre_window": window, "narrative": narrative}


# ─────────────────────────────────────────────────────────────────────────────
# Signal Stats
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/signal-stats/sequence")
def signal_stats_sequence(req: TZSequenceRequest):
    """
    Query Studio DB for next-bar T/Z distribution after a given N-bar sequence.
    Null elements in sequence = wildcard (any signal at that position).
    """
    try:
        return query_tz_sequence(
            sequence  = req.sequence,
            universe  = req.universe,
            regime    = req.regime,
            min_n     = req.min_n,
        )
    except Exception as e:
        log.exception("signal_stats_sequence failed")
        raise HTTPException(500, detail=str(e))


class ExactSequenceRequest(BaseModel):
    bars:       list[dict]          = []   # [{tz, suffix, body_wick, gap_range, line5}, ...]
    universe:   Optional[str]       = None
    strictness: Optional[dict]      = None
    pivot_lr:   int                 = 3


@router.post("/exact-sequence")
def exact_sequence(req: ExactSequenceRequest):
    """
    Exact 5-line N-bar sequence match against enriched Studio DB.
    Returns match count + HL/HH outcome statistics.
    """
    try:
        return query_exact_sequence(
            bars       = req.bars,
            universe   = req.universe,
            strictness = req.strictness,
            pivot_lr   = req.pivot_lr,
        )
    except Exception as e:
        log.exception("exact_sequence failed")
        raise HTTPException(500, detail=str(e))


@router.post("/confluence-sequence")
def confluence_sequence(req: ConfluenceSequenceRequest):
    """
    For an N-bar T/Z sequence, return a confluence funnel:
    how many times the sequence appears with progressively more signal requirements
    (T/Z only → +WLNBB → +Wick → +GOG → +PARA → +VABS).
    """
    try:
        return query_confluence_sequence(
            bars      = req.bars,
            universe  = req.universe,
        )
    except Exception as e:
        log.exception("confluence_sequence failed")
        raise HTTPException(500, detail=str(e))


@router.get("/signal-stats/filters")
def signal_stats_filters():
    """Return available filter values (universes, regimes, date range)."""
    try:
        return get_available_filters()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/signal-stats/query")
def signal_stats_query(req: SignalStatsRequest):
    """
    Compute outcome statistics for a signal combo.
    Returns combo stats + baseline + regime breakdown.
    """
    try:
        return query_combo(
            signals   = req.signals,
            universe  = req.universe,
            regime    = req.regime,
            date_from = req.date_from,
            date_to   = req.date_to,
            turbo_min = req.turbo_min,
            turbo_max = req.turbo_max,
            min_n     = req.min_n,
        )
    except Exception as e:
        log.exception("signal_stats_query failed")
        raise HTTPException(500, detail=str(e))


@router.post("/signal-stats/rank")
def signal_stats_rank(req: SignalRankRequest):
    """
    Rank all single signals by a chosen metric.
    Returns sorted list with forward-return stats per signal.
    """
    try:
        return rank_signals(
            universe  = req.universe,
            regime    = req.regime,
            date_from = req.date_from,
            date_to   = req.date_to,
            turbo_min = req.turbo_min,
            turbo_max = req.turbo_max,
            sort_by   = req.sort_by,
            min_n     = req.min_n,
            top_n     = req.top_n,
        )
    except Exception as e:
        log.exception("signal_stats_rank failed")
        raise HTTPException(500, detail=str(e))


@router.post("/bars/search")
def bars_search(body: dict):
    """
    Flexible bar search.
    body: { "universe": "nasdaq", "date_from": "...", "turbo_min": 60,
            "signals": ["l34", "ad_cluster"], "limit": 200 }
    """
    from studio.db import get_conn
    conn = get_conn(read_only=True)
    try:
        clauses = []
        params: list = []

        if body.get("universe"):
            clauses.append("universe = ?"); params.append(body["universe"])
        if body.get("date_from"):
            clauses.append("date >= ?"); params.append(body["date_from"])
        if body.get("date_to"):
            clauses.append("date <= ?"); params.append(body["date_to"])
        if body.get("turbo_min") is not None:
            clauses.append("turbo_score >= ?"); params.append(body["turbo_min"])
        if body.get("turbo_max") is not None:
            clauses.append("turbo_score <= ?"); params.append(body["turbo_max"])

        # Signal filters
        for sig in (body.get("signals") or []):
            available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
            if sig.lower() in available:
                clauses.append(f"{sig.lower()} >= 1")

        where = " AND ".join(clauses) if clauses else "1=1"
        limit = min(int(body.get("limit", 200)), 2000)

        rows = conn.execute(
            f"""SELECT ticker, date, close, turbo_score, gog_tier, swing_type,
                       fwd_5d, fwd_10d, fwd_20d, fwd_60d, mfe_60d, universe
                FROM bars WHERE {where}
                ORDER BY turbo_score DESC NULLS LAST
                LIMIT {limit}""",
            params,
        ).fetchdf()
        # Replace NaN with None for JSON serialization
        return {"count": len(rows), "rows": _safe_records(rows)}
    finally:
        conn.close()
