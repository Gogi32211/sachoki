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

from studio.db import ensure_schema, get_stats, STUDIO_DB_PATH, UNIVERSE_PRIORITY_SQL, use_tf
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
    tf: str = "1d"
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
    tf: str = "1d"
    event_type:    str         = "BULL_2X_60D"
    pre_window:    int         = 20
    min_lift:      float       = 2.0
    min_n:         int         = 15
    combo_depth:   int         = 3
    include_seqs:  bool        = True
    universes:     Optional[list[str]] = None

class MissRequest(BaseModel):
    tf: str = "1d"
    event_type: str         = "BULL_2X_60D"
    turbo_max:  float       = 15.0
    universes:  Optional[list[str]] = None
    pre_window: int         = 20
    top_n:      int         = 20

class FPRequest(BaseModel):
    tf: str = "1d"
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
    tf: str = "1d"
    score_id:   str
    event_type: str               = "BULL_2X_60D"
    date_from:  Optional[str]     = None
    date_to:    Optional[str]     = None
    universes:  Optional[list[str]] = None

class TZSequenceRequest(BaseModel):
    tf: str = "1d"
    sequence:  list[str | None] = []   # signal names or null for wildcard
    universe:  Optional[str]    = None
    regime:    Optional[str]    = None
    min_n:     int              = 5

class ConfluenceSequenceRequest(BaseModel):
    tf: str = "1d"
    bars:      list[str | None] = []   # T/Z signal per bar (None = wildcard); bars[0]=oldest
    universe:  Optional[str]    = None

class SignalStatsRequest(BaseModel):
    tf: str = "1d"
    years:      Optional[list[int]] = None
    months:     Optional[list[int]] = None
    signals:    list[str]         = []
    universe:   Optional[str]     = None
    regime:     Optional[str]     = None
    date_from:  Optional[str]     = None
    date_to:    Optional[str]     = None
    turbo_min:  Optional[float]   = None
    turbo_max:  Optional[float]   = None
    min_n:      int               = 5

class SignalRankRequest(BaseModel):
    tf: str = "1d"
    years:      Optional[list[int]] = None
    months:     Optional[list[int]] = None
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
    # ISO date → REPAIR mode: overwrite bars from this date instead of append-only.
    refetch_from: str | None = None


def _run_incremental(universes: list[str], refetch_from: str | None = None) -> None:
    """Run the delta-append refresh with ZERO-DOWNTIME staging+swap.

    The delta append + enrich + forward-backfill now run in a SEPARATE process
    against a STAGING copy of the DB (studio.incremental_swap), which is then
    atomically swapped over the live file. This keeps the live backend serving
    read-only queries uninterrupted — the old in-process read-write path broke
    the scanner with DuckDB "different configuration"/lock errors while running.
    """
    global _incremental_running, _incremental_results
    _incremental_running = True
    try:
        from studio.incremental_swap import run_swap
        _incremental_results = run_swap(universes, refetch_from=refetch_from)
        # fresh DB → drop the scan TTL memo so the Edge board shows new data immediately
        try:
            from scan_cache import invalidate as _inv
            _inv()
        except Exception:
            pass
        # recompute the 🧱 order-block dayset on the new bars (also rebuilds/warms the edge frame)
        try:
            import edge_replay as _ER
            _n = _ER.refresh_ob_days()
            log.info("ob_days refreshed: %s day-flags", _n)
        except Exception:
            log.exception("ob_days refresh failed (non-fatal)")
        # warm the mtf-ema JSON cache here (nightly) so the 📐 badge is ready by morning — the
        # scans only READ it (never rebuild), so if we don't warm it the badge silently vanishes.
        try:
            from mtf_ema_scan import scan as _mtf_scan
            _m = _mtf_scan(use_cache_sec=0)
            log.info("mtf_ema warmed: %s rows", _m.get("count") if isinstance(_m, dict) else "?")
        except Exception:
            log.exception("mtf_ema warm failed (non-fatal)")
        # rebuild the journal BASELINE table (~5s) — the per-(ticker,date) outcome of each
        # journal's own exit across the liquid universe. Without it the journal tabs report a
        # bare win%, which measures the market (a 20-bar hold wins ~55-59% on anything in a
        # bull window), not the signal. Cheap, so refresh it with the bars it depends on.
        try:
            import journal_bench as _JB
            _b = _JB.build()
            log.info("journal_bench rebuilt: %s liquid outcomes", len(_b))
        except Exception:
            log.exception("journal_bench rebuild failed (non-fatal)")
        # fill ultra_score / ultra_score_v3 / buy_score on the NEW bars (2026-07-18: all
        # scores are stored historically now; full history was backfilled once via
        # backfill_scores.py, this keeps the fresh bars filled). In-process — the backend
        # owns the analytics DB here, so the write is safe.
        try:
            import backfill_scores as _BS
            _n = _BS.run_incremental(days=6)
            log.info("score backfill (incremental): %s rows", _n)
        except Exception:
            log.exception("score backfill failed (non-fatal)")
        # prebreak_v2 on the INTRADAY DBs (2026-07-19): the model is a pure SQL formula that
        # was only ever applied to the daily DB — the 4h/1h tables carried empty columns.
        # Backfilled in full once; this keeps NEW intraday bars filled (NULL-only, cheap).
        # Needed for the intraday buy_score / MTF score-confirmation work.
        try:
            import duckdb as _dk
            from prebreak_v2 import prebreak_v2_score_sql as _pv2s, prebreak_v2_band_sql as _pv2b
            from studio.paths import db_path as _idbp
            for _tfdb in ("4h", "1h"):
                try:
                    _c = _dk.connect(_idbp(f"studio_{_tfdb}.duckdb"))
                    _c.execute(f"UPDATE bars SET prebreak_v2 = {_pv2s()} WHERE prebreak_v2 IS NULL")
                    _c.execute(f"UPDATE bars SET prebreak_v2_band = {_pv2b()} WHERE prebreak_v2_band IS NULL")
                    _c.commit(); _c.close()
                except Exception:
                    log.exception("intraday prebreak_v2 %s fill failed (non-fatal)", _tfdb)
            log.info("intraday prebreak_v2 incremental fill done")
        except Exception:
            log.exception("intraday prebreak_v2 hook failed (non-fatal)")
    except Exception as exc:
        log.exception("incremental swap refresh failed")
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
    background_tasks.add_task(_run_incremental, req.universes, req.refetch_from)
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
    confirm_lag: int = Query(0, ge=0, le=10),
    evaluate:  bool = Query(False),
    cost:      float = Query(0.5, ge=0.0, le=10.0),
    tf:        str  = Query("1d"),
    years:     Optional[str] = Query(None),
    months:    Optional[str] = Query(None),
):
    """TZ Sequence Lab — rank N-bar T/Z sequences by forward outcome vs baseline.

    When evaluate=true, each row also gets a `verdict` (backtest-expert skill):
    Deploy / Refine / Abandon after the significance, Bonferroni (vs n_candidates)
    and net-edge-after-`cost` gates — so a mirage (significant only because n is
    huge, but below cost) is flagged in the UI."""
    from studio.seq_lab import seq_lab
    try:
      with use_tf(tf):
        res = seq_lab(
            universe=universe, n_bars=n_bars, mode=mode, horizon=horizon,
            min_occ=min_occ, wyc_phase=wyc_phase, prefix=prefix, sort=sort,
            limit=limit, by_phase=by_phase, confirm_lag=confirm_lag,
            years=[int(x) for x in years.split(",") if x.strip()] if years else None,
            months=[int(x) for x in months.split(",") if x.strip()] if months else None,
        )
        if evaluate:
            try:
                from studio.eval_sequence import annotate_seq_lab
                res = annotate_seq_lab(res, cost_per_trade_pct=cost)
            except Exception:
                log.exception("seq-lab verdict annotation failed (rows returned unannotated)")
        res["tf"] = tf
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
    tf:         str   = Query("1d"),
):
    """Realised backtest of an entry condition (entry next open, target/stop/time exit)."""
    from studio.seq_backtest import backtest
    flags = [s for s in (signals or "").split(",") if s.strip()]
    try:
        with use_tf(tf):
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
    tf:         str   = Query("1d"),
):
    """Build the Playbook — run every predefined setup through the realised-backtest
    gate (expectancy>0 & PF>1 & positive in BOTH halves & enough trades) and attach
    today's live tickers for the survivors. One universe per call."""
    from studio.playbook import build_playbook
    try:
      with use_tf(tf):
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


def _swap_worker_liveness() -> dict:
    """Is the staging delta worker alive and actually doing work? (2026-07-28)

    The swap path runs `studio._delta_worker` as a SEPARATE PROCESS against a staging
    copy, so it never touches this process's in-memory progress dict. That made a
    healthy multi-hour run indistinguishable from a hang: the status endpoint reported
    running=true / stage=idle / 0-of-0 / ts=null for hours either way — which is exactly
    how a legitimately running update got killed by mistake.

    These three fields answer "is it alive?" without needing the worker to report in:
      worker_pid    — the running _delta_worker process (None = no worker)
      worker_cpu_s  — CPU seconds it has burned (rising = doing real work, not blocked)
      staging_age_s — seconds since the staging DB was last written (small = progressing)
    Best-effort and never raises: a status endpoint must not fail because `ps` did.
    """
    out = {"worker_pid": None, "worker_cpu_s": None, "staging_age_s": None,
           "staging_gb": None, "worker_elapsed_s": None}
    try:
        import os as _os, subprocess as _sp
        # A bare `pgrep -f studio._delta_worker` also matches any shell / monitor / pgrep
        # whose own command line merely MENTIONS the worker — that reports a phantom worker
        # (and makes a watch-loop that greps for it never terminate, since it sees itself).
        # So: widen with pgrep, then confirm per-pid that it really is `python -m
        # studio._delta_worker`, and never match ourselves. (The pattern cannot start with
        # "-m …" — pgrep parses a leading dash as a flag and silently matches nothing.)
        ps = _sp.run(["pgrep", "-f", r"studio\._delta_worker"],
                     capture_output=True, text=True, timeout=5)
        me = _os.getpid()
        pids = [p for p in (ps.stdout or "").split() if p.isdigit() and int(p) != me]
        for pid in pids:
            det = _sp.run(["ps", "-o", "comm=,command=", "-p", pid],
                          capture_output=True, text=True, timeout=5)
            line = (det.stdout or "").strip()
            if "python" not in line.lower() or "-m studio._delta_worker" not in line:
                continue                                    # a shell mentioning it, not the worker
            out["worker_pid"] = int(pid)
            info = _sp.run(["ps", "-o", "etime=,time=", "-p", pid],
                           capture_output=True, text=True, timeout=5)
            parts = (info.stdout or "").split()
            def _secs(t: str) -> int:                       # [[dd-]hh:]mm:ss → seconds
                t = t.split(".")[0].replace("-", ":")
                bits = [int(x) for x in t.split(":") if x.isdigit()]
                s = 0
                for b in bits:
                    s = s * 60 + b
                return s
            if len(parts) >= 2:
                out["worker_elapsed_s"] = _secs(parts[0])
                out["worker_cpu_s"] = _secs(parts[1])
            break
    except Exception:
        pass
    try:
        import os, time as _t
        from studio.incremental_swap import _STAGING
        if os.path.exists(_STAGING):
            st = os.stat(_STAGING)
            out["staging_age_s"] = round(_t.time() - st.st_mtime, 1)
            out["staging_gb"] = round(st.st_size / 1e9, 2)
    except Exception:
        pass
    return out


@router.get("/incremental-update/status")
def incremental_status():
    """Poll incremental refresh progress.

    Reads from the new delta module's progress file when present, falls back to the
    legacy module otherwise. Always carries `worker` liveness (see _swap_worker_liveness)
    so a running staging swap can be told apart from a genuine hang.
    """
    worker = _swap_worker_liveness()
    try:
        from studio.incremental_delta import get_progress as _delta_progress
        progress = _delta_progress()
        if progress.get("ts"):
            return {
                "running":  _incremental_running,
                "results":  _incremental_results,
                "progress": progress,
                "worker":   worker,
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
        "worker":   worker,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Edge Scanner — "Today's Best Setups" across universe
# ─────────────────────────────────────────────────────────────────────────────
_edge_running = False
_edge_results: dict = {}


class EdgeScanRequest(BaseModel):
    tf: str = "1d"
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
      with use_tf(getattr(req, "tf", "1d")):   # explicit: contextvars don't cross task boundaries
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
    tf:           str   = Query("1d"),
):
    """Rank tickers by AES (accumulation-exit score). Returns latest bar per ticker
    in (typically) ACC_TR phase, sorted by AES descending.
    """
    from studio.db import get_conn_tf, UNIVERSE_PRIORITY_SQL
    conn = get_conn_tf(tf, read_only=True)
    try:
        # A ticker living in >1 universe (e.g. ZS in nasdaq+sp500, RPGL in
        # nasdaq+russell2k) would otherwise appear once PER universe — and since
        # the bars are now identical across universes, those are pure duplicates.
        # Dedup to ONE row per ticker by universe priority (sp500>nasdaq>russell2k),
        # same canonical rule as /studio/bars. When a universe filter is given we
        # pre-filter the source so the dedup happens within it.
        src_params: list = []
        src_where = ""
        if universe:
            src_where = "WHERE universe = ?"
            src_params.append(universe)
        sql = f"""
            WITH src AS (SELECT * FROM bars {src_where}),
            ranked AS (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY date DESC, {UNIVERSE_PRIORITY_SQL}
              ) AS rn
              FROM src
            )
            SELECT ticker, universe, date, close, volume, avg_vol_20d,
                   wyc_phase, aes_stage, aes_score, aes_leading, aes_trend_5d,
                   acc_exit_class, acc_exit_in_n,
                   prebreak_v2, prebreak_v2_band,
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
        params2 = src_params + [min_price, min_volume, min_aes]
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
        sql += f" ORDER BY aes_score DESC, prebreak_v2 DESC, turbo_score DESC LIMIT {limit}"
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
        # 📐 divergence × 🏆RS (2026-07-28). This is the endpoint the Ultra grid actually
        # calls — main._enrich_edges only runs on /api/ultra-scan/results, so anything
        # attached there alone never reaches the DB-instant path (the same trap that once
        # left UV3 and score-hits unfilled here). Best-effort: a cold edge frame yields {}
        # and the column simply shows "—" rather than blocking the scan.
        try:
            import edge_replay as _ER
            _dv = _ER.latest_div_map(lookback=5)
            if _dv:
                for _r in (result.get("results") or []):
                    _d = _dv.get((_r.get("ticker") or "").upper())
                    if _d:
                        _r["div_buy"]    = _d.get("buy")
                        _r["div_deep"]   = _d.get("deep") is not None
                        _r["div_top"]    = _d.get("top")
                        _r["div_rsi_lo"] = _d.get("rsi_lo")
                        _r["div_rsi_hi"] = _d.get("rsi_hi")
        except Exception:
            log.debug("ultra-from-db divergence attach skipped", exc_info=True)
        return _sanitize_for_json(result)
    except Exception as exc:
        log.exception("ultra_from_db failed")
        raise HTTPException(500, detail=str(exc))


@router.post("/ultra-preview")
def ultra_preview(req: UltraDBScanRequest):
    """Hybrid Preview scan — DB history + today's LIVE forming bar (Massive),
    full signal suite recomputed. Falls back to the DB scan off-hours."""
    try:
        from studio.preview_scan import run_preview_scan
        result = run_preview_scan(
            universes    = req.universes,
            min_price    = req.min_price,
            min_volume   = req.min_volume,
            age_lookback = req.age_lookback,
        )
        return _sanitize_for_json(result)
    except Exception as exc:
        log.exception("ultra_preview failed")
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
def stats(tf: str = Query("1d")):
    """Overall DB stats: row count, ticker count, date range, events."""
    try:
        ensure_schema()
        with use_tf(tf):
            return get_stats()
    except Exception as e:
        from studio.db import _is_busy_error
        if _is_busy_error(e):
            # a write (enrich/import/refresh) is briefly holding the DB — report calmly
            return {"updating": True,
                    "message": "Database is busy (writing) — stats available shortly.",
                    "db_path": STUDIO_DB_PATH}
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
    with use_tf(req.tf):
        return detect_events(f, clear_existing=req.clear_existing)


@router.get("/events/summary")
def events_summary(tf: str = Query("1d")):
    with use_tf(tf):
        return get_events_summary()


@router.get("/events/list")
def events_list(
    event_type: Optional[str] = Query(None),
    universe:   Optional[str] = Query(None),
    limit:      int = Query(100, ge=1, le=1000),
    offset:     int = Query(0, ge=0),
    tf:         str = Query("1d"),
):
    with use_tf(tf):
        return list_events(event_type, universe, limit, offset)


# ─────────────────────────────────────────────────────────────────────────────
# Pattern Mining
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/patterns/mine")
def patterns_mine(req: PatternMineRequest):
    """Mine pre-event signal patterns with lift scores."""
    with use_tf(req.tf):
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
    with use_tf(req.tf):
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
    with use_tf(req.tf):
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
    with use_tf(req.tf):
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

from studio.paths import WEEKLY_DB as _WEEKLY_DB

@router.get("/bars/{ticker}")
def get_ticker_bars(
    ticker: str,
    limit:  int = Query(120, ge=1, le=2500),
    offset: int = Query(0, ge=0),
    tf:     str = Query("1d"),
):
    """Get all bars for a ticker. tf routes to the matching DB (1d/1w/4h/1h/15m)."""
    import duckdb as _duckdb
    from studio.db import get_conn, tf_db_path, TF_DB_FILES
    tf = (tf or "1d").lower()
    if tf not in TF_DB_FILES:
        raise HTTPException(400, detail=f"unknown tf '{tf}'")
    if tf != "1d":
        dbp = tf_db_path(tf)
        if not os.path.exists(dbp):
            raise HTTPException(404, detail=f"{tf} DB not built yet")
        try:
            conn = _duckdb.connect(dbp, read_only=True)
        except Exception as e:
            if "lock" in str(e).lower():
                raise HTTPException(503, detail=f"{tf} DB is being built — try again in a few minutes")
            raise
    else:
        conn = get_conn(read_only=True)
    try:
        rows = conn.execute(
            f"""SELECT ticker, date, open, high, low, close, volume,
                      turbo_score, prebreak_v2, prebreak_v2_band,
                      vol_bucket, gog_tier, swing_type,
                      fwd_1d, fwd_5d, fwd_10d, fwd_20d, fwd_60d,
                      mfe_20d, mfe_60d, mae_20d,
                      hit_2x_60d, hit_50pct_20d, drop_20pct_10d,
                      all_signals_text, t_sig, z_sig, l_sig,
                      composite_full_suffix, full_suffix, composite_vol,
                      bar_body_wick, bar_gap_range, bar_line5,
                      swing_type_3, is_pivot_low_3, is_pivot_high_3,
                      w2_accum, w2_break, w2_tr_quality, wt_valid_tr,
                      CASE WHEN w2_sc=1 THEN 'SC' WHEN w2_ar=1 THEN 'AR'
                           WHEN w2_st=1 THEN 'ST' WHEN w2_spring=1 THEN 'SPR'
                           WHEN w2_jac=1 THEN 'JAC' WHEN w2_sos=1 THEN 'SOS'
                           WHEN w2_lps=1 THEN 'LPS' WHEN w2_evr=1 THEN 'EVR'
                           ELSE '' END AS wyc_stage,
                      CASE WHEN wt_spring=1 THEN 'tSPR' WHEN wt_sos=1 THEN 'tSOS'
                           WHEN wt_lps=1 THEN 'tLPS' WHEN wt_evr=1 THEN 'tEVR'
                           ELSE '' END AS wt_stage
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


@router.get("/capit-atom-marks/{ticker}")
def capit_atom_marks(ticker: str, universe: str = Query(None)):
    """Return all historical Capit + Atom signal dates for a ticker from the Studio DB.
    Capit  = L34/L46 + RSI 15-30 + CCI<-100 (B+ quality).
    Atom   = T-sig + close_suffix='O' (weak-close) + bar_gap_class IN ('G2','G3').
    Returns {capit: [{date, l_sig, rsi, cci, close, score}], atom: [{date, t_sig, gap, rsi, close, post_capit}]}
    """
    import numpy as np
    from studio.db import get_conn
    tk = ticker.upper()
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        uni_where = f"AND universe = '{universe}'" if universe in ("sp500","nasdaq","russell2k") else ""
        qualify   = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1"

        # ── Capit: B+ quality capitulation bars ──────────────────────────────
        capit_rows = conn.execute(f"""
            WITH lag20 AS (
              SELECT ticker, universe, date, l_sig, rsi_14, cci_20, close, volume, avg_vol_20d,
                     LAG(close, 20) OVER (PARTITION BY ticker, universe ORDER BY date) AS c20
              FROM bars WHERE ticker = ? {uni_where}
            )
            SELECT date, l_sig, round(rsi_14,1) AS rsi, round(cci_20,0) AS cci,
                   round(close,2) AS close
            FROM lag20
            WHERE l_sig IN ('L34','L46')
              AND rsi_14 >= 15 AND rsi_14 < 30
              AND cci_20 < -100
              AND c20 > 0 AND (close / c20 - 1) > -0.25
              AND avg_vol_20d > 0
            {qualify}
            ORDER BY date
        """, [tk]).fetchdf()

        capit_dates = set(str(d)[:10] for d in capit_rows["date"].tolist())

        # ── Atom: weak-close gap-up T-sig bars ───────────────────────────────
        atom_rows = conn.execute(f"""
            SELECT date, t_sig, bar_gap_class AS gap, round(rsi_14,1) AS rsi,
                   round(close,2) AS close, close_suffix
            FROM bars WHERE ticker = ? {uni_where}
              AND t_sig IS NOT NULL
              AND close_suffix = 'O'
              AND bar_gap_class IN ('G2','G3')
            {qualify}
            ORDER BY date
        """, [tk]).fetchdf()

        # flag post_capit: is there a B+ capit ≤ 15 calendar days before?
        capit_arr = sorted(capit_dates)
        def _days_since(d_str):
            from datetime import date as _date
            d = _date.fromisoformat(d_str[:10])
            prior = [_date.fromisoformat(c) for c in capit_arr if c <= d_str[:10]]
            if not prior: return None
            return (d - prior[-1]).days

        capit_out = [
            {"date": str(r.date)[:10], "l_sig": r.l_sig,
             "rsi": float(r.rsi), "cci": float(r.cci), "close": float(r.close)}
            for r in capit_rows.itertuples()
        ]
        atom_out = []
        for r in atom_rows.itertuples():
            ds = _days_since(str(r.date)[:10])
            atom_out.append({
                "date": str(r.date)[:10], "t_sig": r.t_sig,
                "gap": r.gap, "rsi": float(r.rsi), "close": float(r.close),
                "post_capit": ds is not None and ds <= 15,
                "days_since_capit": int(ds) if ds is not None else None,
            })
        # ── 2nd P66: for each post-capit Atom, find the 2nd P signal that is P66 ──
        post_capit_atom_dates = [a["date"] for a in atom_out if a["post_capit"]]
        p66_out = []
        if post_capit_atom_dates:
            bar_df = conn.execute(f"""
                SELECT date::VARCHAR AS dt, close,
                       sig_p2, sig_p3, sig_p50, sig_p55, sig_p66, sig_p89
                FROM bars WHERE ticker = ? {uni_where}
                {qualify}
                ORDER BY dt
            """, [tk]).fetchdf()
            bar_df['dt'] = bar_df['dt'].str[:10]
            bar_df = bar_df.sort_values('dt').reset_index(drop=True)
            for atom_date in post_capit_atom_dates:
                ai_list = bar_df.index[bar_df['dt'] == atom_date].tolist()
                if not ai_list:
                    continue
                ai = ai_list[0]
                window = bar_df.iloc[ai + 1: ai + 31]
                p_mask = (
                    window['sig_p2'].astype(bool) | window['sig_p3'].astype(bool) |
                    window['sig_p50'].astype(bool) | window['sig_p55'].astype(bool) |
                    window['sig_p66'].astype(bool) | window['sig_p89'].astype(bool)
                )
                p_bars = window[p_mask]
                if len(p_bars) < 2:
                    continue
                second_p = p_bars.iloc[1]
                if not bool(second_p['sig_p66']):
                    continue
                p66_out.append({
                    "date": str(second_p['dt'])[:10],
                    "close": round(float(second_p['close']), 2),
                    "atom_date": atom_date,
                })

        return {"ticker": tk, "capit": capit_out, "atom": atom_out, "p66": p66_out}
    finally:
        conn.close()


@router.get("/mtf-ema-marks/{ticker}")
def mtf_ema_marks(ticker: str):
    """Historical MTF-EMA (SMX/RGTI) variant fires for chart markers — EOD daily snapshot of the
    15m/1h/4h EMA geometry. Returns [{date, variant}] (variant ∈ SMX/LL/UP/UPUP/UPUPUP/ORANGE)."""
    try:
        from mtf_ema_scan import marks_for_ticker
        return marks_for_ticker(ticker)
    except Exception as e:
        log.exception("mtf-ema marks failed")
        raise HTTPException(500, detail=str(e))


@router.get("/seq-marks/{ticker}")
def seq_marks(ticker: str):
    """🟡 The five user-built sequence edges (2026-08-04) as chart markers (1D):
    🌉Z1G4 · 🧲Z9HL · 🌉v2 · 🧺SEQ · 👑Z1G. Reads edge_replay.ticker_edges (TTL-cached,
    same masks as the backtest) and filters to the sequence-chip set — one source, so the
    chart can never disagree with Replay/Ultra."""
    _SEQ = {"🌉Z1G4🟡", "🧲Z9HL🟡", "🌉v2🟡", "🧺SEQ🟡", "👑Z1G🟡"}
    try:
        from edge_replay import ticker_edges
        emap = ticker_edges(ticker.upper())
        marks = [{"date": d, "code": c} for d, cs in sorted(emap.items())
                 for c in cs if c in _SEQ]
        return {"ticker": ticker.upper(), "marks": marks}
    except Exception as e:
        return {"ticker": ticker.upper(), "marks": [], "error": str(e)[:200]}


@router.get("/edge-marks/{ticker}")
def edge_marks(ticker: str, universe: str = Query(None)):
    """All historical Edge-board setup fires for chart markers (1D).
    Returns [{date, setup}] — setup ∈ {spring, g3, core, family, l43base, l22absorb}. Validated definitions:
      spring  = w2_spring + RSI 35-45 + bull-T + non-VB
      g3      = G3 gap + RSI<45 + bull-T + non-VB (any L)
      core    = weak-close (close_suffix=O) gap-up (G2/G3) bull-T + non-VB
      family  = anchor(Z11/Z3/Z1G/Z5, RSI30-45)[-2] → T3/T5[-1] → T11/T12[0]"""
    from studio.db import get_conn
    tk = ticker.upper()
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        uni_where = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        rows = conn.execute(f"""
            WITH b AS (
              SELECT date, rsi_14, coalesce(t_sig,'') t, coalesce(z_sig,'') z,
                     coalesce(vol_bucket,'') vol, coalesce(bar_gap_class,'') gapc,
                     coalesce(close_suffix,'') csfx, coalesce(w2_spring,0) spring,
                     CASE WHEN coalesce(wt_valid_tr,0)=1 AND wt_resistance > wt_support
                               AND wt_support > 0 AND abs(close/wt_support - 1) <= 0.05
                          THEN 1 ELSE 0 END sc_super,
                     CASE WHEN sig_l3 = 1 AND sig_l4 = 1 AND close < open
                               AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                          THEN 1 ELSE 0 END l22c,
                     CASE WHEN sig_l6 = 1 AND sig_l4 = 1 AND close >= open
                               AND volume / NULLIF(avg_vol_20d, 0) BETWEEN 0.7 AND 2.0
                          THEN 1 ELSE 0 END l43c
              FROM bars WHERE ticker = ? {uni_where}
              QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
            ),
            lg AS (
              SELECT *, lag(t,1) OVER (ORDER BY date) t1,
                        lag(z,2) OVER (ORDER BY date) z2,
                        lag(rsi_14,2) OVER (ORDER BY date) rsi2
              FROM b
            )
            SELECT date, setup, sc_super FROM (
              SELECT CAST(date AS DATE)::VARCHAR AS date, 'spring' AS setup, sc_super FROM lg
                WHERE spring = 1 AND rsi_14 >= 35 AND rsi_14 < 45 AND t <> '' AND vol <> 'VB'
              UNION ALL
              SELECT CAST(date AS DATE)::VARCHAR, 'g3', sc_super FROM lg
                WHERE gapc = 'G3' AND rsi_14 < 45 AND t <> '' AND vol <> 'VB'
              UNION ALL
              SELECT CAST(date AS DATE)::VARCHAR, 'core', sc_super FROM lg
                WHERE t <> '' AND csfx = 'O' AND gapc IN ('G2', 'G3') AND vol <> 'VB'
              UNION ALL
              SELECT CAST(date AS DATE)::VARCHAR, 'family', sc_super FROM lg
                WHERE z2 IN ('Z11','Z3','Z1G','Z5') AND rsi2 >= 30 AND rsi2 < 45
                  AND (t1 = 'T3' OR t1 = 'T5') AND (t = 'T11' OR t = 'T12')
              UNION ALL
              SELECT CAST(date AS DATE)::VARCHAR, 'l43base', sc_super FROM lg WHERE l43c = 1
              UNION ALL
              SELECT CAST(date AS DATE)::VARCHAR, 'l22absorb', sc_super FROM lg WHERE l22c = 1
            ) ORDER BY date
        """, [tk]).fetchall()
        return {"ticker": tk, "marks": [{"date": r[0], "setup": r[1], "sc_super": bool(r[2])} for r in rows]}
    finally:
        conn.close()


@router.get("/gann-grid/{ticker}")
def gann_grid(ticker: str, tf: str = Query("1d"), universe: str = Query(None),
              levels: int = Query(6), min_bps: int = Query(20),
              up_asc: int = Query(10), dn_asc: int = Query(5),
              up_dn: int = Query(10), dn_dn: int = Query(5)):
    """Gann parallel grid lines for lightweight-charts overlay.
    Returns ascending (red) and descending (cyan) line series data points.
    Each line = list of {time, value} sampled every ~20 bars.
    """
    import math
    from studio.db import get_conn
    tk = ticker.upper()
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        # pick correct DB / date column
        if tf in ("1h", "4h", "30m", "15m"):
            from studio.db import get_conn_tf
            try:
                conn2 = get_conn_tf(tf, read_only=True)
            except Exception:
                conn2 = conn
            df = conn2.execute(
                "SELECT date::VARCHAR AS dt, high, low, close FROM bars "
                "WHERE ticker=? ORDER BY dt", [tk]
            ).fetchdf()
            if conn2 is not conn:
                conn2.close()
        else:
            uni_where = ""
            if universe:
                uni_where = f"AND universe='{universe}'"
            df = conn.execute(
                f"SELECT date::VARCHAR AS dt, high, low, close FROM bars "
                f"WHERE ticker=? {uni_where} ORDER BY dt", [tk]
            ).fetchdf()

        if df.empty:
            raise HTTPException(404, detail=f"{tk} not in DB")

        df = df.reset_index(drop=True)
        df["dt"] = df["dt"].str[:10]
        n = len(df)

        # locate all-time low and high by close price
        low_idx  = int(df["close"].idxmin())
        high_idx = int(df["close"].idxmax())
        low_price  = float(df.loc[low_idx, "close"])
        high_price = float(df.loc[high_idx, "close"])

        if low_price <= 0:
            raise HTTPException(422, detail="low_price <= 0")

        # vibration step (geometric)
        ratio    = high_price / low_price
        pct_step = (math.pow(ratio, 1.0 / levels) - 1.0) * 100.0
        mult     = 1.0 + pct_step / 100.0
        bars_diff = abs(high_idx - low_idx)
        bps      = max(min_bps, bars_diff // levels)  # bars per step

        anchor_price = low_price
        anchor_idx   = low_idx
        anchor_date  = df.loc[anchor_idx, "dt"]

        # sample bar indices across full range with step ~bps//4 for smoothness
        sample_step = max(5, bps // 4)
        x_range = range(0, n, sample_step)

        def asc_line(level: int):
            pts = []
            for x in x_range:
                exp = level + (x - anchor_idx) / bps
                price = anchor_price * math.pow(mult, exp)
                if price <= 0 or math.isinf(price) or math.isnan(price):
                    continue
                pts.append({"time": df.loc[x, "dt"], "value": round(price, 4)})
            return pts

        def dn_line(level: int):
            pts = []
            for x in x_range:
                exp = level - (x - anchor_idx) / bps
                price = anchor_price * math.pow(mult, exp)
                if price <= 0 or math.isinf(price) or math.isnan(price):
                    continue
                pts.append({"time": df.loc[x, "dt"], "value": round(price, 4)})
            return pts

        ascending  = []
        descending = []

        for i in range(-dn_asc, up_asc + 1):
            pts = asc_line(i)
            if pts:
                ascending.append({"level": i, "points": pts})

        for i in range(-dn_dn, up_dn + 1):
            pts = dn_line(i)
            if pts:
                descending.append({"level": i, "points": pts})

        return {
            "ticker": tk,
            "anchor_price": round(anchor_price, 4),
            "anchor_date": anchor_date,
            "pct_step": round(pct_step, 2),
            "bars_per_step": bps,
            "low_date": df.loc[low_idx, "dt"],
            "high_date": df.loc[high_idx, "dt"],
            "ascending": ascending,
            "descending": descending,
        }
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
      with use_tf(req.tf):
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
    tf:         str                 = "1d"  # "1d" (default, fast) or "1h" (intraday DB)
    match_rows: bool                = False  # also return per-match (ticker, date, hh, hl)
    min_price:  Optional[float]     = None   # close-price band on the entry bar
    max_price:  Optional[float]     = None
    years:      Optional[list[int]] = None   # restrict entry bar to these calendar years
    months:     Optional[list[int]] = None   # restrict entry bar to these months (1-12)


@router.post("/exact-sequence")
def exact_sequence(req: ExactSequenceRequest):
    """
    Exact 5-line N-bar sequence match against the enriched 1D Studio DB, AND — when a
    separate intraday DB exists (~/Downloads/studio_1h.duckdb) — the SAME sequence run
    against 1H bars under `tf_1h`, so a bar-sequence can be compared across timeframes.
    (On 1H, fwd_Nd = N bars ≈ N trading hours ahead.) Returns match count + HL/HH stats.
    """
    try:
        tf = (req.tf or "1d").lower()
        if tf in ("1w", "1h", "4h", "30m", "15m"):
            # query a separate intraday DB (slow — tens of M bars; UI loads it async)
            import os as _os
            from studio.paths import db_path as _dbp
            dbtf = _dbp(tf)
            if not _os.path.exists(dbtf):
                return {"matches": 0, "tf": tf, "error": f"{tf} DB not built yet"}
            import duckdb as _duckdb
            try:
                ctf = _duckdb.connect(dbtf, read_only=True)
            except Exception as _le:
                if "lock" in str(_le).lower():
                    return {"matches": 0, "tf": tf, "error": f"{tf} DB is still being built — try again soon"}
                raise
            try:
                r = query_exact_sequence(
                    bars=req.bars, universe=req.universe,
                    strictness=req.strictness, pivot_lr=req.pivot_lr, conn=ctf,
                    match_rows=req.match_rows,
                    min_price=req.min_price, max_price=req.max_price,
                    years=req.years, months=req.months,
                )
            finally:
                ctf.close()
            r["tf"] = tf
            return r
        # default: 1D (fast)
        return query_exact_sequence(
            bars       = req.bars,
            universe   = req.universe,
            strictness = req.strictness,
            pivot_lr   = req.pivot_lr,
            match_rows = req.match_rows,
            min_price  = req.min_price,
            max_price  = req.max_price,
            years      = req.years,
            months     = req.months,
        )
    except Exception as e:
        log.exception("exact_sequence failed")
        raise HTTPException(500, detail=str(e))


_ICS_CACHE: dict | None = None


@router.get("/intraday-confirm-score")
def intraday_confirm_score(trigger: str = ""):
    """ADDITIVE (does not touch the 1d/4h/1h sequence engine): for a daily TRIGGER
    signal (the last bar's TZ, e.g. 'T1G'), return how the *intraday 1H structure
    inside the trigger day* shifts the forward outcome.

    score = (# confirmer 1H signals present in the trigger day)
          − (# trap 1H signals present), clamped to −2..+2. Monotonic in win%.
    Precomputed population-level (studio/intraday_confirm_score.json) so it stays
    statistically robust — we never subdivide a single small sequence's matches.
    Falls back to '*' (pooled across all triggers) if the trigger is unknown.
    """
    global _ICS_CACHE
    if _ICS_CACHE is None:
        import os as _os
        import json as _json
        p = _os.path.join(_os.path.dirname(__file__), "studio", "intraday_confirm_score.json")
        try:
            _ICS_CACHE = _json.load(open(p))
        except Exception:
            _ICS_CACHE = {"triggers": {}, "confirmers": [], "traps": []}
    trig = (trigger or "").strip().upper()
    tbl = _ICS_CACHE.get("triggers", {})
    bl  = _ICS_CACHE.get("baseline", {})
    key = trig if trig in tbl else "*"
    return {
        "trigger":    trig,
        "resolved":   key,            # which row we actually returned ('*' = pooled fallback)
        "confirmers": _ICS_CACHE.get("confirmers", []),
        "traps":      _ICS_CACHE.get("traps", []),
        "score_def":  _ICS_CACHE.get("score_def", ""),
        "baseline":   bl.get(key, {}),    # unconditional win/med for THIS 1D signal — the Δ reference
        "scores":     tbl.get(key, {}),   # { "-2":{n,med5,win5,med10,win10}, ... "2":{...} }
    }


_ICS_CONF = ['Z3', 'T2G', 'T1', 'T2', 'T3', 'Z1']
_ICS_TRAP = ['T10', 'T11', 'T12', 'Z10', 'Z11', 'Z12']


@router.post("/exact-sequence-1h-filter")
def exact_sequence_1h_filter(req: ExactSequenceRequest):
    """ADDITIVE: take the EXACT 1D sequence's real matches and re-aggregate the SAME
    metric (Next-pivot-HH %, the 61.8% the 1D band shows) on the subset that passes a
    1H-confirm filter — so you see directly whether the intraday 1H structure adds edge
    FOR THIS SEQUENCE (not a population proxy).

    Honest baseline: the filter can only see days the 1H DB covers (~5yr), so we report
    HH% on the 1H-covered subset, then keep ≥+1 / ≥+2 / avoided(≤0) within it.
    """
    try:
        r = query_exact_sequence(bars=req.bars, universe=req.universe,
                                 strictness=req.strictness, pivot_lr=req.pivot_lr,
                                 match_rows=True,
                                 min_price=req.min_price, max_price=req.max_price)
        rows = r.get("rows") or []
        if not rows:
            return {"matches": 0, "error": r.get("error", "no matches")}

        import os as _os
        from studio.paths import db_path as _dbp
        dbtf = _dbp("1h")
        if not _os.path.exists(dbtf):
            return {"matches": len(rows), "error": "1h DB not built"}

        import duckdb as _duckdb
        import pandas as _pd
        md = _pd.DataFrame([{"ticker": x["ticker"], "d": x["date"]} for x in rows])
        conf = "+".join(f"CASE WHEN list_contains(hs,'{s}') THEN 1 ELSE 0 END" for s in _ICS_CONF)
        trap = "+".join(f"CASE WHEN list_contains(hs,'{s}') THEN 1 ELSE 0 END" for s in _ICS_TRAP)
        c = _duckdb.connect(dbtf, read_only=True)
        try:
            c.execute("PRAGMA threads=4")
            c.register("m", md)
            score_rows = c.execute(f"""
              WITH dh AS (
                SELECT b.ticker, CAST(b.date AS DATE) d,
                  list_distinct(list(coalesce(NULLIF(b.t_sig,''),NULLIF(b.z_sig,'')))
                                FILTER (WHERE coalesce(NULLIF(b.t_sig,''),NULLIF(b.z_sig,'')) IS NOT NULL)) hs
                FROM bars b JOIN m ON m.ticker=b.ticker AND CAST(b.date AS DATE)=CAST(m.d AS DATE)
                GROUP BY 1,2)
              SELECT ticker, CAST(d AS VARCHAR), greatest(-2, least(2, ({conf})-({trap}))) FROM dh
            """).fetchall()
        finally:
            c.close()
        smap = {(t, d): int(s) for t, d, s in score_rows}
        for x in rows:
            x["score"] = smap.get((x["ticker"], x["date"]))   # None = no 1H coverage

        def agg(subset):
            n = len(subset)
            hh = sum(1 for x in subset if x["hh"] == 1)
            hl = sum(1 for x in subset if x["hl"] == 1)
            known = hh + hl
            w = [x["fwd_10d"] for x in subset if x["fwd_10d"] is not None]
            return {
                "n": n,
                "hh_pct": round(hh / known * 100, 1) if known else None,
                "pivot_known": known,
                "win10": round(sum(1 for v in w if v > 0) / len(w) * 100, 1) if w else None,
                "avg_fwd10": round(sum(w) / len(w), 2) if w else None,
            }

        covered = [x for x in rows if x["score"] is not None]
        return {
            "matches": len(rows),
            "confirmers": _ICS_CONF, "traps": _ICS_TRAP,
            "all_1d":   agg(rows),                                            # = the 1D band's 61.8%
            "covered":  agg(covered),                                         # honest filter baseline (1H-covered)
            "keep_ge1": agg([x for x in covered if x["score"] >= 1]),
            "keep_ge2": agg([x for x in covered if x["score"] >= 2]),
            "avoided":  agg([x for x in covered if x["score"] <= 0]),
            "n_uncovered": len(rows) - len(covered),
        }
    except Exception as e:
        log.exception("exact_sequence_1h_filter failed")
        raise HTTPException(500, detail=str(e))


@router.post("/confluence-sequence")
def confluence_sequence(req: ConfluenceSequenceRequest):
    """
    For an N-bar T/Z sequence, return a confluence funnel:
    how many times the sequence appears with progressively more signal requirements
    (T/Z only → +WLNBB → +Wick → +GOG → +PARA → +VABS).
    """
    try:
      with use_tf(req.tf):
        return query_confluence_sequence(
            bars      = req.bars,
            universe  = req.universe,
        )
    except Exception as e:
        log.exception("confluence_sequence failed")
        raise HTTPException(500, detail=str(e))


@router.get("/signal-stats/filters")
def signal_stats_filters(tf: str = Query("1d")):
    """Return available filter values (universes, regimes, date range)."""
    try:
        with use_tf(tf):
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
      with use_tf(req.tf):
        return query_combo(
            signals   = req.signals,
            universe  = req.universe,
            regime    = req.regime,
            date_from = req.date_from,
            date_to   = req.date_to,
            turbo_min = req.turbo_min,
            turbo_max = req.turbo_max,
            min_n     = req.min_n,
            years     = req.years,
            months    = req.months,
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
      with use_tf(req.tf):
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
            years     = req.years,
            months    = req.months,
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


# ─────────────────────────────────────────────────────────────────────────────
# Pump Research — daily pump log analysis
# ─────────────────────────────────────────────────────────────────────────────

class PumpAnalyzeRequest(BaseModel):
    tickers: list[str]
    pumps: dict[str, float] = {}   # ticker → pump% (optional)
    window: int = 14               # bars to look back

class PumpLogRequest(BaseModel):
    date: str
    tickers: list[str]
    pumps: dict[str, float] = {}
    rows: list[dict]
    notes: str = ""


@router.post("/pump/analyze")
def pump_analyze(body: PumpAnalyzeRequest):
    """Analyze ALL signals for pump tickers. Signal-agnostic: returns every sig_* column
    that fired, plus cross-ticker frequency so research can discover which signals
    are universally present before pumps — not just capit/atom."""
    from studio.db import get_conn
    if not body.tickers:
        return {"rows": [], "freq": []}
    tks = [t.upper().strip() for t in body.tickers if t.strip()]
    window = max(7, min(body.window, 30))
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        qualify = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1"
        placeholders = ", ".join("?" * len(tks))

        # ── Discover all SMALLINT signal columns dynamically ──────────────────
        schema_df = conn.execute("DESCRIBE bars").fetchdf()
        all_cols = schema_df["column_name"].tolist()
        all_types = dict(zip(schema_df["column_name"], schema_df["column_type"]))

        # Signal columns: SMALLINT that start with sig_, or known signal prefixes
        # Exclude price_gt_/price_lt_ (EMA position, not signals), fwd_*, mfe_*, mae_*
        SIGNAL_COL_PREFIXES = (
            "sig_", "l34", "l43", "l22", "be_up", "bo_up", "bx_up", "vbo_up",
            "g1p", "g2p", "g3p", "g1l", "g2l", "g1c", "g2c", "g3c",
            "rocket", "hilo_buy", "three_g", "svs", "sq", "load", "f8",
            "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
            "wvf_spike", "psar_bull", "psar_bear",
            "eb_bull", "eb_bear", "fbo_bull", "fbo_bear", "bf_buy", "bf_sell",
            "ultra_3up", "ultra_3dn", "best_long", "best_short",
            "d_strong_bull", "d_strong_bear", "d_absorb_bull", "d_absorb_bear",
            "d_div_bull", "d_div_bear", "d_cd_bull", "d_cd_bear",
            "d_surge_bull", "d_surge_bear", "d_blast_bull", "d_blast_bear",
            "d_vd_div_bull", "d_vd_div_bear", "d_spring", "d_upthrust",
            "d_flip_bull", "d_flip_bear", "d_orange_bull",
            "d_blast_bull_red", "d_blast_bear_grn", "d_surge_bull_red", "d_surge_bear_grn",
            "para_prep", "para_start", "para_plus", "para_retest",
            "fly_abcd", "fly_cd", "fly_bd", "fly_ad",
            "tz_bull", "sweet_spot_active", "rsi_le_35", "rsi_ge_70",
            "w2_sc", "w2_ar", "w2_st", "w2_spring", "w2_sos", "w2_jac",
            "w2_lps", "w2_evr", "w2_accum", "w2_break",
            "wt_valid_tr", "wt_sos", "wt_spring", "wt_lps", "wt_evr",
            "prebreak_prime", "prebreak_ready", "prebreak_watch",
            "prebreak_v2", "prebreak_v3", "prebreak_v4",
            "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm", "pb_lvbo", "pb_wvf_confirm",
            "seq_l34_eb", "ad_fresh", "ad_cluster",
        )
        EXCL_PREFIXES = ("fwd_", "mfe_", "mae_", "hit_", "drop_", "price_gt_", "price_lt_",
                         "is_pivot_", "next_pivot_", "bars_to_", "pct_to_", "fwd_swing_",
                         "swing_ret_", "atr_", "acc_exit_", "aes_", "pb_stop_", "pb_macro_",
                         "vix_range", "sig_260308", "sig_l88",  # noisy / meta
                         "w2_state", "w2_tr_quality", "wt_quality", "wt_support", "wt_resistance",
                         "prebreak_score", "profile_score",)
        sig_cols = [
            c for c in all_cols
            if all_types.get(c) == "SMALLINT"
            and any(c.startswith(p) for p in SIGNAL_COL_PREFIXES)
            and not any(c.startswith(ex) for ex in EXCL_PREFIXES)
        ]

        # Always include vol columns and EMA-below flags
        extra_always = ["sig_vol_5x", "sig_vol_10x", "sig_vol_20x",
                        "price_lt_20", "price_lt_50", "price_lt_89", "price_lt_200"]
        for c in extra_always:
            if c in all_cols and c not in sig_cols:
                sig_cols.append(c)

        # Core context columns (not signals, but needed for per-bar summary)
        context_cols = ["ticker", "date::VARCHAR AS dt", "close", "volume", "avg_vol_20d",
                        "t_sig", "z_sig", "l_sig", "bar_gap_class", "close_suffix",
                        "rsi_14", "cci_20", "ultra_score", "turbo_score", "wyc_phase"]

        sel = ", ".join(context_cols + sig_cols)
        df = conn.execute(f"""
            SELECT {sel}
            FROM bars
            WHERE ticker IN ({placeholders})
            {qualify}
            ORDER BY ticker, dt
        """, tks).fetchdf()

        if df.empty:
            return {"rows": [{"ticker": t, "status": "not_in_db"} for t in tks], "freq": []}

        df["dt"] = df["dt"].str[:10]
        df["vol_ratio"] = (df["volume"] / df["avg_vol_20d"].replace(0, float("nan"))).round(1)

        # ── Per-ticker analysis ────────────────────────────────────────────────
        results = []
        ticker_sig_sets = {}   # ticker → set of sig cols that fired at least once

        for tk in tks:
            tdf = df[df["ticker"] == tk].tail(window).copy()
            if tdf.empty:
                results.append({"ticker": tk, "status": "not_in_db"})
                ticker_sig_sets[tk] = set()
                continue

            last = tdf.iloc[-1]
            n = len(tdf)
            max_vol = float(tdf["vol_ratio"].max()) if not tdf.empty else 0

            # ALL signals that fired (any bar in window)
            fired = {}
            for col in sig_cols:
                if col not in tdf.columns:
                    continue
                cnt = int(tdf[col].fillna(0).astype(bool).sum())
                if cnt > 0:
                    fired[col] = cnt  # count of bars where signal was active

            ticker_sig_sets[tk] = set(fired.keys())

            # Categorical signals
            t_vals = tdf[tdf["t_sig"].notna() & (tdf["t_sig"] != "")][["dt", "t_sig"]].values.tolist()
            z_vals = tdf[tdf["z_sig"].notna() & (tdf["z_sig"] != "")][["dt", "z_sig"]].values.tolist()
            l_vals = tdf[tdf["l_sig"].notna() & (tdf["l_sig"] != "")][["dt", "l_sig"]].values.tolist()

            # Vol spikes
            vol_spikes = tdf[tdf["vol_ratio"] >= 3][["dt", "vol_ratio", "close_suffix"]].copy()
            vol_spikes["vol_ratio"] = vol_spikes["vol_ratio"].round(1)

            results.append({
                "ticker": tk,
                "status": "ok",
                "pump_pct": body.pumps.get(tk),
                "last_date": str(last["dt"])[:10],
                "last_close": round(float(last["close"]), 2),
                "last_rsi": round(float(last["rsi_14"]), 1) if pd.notna(last["rsi_14"]) else None,
                "n_bars": n,
                "max_vol_ratio": round(max_vol, 1),
                "t_sigs": t_vals[-6:],
                "z_sigs": z_vals[-6:],
                "l_sigs": l_vals[-6:],
                "vol_spikes": vol_spikes.to_dict("records"),
                "fired": fired,           # {sig_col: bar_count}
                "n_fired": len(fired),
            })

        # ── Cross-ticker signal frequency ──────────────────────────────────────
        ok_count = sum(1 for t in tks if ticker_sig_sets.get(t))
        freq_map = {}
        for tk, sigs in ticker_sig_sets.items():
            for s in sigs:
                freq_map[s] = freq_map.get(s, 0) + 1

        freq = sorted(
            [{"sig": k, "n": v, "pct": round(100 * v / max(ok_count, 1))}
             for k, v in freq_map.items()],
            key=lambda x: x["n"], reverse=True
        )

        return {
            "rows": results,
            "freq": freq,
            "n_tickers": ok_count,
            "sig_cols_total": len(sig_cols),
        }
    finally:
        conn.close()


@router.post("/pump/log")
def pump_log(body: PumpLogRequest):
    """Append a new session to PUMP_RESEARCH.md."""
    import os
    from pathlib import Path
    log_path = Path(__file__).parent.parent / "PUMP_RESEARCH.md"

    # Build markdown table rows
    def _fmt(r):
        tk = r.get("ticker", "?")
        pump = f"+{r['pump_pct']:.0f}%" if r.get("pump_pct") else "—"
        capit = ", ".join(r.get("capit_dates", [])[-2:]) or "—"
        atom = "✅ " + (r.get("atom_date") or "") if r.get("capit_atom") else "—"
        p66 = r.get("p66_date") or "—"
        vol_spikes = r.get("vol_spikes", [])
        vol_str = f"{vol_spikes[-1]['vol_ratio']}x ({vol_spikes[-1]['dt'][-5:]})" if vol_spikes else "—"
        t_sigs = " ".join(x[1] for x in (r.get("t_sigs") or [])[-4:]) or "—"
        rsi = r.get("last_rsi")
        below = r.get("below_ema20", 0)
        score = r.get("recipe_score", 0)
        return f"| {tk} | {pump} | {capit} | {atom} | {p66} | {vol_str} | {t_sigs} | {rsi} | {below}/{r.get('n_bars',14)} | {score}/9 |"

    ok_rows = [r for r in body.rows if r.get("status") == "ok"]
    not_found = [r["ticker"] for r in body.rows if r.get("status") == "not_in_db"]

    header = f"""
## Session: {body.date}

**Tickers**: {', '.join(body.tickers)}
**Not in DB**: {', '.join(not_found) if not_found else 'none'}

| Ticker | Pump% | Capit (last 2) | Capit→Atom | P66 | Vol spike | T-sigs (last 4) | RSI | EMA20↓/{body.rows[0].get('n_bars',14) if body.rows else 14}d | Score |
|--------|-------|----------------|------------|-----|-----------|-----------------|-----|----------|-------|
"""
    rows_md = "\n".join(_fmt(r) for r in ok_rows)

    notes_md = f"\n**Notes**: {body.notes}" if body.notes else ""

    capit_atom_list = [r for r in ok_rows if r.get("capit_atom")]
    ca_md = ""
    if capit_atom_list:
        ca_md = "\n**Capit→Atom setups**: " + ", ".join(
            f"{r['ticker']} ({r['atom_date']})" for r in capit_atom_list
        )

    session_block = header + rows_md + notes_md + ca_md + "\n\n---\n"

    # Append to file (or create if missing)
    if not log_path.exists():
        log_path.write_text("# PUMP_RESEARCH — Daily Signal Log\n\n")

    with open(log_path, "a") as f:
        f.write(session_block)

    return {"ok": True, "path": str(log_path), "sessions_appended": 1}


@router.get("/pump/md")
def pump_md():
    """Return the current PUMP_RESEARCH.md content."""
    from pathlib import Path
    log_path = Path(__file__).parent.parent / "PUMP_RESEARCH.md"
    if not log_path.exists():
        return {"content": ""}
    return {"content": log_path.read_text()}


# ─────────────────────────────────────────────────────────────────────────────
# Pre-Pump Setup Score v2 — chart markers + live screener
# Two-layer model (updated from full 195-signal 10-bar lift analysis):
#
#   SETUP signals  (lookback 5 bars) — background accumulation pattern
#     l_sig='L3'        +4  (6.9x at T-1, strongest day-specific signal)
#     sig_bias_dn       +3  (3.79x — NEW: downward bias = reversal precondition)
#     wyc_in_tr         +2  (3.15x — upgraded from +1)
#     sig_dd_dn_green   +2  (1.97x — NEW: bear day closing green = absorption)
#     wyc_spring        +2  (1.86x — NEW: Wyckoff Spring = classic reversal)
#     sig_260308        +2  (1.73x)
#     sweet_spot_active +2  (1.31x)
#     rsi_le_35         +1  (1.49x)
#     sig_abs           +1  (1.25x)
#     close $0.5–$7     +1  (price sweet spot, current bar)
#
#   TRIGGER signals (current bar only) — something happening TODAY
#     sig_vol_20x +5 / sig_vol_10x +4 / sig_vol_5x +3  [highest vol tier]
#     sig_va      +3  (1.56x over 10d, strong VSA trigger)
#     sig_sc      +3  (2.00x)
#     sig_bc      +3  (2.21x)
#     d_upthrust  +3  (2.15x)
#     d_absorb_bear   +2  (1.60x)
#     d_blast_bear_grn+2  (1.95x)
#     sig_svs     +2  (1.30x)
# ─────────────────────────────────────────────────────────────────────────────

# Trigger score: must fire on the CURRENT bar
_TRIGGER_SCORE_SQL = """
    GREATEST(
      CASE WHEN sig_vol_20x > 0 THEN 5 ELSE 0 END,
      CASE WHEN sig_vol_10x > 0 THEN 4 ELSE 0 END,
      CASE WHEN sig_vol_5x  > 0 THEN 3 ELSE 0 END,
      0
    )
  + (CASE WHEN sig_va           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN sig_sc           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN sig_bc           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN d_upthrust       > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN d_absorb_bear    > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN d_blast_bear_grn > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN sig_svs          > 0 THEN 2 ELSE 0 END)
"""

# Setup score fragment used inside a CTE that declares WINDOW w5
# w5 = PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
_SETUP_SCORE_W5 = """
    (CASE WHEN MAX(CASE WHEN l_sig = 'L3' THEN 1 ELSE 0 END) OVER w5 > 0 THEN 4 ELSE 0 END)
  + (CASE WHEN MAX(sig_bias_dn)       OVER w5 > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN MAX(wyc_in_tr)         OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sig_dd_dn_green)   OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(wyc_spring)        OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sig_260308)        OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sweet_spot_active) OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(rsi_le_35)         OVER w5 > 0 THEN 1 ELSE 0 END)
  + (CASE WHEN MAX(sig_abs)           OVER w5 > 0 THEN 1 ELSE 0 END)
  + (CASE WHEN close BETWEEN 0.5 AND 7 THEN 1 ELSE 0 END)
"""

# Setup column expressions for SELECT (individual flags, used for signals display)
_SETUP_COLS_W5 = """
    MAX(CASE WHEN l_sig = 'L3' THEN 1 ELSE 0 END) OVER w5  AS setup_l3,
    MAX(sig_bias_dn)       OVER w5                          AS setup_bias_dn,
    MAX(wyc_in_tr)         OVER w5                          AS setup_wyc_in_tr,
    MAX(sig_dd_dn_green)   OVER w5                          AS setup_dd_dn_green,
    MAX(wyc_spring)        OVER w5                          AS setup_wyc_spring,
    MAX(sig_260308)        OVER w5                          AS setup_260308,
    MAX(sweet_spot_active) OVER w5                          AS setup_sweet_spot,
    MAX(rsi_le_35)         OVER w5                          AS setup_rsi_le35,
    MAX(sig_abs)           OVER w5                          AS setup_abs
"""


def _pump_score_signals(row) -> list[str]:
    """Extract which signal components fired. Setup signals suffixed ↺, triggers suffixed !"""
    fired = []
    # setup signals (5-bar lookback)
    if row.get("setup_l3", 0):            fired.append("L3↺")
    if row.get("setup_bias_dn", 0):       fired.append("BiasD↺")
    if row.get("setup_wyc_in_tr", 0):     fired.append("WycTR↺")
    if row.get("setup_dd_dn_green", 0):   fired.append("DnGrn↺")
    if row.get("setup_wyc_spring", 0):    fired.append("Spring↺")
    if row.get("setup_260308", 0):        fired.append("260308↺")
    if row.get("setup_sweet_spot", 0):    fired.append("SweetSpot↺")
    if row.get("setup_rsi_le35", 0):      fired.append("RSI<35↺")
    if row.get("setup_abs", 0):           fired.append("Abs↺")
    # trigger signals (current bar)
    if row.get("sig_vol_20x", 0):         fired.append("vol20x!")
    elif row.get("sig_vol_10x", 0):       fired.append("vol10x!")
    elif row.get("sig_vol_5x",  0):       fired.append("vol5x!")
    if row.get("sig_va",           0):    fired.append("VA!")
    if row.get("sig_sc",           0):    fired.append("SC!")
    if row.get("sig_bc",           0):    fired.append("BC!")
    if row.get("d_upthrust",       0):  fired.append("Upthrust!")
    if row.get("d_absorb_bear",    0):  fired.append("AbsorbBear!")
    if row.get("d_blast_bear_grn", 0):  fired.append("BlastBearGrn!")
    if row.get("sig_svs",          0):  fired.append("SVS!")
    return fired


@router.get("/pump-setup/{ticker}")
def pump_setup_marks(ticker: str, universe: str = Query(None), min_score: int = 4):
    """Return historical bars where pump-setup score >= min_score.
    Score = setup_score (5-bar lookback) + trigger_score (current bar).
    trigger_score > 0 required — something must fire today."""
    from studio.db import get_conn
    tk = ticker.upper()
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        uni_where = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        uni_dedup = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1"

        rows = conn.execute(f"""
            WITH scored AS (
                SELECT date, close, rsi_14, l_sig, wyc_phase,
                       sig_vol_20x, sig_vol_10x, sig_vol_5x,
                       sig_va, sig_sc, sig_bc, d_upthrust,
                       d_absorb_bear, d_blast_bear_grn, sig_svs,
                       {_SETUP_COLS_W5},
                       ({_SETUP_SCORE_W5})  AS setup_score,
                       ({_TRIGGER_SCORE_SQL}) AS trigger_score
                FROM bars
                WHERE ticker = ? {uni_where}
                WINDOW w5 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                {uni_dedup}
            )
            SELECT date::VARCHAR AS date,
                   round(close, 2) AS close,
                   round(rsi_14, 1) AS rsi,
                   l_sig, wyc_phase,
                   sig_vol_20x, sig_vol_10x, sig_vol_5x,
                   sig_va, sig_sc, sig_bc, d_upthrust,
                   d_absorb_bear, d_blast_bear_grn, sig_svs,
                   setup_l3, setup_wyc_in_tr, setup_rsi_le35,
                   setup_sweet_spot, setup_260308, setup_abs,
                   setup_score, trigger_score,
                   setup_score + trigger_score AS pump_score
            FROM scored
            WHERE trigger_score > 0
              AND setup_score + trigger_score >= {min_score}
            ORDER BY date
        """, [tk]).fetchdf()

        marks = []
        for r in rows.itertuples(index=False):
            d = r._asdict()
            marks.append({
                "date":          d["date"][:10],
                "close":         float(d["close"])  if d["close"] is not None else None,
                "rsi":           float(d["rsi"])    if d["rsi"]   is not None else None,
                "l_sig":         d["l_sig"],
                "wyc_phase":     d["wyc_phase"],
                "score":         int(d["pump_score"]),
                "setup_score":   int(d["setup_score"]),
                "trigger_score": int(d["trigger_score"]),
                "signals":       _pump_score_signals(d),
            })
        return {"ticker": tk, "marks": marks, "count": len(marks)}
    finally:
        conn.close()


@router.get("/pump-screener")
def pump_screener(
    universe: str = Query("nasdaq"),
    min_score: int = 6,
    max_score: int = 99,
    max_price: float = 10.0,
    limit: int = 40,
):
    """Live screener: latest bar per ticker, scored by setup (5-bar lookback) + trigger (today).
    trigger_score > 0 is mandatory — something must fire on the latest bar."""
    from studio.db import get_conn
    try:
        conn = get_conn(read_only=True)
    except Exception as e:
        raise HTTPException(503, detail=str(e))
    try:
        uni_filter = f"universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else "universe IN ('nasdaq','sp500','russell2k')"
        rows = conn.execute(f"""
            WITH scored AS (
                SELECT ticker, date, close, rsi_14, l_sig, wyc_phase,
                       avg_vol_20d, volume,
                       sig_vol_20x, sig_vol_10x, sig_vol_5x,
                       sig_va, sig_sc, sig_bc, d_upthrust,
                       d_absorb_bear, d_blast_bear_grn, sig_svs,
                       {_SETUP_COLS_W5},
                       ({_SETUP_SCORE_W5})    AS setup_score,
                       ({_TRIGGER_SCORE_SQL}) AS trigger_score,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM bars
                WHERE {uni_filter}
                  AND close BETWEEN 0.30 AND {max_price}
                  AND avg_vol_20d > 0
                WINDOW w5 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
            )
            SELECT ticker, date::VARCHAR AS date,
                   round(close, 2) AS close,
                   round(rsi_14, 1) AS rsi,
                   l_sig, wyc_phase,
                   setup_score, trigger_score,
                   setup_score + trigger_score AS pump_score,
                   round(volume / avg_vol_20d, 2) AS vol_ratio,
                   sig_vol_20x, sig_vol_10x, sig_vol_5x,
                   sig_va, sig_sc, sig_bc, d_upthrust,
                   d_absorb_bear, d_blast_bear_grn, sig_svs,
                   setup_l3, setup_wyc_in_tr, setup_rsi_le35,
                   setup_sweet_spot, setup_260308, setup_abs
            FROM scored
            WHERE rn = 1
              AND trigger_score > 0
              AND setup_score + trigger_score >= {min_score}
              AND setup_score + trigger_score <= {max_score}
            ORDER BY pump_score DESC
            LIMIT {limit}
        """).fetchdf()

        results = []
        for r in rows.itertuples(index=False):
            d = r._asdict()
            results.append({
                "ticker":        d["ticker"],
                "date":          d["date"][:10],
                "close":         float(d["close"]),
                "rsi":           float(d["rsi"]) if d["rsi"] is not None else None,
                "l_sig":         d["l_sig"],
                "wyc_phase":     d["wyc_phase"],
                "score":         int(d["pump_score"]),
                "setup_score":   int(d["setup_score"]),
                "trigger_score": int(d["trigger_score"]),
                "vol_ratio":     float(d["vol_ratio"]) if d["vol_ratio"] is not None else None,
                "signals":       _pump_score_signals(d),
            })
        return {"results": results, "count": len(results), "min_score": min_score, "max_score": max_score}
    finally:
        conn.close()
