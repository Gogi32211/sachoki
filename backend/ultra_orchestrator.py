"""
ultra_orchestrator.py — ULTRA scan orchestrator.

ULTRA is a *display-only* layer. It does NOT:
  • compute a new score
  • introduce a new category
  • modify Turbo scoring or category logic
  • modify any other module's behavior

It DOES, in one user click:
  1. trigger the canonical Turbo scan (`turbo_engine.run_turbo_scan`)
  2. generate the TZ/WLNBB `stock_stat` CSV the secondary modules depend on
     (`analyzers.tz_wlnbb.stock_stat.generate_stock_stat`)
  3. read enrichments from existing read-only scans:
        • TZ Intelligence  (`tz_intelligence.scanner.run_intelligence_scan`)
        • Pullback Miner   (`analyzers.pullback_miner.miner.run_pullback_scan`)
        • Rare Reversal    (`analyzers.rare_reversal.miner.run_rare_reversal_scan`)
        • TZ/WLNBB latest bar per ticker (read direct from the stock_stat CSV)
  4. merge them by ticker on top of the canonical Turbo rows
  5. cache the merged result so `GET /api/ultra-scan/results` is instant

Per-source failure NEVER kills the response — it gets recorded as a warning
and the affected ticker shows `—` in the relevant column.
"""
from __future__ import annotations

import csv as _csv
import gc as _gc
import logging
import os as _os
import threading
import time as _time
from collections import OrderedDict
from typing import Any

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Public state — mutated only by `run_ultra_scan_job` and `_set_phase`.
# Read-only access via `get_ultra_status()` / `get_ultra_results()`.
# ─────────────────────────────────────────────────────────────────────────────

_ultra_state: dict = {
    "running":      False,
    "started_at":   0.0,
    "completed_at": None,
    "universe":     None,
    "tf":           None,
    "nasdaq_batch": None,
    "phase":        None,    # "turbo" | "stock_stat" | "tz_wlnbb" | "tz_intelligence" | "pullback" | "rare_reversal" | "done"
    "phases":       {},      # {phase: {state: pending|running|ok|error|skipped, message: str}}
    "error":        None,
    "warnings":     [],
    "sources":      {},      # final source-status snapshot
    "turbo_done":   0,
    "turbo_total":  0,
    "stock_stat_done":  0,
    "stock_stat_total": 0,
}

# Cache: {(universe, tf, nasdaq_batch): {"results": [...], "meta": {...},
#                                         "warnings": [...], "last_scan": "..."}}
# LRU-ordered so we can evict the oldest entry when the cap is reached.
_ultra_results_cache: "OrderedDict[tuple, dict]" = OrderedDict()
_ultra_lock = threading.Lock()


_PHASE_ORDER = (
    "turbo", "stock_stat", "tz_wlnbb",
    "tz_intelligence", "pullback", "rare_reversal",
    "merge",
)

# Default Phase 2 fan-out.
#
# Memory note: each Phase 2 module independently loads the entire
# stock_stat_tz_wlnbb_*.csv into a `rows_by_ticker` dict (this is internal to
# the existing modules and we don't modify them). For SP500/NASDAQ that's
# easily 200–400 MB per copy, and Turbo is also memory-heavy, so running 4
# parallel readers + Turbo on a small Railway slot OOMs.
#
# Default 2 keeps Phase 2 parallel (faster than sequential) while keeping
# peak memory roughly halved vs. 4. Tighter deployments can pass
# max_workers=1 to fully serialise Phase 2 readers; bigger boxes can pass
# max_workers=4 for maximum throughput.
_DEFAULT_MAX_WORKERS = 2

# Cap how many merged ULTRA responses we hold in memory at once. Prevents the
# results cache from growing unbounded across (universe, tf, batch) combos.
_MAX_RESULTS_CACHE_ENTRIES = 4


def _new_phase_dict() -> dict:
    return {p: {"state": "pending", "message": ""} for p in _PHASE_ORDER}


def _set_phase(phase: str, state: str, message: str = "") -> None:
    with _ultra_lock:
        _ultra_state["phase"] = phase
        ph = _ultra_state.setdefault("phases", _new_phase_dict())
        ph.setdefault(phase, {"state": "pending", "message": ""})
        ph[phase]["state"]   = state
        ph[phase]["message"] = message


def _add_warning(msg: str) -> None:
    with _ultra_lock:
        warnings = _ultra_state.setdefault("warnings", [])
        warnings.append(msg)
    log.warning("ULTRA: %s", msg)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _ultra_tz_batch_stat_path(universe: str, tf: str, nasdaq_batch: str = "") -> str:
    """Mirror of main._tz_batch_stat_path (kept local to avoid circular import)."""
    if nasdaq_batch and nasdaq_batch != "all":
        if universe == "nasdaq":
            return f"stock_stat_tz_wlnbb_nasdaq_{nasdaq_batch}_{tf}.csv"
        if universe == "nasdaq_gt5":
            return f"stock_stat_tz_wlnbb_nasdaq_gt5_{nasdaq_batch}_{tf}.csv"
    return f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"


def _resolve_tz_wlnbb_csv(universe: str, tf: str, nasdaq_batch: str = "") -> str | None:
    candidates = [
        _ultra_tz_batch_stat_path(universe, tf, nasdaq_batch),
        f"stock_stat_tz_wlnbb_{universe}_{tf}.csv",
        f"stock_stat_tz_wlnbb_{tf}.csv",
    ]
    for p in candidates:
        if _os.path.exists(p):
            return p
    return None


def _read_tz_wlnbb_latest(universe: str, tf: str, nasdaq_batch: str = "") -> dict:
    """Latest TZ/WLNBB row per ticker from the stock_stat CSV. Empty dict on miss."""
    path = _resolve_tz_wlnbb_csv(universe, tf, nasdaq_batch)
    if not path:
        return {}
    rows_by_ticker: dict[str, list] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            t = row.get("ticker", "")
            if not t:
                continue
            if row.get("universe", "") and row.get("universe", "") != universe:
                continue
            rows_by_ticker.setdefault(t, []).append(row)
    latest: dict = {}
    for t, rows in rows_by_ticker.items():
        rows.sort(key=lambda r: r.get("bar_datetime") or r.get("date", ""))
        latest[t] = rows[-1]
    return latest


def _project_tz_wlnbb(row: dict) -> dict:
    return {
        "t_signal":      row.get("t_signal", "") or "",
        "z_signal":      row.get("z_signal", "") or "",
        "l_signal":      row.get("l_signal", "") or "",
        "preup_signal":  row.get("preup_signal", "") or "",
        "predn_signal":  row.get("predn_signal", "") or "",
        "lane1_label":   row.get("lane1_label", "") or "",
        "lane3_label":   row.get("lane3_label", "") or "",
        "volume_bucket": row.get("volume_bucket", "") or "",
        "wick_suffix":   row.get("wick_suffix", "") or "",
    }


def _project_tz_intel(row: dict) -> dict:
    return {
        "role":                 row.get("role", "") or "",
        "quality":              row.get("quality", "") or "",
        "action":               row.get("action", "") or "",
        "score":                row.get("score"),
        "matched_status":       row.get("matched_status", "") or "",
        "matched_med10d_pct":   row.get("matched_med10d_pct"),
        "matched_fail10d_pct":  row.get("matched_fail10d_pct"),
    }


def _project_abr(row: dict) -> dict:
    return {
        "category":        row.get("abr_category", "") or "",
        "med10d_pct":      row.get("abr_med10d_pct"),
        "fail10d_pct":     row.get("abr_fail10d_pct"),
        "context_type":    row.get("abr_context_type", "") or "",
        "action_hint":     row.get("abr_action_hint", "") or "",
        "conflict_flag":   bool(row.get("abr_conflict_flag")),
        "confirmation_flag": bool(row.get("abr_confirmation_flag")),
    }


def _project_pullback(row: dict) -> dict:
    return {
        "evidence_tier":              row.get("evidence_tier", "") or "",
        "pullback_stage":             row.get("pullback_stage", "") or "",
        "pattern_key":                row.get("pattern_key", "") or "",
        "pattern_length":             row.get("pattern_length"),
        "score":                      row.get("score"),
        "median_10d_return":          row.get("median_10d_return"),
        "win_rate_10d":               row.get("win_rate_10d"),
        "fail_rate_10d":              row.get("fail_rate_10d"),
        "is_currently_active":        bool(row.get("is_currently_active")),
        "current_pattern_completion": row.get("current_pattern_completion"),
    }


def _project_rare(row: dict) -> dict:
    return {
        "evidence_tier":              row.get("evidence_tier", "") or "",
        "base4_key":                  row.get("base4_key", "") or "",
        "extended5_key":              row.get("extended5_key") or "",
        "extended6_key":              row.get("extended6_key") or "",
        "pattern_length":             row.get("pattern_length"),
        "score":                      row.get("score"),
        "median_10d_return":          row.get("median_10d_return"),
        "fail_rate_10d":              row.get("fail_rate_10d"),
        "is_currently_active":        bool(row.get("is_currently_active")),
        "current_pattern_completion": row.get("current_pattern_completion"),
    }


def _best_pattern_per_ticker(rows: list) -> dict:
    """Pick the highest-score pattern record per ticker. Read-only."""
    by_ticker: dict = {}
    for r in rows or []:
        t = r.get("ticker")
        if not t:
            continue
        prev = by_ticker.get(t)
        if prev is None:
            by_ticker[t] = r
            continue
        if _safe_float(r.get("score"), -1e9) > _safe_float(prev.get("score"), -1e9):
            by_ticker[t] = r
    return by_ticker


# ─────────────────────────────────────────────────────────────────────────────
# Phase implementations
# ─────────────────────────────────────────────────────────────────────────────

def _phase_turbo(universe: str, tf: str, lookback_n: int, partial_day: bool,
                 min_volume: float, min_store_score: float) -> tuple[dict, list, str | None]:
    """Run the canonical Turbo scan synchronously. Returns the same per-ticker
    rows that /api/turbo-scan would. Does NOT mutate Turbo internals."""
    _set_phase("turbo", "running", "Running Turbo scan")
    from turbo_engine import (
        run_turbo_scan, get_turbo_results, get_last_turbo_scan_time,
        get_turbo_progress,
    )

    # Run the canonical scan. It manages its own DB writes / state.
    try:
        run_turbo_scan(
            interval=tf, universe=universe, workers=8,
            lookback_n=lookback_n, partial_day=partial_day,
            min_volume=min_volume, min_store_score=min_store_score,
        )
    except Exception as exc:
        _set_phase("turbo", "error", str(exc))
        raise

    # Pull progress counters into ULTRA state so the UI can show them
    prog = get_turbo_progress()
    with _ultra_lock:
        _ultra_state["turbo_done"]  = prog.get("done", 0)
        _ultra_state["turbo_total"] = prog.get("total", 0)

    rows = get_turbo_results(
        limit=10000, min_score=0, direction="all",
        tf=tf, universe=universe,
    )
    last_time = get_last_turbo_scan_time(tf=tf, universe=universe)
    by_ticker: dict = {}
    order: list = []
    for r in rows:
        t = r.get("ticker")
        if t and t not in by_ticker:
            by_ticker[t] = r
            order.append(t)
    _set_phase("turbo", "ok", f"{len(by_ticker)} tickers")
    return by_ticker, order, last_time


def _phase_stock_stat(universe: str, tf: str, nasdaq_batch: str, bars: int) -> bool:
    """Generate the TZ/WLNBB stock_stat CSV the secondary modules depend on.

    Returns True if a usable CSV is in place when the phase exits, False
    otherwise. Failure is non-fatal — secondary modules will simply report
    no data.
    """
    _set_phase("stock_stat", "running", "Generating TZ/WLNBB stock_stat CSV")
    out_path = _ultra_tz_batch_stat_path(universe, tf, nasdaq_batch)

    # Skip generation if a fresh CSV already exists (avoids redundant work)
    if _resolve_tz_wlnbb_csv(universe, tf, nasdaq_batch):
        _set_phase("stock_stat", "ok", f"existing CSV: {out_path}")
        return True

    try:
        from analyzers.tz_wlnbb.stock_stat import generate_stock_stat
        from scanner import get_universe_tickers

        # nasdaq_gt5 reuses the NASDAQ ticker list with min_price gate inside the generator
        source_universe = "nasdaq" if universe == "nasdaq_gt5" else universe
        gen_min_price   = 5.0     if universe == "nasdaq_gt5" else 0.0

        if universe == "split":
            from split_universe import split_service as _svc
            fresh = _svc.get_split_universe_result(force_refresh=True)
            tickers = list(fresh.tickers)
        else:
            try:
                tickers = get_universe_tickers(source_universe)
            except Exception:
                tickers = []

        if universe in ("nasdaq", "nasdaq_gt5") and nasdaq_batch and nasdaq_batch != "all":
            from main import _filter_nasdaq_batch
            tickers = _filter_nasdaq_batch(tickers, nasdaq_batch)

        with _ultra_lock:
            _ultra_state["stock_stat_total"] = len(tickers)
            _ultra_state["stock_stat_done"]  = 0

        from data_polygon import fetch_bars as _fetch_bars, polygon_available
        if polygon_available():
            def _fetch(ticker, interval, n_bars):
                days = max(int(n_bars * 1.6), 365)
                return _fetch_bars(ticker, interval=interval, days=days)
        else:
            from data import fetch_ohlcv as _fetch_yf
            def _fetch(ticker, interval, n_bars):
                return _fetch_yf(ticker, interval, n_bars)

        def _on_progress(done, total):
            with _ultra_lock:
                _ultra_state["stock_stat_done"]  = done
                _ultra_state["stock_stat_total"] = total

        path, audit = generate_stock_stat(
            tickers, _fetch, universe=universe, tf=tf, bars=bars,
            min_price=gen_min_price, output_path=out_path,
            progress_callback=_on_progress,
        )
        _set_phase("stock_stat", "ok", f"wrote {path}")
        return True
    except Exception as exc:
        _set_phase("stock_stat", "error", str(exc))
        _add_warning(f"TZ/WLNBB stock_stat unavailable: {exc}")
        return False


def _phase_tz_wlnbb(universe: str, tf: str, nasdaq_batch: str) -> dict:
    _set_phase("tz_wlnbb", "running", "")
    try:
        d = _read_tz_wlnbb_latest(universe, tf, nasdaq_batch)
        _set_phase("tz_wlnbb", "ok" if d else "skipped",
                   f"{len(d)} tickers" if d else "no CSV")
        return d
    except Exception as exc:
        _set_phase("tz_wlnbb", "error", str(exc))
        _add_warning(f"TZ/WLNBB unavailable: {exc}")
        return {}


def _phase_tz_intelligence(universe: str, tf: str, nasdaq_batch: str,
                           min_price: float, max_price: float,
                           min_volume: float) -> dict:
    _set_phase("tz_intelligence", "running", "")
    try:
        from tz_intelligence.scanner import run_intelligence_scan
        resp = run_intelligence_scan(
            universe=universe, tf=tf, nasdaq_batch=nasdaq_batch,
            min_price=min_price, max_price=max_price, min_volume=min_volume,
            role_filter="all", scan_mode="latest", limit=10000,
        )
        if isinstance(resp, dict) and resp.get("error"):
            _set_phase("tz_intelligence", "skipped", resp["error"])
            _add_warning(f"TZ Intelligence unavailable: {resp['error']}")
            return {}
        out: dict = {}
        for r in (resp or {}).get("results", []) or []:
            t = r.get("ticker")
            if t and t not in out:
                out[t] = r
        _set_phase("tz_intelligence", "ok", f"{len(out)} tickers")
        return out
    except Exception as exc:
        _set_phase("tz_intelligence", "error", str(exc))
        _add_warning(f"TZ Intelligence unavailable: {exc}")
        return {}


def _phase_pullback(universe: str, tf: str,
                    min_price: float, max_price: float) -> dict:
    _set_phase("pullback", "running", "")
    try:
        from analyzers.pullback_miner.miner import run_pullback_scan
        resp = run_pullback_scan(
            universe=universe, tf=tf,
            min_price=min_price, max_price=max_price,
            limit=10000,
        )
        if isinstance(resp, dict) and resp.get("error"):
            _set_phase("pullback", "skipped", resp["error"])
            _add_warning(f"Pullback Miner unavailable: {resp['error']}")
            return {}
        out = _best_pattern_per_ticker((resp or {}).get("results", []) or [])
        _set_phase("pullback", "ok", f"{len(out)} tickers")
        return out
    except Exception as exc:
        _set_phase("pullback", "error", str(exc))
        _add_warning(f"Pullback Miner unavailable: {exc}")
        return {}


def _phase_rare_reversal(universe: str, tf: str,
                         min_price: float, max_price: float) -> dict:
    _set_phase("rare_reversal", "running", "")
    try:
        from analyzers.rare_reversal.miner import run_rare_reversal_scan
        resp = run_rare_reversal_scan(
            universe=universe, tf=tf,
            min_price=min_price, max_price=max_price,
            limit=10000,
        )
        if isinstance(resp, dict) and resp.get("error"):
            _set_phase("rare_reversal", "skipped", resp["error"])
            _add_warning(f"Rare Reversal unavailable: {resp['error']}")
            return {}
        out = _best_pattern_per_ticker((resp or {}).get("results", []) or [])
        _set_phase("rare_reversal", "ok", f"{len(out)} tickers")
        return out
    except Exception as exc:
        _set_phase("rare_reversal", "error", str(exc))
        _add_warning(f"Rare Reversal unavailable: {exc}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Merge — Turbo rows are the base; everything else is additive enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _merge(turbo_by_ticker: dict, ordered: list, last_scan: str | None,
           tz_wlnbb_by_ticker: dict, tz_intel_by_ticker: dict,
           pullback_by_ticker: dict, rare_by_ticker: dict) -> list:
    out: list = []
    for ticker in ordered:
        base = turbo_by_ticker.get(ticker)
        if base is None:
            continue
        # Start with the canonical Turbo row — DO NOT mutate scoring or category
        row = dict(base)
        tzw   = tz_wlnbb_by_ticker.get(ticker)
        intel = tz_intel_by_ticker.get(ticker)
        pb    = pullback_by_ticker.get(ticker)
        rr    = rare_by_ticker.get(ticker)

        row["ultra_sources"] = {
            "has_turbo":         True,
            "has_tz_wlnbb":      tzw   is not None,
            "has_tz_intel":      intel is not None,
            "has_pullback":      pb    is not None,
            "has_rare_reversal": rr    is not None,
        }
        row["tz_wlnbb"]      = _project_tz_wlnbb(tzw)   if tzw   else None
        row["tz_intel"]      = _project_tz_intel(intel) if intel else None
        row["abr"]           = _project_abr(intel)      if intel else None
        row["pullback"]      = _project_pullback(pb)    if pb    else None
        row["rare_reversal"] = _project_rare(rr)        if rr    else None
        out.append(row)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Job entry point — invoked from a BackgroundTasks worker
# ─────────────────────────────────────────────────────────────────────────────

def run_ultra_scan_job(
    universe: str = "sp500",
    tf: str = "1d",
    lookback_n: int = 5,
    partial_day: bool = False,
    min_volume: float = 0.0,
    min_store_score: float = 5.0,
    nasdaq_batch: str = "",
    stock_stat_bars: int = 500,
    min_price: float = 0.0,
    max_price: float = 1e9,
    max_workers: int = _DEFAULT_MAX_WORKERS,
) -> dict:
    """Dependency-aware orchestrator.

    Pipeline:
      Phase 1 (parallel, 2 workers): Turbo scan ‖ TZ/WLNBB stock_stat generation
      Phase 2 (parallel, max_workers): TZ/WLNBB read · TZ Intelligence ·
                                        Pullback Miner · Rare Reversal Miner
      Phase 3:  merge enrichments onto Turbo rows by ticker

    Phase 2 modules all read the same on-disk stock_stat CSV produced in
    Phase 1, so they're safe to run concurrently.

    Per-source failure NEVER aborts the response — the affected source is
    marked unavailable and a warning is appended.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with _ultra_lock:
        phases = _new_phase_dict()
        # Phase 2 modules are blocked on stock_stat — show as 'pending' upfront
        for ph in ("tz_wlnbb", "tz_intelligence", "pullback", "rare_reversal"):
            phases[ph] = {"state": "pending", "message": "waiting on stock_stat"}
        phases["merge"] = {"state": "pending", "message": ""}
        _ultra_state.update({
            "running":      True,
            "started_at":   _time.time(),
            "completed_at": None,
            "universe":     universe,
            "tf":           tf,
            "nasdaq_batch": nasdaq_batch or None,
            "phase":        None,
            "phases":       phases,
            "error":        None,
            "warnings":     [],
            "sources":      {},
            "turbo_done":   0,
            "turbo_total":  0,
            "stock_stat_done":  0,
            "stock_stat_total": 0,
            "max_workers":  max_workers,
        })

    sources: dict = {
        "turbo":           {"ok": False, "count": 0},
        "tz_wlnbb":        {"ok": False, "count": 0},
        "tz_intelligence": {"ok": False, "count": 0},
        "pullback":        {"ok": False, "count": 0},
        "rare_reversal":   {"ok": False, "count": 0},
    }

    turbo_by_ticker: dict = {}
    ordered: list = []
    last_scan: str | None = None
    results: list = []
    tz_wlnbb_by_ticker: dict = {}
    tz_intel_by_ticker: dict = {}
    pullback_by_ticker: dict = {}
    rare_by_ticker: dict = {}

    def _run_turbo() -> tuple:
        """Phase 1A — Turbo scan."""
        return _phase_turbo(
            universe=universe, tf=tf, lookback_n=lookback_n,
            partial_day=partial_day, min_volume=min_volume,
            min_store_score=min_store_score,
        )

    def _run_stock_stat() -> bool:
        """Phase 1B — TZ/WLNBB stock_stat generation."""
        return _phase_stock_stat(universe, tf, nasdaq_batch, stock_stat_bars)

    try:
        # ── Phase 1: Turbo and stock_stat in parallel ────────────────────────
        # Two raw executors so we can drive Phase 2 off stock_stat completion
        # without blocking on Turbo (Phase 2 only depends on stock_stat).
        ex1 = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ultra-p1")
        ph2_workers = max(1, min(4, max_workers))
        ex2 = ThreadPoolExecutor(
            max_workers=ph2_workers, thread_name_prefix="ultra-p2",
        )
        try:
            fut_turbo      = ex1.submit(_run_turbo)
            fut_stock_stat = ex1.submit(_run_stock_stat)

            # Wait on stock_stat ONLY — Turbo continues running in parallel.
            try:
                fut_stock_stat.result()
            except Exception as exc:
                _add_warning(f"TZ/WLNBB stock_stat unavailable: {exc}")

            # ── Phase 2: secondary readers start NOW ─────────────────────────
            # Triggered as soon as stock_stat is done; Turbo may still be
            # running. All four read the same on-disk stock_stat CSV with no
            # shared mutable state, so parallelism is safe.
            fut2 = {
                "tz_wlnbb":        ex2.submit(_phase_tz_wlnbb,
                                              universe, tf, nasdaq_batch),
                "tz_intelligence": ex2.submit(_phase_tz_intelligence,
                                              universe, tf, nasdaq_batch,
                                              min_price, max_price, min_volume),
                "pullback":        ex2.submit(_phase_pullback,
                                              universe, tf, min_price, max_price),
                "rare_reversal":   ex2.submit(_phase_rare_reversal,
                                              universe, tf, min_price, max_price),
            }

            # Drain everything (Turbo + the four Phase 2 readers) as it
            # completes. We don't act on the as_completed values directly —
            # each phase fn records its own state/warnings — but draining
            # surfaces any unhandled exception promptly via .result() below.
            for _ in as_completed(list(fut2.values()) + [fut_turbo]):
                pass

            # Collect Turbo result (may have finished anywhere along the way)
            try:
                turbo_by_ticker, ordered, last_scan = fut_turbo.result()
                sources["turbo"] = {"ok": True, "count": len(turbo_by_ticker)}
            except Exception as exc:
                _add_warning(f"Turbo scan failed: {exc}")

            # Collect Phase 2 results
            try:
                tz_wlnbb_by_ticker = fut2["tz_wlnbb"].result() or {}
            except Exception as exc:
                _add_warning(f"TZ/WLNBB unavailable: {exc}")
            try:
                tz_intel_by_ticker = fut2["tz_intelligence"].result() or {}
            except Exception as exc:
                _add_warning(f"TZ Intelligence unavailable: {exc}")
            try:
                pullback_by_ticker = fut2["pullback"].result() or {}
            except Exception as exc:
                _add_warning(f"Pullback Miner unavailable: {exc}")
            try:
                rare_by_ticker = fut2["rare_reversal"].result() or {}
            except Exception as exc:
                _add_warning(f"Rare Reversal unavailable: {exc}")
        finally:
            # Both executors must shut down before we return cleanly.
            ex1.shutdown(wait=True)
            ex2.shutdown(wait=True)

        sources["tz_wlnbb"]        = {"ok": bool(tz_wlnbb_by_ticker),
                                       "count": len(tz_wlnbb_by_ticker)}
        sources["tz_intelligence"] = {"ok": bool(tz_intel_by_ticker),
                                       "count": len(tz_intel_by_ticker)}
        sources["pullback"]        = {"ok": bool(pullback_by_ticker),
                                       "count": len(pullback_by_ticker)}
        sources["rare_reversal"]   = {"ok": bool(rare_by_ticker),
                                       "count": len(rare_by_ticker)}

        # ── Phase 3: merge ───────────────────────────────────────────────────
        _set_phase("merge", "running", "")
        results = _merge(
            turbo_by_ticker, ordered, last_scan,
            tz_wlnbb_by_ticker, tz_intel_by_ticker,
            pullback_by_ticker, rare_by_ticker,
        )
        _set_phase("merge", "ok", f"{len(results)} merged rows")

        # Memory hygiene: the secondary modules' rows_by_ticker dicts can
        # each be hundreds of MB. Once merge has consumed them we don't need
        # them anymore — drop them and force a GC pass before serialising
        # the response.
        try:
            tz_wlnbb_by_ticker.clear()
            tz_intel_by_ticker.clear()
            pullback_by_ticker.clear()
            rare_by_ticker.clear()
            turbo_by_ticker.clear()
            del ordered[:]
        except Exception:
            pass
        _gc.collect()

    except Exception as exc:
        with _ultra_lock:
            _ultra_state["error"] = str(exc)
        log.exception("ULTRA orchestrator crashed")

    elapsed_ms = int((_time.time() - _ultra_state.get("started_at", _time.time())) * 1000)
    response = {
        "results":   results,
        "total":     len(results),
        "last_scan": last_scan,
        "warnings":  list(_ultra_state.get("warnings", [])),
        "meta": {
            "universe":     universe,
            "tf":           tf,
            "nasdaq_batch": nasdaq_batch or None,
            "elapsed_ms":   elapsed_ms,
            "sources":      sources,
        },
    }

    cache_key = (universe, tf, nasdaq_batch or "")
    with _ultra_lock:
        # LRU eviction: if the key already exists, move it to the end so the
        # cap below evicts the oldest. New keys are appended at the end too.
        if cache_key in _ultra_results_cache:
            _ultra_results_cache.move_to_end(cache_key)
        _ultra_results_cache[cache_key] = response
        # Cap so old (universe, tf, batch) responses don't accumulate forever
        while len(_ultra_results_cache) > _MAX_RESULTS_CACHE_ENTRIES:
            _ultra_results_cache.popitem(last=False)
        _ultra_state["sources"]      = sources
        _ultra_state["completed_at"] = _time.time()
        _ultra_state["running"]      = False
    return response


def get_ultra_status() -> dict:
    with _ultra_lock:
        s = dict(_ultra_state)
    return s


def get_ultra_results(universe: str, tf: str, nasdaq_batch: str = "") -> dict:
    """Return the most recent cached ULTRA response for this (universe, tf, batch).

    If nothing is cached, returns an empty result with a warning so the UI
    can prompt the user to trigger a scan.
    """
    cache_key = (universe, tf, nasdaq_batch or "")
    with _ultra_lock:
        cached = _ultra_results_cache.get(cache_key)
    if cached is None:
        return {
            "results": [], "total": 0, "last_scan": None,
            "warnings": ["No ULTRA scan has run yet for this universe/tf — press ULTRA Scan."],
            "meta": {
                "universe":     universe,
                "tf":           tf,
                "nasdaq_batch": nasdaq_batch or None,
                "sources": {
                    "turbo":           {"ok": False, "count": 0},
                    "tz_wlnbb":        {"ok": False, "count": 0},
                    "tz_intelligence": {"ok": False, "count": 0},
                    "pullback":        {"ok": False, "count": 0},
                    "rare_reversal":   {"ok": False, "count": 0},
                },
            },
        }
    return cached
