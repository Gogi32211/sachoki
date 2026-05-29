"""
studio/edge_scanner.py — "Today's Best Setups" scanner.

For each ticker in the universe, takes its most recent 3-bar profile, queries
the historical DB for matching sequences, computes outcome statistics, and
ranks all tickers by Pattern Quality Score (PQS).

PQS formula:
    PQS = (HH% - 50) × avg_gain × log10(n + 1) / max(|avg_drawdown|, 1)

Higher PQS = stronger BUY edge.
Lower (negative) PQS = stronger SELL edge (rising drawdown, falling avg_gain).

Output is cached to /tmp/studio_edge_scan_results.json for fast UI loading.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from studio.db import get_conn
from studio.signal_stats import query_exact_sequence

log = logging.getLogger(__name__)

PROGRESS_FILE = "/tmp/studio_edge_scan_progress.json"
RESULTS_FILE  = "/tmp/studio_edge_scan_results.json"

# Default strictness: line1 (TZ) + line2 (L) only — balanced edge discovery
_DEFAULT_STRICTNESS = {
    "line1": True,  "line2": True,
    "line3": False, "line4": False, "line5": False, "line6": False,
}

# Cache key fields (must match strictness levels for cache to be valid)
_CACHE_FIELDS_BY_LINE = {
    "line1": ["t", "z"],
    "line2": ["l_str"],
    "line3": ["suffix"],
    "line4": ["body_wick"],
    "line5": ["gap_range"],
    "line6": ["line5_code"],
}


def _write_progress(stage: str, done: int, total: int, started_at: float,
                    extra: dict | None = None) -> None:
    elapsed = time.time() - started_at
    pct = round(done / total * 100, 1) if total else 0
    eta = round(elapsed / done * (total - done)) if done > 0 else None
    payload = {
        "stage": stage, "done": done, "total": total, "pct": pct,
        "elapsed_seconds": round(elapsed, 1), "eta_seconds": eta,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if extra: payload.update(extra)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)


def compute_pqs(stats: dict) -> float:
    """Pattern Quality Score v2 — robust, direction-consistent edge metric.

    Lessons from v1 (had SPAC/penny-stock noise):
      - avg_pct_to_hh is OUTLIER-biased: a few +100% pumps dominate the average.
        Replaced with avg_fwd_10d (fixed-horizon close-to-close).
      - Tiny drawdown (0.15%) caused division-by-near-zero → fake huge PQS.
        Risk floor of 2.0% prevents this.
      - HH bias alone isn't enough — must AGREE with forward return direction.
        Added direction-consistency check.

    Formula:
      direction  = +1 if HH > 50 AND fwd_10d > 0  (bull)
                   -1 if HH < 50 AND fwd_10d < 0  (bear)
                   0  otherwise (noise — patterns disagreeing internally)

      strength   = |HH - 50| + |win_10d - 50|      # probability edge
      magnitude  = |fwd_10d|                        # %, robust horizon
      risk       = max(|avg_pct_to_hl|, 2.0)       # capped floor
      sample_w   = min(n / 100, 1.0)               # capped at n=100
      consistency = (fraction of fwd_5/10/20 agreeing with direction)

      PQS = direction × (strength × magnitude × sample_w × consistency) / risk

    Result: positive for clear BULL setups, negative for clear BEAR setups, ~0 for noise.
    Typical good signals are in the +20 to +80 range.
    """
    n = stats.get("matches", 0)
    if n < 20:
        return 0.0
    o = stats.get("outcomes", {}) or {}
    hh    = o.get("hh_pct")
    fwd10 = o.get("avg_fwd_10d")
    if hh is None or fwd10 is None:
        return 0.0

    # Direction must be consistent
    bull = (hh > 50.0) and (fwd10 > 0.0)
    bear = (hh < 50.0) and (fwd10 < 0.0)
    if not (bull or bear):
        return 0.0   # internal disagreement = noise
    direction = 1 if bull else -1

    win10 = o.get("win_10d_pct")
    dd    = o.get("avg_pct_to_hl")
    fwd5  = o.get("avg_fwd_5d")  or 0.0
    fwd20 = o.get("avg_fwd_20d") or 0.0

    # Strength: edge over 50% probability
    win_edge = (win10 - 50.0) if win10 is not None else 0.0
    strength = abs(hh - 50.0) + abs(win_edge)

    # Magnitude: robust 10d horizon (not outlier-biased)
    magnitude = abs(fwd10)

    # Risk floor at 2% — prevents division-by-tiny SPAC patterns
    risk = max(abs(dd) if dd is not None else 2.0, 2.0)

    # Sample confidence — caps at n=100 (no inflation past statistical adequacy)
    sample_w = min(n / 100.0, 1.0)

    # Consistency: do 5/10/20d all agree on direction?
    target_sign = 1 if bull else -1
    agree = sum(1 for f in (fwd5, fwd10, fwd20)
                if (1 if f > 0 else -1) == target_sign)
    consistency = agree / 3.0   # 0.33, 0.67, or 1.0

    pqs = direction * (strength * magnitude * sample_w * consistency) / risk
    return round(pqs, 2)


def _ticker_sequence_key(bars: list[dict], strictness: dict) -> tuple:
    """Build a cache key from bars that captures only the strictness-enabled
    fields. Different tickers with the same observable profile share stats."""
    key_parts = []
    for b in bars:
        for line, fields in _CACHE_FIELDS_BY_LINE.items():
            if strictness.get(line):
                for f in fields:
                    key_parts.append(b.get(f, ""))
    return tuple(key_parts)


def _bar_from_row(row: pd.Series) -> dict:
    """Convert a DB row into the bar dict format expected by query_exact_sequence."""
    t = row.get("t_sig") or ""
    z = row.get("z_sig") or ""
    return {
        "tz":        t or z,
        "l":         row.get("l_sig") or "",
        "suffix":    row.get("composite_full_suffix") or row.get("full_suffix") or "",
        "body_wick": row.get("bar_body_wick") or "",
        "gap_range": row.get("bar_gap_range") or "",
        "line5":     row.get("bar_line5") or "",
        # Used for cache-key building
        "t": t, "z": z,
        "l_str":      row.get("l_sig") or "",
        "line5_code": row.get("bar_line5") or "",
    }


def run_edge_scan(
    universes:   list[str] | None = None,
    strictness:  dict | None      = None,
    n_bars:      int = 3,
    min_matches: int = 20,
    pivot_lr:    int = 3,
    min_price:   float = 15.0,     # NEW: skip SPACs/penny stocks
    min_volume:  int   = 100_000,  # NEW: skip illiquid tickers
) -> dict:
    """Run the edge scan across all tickers.

    Filters tradeable universe (price >= min_price, volume >= min_volume on
    the current bar) BEFORE scanning, to avoid penny/SPAC pollution.

    Returns dict with:
      - all_results: list of per-ticker rows (each: ticker, sequence, stats, pqs)
      - top_buys:    top 50 by PQS
      - top_sells:   bottom 50 by PQS
      - by_quality:  top 50 by HH% × 10d fwd (high prob + high mag bulls)
      - duration_sec
    """
    universes = universes or ["sp500", "nasdaq"]
    strictness = {**_DEFAULT_STRICTNESS, **(strictness or {})}
    started = time.time()
    _write_progress("loading recent bars", 0, 0, started,
                    extra={"min_price": min_price, "min_volume": min_volume})

    # ── Fetch each ticker's most recent n_bars from DB ────────────────────
    conn = get_conn(read_only=True)
    try:
        placeholders = ",".join("?" * len(universes))
        df = conn.execute(f"""
            WITH ranked AS (
              SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY ticker, universe ORDER BY date DESC) AS rn
              FROM bars
              WHERE universe IN ({placeholders})
            )
            SELECT * FROM ranked WHERE rn <= ?
            ORDER BY ticker, universe, date ASC
        """, [*universes, n_bars]).fetchdf()
    finally:
        conn.close()

    # Group by (ticker, universe) and build sequences
    grouped = df.groupby(["ticker", "universe"])
    total = len(grouped)
    log.info("Edge scan: %d ticker-universe pairs to process", total)
    _write_progress("scanning", 0, total, started)

    results: list[dict] = []
    sequence_cache: dict[tuple, dict] = {}
    done = 0
    skipped_price  = 0
    skipped_vol    = 0
    skipped_no_tz  = 0
    last_date_by_universe: dict[str, str] = {}

    # NOTE: each query below opens its own short-lived read-only connection.
    # A shared connection held open for the whole scan was tried but reverted —
    # holding one read-only connection for ~10-15 min blocks the daily write
    # (incremental refresh / enrich) on this single-file DuckDB and could stall
    # the scan. Per-ticker open/close releases the file between tickers so writes
    # can still slip in. (A proper speed-up = batching all tickers into one query.)
    for (ticker, universe), group in grouped:
        if len(group) < n_bars:
            done += 1
            continue
        bars = [_bar_from_row(r) for _, r in group.iterrows()]   # oldest → newest
        cur = bars[-1]

        # ── Universe filters (kills SPACs / penny / illiquid) ───────────────
        cur_row = group.iloc[-1]
        cur_close  = float(cur_row.get("close")  or 0)
        cur_volume = float(cur_row.get("volume") or 0)
        if cur_close < min_price:
            skipped_price += 1
            done += 1
            continue
        if cur_volume < min_volume:
            skipped_vol += 1
            done += 1
            continue

        # Skip if current bar has no TZ signal — nothing to predict on
        if not cur["t"] and not cur["z"]:
            skipped_no_tz += 1
            done += 1
            continue

        last_date = str(group.iloc[-1]["date"])
        last_date_by_universe[universe] = max(last_date, last_date_by_universe.get(universe, "0000"))

        cache_key = _ticker_sequence_key(bars, strictness)
        if cache_key in sequence_cache:
            stats = sequence_cache[cache_key]
        else:
            try:
                stats = query_exact_sequence(
                    bars=bars, universe=None,            # match across full DB
                    strictness=strictness, pivot_lr=pivot_lr,
                )
            except Exception as e:
                log.debug("query failed for %s/%s: %s", ticker, universe, e)
                stats = {"matches": 0, "outcomes": {}, "sequence_label": ""}
            sequence_cache[cache_key] = stats

        matches = stats.get("matches", 0)
        if matches < min_matches:
            done += 1
            if done % 100 == 0:
                _write_progress("scanning", done, total, started,
                                {"cached_sequences": len(sequence_cache)})
            continue

        pqs = compute_pqs(stats)
        o   = stats.get("outcomes", {}) or {}
        results.append({
            "ticker":         ticker,
            "universe":       universe,
            "last_date":      last_date,
            "close":          round(cur_close, 2),
            "volume":         int(cur_volume),
            "sequence_label": stats.get("sequence_label", ""),
            "tz":             cur["t"] or cur["z"],
            "l":              cur["l"],
            "suffix":         cur["suffix"],
            "body_wick":      cur["body_wick"],
            "gap_range":      cur["gap_range"],
            "line5":          cur["line5"],
            "matches":        matches,
            "hh_pct":         o.get("hh_pct"),
            "hl_pct":         o.get("hl_pct"),
            "avg_pct_to_hh":  o.get("avg_pct_to_hh"),
            "avg_pct_to_hl":  o.get("avg_pct_to_hl"),
            "avg_bars_to_hh": o.get("avg_bars_to_hh"),
            "avg_bars_to_hl": o.get("avg_bars_to_hl"),
            "avg_fwd_5d":     o.get("avg_fwd_5d"),
            "avg_fwd_10d":    o.get("avg_fwd_10d"),
            "avg_fwd_20d":    o.get("avg_fwd_20d"),
            "win_5d_pct":     o.get("win_5d_pct"),
            "win_10d_pct":    o.get("win_10d_pct"),
            "win_20d_pct":    o.get("win_20d_pct"),
            "pqs":            pqs,
        })

        done += 1
        if done % 100 == 0 or done == total:
            _write_progress("scanning", done, total, started,
                            {"cached_sequences": len(sequence_cache),
                             "results_so_far":   len(results)})

    # ── Ranking ───────────────────────────────────────────────────────────
    # Top BUYs:  only positive PQS (bullish setups), sorted descending
    # Top SELLs: only negative PQS (bearish setups), sorted by |PQS| descending
    bull_results = [r for r in results if (r.get("pqs") or 0) > 0]
    bear_results = [r for r in results if (r.get("pqs") or 0) < 0]
    bull_results.sort(key=lambda r: r["pqs"] or 0,         reverse=True)
    bear_results.sort(key=lambda r: abs(r["pqs"] or 0),    reverse=True)
    top_buys  = bull_results[:50]
    top_sells = bear_results[:50]

    # Sweet Spot: stricter quality bar
    #   HH >= 60, 10d fwd >= +2%, win_10d >= 55%, drawdown <= 5%, n >= 30
    by_quality = sorted(
        [r for r in results
         if (r.get("hh_pct")       or 0)    >= 60
         and (r.get("avg_fwd_10d") or -999) >= 2.0
         and (r.get("win_10d_pct") or 0)    >= 55
         and abs(r.get("avg_pct_to_hl") or 99) <= 5.0
         and r.get("matches", 0) >= 30
         and (r.get("pqs") or 0)            > 0],
        key=lambda r: r.get("pqs") or 0,
        reverse=True,
    )[:50]

    summary = {
        "scanned_at":      datetime.now(timezone.utc).isoformat(),
        "universes":       universes,
        "n_bars":          n_bars,
        "strictness":      strictness,
        "min_matches":     min_matches,
        "min_price":       min_price,
        "min_volume":      min_volume,
        "pivot_lr":        pivot_lr,
        "total_tickers":   total,
        "qualifying":      len(results),
        "skipped_price":   skipped_price,
        "skipped_volume":  skipped_vol,
        "skipped_no_tz":   skipped_no_tz,
        "cached_seqs":     len(sequence_cache),
        "last_data_date":  last_date_by_universe,
        "duration_sec":    round(time.time() - started, 1),
        "top_buys":        top_buys,
        "top_sells":       top_sells,
        "by_quality":      by_quality,
        "all_results":     results,
    }

    # Persist to disk for fast UI loading
    with open(RESULTS_FILE, "w") as f:
        json.dump(summary, f)

    _write_progress("done", done, total, started,
                    {"qualifying": len(results),
                     "cached_seqs": len(sequence_cache)})
    log.info("Edge scan complete: %d qualifying tickers in %.1fs",
             len(results), time.time() - started)
    return summary


def get_progress() -> dict:
    if not os.path.exists(PROGRESS_FILE):
        return {"stage": "idle", "done": 0, "total": 0, "pct": 0}
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"stage": "unknown", "done": 0, "total": 0, "pct": 0}


def get_cached_results() -> dict | None:
    if not os.path.exists(RESULTS_FILE):
        return None
    try:
        with open(RESULTS_FILE) as f:
            return json.load(f)
    except Exception:
        return None
