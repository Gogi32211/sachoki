"""
ai_journal/fib_levels.py — Fibonacci retracement levels per ticker.

Two anchorings (the user wants a button for both):
  macro  — lowest low / highest high over the last `years` (default 5y).
  swing  — lowest low / highest high over the chart's visible range (from_date→).

Returns the level PRICES (same set either way); the ratio LABEL depends on swing
direction (low-before-high = up-move → 0% at the high, retracements are support).

For the live chart this is just "draw today's levels" — no look-ahead concern.
(The zone-event EDGE analysis, when we add it, must instead use trailing anchors
known at each event date — that's a separate, careful step.)
"""
from __future__ import annotations

import logging
from .db import get_analytics_conn

log = logging.getLogger(__name__)

RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]


def _lbl(r: float) -> str:
    return f"{r:.3f}".rstrip("0").rstrip(".")


def fib_levels(ticker: str, mode: str = "macro", from_date: str | None = None,
               years: int = 5) -> dict:
    tk = ticker.upper()
    a = get_analytics_conn()
    try:
        params: list = [tk]
        where = ""
        if mode == "swing" and from_date:
            where = " AND date >= ?"
            params.append(from_date)
        elif mode == "macro":
            where = f" AND date >= (SELECT max(date) FROM bars) - INTERVAL {int(years)} YEAR"
        r = a.execute(f"""
            SELECT min(low) AS lo, max(high) AS hi,
                   arg_min(date, low)  AS lo_date,
                   arg_max(date, high) AS hi_date
            FROM bars WHERE ticker = ?{where}
        """, params).fetchone()
    finally:
        a.close()
    lo, hi, lo_date, hi_date = r
    if lo is None or hi is None or hi <= lo:
        return {"ticker": tk, "mode": mode, "levels": []}
    rng = hi - lo
    up = str(lo_date) < str(hi_date)   # low before high → up-move
    levels = [{
        "ratio": rt,
        "label": _lbl(rt),
        # up-move: 0% sits at the high; down-move: 0% sits at the low
        "price": round((hi - rt * rng) if up else (lo + rt * rng), 4),
    } for rt in RATIOS]
    return {
        "ticker": tk, "mode": mode,
        "low": round(float(lo), 4), "high": round(float(hi), 4),
        "low_date": str(lo_date)[:10], "high_date": str(hi_date)[:10],
        "direction": "up" if up else "down",
        "levels": levels,
    }
