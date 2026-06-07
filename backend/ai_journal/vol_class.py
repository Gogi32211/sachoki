"""
ai_journal/vol_class.py — per-bar volume-class (Bollinger-bucket) overlays.

The `bars.vol_bucket` column is the pre-computed TZ_WLNBB volume class for each
bar: one of W / L / N / B / VB (Bollinger Bands on volume, period 20, ±1 std).
  VB = "Very Big"  (volume ≥ 2·mean + 1·std — exceptional interest)
  W  = "Weak"      (volume <  mean − 1·std   — dried-up / low interest)

These differ from the HV-Zone x2/x5/x10 tiers, which measure volume as a raw
MULTIPLE of avg_vol_20d. VB/W are RELATIVE to the bar's own recent volatility.

`history_vol_class` returns the [low, high] of every bar of a given class for a
ticker, so the chart can draw horizontal S/R lines at those levels (same shape
as zone_retest.history_for_ticker — {zones: [{trigger_date, zone_low, zone_high}]}).
"""
from __future__ import annotations

import logging

from .db import get_analytics_conn

log = logging.getLogger(__name__)

VALID_CLASSES = {"W", "L", "N", "B", "VB"}


def history_vol_class(ticker: str, cls: str = "VB",
                      from_date: str | None = None, limit: int = 500) -> dict:
    """ALL bars of volume-class `cls` for this ticker — chart overlay source.

    cls: one of W / L / N / B / VB (case-insensitive). Defaults to VB.
    from_date: optional lower bound (matches the chart's earliest visible bar
    so we don't draw lines off-screen). Capped at `limit` newest bars.
    """
    cls = (cls or "VB").upper()
    if cls not in VALID_CLASSES:
        return {"ticker": ticker.upper(), "cls": cls, "count": 0, "zones": [],
                "error": f"invalid class {cls!r}; expected one of {sorted(VALID_CLASSES)}"}

    a = get_analytics_conn()
    try:
        params: list = [ticker.upper(), cls]
        where_date = ""
        if from_date:
            where_date = " AND date >= ?"
            params.append(from_date)
        params.append(limit)
        rows = a.execute(f"""
            SELECT date, universe, low, high,
                   CASE WHEN close > open THEN 'bull' ELSE 'bear' END AS direction,
                   volume / NULLIF(avg_vol_20d, 0) AS vol_mult
            FROM bars
            WHERE ticker = ? AND vol_bucket = ?
              AND high > low{where_date}
            ORDER BY date DESC
            LIMIT ?
        """, params).fetchall()
    finally:
        a.close()

    # Same-level-across-universes de-dupe (a dual-listed ticker has identical
    # bars in sp500 & nasdaq — collapse to one line per (date, low, high)).
    seen = set()
    zones = []
    for d, _u, lo, hi, dr, vm in rows:
        key = (str(d)[:10], round(float(lo), 4), round(float(hi), 4))
        if key in seen:
            continue
        seen.add(key)
        zones.append({
            "trigger_date": str(d)[:10],
            "zone_low":     round(float(lo), 4),
            "zone_high":    round(float(hi), 4),
            "direction":    dr,
            "vol_mult":     round(float(vm), 1) if vm is not None else None,
        })
    zones.sort(key=lambda z: z["trigger_date"])  # chronological for the chart
    return {"ticker": ticker.upper(), "cls": cls,
            "from_date": from_date, "count": len(zones), "zones": zones}


def active_tickers(cls: str = "VB") -> dict:
    """Tickers whose LATEST bar is volume-class `cls`. Powers the Ultra filter
    chip (VB / W) — mirrors gann_zones.active_tickers."""
    cls = (cls or "VB").upper()
    if cls not in VALID_CLASSES:
        return {"cls": cls, "tickers": [], "count": 0, "error": "invalid class"}
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        rows = a.execute("""
            WITH latest AS (
              SELECT ticker, vol_bucket,
                     ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
              FROM bars
            )
            SELECT DISTINCT ticker FROM latest WHERE rn = 1 AND vol_bucket = ?
            ORDER BY ticker
        """, [cls]).fetchall()
    finally:
        a.close()
    tickers = [r[0] for r in rows]
    return {"as_of": as_of, "cls": cls, "tickers": tickers, "count": len(tickers)}
