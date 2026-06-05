"""
backtest_pnl.py — walk-forward backtest on realized P&L edge (not HH).

Same conditional-aggregate batching as backtest.py (HH version), but the metric
is the asymmetric-clip P&L of fwd_H (matches pnl_metric.py for the chosen H).

Used by enumerate_greedy to grow combos up to 5 atoms keeping only those with
real (positive, OOS-confirmed) P&L edge.
"""
from __future__ import annotations

import math
import time
import logging

from ai_journal.db import get_analytics_conn

log = logging.getLogger(__name__)

# Brownian-scaled caps so the metric is comparable across horizons.
STOP_BASE_5   = 2.0
TARGET_BASE_5 = 5.0


def caps_for(h: int) -> tuple[float, float]:
    f = math.sqrt(h / 5.0)
    return round(STOP_BASE_5 * f, 3), round(TARGET_BASE_5 * f, 3)


def _pop_for(h: int) -> str:
    return f"fwd_{h}d IS NOT NULL AND fwd_{h}d BETWEEN -90 AND 500"


def baseline_pnl(conn, h: int, start: str, end: str) -> dict:
    s, t = caps_for(h)
    clipped = f"greatest(-{s}, least({t}, fwd_{h}d))"
    r = conn.execute(f"""
        SELECT count(*) n, avg({clipped}) avg_clip,
               avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) win
        FROM bars WHERE date BETWEEN ? AND ? AND {_pop_for(h)}
    """, [start, end]).fetchone()
    return {"n": int(r[0] or 0), "avg_clip": float(r[1] or 0), "win": float(r[2] or 0)}


def batch_pnl(conn, combos: list[dict], h: int, start: str, end: str,
              batch_size: int = 20) -> dict:
    """combos[i] = {combo_id, sql}. Returns {combo_id: {n, avg_clip, win}}."""
    if not combos:
        return {}
    s, t = caps_for(h)
    clipped = f"greatest(-{s}, least({t}, fwd_{h}d))"
    out = {}
    for i in range(0, len(combos), batch_size):
        chunk = combos[i:i + batch_size]
        aggs = []
        for c in chunk:
            cid = c["combo_id"]
            cond = c["sql"]
            f = f"FILTER (WHERE ({cond}))"
            aggs += [
                f"count(*) {f} AS n_{cid}",
                f"avg({clipped}) {f} AS a_{cid}",
                f"avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) {f} AS w_{cid}",
            ]
        sql = f"SELECT {', '.join(aggs)} FROM bars WHERE date BETWEEN ? AND ? AND {_pop_for(h)}"
        row = conn.execute(sql, [start, end]).fetchone()
        for j, c in enumerate(chunk):
            n = int(row[j * 3] or 0)
            out[c["combo_id"]] = {
                "n": n,
                "avg_clip": float(row[j * 3 + 1] or 0) if n else 0.0,
                "win":      float(row[j * 3 + 2] or 0) if n else 0.0,
            }
    return out
