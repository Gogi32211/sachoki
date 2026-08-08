"""brain/pending.py — 🎯 PULLBACK entry orders (the validated entry-timing policy, wired live).

Entry-timing research (2026-07-17, project_entry_timing): edges give STATE, the PULLBACK gives
the WHEN. Definition measured: after a fire on bar D, within (D, D+5] a bar UNDERCUTS low[D]
and CLOSES GREEN (dip-and-reclaim) — enter on that bar. Matched Δ vs immediate entry, 6/6yr
on every edge tested: G3-Abs +1.89 · QZ-Capit +0.74 · Atomic +1.88 · Cluster +1.59 pp.
Every strength-chase trigger LOSES on all edges (follow-through −2.0..−2.3, breakout
−2.7..−3.9). "You buy the winners HIGH if you wait for confirmation; the pullback buys the
same fire cheaper."

Cost accepted BY DESIGN (same as the study's matched columns): a fire that never dips back is
never bought. That is the measured trade-off, not a bug — do not add a chase fallback.

JSON-backed like journal.book — paper only, no broker.
"""
from __future__ import annotations
import json
import os

_DIR = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.join(_DIR, "pending.json")
EXPIRE_BARS = 5                      # the measured (D, D+5] window


def _load() -> list:
    if not os.path.exists(_PATH):
        return []
    with open(_PATH) as f:
        return json.load(f)


def _save(orders: list) -> None:
    with open(_PATH, "w") as f:
        json.dump(orders, f, indent=2, ensure_ascii=False)
        f.write("\n")


def list_pending() -> list:
    return _load()


def place(decision: dict, fire_date: str, below: float, apply: bool = True) -> dict:
    """Queue a pullback order from a BUY decision. Idempotent by ticker."""
    order = {"ticker": decision["ticker"], "edge": decision.get("edge"),
             "edge_title": decision.get("edge_title"), "tier": decision.get("tier"),
             "shares": decision.get("shares"), "stop": decision.get("stop"),
             "target": decision.get("target"), "sector": decision.get("sector"),
             "risk_dollars": decision.get("risk_dollars"), "log": decision.get("log"),
             "fire_date": fire_date, "below": round(float(below), 4),
             "planned_entry": decision.get("entry")}
    if not apply:
        return {**order, "preview": True}
    orders = _load()
    if any(o["ticker"] == order["ticker"] for o in orders):
        return {"ticker": order["ticker"], "skipped": "already pending"}
    orders.append(order)
    _save(orders)
    return order


def check_fills(apply: bool = False) -> dict:
    """Walk each pending order over the daily bars AFTER its fire date. First bar whose low
    undercuts `below` AND that closes green fills the order at that bar's CLOSE (we run after
    the close — the same bar the study entered on). More than EXPIRE_BARS bars without a
    trigger → the order expires unfilled (accepted opportunity cost, see module docstring)."""
    import duckdb
    from studio.db import tf_db_path
    from . import journal
    orders = _load()
    if not orders:
        return {"applied": apply, "filled": [], "expired": [], "waiting": []}
    held = {p["ticker"] for p in journal.open_positions()}
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    filled, expired, waiting, keep = [], [], [], []
    for o in orders:
        tk, below = o["ticker"], float(o["below"])
        if tk in held:                        # opened by other means — drop the order
            expired.append({**o, "why": "already open"});  continue
        # GROUP BY date: multi-universe tickers store the same bar once per universe (AAPL ×3)
        bars = con.execute(
            "SELECT CAST(date AS VARCHAR) d, any_value(open) o, any_value(high) h, "
            "any_value(low) l, any_value(close) c FROM bars "
            "WHERE ticker=? AND substr(CAST(date AS VARCHAR),1,10) > ? "
            "GROUP BY date ORDER BY date",
            [tk, o["fire_date"][:10]]).fetchall()
        fill = None
        for i, (d, op, hi, lo, cl) in enumerate(bars):
            if i >= EXPIRE_BARS:
                break
            if float(lo) <= below and float(cl) > float(op):     # dip-and-reclaim
                fill = (str(d)[:10], round(float(cl), 4));  break
        if fill:
            rec = {**{k: o[k] for k in ("ticker", "edge", "edge_title", "tier", "shares",
                                        "stop", "target", "sector", "risk_dollars", "log",
                                        "fire_date", "below")
                      if o.get(k) is not None},
                   "entry": fill[1], "opened": fill[0]}
            if apply:
                try:
                    journal.open_position(rec)
                except Exception as e:
                    keep.append(o);  waiting.append({**o, "warn": str(e)[:80]});  continue
            filled.append({"ticker": tk, "edge": o.get("edge"), "entry": fill[1],
                           "opened": fill[0], "below": below, "preview": not apply})
        elif len(bars) >= EXPIRE_BARS:
            expired.append({**o, "why": f"no dip-and-reclaim in {EXPIRE_BARS} bars"})
        else:
            keep.append(o)
            waiting.append({"ticker": tk, "below": below, "bars_waited": len(bars),
                            "bars_left": EXPIRE_BARS - len(bars)})
    con.close()
    if apply:
        _save(keep)
    return {"applied": apply, "filled": filled, "expired": expired, "waiting": waiting}
