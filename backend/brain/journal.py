"""brain/journal.py — Layer 9 (live): the account state the brain protects.

Tracks open positions + closed trades so the portfolio envelope (Layer 8) runs on REAL state
instead of an empty paper book: open risk, drawdown, and losing-streak feed straight into the
size/allocation math. JSON-backed, isolated. Starts empty (paper) — behaves like before until
the first trade is recorded, then the envelope becomes live.
"""
from __future__ import annotations
import json
import os
from typing import Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.join(_DIR, "book.json")


def _load() -> dict:
    if not os.path.exists(_PATH):
        return {"starting_capital": 10_000.0, "peak_equity": 10_000.0,
                "positions": [], "closed": []}
    with open(_PATH) as f:
        return json.load(f)


def _save(doc: dict) -> None:
    with open(_PATH, "w") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write("\n")


def open_position(pos: dict) -> dict:
    """Record an opened trade (from a brain BUY decision). Idempotent by ticker (one at a time)."""
    doc = _load()
    if any(p["ticker"] == pos["ticker"] for p in doc["positions"]):
        raise ValueError(f"{pos['ticker']} already open")
    keep = ("ticker", "edge", "edge_title", "tier", "shares", "entry", "stop", "target", "sector",
            "risk_dollars", "opened", "log", "regime_risk_mult", "opened_regime",
            "fire_date", "below")   # 🎯 signal-bar date + pullback trigger — the chart's markers
    rec = {k: pos.get(k) for k in keep if pos.get(k) is not None}
    if "opened" not in rec:
        from datetime import date
        rec["opened"] = date.today().isoformat()
    rec["position_value"] = round(rec.get("shares", 0) * rec.get("entry", 0), 2)
    if "fingerprint" not in rec:             # snapshot which signals were active at entry
        try:
            from . import fingerprint
            rec["fingerprint"] = fingerprint.capture(rec["ticker"])
        except Exception:
            rec["fingerprint"] = []
    doc["positions"].append(rec)
    _save(doc)
    try:                                     # the brain asks for what it can't see, at open time
        from . import requests
        requests.raise_for_position(rec)
    except Exception:
        pass
    return rec


def close_position(ticker: str, exit_price: float, reason: str = "") -> dict:
    doc = _load()
    idx = next((i for i, p in enumerate(doc["positions"]) if p["ticker"] == ticker), None)
    if idx is None:
        raise ValueError(f"{ticker} not open")
    p = doc["positions"].pop(idx)
    pnl = round((exit_price - p["entry"]) * p["shares"], 2)
    from datetime import date
    rec = {**p, "exit": round(exit_price, 4), "pnl": pnl, "closed": date.today().isoformat(),
           "reason": reason}
    # ── autopsy: dissect the trade against its edge's base-rate, store + log the lesson ──
    try:
        from . import autopsy
        rec["analysis"] = autopsy.analyze(rec)
    except Exception as e:
        rec["analysis"] = {"verdict": "unknown", "note": f"autopsy failed: {e}"}
    doc["closed"].append(rec)
    # update peak equity on realized gains
    eq = doc["starting_capital"] + sum(c["pnl"] for c in doc["closed"])
    doc["peak_equity"] = max(doc.get("peak_equity", doc["starting_capital"]), eq)
    _save(doc)
    try:                                     # append the lesson to the brain's learning memory
        from .learn import _log_append
        a = rec.get("analysis", {})
        _log_append({"date": rec["closed"], "kind": "trade_autopsy", "edge": rec.get("edge"),
                     "ticker": ticker, "observation": f"{a.get('verdict')} / {a.get('attribution')} "
                     f"({a.get('ret_pct')}%, {a.get('r_multiple')}R)", "action": a.get("lesson", "")})
    except Exception:
        pass
    return rec


def closed_trades() -> list:
    """Closed trades newest-first, each carrying its autopsy (for the UI's win/loss analysis)."""
    return list(reversed(_load()["closed"]))


def open_positions() -> list:
    """The list the portfolio envelope consumes: {sector, risk_dollars, position_value, ...}."""
    return _load()["positions"]


def account_state() -> dict:
    """Realized equity, drawdown (from realized peak), losing-streak, open risk — fed to the brain."""
    doc = _load()
    closed = doc["closed"]
    realized = sum(c["pnl"] for c in closed)
    equity = doc["starting_capital"] + realized
    peak = max(doc.get("peak_equity", doc["starting_capital"]), equity)
    drawdown = (peak - equity) / peak if peak > 0 else 0.0
    # trailing consecutive losses
    streak = 0
    for c in reversed(closed):
        if c["pnl"] < 0:
            streak += 1
        else:
            break
    open_risk = sum(float(p.get("risk_dollars", 0)) for p in doc["positions"])
    return {
        "starting_capital": doc["starting_capital"],
        "equity": round(equity, 2),
        "realized_pnl": round(realized, 2),
        "drawdown": round(drawdown, 4),
        "losing_streak": streak,
        "open_positions": len(doc["positions"]),
        "open_risk": round(open_risk, 2),
        "open_risk_pct": round(open_risk / doc["starting_capital"], 4),
        "n_closed": len(closed),
    }


def reset() -> None:
    """Wipe the paper book back to starting capital."""
    _save({"starting_capital": 10_000.0, "peak_equity": 10_000.0, "positions": [], "closed": []})
