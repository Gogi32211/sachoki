"""
ai_journal/atomic_journal.py — a SEPARATE paper-trading journal for the atomic
"weak-close gap-up" edge. Independent of the main AI Journal (its own table,
its own rules). Opens paper positions from the atomic scan, sizes them by the
market regime, and grades them with the backtested exit (−15% stop / +100% target,
gap-aware, 20-bar horizon). Paper only — no real orders.

Rules (from the 5-year backtest): entry = the signal bar's close (paper fill),
stop = entry × 0.85, target = entry × 2.00, horizon 20 bars. Size = base × regime
conv_mult (RISK_OFF → ×0.7). Dedup: one open position per ticker.
"""
from __future__ import annotations
import json, time
from .db import get_journal_conn, get_analytics_conn

STOP_PCT, TARGET_PCT, HORIZON = 0.15, 1.00, 20
BASE_SIZE_PCT = 4.0          # % of (paper) capital per position before regime scaling
START_CAPITAL = 10_000.0


def _ensure():
    c = get_journal_conn()
    c.execute("""CREATE TABLE IF NOT EXISTS atomic_position (
        id BIGINT PRIMARY KEY, ticker VARCHAR, universe VARCHAR, open_date DATE, opened_at TIMESTAMP,
        entry_px DOUBLE, stop_px DOUBLE, target_px DOUBLE, size_pct DOUBLE,
        atomic_score INTEGER, atoms VARCHAR, regime VARCHAR,
        status VARCHAR, mark_px DOUBLE, upnl_pct DOUBLE,
        close_date DATE, exit_px DOUBLE, exit_reason VARCHAR, pnl_pct DOUBLE, verdict VARCHAR)""")
    return c


def open_from_scan(top: int = 15, min_score: int = 70) -> dict:
    from .atomic_scan import atomic_scan
    scan = atomic_scan(max_age_days=2)
    reg = scan["regime"]
    c = _ensure()
    open_tk = {r[0] for r in c.execute("SELECT ticker FROM atomic_position WHERE status='OPEN'").fetchall()}
    nxt = (c.execute("SELECT coalesce(max(id),0) FROM atomic_position").fetchone()[0] or 0) + 1
    opened = []
    cand = [r for r in scan["rows"] if r["score"] >= min_score and r["ticker"] not in open_tk][:top]
    for r in cand:
        entry = r["close"]
        if not entry or entry <= 0:
            continue
        size = round(BASE_SIZE_PCT * (reg.get("conv_mult") or 1.0), 2)
        c.execute("""INSERT INTO atomic_position VALUES (?,?,?,?,now(),?,?,?,?,?,?,?,'OPEN',?,0,NULL,NULL,NULL,NULL,'PENDING')""",
                  [nxt, r["ticker"], r["universe"], r["signal_date"], entry,
                   round(entry * (1 - STOP_PCT), 4), round(entry * (1 + TARGET_PCT), 4),
                   size, r["score"], json.dumps(r["atoms"]), reg["label"], entry])
        opened.append(r["ticker"]); nxt += 1
    c.close()
    return {"opened": opened, "count": len(opened), "regime": reg["label"], "as_of": scan["as_of"]}


def grade() -> dict:
    c = _ensure()
    rows = c.execute("SELECT id,ticker,universe,open_date,entry_px,stop_px,target_px FROM atomic_position WHERE status='OPEN'").fetchdf()
    if len(rows) == 0:
        c.close(); return {"graded": 0, "closed": 0}
    a = get_analytics_conn()
    closed = 0
    try:
        for _, p in rows.iterrows():
            bars = a.execute("""SELECT date,open,high,low,close FROM bars
                WHERE ticker=? AND universe=? AND date > ? ORDER BY date LIMIT ?""",
                [p["ticker"], p["universe"], p["open_date"], HORIZON]).fetchdf()
            if len(bars) == 0:
                continue
            entry, stop, tgt = float(p["entry_px"]), float(p["stop_px"]), float(p["target_px"])
            exit_px = exit_reason = exit_date = None
            for _, b in bars.iterrows():
                o, h, l = float(b["open"]), float(b["high"]), float(b["low"])
                if l <= stop:                                   # stop-first (conservative)
                    exit_px = min(stop, o); exit_reason = "stop"; exit_date = b["date"]; break
                if h >= tgt:
                    exit_px = max(tgt, o); exit_reason = "target"; exit_date = b["date"]; break
            if exit_px is None and len(bars) >= HORIZON:        # horizon reached, no hit
                lastb = bars.iloc[-1]; exit_px = float(lastb["close"]); exit_reason = "time"; exit_date = lastb["date"]
            mark = float(bars.iloc[-1]["close"])
            if exit_px is not None:
                pnl = round((exit_px / entry - 1) * 100, 2)
                verdict = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
                c.execute("""UPDATE atomic_position SET status='CLOSED', close_date=?, exit_px=?, exit_reason=?,
                    pnl_pct=?, verdict=?, mark_px=?, upnl_pct=? WHERE id=?""",
                    [exit_date, round(exit_px, 4), exit_reason, pnl, verdict, round(exit_px, 4), pnl, int(p["id"])])
                closed += 1
            else:
                c.execute("UPDATE atomic_position SET mark_px=?, upnl_pct=? WHERE id=?",
                          [round(mark, 4), round((mark / entry - 1) * 100, 2), int(p["id"])])
    finally:
        a.close()
    c.close()
    return {"graded": int(len(rows)), "closed": closed}


def summary() -> dict:
    import numpy as np, pandas as pd
    c = _ensure()
    df = c.execute("SELECT * FROM atomic_position ORDER BY status, open_date DESC").fetchdf()
    c.close()
    df = df.replace({np.nan: None})
    rows = df.to_dict("records") if len(df) else []
    def _clean(v):
        if isinstance(v, (np.integer,)): return int(v)
        if isinstance(v, (np.floating,)): return None if pd.isna(v) else float(v)
        return v
    rows = [{k: _clean(v) for k, v in r.items()} for r in rows]
    for r in rows:
        for k in ("open_date", "close_date", "opened_at"):
            if r.get(k) is not None:
                r[k] = str(r[k])[:19]
        try:
            r["atoms"] = json.loads(r.get("atoms") or "[]")
        except Exception:
            r["atoms"] = []
    op = [r for r in rows if r["status"] == "OPEN"]
    cl = [r for r in rows if r["status"] == "CLOSED"]
    # live mark-to-market for open positions (so "now" / uP&L are real, like the AI Journal)
    live = {}
    if op:
        try:
            from data_polygon import fetch_snapshot
            live = fetch_snapshot([r["ticker"] for r in op])
        except Exception:
            live = {}
    open_pnl_live = 0.0
    for r in op:
        r["dollar_buy"] = round(START_CAPITAL * (r.get("size_pct") or 0) / 100.0, 0)
        lp = (live.get(r["ticker"]) or {}).get("price")
        if lp and r.get("entry_px"):
            r["now_px"] = round(float(lp), 4)
            r["upnl_pct"] = round((float(lp) / r["entry_px"] - 1) * 100, 2)
        else:
            r["now_px"] = r.get("mark_px")
        if r.get("upnl_pct") is not None and r.get("size_pct"):
            open_pnl_live += START_CAPITAL * (r["size_pct"] / 100.0) * (r["upnl_pct"] / 100.0)
    realized = [r["pnl_pct"] for r in cl if r.get("pnl_pct") is not None]
    wins = [x for x in realized if x > 0]
    # paper equity: each position weighted by its size_pct
    eq = START_CAPITAL
    for r in cl:
        if r.get("pnl_pct") is not None and r.get("size_pct"):
            eq += START_CAPITAL * (r["size_pct"] / 100.0) * (r["pnl_pct"] / 100.0)
    try:
        from . import regime as _reg
        reg = _reg.compute_regime()
    except Exception:
        reg = {"label": "NEUTRAL", "score": None, "conv_mult": 1.0}
    stats = {
        "open": len(op), "closed": len(cl),
        "win_rate": round(len(wins) / len(realized) * 100, 1) if realized else None,
        "avg_pnl": round(sum(realized) / len(realized), 2) if realized else None,
        "total_realized_pct": round(sum(realized), 2) if realized else 0,
        "open_pnl_live": round(open_pnl_live, 2),
        "equity": round(eq, 2),
        "equity_live": round(eq + open_pnl_live, 2),
    }
    return {"open": op, "closed": cl, "stats": stats,
            "regime": {"label": reg["label"], "score": reg["score"], "conv_mult": reg["conv_mult"]}}
