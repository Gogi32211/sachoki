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


_ACOLS = ("id,ticker,universe,signal_date,open_date,opened_at,entry_px,stop_px,target_px,size_pct,"
          "atomic_score,atoms,regime,status,mark_px,upnl_pct,close_date,exit_px,exit_reason,pnl_pct,verdict")


def _ensure():
    c = get_journal_conn()
    c.execute("""CREATE TABLE IF NOT EXISTS atomic_position (
        id BIGINT PRIMARY KEY, ticker VARCHAR, universe VARCHAR, signal_date DATE, open_date DATE, opened_at TIMESTAMP,
        entry_px DOUBLE, stop_px DOUBLE, target_px DOUBLE, size_pct DOUBLE,
        atomic_score INTEGER, atoms VARCHAR, regime VARCHAR,
        status VARCHAR, mark_px DOUBLE, upnl_pct DOUBLE,
        close_date DATE, exit_px DOUBLE, exit_reason VARCHAR, pnl_pct DOUBLE, verdict VARCHAR)""")
    cols = [r[0] for r in c.execute("DESCRIBE atomic_position").fetchall()]
    if "signal_date" not in cols:
        c.execute("ALTER TABLE atomic_position ADD COLUMN signal_date DATE")
    return c


def _next_open(a, ticker, universe, after_date):
    """OPEN of the first trading bar AFTER the signal date (execution realism — a signal
    fired while the market is closed fills at the NEXT session's open, not the signal
    close). The 5-yr backtest used next-open too. Returns (date, open) or None."""
    r = a.execute("SELECT date, open FROM bars WHERE ticker=? AND universe=? AND date > ? ORDER BY date LIMIT 1",
                  [ticker, universe, after_date]).fetchone()
    return (str(r[0])[:10], float(r[1])) if r and r[1] else None


def open_from_scan(top: int = 15, min_score: int = 70) -> dict:
    from .atomic_scan import atomic_scan
    scan = atomic_scan(max_age_days=2)
    reg = scan["regime"]
    c = _ensure()
    a = get_analytics_conn()
    try:
        held = {r[0] for r in c.execute("SELECT ticker FROM atomic_position WHERE status IN ('OPEN','PENDING')").fetchall()}
        nxt = (c.execute("SELECT coalesce(max(id),0) FROM atomic_position").fetchone()[0] or 0) + 1
        opened, pending = [], []
        cand = [r for r in scan["rows"] if r["score"] >= min_score and r["ticker"] not in held][:top]
        for r in cand:
            size = round(BASE_SIZE_PCT * (reg.get("conv_mult") or 1.0), 2)
            fill = _next_open(a, r["ticker"], r["universe"], r["signal_date"])
            if fill:                                            # next session printed → fill at open
                od, entry = fill
                c.execute(f"INSERT INTO atomic_position ({_ACOLS}) VALUES (?,?,?,?,?,now(),?,?,?,?,?,?,?,'OPEN',?,0,NULL,NULL,NULL,NULL,'PENDING')",
                          [nxt, r["ticker"], r["universe"], r["signal_date"], od, entry,
                           round(entry * (1 - STOP_PCT), 4), round(entry * (1 + TARGET_PCT), 4),
                           size, r["score"], json.dumps(r["atoms"]), reg["label"], entry])
                opened.append(r["ticker"])
            else:                                               # market closed / no next bar yet → PENDING
                c.execute(f"INSERT INTO atomic_position ({_ACOLS}) VALUES (?,?,?,?,NULL,now(),NULL,NULL,NULL,?,?,?,?,'PENDING',NULL,NULL,NULL,NULL,NULL,NULL,'PENDING')",
                          [nxt, r["ticker"], r["universe"], r["signal_date"], size, r["score"],
                           json.dumps(r["atoms"]), reg["label"]])
                pending.append(r["ticker"])
            nxt += 1
    finally:
        a.close()
    c.close()
    return {"opened": opened, "pending": pending, "count": len(opened) + len(pending),
            "regime": reg["label"], "as_of": scan["as_of"]}


def grade() -> dict:
    c = _ensure()
    a = get_analytics_conn()
    # fill PENDING at the next session's open first
    pend = c.execute("SELECT id,ticker,universe,signal_date FROM atomic_position WHERE status='PENDING'").fetchdf()
    filled = 0
    for _, p in pend.iterrows():
        fill = _next_open(a, p["ticker"], p["universe"], p["signal_date"])
        if fill:
            od, entry = fill
            c.execute("""UPDATE atomic_position SET status='OPEN', open_date=?, entry_px=?, stop_px=?, target_px=?, mark_px=? WHERE id=?""",
                      [od, entry, round(entry * (1 - STOP_PCT), 4), round(entry * (1 + TARGET_PCT), 4), entry, int(p["id"])])
            filled += 1
    rows = c.execute("SELECT id,ticker,universe,open_date,entry_px,stop_px,target_px FROM atomic_position WHERE status='OPEN'").fetchdf()
    if len(rows) == 0:
        a.close(); c.close(); return {"graded": 0, "closed": 0, "filled": filled}
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
    return {"graded": int(len(rows)), "closed": closed, "filled": filled}


def replay(months: int = 6, universe: str | None = None, min_score: int = 70,
           dv_floor: float = 500_000, limit: int = 200) -> dict:
    """Historical backtest of the Atomic weak-close gap-up edge over the last `months`,
    using the EXACT journal rules: entry = NEXT session open (the 5-yr backtest's
    convention), -15% stop / +100% target / 20-bar horizon, gap-aware stop-first, one
    open per ticker, equal 4% paper bets."""
    import numpy as np, pandas as pd
    from datetime import date as _d, timedelta
    _BULL_T = ("T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12")
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        uni = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        ph = ",".join(f"'{s}'" for s in _BULL_T)
        df = a.execute(f"""
            SELECT ticker, universe, date, open, high, low, close, volume,
                   full_suffix AS sfx, bar_line5, vol_bucket,
                   CASE WHEN bar_gap_class='G3' THEN 1 ELSE 0 END AS g3
            FROM bars
            WHERE t_sig IN ({ph}) AND close_suffix='O' AND bar_gap_class IN ('G2','G3')
              AND avg_vol_20d > 0 AND close*volume >= {dv_floor} {uni}
              AND date >= DATE '{as_of}' - INTERVAL {int(months * 31) + 10} DAY
            ORDER BY ticker, universe, date
        """).fetchdf()
    finally:
        a.close()
    if len(df) == 0:
        return {"as_of": as_of, "months": months, "trades": [], "stats": {}, "by_month": []}
    sfx = df.sfx.fillna("").astype(str)
    sc = np.full(len(df), 40.0)
    sc += np.where(df.bar_line5.fillna("").astype(str).str.contains("R2L"), 25, 0)
    sc += np.where(sfx.str[:1] == "E", 15, 0)
    sc += np.where(df.vol_bucket == "B", 15, 0)
    sc += np.where(sfx.str[1:2] == "D", 10, 0)
    sc += np.where(df.g3 == 1, 10, 0)
    df["score"] = np.clip(sc, 0, 100).astype(int)
    win_start = (_d.fromisoformat(as_of) - timedelta(days=int(months * 30.5))).isoformat()
    df["dstr"] = df.date.astype(str).str[:10]

    # need the FULL series per ticker for next-open + 20-bar exit walk → reload all bars
    a = get_analytics_conn()
    try:
        tks = tuple(df.ticker.unique())
        ph2 = ",".join("?" * len(tks))
        allb = a.execute(f"""SELECT ticker,universe,date,open,high,low,close FROM bars
            WHERE ticker IN ({ph2}) AND date >= DATE '{as_of}' - INTERVAL {int(months*31)+40} DAY
            ORDER BY ticker,universe,date""", list(tks)).fetchdf()
    finally:
        a.close()
    allb["dstr"] = allb.date.astype(str).str[:10]
    sig = {(r.ticker, r.universe, r.dstr): int(r.score) for r in df.itertuples()
           if r.dstr >= win_start and int(r.score) >= min_score}

    trades, still_open, seen = [], 0, set()
    for (tk, u), grp in allb.groupby(["ticker", "universe"], sort=False):
        o = grp.open.to_numpy(); hi = grp.high.to_numpy(); lo = grp.low.to_numpy(); c = grp.close.to_numpy()
        ds = grp.dstr.to_numpy(); n = len(c)
        last = -999
        for i in range(n):
            scn = sig.get((tk, u, ds[i]))
            if scn is None or i - last < 20:
                continue
            if (tk, ds[i]) in seen:
                continue
            if i + 1 >= n:
                still_open += 1; continue
            entry = o[i + 1]
            if entry <= 0:
                continue
            last = i; seen.add((tk, ds[i]))
            stop = entry * (1 - STOP_PCT); target = entry * (1 + TARGET_PCT)
            eend = min(i + 1 + HORIZON, n - 1)
            exit_px = reason = exit_date = None
            for j in range(i + 2, eend + 1):
                if lo[j] <= stop:
                    exit_px = min(stop, o[j]); reason = "stop"; exit_date = ds[j]; break
                if hi[j] >= target:
                    exit_px = max(target, o[j]); reason = "target"; exit_date = ds[j]; break
            if exit_px is None:
                if i + 1 + HORIZON <= n - 1:
                    exit_px = c[eend]; reason = "time"; exit_date = ds[eend]
                else:
                    still_open += 1; continue
            pnl = round((exit_px / entry - 1) * 100, 2)
            trades.append({"ticker": tk, "universe": u, "signal_date": ds[i], "open_date": ds[i + 1],
                           "close_date": exit_date, "entry": round(entry, 2), "exit": round(exit_px, 2),
                           "pnl": pnl, "reason": reason, "score": scn, "month": ds[i + 1][:7]})

    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    eq, curve = START_CAPITAL, []
    SIZE = BASE_SIZE_PCT / 100.0
    for t in sorted(trades, key=lambda x: x["close_date"]):
        eq += START_CAPITAL * SIZE * (t["pnl"] / 100.0)
        curve.append({"date": t["close_date"], "equity": round(eq, 0)})
    bm = {}
    for t in trades:
        m = bm.setdefault(t["month"], {"month": t["month"], "n": 0, "sum": 0.0, "w": 0})
        m["n"] += 1; m["sum"] += t["pnl"]; m["w"] += 1 if t["pnl"] > 0 else 0
    by_month = [{"month": v["month"], "n": v["n"], "avg_pnl": round(v["sum"] / v["n"], 2),
                 "win_rate": round(v["w"] / v["n"] * 100, 0)} for v in sorted(bm.values(), key=lambda x: x["month"])]
    stats = {
        "n": len(trades), "win_rate": round(len(wins) / len(pnls) * 100, 1) if pnls else None,
        "avg_pnl": round(float(np.mean(pnls)), 2) if pnls else None,
        "median_pnl": round(float(np.median(pnls)), 2) if pnls else None,
        "best": round(max(pnls), 1) if pnls else None, "worst": round(min(pnls), 1) if pnls else None,
        "stop_pct": round(sum(1 for t in trades if t["reason"] == "stop") / len(trades) * 100, 1) if trades else None,
        "target_pct": round(sum(1 for t in trades if t["reason"] == "target") / len(trades) * 100, 1) if trades else None,
        "equity_end": round(eq, 0), "equity_pct": round((eq / START_CAPITAL - 1) * 100, 1),
        "still_open": still_open,
    }
    return {"as_of": as_of, "months": months, "win_start": win_start,
            "stats": stats, "by_month": by_month, "curve": curve,
            "trades": sorted(trades, key=lambda x: x["close_date"], reverse=True)[:limit]}


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
    pend = [r for r in rows if r["status"] == "PENDING"]
    # live mark-to-market for open positions (so "now" / uP&L are real, like the AI Journal)
    live = {}
    if op:
        try:
            from data_polygon import fetch_snapshot
            live = fetch_snapshot([r["ticker"] for r in op])
        except Exception:
            live = {}
    try:
        from . import memory as _mem
        meta = _mem.load_ticker_meta()
    except Exception:
        meta = {}
    open_pnl_live = 0.0
    for r in op:
        m = meta.get(r["ticker"], {}) if isinstance(meta, dict) else {}
        r["mcap_bucket"] = m.get("mcap_bucket") or ""
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
        "open": len(op), "closed": len(cl), "pending": len(pend),
        "win_rate": round(len(wins) / len(realized) * 100, 1) if realized else None,
        "avg_pnl": round(sum(realized) / len(realized), 2) if realized else None,
        "total_realized_pct": round(sum(realized), 2) if realized else 0,
        "open_pnl_live": round(open_pnl_live, 2),
        "equity": round(eq, 2),
        "equity_live": round(eq + open_pnl_live, 2),
    }
    return {"open": op, "closed": cl, "stats": stats,
            "regime": {"label": reg["label"], "score": reg["score"], "conv_mult": reg["conv_mult"]}}
