"""
ai_journal/ai_replay.py — historical backtest of the AI Journal's DETERMINISTIC
substrate (the candidate pool + rails exit), WITHOUT the LLM. The live journal lets
Sonnet pick a subset of each day's top rails candidates; this replay instead takes the
top-N systematically, so it measures the underlying edge the agent draws from — a
track record you can see without re-running (or paying for) the LLM per day.

Rules (rails.py): candidate = prebreak_v3 >= 20 AND mcap_bucket not in {micro, unknown};
each day rank by coalesce(prebreak_v4, prebreak_v3), take top_n=12. Entry = NEXT session
OPEN (execution realism). Exit = stop entry − 1.5×ATR / target entry + 5×ATR / 10-bar
horizon, gap-aware, stop-first. Equal 5%-paper bets. Dedup one ticker per day.
"""
from __future__ import annotations
from .db import get_analytics_conn

START_CAPITAL = 10_000.0


def replay(months: int = 6, top_n: int = 12, universe: str | None = None, limit: int = 200) -> dict:
    import numpy as np, pandas as pd
    from datetime import date as _d, timedelta
    from . import rails
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        uni = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        lookback = int(months * 31) + 30
        df = a.execute(f"""
            SELECT ticker, universe, date, open, high, low, close, atr_14, prebreak_v3, prebreak_v4
            FROM bars
            WHERE atr_14 > 0 {uni}
              AND date >= DATE '{as_of}' - INTERVAL {lookback} DAY
            ORDER BY ticker, universe, date
        """).fetchdf()
    finally:
        a.close()
    if len(df) == 0:
        return {"as_of": as_of, "months": months, "trades": [], "stats": {}, "by_month": []}

    try:
        from . import memory as _mem
        meta = _mem.load_ticker_meta() or {}
    except Exception:
        meta = {}
    use_cap = len(meta) > 50   # only apply the cap filter if we actually have mcap data
    df["v3"] = pd.to_numeric(df.prebreak_v3, errors="coerce").fillna(0)
    df["v4"] = pd.to_numeric(df.prebreak_v4, errors="coerce").fillna(0)
    df["v3eff"] = np.where(df.v4 > 0, df.v4, df.v3)
    df["dstr"] = df.date.astype(str).str[:10]
    if use_cap:
        df["cap"] = df.ticker.map(lambda t: (meta.get(t, {}).get("mcap_bucket") or "unknown"))
    else:
        df["cap"] = "mid"   # neutral — don't filter everything out when meta is missing

    win_start = (_d.fromisoformat(as_of) - timedelta(days=int(months * 30.5))).isoformat()
    elig = (df.v3 >= rails.V3_MIN) & (~df.cap.isin(rails.MCAP_BLOCK)) & (df.dstr >= win_start)
    e = df[elig].sort_values("v3eff", ascending=False).drop_duplicates(["ticker", "dstr"])   # one ticker/day
    e = e.copy()
    e["rk"] = e.groupby("dstr")["v3eff"].rank(method="first", ascending=False)
    taken = set(zip(e.loc[e.rk <= top_n, "ticker"], e.loc[e.rk <= top_n, "universe"], e.loc[e.rk <= top_n, "dstr"]))

    H, SM, TM = rails.HORIZON_DAYS, rails.STOP_ATR_MULT, rails.TARGET_ATR_MULT
    trades, still_open = [], 0
    for (tk, u), grp in df.groupby(["ticker", "universe"], sort=False):
        o = grp.open.to_numpy(); hi = grp.high.to_numpy(); lo = grp.low.to_numpy(); c = grp.close.to_numpy()
        atr = grp.atr_14.to_numpy(); ds = grp.dstr.to_numpy(); n = len(c)
        for i in range(n):
            if (tk, u, ds[i]) not in taken:
                continue
            if i + 1 >= n:
                still_open += 1; continue
            entry = o[i + 1]
            atr_i = float(atr[i]) if atr[i] and atr[i] > 0 else entry * 0.03
            if entry <= 0:
                continue
            stop = entry - SM * atr_i; target = entry + TM * atr_i
            eend = min(i + 1 + H, n - 1)
            exit_px = reason = exit_date = None
            for j in range(i + 2, eend + 1):
                if lo[j] <= stop:
                    exit_px = min(stop, o[j]); reason = "stop"; exit_date = ds[j]; break
                if hi[j] >= target:
                    exit_px = max(target, o[j]); reason = "target"; exit_date = ds[j]; break
            if exit_px is None:
                if i + 1 + H <= n - 1:
                    exit_px = c[eend]; reason = "time"; exit_date = ds[eend]
                else:
                    still_open += 1; continue
            pnl = round((exit_px / entry - 1) * 100, 2)
            trades.append({"ticker": tk, "universe": u, "signal_date": ds[i], "open_date": ds[i + 1],
                           "close_date": exit_date, "entry": round(entry, 2), "exit": round(exit_px, 2),
                           "pnl": pnl, "reason": reason, "v3": int(grp.v3.to_numpy()[i]), "month": ds[i + 1][:7]})

    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    eq, curve = START_CAPITAL, []
    SIZE = rails.BASE_POS_PCT
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
        "still_open": still_open, "cap_filter": use_cap,
    }
    return {"as_of": as_of, "months": months, "win_start": win_start, "top_n": top_n,
            "stats": stats, "by_month": by_month, "curve": curve,
            "trades": sorted(trades, key=lambda x: x["close_date"], reverse=True)[:limit]}
