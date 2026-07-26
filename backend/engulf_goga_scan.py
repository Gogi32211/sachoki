"""
engulf_goga_scan.py — 🥊 Engulf-Goga ACCUMULATION descriptor (2026-07-07).

⚠️ NOT a validated edge — 7 tests (green/red net, net-rise, big-swallow, L46/L34, EMA-stack,
dip-in-trend, retest) all failed the random-same-size control (project_engulf_goga_no_edge).
This is a DESCRIPTOR only: it surfaces where a green bar is absorbing the prior distribution
(swallows more RED than GREEN candles over a lookback) = a visual "accumulation" read, meant
as a canvas to later layer a bar-pattern on top. Score is descriptive strength, not expectancy.

net = swallowed_red − swallowed_green over the last LOOKBACK bars (swallow = current range
covers the prior candle's open and/or close). Higher net = the bar swept up more red = stronger
absorption signature. Shown with Σ🟢/Σ🔴 and the EMA state for context.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import ANALYTICS_DB

LOOKBACK = 34


def engulf_goga_scan(max_age_days: int = 4, dv_floor: float = 3_000_000,
                     min_net: int = 3, limit: int = 120) -> dict:
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT universe, ticker, date, open, high, low, close, rsi_14,
                              volume, avg_vol_20d,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3
                         AND date >= DATE '{as_of}' - INTERVAL 400 DAY)
            SELECT universe, ticker, CAST(date AS VARCHAR)[:10] d, open, high, low, close,
                   rsi_14, volume, avg_vol_20d
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {"rows": [], "count": 0, "as_of": as_of}

    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    fb = np.zeros(len(df)); fr = np.zeros(len(df)); pb = np.zeros(len(df)); pr = np.zeros(len(df))
    for k in range(1, LOOKBACK + 1):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        oi = (po >= L) & (po <= H); ci = (pc >= L) & (pc <= H)
        full = oi & ci; part = oi ^ ci; bull = pc > po; bear = pc < po
        v = ~np.isnan(po) & ~np.isnan(pc)
        fb += (full & bull & v); fr += (full & bear & v)
        pb += (part & bull & v); pr += (part & bear & v)
    df["sg"] = fb + pb; df["sr"] = fr + pr; df["net"] = df["sr"] - df["sg"]
    df["green"] = df["close"] > df["open"]
    for sp in (20, 50, 200):
        df[f"e{sp}"] = g["close"].transform(lambda s, sp=sp: s.ewm(span=sp, adjust=False).mean())
    df["dit"] = (df["e50"] > df["e20"]) & (df["e20"] > df["e200"])

    days = sorted(df["d"].unique(), reverse=True)
    age = {d: i for i, d in enumerate(days)}
    df["age"] = df["d"].map(age)
    cand = df[(df["green"]) & (df["net"] >= min_net) & (df["age"] <= max_age_days - 1)
              & (df["close"] * df["volume"] >= dv_floor) & (df["avg_vol_20d"] > 0)].copy()

    out, seen = [], set()
    for _, r in cand.sort_values(["ticker", "d"], ascending=[True, False]).iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        seen.add(tk)
        dit = bool(r["dit"])
        rsi = float(r["rsi_14"]) if pd.notna(r["rsi_14"]) else 50
        atoms = [f"net+{int(r['net'])}", f"Σ🟢{int(r['sg'])}/🔴{int(r['sr'])}", f"RSI{rsi:.0f}"]
        if dit:
            atoms.append("📉DiT")
        # descriptive strength (NOT expectancy): net magnitude + DiT + oversold context
        score = 55 + min(int(r["net"]), 10) * 3 + (10 if dit else 0) + (6 if rsi < 45 else 0)
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["d"])[:10],
            "close": round(float(r["close"]), 2), "rsi": round(rsi, 0),
            "tz": f"net+{int(r['net'])}", "l_sig": "",
            "net": int(r["net"]), "swall_green": int(r["sg"]), "swall_red": int(r["sr"]),
            "dit": dit, "dv_m": round(float(r["close"] * r["volume"]) / 1e6, 1),
            "score": min(int(score), 100), "atoms": atoms, "age_days": int(r["age"]),
        })
    out.sort(key=lambda x: (-x["net"], -x["score"]))
    try:
        from charged_state import charged_for
        ch = charged_for([r["ticker"] for r in out])
        for r in out:
            if ch.get(r["ticker"]):
                r["charged"] = True; r["atoms"].append("⚡charged")
    except Exception:
        pass
    return {"as_of": as_of, "count": len(out), "rows": out[:limit], "descriptor": True,
            "edge_note": ("🥊 Engulf-Goga — DESCRIPTOR, NOT a validated edge (net failed the "
                          "random-control 7×). Surfaces green bars ABSORBING the prior distribution "
                          "(net = swallowed-RED − swallowed-GREEN over 34 bars ≥ " + str(min_net) +
                          "). Use it to eyeball accumulation zones; the real tradeable STATE is "
                          "📉DiT (e50>e20>e200) + RSI, not the net. Layer a bar-pattern before acting.")}
