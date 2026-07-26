"""
zone_retest_scan.py — 🔁 Zone-Retest live scanner (validated 2026-07-07, project_zone_retest).

Buy the RETEST (2nd+ touch) of a support zone, NOT the first drop (a knife). Mirrors the
edge_replay E_zoneretest mask exactly:
  support   = causal 25-bar low (shift 3)
  touch     = low within +3% of support & ≥ −10% (not crashed) & CLOSE held above & GREEN bar
  retest    = a touch with ≥1 prior touch in the last 15 bars (been here, left, came back)
  🔥absorb  = a swallowed bar in the last 10 carried L46/L5 VSA (proxy for the +EDGE tier that
              backtests to +1.73/med+0.37/PF1.29 vs base +1.37/PF1.22)
Badges: ⚡charged · ⛔sub200. Suppressor-clean (no bias_dn / vol-extreme). READ-ONLY.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import ANALYTICS_DB


def zone_retest_scan(max_age_days: int = 4, dv_floor: float = 3_000_000, limit: int = 120) -> dict:
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT universe, ticker, date, open, high, low, close, rsi_14,
                              coalesce(l_sig,'') l_sig, volume, avg_vol_20d,
                              CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1
                                        OR sig_vol_20x=1 THEN 1 ELSE 0 END supp,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5
                         AND date >= DATE '{as_of}' - INTERVAL 400 DAY)
            SELECT universe, ticker, CAST(date AS VARCHAR)[:10] d, open, high, low, close,
                   rsi_14, l_sig, volume, avg_vol_20d, supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {"rows": [], "count": 0, "as_of": as_of}

    g = df.groupby("ticker", sort=False)
    df["ref_low"] = g["low"].transform(lambda s: s.rolling(25, min_periods=15).min().shift(3))
    rl = df["ref_low"]
    df["touch"] = (df["low"] <= rl * 1.03) & (df["low"] >= rl * 0.90)
    df["green"] = df["close"] > df["open"]
    df["entry"] = df["touch"] & (df["close"] >= rl) & df["green"] & rl.notna() & (df["supp"] == 0)
    df["prior_touch"] = g["touch"].transform(
        lambda s: s.astype(float).shift(1).rolling(15, min_periods=1).sum()).fillna(0)
    df["retest"] = df["entry"] & (df["prior_touch"] >= 1)
    # 📉 dip-in-trend geometry e50>e20>e200 (validated premium tier: retest & DiT +2.11/med+0.79/
    # PF1.37 vs base +1.37/med−0.06). short-term pulled back below medium, still above long = a
    # real dip in an intact uptrend.
    for sp in (20, 50, 200):
        df[f"e{sp}"] = g["close"].transform(lambda s, sp=sp: s.ewm(span=sp, adjust=False).mean())
    df["dit"] = (df["e50"] > df["e20"]) & (df["e20"] > df["e200"])
    # 🔥 absorb proxy: a swallowed bar (range covers its open/close) in the last 10 carried L46/L5
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    swAbs = np.zeros(len(df))
    for k in range(1, 11):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        fl = g["l_sig"].shift(k).eq("L46").astype(float).to_numpy()   # L46 VSA absorption only
        sw = ((po >= L) & (po <= H)) | ((pc >= L) & (pc <= H))
        swAbs += (sw & ~np.isnan(po) & ~np.isnan(pc) & (fl == 1))
    df["swAbs"] = swAbs

    # freshness: rank distinct trading days latest-first, keep last max_age_days
    days = sorted(df["d"].unique(), reverse=True)
    age = {d: i for i, d in enumerate(days)}
    df["age"] = df["d"].map(age)
    # dip gate: a retest is only actionable in a pullback (RSI≤52) — cuts the broad "any name
    # above its 25-bar low" noise and keeps the tradeable dip-to-support tests.
    cand = df[df["retest"] & (df["age"] <= max_age_days - 1) & (df["rsi_14"] <= 52)
              & (df["close"] * df["volume"] >= dv_floor) & (df["avg_vol_20d"] > 0)].copy()

    out, seen = [], set()
    for _, r in cand.sort_values(["ticker", "d"], ascending=[True, False]).iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        seen.add(tk)
        above = (float(r["close"]) / float(r["ref_low"]) - 1) * 100
        rsi = float(r["rsi_14"]) if pd.notna(r["rsi_14"]) else 50
        absorb = int(r["swAbs"]) >= 2          # ≥2 L46/L5 swallowed (tighter, Engulf-Abs-Lⁿ-style)
        dit = bool(r["dit"])                   # 📉 dip-in-trend premium geometry
        atoms = [f"🔁 retest×{int(r['prior_touch'])}", f"supp ${float(r['ref_low']):.2f}",
                 f"+{above:.0f}%", f"RSI{rsi:.0f}"]
        if dit:
            atoms.append("📉DiT")
        if absorb:
            atoms.append(f"🔥absorb{int(r['swAbs'])}")
        # score: DiT (best tier) + absorb + tight-to-support + dip-RSI; prior-touch minor
        score = 62 + (18 if dit else 0) + (12 if absorb else 0) \
            + (8 if above <= 3 else 4 if above <= 6 else 0) \
            + (6 if 30 <= rsi <= 45 else 0) + min(int(r["prior_touch"]), 3) * 2
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["d"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(float(r["rsi_14"]), 0) if pd.notna(r["rsi_14"]) else None,
            "l_sig": str(r["l_sig"] or ""), "tz": f"retest×{int(r['prior_touch'])}",
            "support": round(float(r["ref_low"]), 2), "pct_above": round(above, 1),
            "prior_touch": int(r["prior_touch"]), "absorb": absorb, "dit": dit,
            "dv_m": round(float(r["close"] * r["volume"]) / 1e6, 1),
            "score": min(int(score), 100), "atoms": atoms, "age_days": int(r["age"]),
        })

    out.sort(key=lambda x: -x["score"])
    # ⚡/⛔ badges via existing helpers
    try:
        from charged_state import charged_for
        from sub200_rally import flags_for
        tks = [r["ticker"] for r in out]
        ch = charged_for(tks); s2 = flags_for(tks)
        for r in out:
            if ch.get(r["ticker"]):
                r["charged"] = True; r["atoms"].append("⚡charged")
            if s2.get(r["ticker"]):
                r["sub200_rally"] = True; r["atoms"].append("⛔sub200")
    except Exception:
        pass
    return {"as_of": as_of, "count": len(out), "rows": out[:limit],
            "edge_note": ("🔁 Zone-Retest — buy the RETEST (2nd+ touch) of a 25-bar support that "
                          "HOLDS (closes above, green), not the first drop (a knife: first-touch "
                          "median −1.80 vs retest −0.06). Base +1.37/PF1.22; 🔥absorb tier (swallows "
                          "an L46/L5 VSA / EDGE signal) +1.73/med+0.37/PF1.29/win51, 4-6yr. Modest "
                          "median — a mean/PF edge that excludes the knife. Pair with an entry trigger.")}
