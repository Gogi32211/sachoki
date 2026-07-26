"""
highbase_scan.py — 🧗 High-Base 15m-Dip live scanner (validated 2026-07-08).

The board's first HIGH-BASE setup — fills the RGTI-2025 gap (strong uptrend re-accumulation
whose dips never get daily-oversold, so every dip/reversal scanner stays silent).
  ctx (daily):  close > EMA200 · RSI_1d 40-60 · close ≥ 85% of 20d-high · GREEN bar
  trigger:      the day's MIN 15m RSI ≤ 28 (intraday washout inside the base)
Backtest: +1.86/med+0.34/PF1.31/win50.8/5-6yr vs random-same-size +1.37±0.08 (6.0σ).
Modest tier (Zone-Retest-E class) — pair with your own entry judgment.

Live 15m RSI is computed from the nightly-topped-up 15m BASE (Wilder ewm, pandas) — the
enriched 15m DB is static, so we don't depend on it here. READ-ONLY.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import ANALYTICS_DB, db_path


def _m15_minrsi(as_of: str, days: int = 14) -> pd.DataFrame:
    """per (ticker, ET-day) MIN 15m RSI over the last `days` sessions, RSI computed
    from the 15m base closes (Wilder ewm alpha=1/14, ~15d warmup pulled)."""
    c = duckdb.connect(db_path("studio_15m_base.duckdb"), read_only=True)
    try:
        f = c.execute(f"""
            SELECT ticker, date, close FROM bars
            WHERE date >= DATE '{as_of}' - INTERVAL {days + 18} DAY
            ORDER BY ticker, date""").fetchdf()
    finally:
        c.close()
    if f.empty:
        return pd.DataFrame(columns=["ticker", "d", "rsi15"])
    g = f.groupby("ticker", sort=False)["close"]
    delta = g.diff()
    up = delta.clip(lower=0); dn = (-delta).clip(lower=0)
    # Wilder RSI via grouped ewm
    au = up.groupby(f["ticker"]).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    ad = dn.groupby(f["ticker"]).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False).mean())
    f["rsi"] = 100 - 100 / (1 + au / ad.replace(0, np.nan))
    f["d"] = (pd.to_datetime(f["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    out = f.groupby(["ticker", "d"], sort=False)["rsi"].min().reset_index()
    return out.rename(columns={"rsi": "rsi15"})


def highbase_scan(max_age_days: int = 4, dv_floor: float = 3_000_000, limit: int = 120) -> dict:
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT universe, ticker, date, open, high, low, close, rsi_14,
                              volume, avg_vol_20d,
                              CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1
                                        OR sig_vol_20x=1 THEN 1 ELSE 0 END supp,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3
                         AND date >= DATE '{as_of}' - INTERVAL 450 DAY)
            SELECT universe, ticker, CAST(date AS VARCHAR)[:10] d, open, high, low, close,
                   rsi_14, volume, avg_vol_20d, supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {"rows": [], "count": 0, "as_of": as_of}
    g = df.groupby("ticker", sort=False)
    df["e200"] = g["close"].transform(lambda s: s.ewm(span=200, adjust=False).mean())
    df["hi20"] = g["high"].transform(lambda s: s.shift(1).rolling(20).max())
    df["green"] = df["close"] > df["open"]
    days = sorted(df["d"].unique(), reverse=True)
    age = {d: i for i, d in enumerate(days)}
    df["age"] = df["d"].map(age)

    m15 = _m15_minrsi(as_of, days=max_age_days + 6)
    df = df.merge(m15, on=["ticker", "d"], how="left")
    r1d = df["rsi_14"]
    sig = (df["close"] > df["e200"]) & r1d.between(40, 60) \
        & (df["close"] >= 0.85 * df["hi20"]) & df["hi20"].notna() & df["green"] \
        & (df["rsi15"] <= 28) & (df["supp"] == 0) & (df["age"] <= max_age_days - 1) \
        & (df["close"] * df["volume"] >= dv_floor) & (df["avg_vol_20d"] > 0)
    cand = df[sig].copy()

    out, seen = [], set()
    for _, r in cand.sort_values(["ticker", "d"], ascending=[True, False]).iterrows():
        tk = str(r["ticker"])
        if tk in seen:
            continue
        seen.add(tk)
        off_hi = (float(r["close"]) / float(r["hi20"]) - 1) * 100
        atoms = [f"🧗 base {off_hi:+.0f}%", f"15mRSI {float(r['rsi15']):.0f}",
                 f"RSI1d {float(r['rsi_14']):.0f}"]
        # score: deeper 15m dip + tighter base + mid RSI = better
        score = 66 + (12 if r["rsi15"] <= 22 else 6 if r["rsi15"] <= 25 else 0) \
            + (8 if off_hi >= -8 else 4) + (6 if 45 <= r["rsi_14"] <= 55 else 0)
        out.append({
            "ticker": tk, "universe": str(r["universe"]), "signal_date": str(r["d"])[:10],
            "close": round(float(r["close"]), 2),
            "rsi": round(float(r["rsi_14"]), 0), "rsi15_min": round(float(r["rsi15"]), 0),
            "tz": f"15m↓{float(r['rsi15']):.0f}", "l_sig": "",
            "off_high_pct": round(off_hi, 1),
            "dv_m": round(float(r["close"] * r["volume"]) / 1e6, 1),
            "score": min(int(score), 100), "atoms": atoms, "age_days": int(r["age"]),
        })
    out.sort(key=lambda x: -x["score"])
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
            "edge_note": ("🧗 High-Base 15m-Dip — strong name (above EMA200, within 15% of 20d "
                          "high, RSI_1d 40-60) whose day printed a DEEP 15m dip (min 15m RSI ≤28) "
                          "and closed green. The board's first HIGH-BASE setup (fills the RGTI-2025 "
                          "re-accumulation gap where dip scanners stay silent). Backtest +1.86/"
                          "med+0.34/PF1.31/5-6yr vs random +1.37±0.08 (6σ). Modest tier — pair "
                          "with an entry trigger.")}
