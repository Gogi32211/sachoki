"""
validate_highbase_h1dip.py — the RGTI-gap setup candidate (2026-07-08).

Pattern (mirror of the validated H1-bottom): a HIGH BASE on daily that never gets
daily-oversold, but whose dips are DEEP on 1H. Daily context + 1H trigger:
  ctx (daily):  close > EMA200 · RSI_1d 40-60 · close >= 85% of 20d-high (high base) · green
  trig (1H):    the day's MIN 1H RSI <= 35 (intraday washout inside the base)
Entry next daily open, path-sim trail25/60, per year. Controls: same context WITHOUT the
1H-dip trigger, and random-same-size from the context pool. 1h coverage = liquid 3203 tickers.
READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def main():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    df = a.execute("""
        WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
                          row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                   FROM bars WHERE close>=3 AND avg_vol_20d>0)
        SELECT ticker, CAST(date AS VARCHAR)[:10] date, open,high,low,close,rsi_14
        FROM r WHERE rn=1 AND dv>=3000000 ORDER BY ticker,date""").fetchdf()
    a.close()
    c1 = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    h1 = c1.execute("""
        SELECT ticker, CAST(CAST(date - INTERVAL 5 HOUR AS DATE) AS VARCHAR) AS date,
               min(rsi_14) AS min_rsi1h
        FROM bars GROUP BY ticker, CAST(date - INTERVAL 5 HOUR AS DATE)""").fetchdf()
    c1.close()
    print(f"daily {len(df):,} · 1h-days {len(h1):,} ({time.time()-t0:.0f}s)", flush=True)
    df = df.merge(h1, on=["ticker", "date"], how="left")
    cov = df.min_rsi1h.notna().mean() * 100
    g = df.groupby("ticker", sort=False)
    df["e200"] = g["close"].transform(lambda s: s.ewm(span=200, adjust=False).mean())
    df["hi20"] = g["high"].transform(lambda s: s.shift(1).rolling(20).max())
    df["green"] = df.close > df.open
    df["yr"] = df.date.str[:4]
    r1d = df["rsi_14"]
    ctx = (df.close > df.e200) & r1d.between(40, 60) & (df.close >= 0.85 * df.hi20) \
          & df.hi20.notna() & df.green & df.min_rsi1h.notna()
    trig35 = df.min_rsi1h <= 35
    trig30 = df.min_rsi1h <= 30
    print(f"1h coverage {cov:.0f}% · ctx pool {int(ctx.sum()):,} · +trig35 {int((ctx&trig35).sum()):,} · +trig30 {int((ctx&trig30).sum()):,}", flush=True)

    def stat(mask, slip=None):
        df["_m"] = mask
        grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}
        return _stats("x", _pathsim(grp, "_m", "trail", 0.10, 0.25, 0.25, 60, slip=slip))

    def rep(lab, s):
        py = s["per_year"]
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  {lab:26s} n={s['n']:>6} mean{s['mean']:+5.2f} med{s['median']:+5.2f} "
              f"win{s['win']:4.1f} pf{str(s['pf']):>4} y{s['pos_years']}/6 | {yr}", flush=True)

    print("\n=== High-Base 1H-Dip ===")
    rep("ctx pool (no trig)", stat(ctx))
    rep("ctx & NO 1H-dip", stat(ctx & ~trig35))
    rep("ctx & 1H-RSI<=35", stat(ctx & trig35))
    rep("ctx & 1H-RSI<=30", stat(ctx & trig30))
    print("\n=== random-same-size controls (from ctx pool) ===")
    pool = np.where(ctx.values)[0]
    n35 = int((ctx & trig35).sum())
    for seed in (1, 2, 3):
        r = np.random.default_rng(seed)
        pick = r.choice(pool, size=n35, replace=False)
        m = np.zeros(len(df), bool); m[pick] = True
        s = stat(pd.Series(m, index=df.index))
        print(f"  random#{seed}              n={s['n']:>6} mean{s['mean']:+5.2f} med{s['median']:+5.2f} pf{str(s['pf']):>4}")
    print("\n=== 2x-slip on trig35 ===")
    s = stat(ctx & trig35, slip=0.003)
    print(f"  2x-slip: mean{s['mean']:+.2f} med{s['median']:+.2f} pf{s['pf']} y{s['pos_years']}/6")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
