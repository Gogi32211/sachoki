"""
validate_early_prep.py — EARLY quiet-accumulation detector (the MXL lesson).

EARLY state at day D (features ending AT D, DST-safe lastbar via arg_max):
    lbrel < 0.85   (3d last-bar volume share vs own 20d baseline — diffuse flow)
    comp3 > 1.0    (3d range vs baseline — expanded, "pot simmering")
    rvol3 < 1.2    (volume NOT yet hot — else it's already CHARGED)
Outcome: does a +15% daily spike occur within the NEXT 10 / 20 trading days?
Universe-wide on BOTH 1h and 15m day-aggregates; lift vs base rate, per-year.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB, db_path

DV = 2_000_000; PMIN = 3.0; SPIKE = 0.15


def day_features(tf: str):
    dbf = "studio_15m_base.duckdb" if tf == "15m" else "studio_1h.duckdb"
    con = duckdb.connect(db_path(dbf), read_only=True)
    try:
        f = con.execute("""
        WITH b AS (
          SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) AS d0, date, high, low, close, volume
          FROM bars),
        a AS (
          SELECT ticker, CAST(d0 AS VARCHAR) AS dstr, count(*) n_bars, sum(volume) tot_vol,
                 arg_max(volume, date) lastbar_vol,
                 max(high) dh, min(low) dl, arg_max(close, date) lc
          FROM b GROUP BY ticker, d0)
        SELECT * FROM a WHERE n_bars >= 5 ORDER BY ticker, dstr""").fetchdf()
    finally:
        con.close()
    f = f.rename(columns={"dstr": "day"})
    f["lastbar_share"] = f.lastbar_vol / f.tot_vol.replace(0, np.nan)
    f["rng"] = (f.dh - f.dl) / f.lc.replace(0, np.nan)
    g = f.groupby("ticker", sort=False)
    # windows END at day D (state as of D's close); baselines end at D-3
    f["vol3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean())
    f["vol20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["rvol3"] = f.vol3 / f.vol20.replace(0, np.nan)
    f["rng3"] = g["rng"].transform(lambda s: s.rolling(3).mean())
    f["rng20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["comp3"] = f.rng3 / f.rng20.replace(0, np.nan)
    f["lb3"] = g["lastbar_share"].transform(lambda s: s.rolling(3).mean())
    f["lb20"] = g["lastbar_share"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["lbrel"] = f.lb3 / f.lb20.replace(0, np.nan)
    return f[["ticker", "day", "rvol3", "comp3", "lbrel"]]


def main():
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, close, volume,
           lag(close) OVER (PARTITION BY ticker ORDER BY date) pc FROM (
        SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
        WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    d["ret"] = d.close / d.pc - 1
    d["dv"] = d.close * d.volume
    d["spike"] = (d.ret >= SPIKE).astype(float)
    g = d.groupby("ticker", sort=False)
    # spike within NEXT 10/20 days (exclude today): reversed rolling max, then shift -1
    for N in (10, 20):
        d[f"fs{N}"] = g["spike"].transform(lambda s: s[::-1].rolling(N, min_periods=1).max()[::-1])
        d[f"fs{N}"] = g[f"fs{N}"].shift(-1)
    liq = d[(d.pc >= PMIN) & (d.dv >= DV)][["ticker", "day", "fs10", "fs20"]]
    print(f"daily ready ({time.time()-t0:.0f}s)", flush=True)

    for tf in ["1h", "15m"]:
        f = day_features(tf)
        print(f"\n===== {tf} · features {len(f):,} ({time.time()-t0:.0f}s) =====", flush=True)
        m = liq.merge(f, on=["ticker", "day"]).dropna(subset=["rvol3", "comp3", "lbrel", "fs10"])
        m["yr"] = m.day.str[:4]
        early = (m.lbrel < 0.85) & (m.comp3 > 1.0) & (m.rvol3 < 1.2)
        charged = (m.rvol3 > 1.2) & (m.comp3 > 1.0) & (m.lbrel < 0.9)
        base10 = m.fs10.mean() * 100; base20 = m.fs20.mean() * 100
        print(f"base rate: spike≤10d {base10:.2f}% · ≤20d {base20:.2f}%  (n={len(m):,})")
        for lab, mask in [("EARLY (quiet prep)", early), ("CHARGED (ref)", charged),
                          ("lbrel<0.85 alone", m.lbrel < 0.85),
                          ("comp3>1 alone", m.comp3 > 1.0)]:
            s = m[mask]
            r10 = s.fs10.mean() * 100; r20 = s.fs20.mean() * 100
            print(f"  {lab:22s} n={len(s):8,d} ({len(s)/len(m)*100:4.1f}% days)  "
                  f"spike≤10d {r10:.2f}% (lift {r10/base10:.2f}×) · ≤20d {r20:.2f}% (lift {r20/base20:.2f}×)")
        # per-year lift of EARLY
        print("  EARLY per-year lift (≤10d):")
        row = []
        for y, sub in m.groupby("yr"):
            b = sub.fs10.mean(); e = sub[early & (m.yr == y)].fs10.mean() if (early & (m.yr == y)).sum() > 200 else np.nan
            row.append(f"{y[2:]}:{e/b:.2f}×" if b > 0 and not np.isnan(e) else f"{y[2:]}:·")
        print("    " + "  ".join(row))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
