"""
validate_prespike_1h.py — same pre-spike prep study on the 1H timeframe,
with FULL per-year AUC for every feature (2022 scrutiny). lastbar_share is
DST-proof here (arg_max(volume, date) = the day's final bar volume).
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB, db_path

DV = 2_000_000; PMIN = 3.0; SPIKE = 0.15


def day_features():
    con = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    try:
        df = con.execute("""
        WITH b AS (
          SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) AS day,
                 date, open, high, low, close, volume,
                 lag(low) OVER (PARTITION BY ticker, CAST(date - INTERVAL 5 HOUR AS DATE)
                                ORDER BY date) AS plow
          FROM bars),
        a AS (
          SELECT ticker, CAST(day AS VARCHAR) AS day,
                 count(*) n_bars, sum(volume) tot_vol,
                 sum(CASE WHEN close > open THEN volume ELSE 0 END) up_vol,
                 arg_max(volume, date) lastbar_vol,
                 max(volume) max_vol, median(volume) med_vol,
                 max(high) dh, min(low) dl,
                 arg_max(close, date) lc, arg_min(open, date) fo,
                 sum(CASE WHEN plow IS NOT NULL AND low > plow THEN 1 ELSE 0 END) hl_cnt
          FROM b GROUP BY ticker, day)
        SELECT * FROM a WHERE n_bars >= 5 ORDER BY ticker, day
        """).fetchdf()
    finally:
        con.close()
    df["upvol_share"] = df.up_vol / df.tot_vol.replace(0, np.nan)
    df["lastbar_share"] = df.lastbar_vol / df.tot_vol.replace(0, np.nan)
    df["maxbar_ratio"] = df.max_vol / df.med_vol.replace(0, np.nan)
    df["hl_share"] = df.hl_cnt / (df.n_bars - 1)
    df["rng"] = (df.dh - df.dl) / df.lc.replace(0, np.nan)
    df["clr"] = (df.lc - df.dl) / (df.dh - df.dl).replace(0, np.nan)
    df["drift1"] = df.lc / df.fo - 1
    return df[["ticker", "day", "tot_vol", "upvol_share", "lastbar_share",
               "maxbar_ratio", "hl_share", "rng", "clr", "drift1"]]


def main():
    t0 = time.time()
    f = day_features()
    print(f"1h day-features: {len(f):,} ({time.time()-t0:.0f}s)", flush=True)
    g = f.groupby("ticker", sort=False)
    for c in ["upvol_share", "lastbar_share", "maxbar_ratio", "hl_share", "clr"]:
        f[c + "_3"] = g[c].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["vol3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["vol20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["rvol3"] = f.vol3 / f.vol20.replace(0, np.nan)
    f["drift3"] = g["drift1"].transform(lambda s: (1 + s).rolling(3).apply(np.prod, raw=True) - 1)\
                    .groupby(f.ticker).shift(1)
    f["rng3"] = g["rng"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["rng20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["comp3"] = f.rng3 / f.rng20.replace(0, np.nan)

    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, close, volume,
                       lag(close) OVER (PARTITION BY ticker ORDER BY date) pc FROM (
        SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
        WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    d["ret"] = d.close / d.pc - 1; d["dv"] = d.close * d.volume
    d = d[(d.pc >= PMIN) & (d.dv >= DV) & d.ret.notna()]
    d["event"] = d.ret >= SPIKE
    m = d.merge(f, on=["ticker", "day"], how="inner"); m["yr"] = m.day.str[:4]
    cols = {"rvol3": "rvol3", "comp3": "comp3", "maxbar_ratio": "maxbar_ratio_3",
            "upvol_share": "upvol_share_3", "lastbar_share": "lastbar_share_3",
            "hl_share": "hl_share_3", "drift3": "drift3", "clr": "clr_3"}
    m = m.dropna(subset=list(cols.values()))
    ev = m[m.event]; ct = m[~m.event]
    print(f"events {len(ev):,} · controls {len(ct):,} ({time.time()-t0:.0f}s)", flush=True)

    from scipy.stats import mannwhitneyu
    cts = ct.sample(min(len(ct), 400_000), random_state=7)
    print(f"\n{'feature':22s} {'ev med':>8s} {'ct med':>8s} {'AUC':>6s}   per-year AUC")
    for lab, col in cols.items():
        e = ev[col].to_numpy(); c_ = cts[col].to_numpy()
        u, _ = mannwhitneyu(e, c_)
        auc = u / (len(e) * len(c_))
        yr_cells = []
        for y, sub in m.groupby("yr"):
            se = sub[sub.event][col].dropna(); sc = sub[~sub.event][col].dropna()
            if len(se) < 100:
                yr_cells.append(f"{y[2:]}:·"); continue
            sc = sc.sample(min(len(sc), 60_000), random_state=7)
            uu, _ = mannwhitneyu(se.to_numpy(), sc.to_numpy())
            yr_cells.append(f"{y[2:]}:{uu/(len(se)*len(sc)):.2f}")
        print(f"{lab:22s} {np.median(e):>8.3f} {np.median(c_):>8.3f} {auc:>6.3f}   " + " ".join(yr_cells))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
