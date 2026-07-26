"""
validate_prespike_15m.py — is there UNUSUAL 15m "preparation" in the 3 days
before a +15% daily spike?

Method (control-matched — the delta-leads-price lesson):
  1. Per (ticker, ET-day) aggregate 15m structure features from studio_15m_base.
  2. Event day D0 = daily close/prev_close - 1 >= +15%, dv>=2M, prev_close>=3.
  3. Prep window = rolling mean of day-features over D-3..D-1 (event day EXCLUDED).
  4. CONTROL = every other liquid day (same filters, no spike within next 1d).
  5. Per feature: event-vs-control medians + AUC (Mann-Whitney). AUC≈0.5 = no
     discrimination; the study only "finds" preparation if AUC meaningfully >0.5
     AND stable per-year.
Caveat printed with results: +15% spikes are often earnings/news — prep signal
may partly be pre-earnings drift; no earnings calendar in DB to exclude.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB, db_path

DV = 2_000_000; PMIN = 3.0; SPIKE = 0.15
FEATS = ["rvol3", "upvol_share", "lasthr_share", "maxbar_ratio", "hl_share",
         "drift3", "comp3", "clr"]


def day_features_15m():
    con = duckdb.connect(db_path("studio_15m_base.duckdb"), read_only=True)
    try:
        df = con.execute("""
        WITH b AS (
          SELECT ticker,
                 CAST(date - INTERVAL 5 HOUR AS DATE) AS day,
                 date, open, high, low, close, volume,
                 EXTRACT(hour FROM date - INTERVAL 5 HOUR) AS eth,
                 lag(low) OVER (PARTITION BY ticker, CAST(date - INTERVAL 5 HOUR AS DATE)
                                ORDER BY date) AS plow
          FROM bars),
        a AS (
          SELECT ticker, CAST(day AS VARCHAR) AS day,
                 count(*) n_bars,
                 sum(volume) tot_vol,
                 sum(CASE WHEN close > open THEN volume ELSE 0 END) up_vol,
                 sum(CASE WHEN eth = 15 THEN volume ELSE 0 END) lasthr_vol,
                 max(volume) max_vol,
                 median(volume) med_vol,
                 max(high) dh, min(low) dl,
                 arg_max(close, date) lc, arg_min(open, date) fo,
                 sum(CASE WHEN plow IS NOT NULL AND low > plow THEN 1 ELSE 0 END) hl_cnt
          FROM b GROUP BY ticker, day)
        SELECT * FROM a WHERE n_bars >= 20 ORDER BY ticker, day
        """).fetchdf()
    finally:
        con.close()
    df["upvol_share"] = df.up_vol / df.tot_vol.replace(0, np.nan)
    df["lasthr_share"] = df.lasthr_vol / df.tot_vol.replace(0, np.nan)
    df["maxbar_ratio"] = df.max_vol / df.med_vol.replace(0, np.nan)
    df["hl_share"] = df.hl_cnt / (df.n_bars - 1)
    df["rng"] = (df.dh - df.dl) / df.lc.replace(0, np.nan)
    df["clr"] = (df.lc - df.dl) / (df.dh - df.dl).replace(0, np.nan)
    df["drift1"] = df.lc / df.fo - 1
    return df[["ticker", "day", "tot_vol", "upvol_share", "lasthr_share",
               "maxbar_ratio", "hl_share", "rng", "clr", "drift1"]]


def main():
    t0 = time.time()
    f = day_features_15m()
    print(f"15m day-features: {len(f):,} ticker-days ({time.time()-t0:.0f}s)", flush=True)
    g = f.groupby("ticker", sort=False)
    # 3-day prep window ENDING at D-1 (shift 1 so the event day itself is excluded)
    for c in ["upvol_share", "lasthr_share", "maxbar_ratio", "hl_share", "clr"]:
        f[c + "_3"] = g[c].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["vol3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["vol20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["rvol3"] = f.vol3 / f.vol20.replace(0, np.nan)          # prep-volume vs own baseline
    f["drift3"] = g["drift1"].transform(lambda s: (1 + s).rolling(3).apply(np.prod, raw=True) - 1)\
                    .groupby(f.ticker).shift(1)
    f["rng3"] = g["rng"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["rng20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["comp3"] = f.rng3 / f.rng20.replace(0, np.nan)          # <1 = compression before

    # daily events
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, close, volume,
                       lag(close) OVER (PARTITION BY ticker ORDER BY date) pc FROM (
        SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
        WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    d["ret"] = d.close / d.pc - 1
    d["dv"] = d.close * d.volume
    d = d[(d.pc >= PMIN) & (d.dv >= DV) & d.ret.notna()]
    d["event"] = d.ret >= SPIKE

    m = d.merge(f, on=["ticker", "day"], how="inner")
    m["yr"] = m.day.str[:4]
    cols = {"rvol3": "rvol3", "upvol_share": "upvol_share_3", "lasthr_share": "lasthr_share_3",
            "maxbar_ratio": "maxbar_ratio_3", "hl_share": "hl_share_3",
            "drift3": "drift3", "comp3": "comp3", "clr": "clr_3"}
    m = m.dropna(subset=list(cols.values()))
    ev = m[m.event]; ct = m[~m.event]
    print(f"events: {len(ev):,} · controls: {len(ct):,} ({time.time()-t0:.0f}s)", flush=True)

    from scipy.stats import mannwhitneyu
    cts = ct.sample(min(len(ct), 400_000), random_state=7)
    print(f"\n{'feature (3d prep window)':26s} {'event med':>10s} {'ctrl med':>10s} {'AUC':>6s}")
    aucs = {}
    for lab, col in cols.items():
        e = ev[col].to_numpy(); c_ = cts[col].to_numpy()
        u, p = mannwhitneyu(e, c_, alternative="two-sided")
        auc = u / (len(e) * len(c_))
        aucs[lab] = auc
        print(f"{lab:26s} {np.median(e):>10.3f} {np.median(c_):>10.3f} {auc:>6.3f}")
    # per-year stability of the top-2 discriminators
    top = sorted(aucs, key=lambda k: -abs(aucs[k] - 0.5))[:3]
    print("\nper-year AUC (top discriminators):")
    for lab in top:
        col = cols[lab]; row = []
        for y, sub in m.groupby("yr"):
            e = sub[sub.event][col].dropna(); c_ = sub[~sub.event][col].dropna()
            if len(e) < 100:
                row.append(f"{y[2:]}:·"); continue
            c_ = c_.sample(min(len(c_), 60_000), random_state=7)
            u, _ = mannwhitneyu(e.to_numpy(), c_.to_numpy())
            row.append(f"{y[2:]}:{u/(len(e)*len(c_)):.3f}")
        print(f"  {lab:22s} " + "  ".join(row))
    print(f"\nCAVEAT: +15% spikes include earnings/news; prep-signal may partly be pre-news drift.")
    print(f"done {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
