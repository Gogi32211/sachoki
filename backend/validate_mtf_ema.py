"""
validate_mtf_ema.py — historical validation of the 6 multi-TF EMA-stack variants
(mtf_ema_scan.py, ported from the user's Pine scripts). For every trading day: EOD
snapshot of 15m/1H/4H EMA geometry + Daily RSI/vol base → variant masks → path-sim
on DAILY bars (trail25/60, gap-realistic, stop-first) per variant, per-year + '22 + TR/TE.
Processes tickers in chunks (the 15m table is 88M rows). READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
CHUNK = 400


def _daily_universe():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=3 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT ticker, CAST(date AS VARCHAR) date, open, high, low, close, volume, rsi_14,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        a.close()


def _tf_daily_snapshot(tickers, tf):
    """EOD (last bar per ET day) EMA features for one tf, restricted to `tickers`."""
    import duckdb
    from studio.paths import db_path
    name = "studio_15m_base.duckdb" if tf == "15m" else f"studio_{tf}.duckdb"
    c = duckdb.connect(db_path(name), read_only=True)
    tk_list = ",".join("'" + t.replace("'", "") + "'" for t in tickers)
    try:
        df = c.execute(f"""
            WITH d AS (SELECT ticker, date, open, close,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY ticker) u
                       FROM bars WHERE ticker IN ({tk_list}))
            SELECT ticker, date, open, close FROM d WHERE u=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        c.close()
    if len(df) == 0:
        return pd.DataFrame()
    g = df.groupby("ticker", sort=False)
    for L in (9, 20, 50, 200):
        df[f"e{L}"] = g["close"].transform(lambda s, L=L: s.ewm(span=L, adjust=False).mean())
    # ET day key (bars stored UTC; ET = UTC-4/5 — sessions never cross midnight ET, and all
    # regular-session UTC stamps 13:30-21:00 map to the same ET calendar day)
    day = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    df["day"] = day.values
    snap = df.groupby(["ticker", "day"], sort=False).tail(1)
    cols = ["ticker", "day", "open", "close"] + [f"e{L}" for L in (9, 20, 50, 200)]
    sfx = {"15m": "_15", "1h": "_1h", "4h": "_4h"}[tf]
    snap = snap[cols].rename(columns={c0: c0 + sfx for c0 in cols if c0 not in ("ticker", "day")})
    return snap


def _masks(m: pd.DataFrame) -> dict:
    """Vectorized variant conditions on the aligned daily frame (mirrors _variant_masks)."""
    p = m["close"]          # price_now = daily close (EOD)
    r = {}
    r["SMX"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h) & (m.e20_4h > m.e9_4h)
                & (m.close_4h > m.e9_4h) & (m.close_4h > m.open_4h)
                & (m.close_1h > m.e9_1h) & (m.e9_1h > m.e20_1h) & (m.close_1h > m.open_1h)
                & (m.e20_15 < m.e9_15) & (m.e200_15 > m.e9_15) & (m.e50_15 < m.e20_15)
                & (m.close_15 > m.e50_15) & (m.rsi_14 > 33))
    r["LL"] = ((m.e50_4h < m.e20_4h) & (m.e20_4h > m.e9_4h) & (p > m.e50_4h) & (p > m.e20_4h)
               & (p > m.e9_1h) & (m.e9_1h < m.e50_1h) & (m.e50_1h > m.e20_1h) & (m.e200_1h < m.e50_1h)
               & (m.e200_15 > m.e50_15))
    r["UP"] = ((m.e50_4h < m.e20_4h) & (m.e20_4h < m.e9_4h) & (p > m.e20_4h) & (p > m.e200_4h)
               & (m.e9_4h > m.e200_4h)
               & (p > m.e9_1h) & (m.e9_1h > m.e50_1h) & (m.e50_1h < m.e20_1h) & (m.e200_1h < m.e50_1h)
               & (m.e20_15 > m.e9_15) & (p > m.e200_15))
    r["UPUP"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h < m.e9_4h) & (m.e20_4h < m.e50_4h) & (p > m.e50_4h)
                 & (m.e200_15 < m.e50_15) & (m.e50_15 > m.e20_15) & (m.e20_15 > m.e9_15) & (p < m.e9_15))
    r["UPUPUP"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h < m.e9_4h) & (m.e20_4h > m.e50_4h)
                   & (p > m.e50_4h) & (p > m.e9_4h)
                   & (m.e9_1h > m.e200_1h) & (m.e200_1h > m.e50_1h)
                   & (m.e200_15 > m.e20_15) & (m.e9_15 < m.e20_15))
    r["ORANGE"] = ((m.e200_1h > m.e9_1h) & (m.e9_1h > m.e20_1h) & (m.e20_1h > m.e50_1h)
                   & (m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h) & (m.e9_4h > m.e20_4h)
                   & (m.e9_15 > m.e20_15) & (m.e20_15 > m.e50_15) & (m.e50_15 > m.e200_15))
    return r


def run():
    print("pulling daily universe…", flush=True)
    daily = _daily_universe()
    daily["day"] = daily["date"].str[:10]
    tickers = daily["ticker"].unique().tolist()
    print(f"daily rows {len(daily):,} · tickers {len(tickers)}", flush=True)

    merged_parts = []
    t0 = time.time()
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        s15 = _tf_daily_snapshot(chunk, "15m")
        s1h = _tf_daily_snapshot(chunk, "1h")
        s4h = _tf_daily_snapshot(chunk, "4h")
        if len(s15) == 0 or len(s1h) == 0 or len(s4h) == 0:
            continue
        d = daily[daily["ticker"].isin(chunk)]
        m = (d.merge(s15, on=["ticker", "day"], how="inner")
               .merge(s1h, on=["ticker", "day"], how="inner")
               .merge(s4h, on=["ticker", "day"], how="inner"))
        merged_parts.append(m)
        el = (time.time() - t0) / 60
        print(f"  chunk {i//CHUNK+1}/{(len(tickers)+CHUNK-1)//CHUNK} rows={sum(len(x) for x in merged_parts):,} "
              f"({el:.1f}min)", flush=True)
    m = pd.concat(merged_parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"aligned rows {len(m):,}", flush=True)

    masks = _masks(m)
    clean = m["supp"] == 0
    for k, v in masks.items():
        m[k] = (v & clean).values
    grp = {tk: g.reset_index(drop=True) for tk, g in m.groupby("ticker", sort=False)}

    print("\ntrail25/60 · gap-realistic · entry next daily open · dv≥3M\n")
    for k in ["SMX", "LL", "UP", "UPUP", "UPUPUP", "ORANGE"]:
        s = _stats(k, _pathsim(grp, k, **KW))
        if not s or s.get("n", 0) == 0:
            print(f"  {k:8s} n=0"); continue
        py = s["per_year"]
        tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  {k:8s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
              f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")
    print("\ndone.")


if __name__ == "__main__":
    run()
