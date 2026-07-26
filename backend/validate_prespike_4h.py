"""
validate_prespike_4h.py — add 4H everywhere:
A) 4H day-feature prep battery (note: only ~2 RTH 4h bars/day → bar-structure
   features are coarse; rvol3/comp3 are day-level and TF-invariant anyway).
B) 4-TF EMA-stack states (15m/30m/1h/4h) — incl the TRUE SMX geometry
   (lower TFs turned up while 4H still down) — spike-next/≤3d lift + per-year
   for any state with |lift|≥1.15.
"""
from __future__ import annotations
import gc, time
import numpy as np, pandas as pd, duckdb
from studio.paths import ANALYTICS_DB, db_path

DV = 2_000_000; PMIN = 3.0; SPIKE = 0.15


def daily_events():
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, close, volume,
           lag(close) OVER (PARTITION BY ticker ORDER BY date) pc FROM (
        SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
        WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    d["ret"] = d.close / d.pc - 1; d["dv"] = d.close * d.volume
    d["spike"] = (d.ret >= SPIKE).astype(float)
    g = d.groupby("ticker", sort=False)
    d["ev_next"] = g["spike"].shift(-1)
    d["ev3"] = g["spike"].transform(lambda s: s[::-1].rolling(3, min_periods=1).max()[::-1])
    d["ev3"] = g["ev3"].shift(-1)
    return d[(d.pc >= PMIN) & (d.dv >= DV)][["ticker", "day", "ev_next", "ev3"]]


def part_a_4h(dev):
    t0 = time.time()
    con = duckdb.connect(db_path("studio_4h.duckdb"), read_only=True)
    f = con.execute("""
    WITH b AS (SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) d0, date, open, high, low, close, volume FROM bars),
    a AS (SELECT ticker, CAST(d0 AS VARCHAR) dstr, count(*) n_bars, sum(volume) tot_vol,
                 sum(CASE WHEN close>open THEN volume ELSE 0 END) up_vol,
                 arg_max(volume,date) lastbar_vol, max(high) dh, min(low) dl, arg_max(close,date) lc
          FROM b GROUP BY ticker, d0)
    SELECT * FROM a WHERE n_bars >= 2 ORDER BY ticker, dstr""").fetchdf()
    con.close()
    f = f.rename(columns={"dstr": "day"})
    print(f"A) 4h day-features {len(f):,} ({time.time()-t0:.0f}s)", flush=True)
    f["upvol_share"] = f.up_vol / f.tot_vol.replace(0, np.nan)
    f["lastbar_share"] = f.lastbar_vol / f.tot_vol.replace(0, np.nan)
    f["rng"] = (f.dh - f.dl) / f.lc.replace(0, np.nan)
    g = f.groupby("ticker", sort=False)
    for c_ in ["upvol_share", "lastbar_share"]:
        f[c_ + "3"] = g[c_].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["v3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["v20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["rvol3"] = f.v3 / f.v20.replace(0, np.nan)
    f["r3"] = g["rng"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["r20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["comp3"] = f.r3 / f.r20.replace(0, np.nan)
    f["lb20"] = g["lastbar_share"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["lbrel"] = f.lastbar_share3 / f.lb20.replace(0, np.nan)
    m = dev.merge(f[["ticker", "day", "rvol3", "comp3", "lbrel", "upvol_share3"]],
                  on=["ticker", "day"]).dropna(subset=["rvol3", "comp3", "lbrel"])
    ev = m[m.ev_next == 1]; ct = m[m.ev_next == 0].sample(min((m.ev_next == 0).sum(), 400_000), random_state=7)
    from scipy.stats import mannwhitneyu
    print(f"   events {len(ev):,} / controls {len(ct):,}")
    for lab in ["rvol3", "comp3", "lbrel", "upvol_share3"]:
        e = ev[lab].dropna().to_numpy(); c_ = ct[lab].dropna().to_numpy()
        u, _ = mannwhitneyu(e, c_)
        print(f"   {lab:14s} ev-med {np.median(e):7.3f}  ct-med {np.median(c_):7.3f}  AUC {u/(len(e)*len(c_)):.3f}")
    del f; gc.collect()


def tf_stack(tf):
    t0 = time.time()
    if tf == "1h":
        db, src = "studio_1h.duckdb", "SELECT ticker, date, close FROM bars"
    elif tf == "4h":
        db, src = "studio_4h.duckdb", "SELECT ticker, date, close FROM bars"
    elif tf == "15m":
        db, src = "studio_15m_base.duckdb", "SELECT ticker, date, close FROM bars WHERE close*volume >= 500000"
    else:  # 30m
        db = "studio_15m_base.duckdb"
        src = """SELECT ticker, b AS date, c AS close FROM (
                   SELECT ticker, time_bucket(INTERVAL '30 minutes', date) AS b,
                          arg_max(close, date) AS c
                   FROM bars WHERE close*volume >= 250000 GROUP BY ticker, b)"""
    con = duckdb.connect(db_path(db), read_only=True)
    df = con.execute(f"SELECT * FROM ({src}) ORDER BY ticker, date").fetchdf()
    con.close()
    g = df.groupby("ticker", sort=False)["close"]
    for L in (9, 20, 50, 200):
        df[f"e{L}"] = g.transform(lambda s, L=L: s.ewm(span=L, adjust=False).mean())
    df["day"] = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    snap = df.groupby(["ticker", "day"], sort=False).tail(1)
    out = snap[["ticker", "day"]].copy()
    out[f"up_{tf}"] = ((snap.e9 > snap.e20) & (snap.e20 > snap.e50)).values
    out[f"ab_{tf}"] = (snap.close > snap.e200).values
    print(f"B) {tf} stack {len(out):,} ({time.time()-t0:.0f}s)", flush=True)
    del df; gc.collect()
    return out


def main():
    t0 = time.time()
    dev = daily_events()
    part_a_4h(dev)
    s15 = tf_stack("15m"); s30 = tf_stack("30m"); s1h = tf_stack("1h"); s4h = tf_stack("4h")
    m = dev.merge(s15, on=["ticker", "day"]).merge(s30, on=["ticker", "day"])\
           .merge(s1h, on=["ticker", "day"]).merge(s4h, on=["ticker", "day"])
    m["yr"] = m.day.str[:4]; m = m.dropna(subset=["ev_next", "ev3"])
    base1 = m.ev_next.mean() * 100; base3 = m.ev3.mean() * 100
    print(f"\nB) merged {len(m):,} · base: next {base1:.2f}% · ≤3d {base3:.2f}%")
    ups = m[["up_15m", "up_30m", "up_1h", "up_4h"]].sum(axis=1)
    states = {
        "ALL-4 UP": ups == 4,
        "ALL-4 DOWN": ups == 0,
        "TRUE SMX: 15m+30m+1h up & 4h DOWN": m.up_15m & m.up_30m & m.up_1h & ~m.up_4h,
        "SMX-lite: 15m+30m up & 4h down": m.up_15m & m.up_30m & ~m.up_4h,
        "inverse: 4h up & 15m+30m down": m.up_4h & ~m.up_15m & ~m.up_30m,
        "4h up & <e200 (4h recovery)": m.up_4h & ~m.ab_4h,
        "ALL-4 up & 4h<e200 (SMX-deep)": (ups == 4) & ~m.ab_4h,
    }
    print(f"   {'state':36s} {'days%':>6s} {'nx':>6s} {'≤3d':>6s}")
    picks = []
    for lab, q in states.items():
        s = m[q]
        if len(s) < 5000:
            print(f"   {lab:36s} n={len(s)} (small)"); continue
        l1 = s.ev_next.mean() * 100 / base1; l3 = s.ev3.mean() * 100 / base3
        print(f"   {lab:36s} {len(s)/len(m)*100:5.1f}% {l1:5.2f}× {l3:5.2f}×")
        if abs(l3 - 1) >= 0.15:
            picks.append((lab, q))
    for lab, q in picks:
        row = []
        for y, sub in m.groupby("yr"):
            b = sub.ev3.mean(); s = sub[q.reindex(sub.index, fill_value=False)]
            row.append(f"{y[2:]}:{s.ev3.mean()/b:.2f}×" if len(s) > 2000 and b > 0 else f"{y[2:]}:·")
        print(f"   per-year ≤3d — {lab}: " + "  ".join(row))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
