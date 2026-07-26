"""
validate_prespike_30m_ema.py — pre-spike prep on 30m bars + multi-TF EMA states.

A) 30m bars built on the fly from studio_15m_base via time_bucket (no new DB):
   same control-matched day-feature battery (rvol3/comp3/lastbar/upvol/maxbar),
   AUC event(+15% next day) vs control.
B) EMA-stack EOD states on 15m / 30m / 1h (SMX/RGTI-style geometry, intraday-only):
   per TF: stackup = e9>e20>e50 · above200 = close>e200.
   Divergence features:
     smx_like   = 15m stackup & 1h NOT stackup   (lower TF turns first)
     recovery_n = # TFs with stackup & !above200 (turn under the long EMA)
     allup_n    = # TFs with stackup (0-3)
   Lift of each state for spike next-day / within-3d, per-year for the top one.
dv floor 500K per 15m bar keeps the EMA pull tractable (as in edge_echo).
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
    d["ret"] = d.close / d.pc - 1
    d["dv"] = d.close * d.volume
    d["spike"] = (d.ret >= SPIKE).astype(float)
    g = d.groupby("ticker", sort=False)
    d["ev_next"] = g["spike"].shift(-1)                                   # spike tomorrow
    d["ev3"] = g["spike"].transform(lambda s: s[::-1].rolling(3, min_periods=1).max()[::-1])
    d["ev3"] = g["ev3"].shift(-1)                                         # spike within 3d
    return d[(d.pc >= PMIN) & (d.dv >= DV)][["ticker", "day", "ev_next", "ev3"]]


def part_a_30m(dev):
    t0 = time.time()
    con = duckdb.connect(db_path("studio_15m_base.duckdb"), read_only=True)
    f = con.execute("""
    WITH m30 AS (
      SELECT ticker, time_bucket(INTERVAL '30 minutes', date) AS b,
             CAST(time_bucket(INTERVAL '30 minutes', date) - INTERVAL 5 HOUR AS DATE) AS d0,
             arg_min(open, date) o, max(high) h, min(low) l, arg_max(close, date) c,
             sum(volume) v
      FROM bars GROUP BY ticker, b),
    a AS (
      SELECT ticker, CAST(d0 AS VARCHAR) AS dstr, count(*) n_bars, sum(v) tot_vol,
             sum(CASE WHEN c > o THEN v ELSE 0 END) up_vol,
             arg_max(v, b) lastbar_vol, max(v) max_vol, median(v) med_vol,
             max(h) dh, min(l) dl, arg_max(c, b) lc
      FROM m30 GROUP BY ticker, d0)
    SELECT * FROM a WHERE n_bars >= 8 ORDER BY ticker, dstr""").fetchdf()
    con.close()
    f = f.rename(columns={"dstr": "day"})
    print(f"A) 30m day-features {len(f):,} ({time.time()-t0:.0f}s)", flush=True)
    f["upvol_share"] = f.up_vol / f.tot_vol.replace(0, np.nan)
    f["lastbar_share"] = f.lastbar_vol / f.tot_vol.replace(0, np.nan)
    f["maxbar_ratio"] = f.max_vol / f.med_vol.replace(0, np.nan)
    f["rng"] = (f.dh - f.dl) / f.lc.replace(0, np.nan)
    g = f.groupby("ticker", sort=False)
    for c_ in ["upvol_share", "lastbar_share", "maxbar_ratio"]:
        f[c_ + "3"] = g[c_].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["v3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["v20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["rvol3"] = f.v3 / f.v20.replace(0, np.nan)
    f["r3"] = g["rng"].transform(lambda s: s.rolling(3).mean()).groupby(f.ticker).shift(1)
    f["r20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["comp3"] = f.r3 / f.r20.replace(0, np.nan)
    f["lb20"] = g["lastbar_share"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(4)
    f["lbrel"] = f.lastbar_share3 / f.lb20.replace(0, np.nan)
    m = dev.merge(f[["ticker", "day", "rvol3", "comp3", "lbrel", "upvol_share3", "maxbar_ratio3"]],
                  on=["ticker", "day"]).dropna(subset=["rvol3", "comp3", "lbrel"])
    ev = m[m.ev_next == 1]; ct = m[m.ev_next == 0].sample(min((m.ev_next == 0).sum(), 400_000), random_state=7)
    from scipy.stats import mannwhitneyu
    print(f"   events {len(ev):,} / controls {len(ct):,}")
    for lab in ["rvol3", "comp3", "lbrel", "upvol_share3", "maxbar_ratio3"]:
        e = ev[lab].dropna().to_numpy(); c_ = ct[lab].dropna().to_numpy()
        u, _ = mannwhitneyu(e, c_)
        print(f"   {lab:16s} ev-med {np.median(e):7.3f}  ct-med {np.median(c_):7.3f}  AUC {u/(len(e)*len(c_)):.3f}")
    del f; gc.collect()


def tf_stack(tf):
    """EOD EMA-stack state per (ticker, day) for 15m/30m/1h."""
    t0 = time.time()
    if tf == "1h":
        src = f"SELECT ticker, date, close FROM bars"
        db = "studio_1h.duckdb"
    else:
        db = "studio_15m_base.duckdb"
        if tf == "15m":
            src = "SELECT ticker, date, close FROM bars WHERE close*volume >= 500000"
        else:  # 30m buckets (inner alias b avoids the date-name collision)
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
    print(f"B) {tf} stack ready {len(out):,} ({time.time()-t0:.0f}s)", flush=True)
    del df; gc.collect()
    return out


def main():
    t0 = time.time()
    dev = daily_events()
    print(f"daily events ready ({time.time()-t0:.0f}s)", flush=True)
    part_a_30m(dev)

    s15 = tf_stack("15m"); s30 = tf_stack("30m"); s1h = tf_stack("1h")
    m = dev.merge(s15, on=["ticker", "day"]).merge(s30, on=["ticker", "day"]).merge(s1h, on=["ticker", "day"])
    m["yr"] = m.day.str[:4]
    m = m.dropna(subset=["ev_next", "ev3"])
    base1 = m.ev_next.mean() * 100; base3 = m.ev3.mean() * 100
    print(f"\nB) merged {len(m):,} · base: spike-next {base1:.2f}% · ≤3d {base3:.2f}%")
    ups = m[["up_15m", "up_30m", "up_1h"]].sum(axis=1)
    recov = ((m.up_15m & ~m.ab_15m).astype(int) + (m.up_30m & ~m.ab_30m).astype(int)
             + (m.up_1h & ~m.ab_1h).astype(int))
    states = {
        "ALL-UP (3/3 stackup)": ups == 3,
        "ALL-DOWN (0/3)": ups == 0,
        "smx-like: 15m up & 1h down": m.up_15m & ~m.up_1h,
        "inverse: 1h up & 15m down": m.up_1h & ~m.up_15m,
        "recovery ≥2 TF (up & <e200)": recov >= 2,
        "ALL-UP & ALL<e200 (deep recovery)": (ups == 3) & ~m.ab_15m & ~m.ab_30m & ~m.ab_1h,
        "ALL-UP & ALL>e200 (mature)": (ups == 3) & m.ab_15m & m.ab_30m & m.ab_1h,
    }
    print(f"   {'state':34s} {'days%':>6s} {'nx lift':>8s} {'≤3d lift':>9s}")
    best = None; bestlift = 0
    for lab, q in states.items():
        s = m[q]
        if len(s) < 5000:
            print(f"   {lab:34s} n={len(s)} (small)"); continue
        l1 = s.ev_next.mean() * 100 / base1; l3 = s.ev3.mean() * 100 / base3
        print(f"   {lab:34s} {len(s)/len(m)*100:5.1f}% {l1:7.2f}× {l3:8.2f}×")
        if abs(l3 - 1) > abs(bestlift - 1):
            best, bestlift = (lab, q), l3
    if best:
        lab, q = best
        print(f"\n   per-year ≤3d lift — '{lab}':")
        row = []
        for y, sub in m.groupby("yr"):
            b = sub.ev3.mean(); s = sub[q & (m.yr == y)]
            row.append(f"{y[2:]}:{(s.ev3.mean()/b):.2f}×" if len(s) > 2000 and b > 0 else f"{y[2:]}:·")
        print("   " + "  ".join(row))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
