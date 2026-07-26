"""
validate_charged_edge.py — does the ⚡CHARGED energy state boost directional entries?

A) CHARGED × Edge fires: tag every edge_replay trade (62mo, trail25) with the
   entry-day CHARGED state (1h day-features: rvol3>1.2 & comp3>1 & lbrel<0.9,
   windows ending at the signal day). Compare charged vs not, per setup, per-year.
B) CHARGED + RANGE-EXIT trigger (the user's idea): CHARGED day D, then within
   5 days the first close > 1.02 × max(close D-2..D) = breakout from the prep
   zone → entry next open, trail25. CONTROL = same breakout without CHARGED.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB, db_path
import edge_replay as ER

SLIP = 0.0015


def charged_map():
    con = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    f = con.execute("""
    WITH b AS (
      SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) AS d0, date, high, low, close, volume
      FROM bars),
    a AS (
      SELECT ticker, CAST(d0 AS VARCHAR) AS dstr, count(*) n_bars, sum(volume) tot_vol,
             arg_max(volume, date) lastbar_vol, max(high) dh, min(low) dl, arg_max(close, date) lc
      FROM b GROUP BY ticker, d0)
    SELECT * FROM a WHERE n_bars >= 5 ORDER BY ticker, dstr""").fetchdf()
    con.close()
    f = f.rename(columns={"dstr": "day"})
    f["lastbar_share"] = f.lastbar_vol / f.tot_vol.replace(0, np.nan)
    f["rng"] = (f.dh - f.dl) / f.lc.replace(0, np.nan)
    g = f.groupby("ticker", sort=False)
    f["vol3"] = g["tot_vol"].transform(lambda s: s.rolling(3).mean())
    f["vol20"] = g["tot_vol"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["rvol3"] = f.vol3 / f.vol20.replace(0, np.nan)
    f["rng3"] = g["rng"].transform(lambda s: s.rolling(3).mean())
    f["rng20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["comp3"] = f.rng3 / f.rng20.replace(0, np.nan)
    f["lb3"] = g["lastbar_share"].transform(lambda s: s.rolling(3).mean())
    f["lb20"] = g["lastbar_share"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(3)
    f["lbrel"] = f.lb3 / f.lb20.replace(0, np.nan)
    f["charged"] = (f.rvol3 > 1.2) & (f.comp3 > 1.0) & (f.lbrel < 0.9)
    return dict(zip(f.ticker + "|" + f.day, f.charged))


def main():
    t0 = time.time()
    cmap = charged_map()
    print(f"charged map ready ({time.time()-t0:.0f}s)", flush=True)

    # ── A) Edge fires × CHARGED ──────────────────────────────────────────────
    grp, as_of = ER._frame(62, 3_000_000)
    FOCUS = ["T1-CapBounce", "Z11-T11", "G3-gap", "L43-TRIPLE", "Atomic-R",
             "Engulf-Abs", "Atomic", "Washout", "D+L1", "H1-bottom"]
    print(f"\n=== A) Edge fires split by ⚡CHARGED at entry (62mo, trail25) · as_of {as_of} ===")
    print(f"{'setup':14s} {'variant':10s} {'n':>6s} {'mean':>7s} {'med':>7s} {'win':>4s}  per-yr med")
    for name in FOCUS:
        col = dict(ER.SETUPS)[name]
        tr = ER._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
        if len(tr) < 100:
            continue
        # CHARGED as of the SIGNAL day (entry is next open)
        sig_day = tr["date_in"]  # date_in is the entry (next open) date; approximate signal day
        tr["ch"] = (tr["ticker"] + "|" + tr["date_in"].str[:10]).map(cmap).fillna(False)
        for lab, sub in [("⚡charged", tr[tr.ch]), ("normal", tr[~tr.ch])]:
            if len(sub) < 60:
                print(f"{name:14s} {lab:10s} n={len(sub)} (small)"); continue
            r = sub["ret"] * 100
            yrm = sub.assign(rp=r).groupby("yr")["rp"].median()
            ys = " ".join(f"{y[2:]}:{v:+.1f}" for y, v in yrm.items())
            print(f"{name:14s} {lab:10s} {len(sub):6d} {r.mean():+7.2f} {r.median():+7.2f} "
                  f"{(r>0).mean()*100:4.0f}  {ys}")
        print()

    # ── B) CHARGED + range-exit breakout trigger ─────────────────────────────
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:4] yr, CAST(date AS VARCHAR)[:10] dstr,
        open, high, low, close, volume,
        lag(close) OVER (PARTITION BY ticker ORDER BY date) pc FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    g = d.groupby("ticker", sort=False)
    d["dv"] = d.close * d.volume
    d["zone_hi"] = g["close"].transform(lambda s: s.rolling(3).max())
    d["zone_hi1"] = g["zone_hi"].shift(1)
    d["ch"] = (d.ticker + "|" + d.dstr).map(cmap).fillna(False)
    # charged within the LAST 5 days (state may precede the breakout by a few days)
    d["ch5"] = g["ch"].transform(lambda s: s.rolling(5, min_periods=1).max()).astype(bool)
    brk = (d.close > d.zone_hi1 * 1.02) & (d.pc >= 3) & (d.dv >= 2_000_000)
    o = d.open.to_numpy(float); h = d.high.to_numpy(float); l = d.low.to_numpy(float); c = d.close.to_numpy(float)
    tick = d.ticker.to_numpy(); n = len(d)

    def ts25(i):
        if i + 1 >= n or tick[i + 1] != tick[i]:
            return None
        ep = o[i + 1] * (1 + SLIP)
        if ep <= 0:
            return None
        pk = ep; end = i + 1
        for j in range(i + 1, min(i + 61, n)):
            if tick[j] != tick[i]:
                break
            end = j; t_ = pk * 0.75
            if j > i + 1 and o[j] <= t_:
                return o[j] / ep - 1 - SLIP
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                return ts / ep - 1 - SLIP
        return c[end] / ep - 1 - SLIP

    print("=== B) range-exit breakout (close >2% over 3d-zone) — user's trigger idea ===")
    print(f"{'variant':30s} {'n':>7s} {'mean':>7s} {'med':>7s} {'win':>4s} {'pos':>4s}  per-yr med")
    for lab, mask in [("breakout, NOT charged (ctrl)", brk & ~d.ch5),
                      ("breakout + ⚡charged≤5d", brk & d.ch5)]:
        idx = np.where(mask.to_numpy())[0]
        rec = [(d.yr.iloc[i], r * 100) for i in idx if (r := ts25(i)) is not None]
        R = pd.DataFrame(rec, columns=["yr", "ret"])
        yrm = R.groupby("yr")["ret"].median(); pos = int((yrm > 0).sum())
        ys = " ".join(f"{y[2:]}:{v:+.1f}" for y, v in yrm.items())
        print(f"{lab:30s} {len(R):7d} {R.ret.mean():+7.2f} {R.ret.median():+7.2f} "
              f"{(R.ret>0).mean()*100:4.0f} {pos}/{len(yrm)}  {ys}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
