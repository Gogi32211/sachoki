"""
validate_3bar_configs.py — systematic 3-bar TZ configuration search.

For every bull-T entry bar, form the 3-bar TZ sequence (tz[-2], tz[-1], tz[0]),
path-sim it (trail25, entry next-open), aggregate by config. Rank by TIME-
ROBUSTNESS (posYrs, positive median, TEST>0) — NOT raw return — because a
brute-force 3-bar search is multiple-testing heavy; raw-return winners are
almost always small-n / 2025-artifacts. n>=200 floor.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB
DVMIN = 2_000_000; PMIN = 3.0; SLIP = 0.0015; NMIN = 200


def main():
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:4] yr, open,high,low,close,volume,
                       coalesce(t_sig,'') t, coalesce(z_sig,'') z FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker,date""").fetchdf()
    a.close()
    d["tz"] = np.where(d.t != "", d.t, np.where(d.z != "", d.z, "·"))
    g = d.groupby("ticker", sort=False)
    d["tz1"] = g["tz"].shift(1); d["tz2"] = g["tz"].shift(2)
    d["dv"] = d.close * d.volume
    o = d.open.to_numpy(float); h = d.high.to_numpy(float); l = d.low.to_numpy(float); c = d.close.to_numpy(float)
    tick = d.ticker.to_numpy(); n = len(d)
    print(f"loaded {n:,} ({time.time()-t0:.0f}s)")

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
            end = j; tsl = pk * 0.75
            if j > i + 1 and o[j] <= tsl:
                return o[j] / ep - 1 - SLIP
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                return ts / ep - 1 - SLIP
        return c[end] / ep - 1 - SLIP

    # entries = bull-T bars with liquidity + 2 prior TZ known
    mask = (d.t != "") & (d.dv >= DVMIN) & (d.close >= PMIN) & d.tz1.notna() & d.tz2.notna()
    idx = np.where(mask.to_numpy())[0]
    print(f"bull-T entries: {len(idx):,} — path-sim… ({time.time()-t0:.0f}s)")
    rec = []
    tz0 = d.tz.to_numpy(); tz1 = d.tz1.to_numpy(); tz2 = d.tz2.to_numpy(); yr = d.yr.to_numpy()
    for i in idx:
        r = ts25(i)
        if r is not None:
            rec.append((f"{tz2[i]}→{tz1[i]}→{tz0[i]}", yr[i], r * 100))
    R = pd.DataFrame(rec, columns=["cfg", "yr", "ret"])
    print(f"sims done: {len(R):,} ({time.time()-t0:.0f}s)")

    rows = []
    for cfg, s in R.groupby("cfg"):
        if len(s) < NMIN:
            continue
        yrm = s.groupby("yr")["ret"].median()
        pos = int((yrm > 0).sum())
        TR = yrm[yrm.index <= "2023"].mean(); TE = yrm[yrm.index > "2023"].mean()
        rows.append({"cfg": cfg, "n": len(s), "mean": s.ret.mean(), "med": s.ret.median(),
                     "win": (s.ret > 0).mean() * 100, "posYrs": pos, "nyr": len(yrm),
                     "TR": TR, "TE": TE})
    T = pd.DataFrame(rows)
    # robustness rank: positive median AND both TR/TE positive AND posYrs>=4
    robust = T[(T.med > 0) & (T.posYrs >= 4) & (T.TR > 0) & (T.TE > 0)].sort_values("med", ascending=False)
    print(f"\n=== ROBUST 3-bar configs (med>0, posYrs>=4, TR>0, TE>0, n>=200) — {len(robust)} of {len(T)} ===")
    print(f"  {'config':22s} {'n':>6s} {'mean':>6s} {'med':>6s} {'win':>4s} {'pos':>4s} {'TR':>6s} {'TE':>6s}")
    for _, r in robust.head(25).iterrows():
        print(f"  {r.cfg:22s} {int(r.n):6d} {r['mean']:+6.2f} {r.med:+6.2f} {r.win:4.0f} {int(r.posYrs)}/{int(r.nyr):d}  {r.TR:+6.2f} {r.TE:+6.2f}")
    print(f"\n=== for contrast: TOP by RAW MEAN (the multiple-testing trap) ===")
    for _, r in T.sort_values("mean", ascending=False).head(8).iterrows():
        print(f"  {r.cfg:22s} n={int(r.n):5d} mean {r['mean']:+.2f} med {r.med:+.2f} win {r.win:.0f}% posYrs {int(r.posYrs)}/{int(r.nyr)}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
