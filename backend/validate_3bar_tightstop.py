"""
validate_3bar_tightstop.py — 3-bar configs under a TIGHT 2% hard stop.

Different exit ⇒ different survivors: a 2% stop rewards configs with immediate
follow-through / low early drawdown, and penalizes the slow-burn ones (that dip
before running). Exit model: entry next-open; hard stop at entry×0.98 (gap-aware,
never moves); winners trail at 25% from peak; 60-bar cap. Reports high-win-rate
AND robust survivors. Also dumps per-year detail for the raw-mean leaders.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB
DVMIN = 2_000_000; PMIN = 3.0; SLIP = 0.0015; NMIN = 200; STOP = 0.02


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

    def sim(i):
        if i + 1 >= n or tick[i + 1] != tick[i]:
            return None
        ep = o[i + 1] * (1 + SLIP)
        if ep <= 0:
            return None
        hard = ep * (1 - STOP); pk = ep; end = i + 1
        for j in range(i + 1, min(i + 61, n)):
            if tick[j] != tick[i]:
                break
            end = j
            trail = pk * 0.75
            lvl = max(hard, trail)                 # tight 2% stop dominates until winner runs
            if j > i + 1 and o[j] <= lvl:
                return o[j] / ep - 1 - SLIP         # gap through
            if l[j] <= lvl:
                return lvl / ep - 1 - SLIP
            pk = max(pk, h[j])
        return c[end] / ep - 1 - SLIP

    mask = (d.t != "") & (d.dv >= DVMIN) & (d.close >= PMIN) & d.tz1.notna() & d.tz2.notna()
    idx = np.where(mask.to_numpy())[0]
    tz0 = d.tz.to_numpy(); tz1 = d.tz1.to_numpy(); tz2 = d.tz2.to_numpy(); yr = d.yr.to_numpy()
    print(f"entries {len(idx):,} — sim (2% stop)… ({time.time()-t0:.0f}s)")
    rec = [(f"{tz2[i]}→{tz1[i]}→{tz0[i]}", yr[i], r * 100) for i in idx if (r := sim(i)) is not None]
    R = pd.DataFrame(rec, columns=["cfg", "yr", "ret"])
    print(f"sims {len(R):,} ({time.time()-t0:.0f}s)")

    rows = []
    for cfg, s in R.groupby("cfg"):
        if len(s) < NMIN:
            continue
        yrm = s.groupby("yr")["ret"].mean()
        pos = int((yrm > 0).sum())
        TR = yrm[yrm.index <= "2023"].mean(); TE = yrm[yrm.index > "2023"].mean()
        rows.append({"cfg": cfg, "n": len(s), "mean": s.ret.mean(), "med": s.ret.median(),
                     "win": (s.ret > 0).mean() * 100, "posYrs": pos, "nyr": len(yrm), "TR": TR, "TE": TE})
    T = pd.DataFrame(rows)

    print(f"\n=== TIGHT 2% STOP · highest WIN% (n>=200) — user wants high win rate ===")
    print(f"  {'config':22s} {'n':>5s} {'mean':>6s} {'win':>4s} {'pos':>4s} {'TR':>6s} {'TE':>6s}")
    for _, r in T.sort_values("win", ascending=False).head(18).iterrows():
        print(f"  {r.cfg:22s} {int(r.n):5d} {r['mean']:+6.2f} {r.win:4.0f} {int(r.posYrs)}/{int(r.nyr)}  {r.TR:+6.2f} {r.TE:+6.2f}")

    print(f"\n=== TIGHT 2% STOP · ROBUST (win>=55, posYrs>=5, mean>0) — high-win AND time-stable ===")
    rob = T[(T.win >= 55) & (T.posYrs >= 5) & (T["mean"] > 0)].sort_values("win", ascending=False)
    print(f"  {len(rob)} configs qualify:")
    for _, r in rob.iterrows():
        print(f"  {r.cfg:22s} {int(r.n):5d} mean {r['mean']:+.2f} med {r.med:+.2f} win {r.win:.0f}% posYrs {int(r.posYrs)}/{int(r.nyr)} TR{r.TR:+.2f} TE{r.TE:+.2f}")

    print(f"\n=== per-year detail — the raw-mean leaders (which years are they positive?) ===")
    for cfg in ["T6→Z1G→T5", "Z1G→T5→T11", "Z1G→T5→T6", "T5→Z3→T4"]:
        s = R[R.cfg == cfg]
        if len(s) < 50:
            print(f"  {cfg}: n={len(s)} few"); continue
        ys = "  ".join(f"{y[2:]}:n{len(v)}/{v.mean():+.1f}/w{(v>0).mean()*100:.0f}" for y, v in s.groupby("yr")["ret"])
        print(f"  {cfg:14s} (n={len(s)}): {ys}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
