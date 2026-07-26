"""
validate_3bar_rsi.py — add an RSI (STATE) filter to the robust 3-bar configs.
Slices each target config by entry-bar RSI band; trail25 path-sim; per-band
mean/median/win/posYrs. Does oversold RSI lift them (GEM1/T5-L5 logic)?
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB
DVMIN = 2_000_000; PMIN = 3.0; SLIP = 0.0015

TARGETS = ["Z11→T9→T2", "Z3→T1G→T6", "T4→T12→T2G", "T5→T11→T2", "T1G→Z4→T5",
           "T1G→Z1→T1G", "Z2G→T1G→T11", "Z10→T1G→T12", "T5→T2G→T12",
           "T6→Z1G→T5"]   # last = the 2025-artifact, to see if RSI rescues it


def main():
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:4] yr, open,high,low,close,volume,rsi_14,
                       coalesce(t_sig,'') t, coalesce(z_sig,'') z FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker,date""").fetchdf()
    a.close()
    d["tz"] = np.where(d.t != "", d.t, np.where(d.z != "", d.z, "·"))
    g = d.groupby("ticker", sort=False)
    d["tz1"] = g["tz"].shift(1); d["tz2"] = g["tz"].shift(2)
    d["dv"] = d.close * d.volume
    d["cfg"] = d.tz2.astype(str) + "→" + d.tz1.astype(str) + "→" + d.tz
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
            end = j; tsl = pk * 0.75
            if j > i + 1 and o[j] <= tsl:
                return o[j] / ep - 1 - SLIP
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                return ts / ep - 1 - SLIP
        return c[end] / ep - 1 - SLIP

    mask = (d.t != "") & (d.dv >= DVMIN) & (d.close >= PMIN) & d.cfg.isin(TARGETS)
    idx = np.where(mask.to_numpy())[0]
    cfg = d.cfg.to_numpy(); yr = d.yr.to_numpy(); rsi = d.rsi_14.to_numpy()
    rec = [(cfg[i], yr[i], rsi[i], r * 100) for i in idx if (r := ts25(i)) is not None]
    R = pd.DataFrame(rec, columns=["cfg", "yr", "rsi", "ret"])
    print(f"sims {len(R):,} ({time.time()-t0:.0f}s)\n")

    def line(s, lab):
        if len(s) < 60:
            print(f"    {lab:14s} n={len(s):4d} (small-n)"); return
        yrm = s.groupby("yr")["ret"].median(); pos = int((yrm > 0).sum())
        print(f"    {lab:14s} n={len(s):4d} mean {s.ret.mean():+6.2f} med {s.ret.median():+6.2f} win {(s.ret>0).mean()*100:3.0f}% posYrs {pos}/{len(yrm)}")

    for t in TARGETS:
        s = R[R.cfg == t]
        if len(s) < 60:
            continue
        print(f"### {t}  (all n={len(s)})")
        line(s, "ALL RSI")
        for lo, hi, lab in [(0, 30, "RSI<30"), (30, 40, "RSI30-40"), (40, 50, "RSI40-50"),
                            (50, 60, "RSI50-60"), (60, 100, "RSI>60")]:
            line(s[(s.rsi >= lo) & (s.rsi < hi)], lab)
        # best oversold cut
        line(s[s.rsi < 45], "RSI<45")
        print()
    print(f"done {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
