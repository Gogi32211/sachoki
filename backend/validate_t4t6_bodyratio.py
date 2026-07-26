"""
validate_t4t6_bodyratio.py — does the T4/T6 body vs PRIOR bar body ratio
predict forward win rate? (GEM1 generalization: GEM1 = small T1 body <0.5×
prior-Z body. T4/T6 are ENGULF signals so ratio is usually >1 — opposite regime.)

For every T4 / T6 bar (equity universe, dv>2M, close>3):
  ratio = |close-open| / |prev_close-prev_open|
  entry = next open, trail25 stop-first (our standard), 60-bar cap.
Report by ratio bucket × prior-bar direction, with per-year medians.
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB

DVMIN = 2_000_000; PMIN = 3.0; SLIP = 0.0015


def main():
    t0 = time.time()
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = a.execute("""SELECT ticker, CAST(date AS VARCHAR)[:4] yr, open, high, low, close, volume,
                            t_sig, rsi_14 FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    a.close()
    g = d.groupby("ticker", sort=False)
    d["pbody"] = (g["close"].shift(1) - g["open"].shift(1))          # signed prior body
    d["pbull"] = d["pbody"] > 0
    d["body"] = (d["close"] - d["open"]).abs()
    d["ratio"] = d["body"] / d["pbody"].abs().replace(0, np.nan)
    d["dv"] = d["close"] * d["volume"]
    # forward arrays for path-sim
    o = d["open"].to_numpy(float); h = d["high"].to_numpy(float)
    l = d["low"].to_numpy(float); c = d["close"].to_numpy(float)
    tick = d["ticker"].to_numpy()
    n = len(d)

    def trail25(i):
        if i + 1 >= n or tick[i + 1] != tick[i]:
            return None
        ep = o[i + 1] * (1 + SLIP)
        if ep <= 0:
            return None
        pk = ep; end = i + 1
        for j in range(i + 1, min(i + 61, n)):
            if tick[j] != tick[i]:
                break
            end = j
            tsl = pk * 0.75
            if j > i + 1 and o[j] <= tsl:
                return o[j] / ep - 1 - SLIP
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                return ts / ep - 1 - SLIP
        return c[end] / ep - 1 - SLIP

    for sig in ["T4", "T6"]:
        sub = d[(d.t_sig == sig) & (d.dv >= DVMIN) & (d.close >= PMIN) & d.ratio.notna() & (d.ratio > 0)]
        idx = sub.index.to_numpy()
        rets = np.array([trail25(i) for i in idx], float)
        ok = ~np.isnan(rets)
        S = sub.iloc[ok].copy(); S["ret"] = rets[ok] * 100
        print(f"\n########## {sig} (full engulf / engulf-bull) — n={len(S):,} · trail25 ##########")
        base = S["ret"]
        print(f"  ALL: mean {base.mean():+.2f} median {base.median():+.2f} win {(base>0).mean()*100:.0f}%")
        buckets = [(0, 0.5, "<0.5×"), (0.5, 1.0, "0.5-1×"), (1.0, 1.5, "1-1.5×"),
                   (1.5, 2.5, "1.5-2.5×"), (2.5, 4.0, "2.5-4×"), (4.0, 1e9, "4×+")]
        print(f"  {'ratio bucket':12s} {'n':>7s} {'mean':>7s} {'med':>7s} {'win':>4s}   per-year median")
        for lo_, hi_, lab in buckets:
            b = S[(S.ratio >= lo_) & (S.ratio < hi_)]
            if len(b) < 200:
                print(f"  {lab:12s} {len(b):7d}  (too few)"); continue
            yr = b.groupby("yr")["ret"].median()
            ys = " ".join(f"{y[2:]}:{v:+.1f}" for y, v in yr.items())
            print(f"  {lab:12s} {len(b):7d} {b.ret.mean():+7.2f} {b.ret.median():+7.2f} {(b.ret>0).mean()*100:4.0f}   {ys}")
        # prior-bar direction split
        print("  — by PRIOR bar direction —")
        for pv, plab in [(True, "prior BULL"), (False, "prior BEAR")]:
            b = S[S.pbull == pv]
            print(f"    {plab}: n={len(b):,} mean {b.ret.mean():+.2f} med {b.ret.median():+.2f} win {(b.ret>0).mean()*100:.0f}%")
        # best cell: prior-bear × small ratio (GEM1-like) vs prior-bear × big engulf
        for lo_, hi_, lab in [(0, 1.0, "≤1×"), (1.0, 2.5, "1-2.5×"), (2.5, 1e9, "2.5×+")]:
            b = S[(~S.pbull) & (S.ratio >= lo_) & (S.ratio < hi_)]
            if len(b) >= 200:
                yr = b.groupby("yr")["ret"].median()
                pos = int((yr > 0).sum())
                print(f"    prior-BEAR × {lab:7s}: n={len(b):5d} mean {b.ret.mean():+.2f} med {b.ret.median():+.2f} win {(b.ret>0).mean()*100:.0f}% posYrs {pos}/{len(yr)}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
