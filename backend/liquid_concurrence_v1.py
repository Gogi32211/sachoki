"""LIQUID_CONCURRENCE_V1 — the registered read. Executes research_out/LIQUID_CONCURRENCE_V1_PREREG.md
(frozen 0af6c02, corrected 164fe2c) without deviation.

READ WINDOW: 2026-01-01 … 2026-06-30 ONLY. 2026-07-01 onward stays sealed and is never touched here,
including on a FAIL. The narrow secondary segment can never substitute for the primary.

Two endpoints, kept separate:
  1 DESCRIPTIVE  RET1/3/5/10/20 — N, mean, median, % > 0, P25, P75. No controls, no matching, no
                 market adjustment. Answers "after concurrence, did price rise or fall".
  2 RELATIVE     diff_d = median(RET20 | cross, day d) − median(RET20 | eligible NON-cross, same
                 segment, same day); primary = median over days, date-clustered bootstrap CI.
                 The comparator is the ENTIRE same-day pool — there is no matched-control k.
"""
from __future__ import annotations
import os, sys
import numpy as np, pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ovd_build as B

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
READ = ("2026-01-01", "2026-06-30")          # H1 only — the registered window
SEALED_FROM = "2026-07-01"                   # never read here
HOR = [1, 3, 5, 10, 20]
MIN_DAY_ROWS = 20                            # days with fewer eligible rows in the segment are dropped
RNG = np.random.default_rng(20260918)
L = lambda c="═", n=92: print(c * n)


def frame():
    X = pd.read_parquet(R + "cross_star_features.parquet")
    X["date"] = pd.to_datetime(X["date"])
    E = X[X.both_covered & (X.date >= READ[0]) & (X.date <= READ[1])].copy()
    assert int((E.date >= SEALED_FROM).sum()) == 0, "sealed window touched"
    cur = B.canonical_current()
    P = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "open", "close"])
    P = P.rename(columns={"session_date": "date"}); P["date"] = pd.to_datetime(P["date"])
    P = P.sort_values(["ticker", "date"]).reset_index(drop=True)
    P["i"] = P.groupby("ticker").cumcount(); P["n"] = P.groupby("ticker")["i"].transform("size")
    return E, P, cur


def rets(rows, P):
    j = rows[["ticker", "date"]].merge(P[["ticker", "date", "i", "n", "close", "open"]],
                                       on=["ticker", "date"], how="left")
    ok = j["i"].notna(); j = j[ok].copy(); j["i"] = j["i"].astype(int)
    idx = P.set_index(["ticker", "i"])
    nxt = idx.reindex(pd.MultiIndex.from_arrays([j.ticker, j.i + 1]))
    j["open1"] = np.where((j.i + 1) < j.n, nxt["open"].to_numpy(), np.nan)
    for h in HOR:
        fut = idx.reindex(pd.MultiIndex.from_arrays([j.ticker, j.i + h]))
        c = np.where((j.i + h) < j.n, fut["close"].to_numpy(), np.nan)
        j[f"r{h}"] = c / j.close.to_numpy() - 1.0
        j[f"o{h}"] = c / j.open1.to_numpy() - 1.0
    return j, int((~ok).sum())


def descriptive(j, label):
    print(f"\n  {label}")
    print(f"  {'Horizon':>8s} {'N resolved':>11s} {'mean':>9s} {'median':>9s} {'% > 0':>8s} {'P25':>9s} {'P75':>9s}")
    for h in HOR:
        v = j[f"r{h}"].to_numpy(); v = v[np.isfinite(v)]
        q = np.percentile(v, [25, 75])
        print(f"  {str(h)+'d':>8s} {len(v):>11,} {100*v.mean():>8.2f}% {100*np.median(v):>8.2f}%"
              f" {100*(v>0).mean():>7.1f}% {100*q[0]:>8.2f}% {100*q[1]:>8.2f}%")


def relative(cross, noncross, tag):
    a = cross.groupby("date")["r20"].agg(["median", "size"]).rename(columns={"median": "c", "size": "nc"})
    b = noncross.groupby("date")["r20"].agg(["median", "size"]).rename(columns={"median": "b", "size": "nb"})
    d = a.join(b, how="inner")
    d = d[(d.nb >= MIN_DAY_ROWS) & d.c.notna() & d.b.notna()]
    d["diff"] = d.c - d.b
    v = d["diff"].to_numpy()
    bs = np.array([RNG.choice(v, len(v), replace=True).mean() for _ in range(5000)])
    lo, hi = np.percentile(bs, [2.5, 97.5])
    print(f"\n  {tag}")
    print(f"    sessions used            {len(d):,}  (min {MIN_DAY_ROWS} eligible non-cross rows/day)")
    print(f"    median(diff_d)           {100*np.median(v):+7.3f} pp")
    print(f"    mean(diff_d)             {100*v.mean():+7.3f} pp")
    print(f"    date-clustered 95 % CI   [{100*lo:+.3f}, {100*hi:+.3f}] pp   "
          f"{'EXCLUDES 0' if lo*hi > 0 else 'includes 0'}")
    print(f"    days with diff_d > 0     {100*(v>0).mean():5.1f} %")
    drop = d["diff"].abs().sort_values(ascending=False).head(3).index
    v2 = d.drop(drop)["diff"].to_numpy()
    bs2 = np.array([RNG.choice(v2, len(v2), replace=True).mean() for _ in range(5000)])
    lo2, hi2 = np.percentile(bs2, [2.5, 97.5])
    print(f"    minus 3 largest movers   {100*np.median(v2):+7.3f} pp · CI [{100*lo2:+.3f}, {100*hi2:+.3f}] pp"
          f"   {'EXCLUDES 0' if lo2*hi2 > 0 else 'includes 0'}")
    return float(np.median(v)), (lo, hi), float((v > 0).mean())


def main():
    E, P, cur = frame()
    L(); print(f"LIQUID_CONCURRENCE_V1 · REGISTERED READ {READ[0]} … {READ[1]} · "
               f"canonical {cur['derived_sha256_16']}"); L()
    for seg_name, seg in (("PRIMARY  close >= $21", E.close >= 21),
                          ("SECONDARY  russell2k · $2-21 · >=$10M",
                           (E.universe == "russell2k") & (E.close >= 2) & (E.close < 21) & (E.dollar_vol >= 1e7))):
        S = E[seg]
        cross = S[S.same_day_cross]; non = S[~S.same_day_cross]
        jc, miss_c = rets(cross, P); jn, _ = rets(non, P)
        L("─"); print(f"{seg_name}   events {len(cross):,} · no canonical price {miss_c:,} · "
                      f"eligible non-cross {len(non):,}"); L("─")
        print("\n  ENDPOINT 1 — DESCRIPTIVE (no controls, no matching, no market adjustment)")
        descriptive(jc, "close(t) -> close(t+N)")
        print(f"\n  {'Horizon':>8s} {'N':>11s} {'mean':>9s} {'median':>9s} {'% > 0':>8s}   [open(t+1) -> close(t+N)]")
        for h in HOR:
            v = jc[f"o{h}"].to_numpy(); v = v[np.isfinite(v)]
            print(f"  {str(h)+'d':>8s} {len(v):>11,} {100*v.mean():>8.2f}% {100*np.median(v):>8.2f}% {100*(v>0).mean():>7.1f}%")
        print("\n  ENDPOINT 2 — RELATIVE (pre-registered test)")
        relative(jc, jn, "diff_d = median(RET20 | CROSS) − median(RET20 | eligible NON-CROSS), same day")


if __name__ == "__main__":
    main()
