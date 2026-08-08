"""Exit-geometry showdown over the FULL 6 years (user-approved 2026-08-06):
  A: trail 25% fixed        (the book's exit)
  B: trail 40% fixed        (the 2024-26 winner — must face 2022)
  D: ATR-adaptive trail = clip( 8 x ATR%, 15%, 60%)   ("fit the stop to the stock")
  E: ATR-adaptive trail = clip(12 x ATR%, 15%, 60%)
Reversal family (8 edges) pooled; per-year medians 2021-26; segment slices (frozen
2021-23 labels — in-sample for the 21-23 portion, disclosed; exit comparisons are
within-segment so label circularity matters little here).

Sparse path-sim replicating edge_replay trail semantics exactly (entry next open
x(1+SLIP), trail from prior peak, gap-through fills at open, -SLIP on exit, 5-bar
spacing, time stop at close) — needed because _pathsim cannot take a per-trade trail.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

BASE = os.path.dirname(os.path.abspath(__file__))
SLIP = er.SLIP
SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]

grp, as_of = er._frame(60, 3_000_000)
g0 = next(iter(grp.values()))
ATRCOL = "atr_14" if "atr_14" in g0 else None
print(f"frame as_of {as_of} · atr col: {ATRCOL}", flush=True)

REV = [("QZC", "E_qzcapit"), ("WSH", "E_washout"), ("D+L1", "E_dl1"),
       ("G3", "E_g3"), ("ZRT", "E_zoneretest"), ("ATM", "E_atomic"),
       ("SPR", "E_spring"), ("G3A", "E_g3abs")]


def pathsim_var(gdf, idx, trails, maxh=60):
    """sparse trail-mode sim; trails[i] = per-trade trail fraction"""
    o = gdf["open"].to_numpy(float); hi = gdf["high"].to_numpy(float)
    lo = gdf["low"].to_numpy(float); cl = gdf["close"].to_numpy(float)
    dts = gdf["date"].astype(str).to_numpy()
    n = len(gdf)
    out = []
    last = -99
    for j_i, i in enumerate(idx):
        if i + 1 >= n or i - last < 5:
            continue
        ep = o[i + 1]
        if not np.isfinite(ep) or ep <= 0:
            continue
        last = i
        trail = trails[j_i]
        entry = ep * (1 + SLIP); ret = None
        end = min(i + 1 + maxh, n); pk = entry; mlo = entry
        for j in range(i + 1, end):
            if lo[j] < mlo: mlo = lo[j]
            ts_prev = pk * (1 - trail)
            if j > i + 1 and o[j] <= ts_prev:
                ret = o[j] / entry - 1 - SLIP; break
            if hi[j] > pk: pk = hi[j]
            ts = pk * (1 - trail)
            if lo[j] <= ts:
                ret = ts / entry - 1 - SLIP; break
        if ret is None:
            ret = cl[end - 1] / entry - 1 - SLIP
        out.append((ret, dts[i][:4], mlo / entry - 1))
    return out


def run_exit(kind, fixed=None, k=None, lo_c=0.15, hi_c=0.60):
    recs = []
    for name, col in REV:
        for tk, g in grp.items():
            if col not in g:
                continue
            idx = np.flatnonzero(g[col].to_numpy(bool))
            if len(idx) == 0:
                continue
            if kind == "fixed":
                trails = np.full(len(idx), fixed)
            else:
                atr = g[ATRCOL].to_numpy(float)[idx]
                clp = g["close"].to_numpy(float)[idx]
                with np.errstate(invalid="ignore", divide="ignore"):
                    t = k * atr / clp
                trails = np.clip(np.nan_to_num(t, nan=fixed or 0.25), lo_c, hi_c)
            for ret, yr, mae in pathsim_var(g, idx, trails):
                recs.append((tk, ret, yr, mae))
    return pd.DataFrame(recs, columns=["ticker", "ret", "yr", "mae"])


def stats_line(sub, label):
    if len(sub) < 50:
        print(f"  {label:6s} n={len(sub)} thin", flush=True); return None
    ym = sub.groupby("yr")["ret"].median() * 100
    w = sub["ret"] > 0
    den = -sub.loc[~w, "ret"].sum()
    pf = (sub.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    print(f"  {label:6s} n={len(sub):>6d} med{sub['ret'].median()*100:>+6.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)}"
          f" worst{ym.min():>+6.2f}", flush=True)
    return ym


VARIANTS = [("A trail25", dict(kind="fixed", fixed=0.25)),
            ("B trail40", dict(kind="fixed", fixed=0.40)),
            ("D ATRx8",   dict(kind="atr", k=8)),
            ("E ATRx12",  dict(kind="atr", k=12))]

for vname, kw in VARIANTS:
    T = run_exit(**kw)
    T["seg"] = T["ticker"].map(SEG).fillna("∅")
    print(f"\n===== {vname} — pooled REV family, ALL YEARS =====", flush=True)
    stats_line(T, "ALL")
    for sgn in ["🏦", "⚡", "🎰", "∅"]:
        stats_line(T[T["seg"] == sgn], sgn)

print("\nDONE", flush=True)
