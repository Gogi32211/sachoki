"""k-plateau for the ATR exit (pre-build sanity, user-approved): k = 10/12/14/16,
clip [15%, 60%], on (a) the pooled reversal family and (b) four marker edges of very
different nature (QZC capitulation, G3 gap, L43 absorption, 👑 watch-seq). If the
improvement degrades smoothly across k, k=12 is a plateau point, not a lucky pick.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

SLIP = er.SLIP
grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

POOL = [("QZC", "E_qzcapit"), ("WSH", "E_washout"), ("D+L1", "E_dl1"), ("G3", "E_g3"),
        ("ZRT", "E_zoneretest"), ("ATM", "E_atomic"), ("SPR", "E_spring"), ("G3A", "E_g3abs")]
MARK = [("QZC", "E_qzcapit"), ("G3", "E_g3"), ("L43", "E_l43triple"), ("👑", "E_z1gcrown")]


def sim(gdf, idx, trails, maxh=60):
    o = gdf["open"].to_numpy(float); hi = gdf["high"].to_numpy(float)
    lo = gdf["low"].to_numpy(float); cl = gdf["close"].to_numpy(float)
    dts = gdf["date"].astype(str).to_numpy()
    n = len(gdf); out = []; last = -99
    for j_i, i in enumerate(idx):
        if i + 1 >= n or i - last < 5:
            continue
        ep = o[i + 1]
        if not np.isfinite(ep) or ep <= 0:
            continue
        last = i
        trail = trails[j_i]
        entry = ep * (1 + SLIP); ret = None
        end = min(i + 1 + maxh, n); pk = entry
        for j in range(i + 1, end):
            ts_prev = pk * (1 - trail)
            if j > i + 1 and o[j] <= ts_prev:
                ret = o[j] / entry - 1 - SLIP; break
            if hi[j] > pk: pk = hi[j]
            ts = pk * (1 - trail)
            if lo[j] <= ts:
                ret = ts / entry - 1 - SLIP; break
        if ret is None:
            ret = cl[end - 1] / entry - 1 - SLIP
        out.append((ret, dts[i][:4]))
    return out


def run(cols, k):
    recs = []
    for _, col in cols:
        for tk, g in grp.items():
            if col not in g:
                continue
            idx = np.flatnonzero(g[col].to_numpy(bool))
            if len(idx) == 0:
                continue
            atr = g["atr_14"].to_numpy(float)[idx]
            clp = g["close"].to_numpy(float)[idx]
            with np.errstate(invalid="ignore", divide="ignore"):
                t = k * atr / clp
            trails = np.clip(np.nan_to_num(t, nan=0.25), 0.15, 0.60)
            recs.extend(sim(g, idx, trails))
    T = pd.DataFrame(recs, columns=["ret", "yr"])
    ym = T.groupby("yr")["ret"].median() * 100
    w = T["ret"] > 0
    den = -T.loc[~w, "ret"].sum()
    return dict(n=len(T), med=T["ret"].median() * 100,
                pf=(T.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                worst=float(ym.min()), pos=int((ym > 0).sum()), ny=len(ym))


print("\n===== pooled REV family =====", flush=True)
print(f"{'k':>4s} {'med':>7s} {'pf':>5s} {'worst':>7s} pos", flush=True)
for k in (10, 12, 14, 16):
    r = run(POOL, k)
    print(f"{k:>4d} {r['med']:>+7.2f} {r['pf']:>5.2f} {r['worst']:>+7.2f} {r['pos']}/{r['ny']}", flush=True)

for name, col in MARK:
    print(f"\n===== {name} =====", flush=True)
    print(f"{'k':>4s} {'med':>7s} {'pf':>5s} {'worst':>7s} pos", flush=True)
    for k in (10, 12, 14, 16):
        r = run([(name, col)], k)
        print(f"{k:>4d} {r['med']:>+7.2f} {r['pf']:>5.2f} {r['worst']:>+7.2f} {r['pos']}/{r['ny']}", flush=True)

print("\nDONE", flush=True)
