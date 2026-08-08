"""FINAL exit test (user-approved): A trail25 vs E ATR-x12(clip 15-60%) on the ACTUAL
built board setups, per edge, full 6 years. Both exits run through the same sparse
simulator (edge_replay trail semantics) so the comparison is apples-to-apples.
Per edge: n, med/pf/worst-year under each exit, delta — plus a book-level verdict.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

SLIP = er.SLIP
grp, as_of = er._frame(60, 3_000_000)
g0 = next(iter(grp.values()))
print(f"frame as_of {as_of}", flush=True)

EDGES = []
seen = set()
for code, col in er.DISPLAY_SETUPS:
    if col in seen or col not in g0:
        continue
    seen.add(col)
    EDGES.append((code, col))
print(f"testing {len(EDGES)} built setups", flush=True)


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


def run(col, mode):
    recs = []
    for tk, g in grp.items():
        if col not in g:
            continue
        idx = np.flatnonzero(g[col].to_numpy(bool))
        if len(idx) == 0:
            continue
        if mode == "A":
            trails = np.full(len(idx), 0.25)
        else:
            atr = g["atr_14"].to_numpy(float)[idx]
            clp = g["close"].to_numpy(float)[idx]
            with np.errstate(invalid="ignore", divide="ignore"):
                t = 12.0 * atr / clp
            trails = np.clip(np.nan_to_num(t, nan=0.25), 0.15, 0.60)
        recs.extend(sim(g, idx, trails))
    T = pd.DataFrame(recs, columns=["ret", "yr"])
    if len(T) < 30:
        return None
    ym = T.groupby("yr")["ret"].median() * 100
    w = T["ret"] > 0
    den = -T.loc[~w, "ret"].sum()
    return dict(n=len(T), med=T["ret"].median() * 100,
                pf=(T.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()))


print(f"\n{'edge':16s} {'n':>6s} | {'A med':>7s} {'pf':>5s} {'worst':>7s} {'pos':>4s} |"
      f" {'E med':>7s} {'pf':>5s} {'worst':>7s} {'pos':>4s} | {'Δmed':>6s} {'Δworst':>7s}",
      flush=True)
imp_med = imp_worst = tot = 0
for code, col in EDGES:
    A = run(col, "A"); E = run(col, "E")
    if A is None or E is None:
        continue
    tot += 1
    dm = E["med"] - A["med"]; dw = E["worst"] - A["worst"]
    if dm > 0: imp_med += 1
    if dw > 0: imp_worst += 1
    mark = " ✅" if (dm > 0 and dw >= 0) else (" ⚠" if dm > 0 else " ❌")
    print(f"{code:16s} {A['n']:>6d} | {A['med']:>+7.2f} {A['pf']:>5.2f} {A['worst']:>+7.2f}"
          f" {A['pos']}/{A['ny']} | {E['med']:>+7.2f} {E['pf']:>5.2f} {E['worst']:>+7.2f}"
          f" {E['pos']}/{E['ny']} | {dm:>+6.2f} {dw:>+7.2f}{mark}", flush=True)

print(f"\nmed improved: {imp_med}/{tot} · worst-year improved: {imp_worst}/{tot}", flush=True)
print("DONE", flush=True)
