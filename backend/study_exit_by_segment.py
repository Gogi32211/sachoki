"""Knot #1 test (user-approved 2026-08-06): is the ⚡-segment's edge weakness an ENTRY
problem or an EXIT problem?

Reversal family (8 edges), frozen 2021-23 labels, OOS trades 2024-26, THREE exits:
  A: trail 25%            (the book's exit — what showed 🏦 > ⚡)
  B: trail 40%            (wide — lets volatile names breathe)
  C: hold-20, no stop     (the CAP contract: "hold ~15-20d · no stop · sit the MAE")
If ⚡ catches up to 🏦 under B/C, the weakness is exit mechanics → the gate should be an
EXIT-SWITCHER, not a suppressor. If ⚡ stays behind under every exit, it is a real
entry-quality deficit → suppressor framing was right after all.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

BASE = os.path.dirname(os.path.abspath(__file__))
SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
print(f"frozen labels: {SEG.value_counts().to_dict()}", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

REV = [("QZC", "E_qzcapit"), ("WSH", "E_washout"), ("D+L1", "E_dl1"),
       ("G3", "E_g3"), ("ZRT", "E_zoneretest"), ("ATM", "E_atomic"),
       ("SPR", "E_spring"), ("G3A", "E_g3abs")]
EXITS = [("A trail25%/60b", dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)),
         ("B trail40%/60b", dict(mode="trail", stop=0.10, target=0.25, trail=0.40, maxh=60)),
         ("C hold20 nostop", dict(mode="trail", stop=0.10, target=0.25, trail=5.00, maxh=20))]

for ename, kw in EXITS:
    frames = []
    for n, col in REV:
        tr = er._pathsim(grp, col, kw["mode"], kw["stop"], kw["target"], kw["trail"], kw["maxh"])
        tr["edge"] = n
        frames.append(tr)
    T = pd.concat(frames, ignore_index=True)
    T["date_in"] = pd.to_datetime(T["date_in"]).astype(str)
    T = T[T["date_in"] >= "2024-01-01"].copy()
    T["seg"] = T["ticker"].map(SEG).fillna("∅")
    T["y"] = T["date_in"].str[:4]
    print(f"\n===== EXIT {ename} =====", flush=True)
    res = {}
    for sgn in ["🏦", "⚡", "🎰", "∅"]:
        sub = T[T["seg"] == sgn]
        if len(sub) < 50:
            print(f"  {sgn} n={len(sub)} thin", flush=True); continue
        ym = sub.groupby("y")["ret"].median() * 100
        w = sub["ret"] > 0
        den = -sub.loc[~w, "ret"].sum()
        pf = (sub.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
        med = sub["ret"].median() * 100
        res[sgn] = med
        ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in (2024, 2025, 2026))
        print(f"  {sgn} n={len(sub):>6d} med{med:>+7.2f} win{w.mean()*100:>5.1f} pf{pf:>5.2f}"
              f" MAE{sub['mae'].median()*100:>6.2f} hold{sub['hold'].mean():>5.1f} | {ys}"
              f" {int((ym>0).sum())}/{len(ym)}", flush=True)
    if "🏦" in res and "⚡" in res:
        print(f"  Δ(🏦−⚡) = {res['🏦'] - res['⚡']:+.2f}", flush=True)
    # per-year delta for the verdict
    for y in ("2024", "2025", "2026"):
        a = T[(T["seg"] == "🏦") & (T["y"] == y)]["ret"].median()
        b = T[(T["seg"] == "⚡") & (T["y"] == y)]["ret"].median()
        if pd.notna(a) and pd.notna(b):
            print(f"    {y}: Δ {100*(a-b):+.2f}", flush=True)

print("\nDONE", flush=True)
