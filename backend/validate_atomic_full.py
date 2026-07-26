"""
validate_atomic_full.py — the FULL investigation of the atomic bull edge.
Builds on validate_atomic (path-sim, per-year, 2022). Adds:
  A. INCREMENTAL additivity — marginal lift as atoms stack; redundancy (co-occurrence).
  B. PRICE-TIER — the fib-zone law: which price bucket carries the edge.
  C. MARKET-REGIME gate — causal breadth (fraction of universe with +20d trailing return);
     does trading only in risk-on breadth rescue 2022 / lift overall?
  D. TEMPORAL stability — FULL edge in 2021-23 vs 2024-26 (atoms are structural, so this
     just confirms it isn't a one-era thing).
READ-ONLY on the 1d DB.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import validate_atomic as VA
from edge_replay import _pathsim, _stats

KW = VA.KW


def _fmt(s, ref=None):
    if not s or s.get("n", 0) == 0:
        return "n=0"
    d = f" (Δ{s['mean']-ref:+.2f})" if ref is not None else ""
    return (f"n={s['n']:>6} mean{s['mean']:+5.2f}{d} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} '22={s['per_year'].get('2022',float('nan')):+5.2f}")


def _breadth(df):
    """Per-date market regime: fraction of universe whose close > close 20 bars ago.
    Causal (uses only past). risk_on = breadth > 0.5."""
    d = df.sort_values(["ticker", "date"]).copy()
    d["c20"] = d.groupby("ticker")["close"].shift(20)
    d["up"] = (d["close"] > d["c20"]).astype(float)
    br = d.dropna(subset=["c20"]).groupby("date")["up"].mean()
    return br  # Series indexed by date


def run():
    print("pulling…", flush=True)
    df, as_of = VA._pull(VA.MONTHS, VA.DVF)
    df = VA._frame(df)
    br = _breadth(df)
    df["risk_on"] = df["date"].map(br) > 0.5
    grp_all = VA._grp(df)
    b = df["base"]

    # ── A. incremental additivity ────────────────────────────────────────────
    print("\n" + "="*90 + "\nA. INCREMENTAL STACK (each atom added on top, ALL universes)\n" + "="*90)
    O=df["csfx"]=="O"; G=df["gap"].isin(("G2","G3")); B=df["vb"]=="B"; E=df["ne"]=="E"; L5=df["l"]=="L5"
    steps = [
        ("bull-T base",        b),
        ("+close=O",           b & O),
        ("+gap G2/3",          b & O & G),
        ("+vol=B",             b & O & G & B),
        ("+ne=E (→EO stack)",  b & O & G & B & E),
        ("+l_sig=L5",          b & O & G & B & E & L5),
    ]
    prev = None
    for label, mask in steps:
        s = _stats(label, _pathsim(_grp_with(df, mask), "_m", **KW))
        print(f"  {label:22s} {_fmt(s, prev)}")
        prev = s["mean"] if s.get("n") else prev

    # redundancy: co-occurrence of atoms within base
    print("\n  atom co-occurrence within bull-T base (P(atom) and pairwise):")
    atoms = {"O":O,"gap":G,"B":B,"E":E,"L5":L5,"R2L":df["r2"]=="R2L"}
    nb = int(b.sum())
    for k,m in atoms.items():
        print(f"    P({k}|base)={(m & b).sum()/nb:.2f}", end="  ")
    print()

    # ── B. price-tier (fib zones) ────────────────────────────────────────────
    print("\n" + "="*90 + "\nB. PRICE-TIER of the FULL edge (O&gap&B) — fib-zone law\n" + "="*90)
    for lo, hi, lbl in [(5,8,"$5-8 casino"),(8,21,"$8-21 dead"),(21,89,"$21-89 quality"),(89,377,"$89-377"),(377,1e9,">$377 mega")]:
        mask = df.A_FULL & df.close.between(lo,hi)
        print(f"  {lbl:16s} {_fmt(_stats(lbl, _pathsim(_grp_with(df,mask),'_m',**KW)))}")

    # ── C. market-regime gate ────────────────────────────────────────────────
    print("\n" + "="*90 + "\nC. MARKET-REGIME gate (breadth>50%) on FULL (O&gap&B)\n" + "="*90)
    print(f"  breadth risk_on share of bars: {(df['risk_on']).mean()*100:.0f}%  (2022 risk_on: {df[df.date.astype(str).str[:4]=='2022']['risk_on'].mean()*100:.0f}%)")
    for lbl, mask in [("FULL all", df.A_FULL), ("FULL risk-ON", df.A_FULL & df.risk_on), ("FULL risk-OFF", df.A_FULL & ~df.risk_on)]:
        print(f"  {lbl:16s} {_fmt(_stats(lbl, _pathsim(_grp_with(df,mask),'_m',**KW)))}")

    # ── D. temporal stability ────────────────────────────────────────────────
    print("\n" + "="*90 + "\nD. TEMPORAL stability of FULL (per-year means)\n" + "="*90)
    s = _stats("FULL", _pathsim(_grp_with(df, df.A_FULL), "_m", **KW))
    py = s["per_year"]
    tr = np.mean([py.get(y,0) for y in ["2021","2022","2023"]]); te = np.mean([py.get(y,0) for y in ["2024","2025","2026"]])
    print(f"  per-year: " + " ".join(f"{y}:{py.get(y,float('nan')):+.2f}" for y in ["2021","2022","2023","2024","2025","2026"]))
    print(f"  TRAIN(21-23) mean {tr:+.2f}  vs  TEST(24-26) mean {te:+.2f}  → {'stable ✅' if tr>0 and te>0 else 'era-dependent ⚠'}")
    print(f"\nas_of {as_of}")


def _grp_with(df, mask):
    d = df.copy(); d["_m"] = mask.values if hasattr(mask,"values") else mask
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


if __name__ == "__main__":
    run()
