"""
validate_engulf_oos.py — temporal-stability / walk-forward for the engulf-reversal edge.
Engulf filters are STRUCTURAL (definitional, not fitted), so the honest OOS question is:
does the engulf-3 LIFT over the bull-T base hold in TEST (2024-26) as in TRAIN (2021-23)?
If Δ(engulf−base) is positive in BOTH halves (and per-year), it's a stable structural edge,
not an era artifact. Best cell from the in-sample sweep: russell2k · ANY bull-T · engulf-3 (range).
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
import validate_t6_engulf as V
from edge_replay import _pathsim, _stats

V.SIG = "ANY"
KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


def _py(mask, df):
    s = _stats("x", _pathsim(V._grp(df, mask), "_m", **KW))
    py = s.get("per_year", {})
    return s, py


def _avg(py, yrs):
    v = [py[y] for y in yrs if y in py]
    return sum(v) / len(v) if v else float("nan")


def run():
    print("pulling ANY-T…", flush=True)
    df, as_of = V._pull(V.MONTHS, V.DVF)
    df = V._prep(df)
    anyT = df["t6"] & df["clean"]
    print(f"as_of {as_of} · trail25/60 · TRAIN=2021-23  TEST=2024-26 · ANY bull-T · engulf-3(range)\n")
    stats = {}
    cells = []
    for u in ("sp500", "nasdaq", "russell2k"):
        um = anyT & (df["universe"] == u)
        cells += [(f"{u} base", um), (f"{u} +engulf3", um & df["engR3"])]
    for name, m in cells:
        s, py = _py(m, df)
        stats[name] = (s, py)
        tr, te = _avg(py, TR), _avg(py, TE)
        pys = " ".join(f"{y}:{py.get(y, float('nan')):+.2f}" for y in TR + TE)
        print(f"  {name:20s} n={s.get('n',0):>6} | TRAIN {tr:+.2f}  TEST {te:+.2f} | med{s.get('median',0):+.2f} pf{s.get('pf')} | {pys}")
        if "+engulf3" in name:
            print()

    print("── ENGULF LIFT Δ(engulf − base), per universe & period ──")
    for u in ("sp500", "nasdaq", "russell2k"):
        _, pb = stats[f"{u} base"]; _, pe = stats[f"{u} +engulf3"]
        dtr = _avg(pe, TR) - _avg(pb, TR); dte = _avg(pe, TE) - _avg(pb, TE)
        verdict = "✅ holds OOS" if (dtr > 0 and dte > 0) else ("⚠ test-only" if dte > 0 else ("⚠ train-only" if dtr > 0 else "❌ no lift"))
        print(f"  {u:10s} Δ TRAIN {dtr:+.2f} · Δ TEST {dte:+.2f}   {verdict}")
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
