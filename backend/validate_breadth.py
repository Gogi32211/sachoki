"""
validate_breadth.py — stress-test the counter-intuitive breadth-regime finding before
trusting it: is the atomic edge REALLY stronger when market breadth is low (risk-off),
or is that an artifact of one threshold / one window / one year?

Tests on the FULL atomic mask (bull-T·close=O·gap·vol=B):
  1. DECILE monotonicity — path-sim by breadth decile. Monotone (low breadth→higher edge)
     = a real structural relationship, not a threshold artifact.  [strongest evidence]
  2. THRESHOLD sensitivity — 40/45/50/55/60% cutoffs.
  3. WINDOW sensitivity — 10d/20d/50d trailing-return breadth.
  4. PER-YEAR — does risk-off > risk-on hold every year (incl. OOS 2024-26)?
Causal: breadth = fraction of universe with close>close[-w], known at signal bar. READ-ONLY.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import validate_atomic as VA
from edge_replay import _pathsim, _stats

KW = VA.KW


def _breadth(df, w):
    d = df.sort_values(["ticker", "date"]).copy()
    d["cw"] = d.groupby("ticker")["close"].shift(w)
    d["up"] = (d["close"] > d["cw"]).astype(float)
    return d.dropna(subset=["cw"]).groupby("date")["up"].mean()


def _grp(df, mask):
    d = df.copy(); d["_m"] = mask.values
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def run():
    print("pulling…", flush=True)
    df, as_of = VA._pull(VA.MONTHS, VA.DVF)
    df = VA._frame(df)
    F = df["A_FULL"]
    for w in (10, 20, 50):
        df[f"br{w}"] = df["date"].map(_breadth(df, w))

    # ── 1. decile monotonicity (20d breadth) ────────────────────────────────
    print("\n" + "="*84 + "\n1. FULL edge by BREADTH DECILE (20d) — is it monotone?\n" + "="*84)
    fb = df[F].dropna(subset=["br20"])
    df["_dec"] = pd.qcut(df["br20"].rank(method="first"), 10, labels=False)  # 0=lowest breadth
    # decile edges by actual breadth
    print(f"  {'decile':>6} {'breadth':>9} {'n':>6} {'mean':>7} {'med':>7} {'win':>6}")
    for dcl in range(10):
        m = F & (df["_dec"] == dcl)
        s = _stats("d", _pathsim(_grp(df, m), "_m", **KW))
        brmean = df.loc[m, "br20"].mean()
        if s.get("n", 0):
            print(f"  {dcl:>6} {brmean:>8.2f}  {s['n']:>6} {s['mean']:>+6.2f} {s['median']:>+6.2f} {s['win']:>5.1f}")

    # ── 2. threshold sensitivity ────────────────────────────────────────────
    print("\n" + "="*84 + "\n2. THRESHOLD sensitivity (20d breadth): risk-OFF vs risk-ON mean\n" + "="*84)
    for cut in (0.40, 0.45, 0.50, 0.55, 0.60):
        off = _stats("o", _pathsim(_grp(df, F & (df["br20"] < cut)), "_m", **KW))
        on = _stats("n", _pathsim(_grp(df, F & (df["br20"] >= cut)), "_m", **KW))
        print(f"  cut {cut:.2f}: OFF mean{off.get('mean',0):+5.2f} (n{off.get('n',0):>5}, {off.get('pos_years',0)}/{off.get('total_years',0)}yr)"
              f"  |  ON mean{on.get('mean',0):+5.2f} (n{on.get('n',0):>5}, {on.get('pos_years',0)}/{on.get('total_years',0)}yr)")

    # ── 3. window sensitivity (cut 0.50) ────────────────────────────────────
    print("\n" + "="*84 + "\n3. WINDOW sensitivity (breadth<50%): does risk-off edge survive 10/20/50d?\n" + "="*84)
    for w in (10, 20, 50):
        off = _stats("o", _pathsim(_grp(df, F & (df[f"br{w}"] < 0.5)), "_m", **KW))
        on = _stats("n", _pathsim(_grp(df, F & (df[f"br{w}"] >= 0.5)), "_m", **KW))
        print(f"  {w}d: OFF mean{off.get('mean',0):+5.2f} med{off.get('median',0):+5.2f} (n{off.get('n',0):>5})"
              f"  |  ON mean{on.get('mean',0):+5.2f} med{on.get('median',0):+5.2f} (n{on.get('n',0):>5})")

    # ── 4. per-year risk-off vs risk-on (20d, 50%) ──────────────────────────
    print("\n" + "="*84 + "\n4. PER-YEAR risk-OFF vs risk-ON mean (20d, <50%) — consistent? OOS?\n" + "="*84)
    off = _stats("o", _pathsim(_grp(df, F & (df["br20"] < 0.5)), "_m", **KW))
    on = _stats("n", _pathsim(_grp(df, F & (df["br20"] >= 0.5)), "_m", **KW))
    print(f"  {'year':>6} {'OFF':>8} {'ON':>8} {'OFF-ON':>8}")
    for y in ["2021", "2022", "2023", "2024", "2025", "2026"]:
        o = off["per_year"].get(y); n = on["per_year"].get(y)
        if o is not None and n is not None:
            print(f"  {y:>6} {o:>+7.2f} {n:>+7.2f} {o-n:>+7.2f}  {'✅' if o>n else '❌'}")
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
