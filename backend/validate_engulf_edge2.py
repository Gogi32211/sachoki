"""
validate_engulf_edge.py — does the engulf bar ABSORBING a recent EDGE signal matter?

Setup: ANY bull-T that RANGE-engulfs the prior 3 bars (our engulf-reversal). Now split by
whether ANY Edge-board setup (L43-TRIPLE, Z11-T11, Washout, D+L1, G3, Atomic, H1, Spring,
P55, Parabola, Atomic-R) fired on one of those 3 swept bars (t-1/t-2/t-3). Hypothesis: an
engulf that swallows a fresh EDGE signal = confirmation/absorption → stronger.
Reuses edge_replay's E_ masks. path-sim trail25/60 + TRAIN/TEST + 2022. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
E_COLS = ["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
          "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR"]


def _grp(df, mask):
    d = df.copy(); d["_m"] = mask.values if hasattr(mask, "values") else mask
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, mask, df):
    s = _stats("x", _pathsim(_grp(df, mask), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"  {lbl:26s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling (edge_replay frame)…", flush=True)
    df, as_of = ER._pull(62, 3_000_000)
    df = ER._prep(df)
    g = df.groupby("ticker", sort=False)
    # ANY bull-T (incl T4/T6, which edge_replay's _BULLT excludes)
    df["anybt"] = df["t"].str.match(r"^T\d").fillna(False)
    # engulf-3 range (outside bar covering prior 3)
    hmax = g["high"].transform(lambda s: s.shift(1).rolling(2).max())
    lmin = g["low"].transform(lambda s: s.shift(1).rolling(2).min())
    df["engR2"] = (df["high"] >= hmax) & (df["low"] <= lmin)
    # any Edge-board setup fired on the CURRENT bar
    df["any_edge"] = df[E_COLS].any(axis=1)
    # …and on one of the 3 bars the engulf sweeps (t-1/t-2/t-3)
    ae = df["any_edge"].astype(float)
    df["edge_in2"] = ((g_ae := ae.groupby(df["ticker"])).shift(1).fillna(0) + g_ae.shift(2).fillna(0)) > 0

    base = df["anybt"] & df["clean"] & df["engR2"]
    qual = base & (df["close"] >= 21) & (df["rsi_14"] < 45)     # the winning filter
    print(f"as_of {as_of} · engulf-3 base n={int(base.sum())} · edge-in-3 rate {df.loc[base,'edge_in2'].mean()*100:.0f}%\n")

    print("── engulf-3 (ANY bull-T) split by EDGE-signal in the 3 swept bars ──")
    print(_line("engulf3 ALL", base, df))
    print(_line("  + edge-in-3 ✓", base & df["edge_in2"], df))
    print(_line("  + NO edge-in-3", base & ~df["edge_in2"], df))

    print("\n── same, on the WINNING filter (≥$21 · RSI<45) ──")
    print(_line("engulf3+qual ALL", qual, df))
    print(_line("  + edge-in-3 ✓", qual & df["edge_in2"], df))
    print(_line("  + NO edge-in-3", qual & ~df["edge_in2"], df))

    print("\n── which Edge setup absorbed (engulf3+qual & that edge in prior 3) ──")
    aeq = qual
    for col in E_COLS:
        c = df[col].astype(float).groupby(df["ticker"])
        inp = (c.shift(1).fillna(0) + c.shift(2).fillna(0)) > 0
        m = aeq & inp
        if int(m.sum()) >= 40:
            print(_line("  " + col.replace("E_", ""), m, df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
