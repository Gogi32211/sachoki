"""
validate_engulf_L.py — the L VSA-line dimension we skipped on the engulf-absorption edge.
Base: ANY bull-T · engulf-2 · ≥$21 · RSI<45 · swallows a fresh Edge signal in prior 2 bars.
Split by l_sig ON the engulf bar, and by the L on the swallowed (prior) bars.
path-sim trail25/60 + TRAIN/TEST + 2022. READ-ONLY.
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


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"    {lbl:16s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"    {lbl:16s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling (edge frame)…", flush=True)
    df, as_of = ER._pull(62, 3_000_000)
    df = ER._prep(df)
    g = df.groupby("ticker", sort=False)
    if "l" not in df.columns:      # edge_replay pulls l_sig as 'l'
        df["l"] = ""
    df["anybt"] = df["t"].str.match(r"^T\d").fillna(False)
    h2 = g["high"].transform(lambda s: s.shift(1).rolling(2).max())
    l2 = g["low"].transform(lambda s: s.shift(1).rolling(2).min())
    df["engR2"] = (df["high"] >= h2) & (df["low"] <= l2)
    ae = df[E_COLS].any(axis=1).astype(float).groupby(df["ticker"])
    df["edge_in2"] = (ae.shift(1).fillna(0) + ae.shift(2).fillna(0)) > 0
    df["pL1"] = g["l"].shift(1).fillna("")
    df["pL2"] = g["l"].shift(2).fillna("")
    core = (df["anybt"] & (df["clean"]) & df["engR2"] & (df["close"] >= 21)
            & (df["rsi_14"] < 45) & df["edge_in2"])
    print(f"as_of {as_of} · engulf-abs base n={int(core.sum())} · trail25/60\n")
    print(_line("BASE (engulf-abs)", core, df))

    print("\n── by L on the ENGULF bar ──")
    for lv in sorted(df.loc[core, "l"].value_counts().index):
        m = core & (df["l"] == lv)
        if int(m.sum()) >= 40:
            print(_line(lv or "(none)", m, df))

    print("\n── by L on the SWALLOWED bars (prior 1-2) ──")
    swL = df["pL1"].where(df["pL1"] != "", df["pL2"])   # nearest non-empty swallowed L
    for lv in sorted(swL[core].value_counts().index):
        m = core & (swL == lv)
        if int(m.sum()) >= 40:
            print(_line("swL=" + (lv or "none"), m, df))

    print("\n── candidate combos ──")
    print(_line("engulf bar L5/L46", core & df["l"].isin(["L5", "L46"]), df))
    print(_line("swallowed L5/L46", core & swL.isin(["L5", "L46"]), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
