"""
validate_engulf_edge_depth.py — deep dive: does a T that engulfs 1/2/3 bars AND swallows
an EDGE signal within them beat one that doesn't? Analyzed SEPARATELY for ANY-T / T6 / T4,
at each engulf depth (is 1 or 2 bars already enough?). edge-in-window depth MATCHES the
engulf depth. path-sim trail25/60 + TRAIN/TEST + 2022. READ-ONLY.
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
        return f"    {lbl:18s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"    {lbl:18s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = ER._pull(62, 3_000_000)
    df = ER._prep(df)
    g = df.groupby("ticker", sort=False)
    df["anybt"] = df["t"].str.match(r"^T\d").fillna(False)
    ae = df[E_COLS].any(axis=1).astype(float)
    gae = ae.groupby(df["ticker"])
    for n in (1, 2, 3):
        hmax = g["high"].transform(lambda s, n=n: s.shift(1).rolling(n).max())
        lmin = g["low"].transform(lambda s, n=n: s.shift(1).rolling(n).min())
        df[f"engR{n}"] = (df["high"] >= hmax) & (df["low"] <= lmin)
        # edge fired within the N swept bars (t-1..t-N)
        acc = sum(gae.shift(k).fillna(0) for k in range(1, n + 1))
        df[f"edgeIn{n}"] = acc > 0
    df["clean_"] = df["clean"]
    print(f"as_of {as_of} · trail25/60 · engulf-depth × edge-in-window (matched)\n")

    sigs = [("ANY-T", df["anybt"]), ("T6", df["t"] == "T6"), ("T4", df["t"] == "T4")]
    for sname, sm in sigs:
        print(f"════════ {sname} ════════")
        for n in (1, 2, 3):
            b = sm & df["clean_"] & df[f"engR{n}"]
            print(f"  engulf-{n}:")
            print(_line(f"+edge-in-{n} ✓", b & df[f"edgeIn{n}"], df))
            print(_line("NO edge", b & ~df[f"edgeIn{n}"], df))
        print()

    # the premium: ANY-T × depth × edge, WITH the winning quality filter (≥$21, RSI<45)
    print("════════ ANY-T + quality (≥$21 · RSI<45) × depth × edge ════════")
    qual = df["anybt"] & df["clean_"] & (df["close"] >= 21) & (df["rsi_14"] < 45)
    for n in (1, 2, 3):
        b = qual & df[f"engR{n}"]
        print(f"  engulf-{n}:")
        print(_line(f"+edge-in-{n} ✓", b & df[f"edgeIn{n}"], df))
        print(_line("NO edge", b & ~df[f"edgeIn{n}"], df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
