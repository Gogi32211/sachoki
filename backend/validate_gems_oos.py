"""
validate_gems_oos.py — full OOS/robustness for the two gems (both STRUCTURAL, so the
TRAIN/TEST split IS the walk-forward — no rules fit).
  GEM1: T1 · prior-Z body >2× T1 body (capitulation) · RSI30-50 · vol=B
  GEM2: engulf-abs (ANY-T·engulf2·≥$21·RSI<45·+edge in prior2) · swallowed bar L46
Reports per-YEAR and per-UNIVERSE for each. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
YEARS = ["2021", "2022", "2023", "2024", "2025", "2026"]
E_COLS = ["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
          "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR"]


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _report(name, mask, df):
    s = _stats("x", _pathsim(_grp(df, mask), "_m", **KW))
    print(f"  {name}: n={s.get('n',0)} mean{s.get('mean',0):+.2f} med{s.get('median',0):+.2f} "
          f"win{s.get('win',0):.1f} pf{s.get('pf')}")
    py = s.get("per_year", {})
    tr = np.mean([py[y] for y in YEARS[:3] if y in py]); te = np.mean([py[y] for y in YEARS[3:] if y in py])
    print("     per-year: " + " ".join(f"{y[2:]}:{py.get(y,float('nan')):+.1f}" for y in YEARS) + f"  | TRAIN{tr:+.2f} TEST{te:+.2f}")
    for u in ("sp500", "nasdaq", "russell2k"):
        su = _stats("x", _pathsim(_grp(df, mask & (df["universe"] == u)), "_m", **KW))
        print(f"     {u:10s} n={su.get('n',0):>4} mean{su.get('mean',0):+.2f} med{su.get('median',0):+.2f} pf{su.get('pf')}")


def run():
    print("pulling…", flush=True)
    df, as_of = ER._pull(62, 3_000_000)
    df = ER._prep(df)
    g = df.groupby("ticker", sort=False)
    body = (df["close"] - df["open"]).abs()
    df["ratio"] = body / body.groupby(df["ticker"]).shift(1).replace(0, np.nan)
    df["prevZ"] = g["z"].shift(1).fillna("") != ""
    # GEM1
    gem1 = ((df["t"] == "T1") & df["clean"] & df["prevZ"] & (df["ratio"] < 0.5)
            & df["rsi_14"].between(30, 50) & (df["vb"] == "B"))
    # GEM2
    h2 = g["high"].transform(lambda s: s.shift(1).rolling(2).max())
    l2 = g["low"].transform(lambda s: s.shift(1).rolling(2).min())
    engR2 = (df["high"] >= h2) & (df["low"] <= l2)
    ae = df[E_COLS].any(axis=1).astype(float).groupby(df["ticker"])
    edge_in2 = (ae.shift(1).fillna(0) + ae.shift(2).fillna(0)) > 0
    swL = g["l"].shift(1).fillna("").where(g["l"].shift(1).fillna("") != "", g["l"].shift(2).fillna(""))
    gem2 = (df["t"].str.match(r"^T\d").fillna(False) & df["clean"] & engR2 & (df["close"] >= 21)
            & (df["rsi_14"] < 45) & edge_in2 & (swL == "L46"))

    print(f"as_of {as_of} · trail25/60\n")
    print("════ GEM1 — T1·Z≫T·RSI30-50·vol=B ════")
    _report("all", gem1, df)
    print("\n════ GEM2 — engulf-abs · swallowed L46 ════")
    _report("all", gem2, df)
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
