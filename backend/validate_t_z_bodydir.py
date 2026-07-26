"""
validate_t_z_bodydir.py — DIRECTIONAL body comparison (the symmetric band hid this):
current T signal (T1/T3/T9) with a prior-bar Z, split by whether the T body DOMINATES the
prior Z body (ratio = T_body / Z_body > 1) vs the reverse (Z body > T body, ratio < 1).
Hypothesis: T≫Z = bull overwhelms the bear (strong reversal); Z≫T = weak bounce.
path-sim trail25/60 + per-year + 2022. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
# ratio = T_body / Z_body  ·  bands from Z-dominant (low) to T-dominant (high)
BANDS = [(0.0, 0.5, "Z≫T  (<0.5)"), (0.5, 0.8, "Z>T  (0.5-0.8)"),
         (0.8, 1.25, "T≈Z  (0.8-1.25)"), (1.25, 2.0, "T>Z  (1.25-2)"),
         (2.0, 1e9, "T≫Z  (>2)")]


def _pull():
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL {62*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"    {lbl:18s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"    {lbl:18s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} "
            f"win{s['win']:4.1f} pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    body = (df["close"] - df["open"]).abs()
    pbody = body.groupby(df["ticker"]).shift(1)
    df["ratio"] = body / pbody.replace(0, np.nan)     # T_body / prior_body
    df["prevZ"] = g["z"].shift(1).fillna("") != ""
    clean = df["supp"] == 0
    print(f"as_of {as_of} · trail25/60 · ratio = T_body / prior-Z_body\n")

    for sig, m0 in [("T4", df["t"] == "T4"), ("T6", df["t"] == "T6"),
                    ("ANY bull-T", df["t"].str.match(r"^T\d").fillna(False))]:
        base = m0 & clean & df["prevZ"]
        print(f"════════ {sig}  (prevZ, n={int(base.sum())}) ════════")
        for lo, hi, lbl in BANDS:
            print(_line(lbl, base & df["ratio"].between(lo, hi, inclusive="left"), df))
        print()
    print(f"as_of {as_of}")


if __name__ == "__main__":
    run()
