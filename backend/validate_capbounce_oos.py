"""Unified OOS for the capitulation-bounce cells: each signal's best directional cell
+ RSI30-50 + vol=B, TRAIN(2021-23) vs TEST(2024-26) + per-year. Which survive OOS?"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
CELLS = [("T1", "Z"), ("T3", "T"), ("T9", "Z"), ("T4", "T"), ("T6", "T"), ("ANY", "Z")]


def _pull():
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL {62*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14, coalesce(vol_bucket,'') vb,
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


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    body = (df["close"] - df["open"]).abs()
    df["ratio"] = body / body.groupby(df["ticker"]).shift(1).replace(0, np.nan)
    df["prevZ"] = g["z"].shift(1).fillna("") != ""
    clean = df["supp"] == 0
    print(f"as_of {as_of} · dir · RSI30-50 · vol=B · TRAIN vs TEST\n")
    print(f"  {'cell':16s}{'n':>5}  {'mean':>6}{'med':>6}{'win':>6}{'pf':>5}  {'TRAIN':>6}{'TEST':>6}  per-year")
    for sig, d in CELLS:
        sm = df["t"].str.match(r"^T\d").fillna(False) if sig == "ANY" else (df["t"] == sig)
        dmask = (df["ratio"] < 0.5) if d == "Z" else (df["ratio"] > 2.0)
        m = sm & clean & df["prevZ"] & dmask & df["rsi_14"].between(30, 50) & (df["vb"] == "B")
        s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
        if not s or s.get("n", 0) == 0:
            print(f"  {sig+'·'+('Z≫T' if d=='Z' else 'T≫Z'):16s}n=0"); continue
        py = s["per_year"]
        tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
        pys = " ".join(f"{y[2:]}:{py.get(y,float('nan')):+.1f}" for y in TR + TE)
        vv = "✅" if (tr > 1 and te > 1 and py.get("2022", -9) > 0) else ("⚠" if te > 1 else "❌")
        print(f"  {sig+'·'+('Z≫T' if d=='Z' else 'T≫Z'):16s}{s['n']:>5}  {s['mean']:>+6.2f}{s['median']:>+6.2f}{s['win']:>6.1f}{str(s['pf']):>5}  {tr:>+6.2f}{te:>+6.2f}  {pys} {vv}")
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
