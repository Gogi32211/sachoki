"""
validate_t1_lowsweep_iso.py — ISOLATION: does the low-sweep ADD over plain T1+STATE, or is
STATE (RSI30-50, vol=B, ≥$21) doing all the work? Compare no-sweep vs sweep-t2 vs sweep-t2+t3
vs deeper, under two STATE bases (full stack, and lighter RSI-only for larger n). Full per-year.
READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


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
                   coalesce(t_sig,'') t,
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
        return f"  {lbl:22s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:22s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    lo2 = g["low"].shift(2); lo3 = g["low"].shift(3); lo4 = g["low"].shift(4)
    isT1 = df["t"] == "T1"; clean = df["supp"] == 0
    sw2 = df["low"] <= lo2
    sw23 = df["low"] <= np.minimum(lo2, lo3)
    sw234 = df["low"] <= np.minimum(np.minimum(lo2, lo3), lo4)
    full = isT1 & clean & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B")
    lite = isT1 & clean & df["rsi_14"].between(30, 50)      # RSI-only STATE, larger n
    print(f"as_of {as_of} · trail25/60 · ISOLATION: does sweep add over T1+STATE?\n")

    print("── FULL STATE (≥$21 · RSI30-50 · volB) ──")
    print(_line("no-sweep (base)", full, df))
    print(_line("+ sweep t-2", full & sw2, df))
    print(_line("+ sweep t-2+t-3", full & sw23, df))
    print(_line("+ sweep t-2+t-3+t-4", full & sw234, df))
    print("\n── LIGHT STATE (RSI30-50 only, larger n) ──")
    print(_line("no-sweep (base)", lite, df))
    print(_line("+ sweep t-2", lite & sw2, df))
    print(_line("+ sweep t-2+t-3", lite & sw23, df))
    print(_line("+ sweep t-2+t-3+t-4", lite & sw234, df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
