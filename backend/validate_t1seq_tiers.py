"""
validate_t1seq_tiers.py — path-sim the T1-seq chart overlay's tiers (any[-2]→any[-1]→T1[0],
classified by the 2 prior bars' Z/T context): ZZ / TZ / ZT / TT. Are the old no-stop Exp
numbers (ZZ +2.26, TZ +2.21) real under stop-first path-sim + per-year + 2022 + OOS?
Uses sig_t1/sig_z/sig_t (the same flags the overlay uses). READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


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
                   coalesce(sig_t1,0) t1, coalesce(sig_z,0) z, coalesce(sig_t,0) t,
                   coalesce(composite_full_suffix,'') sfx,
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
        return f"  {lbl:20s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:20s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    z1 = g["z"].shift(1).fillna(0); t1_ = g["t"].shift(1).fillna(0)
    z2 = g["z"].shift(2).fillna(0); t2 = g["t"].shift(2).fillna(0)
    base = (df["t1"] > 0) & (df["supp"] == 0) & ((z2 > 0) | (t2 > 0)) & ((z1 > 0) | (t1_ > 0))
    df["ZZ"] = base & (z2 > 0) & (z1 > 0)
    df["TZ"] = base & (t2 > 0) & (z1 > 0) & ~df["ZZ"]
    df["ZT"] = base & (z2 > 0) & (t1_ > 0) & ~df["ZZ"] & ~df["TZ"]
    df["TT"] = base & ~df["ZZ"] & ~df["TZ"] & ~df["ZT"]
    print(f"as_of {as_of} · trail25/60 · T1-seq tiers\n")
    print(_line("T1 plain (all)", (df["t1"] > 0) & (df["supp"] == 0), df))
    print(_line("T1-seq base (all tiers)", base, df))
    print("\n── by TIER (2 prior bars' context) ──")
    print(_line("ZZ (T1 tier)⭐", df["ZZ"], df))
    print(_line("TZ (T2 tier)", df["TZ"], df))
    print(_line("ZT (T3 tier)", df["ZT"], df))
    print(_line("TT (T4 tier)", df["TT"], df))
    print("\n── ZZ + refinements (our capitulation-bounce ideas) ──")
    print(_line("ZZ · RSI30-50", df["ZZ"] & df["rsi_14"].between(30, 50), df))
    print(_line("ZZ · suffix EBA/EUR", df["ZZ"] & df["sfx"].str.contains("EBA|EUR", na=False), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
