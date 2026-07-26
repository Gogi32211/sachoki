"""
validate_t1_gem1_overlap.py — OVERLAP between GEM1 (T1 · prior-Z body>2× · RSI30-50 · volB · ≥$21)
and the LOW-SWEEP T1 (T1 · sweep t-2+t-3 · RSI30-50 · volB · ≥$21). Same STATE gate both sides,
so we isolate the geometry difference (magnitude vs low-sweep). Reports set sizes, intersection %,
Jaccard, and path-sim of GEM1∩SWEEP / GEM1-only / SWEEP-only / union. READ-ONLY.
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


def _line(lbl, m, df):
    n = int(m.sum())
    if n == 0:
        return f"  {lbl:16s} n=0"
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:16s} n={n} (no trades)"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:16s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.1f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    po = g["open"].shift(1); pc = g["close"].shift(1); pz = g["z"].shift(1).fillna("")
    lo2 = g["low"].shift(2); lo3 = g["low"].shift(3)
    isT1 = df["t"] == "T1"; clean = df["supp"] == 0
    state = clean & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B")
    curbody = (df["close"] - df["open"]).abs(); prevbody = (pc - po).abs()
    GEM1 = isT1 & state & (pz != "") & (curbody < 0.5 * prevbody)          # prior-Z body >2× current
    SWEEP = isT1 & state & (df["low"] <= np.minimum(lo2, lo3))             # low-sweep t-2+t-3
    inter = GEM1 & SWEEP
    a, b, i = int(GEM1.sum()), int(SWEEP.sum()), int(inter.sum())
    uni = int((GEM1 | SWEEP).sum())
    print(f"as_of {as_of} · trail25/60 · GEM1 ∩ LOW-SWEEP (same STATE gate)\n")
    print(f"  |GEM1|={a}   |SWEEP|={b}   |∩|={i}   |∪|={uni}")
    print(f"  ∩ as % of GEM1 = {100*i/a:.1f}%   ∩ as % of SWEEP = {100*i/b:.1f}%   Jaccard = {100*i/uni:.1f}%\n")
    print(_line("GEM1 (all)", GEM1, df))
    print(_line("SWEEP (all)", SWEEP, df))
    print(_line("GEM1 ∩ SWEEP", inter, df))
    print(_line("GEM1 only", GEM1 & ~SWEEP, df))
    print(_line("SWEEP only", SWEEP & ~GEM1, df))
    print(_line("UNION", GEM1 | SWEEP, df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
