"""
validate_t1_lowsweep.py — T1 signal that ENGULFS THE LOW (minimum) of the bar 2 back (t-2),
skipping t-1 (engulfing t-1 = T4, already exists). i.e. current low sweeps below low[-2]
(a reach-back stop-run/spring). "2 bars" variant = low ≤ min(low[-2], low[-3]) (sweeps
t-2 AND t-3 lows). Reclaim = bull close. Tested raw + reclaim + STATE filters
(RSI30-50, vol=B, ≥$21). path-sim trail25/60 + per-year + 2022 + OOS. READ-ONLY.
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
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:26s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    lo2 = g["low"].shift(2); lo3 = g["low"].shift(3)
    isT1 = df["t"] == "T1"
    clean = df["supp"] == 0
    bull = df["close"] > df["open"]
    df["sw2"] = df["low"] <= lo2                        # sweeps t-2 low (skips t-1)
    df["sw_both"] = df["low"] <= np.minimum(lo2, lo3)   # sweeps t-2 AND t-3 lows
    df["sw2_recl"] = df["sw2"] & (df["close"] > lo2)    # swept then closed back above t-2 low
    print(f"as_of {as_of} · trail25/60 · T1 low-sweep of t-2 (skip t-1) / t-2+t-3\n")

    print("── baseline ──")
    print(_line("T1 plain", isT1 & clean, df))
    print("\n── raw low-sweep ──")
    print(_line("T1 & sweep t-2 low", isT1 & clean & df["sw2"], df))
    print(_line("T1 & sweep t-2 & reclaim", isT1 & clean & df["sw2_recl"], df))
    print(_line("T1 & sweep t-2 & bull", isT1 & clean & df["sw2"] & bull, df))
    print(_line("T1 & sweep t2+t3 lows", isT1 & clean & df["sw_both"], df))
    print(_line("T1 & sweep t2+t3 & bull", isT1 & clean & df["sw_both"] & bull, df))
    print("\n── + STATE (T1 & sweep t-2 & reclaim) ──")
    B = isT1 & clean & df["sw2_recl"]
    print(_line("+ RSI30-50", B & df["rsi_14"].between(30, 50), df))
    print(_line("+ vol=B", B & (df["vb"] == "B"), df))
    print(_line("+ ≥$21", B & (df["close"] >= 21), df))
    print(_line("+ ≥$21·RSI30-50·volB", B & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B"), df))
    print("\n── + STATE (T1 & sweep t2+t3 & bull) ──")
    C = isT1 & clean & df["sw_both"] & bull
    print(_line("+ RSI30-50", C & df["rsi_14"].between(30, 50), df))
    print(_line("+ ≥$21·RSI30-50·volB", C & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B"), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
