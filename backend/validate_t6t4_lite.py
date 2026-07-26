"""
validate_t6t4_lite.py — "T6/T4 Lite": a bull bar that RECLAIMS/engulfs the BODY-TOP of the
bar 2 back (t-2). Body-top = open[-2] if t-2 was bearish, close[-2] if bullish (= max(o,c)[-2]).
Lite = the current bull body crosses that level (open ≤ bt2 ≤ close). Tested raw and with the
STATE filters we know matter (RSI30-50, vol=B, ≥$21). path-sim trail25/60 + per-year + 2022 + OOS.
READ-ONLY.
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
        return f"  {lbl:24s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:24s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    g = df.groupby("ticker", sort=False)
    o2 = g["open"].shift(2); c2 = g["close"].shift(2)
    bt2 = np.maximum(o2, c2)                      # body-top of t-2 (open if bear, close if bull)
    bull = df["close"] > df["open"]
    clean = df["supp"] == 0
    df["lite"] = bull & (df["open"] <= bt2) & (df["close"] >= bt2)      # body reclaims t-2 body-top
    df["lite_hi"] = bull & (df["high"] >= bt2) & (df["open"] < bt2)     # looser: wick reclaim
    df["bt2_bear"] = (c2 < o2)                    # was the t-2 bar bearish?
    df["isT"] = df["t"].str.match(r"^T\d").fillna(False)
    print(f"as_of {as_of} · trail25/60 · T6/T4-Lite = bull reclaims t-2 body-top\n")

    print("── raw ──")
    print(_line("any-bull & lite", clean & df["lite"], df))
    print(_line("bull-T & lite", clean & df["isT"] & df["lite"], df))
    print(_line("bull-T & lite (wick)", clean & df["isT"] & df["lite_hi"], df))
    print("\n── t-2 direction split (bull-T & lite) ──")
    print(_line("t-2 BEAR (reclaim open)", clean & df["isT"] & df["lite"] & df["bt2_bear"], df))
    print(_line("t-2 BULL (reclaim close)", clean & df["isT"] & df["lite"] & ~df["bt2_bear"], df))
    print("\n── + STATE filters (bull-T & lite) ──")
    L = clean & df["isT"] & df["lite"]
    print(_line("+ RSI30-50", L & df["rsi_14"].between(30, 50), df))
    print(_line("+ vol=B", L & (df["vb"] == "B"), df))
    print(_line("+ ≥$21", L & (df["close"] >= 21), df))
    print(_line("+ ≥$21·RSI30-50·volB", L & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B"), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
