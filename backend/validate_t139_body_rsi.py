"""
validate_t139_body_rsi.py — T1 / T3 / T9 studied by BODY SIZE (body/ATR terciles: small
vs large) and by RSI band. Focus: does a LARGE-body T9 (decisive bull inside-ish bar) beat
a SMALL-body one? And how do T1/T3/T9 each respond to RSI? path-sim trail25/60, 62mo. READ-ONLY.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
RSIB = [(0, 30, "RSI<30"), (30, 40, "RSI30-40"), (40, 50, "RSI40-50"),
        (50, 60, "RSI50-60"), (60, 200, "RSI60+")]


def _pull():
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL {62*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, rsi_14, atr_14,
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
        return f"    {lbl:14s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"    {lbl:14s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = _pull()
    df["bodyATR"] = (df["close"] - df["open"]).abs() / df["atr_14"].replace(0, np.nan)
    clean = df["supp"] == 0
    print(f"as_of {as_of} · trail25/60 · body=|C-O|/ATR\n")

    for sig in ("T1", "T3", "T9"):
        m0 = (df["t"] == sig) & clean
        print(f"════════ {sig}  (base) ════════")
        print(_line("base", m0, df))
        # body-size terciles within this signal
        ba = df.loc[m0, "bodyATR"]
        q1, q2 = ba.quantile(0.33), ba.quantile(0.67)
        print(f"  ── by BODY (small<{q1:.2f} / large>{q2:.2f} ×ATR) ──")
        print(_line("small body", m0 & (df["bodyATR"] <= q1), df))
        print(_line("mid body", m0 & (df["bodyATR"] > q1) & (df["bodyATR"] <= q2), df))
        print(_line("LARGE body", m0 & (df["bodyATR"] > q2), df))
        print("  ── by RSI ──")
        for lo, hi, lbl in RSIB:
            print(_line(lbl, m0 & df["rsi_14"].between(lo, hi), df))
        print()
    print(f"as_of {as_of}")


if __name__ == "__main__":
    run()
