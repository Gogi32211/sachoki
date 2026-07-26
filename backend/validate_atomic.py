"""
validate_atomic.py — put the TZ 5-YR research's ATOMIC hypotheses through our rigorous
engine: path-sim (stop-first, trail25, 15bps) + per-year + 2022-survival + universe tier.

The research reports NO-STOP med10 lift (MFE-proxy-ish, in-sample). Atomic components are
STRUCTURAL (definitional, not data-fit rules) so there's no rule-fitting circularity — the
honest test is: does the edge survive realistic execution (stops) AND is it time-robust
(positive most years incl. the 2022 bear)?  We test each atom as a FILTER on bull-T entries
and compare lift vs the bull-T base.

Hypotheses (from ATOMIC_SUFFIX_GAP_DISCOVERIES + research report):
  close=O (+0.30) · EO=escape+weak-close (+0.5-0.6) · gap G2/G3 (+0.4-0.6) · vol=B (best,
  VB worst) · l_sig=L5 (best VSA line) · RSI2 R2L (best, R2X worst) · winning edge =
  bull-T & close=O & gap.  Also: raw signals only work large-cap (SP500) — test the tier.

READ-ONLY on the 1d DB.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats, _BULLT

DVF = 3_000_000
MONTHS = 62   # ~5.2yr → 2022 included


def _pull(months, dv_floor):
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars
                       WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv_floor}
                         AND date >= DATE '{as_of}' - INTERVAL {int(months)*31+40} DAY)
            SELECT universe, ticker, date, open, high, low, close, atr_14,
                   coalesce(t_sig,'') t,
                   coalesce(close_suffix,'') csfx, coalesce(ne_suffix,'') ne,
                   coalesce(wick_suffix,'') wick, coalesce(bar_gap_class,'') gap,
                   coalesce(vol_bucket,'') vb, coalesce(l_sig,'') l, coalesce(rsi2_state,'') r2,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


def _frame(df):
    df["bullt"] = df["t"].isin(_BULLT)
    df["clean"] = df["supp"] == 0
    df["base"] = df["bullt"] & df["clean"]
    b = df["base"]
    # single-atom filters on the bull-T base
    df["A_closeO"] = b & (df["csfx"] == "O")
    df["A_closeA"] = b & (df["csfx"] == "A")            # sanity: research says worst
    df["A_EO"]     = b & (df["ne"] == "E") & (df["csfx"] == "O")
    df["A_gap23"]  = b & df["gap"].isin(("G2", "G3"))
    df["A_gapG3"]  = b & (df["gap"] == "G3")
    df["A_volB"]   = b & (df["vb"] == "B")
    df["A_volVB"]  = b & (df["vb"] == "VB")             # sanity: research says worst
    df["A_L5"]     = b & (df["l"] == "L5")
    df["A_R2L"]    = b & (df["r2"] == "R2L")
    df["A_R2X"]    = b & (df["r2"] == "R2X")            # sanity: research says worst
    df["A_wickD"]  = b & df["wick"].str.contains("D", na=False)
    # stacked "winning edge" + full atomic profile
    df["A_WIN"]    = b & (df["csfx"] == "O") & df["gap"].isin(("G2", "G3"))
    df["A_FULL"]   = df["A_WIN"] & (df["vb"] == "B")
    return df


MASKS = [("base(bull-T)", "base"), ("+close=O", "A_closeO"), ("+close=A✗", "A_closeA"),
         ("+EO", "A_EO"), ("+gapG2/3", "A_gap23"), ("+gapG3", "A_gapG3"),
         ("+vol=B", "A_volB"), ("+vol=VB✗", "A_volVB"), ("+l_sig=L5", "A_L5"),
         ("+RSI2=R2L", "A_R2L"), ("+RSI2=R2X✗", "A_R2X"), ("+wick=D", "A_wickD"),
         ("WIN=O&gap", "A_WIN"), ("FULL=O&gap&B", "A_FULL")]
KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)


def _grp(df):
    return {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}


def _line(label, s, base_mean):
    if not s or s.get("n", 0) == 0:
        return f"  {label:16s} n=0"
    d = s["mean"] - base_mean
    return (f"  {label:16s} n={s['n']:>6} mean{s['mean']:+5.2f} (Δ{d:+5.2f}) med{s['median']:+5.2f} "
            f"win{s['win']:4.1f} pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} "
            f"'22={s['per_year'].get('2022', float('nan')):+5.2f}")


def run(months=MONTHS, dv_floor=DVF):
    print(f"pulling bars ({months}mo, dv≥${dv_floor/1e6:.0f}M)…", flush=True)
    df, as_of = _pull(months, dv_floor)
    df = _frame(df)
    print(f"as_of {as_of} · path-sim trail25 stop-first · bull-T base, each atom as a filter\n")
    for tier, sub in [("ALL universes", df), ("SP500 only", df[df.universe == "sp500"])]:
        grp = _grp(sub)
        base = _stats("base", _pathsim(grp, "base", **KW))
        bm = base.get("mean", 0)
        print(f"══════ {tier}  (base bull-T mean {bm:+.2f}, n={base.get('n',0)}) ══════")
        for label, col in MASKS:
            print(_line(label, _stats(label, _pathsim(grp, col, **KW)), bm))
        print()
    return as_of


if __name__ == "__main__":
    run()
