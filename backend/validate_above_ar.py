"""
validate_above_ar.py — Edge signals firing JUST ABOVE the range resistance (0 to +5%):
close is between wt_resistance and wt_resistance×1.05 = a fresh break above the ceiling.
(Distinct from the earlier ±5% AR band which included below-resistance.) path-sim each Edge
setup ungated vs @above-AR, plus a plain bull-T baseline. trail25/60 gap-realistic. 1d.
READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats, SETUPS

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
BAND = 0.05


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    c = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return c.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        c.close()


def _line(lbl, m, df):
    d2 = df.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:22s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:22s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022', float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)
    res = df["res"].to_numpy(float); cl = df["close"].to_numpy(float)
    # 0 to +5% ABOVE resistance = fresh break above the ceiling
    above = (res > 0) & (cl > res) & (cl <= res * (1 + BAND))
    df["above_ar"] = above
    _bullT = df["t"].str.match(r"^T\d").fillna(False)
    print(f"1d · above-AR = close in (resistance, resistance×1.05] · in-zone bars {int(above.sum()):,} "
          f"· trail25/60 gap-realistic\n")

    print("── pure entries just above the broken ceiling ──")
    print(_line("any bull-T @ >AR", df["above_ar"] & _bullT & (df["supp"] == 0), df))
    print(_line("bull-T @ >AR · RSI<50", df["above_ar"] & _bullT & (df["supp"] == 0) & (df["rsi_14"] < 50), df))
    print(_line("bull-T @ >AR · vol=B", df["above_ar"] & _bullT & (df["supp"] == 0) & (df["vb"] == "B"), df))
    print("\n── EDGE setups: ungated vs @ >AR (0..+5% above resistance) ──")
    for name, col in SETUPS:
        m_a = df[col] & df["above_ar"]
        if int(m_a.sum()) < 25:
            continue
        print(_line(f"{name}", df[col], df))
        print(_line(f"{name} @ >AR", m_a, df))
        print()
    print("done.")


if __name__ == "__main__":
    run()
