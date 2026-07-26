"""
validate_sc_super_robust.py — robustness gate for the SC-SUPER setups before building.
Overfit checks on the 5 that qualified (Parabola/P55/Spring/D+L1/T1-CapBounce):
  · SC-BAND plateau: sweep the ±% support band (3/5/7/10%) — is the lift a PLATEAU or a spike?
  · 2× slip stress at the chosen band.
A real gate holds across band widths and survives friction. path-sim trail25/60 gap-realistic.
1d. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats, SETUPS

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]
SUPER = ["Atomic", "Washout", "H1-bottom"]
COL = {n: c for n, c in SETUPS}


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
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        c.close()


def _stat(m, dfbase, slip=None):
    d2 = dfbase.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW, slip=slip))
    if not s or s.get("n", 0) == 0:
        return None
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py])
    return f"n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} '22{py.get('2022', float('nan')):+5.2f}"


def run():
    print("pulling…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)
    vtr = df["vtr"].to_numpy(); sup = df["sup"].to_numpy(float); res = df["res"].to_numpy(float)
    cl = df["close"].to_numpy(float)
    valid = (vtr == 1) & (res > sup) & (sup > 0)
    ratio = np.full(len(df), np.nan)
    ratio[valid] = np.abs(cl[valid] / sup[valid] - 1)   # distance to support (fraction)

    print("SC-band plateau (sweep ±% around support) + 2× slip · trail25/60 gap-realistic\n")
    for name in SUPER:
        col = COL[name]
        print(f"── {name} ──")
        print(f"  ungated        {_stat(df[col], df)}")
        for band in (0.03, 0.05, 0.07, 0.10):
            sc = df[col] & valid & (ratio <= band)
            print(f"  SC ±{int(band*100):>2}%       {_stat(sc, df)}")
        sc5 = df[col] & valid & (ratio <= 0.05)
        print(f"  SC ±5% @2×slip {_stat(sc5, df, slip=0.003)}")
        print()
    print("done.")


if __name__ == "__main__":
    run()
