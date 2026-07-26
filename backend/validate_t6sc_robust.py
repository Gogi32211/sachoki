"""
validate_t6sc_robust.py — robustness gate for T6 @ SC-zone · RSI<40 before building.
SC-band plateau (±3/5/7/10% support), RSI-threshold plateau (<35/<40/<45/<50), 2× slip.
A real signal holds across band widths + RSI thresholds and survives friction. 1D. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   coalesce(t_sig,'') t, coalesce(vol_bucket,'') vb,
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()


def _stat(m, dfbase, slip=None):
    d2 = dfbase.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW, slip=slip))
    if not s or s.get("n", 0) == 0:
        return "n=0"
    py = s["per_year"]; tr = np.mean([py[y] for y in TR if y in py])
    return (f"n={s['n']:>4} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} '22{py.get('2022', float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df = _pull()
    T6 = (df["t"] == "T6") & (df["supp"] == 0)
    valid = (df["vtr"] == 1) & (df["res"] > df["sup"]) & (df["sup"] > 0)
    dist = (df["close"] / df["sup"].replace(0, np.nan) - 1).abs()
    rsi = df["rsi_14"]

    print("── SC-band plateau (RSI<40 fixed) ──")
    print(f"  ungated (T6·RSI<40)   {_stat(T6 & (rsi < 40), df)}")
    for b in (0.03, 0.05, 0.07, 0.10):
        print(f"  SC ±{int(b*100):>2}% · RSI<40    {_stat(T6 & valid & (dist <= b) & (rsi < 40), df)}")
    print("\n── RSI-threshold plateau (SC ±5% fixed) ──")
    sc5 = valid & (dist <= 0.05)
    for th in (35, 40, 45, 50):
        print(f"  SC ±5% · RSI<{th}        {_stat(T6 & sc5 & (rsi < th), df)}")
    print("\n── 2× slip stress ──")
    print(f"  SC ±5% · RSI<40 @2×slip {_stat(T6 & sc5 & (rsi < 40), df, slip=0.003)}")
    print("\ndone.")


if __name__ == "__main__":
    run()
