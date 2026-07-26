"""
validate_t4t6_zones.py — T4 & T6 (1D) sliced by Wyckoff range ZONE:
  SC   = close within ±5% of support (accumulation floor)
  AR   = close within ±5% of resistance (ceiling)
  >AR  = close 0..+5% ABOVE resistance (fresh break)
  mid  = strictly between, in neither band
Path-sim each (trail25/60, gap-realistic) + per-year + '22 + TR/TE. Also +RSI<35 within the
best zone. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
B = 0.05


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


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:26s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def run():
    print("pulling…", flush=True)
    df = _pull()
    clean = df["supp"] == 0
    valid = (df["vtr"] == 1) & (df["res"] > df["sup"]) & (df["sup"] > 0)
    r = (df["close"] / df["sup"].replace(0, np.nan) - 1).abs()
    rr = (df["close"] / df["res"].replace(0, np.nan) - 1).abs()
    sc = (valid & (r <= B)).fillna(False)
    ar = (valid & (rr <= B)).fillna(False)
    above = ((df["res"] > 0) & (df["close"] > df["res"]) & (df["close"] <= df["res"] * (1 + B))).fillna(False)
    mid = (valid & ~sc & ~ar & (df["sup"] < df["close"]) & (df["close"] < df["res"])).fillna(False)
    zones = {"SC": sc, "AR": ar, ">AR": above, "mid": mid}
    for sig in ("T4", "T6"):
        S = (df["t"] == sig) & clean
        print(f"══ {sig} ══")
        print(_line(f"{sig} ungated", S, df))
        for zn, zm in zones.items():
            print(_line(f"{sig} @ {zn}", S & zm, df))
        # best-zone + oversold
        print(_line(f"{sig} @ SC · RSI<40", S & sc & (df["rsi_14"] < 40), df))
        print(_line(f"{sig} @ SC · RSI30-50 · volB", S & sc & df["rsi_14"].between(30, 50) & (df["vb"] == "B"), df))
        print()
    print("done.")


if __name__ == "__main__":
    run()
