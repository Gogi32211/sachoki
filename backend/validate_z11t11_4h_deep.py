"""
validate_z11t11_4h_deep.py — DEEP dive on Z11-T11 @ 4h (the setup that flipped era-heavy→
era-independent moving 1d→4h). Is it robust or a resample artifact?
Tests: per-year n+return (is 6/6yr real or thin-n?), anchor split (Z11/Z3/Z1G/Z5),
RSI2 band sensitivity, horizon plateau, 2× slip stress, dv-floor sensitivity (was the
all-tf run dv-unfiltered?). 4h vs 1d side by side. gap-realistic, stop-first. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _prep, _pathsim, _stats

YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
_ANCH = ER._ANCH; _CONF = ER._CONF; _RES = ER._RES


def _pull(tf, dv_floor):
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if tf == "1d" else db_path(f"studio_{tf}.duckdb")
    c = duckdb.connect(p, read_only=True)
    dv = f"AND close*volume>={dv_floor}" if dv_floor else ""
    try:
        df = c.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 {dv})
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        c.close()


def _mk(df):
    """rebuild Z11-T11 components + the anchor for slicing (mirrors _prep)."""
    g = df.groupby("ticker", sort=False)
    df["clean"] = df["supp"] == 0
    df["nonvb"] = df["vb"] != "VB"
    df["z2"] = g["z"].shift(2); df["rsi2"] = g["rsi_14"].shift(2); df["t1"] = g["t"].shift(1)
    base = df["clean"] & df["nonvb"]
    df["Z11T11"] = (base & df["z2"].isin(_ANCH) & df["rsi2"].between(30, 45)
                    & df["t1"].isin(_CONF) & df["t"].isin(_RES))
    return df


def _line(lbl, m, df, maxh, slip=None):
    kw = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=maxh)
    d2 = df.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **kw, slip=slip))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:26s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def _nyr(df, m):
    d = df[m.values]; return {y: int((d["date"].str[:4] == y).sum()) for y in YRS}


def run():
    print("Z11-T11 DEEP · 4h vs 1d · gap-realistic\n")
    # 4h no-dv (matches the all-tf run) and 4h dv≥1M (realistic intraday liquidity)
    for tf, dv, maxh in [("1d", 3_000_000, 60), ("4h", 0, 120), ("4h", 1_000_000, 120)]:
        df = _mk(_pull(tf, dv))
        m = df["Z11T11"]
        tag = f"{tf} dv{'0' if not dv else int(dv/1e6)}M maxh{maxh}"
        print(f"── {tag} ──")
        print(_line("Z11-T11 all", m, df, maxh))
        ny = _nyr(df, m)
        print(f"    n/yr: {ny}")
        # anchor split
        for a in ["Z11", "Z3", "Z1G", "Z5"]:
            print(_line(f"  anchor={a}", m & (df["z2"] == a), df, maxh))
        print()
    # on the 4h dv1M frame: RSI2 band + horizon plateau + slip stress
    df = _mk(_pull("4h", 1_000_000)); m = df["Z11T11"]
    print("── 4h dv1M · RSI2 band sensitivity (maxh120) ──")
    for lo, hi in [(25, 40), (30, 45), (35, 50), (30, 50)]:
        print(_line(f"RSI2 {lo}-{hi}", df["clean"] & df["nonvb"] & df["z2"].isin(_ANCH)
                    & df["rsi2"].between(lo, hi) & df["t1"].isin(_CONF) & df["t"].isin(_RES), df, 120))
    print("\n── 4h dv1M · horizon plateau ──")
    for h in (40, 80, 120, 160):
        print(_line(f"maxh {h} (~{h//2}d)", m, df, h))
    print("\n── 4h dv1M · 2× slip stress ──")
    print(_line("Z11-T11 @2×slip", m, df, 120, slip=0.003))
    print("\ndone.")


if __name__ == "__main__":
    run()
