"""
validate_buyscore_weights.py — STAGE 4: weight-plateau test for the new BUY score
    BUY = w_pb×prebreak_v2 + w_rsi×max(0, 55−RSI) + volB_bonus×(vol=B)
    VETO: RSI≥60 → cap 15
Backtest-expert: the weights must sit on a PLATEAU (stable decile-monotonicity and
top-band edge across the grid), not a narrow peak. Vectorized fwd-20d for the 3×3 grid,
then a path-sim band ladder (trail25/60, gap-realistic) for the CENTRAL variant.
READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ("2021", "2022", "2023"); TE = ("2024", "2025", "2026")
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    a = duckdb.connect("/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb",
                       read_only=True)
    try:
        df = a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   prebreak_v2 pbv2, coalesce(vol_bucket,'') vb,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        a.close()


def buy_score(df, w_pb, w_rsi, vb_bonus, veto_cap=15):
    s = (w_pb * df["pbv2"].fillna(0)
         + w_rsi * np.maximum(0, 55 - df["rsi_14"].fillna(50))
         + vb_bonus * (df["vb"] == "B").astype(float))
    s = s.clip(0, 100)
    return np.where(df["rsi_14"] >= 60, np.minimum(s, veto_cap), s)


def run():
    print("pulling…", flush=True)
    df = _pull()
    g = df.groupby("ticker", sort=False)
    df["fwd"] = (g["close"].shift(-21) / g["open"].shift(-1) - 1) * 100
    df["yr"] = df["date"].str[:4]
    base = df[(df["supp"] == 0) & df["fwd"].notna() & (df["fwd"].abs() < 300)].copy()
    print(f"clean rows {len(base):,}\n")

    print("── 3×3 weight grid (volB=12): decile-monotonicity r · top-decile mean/med/'22 · D9−D0 spread ──")
    for w_pb in (1.0, 1.5, 2.0):
        for w_rsi in (0.6, 0.9, 1.2):
            base["_s"] = buy_score(base, w_pb, w_rsi, 12)
            d = pd.qcut(base["_s"].rank(method="first"), 10, labels=False)
            m = base.groupby(d)["fwd"].mean()
            r = np.corrcoef(np.arange(10), m.values)[0, 1]
            top = base[d == 9]
            y22 = top.loc[top["yr"] == "2022", "fwd"].mean()
            print(f"  w_pb={w_pb:.1f} w_rsi={w_rsi:.1f}  mono r={r:+.2f}  "
                  f"D9: mean{top['fwd'].mean():+5.2f} med{top['fwd'].median():+5.2f} '22{y22:+5.2f}  "
                  f"spread{m.iloc[9]-m.iloc[0]:+5.2f}pp")
    print("\n── volB bonus sweep (central w_pb=1.5, w_rsi=0.9) ──")
    for vb in (0, 8, 12, 16):
        base["_s"] = buy_score(base, 1.5, 0.9, vb)
        d = pd.qcut(base["_s"].rank(method="first"), 10, labels=False)
        top = base[d == 9]
        y22 = top.loc[top["yr"] == "2022", "fwd"].mean()
        print(f"  volB+{vb:<3d} D9: mean{top['fwd'].mean():+5.2f} med{top['fwd'].median():+5.2f} '22{y22:+5.2f}")

    print("\n── PATH-SIM band ladder · central variant (1.5 / 0.9 / 12, veto-cap 15) ──")
    df["_s"] = buy_score(df, 1.5, 0.9, 12)
    clean = df["supp"] == 0
    bands = [("0-20 (incl vetoed)", 0, 20), ("20-40", 20, 40), ("40-60", 40, 60),
             ("60-80", 60, 80), ("80+", 80, 101)]
    def _line(lbl, m):
        d2 = df.copy(); d2["_m"] = m.values
        grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
        s = _stats("x", _pathsim(grp, "_m", **KW))
        if not s or s.get("n", 0) == 0:
            return f"  {lbl:20s} n=0"
        py = s["per_year"]
        tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        return (f"  {lbl:20s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
                f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")
    for lbl, lo, hi in bands:
        print(_line(lbl, clean & (df["_s"] >= lo) & (df["_s"] < hi)))
    print("\ndone.")


if __name__ == "__main__":
    run()
