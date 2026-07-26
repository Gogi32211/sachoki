"""
validate_t4t6.py — is there a tradeable T4 or T6 entry edge? Path-sim T4/T6 raw and under
the STATE filters we know matter (RSI band, vol=B, price zone, prior-Z capitulation, SC-zone,
EMA-dip context), per-year + '22 + TRAIN/TEST. edge_replay engine (trail25/60, gap-realistic).
1d. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
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
        return f"  {lbl:28s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:28s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def run():
    print("pulling…", flush=True)
    df = _pull()
    g = df.groupby("ticker", sort=False)
    clean = df["supp"] == 0
    rsi = df["rsi_14"]
    prevZ = g["z"].shift(1).fillna("") != ""
    body = (df["close"] - df["open"]).abs()
    ratio = body / body.groupby(df["ticker"]).shift(1).replace(0, np.nan)
    bigZ = prevZ & (ratio < 0.5)                         # small bar off a big prior-Z (capitulation)
    sc = ((df["vtr"] == 1) & (df["res"] > df["sup"]) & (df["sup"] > 0)
          & ((df["close"] / df["sup"].replace(0, np.nan) - 1).abs() <= 0.05)).fillna(False)
    ema20 = g["close"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    dip = df["close"] <= ema20                              # buy-the-dip (close at/under EMA20)
    for sig in ("T4", "T6"):
        S = (df["t"] == sig) & clean
        print(f"══ {sig} ══")
        print(_line(f"{sig} raw", S, df))
        print(_line(f"{sig} · RSI30-50", S & rsi.between(30, 50), df))
        print(_line(f"{sig} · RSI<35", S & (rsi < 35), df))
        print(_line(f"{sig} · vol=B", S & (df["vb"] == "B"), df))
        print(_line(f"{sig} · ≥$21", S & (df["close"] >= 21), df))
        print(_line(f"{sig} · prevZ-cap (body<0.5×)", S & bigZ, df))
        print(_line(f"{sig} · EMA20-dip", S & dip, df))
        print(_line(f"{sig} · 🌀SC-zone", S & sc, df))
        print(_line(f"{sig} · $21-89·RSI30-50·volB", S & df["close"].between(21, 89) & rsi.between(30, 50) & (df["vb"] == "B"), df))
        print(_line(f"{sig} · dip·RSI30-50·volB", S & dip & rsi.between(30, 50) & (df["vb"] == "B"), df))
        print()
    print("done.")


if __name__ == "__main__":
    run()
