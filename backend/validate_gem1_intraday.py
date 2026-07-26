"""
validate_gem1_intraday.py — do the DAILY gems translate to INTRADAY (4h / 1h)?
Tests, on a tf DB (studio_<tf>.duckdb): base T1+STATE, GEM1 (T1·prior-Z body>2×·RSI30-50·volB),
SWEEP (T1·sweep t-2+t-3 lows), GEM1∩SWEEP. path-sim trail25, tf-appropriate horizons.
Usage:  python validate_gem1_intraday.py 4h        (1h only after its derive finishes/unlocks)
READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

TF = sys.argv[1] if len(sys.argv) > 1 else "4h"
# horizons in BARS (4h ≈ 2 bars/day, 1h ≈ 7 bars/day). Test a short + medium swing hold.
HORIZONS = {"4h": [10, 30], "1h": [14, 42], "30m": [26, 78]}.get(TF, [10, 30])
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import db_path
    p = db_path(f"studio_{TF}.duckdb")
    c = duckdb.connect(p, read_only=True)
    try:
        as_of = str(c.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = c.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   coalesce(vol_bucket,'') vb, coalesce(t_sig,'') t, coalesce(z_sig,'') z,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of, p
    finally:
        c.close()


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df, maxh):
    kw = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=maxh)
    s = _stats("x", _pathsim(_grp(df, m), "_m", **kw))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:18s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:18s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def run():
    print(f"pulling {TF}…", flush=True)
    df, as_of, p = _pull()
    g = df.groupby("ticker", sort=False)
    po = g["open"].shift(1); pc = g["close"].shift(1); pz = g["z"].shift(1).fillna("")
    lo2 = g["low"].shift(2); lo3 = g["low"].shift(3)
    isT1 = df["t"] == "T1"; clean = df["supp"] == 0
    state = clean & (df["close"] >= 21) & df["rsi_14"].between(30, 50) & (df["vb"] == "B")
    curbody = (df["close"] - df["open"]).abs(); prevbody = (pc - po).abs()
    BASE = isT1 & state
    GEM1 = BASE & (pz != "") & (curbody < 0.5 * prevbody)
    SWEEP = BASE & (df["low"] <= np.minimum(lo2, lo3))
    print(f"{TF} · {p.split('/')[-1]} · as_of {as_of} · rows={len(df):,} · trail25/varH\n")
    for H in HORIZONS:
        print(f"── horizon {H} bars (≈{H/({'4h':2,'1h':7,'30m':13}.get(TF,2)):.0f} trading days) ──")
        print(_line("T1 plain", isT1 & clean, df, H))
        print(_line("T1+STATE (base)", BASE, df, H))
        print(_line("GEM1", GEM1, df, H))
        print(_line("SWEEP", SWEEP, df, H))
        print(_line("GEM1 ∩ SWEEP", GEM1 & SWEEP, df, H))
        print()
    print(f"as_of {as_of} · tf {TF}")


if __name__ == "__main__":
    run()
