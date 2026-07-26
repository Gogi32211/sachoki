"""
validate_buyscore_pathsim.py — STAGE 3: path-sim tradability of the candidate FINAL BUY
cells found in stages 1-2 (RSI backbone + prebreak_v2 booster). trail25/60, stop-first,
gap-realistic fills, 15bps + 2× stress. Per-year + TR/TE + '22. READ-ONLY.
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
    a = duckdb.connect("/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb",
                       read_only=True)
    try:
        df = a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   prebreak_v2 pbv2, turbo_score, coalesce(vol_bucket,'') vb,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        a.close()


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df, slip=None):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW, slip=slip))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:34s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
    return (f"  {lbl:34s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")


def run():
    print("pulling…", flush=True)
    df = _pull()
    clean = df["supp"] == 0
    rsi = df["rsi_14"]; pb = df["pbv2"]
    hiPB = pb >= 19          # top tercile/D8 threshold from stage 1-2
    print("trail25/60 · gap-realistic · dv≥3M\n")
    print("── the ladder: RSI backbone → +pbv2 booster → +quality gates ──")
    print(_line("ALL clean (baseline)", clean, df))
    print(_line("RSI<35", clean & (rsi < 35), df))
    print(_line("RSI<35 & pbv2≥19", clean & (rsi < 35) & hiPB, df))
    print(_line("RSI 35-50 & pbv2≥19", clean & rsi.between(35, 50) & hiPB, df))
    print(_line("RSI<35 & pbv2≥19 & $21-89", clean & (rsi < 35) & hiPB & df["close"].between(21, 89), df))
    print(_line("RSI<35 & pbv2≥19 & volB", clean & (rsi < 35) & hiPB & (df["vb"] == "B"), df))
    print("\n── plateaus (pbv2 threshold sweep, RSI<35) ──")
    for th in (13, 16, 19, 22, 25):
        print(_line(f"RSI<35 & pbv2≥{th}", clean & (rsi < 35) & (pb >= th), df))
    print("\n── VETO check: does high-RSI stay bad even with high pbv2? ──")
    print(_line("RSI 60+ & pbv2≥19", clean & (rsi >= 60) & hiPB, df))
    print(_line("RSI 60+ & turbo top (≥44)", clean & (rsi >= 60) & (df["turbo_score"] >= 44), df))
    print("\n── 2× slip stress on the winner cell ──")
    print(_line("RSI<35 & pbv2≥19 @2×slip", clean & (rsi < 35) & hiPB, df, slip=0.003))
    print("\ndone.")


if __name__ == "__main__":
    run()
