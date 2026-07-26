"""
validate_seq_t1t2g.py — DEEP research on the 3-consecutive-bar T-sequences
  A = T1 → T2G      B = T1G → T2G
entry after the last T2 (next open). Full-descriptor discipline: slice the ENTRY bar by
RSI / vol / L / price / gap / body — report BOTH rescuers AND suppressors, per-year + '22 +
TRAIN/TEST, flag small-n. path-sim trail25/60 stop-first. READ-ONLY.
"""
import os, sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = a.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14,
                   coalesce(vol_bucket,'') vb, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
                   coalesce(composite_full_suffix,'') sfx, coalesce(bar_gap_class,'') gap,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        a.close()


GRP = None
def _line(lbl, col):
    s = _stats("x", _pathsim(GRP, col, **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:26s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    flag = " ⚠sm" if s["n"] < 120 else ""
    return (f"  {lbl:26s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.1f}{flag}")


def run():
    global GRP
    print("pulling…", flush=True)
    df = _pull()
    g = df.groupby("ticker", sort=False)
    p1 = g["t"].shift(1); p2 = g["t"].shift(2)
    clean = df["supp"] == 0
    bull = df["close"] > df["open"]
    A = (p1 == "T1") & (df["t"] == "T2G")     # T1 → T2G (2-bar, the ask)
    B = (p1 == "T1G") & (df["t"] == "T2G")    # T1G → T2G (sibling)
    rsi = df["rsi_14"]
    # precompute every mask as a column (group once, reuse)
    M = {
        "A": A, "B": B, "Ac": A & clean, "Bc": B & clean,
        "T2_plain": df["t"] == "T2G",
        # descriptor slices on the ENTRY bar, applied to A clean (T1-T2G)
    }
    fam = A & clean
    M["FAM"] = fam
    M["FAM_rsi_lt30"] = fam & (rsi < 30)
    M["FAM_rsi_30_40"] = fam & rsi.between(30, 40)
    M["FAM_rsi_40_50"] = fam & rsi.between(40, 50)
    M["FAM_rsi_50_60"] = fam & rsi.between(50, 60)
    M["FAM_rsi_ge60"] = fam & (rsi >= 60)
    M["FAM_volB"] = fam & (df["vb"] == "B")
    M["FAM_volVB"] = fam & (df["vb"] == "VB")
    M["FAM_L3"] = fam & (df["l"] == "L3")
    M["FAM_L12"] = fam & (df["l"] == "L12")
    M["FAM_L34"] = fam & (df["l"] == "L34")
    M["FAM_p_8_21"] = fam & df["close"].between(8, 21)
    M["FAM_p_21_89"] = fam & df["close"].between(21, 89)
    M["FAM_p_ge89"] = fam & (df["close"] >= 89)
    M["FAM_bull"] = fam & bull
    M["FAM_bear"] = fam & ~bull
    M["FAM_G1"] = fam & (df["gap"] == "G1")
    M["FAM_G2"] = fam & (df["gap"] == "G2")
    M["FAM_G3"] = fam & (df["gap"] == "G3")
    # best-guess stack
    M["FAM_21_89_rsi3050"] = fam & df["close"].between(21, 89) & rsi.between(30, 50)
    M["FAM_21_89_rsi3050_volB"] = fam & df["close"].between(21, 89) & rsi.between(30, 50) & (df["vb"] == "B")
    for k, v in M.items():
        df[k] = v.values
    GRP = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}

    print("trail25/60 · entry after T2G · dv≥3M\n")
    print("── baselines ──")
    print(_line("T2G plain (any)", "T2_plain"))
    print("\n── the sequences ──")
    print(_line("A: T1-T2G (raw)", "A"))
    print(_line("A: T1-T2G (clean)", "Ac"))
    print(_line("B: T1G-T2G (raw)", "B"))
    print(_line("B: T1G-T2G (clean)", "Bc"))
    print("\n── A = T1-T2G (clean) sliced by ENTRY-bar descriptors ──")
    print(_line("FAM all", "FAM"))
    print("  · RSI:")
    for k in ["FAM_rsi_lt30", "FAM_rsi_30_40", "FAM_rsi_40_50", "FAM_rsi_50_60", "FAM_rsi_ge60"]:
        print(_line("   " + k.replace("FAM_", ""), k))
    print("  · vol / L:")
    for k in ["FAM_volB", "FAM_volVB", "FAM_L3", "FAM_L12", "FAM_L34"]:
        print(_line("   " + k.replace("FAM_", ""), k))
    print("  · price / body / gap:")
    for k in ["FAM_p_8_21", "FAM_p_21_89", "FAM_p_ge89", "FAM_bull", "FAM_bear", "FAM_G1", "FAM_G2", "FAM_G3"]:
        print(_line("   " + k.replace("FAM_", ""), k))
    print("  · stacks:")
    for k in ["FAM_21_89_rsi3050", "FAM_21_89_rsi3050_volB"]:
        print(_line("   " + k.replace("FAM_", ""), k))
    print("\ndone.")


if __name__ == "__main__":
    run()
