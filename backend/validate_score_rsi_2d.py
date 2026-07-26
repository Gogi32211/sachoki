"""
validate_score_rsi_2d.py — STAGE 2: does any score add edge BEYOND RSI?
Double-sort: score band × RSI band → clean fwd-20d (next-open entry). If rows within an
RSI band are flat across score bands, the score is just RSI in disguise (redundant).
Also prints each score decile's mean RSI (correlation check). READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))

TR = ("2021", "2022", "2023"); TE = ("2024", "2025", "2026")
SCORES = ["turbo_score", "prebreak_v2", "profile_score", "aes_score"]
RSI_BANDS = [("<35", 0, 35), ("35-50", 35, 50), ("50-60", 50, 60), ("60+", 60, 101)]


def _pull():
    import duckdb
    a = duckdb.connect("/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb",
                       read_only=True)
    try:
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT ticker, CAST(date AS VARCHAR) date, open, close, rsi_14, {', '.join(SCORES)},
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        a.close()


def run():
    print("pulling…", flush=True)
    df = _pull()
    g = df.groupby("ticker", sort=False)
    df["fwd"] = (g["close"].shift(-21) / g["open"].shift(-1) - 1) * 100
    df["yr"] = df["date"].str[:4]
    base = df[(df["supp"] == 0) & df["fwd"].notna() & (df["fwd"].abs() < 300)
              & df["rsi_14"].notna()].copy()
    print(f"clean rows {len(base):,}\n")

    print("── score-decile mean-RSI (correlation check) ──")
    for col in SCORES:
        d = pd.qcut(base[col].rank(method="first"), 10, labels=False)
        mr = base.groupby(d)["rsi_14"].mean().round(1).tolist()
        corr = base[col].corr(base["rsi_14"])
        print(f"  {col:14s} rsi-by-decile {mr}  corr {corr:+.2f}")
    print()

    for col in SCORES:
        # score terciles WITHIN the clean sample: low / mid / high
        t = pd.qcut(base[col].rank(method="first"), 3, labels=["loS", "midS", "hiS"])
        base["_t"] = t.values
        print(f"── {col}: RSI band × score tercile → fwd20 mean (med) ['22] ──")
        hdr = "  RSI      " + "".join(f"{s:>26s}" for s in ["loS", "midS", "hiS"])
        print(hdr)
        for lbl, lo, hi in RSI_BANDS:
            row = f"  {lbl:8s}"
            m0 = base[(base["rsi_14"] >= lo) & (base["rsi_14"] < hi)]
            for s in ["loS", "midS", "hiS"]:
                c = m0[m0["_t"] == s]
                if len(c) < 2000:
                    row += f"{'n<2k':>26s}"; continue
                y22 = c.loc[c["yr"] == "2022", "fwd"].mean()
                row += f"  {c['fwd'].mean():+5.2f}({c['fwd'].median():+5.2f})[{y22:+5.1f}]"
            print(row + f"   n_band={len(m0):,}")
        print()
    print("done.")


if __name__ == "__main__":
    run()
