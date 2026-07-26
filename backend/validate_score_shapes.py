"""
validate_score_shapes.py — STAGE 1 of the Ultra-screener scoring analysis (backtest-expert).
Hypothesis (user): forward edge vs score is INVERTED-U — the middle band is best, the top
is overextended, the bottom is oversold-knife. Test the SHAPE for every stored score column:
decile bands (percentile-based, with the covered score range shown) → CLEAN forward 20d
return (entry next-open → close 20 bars on; the precomputed fwd_* cols are corrupt — never
used). Reports n / mean / median / win / TRAIN(21-23) / TEST(24-26) / 2022 per decile.
RSI deciles included as the reference shape. Vectorized (no path-sim) — shape first,
tradability (path-sim) is stage 2 on the interesting bands. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))

SCORES = ["turbo_score", "final_bull_score", "beta_score", "gog_score", "prebreak_score",
          "prebreak_v2", "prebreak_v3", "profile_score", "rtb_total", "aes_score", "rsi_14"]
TR = ("2021", "2022", "2023"); TE = ("2024", "2025", "2026")


def _pull():
    import duckdb
    a = duckdb.connect("/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb",
                       read_only=True)
    try:
        cols = ", ".join(SCORES)
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
            SELECT ticker, CAST(date AS VARCHAR) date, open, close, {cols},
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
    # clean forward: entry at NEXT open, exit at close 20 bars after entry
    entry = g["open"].shift(-1)
    exit_ = g["close"].shift(-21)
    df["fwd"] = (exit_ / entry - 1) * 100
    df["yr"] = df["date"].str[:4]
    base = df[(df["supp"] == 0) & df["fwd"].notna() & (df["fwd"].abs() < 300)].copy()
    print(f"rows={len(df):,} clean-with-fwd={len(base):,} · fwd = next-open → close+20b (no mgmt)\n")
    bl = base["fwd"]
    bl_tr = base.loc[base["yr"].isin(TR), "fwd"]; bl_te = base.loc[base["yr"].isin(TE), "fwd"]
    print(f"BASELINE all bars: n={len(bl):,} mean{bl.mean():+.2f} med{bl.median():+.2f} "
          f"win{(bl>0).mean()*100:.1f} TR{bl_tr.mean():+.2f} TE{bl_te.mean():+.2f} "
          f"'22{base.loc[base['yr']=='2022','fwd'].mean():+.2f}\n")

    for col in SCORES:
        s = base[col].astype(float)
        ok = s.notna()
        # decile by rank (ties share a bucket); show the covered raw-score range per decile
        try:
            b = pd.qcut(s[ok].rank(method="first"), 10, labels=False)
        except Exception:
            print(f"── {col}: cannot decile (constant?) ──"); continue
        sub = base[ok].copy(); sub["dec"] = b.values
        print(f"── {col} — deciles (low→high) ──")
        rows = []
        for d in range(10):
            m = sub[sub["dec"] == d]
            lo, hi = m[col].min(), m[col].max()
            f = m["fwd"]
            tr = m.loc[m["yr"].isin(TR), "fwd"].mean(); te = m.loc[m["yr"].isin(TE), "fwd"].mean()
            y22 = m.loc[m["yr"] == "2022", "fwd"].mean()
            rows.append((d, lo, hi, len(m), f.mean(), f.median(), (f > 0).mean() * 100, tr, te, y22))
        for d, lo, hi, n, mn, md, w, tr, te, y22 in rows:
            print(f"  D{d}  [{lo:6.1f}..{hi:6.1f}] n={n:>7,} mean{mn:+6.2f} med{md:+6.2f} "
                  f"win{w:4.1f} TR{tr:+6.2f} TE{te:+6.2f} '22{y22:+6.2f}")
        # crude shape verdict on means
        mns = [r[4] for r in rows]
        peak = int(np.argmax(mns))
        mono_up = all(mns[i] <= mns[i + 1] + 0.05 for i in range(9))
        mono_dn = all(mns[i] >= mns[i + 1] - 0.05 for i in range(9))
        shape = ("MONOTONE-UP" if mono_up else "MONOTONE-DOWN" if mono_dn
                 else f"PEAK at D{peak} (inverted-U?)" if 0 < peak < 9 else f"peak D{peak}")
        print(f"  → shape: {shape} · spread(best-worst) {max(mns)-min(mns):+.2f}pp\n")
    print("done.")


if __name__ == "__main__":
    run()
