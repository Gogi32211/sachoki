"""
validate_range_breakout.py — Wyckoff Trading-Range breakout study.
Question (user): a signal fires WHILE INSIDE a validated trading range (wyc_in_tr, bounded by
wt_support/wt_resistance = the SC/AR-style range floor/ceiling). Within the next M bars, does
price break OUT the top (close > resistance = UP) or the bottom (close < support = DOWN) first?
Per signal token (T1/T2G/Z11/…), report up% vs down% vs no-break — WHICH in-range signal best
predicts an UP breakout. Levels are FROZEN at the signal bar (no lookahead). Also a baseline
(all in-range bars) so each signal's lift over base is visible. 1d default. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd

TF = sys.argv[1] if len(sys.argv) > 1 else "1d"
M = 30          # breakout window in bars
MIN_N = 150     # only report tokens with enough samples


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if TF == "1d" else db_path(f"studio_{TF}.duckdb")
    c = duckdb.connect(p, read_only=True)
    try:
        df = c.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=1000000)
            SELECT ticker, CAST(date AS VARCHAR) date, close, high, low,
                   coalesce(wyc_in_tr,0) in_tr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, rsi_14
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        c.close()


def run():
    print(f"pulling {TF}…", flush=True)
    df = _pull()
    recs = []
    for tk, g in df.groupby("ticker", sort=False):
        cl = g["close"].to_numpy(float); intr = g["in_tr"].to_numpy()
        sup = g["sup"].to_numpy(float); res = g["res"].to_numpy(float)
        tok_t = g["t"].to_numpy(); tok_z = g["z"].to_numpy()
        yr = g["date"].str[:4].to_numpy(); n = len(g)
        for i in range(n - 1):
            # in a valid range, strictly INSIDE it (not already at/through an edge)
            if intr[i] != 1 or not (res[i] > sup[i] > 0) or not (sup[i] < cl[i] < res[i]):
                continue
            up_lvl, dn_lvl = res[i], sup[i]
            end = min(i + 1 + M, n)
            outcome = "none"; jb = end - 1
            for j in range(i + 1, end):
                if cl[j] > up_lvl:
                    outcome = "up"; jb = j; break
                if cl[j] < dn_lvl:
                    outcome = "down"; jb = j; break
            tok = tok_t[i] if tok_t[i] else tok_z[i]
            if not tok:
                continue
            fwd = (cl[jb] / cl[i] - 1) * 100
            recs.append((tok, outcome, fwd, yr[i]))
    R = pd.DataFrame(recs, columns=["tok", "out", "fwd", "yr"])
    print(f"{TF} · in-range signal bars {len(R):,} · breakout window M={M} bars · levels frozen at signal\n")

    base = R
    b_up = (base["out"] == "up").mean() * 100
    b_dn = (base["out"] == "down").mean() * 100
    print(f"BASELINE (all in-range signals): n={len(base):,}  UP {b_up:.1f}%  DOWN {b_dn:.1f}%  "
          f"none {100-b_up-b_dn:.1f}%  · up/down ratio {b_up/max(b_dn,0.1):.2f}  · mean fwd {base['fwd'].mean():+.2f}\n")

    agg = []
    for tok, sub in R.groupby("tok"):
        if len(sub) < MIN_N:
            continue
        up = (sub["out"] == "up").mean() * 100
        dn = (sub["out"] == "down").mean() * 100
        agg.append({"tok": tok, "n": len(sub), "up%": up, "down%": dn,
                    "none%": 100 - up - dn, "ratio": up / max(dn, 0.1),
                    "up_lift": up - b_up, "fwd": sub["fwd"].mean(),
                    "fwd_up": sub.loc[sub["out"] == "up", "fwd"].mean()})
    A = pd.DataFrame(agg)

    print("── TOP up-breakout predictors (in-range, n≥150, sorted by UP%) ──")
    for _, r in A.sort_values("up%", ascending=False).head(18).iterrows():
        print(f"  {r['tok']:7s} n={int(r['n']):>5}  UP {r['up%']:4.1f}% (lift {r['up_lift']:+4.1f})  "
              f"DOWN {r['down%']:4.1f}%  ratio {r['ratio']:.2f}  none {r['none%']:4.1f}%  "
              f"fwd{r['fwd']:+5.2f}  fwd|up {r['fwd_up']:+5.2f}")
    print("\n── BOTTOM (most DOWN-biased in-range signals) ──")
    for _, r in A.sort_values("up%", ascending=True).head(8).iterrows():
        print(f"  {r['tok']:7s} n={int(r['n']):>5}  UP {r['up%']:4.1f}%  DOWN {r['down%']:4.1f}%  "
              f"ratio {r['ratio']:.2f}  fwd{r['fwd']:+5.2f}")
    print("\ndone.")


if __name__ == "__main__":
    run()
