"""
validate_range_breakout_edge.py — Wyckoff Trading-Range breakout study for the EDGE SETUPS.
For each validated Edge setup (GEM1, G3, Z11-T11, L43, Atomic-R, Engulf-Abs, Washout, D+L1,
Spring, P55, Parabola, …) that fires WHILE INSIDE a validated range (wyc_in_tr, bounded by
wt_support/wt_resistance): within the next M bars does price break the top (UP) or bottom
(DOWN) first? Levels FROZEN at the signal bar. Reports per-setup up%/down%/ratio + per-year
up% (is the up-tilt era-stable?). Reuses edge_replay._prep so masks == the live Edge board.
1d default. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, SETUPS

TF = sys.argv[1] if len(sys.argv) > 1 else "1d"
M = 30
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if TF == "1d" else db_path(f"studio_{TF}.duckdb")
    c = duckdb.connect(p, read_only=True)
    try:
        df = c.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=1000000)
            SELECT universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   coalesce(wyc_in_tr,0) in_tr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        c.close()


def run():
    print(f"pulling {TF}…", flush=True)
    df = _pull()
    df = _prep(df)
    setup_cols = [c for _, c in SETUPS]

    # walk forward per ticker, recording breakout for every in-range bar (store index → outcome)
    df = df.reset_index(drop=True)
    outcome = np.full(len(df), "", dtype=object)
    fwd = np.full(len(df), np.nan)
    for tk, g in df.groupby("ticker", sort=False):
        idx = g.index.to_numpy()
        cl = g["close"].to_numpy(float); intr = g["in_tr"].to_numpy()
        sup = g["sup"].to_numpy(float); res = g["res"].to_numpy(float); n = len(g)
        for k in range(n - 1):
            if intr[k] != 1 or not (res[k] > sup[k] > 0) or not (sup[k] < cl[k] < res[k]):
                continue
            end = min(k + 1 + M, n); out = "none"; jb = end - 1
            for j in range(k + 1, end):
                if cl[j] > res[k]:
                    out = "up"; jb = j; break
                if cl[j] < sup[k]:
                    out = "down"; jb = j; break
            outcome[idx[k]] = out
            fwd[idx[k]] = (cl[jb] / cl[k] - 1) * 100
    df["out"] = outcome; df["fwd"] = fwd
    df["yr"] = df["date"].str[:4]
    inr = df["out"] != ""   # evaluated (in-range) bars

    b_up = (df.loc[inr, "out"] == "up").mean() * 100
    b_dn = (df.loc[inr, "out"] == "down").mean() * 100
    print(f"{TF} · in-range evaluated bars {int(inr.sum()):,} · M={M} · levels frozen at signal\n")
    print(f"BASELINE (all in-range): UP {b_up:.1f}%  DOWN {b_dn:.1f}%  ratio {b_up/max(b_dn,0.1):.2f}\n")

    print("── EDGE setups fired IN-RANGE → breakout direction (sorted by UP%) ──")
    rows = []
    for name, col in SETUPS:
        sub = df[(df[col]) & inr]
        if len(sub) < 25:
            rows.append((name, len(sub), None)); continue
        up = (sub["out"] == "up").mean() * 100
        dn = (sub["out"] == "down").mean() * 100
        fu = sub.loc[sub["out"] == "up", "fwd"].mean()
        # per-year up%
        pyr = {}
        for y in YRS:
            s2 = sub[sub["yr"] == y]
            pyr[y] = (s2["out"] == "up").mean() * 100 if len(s2) >= 10 else None
        rows.append((name, len(sub), {"up": up, "dn": dn, "ratio": up / max(dn, 0.1),
                                       "lift": up - b_up, "fwd": sub["fwd"].mean(), "fu": fu, "pyr": pyr}))
    for name, n, r in sorted(rows, key=lambda x: (x[2]["up"] if x[2] else -1), reverse=True):
        if r is None:
            print(f"  {name:14s} n={n:>4}  (too few in-range)"); continue
        yrs_up = " ".join(f"{y[2:]}:{('%2.0f'%r['pyr'][y]) if r['pyr'][y] is not None else ' -'}" for y in YRS)
        print(f"  {name:14s} n={n:>4}  UP {r['up']:4.1f}% (lift{r['lift']:+5.1f}) DOWN {r['dn']:4.1f}% "
              f"ratio {r['ratio']:.2f}  fwd{r['fwd']:+5.2f} fwd|up{r['fu']:+5.2f} | up%/yr {yrs_up}")
    print("\ndone.")


if __name__ == "__main__":
    run()
