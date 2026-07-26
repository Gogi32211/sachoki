"""
validate_edge_bc_ar_zones.py — where do the EDGE setups fire relative to the Wyckoff range
boundaries (SC/support floor and AR/BC-resistance ceiling), and what happens next?
For each edge signal inside a VALID trading range (wt_valid_tr, bounded by wt_support/
wt_resistance), classify by PRICE ±5% zone:
   SC-zone  = close within ±5% of support   (bottom / Selling-Climax edge)
   AR-zone  = close within ±5% of resistance (top / Automatic-Rally-BC edge)
   mid      = strictly between, in neither ±5% band
Forward M bars (levels frozen at signal): reached_res% (rallied to the ceiling = bullish
resolution), broke_sup% (fell through the floor = bearish), mean fwd-20. Per setup × zone,
with per-year up-resolution for the standout. Reuses edge_replay._prep. 1d default. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, SETUPS

TF = sys.argv[1] if len(sys.argv) > 1 else "1d"
M = 30
BAND = 0.05
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if TF == "1d" else db_path(f"studio_{TF}.duckdb")
    c = duckdb.connect(p, read_only=True)
    try:
        return c.execute("""
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
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) sup, coalesce(wt_resistance,0) res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        c.close()


def run():
    print(f"pulling {TF}…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)
    # forward outcomes per bar that sits in a valid range
    n = len(df)
    reached_res = np.zeros(n, bool); broke_sup = np.zeros(n, bool)
    fwd20 = np.full(n, np.nan); zone = np.full(n, "", dtype=object)
    for tk, g in df.groupby("ticker", sort=False):
        idx = g.index.to_numpy()
        cl = g["close"].to_numpy(float); vtr = g["vtr"].to_numpy()
        sup = g["sup"].to_numpy(float); res = g["res"].to_numpy(float); m = len(g)
        for k in range(m - 1):
            if vtr[k] != 1 or not (res[k] > sup[k] > 0):
                continue
            c0 = cl[k]
            if abs(c0 / sup[k] - 1) <= BAND:
                zone[idx[k]] = "SC"
            elif abs(c0 / res[k] - 1) <= BAND:
                zone[idx[k]] = "AR"
            elif sup[k] < c0 < res[k]:
                zone[idx[k]] = "mid"
            else:
                continue
            end = min(k + 1 + M, m)
            for j in range(k + 1, end):
                if cl[j] >= res[k] and not reached_res[idx[k]]:
                    reached_res[idx[k]] = True
                if cl[j] <= sup[k] * (1 - BAND):
                    broke_sup[idx[k]] = True; break
                if reached_res[idx[k]]:
                    break
            f = k + 20
            if f < m:
                fwd20[idx[k]] = (cl[f] / c0 - 1) * 100
    df["zone"] = zone; df["reached_res"] = reached_res; df["broke_sup"] = broke_sup
    df["fwd20"] = fwd20; df["yr"] = df["date"].str[:4]
    inz = df["zone"] != ""

    print(f"{TF} · valid-range signal bars {int(inz.sum()):,} · ±{int(BAND*100)}% zones · M={M}\n")
    for z in ("SC", "AR", "mid"):
        s = df[df["zone"] == z]
        print(f"BASELINE {z:3s}: n={len(s):>7,}  reached-RES {(s['reached_res'].mean()*100):4.1f}%  "
              f"broke-SUP {(s['broke_sup'].mean()*100):4.1f}%  fwd20 {s['fwd20'].mean():+.2f}")
    print()

    for z in ("SC", "AR"):
        print(f"══ EDGE setups in the {z} ±5% zone (near {'support/Selling-Climax' if z=='SC' else 'resistance/AR-BC ceiling'}) — sorted by reached-RES% ══")
        rows = []
        for name, col in SETUPS:
            sub = df[(df[col]) & (df["zone"] == z)]
            if len(sub) < 25:
                continue
            rows.append((name, len(sub), sub["reached_res"].mean() * 100,
                         sub["broke_sup"].mean() * 100, sub["fwd20"].mean(),
                         {y: ((sub[sub["yr"] == y]["reached_res"].mean() * 100) if (sub["yr"] == y).sum() >= 10 else None) for y in YRS}))
        for name, nn, rr, bs, fw, pyr in sorted(rows, key=lambda x: -x[2]):
            yrs = " ".join(f"{y[2:]}:{('%2.0f'%pyr[y]) if pyr[y] is not None else ' -'}" for y in YRS)
            print(f"  {name:14s} n={nn:>4}  reached-RES {rr:4.1f}%  broke-SUP {bs:4.1f}%  fwd20{fw:+5.2f} | rr%/yr {yrs}")
        print()
    print("done.")


if __name__ == "__main__":
    run()
