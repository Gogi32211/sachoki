"""
validate_edge_ar_zone_updown.py — for EDGE setups firing IN THE AR ZONE (±5% of
wt_resistance, inside a valid Wyckoff range): detailed UP-move vs DOWN-move behavior.
Not just reached/broke booleans — actual MAGNITUDE of continuation-up (MFE above the
signal close) vs rejection-down (MAE below), over M bars. Levels/entry frozen at signal.
Reports per setup: breakout-up% (extends >5% above resistance) vs reject-down% (falls
back >5% below support) vs stays-in-range%, plus mean MFE/MAE and forward mean/median.
1d default. READ-ONLY.
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
    n = len(df)

    ar_zone = np.zeros(n, bool)
    mfe = np.full(n, np.nan); mae = np.full(n, np.nan)      # % moves from signal close
    brk_up = np.zeros(n, bool)       # extended >5% ABOVE resistance within M bars
    rej_dn = np.zeros(n, bool)       # fell >5% BELOW support within M bars (failed at ceiling)
    fwd20 = np.full(n, np.nan)

    for tk, g in df.groupby("ticker", sort=False):
        idx = g.index.to_numpy()
        cl = g["close"].to_numpy(float); hi = g["high"].to_numpy(float); lo = g["low"].to_numpy(float)
        vtr = g["vtr"].to_numpy(); sup = g["sup"].to_numpy(float); res = g["res"].to_numpy(float)
        m = len(g)
        for k in range(m - 1):
            if vtr[k] != 1 or not (res[k] > sup[k] > 0):
                continue
            c0 = cl[k]
            if abs(c0 / res[k] - 1) > BAND:
                continue
            ar_zone[idx[k]] = True
            end = min(k + 1 + M, m)
            if end <= k + 1:
                continue
            path_hi = hi[k + 1:end].max(); path_lo = lo[k + 1:end].min()
            mfe[idx[k]] = (path_hi / c0 - 1) * 100
            mae[idx[k]] = (path_lo / c0 - 1) * 100
            brk_up[idx[k]] = path_hi > res[k] * (1 + BAND)
            rej_dn[idx[k]] = path_lo < sup[k] * (1 - BAND)
            f = k + 20
            if f < m:
                fwd20[idx[k]] = (cl[f] / c0 - 1) * 100

    df["ar_zone"] = ar_zone; df["mfe"] = mfe; df["mae"] = mae
    df["brk_up"] = brk_up; df["rej_dn"] = rej_dn; df["fwd20"] = fwd20
    df["yr"] = df["date"].str[:4]
    arz = df["ar_zone"]

    base = df[arz]
    print(f"{TF} · AR-zone (±{int(BAND*100)}% of resistance, valid range) · M={M}\n")
    print(f"BASELINE AR-zone: n={len(base):,}  break-UP(>+5% past res) {base['brk_up'].mean()*100:.1f}%  "
          f"reject-DOWN(<-5% past sup) {base['rej_dn'].mean()*100:.1f}%  "
          f"mean MFE {base['mfe'].mean():+.2f}%  mean MAE {base['mae'].mean():+.2f}%  fwd20 {base['fwd20'].mean():+.2f}\n")

    print("── EDGE setups firing IN the AR zone — up-continuation vs down-rejection magnitude ──")
    rows = []
    for name, col in SETUPS:
        sub = df[(df[col]) & arz]
        if len(sub) < 25:
            continue
        rows.append({
            "name": name, "n": len(sub),
            "brk_up%": sub["brk_up"].mean() * 100, "rej_dn%": sub["rej_dn"].mean() * 100,
            "mfe": sub["mfe"].mean(), "mae": sub["mae"].mean(),
            "fwd20": sub["fwd20"].mean(), "fwd20_med": sub["fwd20"].median(),
        })
    A = pd.DataFrame(rows).sort_values("brk_up%", ascending=False)
    for _, r in A.iterrows():
        print(f"  {r['name']:14s} n={int(r['n']):>5}  brk-UP {r['brk_up%']:4.1f}%  rej-DOWN {r['rej_dn%']:4.1f}%  "
              f"MFE{r['mfe']:+6.2f}%  MAE{r['mae']:+6.2f}%  fwd20 mean{r['fwd20']:+5.2f} med{r['fwd20_med']:+5.2f}")
    print("\ndone.")


if __name__ == "__main__":
    run()
