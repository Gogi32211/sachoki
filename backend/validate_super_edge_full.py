"""
validate_super_edge_full.py — FULL per-edge × zone path-sim matrix. For every Edge setup:
ungated vs SC-zone (±5% support) vs AR-zone (±5% resistance) vs mid (strictly between).
Picks each setup's own best zone (needs n≥50, mean AND median both ≥ ungated, and TR not
badly negative) — "SUPER" gate is individual per setup, not one-size-fits-all.
trail25/60, gap-realistic, stop-first (same masks as the live board). 1d default. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _prep, _pathsim, _stats, SETUPS

TF = sys.argv[1] if len(sys.argv) > 1 else "1d"
KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
BAND = 0.05


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if TF == "1d" else db_path(f"studio_{TF}.duckdb")
    c = duckdb.connect(p, read_only=True)
    try:
        return c.execute("""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
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


def _stat(m, dfbase):
    d2 = dfbase.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return None
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py])
    return {"n": s["n"], "mean": s["mean"], "median": s["median"], "pf": s["pf"],
            "yr": f"{s['pos_years']}/{s['total_years']}", "TR": tr, "22": py.get("2022")}


def _fmt(s):
    if s is None:
        return "n<50 (skip)"
    tr = s["TR"]; y22 = s["22"]
    return (f"n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['yr']} TR{tr:+5.2f} '22{y22:+5.2f}" if y22 is not None else
            f"n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} y{s['yr']} TR{tr:+5.2f}")


def run():
    print(f"pulling {TF}…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)

    vtr = df["vtr"].to_numpy(); sup = df["sup"].to_numpy(float); res = df["res"].to_numpy(float)
    cl = df["close"].to_numpy(float)
    valid = (vtr == 1) & (res > sup) & (sup > 0)
    sc = np.zeros(len(df), bool); ar = np.zeros(len(df), bool); mid = np.zeros(len(df), bool)
    sc[valid] = np.abs(cl[valid] / sup[valid] - 1) <= BAND
    ar[valid] = np.abs(cl[valid] / res[valid] - 1) <= BAND
    mid[valid] = valid[valid] & ~sc[valid] & ~ar[valid] & (sup[valid] < cl[valid]) & (cl[valid] < res[valid])
    df["sc"] = sc; df["ar"] = ar; df["mid"] = mid

    print(f"{TF} · zones: SC=±{int(BAND*100)}%support · AR=±{int(BAND*100)}%resistance · mid=between "
          f"· trail25/60 gap-realistic\n")

    verdicts = []
    for name, col in SETUPS:
        base_m = df[col]
        n_base = int(base_m.sum())
        if n_base < 30:
            print(f"── {name} (n={n_base}, too few overall — skip) ──\n"); continue
        s_all = _stat(base_m, df)
        s_sc = _stat(base_m & df["sc"], df)
        s_ar = _stat(base_m & df["ar"], df)
        s_mid = _stat(base_m & df["mid"], df)
        print(f"── {name} ──")
        print(f"  ungated   {_fmt(s_all)}")
        print(f"  SC-zone   {_fmt(s_sc)}")
        print(f"  AR-zone   {_fmt(s_ar)}")
        print(f"  mid-zone  {_fmt(s_mid)}")

        # pick best: candidate zones beating ungated on BOTH mean and median, with n>=50 and TR not deeply negative
        cands = []
        for zone_name, s in (("SC", s_sc), ("AR", s_ar), ("mid", s_mid)):
            if s is None or s["n"] < 50:
                continue
            if s["mean"] >= s_all["mean"] and s["median"] >= s_all["median"] and s["TR"] > -1.5:
                cands.append((zone_name, s))
        if cands:
            best = max(cands, key=lambda x: x[1]["mean"] + x[1]["median"])
            verdicts.append((name, best[0], best[1], s_all))
            print(f"  → SUPER zone: {best[0]}  ({_fmt(best[1])})")
        else:
            verdicts.append((name, None, None, s_all))
            print(f"  → SUPER zone: none (ungated stays best)")
        print()

    print("═" * 70)
    print("SUMMARY — per-edge SUPER zone verdict")
    for name, zone, s, s_all in verdicts:
        if zone:
            print(f"  {name:14s} → {zone:4s}  (ungated m{s_all['mean']:+.2f}/md{s_all['median']:+.2f} "
                  f"→ {zone} m{s['mean']:+.2f}/md{s['median']:+.2f})")
        else:
            print(f"  {name:14s} → none (stay ungated, m{s_all['mean']:+.2f}/md{s_all['median']:+.2f})")
    print("\ndone.")


if __name__ == "__main__":
    run()
