"""
validate_super_edge_zone.py — does gating each Edge setup by its "SC zone" (fires within
±5% of wt_support, in a valid Wyckoff range) IMPROVE path-sim tradeability (not just raw
fwd20)? Compares UNGATED vs SC-GATED for every setup with enough SC-zone samples.
trail25/60, gap-realistic, stop-first (edge_replay engine) — same masks as the live board.
1d default. READ-ONLY.
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


def _line(lbl, m, grp_base):
    d2 = grp_base.copy(); d2["_m"] = m.values
    grp = {tk: gg.reset_index(drop=True) for tk, gg in d2.groupby("ticker", sort=False)}
    s = _stats("x", _pathsim(grp, "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:20s} n=0"
    py = s["per_year"]
    tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
    return (f"  {lbl:20s} n={s['n']:>5} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
            f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022', float('nan')):+5.2f}")


def run():
    print(f"pulling {TF}…", flush=True)
    df = _pull().reset_index(drop=True)
    df = _prep(df)

    # SC-zone flag: close within ±5% of support, in a valid range (frozen, no lookahead)
    sc = np.zeros(len(df), bool)
    vtr = df["vtr"].to_numpy(); sup = df["sup"].to_numpy(float); res = df["res"].to_numpy(float)
    cl = df["close"].to_numpy(float)
    valid = (vtr == 1) & (res > sup) & (sup > 0)
    sc[valid] = np.abs(cl[valid] / sup[valid] - 1) <= BAND
    df["sc_zone"] = sc

    print(f"{TF} · SC-zone gate = valid-range & within ±{int(BAND*100)}% of support · trail25/60 gap-realistic\n")
    for name, col in SETUPS:
        m_all = df[col]
        m_sc = df[col] & df["sc_zone"]
        if int(m_sc.sum()) < 20:
            continue
        print(f"── {name} ──")
        print(_line("ungated (all)", m_all, df))
        print(_line("+ SC-zone gate", m_sc, df))
        print()
    print("done.")


if __name__ == "__main__":
    run()
