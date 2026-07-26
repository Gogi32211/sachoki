"""
validate_edge_alltf.py — run the FULL edge_replay setup battery on ANY timeframe DB
(1d / 4h / 1h / 15m) and compare. Reuses edge_replay._prep/_pathsim/_stats/SETUPS so the
masks are IDENTICAL to the live Edge board; only the source DB + hold horizon change.
Horizon is scaled to a comparable CALENDAR window (~60 trading days) per tf so the trailing
exit means the same thing across timeframes. gap-realistic fills, stop-first.
Usage:  python validate_edge_alltf.py 1d|4h|1h|15m
NOTE: h1-bottom is 1d-only (onehour_capit.json dates); it reads empty on intraday. READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _prep, _pathsim, _stats, SETUPS

TF = sys.argv[1] if len(sys.argv) > 1 else "1d"
# bars/trading-day: 1w≈0.2 (1 bar=5 days), 1d=1, 4h≈2, 1h≈7, 15m≈26. maxh ≈ 60 trading days.
BPD = {"1w": 0.2, "1d": 1, "4h": 2, "1h": 7, "15m": 26}[TF]
MAXH = max(6, int(round(60 * BPD)))   # 1w → 12 weeks
KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=MAXH)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if TF == "1d" else db_path(f"studio_{'15m' if TF == '15m' else TF}.duckdb")
    if TF == "15m":
        p = db_path("studio_15m.duckdb")   # enriched, NOT the lean base
    c = duckdb.connect(p, read_only=True)
    # 15m has ~80M rows — a full-frame pandas pull is ~20GB. A per-bar $-volume floor keeps it
    # tractable (~44M rows) and only drops thin, non-tradeable 15m bars. Other tf: no floor.
    _dv = "AND close*volume >= 500000" if TF == "15m" else ""
    # project the needed columns FIRST (not SELECT *) so the dedup window sorts ~30 cols not 394;
    # intraday DBs have one universe per ticker (0 (ticker,date) dups) → skip dedup entirely.
    _proj = """universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) wt_sup,
                   coalesce(wt_resistance,0) wt_res,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp"""
    try:
        as_of = str(c.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        if TF == "1d":   # 1d has cross-universe (ticker,date) dups → dedup needed
            df = c.execute(f"""
                WITH r AS (SELECT {_proj}, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                           FROM bars WHERE close>=5 AND avg_vol_20d>0)
                SELECT * EXCLUDE (rn) FROM r WHERE rn=1 ORDER BY ticker, date
            """).fetchdf()
        else:            # intraday: one universe per ticker → no dedup, direct projection
            df = c.execute(f"""
                SELECT {_proj} FROM bars WHERE close>=5 AND avg_vol_20d>0 {_dv}
                ORDER BY ticker, date
            """).fetchdf()
        return df, as_of, p
    finally:
        c.close()


def run():
    print(f"pulling {TF}…", flush=True)
    df, as_of, p = _pull()
    df = _prep(df)
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"{TF} · {p.split('/')[-1]} · as_of {as_of} · rows={len(df):,} · maxh={MAXH} (~60 trading days) · trail25 gap-realistic\n")
    print(f"  {'setup':14s} {'n':>6}  {'mean':>6} {'med':>6} {'pf':>5} {'yr':>4} {'TR':>6} {'TE':>6}  {'22':>6}")
    out = []
    for name, col in SETUPS:
        s = _stats(name, _pathsim(grp, col, **KW))
        if not s or s.get("n", 0) == 0:
            print(f"  {name:14s} {'n=0':>6}"); continue
        py = s["per_year"]
        tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
        out.append((name, s, tr, te))
        print(f"  {name:14s} {s['n']:>6}  {s['mean']:+6.2f} {s['median']:+6.2f} {str(s['pf']):>5} "
              f"{s['pos_years']}/{s['total_years']} {tr:+6.2f} {te:+6.2f}  {py.get('2022', float('nan')):+6.2f}")
    print(f"\nas_of {as_of} · tf {TF}")


if __name__ == "__main__":
    run()
