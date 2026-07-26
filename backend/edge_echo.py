"""
edge_echo.py — cross-timeframe ECHO test for every Edge setup.

Principle (from the T6→Z1G→T5 finding, 2026-07-05): a REAL market edge is
approximately fractal — the same STATE should pay (at least in sign) on
neighboring timeframes. A single-TF-single-era result is a coincidence.

Runs the full edge_replay battery (identical masks to the live board) on
1w/1d/4h/1h/15m with calendar-comparable horizons (~60 trading days), and
records per-setup per-TF per-YEAR stats (n / mean / median / win) to JSON.

Output: edge_echo.json + printed matrices. h1-bottom is 1d-only by design.
READ-ONLY on all DBs.
"""
from __future__ import annotations
import gc, json, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _prep, _pathsim, SETUPS

OUT = os.path.join(os.path.dirname(__file__), "edge_echo.json")
BPD = {"1w": 0.2, "1d": 1, "4h": 2, "1h": 7, "15m": 26}
TFS = ["1w", "1d", "4h", "1h", "15m"]          # heaviest last
_PROJ = """universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
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


def pull(tf: str):
    import duckdb
    from studio.paths import ANALYTICS_DB, db_path
    p = ANALYTICS_DB if tf == "1d" else db_path(f"studio_{tf}.duckdb")
    dv = "AND close*volume >= 500000" if tf == "15m" else ""
    c = duckdb.connect(p, read_only=True)
    try:
        if tf == "1d":
            df = c.execute(f"""WITH r AS (SELECT {_PROJ},
                    row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                  FROM bars WHERE close>=5 AND avg_vol_20d>0)
                SELECT * EXCLUDE (rn) FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
        else:
            df = c.execute(f"""SELECT {_PROJ} FROM bars
                WHERE close>=5 AND avg_vol_20d>0 {dv} ORDER BY ticker, date""").fetchdf()
        return df
    finally:
        c.close()


def main():
    t0 = time.time()
    res: dict = {}
    for tf in TFS:
        maxh = max(6, int(round(60 * BPD[tf])))
        print(f"\n===== {tf} · maxh={maxh} =====", flush=True)
        df = pull(tf)
        df = _prep(df)
        grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
        nrows = len(df)
        del df; gc.collect()
        print(f"rows={nrows:,} pulled+prepped ({time.time()-t0:.0f}s)", flush=True)
        res[tf] = {}
        for name, col in SETUPS:
            if "🌀" in name:
                continue                        # SC variants excluded: keep the matrix readable
            tr = _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, maxh)
            if len(tr) == 0:
                res[tf][name] = {"n": 0}
                continue
            r = tr["ret"] * 100
            per_year = {}
            for y, s in tr.assign(ret_pct=r).groupby("yr"):
                v = s["ret_pct"]
                per_year[y] = {"n": int(len(v)), "mean": round(float(v.mean()), 2),
                               "med": round(float(v.median()), 2),
                               "win": round(float((v > 0).mean() * 100), 1)}
            res[tf][name] = {"n": int(len(r)), "mean": round(float(r.mean()), 2),
                             "med": round(float(r.median()), 2),
                             "win": round(float((r > 0).mean() * 100), 1),
                             "per_year": per_year}
            print(f"  {name:14s} n={len(r):>6} mean {r.mean():+6.2f} med {r.median():+6.2f} "
                  f"win {(r > 0).mean()*100:3.0f}%", flush=True)
        del grp; gc.collect()
        with open(OUT, "w") as f:                # checkpoint after every tf
            json.dump(res, f)
        print(f"[checkpoint saved · {time.time()-t0:.0f}s]", flush=True)
    print(f"\nALL DONE {time.time()-t0:.0f}s → {OUT}")


if __name__ == "__main__":
    main()
