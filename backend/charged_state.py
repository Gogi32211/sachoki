"""
charged_state.py — the ⚡CHARGED energy flag (validated 2026-07-06).

State (1h day-aggregates, windows ending at the latest day):
    rvol3 > 1.2   (3d volume vs own 20d baseline — hot)
    comp3 > 1.0   (3d range vs baseline — expanded)
    lbrel < 0.9   (last-bar volume share vs baseline — diffuse intraday flow)

Evidence: prespike study (AUC 0.63, 6/6yr, 2 TFs) + Edge-fire booster test:
9/10 setups improve when entered charged (Z11 +4.0→+14.6 med/81% win, G3
+0.66→+4.83 n=3.5k, L43 +1.78→+8.63; Atomic/Washout/H1-bottom flip positive).
Exception: D+L1 (no benefit). Direction does NOT leak (buy-side AUC 0.5) —
charged is ENERGY only; the Edge signal supplies direction. Badge-only.

Usage (scanner-side, candidates only):
    from charged_state import charged_for
    ch = charged_for([r["ticker"] for r in rows])
    if ch.get(r["ticker"]): r["charged"] = True; r["atoms"].append("⚡charged")
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import db_path


def charged_for(tickers: "list[str]") -> "dict[str, bool]":
    tks = sorted({t.upper() for t in tickers if t})
    if not tks:
        return {}
    con = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    try:
        f = con.execute(f"""
        WITH b AS (
          SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) AS d0, date, high, low, close, volume
          FROM bars
          WHERE ticker IN ({','.join('?' * len(tks))})
            AND date >= now() - INTERVAL 60 DAY),
        a AS (
          SELECT ticker, CAST(d0 AS VARCHAR) AS dstr, count(*) n_bars, sum(volume) tot_vol,
                 arg_max(volume, date) lastbar_vol, max(high) dh, min(low) dl,
                 arg_max(close, date) lc
          FROM b GROUP BY ticker, d0)
        SELECT * FROM a WHERE n_bars >= 5 ORDER BY ticker, dstr""", tks).fetchdf()
    finally:
        con.close()
    out: dict[str, bool] = {}
    for tk, g in f.groupby("ticker", sort=False):
        if len(g) < 24:                       # need the 20d baseline + 3d window
            out[tk] = False
            continue
        tot = g["tot_vol"].to_numpy(float)
        lb = (g["lastbar_vol"] / g["tot_vol"].replace(0, np.nan)).to_numpy(float)
        rng = ((g["dh"] - g["dl"]) / g["lc"].replace(0, np.nan)).to_numpy(float)
        rvol3 = tot[-3:].mean() / max(tot[-23:-3].mean(), 1e-9)
        comp3 = np.nanmean(rng[-3:]) / max(np.nanmean(rng[-23:-3]), 1e-9)
        lbrel = np.nanmean(lb[-3:]) / max(np.nanmean(lb[-23:-3]), 1e-9)
        out[tk] = bool(rvol3 > 1.2 and comp3 > 1.0 and lbrel < 0.9)
    return out
