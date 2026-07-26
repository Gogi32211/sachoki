"""gex_edge_logger.py — forward-accumulation log of GEX context at each Edge fire
(2026-07-22). Massive has NO historical options snapshot, so GEX-confluence cannot be
backtested retroactively — we must capture the LIVE GEX context on each edge-fire day
and join forward returns LATER (months out) to validate "Edge + GEX-context" tiers.

Run once daily after the US close (wired into the nightly). Appends today's edge-fires
+ their GEX levels/distances to data/gex_edge_log.parquet, deduped by (date,ticker,edge).

Validate later with: join (date,ticker) → bars forward path-sim, split by regime /
near-put-wall / near-flip, compare to edge-alone. ISOLATED: reads edges + options only,
writes its own parquet; touches nothing in the stock pipeline.
"""
from __future__ import annotations
import os
import logging
from datetime import date

import pandas as pd

log = logging.getLogger(__name__)
_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "gex_edge_log.parquet")


def _dist(spot, level):
    if spot and level and spot > 0:
        return round((level - spot) / spot * 100, 2)   # % of spot; +above / −below
    return None


def capture(as_of: str | None = None, max_dte: int = 45) -> dict:
    """Snapshot today's edge-fires with their live GEX context. Returns a summary dict."""
    from edge_replay import latest_edges_map
    from gex_engine import gex_for_ticker

    today = as_of or str(date.today())
    edges = latest_edges_map(lookback=1, build=True) or {}   # {ticker: [codes]} fired ≤1 bar
    if not edges:
        return {"date": today, "edge_tickers": 0, "logged": 0, "note": "no edge fires today"}

    rows = []
    gex_cache: dict = {}
    for tk, codes in edges.items():
        code_list = codes if isinstance(codes, (list, tuple)) else [codes]
        # latest_edges_map yields either bare code strings OR (code, age) tuples —
        # take element 0 when it's a tuple, then strip any "·4d" age suffix.
        def _code(c):
            base = c[0] if isinstance(c, (list, tuple)) else c
            return str(base).split("·")[0]
        base_codes = sorted({_code(c) for c in code_list})
        g = gex_cache.get(tk)
        if g is None:
            g = gex_for_ticker(tk, max_dte=max_dte) or {}
            gex_cache[tk] = g
        if not g or g.get("regime") is None:  # gex_for_ticker sets 'regime' only when it worked
            continue                          # no options chain → skip (illiquid)
        spot = g.get("spot")
        for code in base_codes:
            rows.append({
                "date": today, "ticker": tk, "edge": code,
                "spot": spot, "regime": g.get("regime"), "net_gex": g.get("net_gex"),
                "atm_iv": g.get("atm_iv"),
                # ⚖️ VRP (2026-07-26): IV vs ATR-realized vol — accumulate for future
                # "edge + EVENT-PRICED/COMPLACENT" tier validation (forward-only, like GEX)
                "rv_atr": g.get("rv_atr"), "vrp": g.get("vrp"),
                "gamma_flip": g.get("gamma_flip"), "power_zone": g.get("power_zone"),
                "call_wall": g.get("call_wall"), "put_wall": g.get("put_wall"),
                "max_pain": g.get("max_pain"),
                "dist_flip": _dist(spot, g.get("gamma_flip")),
                "dist_pz": _dist(spot, g.get("power_zone")),
                "dist_callwall": _dist(spot, g.get("call_wall")),
                "dist_putwall": _dist(spot, g.get("put_wall")),
                "dist_maxpain": _dist(spot, g.get("max_pain")),
                "total_oi": g.get("total_oi"),
            })
    if not rows:
        return {"date": today, "edge_tickers": len(edges), "logged": 0,
                "note": "edge tickers had no optionable GEX"}

    new = pd.DataFrame(rows)
    if os.path.exists(_LOG_PATH):
        old = pd.read_parquet(_LOG_PATH)
        combined = pd.concat([old, new], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "ticker", "edge"], keep="last")
    else:
        combined = new
    combined.to_parquet(_LOG_PATH, index=False)
    return {"date": today, "edge_tickers": len(edges), "optionable": new["ticker"].nunique(),
            "logged": len(new), "total_rows": len(combined), "path": _LOG_PATH}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    print(json.dumps(capture(), indent=1, default=str))
