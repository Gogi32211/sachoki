"""
ai_journal/industry_pulse.py — market-context dashboard (NOT a stock-picking edge).

Combines what we validated as useful CONTEXT:
  - market regime (breadth)            → regime.compute_regime
  - sector heat (today's move by sector) → bars(latest) x ticker_meta
  - top movers (gainers / losers)
  - market-cap distribution

Honest framing: sector is a weak alpha dimension (validated), so this is a
situational read of the tape, not a source of picks.
"""
from __future__ import annotations

import pandas as pd

from .db import get_analytics_conn
from .memory import load_ticker_meta
from .regime import compute_regime


def compute_pulse(as_of: str | None = None) -> dict:
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        prev = a.execute("SELECT max(date) FROM bars WHERE date < ?", [as_of]).fetchone()[0]
        df = a.execute("""
            SELECT cur.ticker, cur.universe, cur.close, cur.rsi_14, cur.prebreak_v3,
                   cur.close / NULLIF(prv.close, 0) - 1 AS day_ret
            FROM (SELECT ticker, universe, close, rsi_14, prebreak_v3 FROM bars WHERE date = ?) cur
            LEFT JOIN (SELECT ticker, universe, close FROM bars WHERE date = ?) prv
              ON cur.ticker = prv.ticker AND cur.universe = prv.universe
        """, [as_of, str(prev)[:10]]).fetchdf()
    finally:
        a.close()

    df = df.drop_duplicates(subset=["ticker"])               # ticker may sit in >1 universe
    meta = load_ticker_meta()
    df["sector"] = df["ticker"].map(lambda t: (meta.get(t) or {}).get("sector") or "")
    df["mcap"]   = df["ticker"].map(lambda t: (meta.get(t) or {}).get("mcap_bucket") or "unknown")
    df["day_ret_pct"] = df["day_ret"] * 100

    # ── Sector heat ──────────────────────────────────────────────────────────
    sec = df[df["sector"] != ""]
    sectors = []
    for name, g in sec.groupby("sector"):
        rets = g["day_ret_pct"].dropna()
        sectors.append({
            "sector": name, "n": int(len(g)),
            "avg_chg": round(float(rets.mean()), 2) if len(rets) else None,
            "pct_up": round(float((rets > 0).mean() * 100), 1) if len(rets) else None,
            "setup_density": round(float((g["prebreak_v3"] >= 25).mean() * 100), 1),
            "med_rsi": round(float(g["rsi_14"].median()), 1) if g["rsi_14"].notna().any() else None,
        })
    sectors.sort(key=lambda s: (s["avg_chg"] is not None, s["avg_chg"] or -999), reverse=True)

    # ── Movers (liquid-ish: exclude micro to avoid penny noise dominating) ────
    liq = df[(df["mcap"].isin(["mid", "large", "mega", "small"])) & df["day_ret_pct"].notna()]
    def _mv(d):
        return [{"ticker": r["ticker"], "chg": round(r["day_ret_pct"], 2),
                 "sector": r["sector"] or "?", "mcap": r["mcap"], "price": round(float(r["close"]), 2)}
                for _, r in d.iterrows()]
    gainers = _mv(liq.nlargest(12, "day_ret_pct"))
    losers  = _mv(liq.nsmallest(12, "day_ret_pct"))

    mcap_dist = (df["mcap"].value_counts().to_dict())

    return {
        "as_of": as_of, "prev": str(prev)[:10] if prev else None,
        "regime": compute_regime(as_of),
        "sectors": sectors,
        "gainers": gainers, "losers": losers,
        "mcap_dist": {k: int(v) for k, v in mcap_dist.items()},
    }


if __name__ == "__main__":
    import json
    p = compute_pulse()
    print("regime:", p["regime"]["label"], p["regime"]["score"])
    print("sectors:")
    for s in p["sectors"]:
        print(f"  {s['sector']:24} n={s['n']:5} avg={s['avg_chg']}% up={s['pct_up']}% setup={s['setup_density']}% rsi={s['med_rsi']}")
    print("top gainers:", [(g["ticker"], g["chg"]) for g in p["gainers"][:5]])
