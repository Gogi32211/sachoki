"""
ai_journal/gann_zones.py — Gann's "highest bar" and "lowest bar" zones.

W.D. Gann: "the most important part of a chart is the lowest stick of the
highest bar, and the highest stick of the lowest bar". For a lookback window:

  TOP zone = [low, high] of the bar with the HIGHEST high in the window
             (the "ceiling" — where the previous push topped out, and from
              what level it started its final move).
  BOTTOM zone = [low, high] of the bar with the LOWEST low in the window
             (the "floor" — same idea inverted).

Both are magnet-like S/R zones in Gann's framework. A "Gann re-test" filter
flags tickers whose CURRENT close sits inside one of these zones.

Same per-bar classification as HV-Zones (inside / cross / touch_below /
touch_above / above / below) — for the marker overlay.
"""
from __future__ import annotations

import logging

from .db import get_analytics_conn, get_journal_conn
from .zone_retest import classify_bar, classify_recent_bars

log = logging.getLogger(__name__)

DEFAULT_LOOKBACK = 90      # days back to find highest/lowest bar


def _zones_sql(as_of: str, lookback: int) -> str:
    """Per (ticker, universe) — find the highest-high bar and the lowest-low bar
    in the lookback window, return their [low, high] as top/bottom zones."""
    return f"""
        WITH win AS (
            SELECT ticker, universe, date, low, high, close,
                   row_number() OVER (PARTITION BY ticker, universe ORDER BY high DESC, date ASC) AS top_rk,
                   row_number() OVER (PARTITION BY ticker, universe ORDER BY low  ASC, date ASC) AS bot_rk
            FROM bars
            WHERE date BETWEEN (DATE '{as_of}' - INTERVAL {lookback} DAY)
                           AND (DATE '{as_of}' - INTERVAL 1 DAY)
              AND high > low
        ),
        cur AS (
            SELECT ticker, universe, close AS current_close
            FROM bars WHERE date = DATE '{as_of}'
        ),
        agg AS (
            SELECT w.ticker, w.universe,
                   MAX(CASE WHEN top_rk = 1 THEN date END)::DATE AS top_date,
                   MAX(CASE WHEN top_rk = 1 THEN low  END)       AS top_low,
                   MAX(CASE WHEN top_rk = 1 THEN high END)       AS top_high,
                   MAX(CASE WHEN bot_rk = 1 THEN date END)::DATE AS bot_date,
                   MAX(CASE WHEN bot_rk = 1 THEN low  END)       AS bot_low,
                   MAX(CASE WHEN bot_rk = 1 THEN high END)       AS bot_high
            FROM win w GROUP BY w.ticker, w.universe
        )
        SELECT a.ticker, a.universe,
               a.top_date, a.top_low, a.top_high,
               a.bot_date, a.bot_low, a.bot_high,
               c.current_close
        FROM agg a JOIN cur c USING (ticker, universe)
        WHERE a.top_low IS NOT NULL AND a.bot_high IS NOT NULL
    """


def zones_for_ticker(ticker: str, as_of: str | None = None,
                     lookback: int = DEFAULT_LOOKBACK) -> list[dict]:
    """Returns 2 zones for one ticker: [top_zone, bottom_zone]. Each zone has
    the same shape as HV-Zones (so the chart code can render it the same way)."""
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(_zones_sql(as_of, lookback)).fetchdf()
    finally:
        a.close()
    df = df[df["ticker"] == ticker.upper()]
    if df.empty:
        return []
    r = df.iloc[0]
    universe = str(r["universe"])
    zones = []
    # TOP zone
    if r["top_low"] is not None and r["top_high"] is not None:
        zones.append({
            "kind": "top",
            "trigger_date": str(r["top_date"])[:10],
            "zone_low":  round(float(r["top_low"]), 4),
            "zone_high": round(float(r["top_high"]), 4),
            "current_close": round(float(r["current_close"]), 4),
        })
    # BOTTOM zone
    if r["bot_low"] is not None and r["bot_high"] is not None:
        zones.append({
            "kind": "bottom",
            "trigger_date": str(r["bot_date"])[:10],
            "zone_low":  round(float(r["bot_low"]), 4),
            "zone_high": round(float(r["bot_high"]), 4),
            "current_close": round(float(r["current_close"]), 4),
        })
    # Attach classifications (re-use HV classifier — it's identical logic).
    for z in zones:
        z["bar_classifications"] = classify_recent_bars(
            ticker.upper(), universe, z["zone_low"], z["zone_high"],
            since_date=z["trigger_date"], until_date=as_of)
    return zones


def active_tickers(as_of: str | None = None, lookback: int = DEFAULT_LOOKBACK,
                   zone_kind: str = "any") -> dict:
    """Tickers whose current close is INSIDE top zone, bottom zone, or either
    (default). Used by the Ultra filter chip."""
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(_zones_sql(as_of, lookback)).fetchdf()
    finally:
        a.close()
    in_top = (df["current_close"] >= df["top_low"]) & (df["current_close"] <= df["top_high"])
    in_bot = (df["current_close"] >= df["bot_low"]) & (df["current_close"] <= df["bot_high"])
    if zone_kind == "top":
        mask = in_top
    elif zone_kind == "bottom":
        mask = in_bot
    else:
        mask = in_top | in_bot
    tickers = sorted(df[mask]["ticker"].unique().tolist())
    return {"as_of": as_of, "lookback": lookback, "zone_kind": zone_kind,
            "tickers": tickers, "count": len(tickers)}


def history_pivots(ticker: str, pivot: int = 5,
                   from_date: str | None = None, limit: int = 500) -> dict:
    """Pivot highs and lows over the visible chart range (Gann historical
    overlay). A pivot HIGH is a bar whose high is the max over [-pivot, +pivot]
    window centred on it; a pivot LOW is its min. These are the "highest
    sticks" and "lowest sticks" of local swings.

    The last `pivot` days can never confirm a pivot (no future bars yet) — that
    is the inherent cost of confirmed swings."""
    a = get_analytics_conn()
    try:
        params = [ticker.upper()]
        date_where = ""
        if from_date:
            date_where = " AND date >= ?"
            params.append(from_date)
        params.append(limit)
        rows = a.execute(f"""
            WITH ranked AS (
                SELECT date, low, high, open, close, universe,
                  MAX(high) OVER (
                    PARTITION BY universe ORDER BY date
                    ROWS BETWEEN {pivot} PRECEDING AND {pivot} FOLLOWING) AS roll_max_h,
                  MIN(low) OVER (
                    PARTITION BY universe ORDER BY date
                    ROWS BETWEEN {pivot} PRECEDING AND {pivot} FOLLOWING) AS roll_min_l
                FROM bars WHERE ticker = ?{date_where}
            )
            SELECT date, low, high, open, close, universe,
                   (high = roll_max_h) AS is_top,
                   (low  = roll_min_l) AS is_bot
            FROM ranked
            WHERE high = roll_max_h OR low = roll_min_l
            ORDER BY date DESC LIMIT ?
        """, params).fetchall()
    finally:
        a.close()
    seen = set()
    zones = []
    for d, lo, hi, o, c, _u, is_top, is_bot in rows:
        key = (str(d)[:10], round(float(lo), 4), round(float(hi), 4))
        if key in seen: continue
        seen.add(key)
        direction = "bull" if (c or 0) > (o or 0) else "bear"
        kind = "top" if is_top else "bottom"
        zones.append({
            "trigger_date": str(d)[:10],
            "zone_low":     round(float(lo), 4),
            "zone_high":    round(float(hi), 4),
            "direction":    direction,
            "kind":         kind,
        })
    zones.sort(key=lambda z: z["trigger_date"])
    return {"ticker": ticker.upper(), "pivot": pivot,
            "from_date": from_date, "count": len(zones), "zones": zones}


def scan(as_of: str | None = None, lookback: int = DEFAULT_LOOKBACK) -> dict:
    """List view: every ticker with its top/bottom zones + current relation.
    Sidebar of the Gann-Zones page."""
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(_zones_sql(as_of, lookback)).fetchdf()
    finally:
        a.close()
    rows = []
    for _, r in df.iterrows():
        cc = float(r["current_close"])
        # Per-zone relation flag from current price
        def _rel(lo, hi):
            if cc < lo: return "below"
            if cc > hi: return "above"
            return "inside"
        rel_top = _rel(float(r["top_low"]), float(r["top_high"]))
        rel_bot = _rel(float(r["bot_low"]), float(r["bot_high"]))
        # Only surface tickers currently INSIDE either zone (otherwise list is huge)
        if rel_top != "inside" and rel_bot != "inside":
            continue
        rows.append({
            "ticker": r["ticker"], "universe": r["universe"],
            "current_close": round(cc, 4),
            "top_date":  str(r["top_date"])[:10],
            "top_low":   round(float(r["top_low"]),  4),
            "top_high":  round(float(r["top_high"]), 4),
            "bot_date":  str(r["bot_date"])[:10],
            "bot_low":   round(float(r["bot_low"]),  4),
            "bot_high":  round(float(r["bot_high"]), 4),
            "current_rel_top": rel_top,
            "current_rel_bot": rel_bot,
        })
    return {"as_of": as_of, "lookback": lookback, "count": len(rows), "rows": rows}


if __name__ == "__main__":
    import json, sys
    lb = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LOOKBACK
    print("=== scan (top 5) ===")
    s = scan(lookback=lb)
    print(f"as_of={s['as_of']} lookback={lb}d count={s['count']}")
    for r in s["rows"][:5]:
        print(f"  {r['ticker']:6} close=${r['current_close']} "
              f"top=[{r['top_low']}..{r['top_high']}] ({r['top_date']}, {r['current_rel_top']}) "
              f"bot=[{r['bot_low']}..{r['bot_high']}] ({r['bot_date']}, {r['current_rel_bot']})")
    print("\n=== zones_for_ticker AAPL ===")
    print(json.dumps(zones_for_ticker("AAPL", lookback=lb), indent=2, default=str)[:600])
