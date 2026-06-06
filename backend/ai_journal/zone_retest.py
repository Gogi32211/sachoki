"""
ai_journal/zone_retest.py — high-volume zone re-test filter.

Concept (Wyckoff / Volume Profile): a bar with vol ≥ 10× avg_vol_20d marks an
institutional footprint. The bar's [low, high] is a "decision zone". If price
LEAVES the zone (closes above zone_high) and then RETURNS to it, that's a true
re-test — a decision point where the previous battle may be re-fought.

Live filter — for each ticker, on the latest bar:
  1. Find recent bullish high-volume triggers (20-60 days back, close > open,
     vol ≥ 10 × avg_vol_20d).
  2. For each trigger zone, require at least one intermediate bar's close above
     zone_high (the price LEFT the zone upward).
  3. Require today's close ∈ [zone_low, zone_high] (the price RETURNED).

Edge validation: same logic backtested historically — compute forward P&L from
the day of re-entry. Surfaced honestly alongside the filter.
"""
from __future__ import annotations

import logging

from .db import get_analytics_conn, get_journal_conn

log = logging.getLogger(__name__)


# Filter parameters — kept explicit so the UI can show them.
TRIGGER_VOL_MULT = 10
TRIGGER_LOOKBACK_MIN = 8       # days ago (most recent trigger eligible)
TRIGGER_LOOKBACK_MAX = 90      # days ago (oldest trigger eligible)


def _zone_retest_sql(as_of: str, lb_min: int, lb_max: int,
                     vol_min: float, vol_max: float | None = None) -> str:
    """The core re-test query. One pass; window-based.
    vol_min/vol_max: volume/avg_vol_20d band [vol_min, vol_max) (vol_max=None → open-ended).

    Triggers include both bullish AND bearish high-volume bars (the institutional
    footprint matters regardless of direction). Bullish bar → accumulation-style
    re-test from below; bearish bar → distribution-style re-test from above.
    `direction` is surfaced in the result.

    Returns (ticker, universe, trigger_date, zone_low, zone_high, trigger_close,
             direction, ..., left_date, current_close) for every active re-test
    today (cur close back inside zone after at least one bar closed OUTSIDE it)."""
    upper = f" AND volume < {vol_max} * avg_vol_20d" if vol_max else ""
    return f"""
        WITH triggers AS (
            SELECT ticker, universe, date AS trigger_date,
                   low  AS zone_low, high AS zone_high, close AS trigger_close,
                   CASE WHEN close > open THEN 'bull' ELSE 'bear' END AS direction,
                   volume, avg_vol_20d
            FROM bars
            WHERE date BETWEEN (DATE '{as_of}' - INTERVAL {lb_max} DAY)
                           AND (DATE '{as_of}' - INTERVAL {lb_min} DAY)
              AND avg_vol_20d > 0
              AND volume >= {vol_min} * avg_vol_20d{upper}
              AND high > low
        ),
        current_state AS (
            SELECT ticker, universe, date AS current_date,
                   close AS current_close, atr_14
            FROM bars WHERE date = DATE '{as_of}'
        ),
        -- For every (trigger, candidate) pair: did the price leave the zone
        -- (any direction) and is it back inside now?
        with_exit AS (
            SELECT t.*, cs.current_close, cs.atr_14, cs.current_date,
                   MAX(CASE WHEN b.close NOT BETWEEN t.zone_low AND t.zone_high
                            THEN b.date END) AS left_date
            FROM triggers t
            JOIN current_state cs USING (ticker, universe)
            JOIN bars b ON b.ticker = t.ticker AND b.universe = t.universe
                       AND b.date  > t.trigger_date AND b.date < cs.current_date
            GROUP BY t.ticker, t.universe, t.trigger_date, t.zone_low, t.zone_high,
                     t.trigger_close, t.direction, t.volume, t.avg_vol_20d,
                     cs.current_close, cs.atr_14, cs.current_date
        )
        SELECT ticker, universe, trigger_date, zone_low, zone_high, trigger_close,
               direction, volume, avg_vol_20d, current_close, current_date, atr_14,
               left_date, (current_close - zone_low) / (zone_high - zone_low) AS pct_in_zone
        FROM with_exit
        WHERE left_date IS NOT NULL
          AND current_close BETWEEN zone_low AND zone_high
        ORDER BY ticker, trigger_date DESC
    """


def zones_for_ticker(ticker: str, as_of: str | None = None,
                     lb_min: int = TRIGGER_LOOKBACK_MIN,
                     lb_max: int = TRIGGER_LOOKBACK_MAX,
                     vol_min: float = 2.0,
                     vol_max: float | None = None) -> list[dict]:
    """All currently-active zones for ONE ticker (keep all triggers, sorted
    most-recent first). For chart overlay — default vol_min=2 shows every tier."""
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        sql = _zone_retest_sql(as_of, lb_min, lb_max, vol_min, vol_max) + " "
        df = a.execute(sql).fetchdf()
    finally:
        a.close()
    df = df[df["ticker"] == ticker.upper()].sort_values("trigger_date", ascending=False)
    # A ticker can sit in >1 universe → same zone reported twice. De-dup.
    df = df.drop_duplicates(subset=["trigger_date", "zone_low", "zone_high"], keep="first")
    out = []
    for _, r in df.iterrows():
        out.append({
            "trigger_date":  str(r["trigger_date"])[:10],
            "left_date":     str(r["left_date"])[:10] if r["left_date"] else None,
            "zone_low":      round(float(r["zone_low"]), 4),
            "zone_high":     round(float(r["zone_high"]), 4),
            "trigger_close": round(float(r["trigger_close"]), 4),
            "trigger_vol_mult": round(float(r["volume"]) / float(r["avg_vol_20d"]), 1),
            "direction":     str(r["direction"]),
            "current_close": round(float(r["current_close"]), 4),
        })
    return out


def active_retests(as_of: str | None = None,
                   lb_min: int = TRIGGER_LOOKBACK_MIN,
                   lb_max: int = TRIGGER_LOOKBACK_MAX,
                   vol_min: float = TRIGGER_VOL_MULT,
                   vol_max: float | None = None,
                   limit: int = 200,
                   dedupe_per_ticker: bool = True) -> dict:
    """Tickers currently re-testing a recent high-volume zone.
    dedupe_per_ticker=True keeps only the most-recent trigger per ticker;
    set False (used by the sidebar scan) to surface every zone."""
    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(_zone_retest_sql(as_of, lb_min, lb_max, vol_min, vol_max)).fetchdf()
    finally:
        a.close()
    # Drop true cross-universe duplicates (same (ticker, trigger_date, zone)).
    df = df.sort_values(["ticker", "trigger_date"], ascending=[True, False])
    df = df.drop_duplicates(subset=["ticker", "trigger_date", "zone_low", "zone_high"], keep="first")
    if dedupe_per_ticker:
        df = df.drop_duplicates(subset=["ticker"], keep="first")
    df = df.head(limit)

    # Enrich with ticker_meta
    j = get_journal_conn()
    try:
        meta = {t: {"sector": s, "mcap_bucket": b, "name": n}
                for t, n, s, b in j.execute(
                    "SELECT ticker, name, sector, mcap_bucket FROM ticker_meta").fetchall()}
    finally:
        j.close()

    rows = []
    for _, r in df.iterrows():
        m = meta.get(r["ticker"], {})
        rows.append({
            "ticker":         r["ticker"],
            "universe":       r["universe"],
            "name":           m.get("name") or "",
            "sector":         m.get("sector") or "",
            "mcap_bucket":    m.get("mcap_bucket") or "unknown",
            "trigger_date":   str(r["trigger_date"])[:10],
            "left_date":      str(r["left_date"])[:10] if r["left_date"] else None,
            "zone_low":       round(float(r["zone_low"]), 2),
            "zone_high":      round(float(r["zone_high"]), 2),
            "trigger_close":  round(float(r["trigger_close"]), 2),
            "trigger_vol_mult": round(float(r["volume"]) / float(r["avg_vol_20d"]), 1),
            "direction":      str(r["direction"]),
            "current_close":  round(float(r["current_close"]), 2),
            "pct_in_zone":    round(float(r["pct_in_zone"]) * 100, 1),  # 0=low, 100=high
            "atr_14":         round(float(r["atr_14"] or 0), 2),
        })
    return {
        "as_of": as_of, "count": len(rows),
        "params": {"lookback_min": lb_min, "lookback_max": lb_max,
                   "vol_min": vol_min, "vol_max": vol_max},
        "rows": rows,
    }


# ── Edge validation (one historical SQL pass) ────────────────────────────────

def edge_check(horizon: int = 10) -> dict:
    """Forward P&L edge of the re-entry event itself (across all history).

    Question: when the re-entry happens on date D, what is the forward return
    from close(D) to close(D+H)? Compared to baseline forward return on the
    same population. ONE SQL pass — fast, honest summary, no Bonferroni."""
    import math
    s = round(2.0 * math.sqrt(horizon / 5), 3)
    t = round(5.0 * math.sqrt(horizon / 5), 3)
    clipped = f"greatest(-{s}, least({t}, fwd_{horizon}d))"
    sql = f"""
        WITH triggers AS (
            SELECT ticker, universe, date AS trigger_date,
                   low AS zone_low, high AS zone_high
            FROM bars
            WHERE close > open AND avg_vol_20d > 0
              AND volume >= {TRIGGER_VOL_MULT} * avg_vol_20d
        ),
        events AS (
            SELECT b.ticker, b.universe, b.date AS event_date,
                   b.fwd_{horizon}d, {clipped} AS clipped_ret
            FROM bars b
            JOIN triggers t ON b.ticker = t.ticker AND b.universe = t.universe
            -- the trigger sits between {TRIGGER_LOOKBACK_MIN}..{TRIGGER_LOOKBACK_MAX} bars before b
            WHERE b.date BETWEEN (t.trigger_date + INTERVAL {TRIGGER_LOOKBACK_MIN} DAY)
                              AND (t.trigger_date + INTERVAL {TRIGGER_LOOKBACK_MAX} DAY)
              AND b.close BETWEEN t.zone_low AND t.zone_high                -- in zone now
              AND EXISTS (                                                   -- and DID leave
                  SELECT 1 FROM bars b2
                  WHERE b2.ticker = b.ticker AND b2.universe = b.universe
                    AND b2.date > t.trigger_date AND b2.date < b.date
                    AND b2.close > t.zone_high
              )
              AND b.fwd_{horizon}d IS NOT NULL
              AND b.fwd_{horizon}d BETWEEN -90 AND 500
        ),
        base AS (
            SELECT avg({clipped}) base_avg, avg(CASE WHEN {clipped}>0 THEN 1.0 ELSE 0 END) base_win,
                   count(*) base_n
            FROM bars WHERE fwd_{horizon}d IS NOT NULL AND fwd_{horizon}d BETWEEN -90 AND 500
        )
        SELECT
            (SELECT count(*) FROM events) AS n,
            (SELECT avg(clipped_ret) FROM events) AS avg_clip,
            (SELECT avg(CASE WHEN clipped_ret>0 THEN 1.0 ELSE 0 END) FROM events) AS win,
            (SELECT base_avg FROM base) AS base_avg,
            (SELECT base_win FROM base) AS base_win
    """
    a = get_analytics_conn()
    try:
        r = a.execute(sql).fetchone()
    finally:
        a.close()
    n, avg, win, ba, bw = r
    return {
        "horizon": horizon,
        "n_events": int(n or 0),
        "avg_clip_pct": round(float(avg or 0), 3),
        "win_rate_pct": round(float(win or 0) * 100, 1),
        "base_avg_clip_pct": round(float(ba or 0), 3),
        "base_win_rate_pct": round(float(bw or 0) * 100, 1),
        "edge_avg_pct": round(float(avg or 0) - float(ba or 0), 3),
        "edge_win_pp": round((float(win or 0) - float(bw or 0)) * 100, 1),
        "stop_target_pct": f"-{s}% / +{t}%",
    }


def classify_bar(bar_low: float, bar_high: float, bar_close: float,
                 zone_low: float, zone_high: float) -> str:
    """Single-bar relationship to a zone:
        inside       — bar fully within [zone_low, zone_high]
        cross        — bar spans the whole zone (low < zone_low AND high > zone_high)
        touch_below  — bar pokes the zone from BELOW (low < zone_low ≤ high < zone_high)
        touch_above  — bar pokes the zone from ABOVE (zone_low < low ≤ zone_high < high)
        below        — entire bar below zone   (high < zone_low)
        above        — entire bar above zone   (low > zone_high)
    """
    if bar_low >= zone_low and bar_high <= zone_high:
        return "inside"
    if bar_low < zone_low and bar_high > zone_high:
        return "cross"
    if bar_high < zone_low:
        return "below"
    if bar_low > zone_high:
        return "above"
    if bar_low < zone_low <= bar_high <= zone_high:
        return "touch_below"
    if zone_low <= bar_low <= zone_high < bar_high:
        return "touch_above"
    return "inside"   # fallback (overlap edge cases)


def classify_recent_bars(ticker: str, universe: str, zone_low: float, zone_high: float,
                         since_date: str, until_date: str) -> list[dict]:
    """For each daily bar in (since_date, until_date], compute its relationship
    to the given zone. since_date is typically the trigger_date."""
    a = get_analytics_conn()
    try:
        rows = a.execute("""
            SELECT date, open, high, low, close, volume
            FROM bars WHERE ticker=? AND universe=? AND date > ? AND date <= ?
            ORDER BY date ASC""",
            [ticker, universe, since_date, until_date]).fetchall()
    finally:
        a.close()
    out = []
    for d, o, h, l, c, v in rows:
        rel = classify_bar(float(l or 0), float(h or 0), float(c or 0), zone_low, zone_high)
        out.append({"date": str(d)[:10], "open": float(o), "high": float(h),
                    "low": float(l), "close": float(c), "volume": int(v or 0),
                    "rel": rel})
    return out


def scan() -> dict:
    """Full list of tickers × active zones — sidebar for the HV-Zones page.
    Returns ONE row per (ticker, zone) so a ticker with multiple HV-spikes
    appears multiple times. Sidebar can group / count or show inline."""
    res = active_retests(vol_min=2.0, limit=10000, dedupe_per_ticker=False)
    # Count zones per ticker first so the row can carry that summary.
    from collections import Counter
    zone_counts = Counter(r["ticker"] for r in res["rows"])
    rows = []
    for r in res["rows"]:
        rel = classify_bar(r["current_close"] - 1e-9, r["current_close"] + 1e-9,
                           r["current_close"], r["zone_low"], r["zone_high"])
        vm = r["trigger_vol_mult"]
        tier = "x10+" if vm >= 10 else "x5-10" if vm >= 5 else "x2-5"
        rows.append({**r, "current_rel": rel, "tier": tier,
                     "n_zones": zone_counts[r["ticker"]]})
    return {"as_of": res["as_of"], "count": len(rows),
            "n_tickers": len(zone_counts), "rows": rows}


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print("=== edge check (H=10) ===")
    print(json.dumps(edge_check(10), indent=2))
    print("=== edge check (H=5) ===")
    print(json.dumps(edge_check(5), indent=2))
    print("\n=== active re-tests today ===")
    res = active_retests()
    print(f"count: {res['count']}, as_of: {res['as_of']}")
    for r in res["rows"][:8]:
        print(f"  {r['ticker']:6} {r['mcap_bucket']:7} zone=[{r['zone_low']}..{r['zone_high']}] "
              f"now={r['current_close']} pct={r['pct_in_zone']}% trig={r['trigger_date']} "
              f"vol×{r['trigger_vol_mult']}")
