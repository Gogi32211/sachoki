"""
ai_journal/zone_events.py — zone EXIT vs RETEST event analytics.

Builds a labelled event log over ALL history and measures the forward edge of
each event type, optionally conditioned on the event-bar's signal / pattern /
description context (re-using the rich `bars` feature columns).

Event model (per HV zone = [low, high] of a vol-spike bar):
    exit_up    — close first crosses ABOVE zone_high   (breakout)
    exit_down  — close first crosses BELOW zone_low     (breakdown)
    retest     — close RE-ENTERS the zone after leaving (return to S/R)

For each event we attach the bar's forward returns (fwd_5/10/20d, mfe/mae) and a
curated set of context features. Aggregation = honest edge vs the full-population
baseline (same clipped-return convention as zone_retest.edge_check).

NOTE: this is research analytics, not a live signal. It tells us WHERE the edge
is (which event × which context) so a tradeable rule can be designed next.
"""
from __future__ import annotations

import logging
import math

from .db import get_analytics_conn

log = logging.getLogger(__name__)

# Curated context features attached to every event bar. Booleans are SMALLINT
# 0/1 in the DB; categoricals are strings. Kept small & meaningful on purpose —
# the point is interpretable lift, not a 394-column dump.
_BOOL_CTX = [
    # T/Z timing — "already going up" at the event bar
    "tz_bull", "sig_t2g", "sig_t1g", "sig_tz3", "sig_buy",
    # structure / absorption / pattern
    "wyc_spring", "wyc_sos", "wyc_in_tr",
    "d_absorb_bull", "d_absorb_bear", "d_div_bull", "d_strong_bull",
    "sig_abs", "vbo_up", "eb_bull", "fbo_bull", "prebreak_prime", "pb_lvbo",
    "l34", "be_up",
]
# Bar-description categoricals (low cardinality) — the SHAPE of the event bar.
# The atomic suffixes (wick/close/penetration/ne) are the building blocks the
# combo search recombines; bar_line5 is the price-action code.
_CAT_CTX = ["vol_bucket", "bar_body_wick", "wick_suffix", "close_suffix",
            "penetration_suffix", "ne_suffix", "bar_gap_class", "bar_range_class",
            "bar_line5"]
# Derived booleans: tz_up_next3 (T/Z flip in the 3 bars after) and at_fib (event
# close within 0.5×ATR of a Fibonacci level of the TRAILING range — confluence).
_DERIVED_CTX = ["tz_up_next3", "at_fib"]
# Derived categorical: which Fib level the event sits on (None when not at one).
_DERIVED_CAT = ["fib_level"]


def _events_sql(vol_min: float, lb_max: int, ticker: str | None = None) -> str:
    bool_cols = ", ".join(f"b.{c}" for c in _BOOL_CTX)
    cat_cols = ", ".join(f"b.{c}" for c in _CAT_CTX)
    tk_filter = ""
    if ticker:
        tk = "".join(c for c in ticker.upper() if c.isalnum() or c in ".-")
        tk_filter = f" AND ticker = '{tk}'"
    return f"""
        WITH zones AS (
            SELECT ticker, universe, date AS z_date,
                   low AS z_low, high AS z_high, atr_14 AS z_atr,
                   volume / avg_vol_20d AS z_mult,
                   CASE WHEN close > open THEN 'bull' ELSE 'bear' END AS z_dir
            FROM bars
            WHERE avg_vol_20d > 0 AND volume >= {vol_min} * avg_vol_20d AND high > low{tk_filter}
        ),
        fwd AS (
            SELECT z.ticker, z.universe, z.z_date, z.z_low, z.z_high, z.z_atr,
                   z.z_mult, z.z_dir, b.date AS e_date, b.tz_bull AS tzb,
                   datediff('day', z.z_date, b.date) AS age_days,
                   (b.close > z.z_high)                      AS above,
                   (b.close < z.z_low)                       AS below,
                   (b.close BETWEEN z.z_low AND z.z_high)    AS inside
            FROM zones z
            JOIN bars b ON b.ticker = z.ticker AND b.universe = z.universe
                       AND b.date > z.z_date
                       AND b.date <= z.z_date + INTERVAL {lb_max} DAY
        ),
        seq AS (
            SELECT *,
                   coalesce(lag(above)  OVER w, FALSE) AS p_above,
                   coalesce(lag(below)  OVER w, FALSE) AS p_below,
                   -- T/Z follow-through: tz_bull turns on in the next 3 bars (window,
                   -- not a per-row subquery — keeps the whole scan one pass).
                   coalesce(max(tzb) OVER (PARTITION BY ticker, universe, z_date
                       ORDER BY e_date ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING), 0) AS tz_up_next3
            FROM fwd
            WINDOW w AS (PARTITION BY ticker, universe, z_date ORDER BY e_date)
        ),
        events AS (
            SELECT *,
                   CASE
                     WHEN above AND NOT p_above THEN 'exit_up'
                     WHEN below AND NOT p_below THEN 'exit_down'
                     WHEN inside AND (p_above OR p_below) THEN 'retest'
                   END AS event_type
            FROM seq
        ),
        ranked AS (
            SELECT *,
                   row_number() OVER (PARTITION BY ticker, universe, z_date, event_type
                                      ORDER BY e_date) AS ev_seq
            FROM events
            WHERE event_type IS NOT NULL
        ),
        -- TRAILING extremes known at each bar (expanding low/high so far) — the
        -- Fib anchors, with NO look-ahead. Data spans ~5y so this ≈ 5y low/high.
        bx AS (
            SELECT ticker, universe, date,
                   min(low)  OVER w AS t_lo,
                   max(high) OVER w AS t_hi
            FROM bars{tk_filter}
            WINDOW w AS (PARTITION BY ticker, universe ORDER BY date ROWS UNBOUNDED PRECEDING)
        )
        SELECT e.ticker, e.universe, e.z_date, e.z_low, e.z_high, e.z_mult, e.z_dir,
               e.e_date, e.event_type, e.age_days, e.ev_seq,
               (e.z_high - e.z_low) / NULLIF(e.z_atr, 0) AS width_atr,
               b.fwd_5d, b.fwd_10d, b.fwd_20d, b.mfe_10d, b.mae_10d,
               b.close AS e_close, b.atr_14 AS e_atr, x.t_lo, x.t_hi,
               b.t_sig, b.l_sig, b.full_suffix,    -- extra slots for the pattern builder
               -- genuine FLIP: not bullish at the event bar, turns bullish within 3 bars
               CASE WHEN coalesce(e.tzb, 0) = 0 AND e.tz_up_next3 = 1 THEN 1 ELSE 0 END AS tz_up_next3,
               {cat_cols}, {bool_cols}
        FROM ranked e
        JOIN bars b ON b.ticker = e.ticker AND b.universe = e.universe AND b.date = e.e_date
        JOIN bx   x ON x.ticker = e.ticker AND x.universe = e.universe AND x.date = e.e_date
    """


import threading as _threading
_EVENTS_CACHE: dict = {}
_EVENTS_LOCK = _threading.Lock()


def _load_events(vol_min: float, lb_max: int, ticker: str | None = None):
    """Build (or reuse) the labelled event log. Cached + serialised so the panel's
    several concurrent endpoints share ONE scan instead of recomputing it each."""
    key = (float(vol_min), int(lb_max), ticker)
    with _EVENTS_LOCK:                       # first caller computes; the rest hit cache
        if key in _EVENTS_CACHE:
            return _EVENTS_CACHE[key]
        a = get_analytics_conn()
        try:
            df = a.execute(_events_sql(vol_min, lb_max, ticker=ticker)).fetchdf()
        finally:
            a.close()
        if not df.empty:
            # A ticker dual-listed across universes yields the same event twice.
            df = df.drop_duplicates(subset=["ticker", "z_date", "z_low", "z_high", "e_date", "event_type"])
            _add_fib(df)
            if "bar_gap_class" in df.columns and "bar_range_class" in df.columns:
                df["gap_rng"] = (df["bar_gap_class"].fillna("").astype(str) + "-"
                                 + df["bar_range_class"].fillna("").astype(str)).replace("-", None)
        _EVENTS_CACHE[key] = df
        if len(_EVENTS_CACHE) > 8:
            _EVENTS_CACHE.pop(next(iter(_EVENTS_CACHE)))
        return df


# Pattern-builder slots → the column each maps to (the user's full bar-code).
PATTERN_SLOTS = {
    "tz":     "t_sig",         # T2G, T1 …
    "l":      "l_sig",         # L34, L46 …
    "suffix": "full_suffix",   # EU, ED …
    "bodywk": "bar_body_wick", # STB, M …
    "gaprng": "gap_rng",       # G1-C …
    "l5":     "bar_line5",     # PS-R2X …
    "vol":    "vol_bucket",    # VB, W …
}


_FIB_RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
_FIB_LABELS = ["0", "0.236", "0.382", "0.5", "0.618", "0.786", "1"]


def _add_fib(df, tol_atr: float = 0.5):
    """Add at_fib (close within tol_atr×ATR of any Fib level of the TRAILING
    range) and fib_level (the nearest level's label, or None). Anchors are the
    expanding low/high (no look-ahead). Ratio measured from the trailing low."""
    import numpy as np
    if df.empty or "t_lo" not in df.columns:
        df["at_fib"] = 0
        df["fib_level"] = None
        return df
    lo = df["t_lo"].to_numpy(dtype=float)
    hi = df["t_hi"].to_numpy(dtype=float)
    close = df["e_close"].to_numpy(dtype=float)
    atr = df["e_atr"].to_numpy(dtype=float)
    rng = hi - lo
    ratios = np.array(_FIB_RATIOS)
    levels = lo[:, None] + ratios[None, :] * rng[:, None]    # n × 7 prices
    dist = np.abs(close[:, None] - levels)
    near_idx = dist.argmin(axis=1)
    near_dist = dist[np.arange(len(df)), near_idx]
    tol = tol_atr * atr
    at = (near_dist <= tol) & (rng > 0) & np.isfinite(atr) & (atr > 0)
    df["at_fib"] = at.astype(int)
    labels = np.array(_FIB_LABELS)
    df["fib_level"] = np.where(at, labels[near_idx], None)
    return df


_OOS_FROM = "2025-01-01"   # events on/after this date = out-of-sample test set


def _split_stats(sub, col, oos_from: str = _OOS_FROM):
    """In-sample vs out-of-sample win-rate for a df subset (split by event date)."""
    if sub is None or not len(sub):
        return {"is": {"n": 0, "win_rate_pct": None}, "oos": {"n": 0, "win_rate_pct": None}}
    ed = sub["e_date"].astype(str)

    def _stat(d):
        c = d[col].dropna()
        c = c[c.between(-90, 500)]
        return {"n": int(len(d)), "win_rate_pct": round(float((c > 0).mean()) * 100, 1) if len(c) else None}
    return {"is": _stat(sub[ed < oos_from]), "oos": _stat(sub[ed >= oos_from]), "oos_from": oos_from}


def _clip_bounds(horizon: int):
    s = round(2.0 * math.sqrt(horizon / 5), 3)
    t = round(5.0 * math.sqrt(horizon / 5), 3)
    return s, t


def _baseline(horizon: int):
    """Full-population forward-return baseline (clipped), for honest comparison."""
    s, t = _clip_bounds(horizon)
    a = get_analytics_conn()
    try:
        r = a.execute(f"""
            SELECT avg(greatest(-{s}, least({t}, fwd_{horizon}d))) AS avg,
                   avg(CASE WHEN fwd_{horizon}d > 0 THEN 1.0 ELSE 0 END) AS win,
                   count(*) AS n
            FROM bars
            WHERE fwd_{horizon}d IS NOT NULL AND fwd_{horizon}d BETWEEN -90 AND 500
        """).fetchone()
    finally:
        a.close()
    return {"avg": float(r[0] or 0), "win": float(r[1] or 0), "n": int(r[2] or 0)}


def event_edge(vol_min: float = 5.0, lb_max: int = 90, horizon: int = 10,
               first_only: bool = True) -> dict:
    """Forward edge of each event type (exit_up / exit_down / retest) vs the
    full-population baseline. first_only=True keeps the FIRST occurrence of each
    event type per zone (the cleanest, least-autocorrelated sample)."""
    import pandas as pd
    df = _load_events(vol_min, lb_max)
    s, t = _clip_bounds(horizon)
    base = _baseline(horizon)
    col = f"fwd_{horizon}d"
    out = []
    if not df.empty:
        if first_only:
            df = df[df["ev_seq"] == 1]
        df = df[df[col].notna() & df[col].between(-90, 500)]
        for et in ["exit_up", "exit_down", "retest"]:
            sub = df[df["event_type"] == et]
            if not len(sub):
                continue
            clip = sub[col].clip(lower=-s, upper=t)
            avg = float(clip.mean())
            win = float((sub[col] > 0).mean())
            out.append({
                "event_type": et,
                "n": int(len(sub)),
                "avg_clip_pct": round(avg, 3),
                "win_rate_pct": round(win * 100, 1),
                "edge_avg_pct": round(avg - base["avg"], 3),
                "edge_win_pp": round((win - base["win"]) * 100, 1),
                "avg_mfe_pct": round(float(sub["mfe_10d"].mean()), 2) if "mfe_10d" in sub else None,
                "avg_mae_pct": round(float(sub["mae_10d"].mean()), 2) if "mae_10d" in sub else None,
            })
    return {
        "params": {"vol_min": vol_min, "lb_max": lb_max, "horizon": horizon,
                   "first_only": first_only, "clip": f"-{s}/+{t}"},
        "baseline": {"avg_clip_pct": round(base["avg"], 3),
                     "win_rate_pct": round(base["win"] * 100, 1), "n": base["n"]},
        "events": out,
    }


def full_report(vol_min: float = 5.0, lb_max: int = 90, horizon: int = 10,
                first_only: bool = True, min_n: int = 30, top: int = 8) -> dict:
    """Edge of all three event types AND the top/bottom context lifts for each —
    computed from a SINGLE events load (one DB pass) so the UI is cheap."""
    df = _load_events(vol_min, lb_max)
    s, t = _clip_bounds(horizon)
    base = _baseline(horizon)
    col = f"fwd_{horizon}d"
    events_out, context_out = [], {}
    if not df.empty:
        if first_only:
            df = df[df["ev_seq"] == 1]
        df = df[df[col].notna() & df[col].between(-90, 500)]
        for et in ["exit_up", "exit_down", "retest"]:
            sub = df[df["event_type"] == et]
            if not len(sub):
                continue
            clip = sub[col].clip(lower=-s, upper=t)
            avg, win = float(clip.mean()), float((sub[col] > 0).mean())

            def _winh(d, h):
                c = d[f"fwd_{h}d"].dropna()
                c = c[c.between(-90, 500)]
                return round((c > 0).mean() * 100, 1) if len(c) else None
            mfe = float(sub["mfe_10d"].mean()); mae = float(sub["mae_10d"].mean())
            rr = round(mfe / abs(mae), 2) if mae else None
            # T/Z follow-through: outcome WHEN tz flips up in the 3 bars AFTER the event
            ftz = sub[sub["tz_up_next3"] == 1]
            tz_follow = None
            if len(ftz) >= min_n:
                tz_follow = {
                    "n": int(len(ftz)),
                    "win10_pct": _winh(ftz, 10),
                    "avg10_pct": round(float(ftz[col].clip(lower=-s, upper=t).mean()), 3),
                    "share_pct": round(len(ftz) / len(sub) * 100, 1),
                }
            events_out.append({
                "event_type": et, "n": int(len(sub)),
                "avg_clip_pct": round(avg, 3), "win_rate_pct": round(win * 100, 1),
                "win5_pct": _winh(sub, 5), "win10_pct": _winh(sub, 10), "win20_pct": _winh(sub, 20),
                "edge_avg_pct": round(avg - base["avg"], 3),
                "edge_win_pp": round((win - base["win"]) * 100, 1),
                "avg_mfe_pct": round(mfe, 2), "avg_mae_pct": round(mae, 2), "rr_ratio": rr,
                "tz_follow": tz_follow,
            })
            # context lift within this event type
            feats = []
            base_avg = avg
            base_win = win
            for f in _BOOL_CTX + _DERIVED_CTX:
                if f not in sub.columns:
                    continue
                s2 = sub[sub[f] == 1]
                if len(s2) < min_n:
                    continue
                a2 = float(s2[col].clip(lower=-s, upper=t).mean())
                w2 = float((s2[col] > 0).mean())
                feats.append({"feature": f, "value": "1", "n": int(len(s2)),
                              "avg_clip_pct": round(a2, 3), "win_rate_pct": round(w2 * 100, 1),
                              "lift_avg_pct": round(a2 - base_avg, 3),
                              "lift_win_pp": round((w2 - base_win) * 100, 1)})
            for f in _CAT_CTX + _DERIVED_CAT:
                if f not in sub.columns:
                    continue
                for val, s2 in sub.groupby(f):
                    if val is None or len(s2) < min_n:
                        continue
                    a2 = float(s2[col].clip(lower=-s, upper=t).mean())
                    w2 = float((s2[col] > 0).mean())
                    feats.append({"feature": f, "value": str(val), "n": int(len(s2)),
                                  "avg_clip_pct": round(a2, 3), "win_rate_pct": round(w2 * 100, 1),
                                  "lift_avg_pct": round(a2 - base_avg, 3),
                                  "lift_win_pp": round((w2 - base_win) * 100, 1)})
            feats.sort(key=lambda r: r["lift_avg_pct"], reverse=True)
            context_out[et] = {"best": feats[:top], "worst": feats[-top:][::-1]}
    return {
        "params": {"vol_min": vol_min, "lb_max": lb_max, "horizon": horizon,
                   "first_only": first_only, "min_n": min_n, "clip": f"-{s}/+{t}"},
        "baseline": {"avg_clip_pct": round(base["avg"], 3),
                     "win_rate_pct": round(base["win"] * 100, 1), "n": base["n"]},
        "events": events_out,
        "context": context_out,
    }


def events_for_ticker(ticker: str, vol_min: float = 5.0, lb_max: int = 90,
                      from_date: str | None = None, horizon: int = 10) -> dict:
    """Every EXIT/RETEST event for ONE ticker — chart overlay (zone box + event
    markers + T/Z-flip flag + forward outcome). Fast (zones filtered to ticker)."""
    df = _load_events(vol_min, lb_max, ticker=ticker.upper())
    col = f"fwd_{horizon}d"
    events = []
    if not df.empty:
        if from_date:
            df = df[df["e_date"].astype(str) >= from_date]
        df = df.sort_values("e_date")
        for _, r in df.iterrows():
            fwd = r.get(col)
            events.append({
                "trigger_date": str(r["z_date"])[:10],
                "zone_low":     round(float(r["z_low"]), 4),
                "zone_high":    round(float(r["z_high"]), 4),
                "event_date":   str(r["e_date"])[:10],
                "event_type":   r["event_type"],
                "tz_flip":      int(r.get("tz_up_next3", 0) or 0),
                "z_mult":       round(float(r["z_mult"]), 1),
                f"fwd_{horizon}d": None if fwd is None or fwd != fwd else round(float(fwd), 2),
                "vol_bucket":   r.get("vol_bucket"),
                "bar_body_wick": r.get("bar_body_wick"),
                "wyc_spring":   int(r.get("wyc_spring", 0) or 0),
            })
    return {"ticker": ticker.upper(), "vol_min": vol_min, "horizon": horizon,
            "count": len(events), "events": events}


def examples(event_type: str = "retest", require_flip: bool = True, vol_min: float = 5.0,
             lb_max: int = 90, horizon: int = 10, first_only: bool = True,
             limit: int = 20) -> dict:
    """~`limit` concrete example instances of a pattern (one per ticker, most
    recent) — so the user can open those tickers and SEE the events on the chart."""
    df = _load_events(vol_min, lb_max)
    col = f"fwd_{horizon}d"
    rows = []
    if not df.empty:
        if first_only:
            df = df[df["ev_seq"] == 1]
        df = df[(df["event_type"] == event_type) & df[col].notna() & df[col].between(-90, 500)]
        if require_flip:
            df = df[df["tz_up_next3"] == 1]
        df = df.sort_values("e_date", ascending=False).drop_duplicates(subset=["ticker"])
        for _, r in df.head(limit).iterrows():
            rows.append({
                "ticker":      r["ticker"],
                "event_date":  str(r["e_date"])[:10],
                "trigger_date": str(r["z_date"])[:10],
                "zone_low":    round(float(r["z_low"]), 4),
                "zone_high":   round(float(r["z_high"]), 4),
                "z_mult":      round(float(r["z_mult"]), 1),
                "tz_flip":     int(r.get("tz_up_next3", 0) or 0),
                f"fwd_{horizon}d": round(float(r[col]), 2),
                "win":         bool(r[col] > 0),
                "vol_bucket":  r.get("vol_bucket"),
                "bar_body_wick": r.get("bar_body_wick"),
            })
    return {"event_type": event_type, "require_flip": require_flip,
            "params": {"vol_min": vol_min, "horizon": horizon},
            "count": len(rows), "examples": rows}


def pattern(event_type: str = "retest", slots: dict | None = None,
            require_flip: bool = False, vol_min: float = 5.0, lb_max: int = 90,
            horizon: int = 10, first_only: bool = True) -> dict:
    """Filter zone events by a full bar-code PATTERN (each slot = a value or *)
    and report the matched edge vs the event base + example tickers. This is the
    'how do all the bar lines work together' builder."""
    slots = slots or {}
    df = _load_events(vol_min, lb_max)
    s, t = _clip_bounds(horizon)
    col = f"fwd_{horizon}d"
    if df.empty:
        return {"event_type": event_type, "matched": {"n": 0}, "examples": []}
    if first_only:
        df = df[df["ev_seq"] == 1]
    df = df[(df["event_type"] == event_type) & df[col].notna() & df[col].between(-90, 500)]
    if require_flip:
        df = df[df["tz_up_next3"] == 1]
    base_n = int(len(df))
    base_win = float((df[col] > 0).mean()) if base_n else 0.0
    base_avg = float(df[col].clip(lower=-s, upper=t).mean()) if base_n else 0.0

    applied = {}
    m = df
    for slot, val in slots.items():
        if not val or val == "*":
            continue
        c = PATTERN_SLOTS.get(slot)
        if c and c in m.columns:
            m = m[m[c].astype(str) == str(val)]
            applied[slot] = val
    n = int(len(m))

    def _winh(h):
        cc = m[f"fwd_{h}d"].dropna()
        cc = cc[cc.between(-90, 500)]
        return round((cc > 0).mean() * 100, 1) if len(cc) else None

    win = float((m[col] > 0).mean()) if n else None
    avg = float(m[col].clip(lower=-s, upper=t).mean()) if n else None
    ex = []
    if n:
        mm = m.sort_values("e_date", ascending=False).drop_duplicates("ticker").head(12)
        for _, r in mm.iterrows():
            ex.append({"ticker": r["ticker"], "event_date": str(r["e_date"])[:10],
                       f"fwd_{horizon}d": round(float(r[col]), 2), "win": bool(r[col] > 0)})
    return {
        "event_type": event_type, "require_flip": require_flip, "applied": applied,
        "base": {"n": base_n, "win_rate_pct": round(base_win * 100, 1),
                 "avg_clip_pct": round(base_avg, 3)},
        "matched": {"n": n,
                    "win_rate_pct": round(win * 100, 1) if n else None,
                    "win5_pct": _winh(5), "win10_pct": _winh(10), "win20_pct": _winh(20),
                    "avg_clip_pct": round(avg, 3) if n else None,
                    "lift_win_pp": round((win - base_win) * 100, 1) if n else None,
                    "lift_avg_pct": round(avg - base_avg, 3) if n else None,
                    "split": _split_stats(m, col)},
        "examples": ex,
    }


def pattern_values(event_type: str = "retest", require_flip: bool = False,
                   vol_min: float = 5.0, lb_max: int = 90, horizon: int = 10,
                   first_only: bool = True, top: int = 30) -> dict:
    """Distinct values present per slot in the event population — for the
    builder's dropdowns (most frequent first)."""
    df = _load_events(vol_min, lb_max)
    col = f"fwd_{horizon}d"
    out = {}
    if not df.empty:
        if first_only:
            df = df[df["ev_seq"] == 1]
        df = df[(df["event_type"] == event_type) & df[col].notna()]
        if require_flip:
            df = df[df["tz_up_next3"] == 1]
        for slot, c in PATTERN_SLOTS.items():
            if c in df.columns:
                col_s = df[c].dropna().astype(str).str.strip()
                col_s = col_s[(col_s != "") & (col_s != "-")]   # drop blanks / empty gap-rng
                vc = col_s.value_counts().head(top)
                out[slot] = [{"value": str(v), "n": int(cnt)} for v, cnt in vc.items()]
            else:
                out[slot] = []
    return {"event_type": event_type, "n": int(len(df)) if not df.empty else 0, "slots": out}


def combo_lift(event_type: str = "retest", vol_min: float = 5.0, lb_max: int = 90,
               horizon: int = 10, first_only: bool = True, min_n: int = 40,
               top: int = 15, anchor: str | None = None, ways: int = 2) -> dict:
    """2-way COMBINATIONS of context features (signals + bar-description shapes)
    on one event type, ranked by forward-edge lift vs the event's own average.
    Single features rarely move the needle — pairs (e.g. follow-through + a
    rejection shape) are where the edge concentrates. Matrix-computed (MᵀM) so
    thousands of pairs cost ~nothing. anchor=<label> restricts to pairs that
    include that feature (e.g. 'tz_up_next3' → best partners for follow-through)."""
    import numpy as np
    df = _load_events(vol_min, lb_max)
    s, t = _clip_bounds(horizon)
    col = f"fwd_{horizon}d"
    empty = {"event_type": event_type, "event_base": None, "best": [], "worst": []}
    if df.empty:
        return empty
    if first_only:
        df = df[df["ev_seq"] == 1]
    df = df[(df["event_type"] == event_type) & df[col].notna() & df[col].between(-90, 500)]
    if not len(df):
        return empty
    ret = df[col].clip(lower=-s, upper=t).to_numpy(dtype=float)
    winv = (df[col].to_numpy(dtype=float) > 0).astype(float)
    base_avg, base_win, base_n = float(ret.mean()), float(winv.mean()), int(len(df))

    feats = []  # (label, bool vector)
    for f in _BOOL_CTX + _DERIVED_CTX:
        if f in df.columns:
            v = (df[f].fillna(0).to_numpy() == 1)
            if v.sum() >= min_n:
                feats.append((f, v))
    for f in _CAT_CTX + _DERIVED_CAT:
        if f not in df.columns:
            continue
        for val, cnt in df[f].value_counts().items():
            if val is None or cnt < min_n:
                continue
            feats.append((f"{f}={val}", (df[f].to_numpy() == val)))
    if len(feats) < 2:
        return {**empty, "event_base": {"avg_clip_pct": round(base_avg, 3),
                                        "win_rate_pct": round(base_win * 100, 1), "n": base_n}}

    labels = [lbl for lbl, _ in feats]
    field_of = [lbl.split("=")[0] for lbl in labels]
    M = np.array([v for _, v in feats], dtype=float).T          # events × F
    F = len(labels)
    oos = (df["e_date"].astype(str).to_numpy() >= _OOS_FROM).astype(float)
    in_s = 1.0 - oos

    def _stats(vec):
        """For a boolean combo vector: overall + IS/OOS win, avg, lift."""
        n = float(vec.sum())
        avg = float((vec * ret).sum() / n)
        win = float((vec * winv).sum() / n)
        n_is = float((vec * in_s).sum()); n_oos = float((vec * oos).sum())
        win_is = float((vec * winv * in_s).sum() / n_is) if n_is else None
        win_oos = float((vec * winv * oos).sum() / n_oos) if n_oos else None
        return {"n": int(n), "avg_clip_pct": round(avg, 3),
                "win_rate_pct": round(win * 100, 1),
                "lift_avg_pct": round(avg - base_avg, 3),
                "lift_win_pp": round((win - base_win) * 100, 1),
                "win_is_pct": round(win_is * 100, 1) if win_is is not None else None,
                "win_oos_pct": round(win_oos * 100, 1) if win_oos is not None else None,
                "n_is": int(n_is), "n_oos": int(n_oos)}

    C = M.T @ M
    combos = []
    if ways >= 3:
        # apriori: extend each FREQUENT pair with a third feature (cheap)
        for i in range(F):
            for j in range(i + 1, F):
                if field_of[i] == field_of[j] or C[i, j] < min_n:
                    continue
                if anchor and anchor not in (labels[i], labels[j]):
                    # for anchored 3-way, require the anchor in the base pair
                    continue
                vij = M[:, i] * M[:, j]
                ck = vij @ M                       # counts of (i,j,k) for all k
                for k in range(j + 1, F):
                    if field_of[k] in (field_of[i], field_of[j]) or ck[k] < min_n:
                        continue
                    vec = vij * M[:, k]
                    st = _stats(vec)
                    combos.append({"a": labels[i], "b": labels[j], "c": labels[k], **st})
    else:
        for i in range(F):
            for j in range(i + 1, F):
                if field_of[i] == field_of[j] or C[i, j] < min_n:
                    continue
                if anchor and anchor not in (labels[i], labels[j]):
                    continue
                combos.append({"a": labels[i], "b": labels[j], **_stats(M[:, i] * M[:, j])})
    combos.sort(key=lambda r: r["lift_avg_pct"], reverse=True)
    return {
        "event_type": event_type,
        "params": {"vol_min": vol_min, "lb_max": lb_max, "horizon": horizon,
                   "first_only": first_only, "min_n": min_n, "anchor": anchor,
                   "ways": ways, "oos_from": _OOS_FROM, "n_features": F, "n_combos": len(combos)},
        "event_base": {"avg_clip_pct": round(base_avg, 3),
                       "win_rate_pct": round(base_win * 100, 1), "n": base_n},
        "best": combos[:top],
        "worst": combos[-top:][::-1],
    }


def context_lift(event_type: str = "retest", vol_min: float = 5.0, lb_max: int = 90,
                 horizon: int = 10, first_only: bool = True, min_n: int = 20) -> dict:
    """For ONE event type, how much each context feature lifts the forward edge
    vs the event's own average (i.e. does absorption / spring / long wick at the
    event bar improve the outcome?). Ranked by avg-return lift."""
    df = _load_events(vol_min, lb_max)
    s, t = _clip_bounds(horizon)
    col = f"fwd_{horizon}d"
    rows = []
    if not df.empty:
        if first_only:
            df = df[df["ev_seq"] == 1]
        df = df[(df["event_type"] == event_type) & df[col].notna() & df[col].between(-90, 500)]
        if len(df):
            base_avg = float(df[col].clip(lower=-s, upper=t).mean())
            base_win = float((df[col] > 0).mean())
            base_n = int(len(df))
            # boolean features: feature == 1
            for f in _BOOL_CTX:
                if f not in df.columns:
                    continue
                sub = df[df[f] == 1]
                if len(sub) < min_n:
                    continue
                avg = float(sub[col].clip(lower=-s, upper=t).mean())
                win = float((sub[col] > 0).mean())
                rows.append({"feature": f, "value": "1", "n": int(len(sub)),
                             "avg_clip_pct": round(avg, 3), "win_rate_pct": round(win * 100, 1),
                             "lift_avg_pct": round(avg - base_avg, 3),
                             "lift_win_pp": round((win - base_win) * 100, 1)})
            # categorical features: per distinct value
            for f in _CAT_CTX + _DERIVED_CAT:
                if f not in df.columns:
                    continue
                for val, sub in df.groupby(f):
                    if val is None or len(sub) < min_n:
                        continue
                    avg = float(sub[col].clip(lower=-s, upper=t).mean())
                    win = float((sub[col] > 0).mean())
                    rows.append({"feature": f, "value": str(val), "n": int(len(sub)),
                                 "avg_clip_pct": round(avg, 3), "win_rate_pct": round(win * 100, 1),
                                 "lift_avg_pct": round(avg - base_avg, 3),
                                 "lift_win_pp": round((win - base_win) * 100, 1)})
            rows.sort(key=lambda r: r["lift_avg_pct"], reverse=True)
            return {"event_type": event_type,
                    "params": {"vol_min": vol_min, "lb_max": lb_max, "horizon": horizon,
                               "first_only": first_only, "min_n": min_n},
                    "event_base": {"avg_clip_pct": round(base_avg, 3),
                                   "win_rate_pct": round(base_win * 100, 1), "n": base_n},
                    "features": rows}
    return {"event_type": event_type, "event_base": None, "features": []}
