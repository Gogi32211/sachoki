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
    "wyc_spring", "wyc_sos", "wyc_in_tr",
    "d_absorb_bull", "d_absorb_bear", "d_div_bull", "d_strong_bull",
    "sig_abs", "vbo_up", "eb_bull", "fbo_bull", "prebreak_prime", "pb_lvbo",
    "l34", "be_up",
]
_CAT_CTX = ["vol_bucket", "bar_body_wick"]


def _events_sql(vol_min: float, lb_max: int) -> str:
    bool_cols = ", ".join(f"b.{c}" for c in _BOOL_CTX)
    cat_cols = ", ".join(f"b.{c}" for c in _CAT_CTX)
    return f"""
        WITH zones AS (
            SELECT ticker, universe, date AS z_date,
                   low AS z_low, high AS z_high, atr_14 AS z_atr,
                   volume / avg_vol_20d AS z_mult,
                   CASE WHEN close > open THEN 'bull' ELSE 'bear' END AS z_dir
            FROM bars
            WHERE avg_vol_20d > 0 AND volume >= {vol_min} * avg_vol_20d AND high > low
        ),
        fwd AS (
            SELECT z.ticker, z.universe, z.z_date, z.z_low, z.z_high, z.z_atr,
                   z.z_mult, z.z_dir, b.date AS e_date,
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
                   coalesce(lag(below)  OVER w, FALSE) AS p_below
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
        )
        SELECT e.ticker, e.universe, e.z_date, e.z_low, e.z_high, e.z_mult, e.z_dir,
               e.e_date, e.event_type, e.age_days, e.ev_seq,
               (e.z_high - e.z_low) / NULLIF(e.z_atr, 0) AS width_atr,
               b.fwd_5d, b.fwd_10d, b.fwd_20d, b.mfe_10d, b.mae_10d,
               {cat_cols}, {bool_cols}
        FROM ranked e
        JOIN bars b ON b.ticker = e.ticker AND b.universe = e.universe AND b.date = e.e_date
    """


def _load_events(vol_min: float, lb_max: int):
    import pandas as pd
    a = get_analytics_conn()
    try:
        df = a.execute(_events_sql(vol_min, lb_max)).fetchdf()
    finally:
        a.close()
    if df.empty:
        return df
    # A ticker dual-listed across universes yields the same zone/event twice.
    df = df.drop_duplicates(subset=["ticker", "z_date", "z_low", "z_high", "e_date", "event_type"])
    return df


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
            events_out.append({
                "event_type": et, "n": int(len(sub)),
                "avg_clip_pct": round(avg, 3), "win_rate_pct": round(win * 100, 1),
                "edge_avg_pct": round(avg - base["avg"], 3),
                "edge_win_pp": round((win - base["win"]) * 100, 1),
                "avg_mfe_pct": round(float(sub["mfe_10d"].mean()), 2),
                "avg_mae_pct": round(float(sub["mae_10d"].mean()), 2),
            })
            # context lift within this event type
            feats = []
            base_avg = avg
            base_win = win
            for f in _BOOL_CTX:
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
            for f in _CAT_CTX:
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
            for f in _CAT_CTX:
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
