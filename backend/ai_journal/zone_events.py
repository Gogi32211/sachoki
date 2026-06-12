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
# The full 6-line bar code on the EVENT bar — same lines as the Sequence Builder.
# T/Z are usually empty on a retest bar (they're the follow-through, captured by
# flip_code) but L / suffix / body-wk / gap-rng / l5 / volume ARE filled, so they
# all participate in the combo / context-lift search.
_CAT_CTX = ["l_sig", "composite_full_suffix",                      # L-line, FULL suffix (EBA/EBO/NDI…)
            "vol_bucket", "bar_body_wick", "bar_line5",            # volume, body/wick, line5
            "wick_suffix", "close_suffix", "penetration_suffix", "ne_suffix",  # atomic suffix parts
            "bar_gap_class", "bar_range_class"]                    # gap / range
# Derived booleans: tz_up_next3 (T/Z flip in the 3 bars after) and at_fib (event
# close within 0.5×ATR of a Fibonacci level of the TRAILING range — confluence).
_DERIVED_CTX = ["tz_up_next3", "at_fib"]
# Derived categoricals: which Fib level the event sits on, and which specific
# T-code drove the follow-through flip (T4/T6/T2G…). Both None when N/A.
_DERIVED_CAT = ["fib_level", "flip_code"]
# SEQUENCE features — codes of the bars BEFORE the event (bar -1, bar -2). Makes
# the analysis multi-bar (like the Sequence Builder), zone-anchored.
_SEQ_CAT = ["p1_tz", "p1_z", "p1_vol", "p1_l5", "p2_tz", "p2_z"]

# ── FULL Ultra-screener signal suite ──────────────────────────────────────────
# The curated _BOOL_CTX above is the readable core; the WHOLE boolean signal set
# from the live `bars` schema is folded in below so every Ultra-screener filter
# participates in the lift / combo / sequence search. Forward-looking OUTCOME
# columns are excluded — they encode the future and would leak look-ahead edge.
_BOOL_DENY_PREFIX = ("hit_", "drop_", "next_pivot_is_", "is_pivot", "fwd_", "mfe", "mae",
                     "ret_", "fut_", "target")   # is_pivot_* repaint (confirmed N bars late) → look-ahead
_BOOL_DENY = {"vix_range"}                       # not a per-bar setup signal
_DISCOVERED_BOOL = None                          # cache (schema introspected once)


def _all_bool_ctx() -> list:
    """The full boolean signal suite present in `bars` (SMALLINT/BOOLEAN 0/1
    flags) — the entire Ultra-screener signal set, not just the curated core.
    Curated core kept first for stable ordering; forward-looking outcome columns
    dropped (no look-ahead leakage). Discovered from the schema once, cached."""
    global _DISCOVERED_BOOL
    if _DISCOVERED_BOOL is not None:
        return _DISCOVERED_BOOL
    found = []
    try:
        a = get_analytics_conn()
        try:
            info = a.execute("PRAGMA table_info('bars')").fetchall()
        finally:
            a.close()
        taken = set(_CAT_CTX) | set(_DERIVED_CAT) | set(_SEQ_CAT) | {"t_sig", "z_sig", "l_sig"}
        for row in info:
            name = row[1]; typ = str(row[2]).upper()
            low = name.lower()
            if typ not in ("SMALLINT", "TINYINT", "BOOLEAN"):
                continue
            if name in taken or name in _BOOL_DENY:
                continue
            if any(low.startswith(p) for p in _BOOL_DENY_PREFIX):
                continue
            found.append(name)
    except Exception as e:                       # DB unavailable → degrade to core
        log.warning("signal-column discovery failed (%s) — using curated core only", e)
        found = []
    seen, out = set(), []
    for c in _BOOL_CTX + found:
        if c not in seen:
            seen.add(c); out.append(c)
    _DISCOVERED_BOOL = out
    log.info("zone-edge context: %d boolean signal features (%d core + %d discovered)",
             len(out), len(_BOOL_CTX), len(out) - len(_BOOL_CTX))
    return out


def _events_sql(vol_min: float, lb_max: int, ticker: str | None = None) -> str:
    bool_cols = ", ".join(f"b.{c}" for c in _all_bool_ctx())
    cat_cols = ", ".join(f"b.{c}" for c in _CAT_CTX)
    tk_filter = ""   # appended to an existing WHERE (zones CTE)
    tk_where = ""    # standalone WHERE (bx CTE, which has none)
    if ticker:
        tk = "".join(c for c in ticker.upper() if c.isalnum() or c in ".-")
        tk_filter = f" AND ticker = '{tk}'"
        tk_where = f" WHERE ticker = '{tk}'"
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
                   z.z_mult, z.z_dir, b.date AS e_date, b.tz_bull AS tzb, b.t_sig AS tsig,
                   b.z_sig AS f_zs, b.vol_bucket AS f_vb, b.bar_line5 AS f_l5,
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
                       ORDER BY e_date ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING), 0) AS tz_up_next3,
                   -- the T-code of each of the next 3 bars (which specific T drives the flip)
                   lead(tsig, 1) OVER w AS ld1,
                   lead(tsig, 2) OVER w AS ld2,
                   lead(tsig, 3) OVER w AS ld3,
                   -- the SEQUENCE before the event: codes of bar -1 and bar -2
                   lag(tsig, 1) OVER w AS p1_tz,  lag(tsig, 2) OVER w AS p2_tz,
                   lag(f_zs, 1) OVER w AS p1_z,   lag(f_zs, 2) OVER w AS p2_z,
                   lag(f_vb, 1) OVER w AS p1_vol, lag(f_l5, 1) OVER w AS p1_l5
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
            FROM bars{tk_where}
            WINDOW w AS (PARTITION BY ticker, universe ORDER BY date ROWS UNBOUNDED PRECEDING)
        )
        SELECT e.ticker, e.universe, e.z_date, e.z_low, e.z_high, e.z_mult, e.z_dir,
               e.e_date, e.event_type, e.age_days, e.ev_seq,
               (e.z_high - e.z_low) / NULLIF(e.z_atr, 0) AS width_atr,
               b.fwd_5d, b.fwd_10d, b.fwd_20d, b.mfe_10d, b.mae_10d,
               b.close AS e_close, b.atr_14 AS e_atr, x.t_lo, x.t_hi,
               b.t_sig, b.z_sig,                            -- T/Z slots (l_sig/full_suffix come via cat_cols)
               e.ld1, e.ld2, e.ld3,                          -- next-3-bar T-codes (flip driver)
               e.p1_tz, e.p2_tz, e.p1_z, e.p2_z, e.p1_vol, e.p1_l5,  -- sequence: bars -1, -2
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
            _add_flip_code(df)
            # Downcast the (now ~220) boolean signal columns 0/1 → int8. Without
            # this the full Ultra suite would balloon the cached frame past ~850MB
            # (float64 per flag); int8 keeps it ~0.5GB and speeds the combo matrix.
            bcols = [c for c in _all_bool_ctx() if c in df.columns]
            if bcols:
                df[bcols] = df[bcols].fillna(0).astype("int8")
        _EVENTS_CACHE[key] = df
        if len(_EVENTS_CACHE) > 4:               # each frame is ~0.5GB — cap tight
            _EVENTS_CACHE.pop(next(iter(_EVENTS_CACHE)))
        return df


# Pattern-builder slots → the column each maps to (the user's full bar-code).
PATTERN_SLOTS = {
    "tz":     "t_sig",         # T-code ON the event bar (empty on retest)
    "z":      "z_sig",         # Z-code ON the event bar
    "flip":   "flip_code",     # the FLIP T-code (T1G/T1/T4 …) — fires AFTER a retest
    "l":      "l_sig",         # L34, L46 …
    "suffix": "composite_full_suffix",   # EBA, EBO, NDI … (full code w/ close pos)
    "bodywk": "bar_body_wick", # STB, M …
    "gaprng": "gap_rng",       # G1-C …
    "l5":     "bar_line5",     # PS-R2X …
    "vol":    "vol_bucket",    # VB, W …
}


_FIB_RATIOS = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
_FIB_LABELS = ["0", "0.236", "0.382", "0.5", "0.618", "0.786", "1"]


def _add_flip_code(df):
    """flip_code = WHICH specific T-code drove the follow-through flip (the first
    non-empty t_sig in the next 3 bars), only for genuine flips. Lets us see which
    T (T4/T6 engulf vs T9 inside …) actually carries the edge, not just 'any T'."""
    import numpy as np
    if df.empty or "ld1" not in df.columns:
        df["flip_code"] = None
        return df
    ld1 = df["ld1"].fillna("").astype(str)
    ld2 = df["ld2"].fillna("").astype(str)
    ld3 = df["ld3"].fillna("").astype(str)
    fc = ld1.where(ld1 != "", ld2)
    fc = fc.where(fc != "", ld3)
    flipped = df["tz_up_next3"].fillna(0).to_numpy() == 1
    df["flip_code"] = np.where(flipped & (fc.to_numpy() != ""), fc.to_numpy(), None)
    return df


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
            for f in _all_bool_ctx() + _DERIVED_CTX:
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
            for f in _CAT_CTX + _DERIVED_CAT + _SEQ_CAT:
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


# Named OOS-validated robust patterns (confirmed retest = flip implied). Each
# setup is tagged with whichever of these its event bar satisfies — so the user
# sees BY WHICH pattern it qualifies, strongest first.
_ROBUST_PATTERNS = [
    {"name": "vol=B + range=N", "tag": "B·N", "oos_win": 64.5,
     "cond": lambda r: str(r.get("vol_bucket")) == "B" and str(r.get("bar_range_class")) == "N"},
    {"name": "sig_abs + vol=B", "tag": "abs·B", "oos_win": 59.4,
     "cond": lambda r: str(r.get("vol_bucket")) == "B" and int(r.get("sig_abs") or 0) == 1},
    {"name": "vol=B", "tag": "B", "oos_win": 53.2,
     "cond": lambda r: str(r.get("vol_bucket")) == "B"},
]


def _match_patterns(r) -> list:
    out = []
    for p in _ROBUST_PATTERNS:
        try:
            if p["cond"](r):
                out.append({"tag": p["tag"], "name": p["name"], "oos_win": p["oos_win"]})
        except Exception:
            pass
    return out


def live_setups(event_type: str = "retest", slots: dict | None = None,
                bools: list | None = None, cats: dict | None = None,
                require_flip: bool = False,
                vol_min: float = 5.0, lb_max: int = 90,
                horizon: int = 10, max_age_days: int = 5, limit: int = 80) -> dict:
    """LIVE scan: tickers whose RECENT bars (last `max_age_days`) are a setup
    matching the pattern. Each is 'confirmed' (T/Z already flipped up after the
    event → actionable) or 'pending' (event fired, not bullish yet → watch for
    the flip). Turns the OOS-validated pattern into a daily alert list."""
    import pandas as pd
    slots = slots or {}
    df = _load_events(vol_min, lb_max)
    if df.empty:
        return {"as_of": None, "count": 0, "setups": []}
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
    finally:
        a.close()
    cutoff = (pd.Timestamp(as_of) - pd.Timedelta(days=max_age_days)).strftime("%Y-%m-%d")
    d = df[(df["event_type"] == event_type) & (df["e_date"].astype(str) >= cutoff)].copy()
    applied = {}
    for slot, val in slots.items():
        if not val or val == "*":
            continue
        c = PATTERN_SLOTS.get(slot)
        if c and c in d.columns:
            d = d[d[c].astype(str) == str(val)]
            applied[slot] = val
    # boolean signal filters (sig_abs, wyc_spring, at_fib, …) from the combos
    for b in (bools or []):
        if b and b in d.columns:
            d = d[d[b] == 1]
            applied[b] = "1"
    # generic categorical filters from the combos: flip_code, sequence (p1_*/p2_*),
    # fib_level — anything not one of the 8 bar-code slots.
    for col, val in (cats or {}).items():
        if col and val and val != "*" and col in d.columns:
            d = d[d[col].astype(str) == str(val)]
            applied[col] = val
    if d.empty:
        return {"as_of": as_of, "event_type": event_type, "applied": applied,
                "count": 0, "confirmed": 0, "pending": 0, "setups": []}
    d["days_ago"] = (pd.Timestamp(as_of) - pd.to_datetime(d["e_date"])).dt.days

    def _status(r):
        if int(r.get("tz_up_next3", 0) or 0) == 1:
            return "confirmed"
        if int(r.get("tz_bull", 0) or 0) == 0:
            return "pending"
        return "extended"
    d["status"] = d.apply(_status, axis=1)
    d = d[d["status"].isin(["confirmed"] if require_flip else ["confirmed", "pending"])]
    # confirmed first, then by recency — so the limit keeps actionable ones
    d["_rank"] = (d["status"] != "confirmed").astype(int)
    d = d.sort_values(["_rank", "e_date"], ascending=[True, False]).drop_duplicates("ticker")
    # cap EACH group (by recency) so neither crowds the other out
    per = max(12, limit // 2)
    d = pd.concat([d[d["status"] == "confirmed"].head(per),
                   d[d["status"] == "pending"].head(per)])

    setups = []
    for _, r in d.iterrows():
        setups.append({
            "ticker": r["ticker"], "event_date": str(r["e_date"])[:10],
            "days_ago": int(r["days_ago"]), "status": r["status"],
            "zone_low": round(float(r["z_low"]), 4), "zone_high": round(float(r["z_high"]), 4),
            "close": round(float(r["e_close"]), 4), "z_mult": round(float(r["z_mult"]), 1),
            "vol_bucket": r.get("vol_bucket"), "range": r.get("bar_range_class"),
            "flip_code": r.get("flip_code"),
            "patterns": _match_patterns(r),
        })
    setups.sort(key=lambda x: (0 if x["status"] == "confirmed" else 1, x["days_ago"]))
    return {"as_of": as_of, "event_type": event_type, "applied": applied,
            "count": len(setups),
            "confirmed": sum(1 for s in setups if s["status"] == "confirmed"),
            "pending": sum(1 for s in setups if s["status"] == "pending"),
            "setups": setups}


def tickers_in_zone(vol_min: float = 5.0, lb_max: int = 90) -> dict:
    """Every ticker whose LATEST bar's close currently sits INSIDE an active HV
    zone band. A 'zone' = the [low, high] of a volume-spike bar
    (volume >= vol_min × avg_vol_20d) that is still fresh (z_date within the last
    lb_max days). Returns {ticker: {zone metadata}} keyed by ticker, keeping the
    freshest qualifying zone per ticker. This is the screener feed for the Ultra
    'ZONE' universe — 'who is sitting on support/resistance right now'."""
    sql = f"""
        WITH latest AS (
            SELECT ticker, max(date) AS d FROM bars GROUP BY ticker
        ),
        last_bar AS (
            SELECT b.ticker, b.universe, b.date AS e_date,
                   b.close, b.low AS b_low, b.high AS b_high
            FROM bars b
            JOIN latest l ON b.ticker = l.ticker AND b.date = l.d
        ),
        zones AS (
            SELECT ticker, universe, date AS z_date, low AS z_low, high AS z_high,
                   volume / avg_vol_20d AS z_mult,
                   CASE WHEN close > open THEN 'bull' ELSE 'bear' END AS z_dir
            FROM bars
            WHERE avg_vol_20d > 0 AND volume >= {vol_min} * avg_vol_20d AND high > low
        ),
        hits AS (
            SELECT lb.ticker, lb.universe, lb.e_date, lb.close,
                   z.z_low, z.z_high, z.z_date, z.z_mult, z.z_dir,
                   datediff('day', z.z_date, lb.e_date) AS age_days,
                   row_number() OVER (PARTITION BY lb.ticker
                       ORDER BY z.z_date DESC, z.z_mult DESC) AS rn
            FROM last_bar lb
            JOIN zones z ON z.ticker = lb.ticker AND z.universe = lb.universe
                        AND z.z_date < lb.e_date
                        AND z.z_date >= lb.e_date - INTERVAL {lb_max} DAY
                        AND lb.close BETWEEN z.z_low AND z.z_high
        )
        SELECT * FROM hits WHERE rn = 1
    """
    a = get_analytics_conn()
    try:
        df = a.execute(sql).fetchdf()
    finally:
        a.close()
    out = {}
    if df.empty:
        return out
    for _, r in df.iterrows():
        lo, hi, px = float(r["z_low"]), float(r["z_high"]), float(r["close"])
        width = hi - lo
        pos = (px - lo) / width if width > 0 else 0.5       # 0=at floor, 1=at ceiling
        out[str(r["ticker"])] = {
            "zone_low":          round(lo, 4),
            "zone_high":         round(hi, 4),
            "zone_date":         str(r["z_date"])[:10],
            "zone_mult":         round(float(r["z_mult"]), 1),
            "zone_dir":          str(r["z_dir"]),
            "zone_age_days":     int(r["age_days"]),
            "zone_pos":          round(pos, 3),
            "zone_dist_high_pct": round((hi - px) / px * 100, 2) if px else None,  # room to ceiling
            "zone_dist_low_pct":  round((px - lo) / px * 100, 2) if px else None,  # room to floor
        }
    return out


def _wilson_lb(wins, n, z=1.96):
    """Wilson score lower bound of a win proportion at ~95%. Shrinks high win-rates
    on small n toward 0 (a 10/12 = 83% → ~55%), so noise on tiny OOS samples stops
    masquerading as a durable edge. Returns a 0..1 fraction (or None if n<=0)."""
    import math as _m
    if not n or n <= 0:
        return None
    p = wins / n
    denom = 1.0 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * _m.sqrt(max(p * (1 - p) / n + z * z / (4 * n * n), 0.0))
    return max(0.0, (centre - margin) / denom)


def _combo_search(feats, ret, winv, oos, base_avg, base_win, min_n, ways, top,
                  anchor=None, field_fn=None):
    """Shared MᵀM combo miner. feats = [(label, 0/1 vector)]. Returns
    (combos_sorted, n_features). field_fn(label)→field decides which features may
    NOT be combined (same field): for bar-code combos two values of one column
    can't co-occur (split on '='); for the sequence miner every (signal, bar)
    feature is independent, so field_fn = identity. Combos carry IS/OOS split."""
    import numpy as np
    field_fn = field_fn or (lambda l: l.split("=")[0])
    # cap features for N-way (rare triples overfit; keeps the search bounded)
    max_feats = 90 if ways >= 3 else 220
    if len(feats) > max_feats:
        feats = sorted(feats, key=lambda fv: int(fv[1].sum()), reverse=True)
        kept = feats[:max_feats]
        if anchor and not any(lbl == anchor for lbl, _ in kept):
            anch = [fv for fv in feats if fv[0] == anchor]
            if anch:
                kept = kept[:max_feats - 1] + anch
        feats = kept
    labels = [lbl for lbl, _ in feats]
    field_of = [field_fn(l) for l in labels]
    M = np.array([v for _, v in feats], dtype=float).T          # events × F
    F = len(labels)
    in_s = 1.0 - oos

    def _stats(vec):
        n = float(vec.sum())
        if not n:
            return None
        avg = float((vec * ret).sum() / n)
        win = float((vec * winv).sum() / n)
        n_is = float((vec * in_s).sum()); n_oos = float((vec * oos).sum())
        win_is = float((vec * winv * in_s).sum() / n_is) if n_is else None
        win_oos = float((vec * winv * oos).sum() / n_oos) if n_oos else None
        # Wilson 95% lower bound — an HONEST win-rate that penalises small samples
        # (a 83% on n=12 collapses to ~55%), so the "holds" gate stops trusting noise.
        win_lb = _wilson_lb(win * n, n)
        oos_lb = _wilson_lb(win_oos * n_oos, n_oos) if (win_oos is not None and n_oos) else None
        return {"n": int(n), "avg_clip_pct": round(avg, 3),
                "win_rate_pct": round(win * 100, 1),
                "win_lb_pct": round(win_lb * 100, 1) if win_lb is not None else None,
                "lift_avg_pct": round(avg - base_avg, 3),
                "lift_win_pp": round((win - base_win) * 100, 1),
                "win_is_pct": round(win_is * 100, 1) if win_is is not None else None,
                "win_oos_pct": round(win_oos * 100, 1) if win_oos is not None else None,
                "oos_lb_pct": round(oos_lb * 100, 1) if oos_lb is not None else None,
                "n_is": int(n_is), "n_oos": int(n_oos)}

    C = M.T @ M
    combos = []
    if ways >= 3:
        TOP_PAIRS = 200
        pairs = []
        for i in range(F):
            for j in range(i + 1, F):
                if field_of[i] == field_of[j] or C[i, j] < min_n:
                    continue
                if anchor and anchor not in (labels[i], labels[j]):
                    continue
                vij = M[:, i] * M[:, j]
                pairs.append((abs(_stats(vij)["lift_avg_pct"]), i, j, vij))
        pairs.sort(key=lambda p: p[0], reverse=True)
        seen = set()
        for _, i, j, vij in pairs[:TOP_PAIRS]:
            ck = vij @ M
            for k in range(F):
                if k == i or k == j or field_of[k] in (field_of[i], field_of[j]) or ck[k] < min_n:
                    continue
                key3 = frozenset((i, j, k))
                if key3 in seen:
                    continue
                seen.add(key3)
                combos.append({"a": labels[i], "b": labels[j], "c": labels[k],
                               **_stats(vij * M[:, k])})
    else:
        for i in range(F):
            for j in range(i + 1, F):
                if field_of[i] == field_of[j] or C[i, j] < min_n:
                    continue
                if anchor and anchor not in (labels[i], labels[j]):
                    continue
                combos.append({"a": labels[i], "b": labels[j], **_stats(M[:, i] * M[:, j])})
    combos.sort(key=lambda r: r["lift_avg_pct"], reverse=True)
    return combos, F


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
    for f in _all_bool_ctx() + _DERIVED_CTX:
        if f in df.columns:
            v = (df[f].fillna(0).to_numpy() == 1)
            if v.sum() >= min_n:
                feats.append((f, v))
    for f in _CAT_CTX + _DERIVED_CAT + _SEQ_CAT:
        if f not in df.columns:
            continue
        for val, cnt in df[f].value_counts().items():
            if val is None or cnt < min_n:
                continue
            feats.append((f"{f}={val}", (df[f].to_numpy() == val)))
    if len(feats) < 2:
        return {**empty, "event_base": {"avg_clip_pct": round(base_avg, 3),
                                        "win_rate_pct": round(base_win * 100, 1), "n": base_n}}

    oos = (df["e_date"].astype(str).to_numpy() >= _OOS_FROM).astype(float)
    combos, F = _combo_search(feats, ret, winv, oos, base_avg, base_win,
                              min_n, ways, top, anchor=anchor)
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


# ── EXIT-SEQUENCE MINER ────────────────────────────────────────────────────────
# Curated "lead-in" signals — the ones that plausibly BUILD a move in the bars
# before a zone exit (momentum / coil / absorption / structure / volume). The
# sequence miner lags these over the bars before the exit and finds which
# multi-bar buildups precede the highest-edge breakouts. Kept to a focused set
# (not the full 220) because an N-bar × full-suite search is both intractable and
# hopelessly overfit; these are the move-initiation signals that matter.
_LEADIN_SIGNALS = [
    # momentum / breakout
    "vbo_up", "eb_bull", "be_up", "fbo_bull", "bo_up", "bx_up",
    # coil / prebreak / consolidation
    "prebreak_ready", "prebreak_prime", "prebreak_v3", "prebreak_v4", "pb_lvbo",
    "sig_conso", "sq",
    # absorption / Wyckoff / spring
    "sig_abs", "l34", "wyc_spring", "wyc_sos", "d_absorb_bull", "d_spring",
    "w2_spring", "w2_sos",
    # surge / strength / divergence
    "d_surge_bull", "d_blast_bull", "d_strong_bull", "d_div_bull",
    # T/Z timing
    "sig_t1g", "sig_t2g", "sig_buy", "sig_t6",
    # structure / trend / volume
    "psar_bull",
    "sig_vol_5x", "sig_vol_10x",
    # parabolic / momentum-late
    "para_prep", "para_start", "rocket",
]


def _leadin_cols():
    """The lead-in signals that actually exist as columns in `bars` (guards the
    SQL against schema drift), PLUS the derived atomic lead-ins (close_o / gap_up /
    r2l_os / atomic) which are SQL expressions, not columns. Cached via _all_bool_ctx."""
    have = set(_all_bool_ctx())
    return [s for s in _LEADIN_SIGNALS if s in have] + list(_DERIVED_LEADIN)


# Atomic lead-in signals (5-year-validated): derived boolean expressions (not raw
# bool columns) so the miner can discover whether the weak-close / gap / oversold
# axis shows up in winning lead-in sequences. Keys are virtual signal names.
_DERIVED_LEADIN = {
    "close_o": "CAST(b.close_suffix = 'O' AS INTEGER)",                       # weak close (below prior body)
    "gap_up":  "CAST(b.bar_gap_class IN ('G2','G3') AS INTEGER)",             # gap-up bar
    "r2l_os":  "CAST(b.bar_line5 LIKE '%R2L%' AS INTEGER)",                   # RSI2 oversold
    "atomic":  "CAST(b.close_suffix='O' AND b.bar_gap_class IN ('G2','G3') AS INTEGER)",  # weak-close gap-up
}


def _seq_sql(vol_min: float, depth: int, sigs: list, zone_def: str = "spike") -> str:
    """Zone-exit population with the lead-in signals lagged over the `depth` bars
    ending at the exit (offset 0 = exit bar, 1..depth-1 = prior bars).
    zone_def: 'spike' = zone formed by a volume ≥ vol_min×avg_vol_20d bar (V1);
              'vb'    = zone formed by a VB vol-class bar (vol_bucket='VB', V2)."""
    _expr = lambda s: _DERIVED_LEADIN.get(s, f"b.{s}")
    fwd_sel = ", ".join(f"{_expr(s)} AS e0_{s}" for s in sigs)
    lag_sel = ", ".join(
        f"lag(e0_{s}, {k}) OVER w AS e{k}_{s}"
        for k in range(1, depth) for s in sigs)
    out_e0 = ", ".join(f"r.e0_{s}" for s in sigs)
    out_lag = ", ".join(f"r.e{k}_{s}" for k in range(1, depth) for s in sigs)
    extra_lag = (", " + lag_sel) if lag_sel else ""
    extra_outlag = (", " + out_lag) if out_lag else ""
    if zone_def == "vb":
        zone_where = "vol_bucket = 'VB' AND high > low"
    elif zone_def == "spike25":      # moderate spike band: 2× ≤ vol < 5× avg
        zone_where = ("avg_vol_20d > 0 AND volume >= 2 * avg_vol_20d "
                      "AND volume < 5 * avg_vol_20d AND high > low")
    elif zone_def == "cci":          # NON-volume nature: zone formed by a CCI-extreme bar
        zone_where = "sig_ccib = 1 AND high > low"
    else:                             # 'spike' (V1): vol ≥ vol_min × avg
        zone_where = f"avg_vol_20d > 0 AND volume >= {vol_min} * avg_vol_20d AND high > low"
    return f"""
        WITH zones AS (
            SELECT ticker, universe, date AS z_date, low AS z_low, high AS z_high
            FROM bars
            WHERE {zone_where}
        ),
        fwd AS (
            SELECT z.ticker, z.universe, z.z_date, b.date AS e_date,
                   (b.close > z.z_high) AS above, (b.close < z.z_low) AS below,
                   {fwd_sel}
            FROM zones z
            JOIN bars b ON b.ticker = z.ticker AND b.universe = z.universe
                       AND b.date > z.z_date AND b.date <= z.z_date + INTERVAL {LB_MAX_SEQ} DAY
        ),
        seq AS (
            SELECT *,
                   coalesce(lag(above) OVER w, FALSE) AS p_above,
                   coalesce(lag(below) OVER w, FALSE) AS p_below{extra_lag}
            FROM fwd
            WINDOW w AS (PARTITION BY ticker, universe, z_date ORDER BY e_date)
        ),
        events AS (
            SELECT *, CASE WHEN above AND NOT p_above THEN 'exit_up'
                           WHEN below AND NOT p_below THEN 'exit_down' END AS et
            FROM seq
        ),
        ranked AS (
            SELECT *, row_number() OVER (PARTITION BY ticker, universe, z_date, et
                                         ORDER BY e_date) AS ev_seq
            FROM events WHERE et IS NOT NULL
        )
        SELECT r.ticker, r.e_date, r.et, r.ev_seq,
               b.fwd_5d, b.fwd_10d, b.fwd_20d, {out_e0}{extra_outlag}
        FROM ranked r
        JOIN bars b ON b.ticker = r.ticker AND b.universe = r.universe AND b.date = r.e_date
    """


LB_MAX_SEQ = 90


def exit_sequences(event_type: str = "exit_up", depth: int = 3, horizon: int = 10,
                   vol_min: float = 5.0, min_n: int = 30, top: int = 20,
                   ways: int = 2, first_only: bool = True, zone_def: str = "spike") -> dict:
    """AUTO-MINER: rank the multi-bar lead-in signal buildups that precede the
    highest-edge zone exits. Each feature is a (signal, bar-offset) — e.g.
    prebreak_ready@-2 — so a 2/3-way combo IS an ordered sequence like
    '−2:prebreak_ready → −1:pb_lvbo → 0:vbo_up'. depth = how many bars back
    (2–4). Forward edge measured vs the exit population, with IS/OOS split."""
    import numpy as np
    depth = max(2, min(4, int(depth)))
    sigs = _leadin_cols()
    empty = {"event_type": event_type, "depth": depth, "best": [], "worst": [], "event_base": None}
    if not sigs:
        return empty
    a = get_analytics_conn()
    try:
        df = a.execute(_seq_sql(vol_min, depth, sigs, zone_def=zone_def)).fetchdf()
    finally:
        a.close()
    if df.empty:
        return empty
    df = df.drop_duplicates(subset=["ticker", "e_date", "et"])
    col = f"fwd_{horizon}d"
    if first_only:
        df = df[df["ev_seq"] == 1]
    df = df[(df["et"] == event_type) & df[col].notna() & df[col].between(-90, 500)]
    if not len(df):
        return empty
    s, t = _clip_bounds(horizon)
    ret = df[col].clip(lower=-s, upper=t).to_numpy(dtype=float)
    winv = (df[col].to_numpy(dtype=float) > 0).astype(float)
    base_avg, base_win, base_n = float(ret.mean()), float(winv.mean()), int(len(df))

    # features = (signal, bar-offset). offset 0 = exit bar, k = k bars before.
    feats = []
    for k in range(0, depth):
        for sg in sigs:
            cname = f"e{k}_{sg}"
            if cname not in df.columns:
                continue
            v = (df[cname].fillna(0).to_numpy() == 1)
            if v.sum() >= min_n:
                feats.append((f"{sg}@-{k}", v))
    if len(feats) < 2:
        return {**empty, "event_base": {"avg_clip_pct": round(base_avg, 3),
                                        "win_rate_pct": round(base_win * 100, 1), "n": base_n}}
    oos = (df["e_date"].astype(str).to_numpy() >= _OOS_FROM).astype(float)
    # field_fn = identity → every (signal, bar) feature is independent, so the same
    # signal on different bars (a genuine 2-bar sequence) and different signals on
    # the same bar both combine.
    combos, F = _combo_search(feats, ret, winv, oos, base_avg, base_win,
                              min_n, ways, top, field_fn=lambda l: l)

    def _order(c):
        """Render a combo as an ordered buildup: earliest bar → exit bar."""
        parts = [c.get("a"), c.get("b"), c.get("c")]
        items = []
        for p in parts:
            if not p or "@-" not in p:
                continue
            sg, off = p.split("@-")
            items.append((int(off), sg))
        items.sort(key=lambda x: -x[0])          # −2, −1, 0
        return [{"bar": (f"−{o}" if o else "exit"), "signal": sg} for o, sg in items]

    for c in combos:
        c["sequence"] = _order(c)
    return {
        "event_type": event_type, "depth": depth,
        "params": {"vol_min": vol_min, "horizon": horizon, "min_n": min_n,
                   "ways": ways, "first_only": first_only, "oos_from": _OOS_FROM,
                   "zone_def": zone_def,
                   "n_signals": len(sigs), "n_features": F, "n_combos": len(combos)},
        "event_base": {"avg_clip_pct": round(base_avg, 3),
                       "win_rate_pct": round(base_win * 100, 1), "n": base_n},
        "best": combos[:top],
        "worst": combos[-top:][::-1],
    }


# Cross-stable de-biased patterns worth flagging live (from the sweep). Each is a
# set of "signal@-offset" tokens; a recent exit containing ALL tokens MATCHES.
_LIVE_PATTERNS = {
    "exit_up": [
        {"name": "T6→RKT",  "tokens": ["sig_t6@-2", "rocket@-0"]},
        {"name": "ABS→EB",  "tokens": ["sig_abs@-2", "eb_bull@-1"]},
        {"name": "SQ→Ab",   "tokens": ["sq@-2", "d_absorb_bull@-0"]},
        {"name": "BE→W·SOS", "tokens": ["be_up@-2", "w2_sos@-0"]},
    ],
    "exit_down": [
        {"name": "FBO→SPRING", "tokens": ["fbo_bull@-2", "wyc_spring@-2"]},
        {"name": "T2G→BUY",    "tokens": ["sig_t2g@-1", "sig_buy@-1"]},
        {"name": "BO→BE",      "tokens": ["bo_up@-3", "be_up@-1"]},
    ],
}


def live_sequences(event_type: str = "exit_down", zone_def: str = "spike",
                   depth: int = 4, max_age_days: int = 10, vol_min: float = 5.0,
                   min_sigs: int = 2, top: int = 50) -> dict:
    """LIVE counterpart of the miner: recent zone exits with the lead-in buildup
    that actually fired. Lists the genuine (de-biased, no-pivot) lead-in signals
    per recent exit and flags any cross-stable validated pattern. Tradeable: every
    listed signal is computed from the bar + its past (no look-ahead)."""
    import pandas as pd
    sigs = _leadin_cols()
    empty = {"event_type": event_type, "zone_def": zone_def, "setups": [], "count": 0}
    if not sigs:
        return empty
    a = get_analytics_conn()
    try:
        df = a.execute(_seq_sql(vol_min, depth, sigs, zone_def=zone_def)).fetchdf()
    finally:
        a.close()
    if df.empty:
        return empty
    df = df.drop_duplicates(subset=["ticker", "e_date", "et"])
    df = df[df["et"] == event_type]
    if df.empty:
        return empty
    maxd = pd.to_datetime(df["e_date"]).max()
    cut = maxd - pd.Timedelta(days=max_age_days)
    df = df[pd.to_datetime(df["e_date"]) >= cut]
    pats = _LIVE_PATTERNS.get(event_type, [])
    out = []
    for _, r in df.iterrows():
        present = [(k, sg) for k in range(depth) for sg in sigs
                   if r.get(f"e{k}_{sg}") == 1]
        if len(present) < min_sigs:
            continue
        present.sort(key=lambda x: -x[0])                 # earliest (largest k) first
        tokens = {f"{sg}@-{k}" for k, sg in present}
        matched = [p["name"] for p in pats if set(p["tokens"]).issubset(tokens)]
        ed = str(r["e_date"])[:10]
        out.append({
            "ticker": r["ticker"], "exit_date": ed,
            "age_days": int((maxd - pd.to_datetime(ed)).days),
            "sequence": [{"bar": ("0" if k == 0 else f"−{k}"), "signal": sg} for k, sg in present],
            "patterns": matched, "n_sigs": len(present),
        })
    out.sort(key=lambda s: (len(s["patterns"]) > 0, s["n_sigs"], -s["age_days"]), reverse=True)
    return {"event_type": event_type, "zone_def": zone_def, "as_of": str(maxd)[:10],
            "count": len(out), "matched": sum(1 for s in out if s["patterns"]),
            "setups": out[:top]}


def sequence_board(zone_def: str = "spike", depth: int = 4, vol_min: float = 5.0,
                   max_age_days: int = 20, min_oos: float = 55.0, min_n: int = 40,
                   top: int = 120) -> dict:
    """The 'Setups Board': recent tickers that built an OOS-HOLDING lead-in sequence,
    each scored with the sequence's out-of-sample win-rate (= probability the move
    follows through), why, last price, and journal status. Loads the seq frame ONCE
    per event type and both mines the holding patterns and matches recent tickers
    from it."""
    import numpy as np, pandas as pd
    sigs = _leadin_cols()
    s, t = _clip_bounds(10)
    rows = {}                                   # ticker -> best board row
    for et in ("exit_up", "exit_down"):
        a = get_analytics_conn()
        try:
            df = a.execute(_seq_sql(vol_min, depth, sigs, zone_def=zone_def)).fetchdf()
        finally:
            a.close()
        if df.empty:
            continue
        df = df.drop_duplicates(subset=["ticker", "e_date", "et"])
        df = df[(df["et"] == et) & df["fwd_10d"].notna() & df["fwd_10d"].between(-90, 500)]
        if len(df) < min_n:
            continue
        ret = df["fwd_10d"].clip(lower=-s, upper=t).to_numpy(dtype=float)
        winv = (df["fwd_10d"].to_numpy(dtype=float) > 0).astype(float)
        base_avg, base_win = float(ret.mean()), float(winv.mean())
        oos = (df["e_date"].astype(str).to_numpy() >= _OOS_FROM).astype(float)
        feats = []
        for k in range(depth):
            for sg in sigs:
                c = f"e{k}_{sg}"
                if c in df.columns:
                    v = (df[c].fillna(0).to_numpy() == 1)
                    if v.sum() >= min_n:
                        feats.append((f"{sg}@-{k}", v))
        if len(feats) < 2:
            continue
        combos, _ = _combo_search(feats, ret, winv, oos, base_avg, base_win,
                                  min_n, 2, top, field_fn=lambda l: l)
        base_win_pct = round(base_win * 100, 1)
        holding = [c for c in combos
                   if c.get("win_is_pct") is not None and c.get("win_oos_pct") is not None
                   and (c["win_oos_pct"] - c["win_is_pct"]) >= -6
                   and c["win_oos_pct"] >= min_oos and c["win_oos_pct"] > base_win_pct]
        maxd = pd.to_datetime(df["e_date"]).max()
        recent = df[pd.to_datetime(df["e_date"]) >= (maxd - pd.Timedelta(days=max_age_days))]
        for c in holding:
            toks = [c.get("a"), c.get("b")] + ([c.get("c")] if c.get("c") else [])
            need = []
            for tok in toks:
                if tok and "@-" in tok:
                    sg, k = tok.rsplit("@-", 1)
                    need.append((int(k), sg))
            label = " → ".join(("0" if k == 0 else f"−{k}") + ":" + sg for k, sg in
                               sorted(need, key=lambda x: -x[0]))
            m = recent
            for k, sg in need:
                col = f"e{k}_{sg}"
                m = m[m[col] == 1] if col in m.columns else m.iloc[0:0]
            for _, r in m.sort_values("e_date", ascending=False).drop_duplicates("ticker").iterrows():
                tk = r["ticker"]
                prob = c["win_oos_pct"]
                if tk in rows and rows[tk]["prob_up"] >= prob:
                    continue
                rows[tk] = {
                    "ticker": tk, "event_type": et, "exit_date": str(r["e_date"])[:10],
                    "age_days": int((maxd - pd.to_datetime(str(r["e_date"])[:10])).days),
                    "sequence": label, "prob_up": prob, "edge_pp": c.get("lift_win_pp"),
                    "n": c["n"], "win_is": c.get("win_is_pct"), "base": base_win_pct,
                    "n_oos": c.get("n_oos"), "n_is": c.get("n_is"),
                    "why": ("failed-breakdown bounce: " if et == "exit_down" else "breakout follow-through: ")
                           + label + f"  (holds OOS {prob}%, n={c['n']})",
                }
    if not rows:
        return {"zone_def": zone_def, "count": 0, "rows": []}
    tickers = list(rows)
    ph = ",".join("?" * len(tickers))
    # enrich: last price + sector
    a = get_analytics_conn()
    try:
        px = a.execute(f"""
            SELECT ticker, close AS last_price, universe, date AS px_date, rsi_14 AS rsi
            FROM bars WHERE date = (SELECT max(date) FROM bars) AND ticker IN ({ph})
        """, tickers).fetchdf()
    finally:
        a.close()
    pxmap = {r["ticker"]: r for _, r in px.iterrows()}
    from .db import get_journal_conn
    try:
        from . import memory as _mem
        meta = _mem.load_ticker_meta()
    except Exception:
        meta = {}
    # journal status
    jstat = {}
    try:
        j = get_journal_conn(read_only=True)
        try:
            for r in j.execute(f"SELECT ticker, status, conviction FROM journal_position "
                               f"WHERE status IN ('OPEN','PENDING_OPEN') AND ticker IN ({ph})",
                               tickers).fetchall():
                jstat[r[0]] = {"status": r[1], "conviction": r[2]}
        finally:
            j.close()
    except Exception:
        pass
    out = []
    for tk, d in rows.items():
        p = pxmap.get(tk, {})
        m = meta.get(tk, {}) if isinstance(meta, dict) else {}
        d["last_price"] = round(float(p["last_price"]), 2) if p is not None and p.get("last_price") is not None else None
        d["price_date"] = str(p.get("px_date"))[:10] if p is not None and p.get("px_date") is not None else None
        d["universe"] = p.get("universe") if p is not None else None
        d["rsi"] = round(float(p["rsi"]), 0) if p is not None and p.get("rsi") is not None else None
        d["sector"] = m.get("sector") or ""
        d["mcap_bucket"] = m.get("mcap_bucket") or ""
        d["journal"] = jstat.get(tk)
        # score 0-100: blend OOS prob + edge + recency
        prob = d["prob_up"] or 0
        edge = d["edge_pp"] or 0
        rec = max(0, 10 - d["age_days"])
        d["score"] = round(min(100, prob * 0.8 + edge * 1.0 + rec * 0.8), 0)
        out.append(d)
    out.sort(key=lambda r: (r["score"], r["prob_up"]), reverse=True)
    return {"zone_def": zone_def, "as_of": d.get("price_date"),
            "count": len(out), "rows": out[:top]}


# combo feature-column → live_setups slot key (mirrors the frontend SLOT_OF_COL)
_COMBO_SLOT_OF_COL = {
    "t_sig": "tz", "z_sig": "z", "flip_code": "flip", "l_sig": "l",
    "full_suffix": "suffix", "composite_full_suffix": "suffix",
    "bar_body_wick": "bodywk", "gap_rng": "gaprng", "bar_line5": "l5",
    "vol_bucket": "vol",
}


def _combo_to_filters(c: dict):
    """Decompose a combo's a/b/c labels into live_setups filters (mirrors the
    frontend applyCombo): slot dict, bool list, cat dict, and require-flip flag."""
    slots, bools, cats, flip = {}, [], {}, False
    for f in (c.get("a"), c.get("b"), c.get("c")):
        if not f:
            continue
        if f == "tz_up_next3":
            flip = True
        elif "=" in f:
            i = f.index("="); col, val = f[:i], f[i + 1:]
            slot = _COMBO_SLOT_OF_COL.get(col)
            if slot:
                slots[slot] = val
            else:
                cats[col] = val
        else:
            bools.append(f)
    return slots, bools, cats, flip


_COMBO_EVENT_LABEL = {"retest": "retest", "exit_up": "breakout", "exit_down": "spring"}


def combo_board(event_types=("retest", "exit_up", "exit_down"), vol_min: float = 5.0,
                horizon: int = 10, ways: int = 2, min_n: int = 40, top_combos: int = 50,
                max_age_days: int = 10, anchor: str | None = "tz_up_next3",
                limit: int = 120) -> dict:
    """The 'Combos Board': across ALL buy event types (retest / breakout=exit_up /
    spring=exit_down — same spread as the sequence Setups Board), take the
    OOS-HOLDING combos, match each to the recent tickers that satisfy it,
    aggregate per ticker keeping the best combo, and score each for BUY quality
    (OOS win + lift + confirmed-flip bonus + recency). One scored ticker list."""
    import pandas as pd
    if isinstance(event_types, str):
        event_types = [e.strip() for e in event_types.split(",") if e.strip()]
    rows = {}                                   # ticker -> best scored candidate
    bases, n_holding = {}, 0
    for et in event_types:
        cl = combo_lift(event_type=et, vol_min=vol_min, horizon=horizon,
                        first_only=True, min_n=min_n, top=top_combos, anchor=anchor, ways=ways)
        base = float((cl.get("event_base") or {}).get("win_rate_pct") or 0)
        bases[et] = round(base, 1)
        holding = [c for c in (cl.get("best") or [])
                   if c.get("win_is_pct") is not None and c.get("win_oos_pct") is not None
                   and (c["win_oos_pct"] - c["win_is_pct"]) >= -6 and c["win_oos_pct"] > base]
        n_holding += len(holding)
        etlabel = _COMBO_EVENT_LABEL.get(et, et)
        for c in holding:
            slots, bools, cats, flip = _combo_to_filters(c)
            try:
                live = live_setups(event_type=et, slots=slots, bools=bools, cats=cats,
                                   require_flip=flip, vol_min=vol_min, horizon=horizon,
                                   max_age_days=max_age_days, limit=limit)
            except Exception:
                continue
            lbl = " + ".join(x for x in (c.get("a"), c.get("b"), c.get("c")) if x)
            prob = c["win_oos_pct"]; lift = c.get("lift_win_pp") or 0
            n_oos = c.get("n_oos")
            optimistic = (c.get("win_is_pct") is not None and (prob - c["win_is_pct"]) > 20
                          and n_oos is not None and n_oos < 15)
            for s in (live.get("setups") or []):
                tk = s["ticker"]; confirmed = s.get("status") == "confirmed"
                rec = max(0, 10 - int(s.get("days_ago", 99) or 99))
                score = round(min(100, prob * 0.7 + lift * 1.0 + (12 if confirmed else 0)
                                  + rec * 0.8 - (15 if optimistic else 0)))
                cand = {
                    "ticker": tk, "combo": lbl, "status": s.get("status"),
                    "event_type": et, "event_label": etlabel,
                    "days_ago": int(s.get("days_ago", 0) or 0),
                    "event_date": s.get("event_date"), "close": s.get("close"),
                    "prob_up": prob, "win_is": c.get("win_is_pct"), "base": round(base, 1),
                    "edge_pp": lift, "n": c.get("n"), "n_oos": n_oos,
                    "optimistic": bool(optimistic), "score": score,
                    "flip_code": s.get("flip_code"), "vol_bucket": s.get("vol_bucket"),
                }
                prev = rows.get(tk)
                if prev is None or score > prev["score"]:
                    rows[tk] = cand
    if not rows:
        return {"event_types": list(event_types), "bases": bases, "n_combos": n_holding,
                "count": 0, "rows": []}
    tickers = list(rows)
    ph = ",".join("?" * len(tickers))
    a = get_analytics_conn()
    try:
        px = a.execute(f"""
            SELECT ticker, close AS last_price, universe, date AS px_date, rsi_14 AS rsi
            FROM bars WHERE date = (SELECT max(date) FROM bars) AND ticker IN ({ph})
        """, tickers).fetchdf()
        # ── Blow-off guard ────────────────────────────────────────────────────
        # The combo anchors on the (older) event bar and is BLIND to a pump-dump
        # that happened AFTER it. Validated on 5yr/8M bars: a bar whose intraday
        # high ran >= +80% over the prior close has median fwd_10d excess -22..-27
        # and is 0/6 years positive — buying it is a -25% trade. A weak/rejected
        # close (close < 55% of the high) makes it worse. EDHL is the canonical
        # trap: spring 06-09 @ $3.10, then 06-11 gapped to $16.53, spiked $17.49,
        # crashed -66% to close $5.98 (61M vol). We scan the recent window for the
        # worst such spike and flag the row so the board never reads it as a buy.
        bdf = a.execute(f"""
            WITH recent AS (
              SELECT ticker, date, high, low, close, volume, avg_vol_20d,
                     lag(close) OVER (PARTITION BY ticker ORDER BY date) AS pc,
                     row_number() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
              FROM bars WHERE ticker IN ({ph})
            )
            SELECT ticker, date, high, low, close, volume, avg_vol_20d, pc
            FROM recent
            WHERE rn <= {int(max_age_days) + 3} AND pc > 0 AND high > low
              AND high / pc - 1 >= 0.80
        """, tickers).fetchdf()
    finally:
        a.close()
    pxmap = {r["ticker"]: r for _, r in px.iterrows()}
    blow = {}
    for _, r in bdf.iterrows():
        tk = r["ticker"]
        hi_run = (float(r["high"]) / float(r["pc"]) - 1) * 100
        c_vs_hi = float(r["close"]) / float(r["high"]) if r["high"] else 1.0
        volx = (float(r["volume"]) / float(r["avg_vol_20d"])) if r["avg_vol_20d"] else None
        cur = blow.get(tk)
        if cur is None or hi_run > cur["hi_run"]:   # keep the worst spike in the window
            blow[tk] = {
                "date": str(r["date"])[:10], "hi_run": round(hi_run),
                "c_vs_hi": round(c_vs_hi, 2), "volx": round(volx, 1) if volx else None,
                "severe": bool(hi_run >= 150 or c_vs_hi < 0.55),
            }
    jstat = {}
    try:
        from .db import get_journal_conn
        j = get_journal_conn(read_only=True)
        try:
            for r in j.execute(f"SELECT ticker, status, conviction FROM journal_position "
                               f"WHERE status IN ('OPEN','PENDING_OPEN') AND ticker IN ({ph})",
                               tickers).fetchall():
                jstat[r[0]] = {"status": r[1], "conviction": r[2]}
        finally:
            j.close()
    except Exception:
        pass
    # split markers — tickers with a (reverse) split in the last ~100 days. Uses the
    # ALREADY-CACHED split universe only (never triggers a blocking NASDAQ fetch on
    # the board's request path); it warms via the Ultra 'split' feature / periodic
    # refresh. Best-effort — never let the splits source break the board.
    split_map = {}
    try:
        from datetime import date, datetime
        import split_universe as _su
        _svc = _su.split_service
        _cached = getattr(_svc, "_last_result", None)
        _valid = getattr(_svc, "_is_cache_valid", lambda: False)()
        _rows = _cached.rows if (_cached is not None and _valid) else []
        if not _rows:   # cache cold → warm it in the background; splits show on next load
            import threading
            threading.Thread(target=_svc.get_split_universe, daemon=True).start()
        _today = date.today()
        for _sr in _rows:
            _sym = _su.normalize_split_symbol(_sr.get("ticker", ""))
            _sd = _sr.get("split_date")
            if not _sym or not _sd:
                continue
            try:
                _ago = (_today - datetime.strptime(str(_sd)[:10], "%Y-%m-%d").date()).days
            except Exception:
                continue
            if 0 <= _ago <= 100:
                _prev = split_map.get(_sym)
                if _prev is None or _ago < _prev["days_ago"]:   # keep the most recent split
                    split_map[_sym] = {"date": str(_sd)[:10], "ratio": _sr.get("ratio"), "days_ago": _ago}
    except Exception:
        pass
    as_of = None
    out = []
    for tk, d in rows.items():
        p = pxmap.get(tk)
        if p is not None and p.get("px_date") is not None:
            as_of = str(p["px_date"])[:10]
        d["last_price"] = round(float(p["last_price"]), 2) if p is not None and p.get("last_price") is not None else d.get("close")
        d["universe"] = p.get("universe") if p is not None else None
        d["rsi"] = round(float(p["rsi"]), 0) if p is not None and p.get("rsi") is not None else None
        d["journal"] = jstat.get(tk)
        d["split"] = split_map.get(str(tk).upper())   # {date, ratio, days_ago} or None
        d["blowoff"] = blow.get(tk)   # {date,hi_run,c_vs_hi,volx,severe} or None
        if d["blowoff"]:              # demote: a contaminated setup is not a buy
            d["score"] = max(0, d["score"] - (35 if d["blowoff"]["severe"] else 20))
        out.append(d)
    # clean rows first, then confirmed, then score (blow-off rows sink to the bottom)
    out.sort(key=lambda r: (not r.get("blowoff"), r["status"] == "confirmed",
                            r["score"], r["prob_up"]), reverse=True)
    return {"event_types": list(event_types), "bases": bases, "as_of": as_of,
            "n_combos": n_holding, "count": len(out), "rows": out}


def sequence_tickers(seq: str, event_type: str = "exit_up", zone_def: str = "spike",
                     depth: int = 4, vol_min: float = 5.0, max_age_days: int = 60,
                     top: int = 200) -> dict:
    """Drill-down: the recent tickers whose zone-exit built ONE specific lead-in
    sequence. `seq` = comma-separated 'signal@-k' tokens (k=0 is the exit bar),
    exactly the tokens of a clicked miner row. Returns the matching tickers with
    their exit date + age (most recent first)."""
    import pandas as pd
    toks = []
    for t in (seq or "").split(","):
        t = t.strip()
        if "@-" not in t:
            continue
        sg, k = t.rsplit("@-", 1)
        try:
            toks.append((int(k), sg))
        except ValueError:
            pass
    if not toks:
        return {"seq": seq, "tickers": [], "count": 0}
    sigs = _leadin_cols()
    depth = max(depth, (max(k for k, _ in toks) + 1))
    a = get_analytics_conn()
    try:
        df = a.execute(_seq_sql(vol_min, depth, sigs, zone_def=zone_def)).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {"seq": seq, "tickers": [], "count": 0}
    df = df.drop_duplicates(subset=["ticker", "e_date", "et"])
    df = df[df["et"] == event_type]
    if df.empty:
        return {"seq": seq, "tickers": [], "count": 0}
    maxd = pd.to_datetime(df["e_date"]).max()
    df = df[pd.to_datetime(df["e_date"]) >= (maxd - pd.Timedelta(days=max_age_days))]
    for k, sg in toks:
        col = f"e{k}_{sg}"
        if col in df.columns:
            df = df[df[col] == 1]
        else:
            return {"seq": seq, "tickers": [], "count": 0, "note": f"unknown signal {sg}"}
    df = df.sort_values("e_date", ascending=False).drop_duplicates("ticker")
    out = [{"ticker": r["ticker"], "exit_date": str(r["e_date"])[:10],
            "age_days": int((maxd - pd.to_datetime(str(r["e_date"])[:10])).days)}
           for _, r in df.iterrows()]
    return {"seq": seq, "event_type": event_type, "zone_def": zone_def,
            "as_of": str(maxd)[:10], "count": len(out), "tickers": out[:top]}


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
            for f in _all_bool_ctx():
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
            for f in _CAT_CTX + _DERIVED_CAT + _SEQ_CAT:
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
