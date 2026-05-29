"""
studio/miss_analyzer.py — Miss analysis and False Positive analysis.

Miss:  tickers that had big moves but NO signal fired before it.
FP:    tickers where signal fired but price dropped.

Both compare pre-window bar features to find discriminating factors.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import numpy as np

from studio.db import get_conn

log = logging.getLogger(__name__)

_FEATURE_COLS = [
    "turbo_score", "already_extended_flag", "sig_bc", "rsi_ge_70", "rsi_le_35",
    "ad_cluster", "ad_fresh", "sig_para_retest", "sig_fri34", "l34",
    "g1c", "g1p", "g1l", "sig_gog_plus", "sig_be_any", "sig_l_any",
    "price_gt_200", "price_lt_200", "prebreak_prime", "prebreak_ready",
    "wyc_spring", "wyc_sos", "sig_bias_dn", "sig_bias_up", "sig_not_ext",
    "sig_vol_10x", "sig_wk_up", "sig_wk_dn", "sig_tz_flip", "sig_best_up",
]


def _load_bars_for_tickers(tickers: list[str], pre_window: int = 20) -> pd.DataFrame:
    """Load last pre_window bars before each event for a list of tickers."""
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        feat_present = [c for c in _FEATURE_COLS if c in available]
        sel = ", ".join(["ticker", "date", "close", "universe"] + feat_present)
        placeholders = ", ".join("?" * len(tickers))
        return conn.execute(
            f"SELECT {sel} FROM bars WHERE ticker IN ({placeholders}) ORDER BY ticker, date",
            tickers,
        ).fetchdf()
    finally:
        conn.close()


def analyze_misses(
    event_type:   str = "BULL_2X_60D",
    turbo_max:    float = 15.0,     # "no signal" = turbo <= this threshold
    universes:    list[str] | None = None,
    pre_window:   int = 20,
    top_n:        int = 20,
) -> dict:
    """
    Find events where big move happened but turbo score was LOW (we missed it).
    Analyze what was in the pre-window bars.
    """
    conn = get_conn(read_only=True)
    try:
        where = "event_type = ?"
        params: list = [event_type]
        if universes:
            where += f" AND universe IN ({', '.join('?' * len(universes))})"
            params.extend(universes)

        events = conn.execute(
            f"""SELECT id, ticker, event_date, mfe_60d, fwd_30d, universe, turbo_at_event
                FROM events WHERE {where} ORDER BY mfe_60d DESC NULLS LAST""",
            params,
        ).fetchdf()
    finally:
        conn.close()

    if len(events) == 0:
        return {"total_events": 0, "missed": 0, "examples": []}

    total = len(events)
    # "Missed" = turbo was very low at event time
    missed = events[events["turbo_at_event"].fillna(0) <= turbo_max]
    caught = events[events["turbo_at_event"].fillna(0) >  turbo_max]
    miss_rate = round(len(missed) / total * 100, 1)

    # Load pre-window bars for missed tickers
    miss_tickers = missed["ticker"].unique().tolist()
    bars_df = _load_bars_for_tickers(miss_tickers, pre_window)

    # For each missed event, find pre-window bars
    examples = []
    for _, ev in missed.head(top_n).iterrows():
        tkr_bars = bars_df[bars_df["ticker"] == ev["ticker"]].copy()
        tkr_bars["date"] = pd.to_datetime(tkr_bars["date"])
        ev_date = pd.to_datetime(ev["event_date"])
        pre = tkr_bars[tkr_bars["date"] < ev_date].tail(pre_window)
        if len(pre) == 0:
            continue

        # What signals were present in pre-window?
        feat_present = [c for c in _FEATURE_COLS if c in pre.columns]
        sig_counts = {}
        for col in feat_present:
            vals = pd.to_numeric(pre[col], errors="coerce").fillna(0)
            if vals.sum() > 0:
                sig_counts[col] = int(vals.sum())

        max_turbo = float(pd.to_numeric(pre.get("turbo_score", pd.Series([0])), errors="coerce").max())
        examples.append({
            "ticker":       ev["ticker"],
            "event_date":   str(ev["event_date"]),
            "mfe_60d":      round(float(ev["mfe_60d"]), 1) if pd.notna(ev["mfe_60d"]) else None,
            "fwd_30d":      round(float(ev["fwd_30d"]), 1) if pd.notna(ev["fwd_30d"]) else None,
            "universe":     ev["universe"],
            "turbo_at_event": round(float(ev["turbo_at_event"]), 0) if pd.notna(ev["turbo_at_event"]) else 0,
            "max_turbo_in_prewindow": round(max_turbo, 0),
            "signals_in_prewindow":   sig_counts,
            "pre_window_bars":        len(pre),
        })

    # Why we missed — aggregate feature analysis
    why_missed = _discriminate_features(missed, caught, bars_df, pre_window)

    return {
        "total_events":  total,
        "missed":        len(missed),
        "caught":        len(caught),
        "miss_rate_pct": miss_rate,
        "turbo_threshold_used": turbo_max,
        "why_missed":    why_missed,
        "examples":      examples,
    }


def analyze_false_positives(
    turbo_min:     float = 50.0,
    fwd_max:       float = -10.0,    # dropped -10%+ in 10 days
    fwd_col:       str   = "fwd_10d",
    universes:     list[str] | None = None,
    pre_window:    int   = 5,
    top_n:         int   = 20,
) -> dict:
    """
    Find bars where turbo signal was strong but price dropped.
    Compare against winners (same turbo level, price went up).
    """
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        if fwd_col not in available:
            return {"error": f"{fwd_col} column not available in DB"}

        feat_present = [c for c in _FEATURE_COLS if c in available]
        sel = ", ".join(["ticker", "date", "close", "universe", fwd_col] + feat_present)
        where = "turbo_score >= ?"
        params: list = [turbo_min]
        if universes:
            where += f" AND universe IN ({', '.join('?' * len(universes))})"
            params.extend(universes)
        where += f" AND {fwd_col} IS NOT NULL"

        df = conn.execute(f"SELECT {sel} FROM bars WHERE {where}", params).fetchdf()
    finally:
        conn.close()

    if len(df) == 0:
        return {"total": 0, "fp": 0, "winners": 0}

    fp_mask      = df[fwd_col] <= fwd_max
    winner_mask  = df[fwd_col] >= 5.0

    fp_df      = df[fp_mask]
    winner_df  = df[winner_mask]
    total      = len(df)

    fp_rate    = round(len(fp_df) / total * 100, 1)
    win_rate   = round(len(winner_df) / total * 100, 1)

    # Discriminating features
    feat_present = [c for c in _FEATURE_COLS if c in df.columns]
    discriminators = []
    for col in feat_present:
        fp_v  = pd.to_numeric(fp_df[col],     errors="coerce").fillna(0).mean()
        win_v = pd.to_numeric(winner_df[col],  errors="coerce").fillna(0).mean()
        all_v = pd.to_numeric(df[col],         errors="coerce").fillna(0).mean()
        if abs(fp_v - win_v) > 0.05 and (len(fp_df) >= 20 and len(winner_df) >= 20):
            power = "HIGH" if abs(fp_v - win_v) > 0.25 else ("MEDIUM" if abs(fp_v - win_v) > 0.12 else "LOW")
            discriminators.append({
                "feature":    col,
                "in_fp_pct":  round(fp_v * 100, 1),
                "in_win_pct": round(win_v * 100, 1),
                "diff":       round((fp_v - win_v) * 100, 1),
                "power":      power,
            })
    discriminators.sort(key=lambda x: abs(x["diff"]), reverse=True)

    # FP killer combos — feature combos most predictive of FP
    fp_killers = []
    high_power = [d["feature"] for d in discriminators if d["power"] == "HIGH" and d["in_fp_pct"] > d["in_win_pct"]]
    from itertools import combinations as _comb
    for a, b in list(_comb(high_power[:8], 2)):
        if a not in df.columns or b not in df.columns:
            continue
        a_v = pd.to_numeric(df[a], errors="coerce").fillna(0) >= 1
        b_v = pd.to_numeric(df[b], errors="coerce").fillna(0) >= 1
        both = a_v & b_v
        if both.sum() < 10:
            continue
        fp_prec = float((both & fp_mask).sum() / both.sum())
        if fp_prec >= 0.55:
            fp_killers.append({
                "combo": f"{a} + {b}",
                "fp_precision": round(fp_prec * 100, 1),
                "n": int(both.sum()),
            })
    fp_killers.sort(key=lambda x: x["fp_precision"], reverse=True)

    # Top FP examples
    fp_examples = fp_df.head(top_n)[["ticker","date","close",fwd_col] + feat_present[:6]].to_dict("records")

    return {
        "total_bars_with_signal": total,
        "fp":          len(fp_df),
        "winners":     len(winner_df),
        "fp_rate_pct": fp_rate,
        "win_rate_pct": win_rate,
        "turbo_min_used": turbo_min,
        "fwd_col":     fwd_col,
        "fwd_threshold": fwd_max,
        "discriminators":  discriminators[:15],
        "fp_killer_combos": fp_killers[:8],
        "fp_examples": fp_examples,
    }


def _discriminate_features(
    group_a: pd.DataFrame,
    group_b: pd.DataFrame,
    bars_df: pd.DataFrame,
    pre_window: int,
) -> list[dict]:
    """Compare feature frequency in group_a (missed) vs group_b (caught)."""
    feat_present = [c for c in _FEATURE_COLS if c in bars_df.columns]

    def _event_freq(events_df: pd.DataFrame) -> dict[str, float]:
        freq = {}
        for col in feat_present:
            hits = 0
            for _, ev in events_df.iterrows():
                tkr_bars = bars_df[bars_df["ticker"] == ev["ticker"]]
                tkr_bars = tkr_bars.copy()
                tkr_bars["date"] = pd.to_datetime(tkr_bars["date"])
                ev_date = pd.to_datetime(ev["event_date"])
                pre = tkr_bars[tkr_bars["date"] < ev_date].tail(pre_window)
                if len(pre) == 0:
                    continue
                vals = pd.to_numeric(pre[col], errors="coerce").fillna(0)
                if vals.sum() >= 1:
                    hits += 1
            freq[col] = hits / max(len(events_df), 1)
        return freq

    freq_a = _event_freq(group_a.head(200))
    freq_b = _event_freq(group_b.head(200))

    result = []
    for col in feat_present:
        fa = freq_a.get(col, 0)
        fb = freq_b.get(col, 0)
        if abs(fa - fb) > 0.05:
            result.append({
                "feature": col,
                "in_missed_pct": round(fa * 100, 1),
                "in_caught_pct": round(fb * 100, 1),
                "diff": round((fa - fb) * 100, 1),
            })
    result.sort(key=lambda x: abs(x["diff"]), reverse=True)
    return result[:12]
