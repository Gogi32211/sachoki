"""
studio/pattern_miner.py — Mine signal patterns from pre-event windows.

For each detected event, look N bars BEFORE it and find:
1. Single signal lift scores  (freq_in_events / base_freq)
2. 2-way and 3-way signal combos
3. Ordered sequences (signal A then B then C within window)

Lift = how much more likely a signal is in pre-event windows vs. random bars.
"""

from __future__ import annotations

import json
import logging
import uuid
from itertools import combinations
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)

# All binary signal columns we mine across
_SIGNAL_COLS = [
    # Volume / VABS
    "sig_abs","sig_clm","sig_ns_vabs","sig_sc","sig_bc","sig_fbo_up","sig_eb_up",
    "sig_3up","sig_fbo_dn","sig_eb_dn",
    # WLNBB / L
    "sig_fri34","sig_fri43","sig_fri64","sig_blue","sig_cci","sig_cci0r","sig_ccib",
    "sig_rl","sig_rh","sig_pp","sig_l_any","sig_be_any","sig_l555","sig_l2l4",
    "l34","l43","l22","be_up","bo_up","bx_up","vbo_up",
    # GOG
    "sig_g1","sig_g2","sig_g4","sig_g6","sig_g11","sig_gog_plus",
    "g1p","g2p","g3p","g1l","g2l","g1c","g2c","g3c",
    # WICK / X
    "sig_fly_abcd","sig_fly_cd","sig_fly_bd","sig_fly_ad",
    "sig_wk_up","sig_wk_dn","sig_x1","sig_x2","sig_x1g","sig_x3",
    # TZ state
    "sig_tz_flip","sig_bias_up",
    # Individual T signals
    "sig_t1g","sig_t2g","sig_t1","sig_t2","sig_t3","sig_t4","sig_t5","sig_t6",
    "sig_t9","sig_t10","sig_t11","sig_t12",
    # Individual Z signals
    "sig_z1g","sig_z2g","sig_z1","sig_z2","sig_z3","sig_z4","sig_z5","sig_z6",
    "sig_z9","sig_z10","sig_z11","sig_z12",
    # EMA / PREUP
    "sig_p55","sig_p66","sig_p89","sig_any_p",
    # Combo / Momentum
    "sig_buy","sig_3g","sig_conso","sig_svs","sig_cd","sig_ca","sig_cw","sig_seq_bcont",
    "sig_va","sig_vol_5x","sig_vol_10x","sig_vol_20x",
    "rocket","hilo_buy","three_g","sq",
    # Delta
    "sig_flp_up","sig_org_up","sig_dd_up_red","sig_d_up_red",
    # CISD / PARA
    "sig_cisd_cplus","sig_para_prep","sig_para_start","sig_para_plus","sig_para_retest",
    # Meta
    "sig_not_ext","already_extended_flag",
    # AD / Wyckoff / Prebreak
    "ad_fresh","ad_cluster","wyc_spring","wyc_sos","wyc_in_tr",
    "prebreak_prime","prebreak_ready","prebreak_watch","pb_lvbo","pb_wvf_confirm",
    # EMA position
    "price_gt_89","price_gt_200","price_lt_89","price_lt_200",
    "rsi_le_35","rsi_ge_70",
]

# Known column aliases: raw name → canonical sig_* name.
# When both are present in the data, drop the alias to avoid phantom combos
# with absurdly high lift (perfectly correlated = independence assumption breaks).
_ALIAS_MAP: dict[str, str] = {
    "three_g": "sig_3g",    # same signal, two column names
    "svs":     "sig_svs",   # same signal, two column names
}


def _load_event_bars(
    event_type: str,
    pre_window: int,
    universes: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      event_df   — rows from bars table that are events (one row per event)
      pre_df     — rows from bars table in pre-window (up to pre_window bars before each event)
                   with 'event_id' column linking back to event_df
    """
    conn = get_conn(read_only=True)
    try:
        # Get events (only ones detected on the active timeframe)
        from studio.db import current_tf
        ev_where = "event_type = ? AND coalesce(tf,'1d') = ?"
        ev_params = [event_type, current_tf()]
        if universes:
            ev_where += f" AND universe IN ({', '.join('?' * len(universes))})"
            ev_params.extend(universes)

        events = conn.execute(
            f"SELECT id, ticker, event_date, universe FROM events WHERE {ev_where}",
            ev_params,
        ).fetchdf()

        if len(events) == 0:
            return pd.DataFrame(), pd.DataFrame()

        # Load all bars for involved tickers
        tickers = events["ticker"].unique().tolist()
        placeholders = ", ".join("?" * len(tickers))

        # Get available signal columns
        available_cols = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        sig_cols_present = [c for c in _SIGNAL_COLS if c in available_cols]
        extra_cols = ["ticker", "date", "close", "turbo_score", "universe"]
        sel_cols = ", ".join(extra_cols + sig_cols_present)

        bars = conn.execute(
            f"SELECT {sel_cols} FROM bars WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchdf()

        if len(bars) == 0:
            return events, pd.DataFrame()

        bars["date"] = pd.to_datetime(bars["date"])
        events["event_date"] = pd.to_datetime(events["event_date"])

        # For each event, collect pre-window bars
        pre_rows = []
        for _, ev in events.iterrows():
            ticker_bars = bars[bars["ticker"] == ev["ticker"]].sort_values("date")
            ev_date = ev["event_date"]
            pre = ticker_bars[ticker_bars["date"] < ev_date].tail(pre_window).copy()
            pre["event_id"] = ev["id"]
            pre["days_before"] = (ev_date - pre["date"]).dt.days
            pre_rows.append(pre)

        pre_df = pd.concat(pre_rows, ignore_index=True) if pre_rows else pd.DataFrame()
        return events, pre_df

    finally:
        conn.close()


def _load_baseline_bars(universes: list[str] | None = None) -> pd.DataFrame:
    """Load a random sample of all bars for baseline frequency calculation."""
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        sig_cols_present = [c for c in _SIGNAL_COLS if c in available]
        sel = ", ".join(sig_cols_present)

        where = ""
        params: list = []
        if universes:
            where = f"WHERE universe IN ({', '.join('?' * len(universes))})"
            params.extend(universes)

        # Sample up to 100K rows for baseline (faster)
        df = conn.execute(
            f"SELECT {sel} FROM bars {where} USING SAMPLE 100000",
            params,
        ).fetchdf()
        return df
    finally:
        conn.close()


def mine_patterns(
    event_type:   str,
    pre_window:   int  = 20,
    min_lift:     float = 2.0,
    min_n:        int   = 15,
    combo_depth:  int   = 3,
    include_seqs: bool  = True,
    universes:    list[str] | None = None,
) -> dict:
    """
    Mine signal patterns from pre-event windows.

    Returns dict with:
      run_id, event_type, n_events,
      single_signals, combos_2way, combos_3way, sequences
    """
    run_id = str(uuid.uuid4())[:8]
    log.info("Pattern mining run=%s event=%s window=%d", run_id, event_type, pre_window)

    events, pre_df = _load_event_bars(event_type, pre_window, universes)
    n_events = len(events)
    log.info("  Events: %d, Pre-window rows: %d", n_events, len(pre_df))

    if n_events == 0 or len(pre_df) == 0:
        return {"run_id": run_id, "event_type": event_type, "n_events": 0,
                "single_signals": [], "combos_2way": [], "combos_3way": [], "sequences": []}

    baseline = _load_baseline_bars(universes)
    log.info("  Baseline rows: %d", len(baseline))

    # ── Signal presence per EVENT (not per bar — any bar in window counts) ────
    sig_cols = [c for c in _SIGNAL_COLS if c in pre_df.columns]

    # Drop alias columns when their canonical counterpart is also present.
    # Keeping both would create perfectly-correlated pairs whose joint baseline
    # (computed under the independence assumption) is far too low, producing
    # meaningless lift values like 2000×.
    _aliases_to_drop = {
        alias for alias, canonical in _ALIAS_MAP.items()
        if alias in sig_cols and canonical in sig_cols
    }
    if _aliases_to_drop:
        log.info("  Dropping alias columns to avoid phantom combos: %s", sorted(_aliases_to_drop))
        sig_cols = [c for c in sig_cols if c not in _aliases_to_drop]

    # Group pre_df by event_id: for each event, was signal present in ANY pre-window bar?
    event_presence = (
        pre_df.groupby("event_id")[sig_cols]
              .max()          # 1 if any bar had it, 0 otherwise
              .reset_index()
    )

    n_ev = len(event_presence)  # events that had pre-window data
    if n_ev == 0:
        return {"run_id": run_id, "event_type": event_type, "n_events": n_events,
                "single_signals": [], "combos_2way": [], "combos_3way": [], "sequences": []}

    # Baseline frequencies
    base_freq: dict[str, float] = {}
    for col in sig_cols:
        if col in baseline.columns:
            vals = pd.to_numeric(baseline[col], errors="coerce").fillna(0)
            base_freq[col] = float(vals.mean())
        else:
            base_freq[col] = 0.0

    # ── Single signals ────────────────────────────────────────────────────────
    single_rows = []
    for col in sig_cols:
        if col not in event_presence.columns:
            continue
        col_vals = pd.to_numeric(event_presence[col], errors="coerce").fillna(0)
        freq = float(col_vals.mean())
        bf = base_freq.get(col, 0.0)
        n_hit = int(col_vals.sum())
        lift = freq / bf if bf > 0 else 999.0
        if lift >= min_lift and n_hit >= min_n:
            single_rows.append({
                "signal": col,
                "freq_in_events": round(freq * 100, 1),
                "base_freq": round(bf * 100, 2),
                "lift": round(lift, 1),
                "n_events_with_signal": n_hit,
            })

    single_rows.sort(key=lambda x: x["lift"], reverse=True)
    log.info("  Single signals with lift≥%.1f: %d", min_lift, len(single_rows))

    # ── 2-way combos ─────────────────────────────────────────────────────────
    # Use top-50 by lift as pool
    pool = [r["signal"] for r in single_rows[:50]]
    combos_2 = []
    if combo_depth >= 2 and len(pool) >= 2:
        for a, b in combinations(pool, 2):
            if a not in event_presence.columns or b not in event_presence.columns:
                continue
            both = ((pd.to_numeric(event_presence[a], errors="coerce").fillna(0) >= 1) &
                    (pd.to_numeric(event_presence[b], errors="coerce").fillna(0) >= 1))
            n_both = int(both.sum())
            if n_both < min_n:
                continue
            freq = float(both.mean())
            bf_a = base_freq.get(a, 0); bf_b = base_freq.get(b, 0)
            # Joint baseline frequency (assume independence)
            bf_joint = bf_a * bf_b
            lift = freq / bf_joint if bf_joint > 0 else 999.0
            if lift >= min_lift:
                combos_2.append({
                    "signals": [a, b],
                    "freq_in_events": round(freq * 100, 1),
                    "base_freq": round(bf_joint * 100, 3),
                    "lift": round(lift, 1),
                    "n": n_both,
                })
        combos_2.sort(key=lambda x: x["lift"], reverse=True)
        combos_2 = combos_2[:60]
    log.info("  2-way combos: %d", len(combos_2))

    # ── 3-way combos ─────────────────────────────────────────────────────────
    combos_3 = []
    pool3 = [r["signal"] for r in single_rows[:25]]
    if combo_depth >= 3 and len(pool3) >= 3:
        for a, b, c in combinations(pool3, 3):
            if not all(x in event_presence.columns for x in [a, b, c]):
                continue
            all3 = (
                (pd.to_numeric(event_presence[a], errors="coerce").fillna(0) >= 1) &
                (pd.to_numeric(event_presence[b], errors="coerce").fillna(0) >= 1) &
                (pd.to_numeric(event_presence[c], errors="coerce").fillna(0) >= 1)
            )
            n_all = int(all3.sum())
            if n_all < min_n:
                continue
            freq = float(all3.mean())
            bf_joint = base_freq.get(a, 0) * base_freq.get(b, 0) * base_freq.get(c, 0)
            lift = freq / bf_joint if bf_joint > 0 else 999.0
            if lift >= min_lift:
                combos_3.append({
                    "signals": [a, b, c],
                    "freq_in_events": round(freq * 100, 1),
                    "lift": round(lift, 1),
                    "n": n_all,
                })
        combos_3.sort(key=lambda x: x["lift"], reverse=True)
        combos_3 = combos_3[:30]
    log.info("  3-way combos: %d", len(combos_3))

    # ── Sequences (ordered patterns across days) ──────────────────────────────
    sequences = []
    if include_seqs and len(pool) >= 2:
        # For each event, get ordered list of (days_before, signal) pairs
        # A "sequence" = signal A fires before signal B (A is earlier = larger days_before)
        seq_pool = [r["signal"] for r in single_rows[:20]]
        seq_counts: dict[tuple, int] = {}

        for ev_id, grp in pre_df.groupby("event_id"):
            grp = grp.sort_values("days_before", ascending=False)  # oldest first
            for sig in seq_pool:
                if sig not in grp.columns:
                    continue
            # Build ordered list of signals that fired in this event's window
            # (use first appearance of each signal, ordered by days_before desc = oldest first)
            fired_signals = []
            for _, bar in grp.iterrows():
                for sig in seq_pool:
                    if sig in bar and pd.to_numeric(bar[sig], errors="coerce") >= 1:
                        if sig not in [s for s, _ in fired_signals]:
                            fired_signals.append((sig, int(bar["days_before"])))

            # Extract 2-signal ordered pairs
            for i in range(len(fired_signals) - 1):
                for j in range(i + 1, len(fired_signals)):
                    s1, d1 = fired_signals[i]
                    s2, d2 = fired_signals[j]
                    if d1 > d2:  # s1 fires before s2
                        key = (s1, s2)
                        seq_counts[key] = seq_counts.get(key, 0) + 1

        for (s1, s2), cnt in seq_counts.items():
            if cnt >= min_n:
                freq = cnt / n_ev
                sequences.append({
                    "sequence": [s1, s2],
                    "description": f"{s1} → {s2}",
                    "freq_in_events": round(freq * 100, 1),
                    "n": cnt,
                })
        sequences.sort(key=lambda x: x["n"], reverse=True)
        sequences = sequences[:30]
    log.info("  Sequences: %d", len(sequences))

    # ── Persist to DB ─────────────────────────────────────────────────────────
    conn = get_conn()
    try:
        conn.execute("DELETE FROM mined_patterns WHERE run_id = ?", [run_id])
        for r in single_rows:
            conn.execute("""
                INSERT INTO mined_patterns
                  (run_id, event_type, pre_window, pattern_type, signals,
                   freq_in_events, base_freq, lift, n_events)
                VALUES (?, ?, ?, 'single', ?, ?, ?, ?, ?)
            """, [run_id, event_type, pre_window,
                  json.dumps([r["signal"]]),
                  r["freq_in_events"], r["base_freq"], r["lift"],
                  r["n_events_with_signal"]])
        for r in combos_2:
            conn.execute("""
                INSERT INTO mined_patterns
                  (run_id, event_type, pre_window, pattern_type, signals,
                   freq_in_events, lift, n_events)
                VALUES (?, ?, ?, 'combo_2', ?, ?, ?, ?)
            """, [run_id, event_type, pre_window,
                  json.dumps(r["signals"]), r["freq_in_events"], r["lift"], r["n"]])
        conn.commit()
    finally:
        conn.close()

    return {
        "run_id":         run_id,
        "event_type":     event_type,
        "n_events_total": n_events,
        "n_events_with_prewindow": n_ev,
        "pre_window":     pre_window,
        "single_signals": single_rows[:40],
        "combos_2way":    combos_2,
        "combos_3way":    combos_3,
        "sequences":      sequences,
    }
