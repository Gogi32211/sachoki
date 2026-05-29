"""
studio/acc_exit_miner.py — Empirical Lift Mining for ACC_TR → MARKUP transition.

For each candidate pre-breakout signal, computes:

  Lift_close(s) = P(acc_exit_class in {BO_NOW, BO_1} | signal=1 ∧ wyc_phase=ACC_TR)
                / P(acc_exit_class in {BO_NOW, BO_1} | wyc_phase=ACC_TR)

  Lift_2_3(s)  = P(acc_exit_class = BO_2_3 | signal=1 ∧ wyc_phase=ACC_TR)
               / P(acc_exit_class = BO_2_3 | wyc_phase=ACC_TR)

A lift > 1.0 means the signal increases the probability of a near-term breakout.
We store these in `acc_exit_lift_v1` and use them in the AES score formula.

Runs in ~30 seconds across SP500 + NASDAQ (3.79M bars).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)

PROGRESS_FILE = "/tmp/studio_lift_mine_progress.json"

# Candidate signals to mine — COMPREHENSIVE: every flag column we have
_CANDIDATE_SIGNALS = [
    # ── Wyckoff context ──────────────────────────────────────────────────────
    "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
    # ── Prebreak family ──────────────────────────────────────────────────────
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "pb_lvbo", "pb_wvf_confirm", "pb_stop_cause", "pb_macro_penalty",
    # ── AD (Accumulation/Distribution) ───────────────────────────────────────
    "ad_fresh", "ad_cluster",
    # ── TZ family (T1..T12 + T1G/T2G) ────────────────────────────────────────
    *(f"sig_t{n}" for n in range(1, 13)),
    "sig_t1g", "sig_t2g",
    # ── TZ family (Z1..Z12 + Z1G/Z2G) ────────────────────────────────────────
    *(f"sig_z{n}" for n in range(1, 13)),
    "sig_z1g", "sig_z2g",
    # ── TZ state ─────────────────────────────────────────────────────────────
    "sig_tz_flip", "sig_bias_up", "sig_bias_dn", "tz_bull",
    # ── L digits (individual) ────────────────────────────────────────────────
    "sig_l1", "sig_l2", "sig_l3", "sig_l4", "sig_l5", "sig_l6",
    # ── WLNBB combos ─────────────────────────────────────────────────────────
    "l34", "l43", "l22", "be_up", "bo_up", "bx_up", "vbo_up",
    "sig_fri34", "sig_fri43", "sig_fri64", "sig_l555", "sig_l2l4",
    "sig_blue", "sig_l_any", "sig_be_any",
    # ── CCI family ───────────────────────────────────────────────────────────
    "sig_cci", "sig_cci0r", "sig_ccib",
    # ── Volume / VABS ────────────────────────────────────────────────────────
    "sig_vol_5x", "sig_vol_10x", "sig_vol_20x", "sig_va",
    "sig_abs", "sig_clm", "sig_sc", "sig_bc",
    "sig_best", "sig_strong", "sig_best_up",
    "sig_fbo_up", "sig_eb_up", "sig_3up",
    "sig_fbo_dn", "sig_eb_dn", "sig_vbo_dn",
    # ── GOG ──────────────────────────────────────────────────────────────────
    "sig_g1", "sig_g2", "sig_g4", "sig_g6", "sig_g11", "sig_gog_plus",
    "g1p", "g2p", "g3p", "g1l", "g2l", "g1c", "g2c", "g3c",
    # ── FLY ──────────────────────────────────────────────────────────────────
    "sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad",
    # ── WICK ─────────────────────────────────────────────────────────────────
    "sig_wk_up", "sig_wk_dn", "sig_x1", "sig_x2", "sig_x1g", "sig_x3",
    # ── PREUP / PREDN ────────────────────────────────────────────────────────
    "sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89", "sig_any_p",
    "sig_d2", "sig_d3", "sig_d50", "sig_d55", "sig_d66", "sig_d89", "sig_any_d",
    # ── Combo / Momentum ─────────────────────────────────────────────────────
    "sig_buy", "sig_3g", "sig_conso", "sig_svs", "sig_cd", "sig_ca", "sig_cw",
    "sig_seq_bcont", "rocket", "hilo_buy", "three_g", "svs", "sq", "load", "f8",
    # ── Delta / CISD ─────────────────────────────────────────────────────────
    "sig_flp_up", "sig_org_up", "sig_dd_up_red", "sig_d_up_red",
    "sig_d_dn_green", "sig_dd_dn_green",
    "sig_cisd_cplus", "sig_cisd_cplus_minus", "sig_cisd_cplus_mm",
    # ── PARA ─────────────────────────────────────────────────────────────────
    "sig_para_prep", "sig_para_start", "sig_para_plus", "sig_para_retest",
    # ── EMA position ─────────────────────────────────────────────────────────
    "price_gt_20", "price_gt_50", "price_gt_89", "price_gt_200",
    "price_lt_20", "price_lt_50", "price_lt_89", "price_lt_200",
    # ── RSI ──────────────────────────────────────────────────────────────────
    "rsi_le_35", "rsi_ge_70",
    # ── Line5 booleans ───────────────────────────────────────────────────────
    "wvf_spike", "vix_range", "psar_bull", "psar_bear",
    # ── Meta ─────────────────────────────────────────────────────────────────
    "sig_not_ext", "already_extended_flag",
]


def _write_progress(stage: str, done: int, total: int, started_at: float,
                    extra: dict | None = None) -> None:
    elapsed = time.time() - started_at
    pct = round(done / total * 100, 1) if total else 0
    payload = {
        "stage": stage, "done": done, "total": total, "pct": pct,
        "elapsed_seconds": round(elapsed, 1),
    }
    if extra: payload.update(extra)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)


def mine_lifts(min_samples: int = 100) -> dict:
    """Run lift mining job. Stores results in `acc_exit_lift_v1` table.

    Returns summary with per-signal lifts and metadata.
    """
    started = time.time()
    _write_progress("loading population", 0, 100, started)

    conn = get_conn(read_only=False)
    try:
        # Get ACC_TR population — base for all lift calculations
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        signals = [s for s in _CANDIDATE_SIGNALS if s in available]
        if not signals:
            return {"error": "no candidate signals exist in DB"}

        # Pre-check required columns
        for req in ("acc_exit_class", "wyc_phase"):
            if req not in available:
                return {"error": f"missing column {req} — run enrichment first"}

        # ── Baseline rates within ACC_TR ─────────────────────────────────────
        baseline = conn.execute("""
            SELECT
              COUNT(*) AS total_acc,
              SUM(CASE WHEN acc_exit_class IN ('BO_NOW', 'BO_1') THEN 1 ELSE 0 END) AS bo_close,
              SUM(CASE WHEN acc_exit_class = 'BO_2_3' THEN 1 ELSE 0 END) AS bo_2_3,
              SUM(CASE WHEN acc_exit_class = 'BO_4_5' THEN 1 ELSE 0 END) AS bo_4_5
            FROM bars
            WHERE wyc_phase = 'ACC_TR'
        """).fetchone()

        total_acc = baseline[0] or 0
        if total_acc < 1000:
            return {"error": f"too few ACC_TR bars ({total_acc}) — re-enrich first"}

        base_close = (baseline[1] or 0) / total_acc
        base_2_3   = (baseline[2] or 0) / total_acc
        base_4_5   = (baseline[3] or 0) / total_acc

        log.info("Lift base rates: close=%.3f, 2_3=%.3f, 4_5=%.3f (n=%d ACC_TR)",
                 base_close, base_2_3, base_4_5, total_acc)

        _write_progress("mining signals", 0, len(signals), started)

        # ── Per-signal lifts ─────────────────────────────────────────────────
        results = []
        for i, sig in enumerate(signals):
            try:
                r = conn.execute(f"""
                    SELECT
                      COUNT(*) AS n,
                      SUM(CASE WHEN acc_exit_class IN ('BO_NOW', 'BO_1') THEN 1 ELSE 0 END) AS bo_close,
                      SUM(CASE WHEN acc_exit_class = 'BO_2_3' THEN 1 ELSE 0 END) AS bo_2_3,
                      SUM(CASE WHEN acc_exit_class = 'BO_4_5' THEN 1 ELSE 0 END) AS bo_4_5
                    FROM bars
                    WHERE wyc_phase = 'ACC_TR'
                      AND COALESCE({sig}, 0) = 1
                """).fetchone()

                n = r[0] or 0
                if n < min_samples:
                    log.debug("Skipping %s — only %d samples", sig, n)
                    continue

                p_close = (r[1] or 0) / n
                p_2_3   = (r[2] or 0) / n
                p_4_5   = (r[3] or 0) / n

                lift_close = (p_close / base_close) if base_close > 0 else 1.0
                lift_2_3   = (p_2_3   / base_2_3)   if base_2_3   > 0 else 1.0
                lift_4_5   = (p_4_5   / base_4_5)   if base_4_5   > 0 else 1.0

                results.append({
                    "signal":     sig,
                    "n_acc":      n,
                    "p_close":    round(p_close, 4),
                    "p_2_3":      round(p_2_3, 4),
                    "p_4_5":      round(p_4_5, 4),
                    "lift_close": round(lift_close, 3),
                    "lift_2_3":   round(lift_2_3, 3),
                    "lift_4_5":   round(lift_4_5, 3),
                })

                _write_progress("mining signals", i + 1, len(signals), started,
                                {"qualifying": len(results)})

            except Exception as e:
                log.warning("Failed for %s: %s", sig, e)

        # Sort by lift_2_3 desc (sweet spot for early warning)
        results.sort(key=lambda r: r["lift_2_3"] or 0, reverse=True)

        # ── Persist to DB ────────────────────────────────────────────────────
        conn.execute("DROP TABLE IF EXISTS acc_exit_lift_v1")
        conn.execute("""
            CREATE TABLE acc_exit_lift_v1 (
              signal     VARCHAR PRIMARY KEY,
              n_acc      INTEGER,
              p_close    DOUBLE,
              p_2_3      DOUBLE,
              p_4_5      DOUBLE,
              lift_close DOUBLE,
              lift_2_3   DOUBLE,
              lift_4_5   DOUBLE,
              mined_at   TIMESTAMP DEFAULT now()
            )
        """)
        if results:
            df = pd.DataFrame(results)
            conn.register("lift_df", df)
            conn.execute("""
                INSERT INTO acc_exit_lift_v1 (signal, n_acc, p_close, p_2_3, p_4_5,
                                              lift_close, lift_2_3, lift_4_5)
                SELECT signal, n_acc, p_close, p_2_3, p_4_5,
                       lift_close, lift_2_3, lift_4_5
                FROM lift_df
            """)
            conn.unregister("lift_df")
        conn.commit()

    finally:
        conn.close()

    duration = time.time() - started
    summary = {
        "total_acc_bars":  total_acc,
        "base_rate_close": round(base_close, 4),
        "base_rate_2_3":   round(base_2_3, 4),
        "base_rate_4_5":   round(base_4_5, 4),
        "signals_mined":   len(results),
        "top_5_close":     results[:5] if results else [],
        "top_5_2_3":       sorted(results, key=lambda r: r["lift_2_3"] or 0, reverse=True)[:5],
        "duration_sec":    round(duration, 1),
    }
    _write_progress("done", len(signals), len(signals), started, {"summary": summary})
    log.info("Lift mining complete: %s", summary)
    return summary


def get_progress() -> dict:
    if not os.path.exists(PROGRESS_FILE):
        return {"stage": "idle", "done": 0, "total": 0, "pct": 0}
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"stage": "unknown", "done": 0, "total": 0, "pct": 0}


def get_cached_lifts() -> list[dict]:
    """Return current mined lift table as list of dicts (or empty)."""
    try:
        conn = get_conn(read_only=True)
        try:
            tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
            if "acc_exit_lift_v1" not in tables:
                return []
            df = conn.execute("""
                SELECT signal, n_acc, p_close, p_2_3, p_4_5,
                       lift_close, lift_2_3, lift_4_5
                FROM acc_exit_lift_v1
                ORDER BY lift_2_3 DESC
            """).fetchdf()
            return df.to_dict("records")
        finally:
            conn.close()
    except Exception:
        return []
