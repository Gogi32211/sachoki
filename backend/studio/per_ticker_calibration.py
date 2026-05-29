"""
studio/per_ticker_calibration.py — Per-ticker AES calibration via Bayesian shrinkage.

For each (ticker, signal) combination, computes:

  lift_local = P(BO_2_3 | ACC_TR ∧ signal=1, this_ticker)
             / P(BO_2_3 | ACC_TR, this_ticker)

  lift_blended = (n_local × lift_local + α × lift_global) / (n_local + α)

The shrinkage parameter α (default 20) controls how much per-ticker data dominates:
  - n_local = 0  → lift_blended = lift_global (no per-ticker info)
  - n_local = 20 → 50/50 blend
  - n_local = 100 → mostly local (~85% local)

Why shrinkage matters: per-ticker ACC_TR samples are often only 5-30 — too few
for reliable lift estimation on their own. Shrinking toward the global lift
gives statistical stability while still capturing ticker-specific quirks.

Output table: ticker_signal_lift_v1
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

PROGRESS_FILE = "/tmp/studio_per_ticker_calib_progress.json"

# Shrinkage prior strength — increasing makes per-ticker lifts less divergent
# from global. 20 is a good balance: ticker needs ~20 supporting samples to
# meaningfully deviate from global.
_SHRINKAGE_ALPHA = 20.0

# Minimum ACC_TR bars per ticker to be eligible for calibration
_MIN_TICKER_ACC_BARS = 30

# Signals to calibrate — must match what's in acc_exit_lift_v1
_SIGNALS_TO_CALIBRATE = [
    "pb_lvbo", "pb_wvf_confirm", "pb_stop_cause",
    "wyc_spring", "wyc_sos",
    "ad_fresh", "ad_cluster",
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "sig_bias_up", "sig_3up", "sig_clm", "sig_blue",
    "sig_fri34", "sig_fri43", "sig_fri64",
    "be_up", "bo_up", "bx_up",
    "sig_vol_5x", "sig_vol_10x", "sig_vol_20x",
    "sig_g1", "sig_g2", "sig_g4", "sig_g6",
    "sig_para_prep", "sig_para_start",
    "sig_t1", "sig_t1g", "sig_t2", "sig_t2g",
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


def calibrate_per_ticker(alpha: float = _SHRINKAGE_ALPHA,
                        min_acc_bars: int = _MIN_TICKER_ACC_BARS) -> dict:
    """Run per-ticker lift calibration. Stores results in ticker_signal_lift_v1.

    Returns summary dict.
    """
    started = time.time()
    _write_progress("loading global lifts", 0, 100, started)

    conn = get_conn(read_only=False)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())

        # Load global lifts (from acc_exit_lift_v1)
        try:
            global_lifts_df = conn.execute("""
                SELECT signal, lift_close, lift_2_3, lift_4_5, n_acc AS n_global
                FROM acc_exit_lift_v1
            """).fetchdf()
        except Exception:
            return {"error": "acc_exit_lift_v1 missing — run global lift mining first"}

        if len(global_lifts_df) == 0:
            return {"error": "no global lifts found"}

        global_lifts = {
            row["signal"]: {
                "lift_close": float(row["lift_close"] or 1.0),
                "lift_2_3":   float(row["lift_2_3"] or 1.0),
                "lift_4_5":   float(row["lift_4_5"] or 1.0),
                "n_global":   int(row["n_global"] or 0),
            }
            for _, row in global_lifts_df.iterrows()
        }

        signals = [s for s in _SIGNALS_TO_CALIBRATE
                   if s in available and s in global_lifts]
        if not signals:
            return {"error": "no signals available for calibration"}

        # ── Get eligible tickers (≥ min_acc_bars ACC_TR bars) ────────────────
        eligible = conn.execute(f"""
            SELECT ticker, universe, COUNT(*) AS n_acc
            FROM bars
            WHERE wyc_phase = 'ACC_TR'
            GROUP BY ticker, universe
            HAVING n_acc >= {min_acc_bars}
            ORDER BY n_acc DESC
        """).fetchdf()
        total_tickers = len(eligible)
        log.info("Per-ticker calibration: %d eligible tickers", total_tickers)

        if total_tickers == 0:
            return {"error": f"no tickers have ≥{min_acc_bars} ACC_TR bars"}

        _write_progress("calibrating tickers", 0, total_tickers, started)

        rows_out: list[dict] = []
        done = 0

        for _, t_row in eligible.iterrows():
            ticker = t_row["ticker"]
            universe = t_row["universe"]
            n_acc_local = int(t_row["n_acc"])

            # Per-ticker base rate
            base = conn.execute("""
                SELECT
                  COUNT(*) AS total_acc,
                  SUM(CASE WHEN acc_exit_class = 'BO_2_3' THEN 1 ELSE 0 END) AS bo_2_3,
                  SUM(CASE WHEN acc_exit_class IN ('BO_NOW','BO_1') THEN 1 ELSE 0 END) AS bo_close
                FROM bars
                WHERE ticker = ? AND universe = ? AND wyc_phase = 'ACC_TR'
            """, [ticker, universe]).fetchone()

            base_total = base[0] or 0
            if base_total < min_acc_bars:
                done += 1
                continue
            base_2_3   = (base[1] or 0) / base_total
            base_close = (base[2] or 0) / base_total

            # Per-signal local lifts
            for sig in signals:
                try:
                    r = conn.execute(f"""
                        SELECT
                          COUNT(*) AS n_local,
                          SUM(CASE WHEN acc_exit_class = 'BO_2_3' THEN 1 ELSE 0 END) AS bo_2_3,
                          SUM(CASE WHEN acc_exit_class IN ('BO_NOW','BO_1') THEN 1 ELSE 0 END) AS bo_close
                        FROM bars
                        WHERE ticker = ? AND universe = ? AND wyc_phase = 'ACC_TR'
                          AND COALESCE({sig}, 0) = 1
                    """, [ticker, universe]).fetchone()

                    n_loc = r[0] or 0
                    bo_2_3_loc = r[1] or 0
                    bo_cl_loc = r[2] or 0

                    g = global_lifts[sig]

                    if n_loc < 1 or base_2_3 == 0:
                        # No data — use global directly
                        lift_2_3_loc = g["lift_2_3"]
                        lift_cl_loc = g["lift_close"]
                        lift_2_3_blend = g["lift_2_3"]
                        lift_cl_blend = g["lift_close"]
                    else:
                        p_2_3_loc = bo_2_3_loc / n_loc if n_loc > 0 else 0
                        p_cl_loc  = bo_cl_loc / n_loc if n_loc > 0 else 0
                        lift_2_3_loc = (p_2_3_loc / base_2_3) if base_2_3 > 0 else 1.0
                        lift_cl_loc  = (p_cl_loc / base_close) if base_close > 0 else 1.0
                        # Bayesian shrinkage
                        lift_2_3_blend = (n_loc * lift_2_3_loc + alpha * g["lift_2_3"]) / (n_loc + alpha)
                        lift_cl_blend  = (n_loc * lift_cl_loc + alpha * g["lift_close"]) / (n_loc + alpha)

                    rows_out.append({
                        "ticker":         ticker,
                        "universe":       universe,
                        "signal":         sig,
                        "n_acc":          base_total,
                        "n_local":        n_loc,
                        "lift_local_2_3": round(lift_2_3_loc, 3),
                        "lift_local_cl":  round(lift_cl_loc, 3),
                        "lift_blend_2_3": round(lift_2_3_blend, 3),
                        "lift_blend_cl":  round(lift_cl_blend, 3),
                        "lift_global_2_3": round(g["lift_2_3"], 3),
                    })
                except Exception:
                    pass

            done += 1
            if done % 100 == 0 or done == total_tickers:
                _write_progress("calibrating tickers", done, total_tickers, started,
                                {"rows_so_far": len(rows_out)})

        # ── Persist ──────────────────────────────────────────────────────────
        conn.execute("DROP TABLE IF EXISTS ticker_signal_lift_v1")
        conn.execute("""
            CREATE TABLE ticker_signal_lift_v1 (
              ticker          VARCHAR,
              universe        VARCHAR,
              signal          VARCHAR,
              n_acc           INTEGER,
              n_local         INTEGER,
              lift_local_2_3  DOUBLE,
              lift_local_cl   DOUBLE,
              lift_blend_2_3  DOUBLE,
              lift_blend_cl   DOUBLE,
              lift_global_2_3 DOUBLE,
              calibrated_at   TIMESTAMP DEFAULT now(),
              PRIMARY KEY (ticker, universe, signal)
            )
        """)
        if rows_out:
            df = pd.DataFrame(rows_out)
            conn.register("calib_df", df)
            conn.execute("""
                INSERT INTO ticker_signal_lift_v1
                  (ticker, universe, signal, n_acc, n_local,
                   lift_local_2_3, lift_local_cl,
                   lift_blend_2_3, lift_blend_cl, lift_global_2_3)
                SELECT ticker, universe, signal, n_acc, n_local,
                       lift_local_2_3, lift_local_cl,
                       lift_blend_2_3, lift_blend_cl, lift_global_2_3
                FROM calib_df
            """)
            conn.unregister("calib_df")
        conn.commit()

    finally:
        conn.close()

    duration = time.time() - started
    summary = {
        "alpha":           alpha,
        "min_acc_bars":    min_acc_bars,
        "eligible_tickers": total_tickers,
        "rows_stored":     len(rows_out),
        "signals_count":   len(signals),
        "duration_sec":    round(duration, 1),
    }
    _write_progress("done", total_tickers, total_tickers, started, {"summary": summary})
    log.info("Per-ticker calibration complete: %s", summary)
    return summary


def get_progress() -> dict:
    if not os.path.exists(PROGRESS_FILE):
        return {"stage": "idle", "done": 0, "total": 0, "pct": 0}
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"stage": "unknown", "done": 0, "total": 0, "pct": 0}


def get_ticker_lifts(ticker: str, universe: Optional[str] = None) -> list[dict]:
    """Return calibrated lifts for one ticker."""
    try:
        conn = get_conn(read_only=True)
        try:
            tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
            if "ticker_signal_lift_v1" not in tables:
                return []
            params: list = [ticker]
            sql = """
                SELECT signal, n_acc, n_local,
                       lift_local_2_3, lift_local_cl,
                       lift_blend_2_3, lift_blend_cl, lift_global_2_3
                FROM ticker_signal_lift_v1
                WHERE ticker = ?
            """
            if universe:
                sql += " AND universe = ?"
                params.append(universe)
            sql += " ORDER BY lift_blend_2_3 DESC"
            df = conn.execute(sql, params).fetchdf()
            return df.to_dict("records")
        finally:
            conn.close()
    except Exception:
        return []
