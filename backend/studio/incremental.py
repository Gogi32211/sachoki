"""
studio/incremental.py — Daily incremental refresh of Studio DB.

⚠️ DEPRECATED — DO NOT USE IN PRODUCTION.
   This legacy path writes only a ~40-column subset of each bar row, which leaves
   most enrichment/signal columns NULL and can silently corrupt the table if mixed
   with full-row imports. The live path is studio.incremental_delta (full 320-col
   rows, validated cell-for-cell). This module is retained only for its unit test
   (tests/test_phase2_incremental.py) and is no longer imported by studio_api.

Adds the most recent bar(s) to existing tickers without re-scanning history.
Designed to run after market close (16:00 ET + 1h = 17:00 ET) on weekdays.

Flow per ticker:
  1. Query DB: SELECT MAX(date) FROM bars WHERE ticker = ?
  2. Fetch the last ~10 bars from api_bar_signals (covers weekend gaps)
  3. Filter to bars with date > last_date_in_db
  4. INSERT new rows into DB (using same column mapping as bulk import)
  5. After all tickers done, run enricher to recompute derived columns
     (ATR, swing/pivot, suffixes etc.) for the affected ticker(s)

Idempotent: re-running on a market close day adds at most 1 bar per ticker.
On weekends/holidays: returns "no new data" without errors.
"""
from __future__ import annotations

import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from studio.db import get_conn, STUDIO_DB_PATH
from studio.enricher import enrich_universe

log = logging.getLogger(__name__)

PROGRESS_FILE = "/tmp/studio_incremental_progress.json"

# How many bars to fetch per ticker (covers weekend gaps + safety margin)
_FETCH_BARS = 10


def _write_progress(stage: str, done: int, total: int, started_at: float,
                    extra: dict | None = None) -> None:
    elapsed = time.time() - started_at
    pct = round(done / total * 100, 1) if total else 0
    eta = round(elapsed / done * (total - done)) if done > 0 else None
    payload = {
        "stage": stage, "done": done, "total": total, "pct": pct,
        "elapsed_seconds": round(elapsed, 1), "eta_seconds": eta,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        payload.update(extra)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)


def _bar_dict_to_db_row(b: dict, ticker: str, universe: str) -> dict:
    """Convert api_bar_signals bar dict to a DB row dict (subset of columns).
    Only includes columns we know are populated by api_bar_signals + that exist in DB.
    Enricher will fill the derived columns afterwards.
    """
    def _join(lst):
        if not lst: return ""
        if isinstance(lst, list): return " ".join(str(x) for x in lst)
        return str(lst)

    tz = b.get("tz", "")
    return {
        "ticker":   ticker,
        "date":     b.get("date", ""),
        "universe": universe,
        "open":     float(b.get("open", 0)),
        "high":     float(b.get("high", 0)),
        "low":      float(b.get("low", 0)),
        "close":    float(b.get("close", 0)),
        "volume":   int(float(b.get("volume", 0))),
        "vol_bucket": b.get("vol_bucket", ""),
        "turbo_score": float(b.get("turbo_score", 0)),
        "rtb_phase": b.get("rtb_phase", ""),
        "rtb_total": float(b.get("rtb_total", 0)),
        # T/Z/L core
        "t_sig":  tz if tz.startswith("T") else "",
        "z_sig":  tz if tz.startswith("Z") else "",
        "l_sig":  b.get("l_chart", "") or _join(b.get("l")),   # prefer chart-format
        # 260523 fields
        "ad_fresh":         1 if b.get("ad_fresh") else 0,
        "ad_cluster":       1 if b.get("ad_cluster") else 0,
        "wyc_phase":        b.get("wyc_phase", ""),
        "wyc_spring":       1 if b.get("wyc_spring") else 0,
        "wyc_sos":          1 if b.get("wyc_sos") else 0,
        "wyc_in_tr":        1 if b.get("wyc_in_tr") else 0,
        "wyc_sow":          1 if b.get("wyc_sow") else 0,
        "prebreak_score":   float(b.get("prebreak_score", 0)),
        "prebreak_prime":   1 if b.get("prebreak_prime") else 0,
        "prebreak_ready":   1 if b.get("prebreak_ready") else 0,
        "prebreak_watch":   1 if b.get("prebreak_watch") else 0,
        "pb_lvbo":          1 if b.get("pb_lvbo") else 0,
        "pb_wvf_confirm":   1 if b.get("pb_wvf_confirm") else 0,
        "pb_stop_cause":    1 if b.get("pb_stop_cause") else 0,
        "pb_macro_penalty": 1 if b.get("pb_macro_penalty") else 0,
        "swing_type":       b.get("swing_type", ""),
    }


def _fetch_ticker_new_bars(ticker: str, universe: str, last_date) -> list[dict]:
    """Fetch new bars (post last_date) for a ticker using api_bar_signals."""
    # Lazy import to avoid circular ref at module load
    from main import api_bar_signals

    try:
        bars = api_bar_signals(ticker, tf="1d", bars=_FETCH_BARS, universe=universe)
    except Exception as e:
        log.debug("fetch failed for %s: %s", ticker, e)
        return []

    # Filter to bars strictly after last_date
    new_bars = []
    last_date_str = str(last_date) if last_date is not None else "1900-01-01"
    for b in bars:
        bdate = str(b.get("date", ""))
        if bdate and bdate > last_date_str:
            new_bars.append(b)
    return new_bars


def incremental_refresh(universes: list[str] | None = None) -> dict:
    """Run incremental daily refresh across one or more universes.

    Returns summary dict.
    """
    universes = universes or ["sp500", "nasdaq"]
    started = time.time()
    _write_progress("starting", 0, 0, started)

    overall_summary = {
        "universes": {},
        "started_at": datetime.now(timezone.utc).isoformat(),
        "duration_sec": 0,
    }

    for universe in universes:
        log.info("Incremental refresh: %s", universe)

        # Get tickers + their max(date)
        conn = get_conn(read_only=True)
        try:
            ticker_dates = conn.execute(
                "SELECT ticker, MAX(date) AS max_date FROM bars "
                "WHERE universe = ? GROUP BY ticker ORDER BY ticker",
                [universe]
            ).fetchall()
        finally:
            conn.close()

        total = len(ticker_dates)
        log.info("  %d tickers to refresh in %s", total, universe)
        _write_progress(f"fetching {universe}", 0, total, started,
                        extra={"current_universe": universe})

        rows_to_insert: list[dict] = []
        affected_tickers: set[str] = set()
        errors: list[dict] = []
        done = 0

        for ticker, last_date in ticker_dates:
            try:
                new_bars = _fetch_ticker_new_bars(ticker, universe, last_date)
                for b in new_bars:
                    rows_to_insert.append(_bar_dict_to_db_row(b, ticker, universe))
                    affected_tickers.add(ticker)
            except Exception as e:
                errors.append({"ticker": ticker, "error": f"{type(e).__name__}: {e}"})

            done += 1
            if done % 50 == 0 or done == total:
                _write_progress(f"fetching {universe}", done, total, started,
                                extra={"current_universe": universe,
                                       "new_rows": len(rows_to_insert),
                                       "affected_tickers": len(affected_tickers),
                                       "errors": len(errors)})

        # ── INSERT new rows ───────────────────────────────────────────────────
        rows_inserted = 0
        if rows_to_insert:
            _write_progress(f"inserting {universe}", done, total, started,
                            extra={"new_rows": len(rows_to_insert)})

            # Build DataFrame matching DB column subset
            new_df = pd.DataFrame(rows_to_insert)
            # Coerce date column
            new_df["date"] = pd.to_datetime(new_df["date"]).dt.date

            conn = get_conn(read_only=False)
            try:
                # Delete any pre-existing rows for this date+ticker (idempotent re-run)
                # We use the (ticker, date, universe) PK to upsert
                conn.register("inc_tmp", new_df)
                conn.execute(f"""
                    DELETE FROM bars
                    WHERE universe = '{universe}'
                      AND (ticker, date) IN (SELECT ticker, date FROM inc_tmp)
                """)
                # Assign unique IDs (DuckDB has no auto-increment for INSERT…SELECT).
                # Without this, new rows get id=NULL → enrichment UPDATE … WHERE id=…
                # silently matches nothing, leaving avg_vol_20d / rsi_14 / scores NULL.
                max_id = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM bars"
                ).fetchone()[0]
                cols_no_id = [c for c in new_df.columns if c != "id"]
                cols_str   = ", ".join(["id"] + cols_no_id)
                src_str    = ", ".join(
                    [f"ROW_NUMBER() OVER () + {max_id} AS id"] + cols_no_id
                )
                conn.execute(
                    f"INSERT INTO bars ({cols_str}) SELECT {src_str} FROM inc_tmp"
                )
                conn.unregister("inc_tmp")
                conn.commit()
                rows_inserted = len(new_df)
                log.info("  Inserted %d new rows for %s", rows_inserted, universe)
            except Exception as e:
                log.exception("INSERT failed for %s", universe)
                errors.append({"stage": "insert", "error": str(e)})
            finally:
                conn.close()

        # ── Re-enrich the affected universe (idempotent — fast) ─────────────
        if rows_inserted > 0:
            _write_progress(f"enriching {universe}", done, total, started,
                            extra={"new_rows": rows_inserted})
            try:
                enrich_summary = enrich_universe(universe=universe, max_workers=1)
            except Exception as e:
                log.exception("enrich failed for %s", universe)
                enrich_summary = {"error": str(e)}
        else:
            enrich_summary = {"skipped": "no new rows"}

        overall_summary["universes"][universe] = {
            "tickers_checked":   total,
            "new_rows_inserted": rows_inserted,
            "affected_tickers":  len(affected_tickers),
            "errors":            len(errors),
            "error_samples":     errors[:5],
            "enrich":            enrich_summary,
        }

    overall_summary["duration_sec"] = round(time.time() - started, 1)
    _write_progress("done", 0, 0, started, extra={"summary": overall_summary})
    log.info("Incremental refresh complete: %s", overall_summary)
    return overall_summary


def get_progress() -> dict:
    if not os.path.exists(PROGRESS_FILE):
        return {"stage": "idle", "done": 0, "total": 0, "pct": 0}
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"stage": "unknown", "done": 0, "total": 0, "pct": 0}
