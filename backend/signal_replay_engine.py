"""
signal_replay_engine.py — orchestrator for the Signal Replay / Research engine.

Pipeline (Phase 1, daily / 1d only):
  1. Create signal_replay_runs row, status='running'.
  2. Resolve universe tickers (with optional historical close>=5 filter).
  3. For each ticker, fetch enriched bar series via api_bar_signals(ticker, "1d", N).
  4. For each scan_date (single_day or date_range), find the bar with that date
     and extract events using bars[: idx+1] (leak-free).
  5. Compute outcomes for each event using bars[idx+1 : idx+21] (forward only),
     with SPY/QQQ benchmark slices.
  6. Insert events + outcomes (batched).
  7. Aggregate replay_signal_statistics rows.
  8. Mark run completed.

Concurrency:
  - One run at a time. _state.running is the lock.
  - FastAPI BackgroundTasks invokes `run_signal_replay(run_id)`.
  - HTTP polls `get_state()` for progress.

NEVER reads forward bars during event extraction.
NEVER writes replay data into live scanner tables.
"""
from __future__ import annotations
import json
import logging
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any

from db import get_db, USE_PG

log = logging.getLogger(__name__)

# Module-level state (single concurrent run)
_state: dict[str, Any] = {
    "running":           False,
    "run_id":            None,
    "status":            "idle",
    "mode":              None,
    "universe":          None,
    "current_date":      None,
    "days_total":        0,
    "days_completed":    0,
    "symbols_total":     0,
    "symbols_completed": 0,
    "events_found":      0,
    "outcomes_computed": 0,
    "statistics_rows":   0,
    "started_at":        None,
    "elapsed_secs":      0,
    "error":             None,
}


def get_state() -> dict:
    return dict(_state)


def _set(**kv) -> None:
    _state.update(kv)


def _ph() -> str:
    """Return param placeholder for current DB driver."""
    return "%s" if USE_PG else "?"


# ─── DB helpers ───────────────────────────────────────────────────────────────

def _insert_run(payload: dict) -> int:
    cols = [
        "status", "mode", "universe", "timeframe", "as_of_date",
        "start_date", "end_date", "event_scope", "min_price",
        "min_volume", "min_dollar_volume", "benchmark_symbol",
        "settings_json",
    ]
    vals = [
        "running", payload["mode"], payload["universe"], "1d",
        payload.get("as_of_date"), payload.get("start_date"),
        payload.get("end_date"), payload.get("event_scope", "all_signals"),
        payload.get("min_price"), payload.get("min_volume"),
        payload.get("min_dollar_volume"), payload.get("benchmark_symbol", "QQQ"),
        json.dumps(payload, default=str),
    ]
    ph = _ph()
    sql = (f"INSERT INTO signal_replay_runs ({', '.join(cols)}) "
           f"VALUES ({', '.join([ph]*len(cols))}) "
           f"{'RETURNING id' if USE_PG else ''}")
    with get_db() as db:
        if USE_PG:
            db.execute(sql, vals)
            row = db.fetchone()
            run_id = row["id"] if isinstance(row, dict) else row[0]
        else:
            cur = db.execute(sql, vals)
            run_id = cur.lastrowid
        db.commit()
    return int(run_id)


def _update_run(run_id: int, **fields) -> None:
    if not fields:
        return
    ph = _ph()
    sets = ", ".join(f"{k}={ph}" for k in fields.keys())
    now_expr = "NOW()" if USE_PG else "datetime('now')"
    sets += f", updated_at={now_expr}"
    sql = f"UPDATE signal_replay_runs SET {sets} WHERE id={ph}"
    vals = list(fields.values()) + [run_id]
    try:
        with get_db() as db:
            db.execute(sql, vals)
            db.commit()
    except Exception as exc:
        log.warning("signal_replay_runs update failed: %s", exc)


def _bulk_insert(table: str, rows: list[dict]) -> list[int]:
    """Insert rows and return their generated IDs (in same order)."""
    if not rows:
        return []
    cols = [c for c in rows[0].keys()]
    ph = _ph()
    placeholders = "(" + ", ".join([ph] * len(cols)) + ")"

    # PG: insert with RETURNING id (single multi-row insert)
    # SQLite: insert one row at a time (lastrowid)
    ids: list[int] = []
    with get_db() as db:
        if USE_PG:
            sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES "
                   + ", ".join([placeholders] * len(rows))
                   + " RETURNING id")
            flat: list[Any] = []
            for r in rows:
                flat.extend(r[c] for c in cols)
            db.execute(sql, flat)
            res = db.fetchall()
            for rec in res:
                ids.append(rec["id"] if isinstance(rec, dict) else rec[0])
        else:
            sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
                   f"VALUES {placeholders}")
            for r in rows:
                cur = db.execute(sql, [r[c] for c in cols])
                ids.append(cur.lastrowid)
        db.commit()
    return ids


# ─── Date list resolution ─────────────────────────────────────────────────────

def _normalize_date(d: Any) -> str | None:
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    s = str(d)[:10]
    return s if s else None


def _resolve_scan_dates(payload: dict, sample_bars: list[dict]) -> list[str]:
    """Returns sorted list of YYYY-MM-DD strings.

    For single_day: [as_of_date].
    For date_range: market dates from sample_bars that fall in [start_date, end_date].
    """
    mode = payload["mode"]
    if mode == "single_day":
        return [_normalize_date(payload["as_of_date"])]
    start = _normalize_date(payload.get("start_date"))
    end   = _normalize_date(payload.get("end_date"))
    if not start or not end:
        return []
    dates = []
    for b in sample_bars:
        d = _normalize_date(b.get("date"))
        if d and start <= d <= end:
            dates.append(d)
    return sorted(set(dates))


# ─── Main run loop ────────────────────────────────────────────────────────────

def run_signal_replay(run_id: int, payload: dict) -> None:
    """Background entrypoint. Updates _state + DB as it goes."""
    from scanner import get_universe_tickers
    from main import api_bar_signals
    from signal_event_extractor import extract_events
    from signal_outcome_engine import compute_outcomes
    from signal_statistics_engine import build_signal_statistics

    _set(running=True, run_id=run_id, status="running",
         mode=payload.get("mode"), universe=payload.get("universe"),
         current_date=None,
         days_total=0, days_completed=0,
         symbols_total=0, symbols_completed=0,
         events_found=0, outcomes_computed=0, statistics_rows=0,
         started_at=time.time(), elapsed_secs=0, error=None)
    t0 = time.time()

    try:
        universe = payload["universe"]
        # Universe loading. nasdaq_gt5 is "nasdaq" filtered to close>=5 on scan_date
        is_gt5 = universe == "nasdaq_gt5"
        scanner_universe = "nasdaq" if is_gt5 else universe
        tickers = list(get_universe_tickers(scanner_universe))
        log.info("signal_replay: %s universe → %d tickers", universe, len(tickers))
        _set(symbols_total=len(tickers))

        # Pre-fetch benchmark series (SPY + QQQ) once for the whole run
        try:
            spy_bars = api_bar_signals("SPY", "1d", 500)
        except Exception:
            spy_bars = []
        try:
            qqq_bars = api_bar_signals("QQQ", "1d", 500)
        except Exception:
            qqq_bars = []

        # Resolve scan dates. For date_range we need a sample bar series to know
        # which calendar dates were trading days.
        sample_bars = spy_bars or qqq_bars or []
        scan_dates = _resolve_scan_dates(payload, sample_bars)
        _set(days_total=len(scan_dates))

        events_total = 0
        outcomes_total = 0
        all_events: list[dict] = []
        all_outcomes: list[dict] = []

        for sym_idx, ticker in enumerate(tickers):
            try:
                bars = api_bar_signals(ticker, "1d", 500)
            except Exception as fetch_err:
                log.debug("signal_replay: bar fetch failed for %s: %s", ticker, fetch_err)
                _set(symbols_completed=sym_idx + 1,
                     elapsed_secs=round(time.time() - t0, 1))
                continue

            if not bars or len(bars) < 60:
                _set(symbols_completed=sym_idx + 1,
                     elapsed_secs=round(time.time() - t0, 1))
                continue

            # Map date → index for fast lookup
            date_to_idx: dict[str, int] = {}
            for i, b in enumerate(bars):
                d = _normalize_date(b.get("date"))
                if d:
                    date_to_idx[d] = i

            event_rows_for_ticker: list[dict] = []
            outcome_rows_for_ticker: list[tuple[int, dict]] = []  # (event_idx_within_batch, oc)

            for scan_date in scan_dates:
                idx = date_to_idx.get(scan_date)
                if idx is None or idx < 30:
                    continue

                # nasdaq_gt5 historical price filter (leak-free: uses scan_date close)
                if is_gt5:
                    close = bars[idx].get("close")
                    if close is None or close < 5:
                        continue

                events = extract_events(
                    bars, idx,
                    ticker=ticker, universe=universe, replay_run_id=run_id,
                )
                if not events:
                    continue

                future_bars = bars[idx + 1 : idx + 22]
                spy_future = _future_window(spy_bars, scan_date, 22)
                qqq_future = _future_window(qqq_bars, scan_date, 22)

                for ev in events:
                    event_rows_for_ticker.append(ev)

                # Outcomes — we can't yet attach event ids; defer until events are inserted
                # Store (placeholder_offset, computed_outcomes_for_this_event)
                for ev in events:
                    ocs = compute_outcomes(
                        ev, future_bars,
                        spy_future=spy_future, qqq_future=qqq_future,
                        replay_run_id=run_id,
                    )
                    outcome_rows_for_ticker.append((len(event_rows_for_ticker) - 1, ocs))

            # Insert events for this ticker, then attach outcomes
            if event_rows_for_ticker:
                ev_ids = _bulk_insert("replay_signal_events", event_rows_for_ticker)
                for offset, ocs in outcome_rows_for_ticker:
                    if offset >= len(ev_ids):
                        continue
                    ev_id = ev_ids[offset]
                    for oc in ocs:
                        oc["signal_event_id"] = ev_id
                        all_outcomes.append(oc)
                all_events.extend([
                    {**ev, "id": ev_ids[i]}
                    for i, ev in enumerate(event_rows_for_ticker)
                ])
                events_total += len(event_rows_for_ticker)

            _set(symbols_completed=sym_idx + 1,
                 events_found=events_total,
                 elapsed_secs=round(time.time() - t0, 1))

        # Flush outcomes
        if all_outcomes:
            _bulk_insert("replay_signal_outcomes", all_outcomes)
            outcomes_total = len(all_outcomes)

        _set(outcomes_computed=outcomes_total, days_completed=len(scan_dates),
             elapsed_secs=round(time.time() - t0, 1))

        # Aggregate statistics
        events_for_stats = [{
            "id": ev["id"],
            "event_signal": ev.get("event_signal"),
            "event_signal_family": ev.get("event_signal_family"),
            "event_signal_type":   ev.get("event_signal_type"),
            "event_direction":     ev.get("event_direction"),
            "role":   ev.get("role"),
            "matched_status": ev.get("matched_status"),
            "score_bucket":   ev.get("score_bucket"),
            "symbol":    ev.get("symbol"),
            "scan_date": ev.get("scan_date"),
        } for ev in all_events]

        stats_rows = build_signal_statistics(
            events_for_stats, all_outcomes, replay_run_id=run_id,
        )
        if stats_rows:
            _bulk_insert("replay_signal_statistics", stats_rows)
        _set(statistics_rows=len(stats_rows))

        _update_run(
            run_id,
            status="completed",
            total_days=len(scan_dates), days_completed=len(scan_dates),
            total_symbols=len(tickers), symbols_completed=len(tickers),
            total_events=events_total, total_outcomes=outcomes_total,
            total_statistics_rows=len(stats_rows),
        )
        _finalize_finished_at(run_id)
        _set(status="completed", running=False,
             elapsed_secs=round(time.time() - t0, 1))

    except Exception as exc:
        tb = traceback.format_exc()
        log.error("signal_replay failed: %s\n%s", exc, tb)
        _set(status="failed", running=False, error=str(exc),
             elapsed_secs=round(time.time() - t0, 1))
        _update_run(run_id, status="failed", error_message=str(exc))
        _finalize_finished_at(run_id)


def _future_window(bars: list[dict], scan_date: str, n: int) -> list[dict]:
    """Return the n bars immediately AFTER scan_date in the given series."""
    if not bars:
        return []
    for i, b in enumerate(bars):
        d = _normalize_date(b.get("date"))
        if d and d > scan_date:
            return bars[i : i + n]
    return []


def _finalize_finished_at(run_id: int) -> None:
    """Helper because _update_run doesn't accept the NOW() expression directly."""
    ph = _ph()
    expr = "NOW()" if USE_PG else "datetime('now')"
    sql = f"UPDATE signal_replay_runs SET finished_at={expr} WHERE id={ph}"
    try:
        with get_db() as db:
            db.execute(sql, [run_id])
            db.commit()
    except Exception:
        pass
