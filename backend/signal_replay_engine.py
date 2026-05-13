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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any

from db import get_db, USE_PG

log = logging.getLogger(__name__)

# Module-level state (single concurrent run)
_state: dict[str, Any] = {
    "running":              False,
    "run_id":               None,
    "status":               "idle",
    "mode":                 None,
    "universe":             None,
    "current_date":         None,
    "days_total":           0,
    "days_completed":       0,
    "symbols_total":        0,
    "symbols_completed":    0,
    "events_found":         0,
    "outcomes_computed":    0,
    "statistics_rows":      0,
    "pattern_rows":         0,
    "filter_impact_rows":   0,
    "combo_rows":           0,
    "unique_symbols":       0,
    "unique_symbol_dates":  0,
    "unique_tz_events":     0,
    "unique_combo_events":  0,
    "started_at":           None,
    "elapsed_secs":         0,
    "error":                None,
    "stop_requested":       False,
    "pause_requested":      False,
}


def get_state() -> dict:
    return dict(_state)


def _set(**kv) -> None:
    _state.update(kv)


def request_stop() -> None:
    _state["stop_requested"] = True


def request_pause() -> None:
    _state["pause_requested"] = True


def request_resume() -> None:
    _state["pause_requested"] = False


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


_PG_CHUNK = 500  # rows per INSERT for PG (avoids param-limit issues with wide tables)


def _bulk_insert(table: str, rows: list[dict]) -> list[int]:
    """Insert rows and return their generated IDs (in same order)."""
    if not rows:
        return []
    cols = list(rows[0].keys())
    ph = _ph()
    placeholders = "(" + ", ".join([ph] * len(cols)) + ")"

    ids: list[int] = []
    with get_db() as db:
        if USE_PG:
            for start in range(0, len(rows), _PG_CHUNK):
                chunk = rows[start : start + _PG_CHUNK]
                sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES "
                       + ", ".join([placeholders] * len(chunk))
                       + " RETURNING id")
                flat: list[Any] = []
                for r in chunk:
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

    Modes:
      single_day  → [as_of_date]
      date_range  → market dates in [start_date, end_date]
      last_n_days → last N trading days available in sample_bars
      ytd         → Jan 1 of current year through today
    """
    mode = payload["mode"]
    today_str = date.today().strftime("%Y-%m-%d")

    if mode == "single_day":
        return [_normalize_date(payload["as_of_date"])]

    all_bar_dates = sorted(set(
        d for b in sample_bars
        if (d := _normalize_date(b.get("date")))
    ))

    if mode == "last_n_days":
        n = max(1, int(payload.get("lookback_days") or 20))
        return all_bar_dates[-n:] if len(all_bar_dates) >= n else all_bar_dates

    if mode == "ytd":
        start = f"{date.today().year}-01-01"
        end   = today_str
        return [d for d in all_bar_dates if start <= d <= end]

    # date_range
    start = _normalize_date(payload.get("start_date"))
    end   = _normalize_date(payload.get("end_date"))
    if not start or not end:
        return []
    return [d for d in all_bar_dates if start <= d <= end]


# ─── Per-ticker worker (pure compute, no DB) ──────────────────────────────────

_WORKERS = 8  # parallel ticker fetch + extract; DB writes stay serialized


def _process_ticker_for_replay(
    ticker: str,
    *,
    scan_dates: list[str],
    is_gt5: bool,
    min_price: float | None,
    min_volume: int | None,
    min_dollar_volume: float | None,
    lookback_bars: int,
    run_id: int,
    universe: str,
    spy_bars: list[dict],
    qqq_bars: list[dict],
) -> tuple[list[dict], list[tuple[int, list[dict]]], dict]:
    """Fetch bars, extract events, compute outcomes for one ticker. No DB.

    Returns (event_rows, [(event_offset_within_returned_list, outcomes), ...], counters).
    """
    from main import api_bar_signals
    from signal_event_extractor import extract_events
    from signal_outcome_engine import compute_outcomes

    counters = {
        "unique_symbol_dates": set(),
        "tz_events":          0,
        "combo_events":       0,
    }

    try:
        bars = api_bar_signals(ticker, "1d", lookback_bars)
    except Exception as fetch_err:
        log.debug("signal_replay: bar fetch failed for %s: %s", ticker, fetch_err)
        return ([], [], counters)

    if not bars or len(bars) < 60:
        return ([], [], counters)

    date_to_idx: dict[str, int] = {}
    for i, b in enumerate(bars):
        d = _normalize_date(b.get("date"))
        if d:
            date_to_idx[d] = i

    event_rows: list[dict] = []
    outcome_offsets: list[tuple[int, list[dict]]] = []

    for scan_date in scan_dates:
        idx = date_to_idx.get(scan_date)
        if idx is None or idx < 30:
            continue

        if is_gt5:
            close = bars[idx].get("close")
            if close is None or close < 5:
                continue

        bar_close  = float(bars[idx].get("close")  or 0)
        bar_volume = float(bars[idx].get("volume") or 0)
        if min_price is not None and bar_close < min_price:
            continue
        if min_volume is not None and bar_volume < min_volume:
            continue
        if min_dollar_volume is not None and (bar_close * bar_volume) < min_dollar_volume:
            continue

        events = extract_events(
            bars, idx,
            ticker=ticker, universe=universe, replay_run_id=run_id,
        )
        if not events:
            continue

        future_bars = bars[idx + 1 : idx + 22]
        spy_future  = _future_window(spy_bars, scan_date, 22)
        qqq_future  = _future_window(qqq_bars, scan_date, 22)

        base_offset = len(event_rows)
        for ev in events:
            event_rows.append(ev)
            counters["unique_symbol_dates"].add((ticker, scan_date))
            ev_sig  = ev.get("event_signal") or ""
            sig_fam = ev.get("event_signal_family") or ""
            if ev_sig.startswith(("T", "Z")):
                counters["tz_events"] += 1
            elif sig_fam in ("COMBO", "L", "F", "G", "B", "EMA"):
                counters["combo_events"] += 1

        for i, ev in enumerate(events):
            ocs = compute_outcomes(
                ev, future_bars,
                spy_future=spy_future, qqq_future=qqq_future,
                replay_run_id=run_id,
            )
            outcome_offsets.append((base_offset + i, ocs))

    return (event_rows, outcome_offsets, counters)


# ─── Main run loop ────────────────────────────────────────────────────────────

def run_signal_replay(run_id: int, payload: dict) -> None:
    """Background entrypoint. Updates _state + DB as it goes."""
    from scanner import get_universe_tickers
    from main import api_bar_signals
    from signal_event_extractor import extract_events
    from signal_outcome_engine import compute_outcomes
    from signal_statistics_engine import build_signal_statistics
    from signal_pattern_engine import build_pattern_statistics
    from signal_filter_impact_engine import build_filter_impact_statistics
    from signal_combo_engine import build_combo_statistics

    _set(running=True, run_id=run_id, status="running",
         mode=payload.get("mode"), universe=payload.get("universe"),
         current_date=None,
         days_total=0, days_completed=0,
         symbols_total=0, symbols_completed=0,
         events_found=0, outcomes_computed=0, statistics_rows=0,
         pattern_rows=0, filter_impact_rows=0, combo_rows=0,
         unique_symbols=0, unique_symbol_dates=0,
         unique_tz_events=0, unique_combo_events=0,
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

        # Configurable bar lookback: 500/1000/1500/2000; clamped to [200, 2000]
        lookback_bars = max(200, min(2000, int(payload.get("lookback_bars") or 500)))

        # Per-bar filters from RunRequest
        min_price         = payload.get("min_price")
        min_volume        = payload.get("min_volume")
        min_dollar_volume = payload.get("min_dollar_volume")

        # Pre-fetch benchmark series (SPY + QQQ) once for the whole run
        try:
            spy_bars = api_bar_signals("SPY", "1d", lookback_bars)
        except Exception:
            spy_bars = []
        try:
            qqq_bars = api_bar_signals("QQQ", "1d", lookback_bars)
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
        unique_symbols_set:       set[str]          = set()
        unique_symbol_dates_set:  set[tuple]        = set()
        unique_tz_events_count:   int               = 0
        unique_combo_events_count: int              = 0

        # Parallel ticker processing: 8 workers fetch + extract concurrently.
        # DB writes stay in this (main) thread to avoid lock contention.
        worker_kwargs = dict(
            scan_dates=scan_dates, is_gt5=is_gt5,
            min_price=min_price, min_volume=min_volume,
            min_dollar_volume=min_dollar_volume,
            lookback_bars=lookback_bars, run_id=run_id,
            universe=universe, spy_bars=spy_bars, qqq_bars=qqq_bars,
        )
        completed_symbols = 0
        log.info("signal_replay: starting parallel processing with %d workers", _WORKERS)

        with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
            futures = {
                pool.submit(_process_ticker_for_replay, t, **worker_kwargs): t
                for t in tickers
            }

            for fut in as_completed(futures):
                # ── Pause: hold this result until resumed ─────────────────
                while _state.get("pause_requested"):
                    _set(elapsed_secs=round(time.time() - t0, 1))
                    time.sleep(0.5)

                # ── Stop: cancel pending and exit ─────────────────────────
                if _state.get("stop_requested"):
                    log.info("signal_replay: stop requested; cancelling pending futures")
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                ticker = futures[fut]
                try:
                    event_rows, outcome_offsets, counters = fut.result()
                except Exception as exc:
                    log.debug("signal_replay: %s worker raised: %s", ticker, exc)
                    event_rows, outcome_offsets, counters = [], [], {
                        "unique_symbol_dates": set(),
                        "tz_events": 0, "combo_events": 0,
                    }

                # Insert events + link outcomes (main-thread serialized)
                if event_rows:
                    ev_ids = _bulk_insert("replay_signal_events", event_rows)
                    for offset, ocs in outcome_offsets:
                        if offset >= len(ev_ids):
                            continue
                        ev_id = ev_ids[offset]
                        for oc in ocs:
                            oc["signal_event_id"] = ev_id
                            all_outcomes.append(oc)
                    all_events.extend([
                        {**ev, "id": ev_ids[i]}
                        for i, ev in enumerate(event_rows)
                    ])
                    events_total += len(event_rows)
                    unique_symbols_set.add(ticker)

                unique_symbol_dates_set.update(counters.get("unique_symbol_dates") or set())
                unique_tz_events_count    += counters.get("tz_events", 0)
                unique_combo_events_count += counters.get("combo_events", 0)

                completed_symbols += 1
                _set(symbols_completed=completed_symbols,
                     events_found=events_total,
                     unique_symbols=len(unique_symbols_set),
                     unique_symbol_dates=len(unique_symbol_dates_set),
                     unique_tz_events=unique_tz_events_count,
                     unique_combo_events=unique_combo_events_count,
                     elapsed_secs=round(time.time() - t0, 1))

        # Flush outcomes
        if all_outcomes:
            _bulk_insert("replay_signal_outcomes", all_outcomes)
            outcomes_total = len(all_outcomes)

        _set(outcomes_computed=outcomes_total, days_completed=len(scan_dates),
             elapsed_secs=round(time.time() - t0, 1))

        # Aggregate statistics (Phase 1)
        events_for_stats = [{
            "id": ev["id"],
            "event_signal":        ev.get("event_signal"),
            "event_signal_family": ev.get("event_signal_family"),
            "event_signal_type":   ev.get("event_signal_type"),
            "event_direction":     ev.get("event_direction"),
            "role":                ev.get("role"),
            "matched_status":      ev.get("matched_status"),
            "score_bucket":        ev.get("score_bucket"),
            "symbol":              ev.get("symbol"),
            "scan_date":           ev.get("scan_date"),
        } for ev in all_events]

        stats_rows = build_signal_statistics(
            events_for_stats, all_outcomes, replay_run_id=run_id,
        )
        if stats_rows:
            _bulk_insert("replay_signal_statistics", stats_rows)
        _set(statistics_rows=len(stats_rows),
             elapsed_secs=round(time.time() - t0, 1))

        # Phase 2: pattern statistics (sequences)
        events_for_patterns = [{
            "id":             ev["id"],
            "event_signal":   ev.get("event_signal"),
            "sequence_2bar":  ev.get("sequence_2bar"),
            "sequence_3bar":  ev.get("sequence_3bar"),
            "sequence_4bar":  ev.get("sequence_4bar"),
            "sequence_5bar":  ev.get("sequence_5bar"),
            "sequence_7bar":  ev.get("sequence_7bar"),
            "sequence_10bar": ev.get("sequence_10bar"),
        } for ev in all_events]

        pattern_rows = build_pattern_statistics(
            events_for_patterns, all_outcomes, replay_run_id=run_id,
        )
        if pattern_rows:
            _bulk_insert("replay_pattern_statistics", pattern_rows)
        _set(pattern_rows=len(pattern_rows),
             elapsed_secs=round(time.time() - t0, 1))

        # Phase 2: filter impact statistics (signal × context)
        events_for_filters = [{
            "id":                       ev["id"],
            "event_signal":             ev.get("event_signal"),
            "event_signal_family":      ev.get("event_signal_family"),
            "ema50_state":              ev.get("ema50_state"),
            "volume_bucket":            ev.get("volume_bucket"),
            "abr_category":             ev.get("abr_category"),
            "candle_color":             ev.get("candle_color"),
            "price_pos_20bar_bucket":   ev.get("price_pos_20bar_bucket"),
            "score_bucket":             ev.get("score_bucket"),
            "had_t_last_3d":            ev.get("had_t_last_3d"),
            "had_z_last_3d":            ev.get("had_z_last_3d"),
            "had_wlnbb_l_last_5d":      ev.get("had_wlnbb_l_last_5d"),
            "had_ema50_reclaim_last_5d": ev.get("had_ema50_reclaim_last_5d"),
            "had_volume_burst_last_5d": ev.get("had_volume_burst_last_5d"),
        } for ev in all_events]

        filter_rows = build_filter_impact_statistics(
            events_for_filters, all_outcomes, replay_run_id=run_id,
        )
        if filter_rows:
            _bulk_insert("replay_filter_impact_statistics", filter_rows)
        _set(filter_impact_rows=len(filter_rows),
             elapsed_secs=round(time.time() - t0, 1))

        # Multi-context combination statistics (stored in replay_signal_statistics)
        events_for_combos = [{
            "id":                     ev["id"],
            "event_signal":           ev.get("event_signal"),
            "event_signal_family":    ev.get("event_signal_family"),
            "sequence_4bar":          ev.get("sequence_4bar"),
            "abr_category":           ev.get("abr_category"),
            "ema50_state":            ev.get("ema50_state"),
            "had_wlnbb_l_last_5d":    ev.get("had_wlnbb_l_last_5d"),
            "price_pos_20bar_bucket": ev.get("price_pos_20bar_bucket"),
            "score_bucket":           ev.get("score_bucket"),
            "volume_bucket":          ev.get("volume_bucket"),
            "candle_color":           ev.get("candle_color"),
            "symbol":                 ev.get("symbol"),
            "scan_date":              ev.get("scan_date"),
        } for ev in all_events]

        combo_rows = build_combo_statistics(
            events_for_combos, all_outcomes, replay_run_id=run_id,
        )
        if combo_rows:
            _bulk_insert("replay_signal_statistics", combo_rows)
        _set(combo_rows=len(combo_rows),
             elapsed_secs=round(time.time() - t0, 1))

        final_status = "stopped" if _state.get("stop_requested") else "completed"
        _update_run(
            run_id,
            status=final_status,
            total_days=len(scan_dates),
            days_completed=_state.get("days_completed", 0),
            total_symbols=len(tickers),
            symbols_completed=_state.get("symbols_completed", 0),
            total_events=events_total, total_outcomes=outcomes_total,
            total_statistics_rows=len(stats_rows) + len(combo_rows),
        )
        _set(
            unique_symbols=len(unique_symbols_set),
            unique_symbol_dates=len(unique_symbol_dates_set),
            unique_tz_events=unique_tz_events_count,
            unique_combo_events=unique_combo_events_count,
        )
        _finalize_finished_at(run_id)
        _set(status=final_status, running=False,
             stop_requested=False, pause_requested=False,
             elapsed_secs=round(time.time() - t0, 1))

    except Exception as exc:
        tb = traceback.format_exc()
        log.error("signal_replay failed: %s\n%s", exc, tb)
        _set(status="failed", running=False, error=str(exc),
             stop_requested=False, pause_requested=False,
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
