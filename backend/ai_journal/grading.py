"""
ai_journal/grading.py — deterministic outcome grader. NO LLM.

For every OPEN position whose horizon has elapsed, replay the daily bars AFTER the
decision date and resolve the exit by the rails (stop / target / time-stop), using
realistic fills (next bars' OHLC). Writes pnl / verdict back to journal_position.

This is the objective half of the learning loop — the agent never grades itself.

Run:  python -m ai_journal.grading
"""
from __future__ import annotations

import time
import logging
from datetime import date as _date

from .db import get_analytics_conn, get_journal_conn

log = logging.getLogger(__name__)


def _bars_after(a, ticker: str, universe: str, after: str, limit: int = 30):
    """Daily bars strictly AFTER the decision date (chronological)."""
    return a.execute(
        """SELECT date, open, high, low, close
           FROM bars
           WHERE ticker = ? AND universe = ? AND date > ?
           ORDER BY date ASC LIMIT ?""",
        [ticker, universe, after, limit],
    ).fetchall()


def _resolve_exit(bars, entry_px, stop_px, target_px, horizon_days):
    """Walk daily bars; return (exit_date, exit_px, reason). Intrabar priority:
    if a bar's low<=stop AND high>=target we assume STOP first (conservative)."""
    for i, (d, o, h, l, c) in enumerate(bars):
        if stop_px is not None and l is not None and l <= stop_px:
            return d, stop_px, "stop"
        if target_px is not None and h is not None and h >= target_px:
            return d, target_px, "target"
        if (i + 1) >= horizon_days:
            return d, c, "time"
    if bars:
        d, o, h, l, c = bars[-1]
        return d, c, "time"      # ran out of data → mark-to-last
    return None, None, None       # no forward bars yet → still pending


def grade_open_positions() -> dict:
    """Resolve every OPEN position whose horizon has elapsed. Returns summary."""
    t0 = time.time()
    j = get_journal_conn(read_only=False)
    a = get_analytics_conn()
    graded = 0
    try:
        rows = j.execute(
            """SELECT id, ticker, universe, decision_date, entry_px, shares,
                      stop_px, target_px, horizon_days
               FROM journal_position WHERE status = 'OPEN'"""
        ).fetchall()
        for (pid, ticker, universe, ddate, entry_px, shares,
             stop_px, target_px, horizon) in rows:
            bars = _bars_after(a, ticker, universe or "sp500", str(ddate)[:10],
                               limit=max(30, (horizon or 5) + 5))
            if not bars or len(bars) < (horizon or 5):
                continue   # not enough forward bars yet → leave PENDING/OPEN
            exit_date, exit_px, reason = _resolve_exit(
                bars, entry_px, stop_px, target_px, horizon or 5)
            if exit_px is None:
                continue
            pnl_pct = (exit_px - entry_px) / entry_px * 100.0 if entry_px else 0.0
            pnl = (exit_px - entry_px) * (shares or 0)
            verdict = "WIN" if pnl_pct > 0.5 else ("LOSS" if pnl_pct < -0.5 else "FLAT")
            j.execute(
                """UPDATE journal_position
                   SET status='CLOSED', closed_at=current_timestamp, exit_date=?,
                       exit_px=?, exit_reason=?, pnl=?, pnl_pct=?, verdict=?
                   WHERE id=?""",
                [exit_date, exit_px, reason, pnl, pnl_pct, verdict, pid],
            )
            graded += 1
        j.commit()
    finally:
        a.close()
        j.close()
    dur = time.time() - t0
    log.info("graded %d positions in %.1fs", graded, dur)
    return {"graded": graded, "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(grade_open_positions())
