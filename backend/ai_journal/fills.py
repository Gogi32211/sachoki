"""
ai_journal/fills.py — fill PENDING_OPEN positions at the NEXT session's OPEN.

Enforces the execution-realism rule: a BUY decided while the exchange was closed
cannot be filled at the decision price — it is entered at the open of the next
trading session. We take that open from the first daily bar AFTER the decision
date (deterministic, look-ahead-safe). Stop/target are computed from the realized
open + the ATR snapshot taken at decision time.

Run:  python -m ai_journal.fills
"""
from __future__ import annotations

import time
import logging

from .db import get_analytics_conn, get_journal_conn
from . import rails

log = logging.getLogger(__name__)


def fill_pending_open() -> dict:
    t0 = time.time()
    j = get_journal_conn(read_only=False)
    a = get_analytics_conn()
    filled, still_pending = 0, 0
    try:
        rows = j.execute(
            """SELECT id, ticker, universe, decision_date, size_pct, atr_at_decision
               FROM journal_position WHERE status = 'PENDING_OPEN'"""
        ).fetchall()
        capital = float(j.execute("SELECT capital FROM journal_state WHERE id=1").fetchone()[0])
        for pid, ticker, universe, ddate, size_pct, atr in rows:
            nxt = a.execute(
                """SELECT date, open FROM bars
                   WHERE ticker=? AND universe=? AND date > ?
                   ORDER BY date ASC LIMIT 1""",
                [ticker, universe or "sp500", str(ddate)[:10]],
            ).fetchone()
            if not nxt or nxt[1] is None:
                still_pending += 1     # next session not in the DB yet → stay pending
                continue
            fill_date, open_px = nxt[0], float(nxt[1])
            stop, target = rails.stop_target(open_px, float(atr or 0))
            shares = round(capital * float(size_pct or 0) / open_px, 4) if open_px else 0
            j.execute(
                """UPDATE journal_position
                   SET status='OPEN', entry_px=?, shares=?, stop_px=?, target_px=?,
                       opened_at=current_timestamp, filled_date=?
                   WHERE id=?""",
                [open_px, shares, stop, target, fill_date, pid],
            )
            filled += 1
        j.commit()
    finally:
        a.close()
        j.close()
    dur = time.time() - t0
    log.info("filled %d pending-open at next-session open (%d still pending) in %.1fs",
             filled, still_pending, dur)
    return {"filled": filled, "still_pending": still_pending, "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(fill_pending_open())
