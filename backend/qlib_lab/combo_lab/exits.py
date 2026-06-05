"""
exits.py — for each passing combo, grid-search (stop_atr, target_atr, time_stop).

Simulation rules (look-ahead-safe, matches the journal's execution-realism):
  - Entry: next bar's OPEN (after the signal date), with slippage_bps applied.
  - Stop / target measured from entry, in units of ATR(at signal date).
  - First-touch wins; if both within a bar, conservative (stop first).
  - Time stop: close N bars after entry if neither hit.

Best grid point chosen on TRAIN by realized_avg, validated OOS by the same
metric. We store the chosen exits in combo_catalog.
"""
from __future__ import annotations

import time
import logging
import duckdb

from ai_journal.db import get_analytics_conn, get_journal_conn

log = logging.getLogger(__name__)

STOP_ATRS   = [1.0, 1.5, 2.0]
TARGET_ATRS = [1.5, 2.5, 4.0]
HOLD_DAYS   = [3, 5, 7, 10, 15]
SLIPPAGE_BPS = 5     # 5 bps each side (0.05% in + 0.05% out = 0.1% round-trip cost)


def _signal_dates(conn, sql: str, start: str, end: str) -> list:
    """Returns [(ticker, universe, date, atr, open_next)] for every signal day."""
    return conn.execute(f"""
        WITH sig AS (
            SELECT ticker, universe, date, atr_14
            FROM bars
            WHERE date BETWEEN ? AND ?
              AND ({sql})
              AND atr_14 IS NOT NULL AND atr_14 > 0
        ),
        nxt AS (
            SELECT s.ticker, s.universe, s.date AS sig_date, s.atr_14,
                   b.date AS open_date, b.open AS entry_px
            FROM sig s
            LEFT JOIN LATERAL (
                SELECT date, open FROM bars b
                WHERE b.ticker = s.ticker AND b.universe = s.universe AND b.date > s.date
                ORDER BY b.date ASC LIMIT 1
            ) b ON true
        )
        SELECT ticker, universe, sig_date, atr_14, open_date, entry_px
        FROM nxt WHERE entry_px IS NOT NULL AND entry_px > 0
    """, [start, end]).fetchall()


def _forward_bars(conn, ticker: str, universe: str, after: str, n: int):
    return conn.execute("""
        SELECT date, open, high, low, close FROM bars
        WHERE ticker=? AND universe=? AND date >= ?
        ORDER BY date ASC LIMIT ?""", [ticker, universe, after, n]).fetchall()


def _simulate(entries: list, conn, stop_atr: float, target_atr: float, hold: int) -> dict:
    """Walks each entry through up to `hold` daily bars; first-touch close.
    Returns aggregate {n, win_rate, avg_pnl_pct}."""
    pnls = []
    slip = SLIPPAGE_BPS / 10000.0
    for (ticker, universe, sig_date, atr, open_date, entry) in entries:
        e = entry * (1 + slip)              # buy at open + slippage
        stop = e - stop_atr * atr
        targ = e + target_atr * atr
        bars = _forward_bars(conn, ticker, universe, str(open_date), hold)
        exit_px = None
        for (_d, o, h, l, c) in bars:
            if l is not None and l <= stop:
                exit_px = stop; break
            if h is not None and h >= targ:
                exit_px = targ; break
        if exit_px is None and bars:
            exit_px = bars[-1][4]           # mark-to-last close
        if exit_px is None:
            continue
        exit_px = exit_px * (1 - slip)
        pnls.append((exit_px - e) / e * 100.0)
    if not pnls:
        return {"n": 0, "win_rate": None, "avg_pnl": None}
    return {"n": len(pnls),
            "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
            "avg_pnl":  sum(pnls) / len(pnls)}


def optimize_exits_for_combo(combo_id: str, predicates_sql: str,
                             train=("2021-01-01", "2024-12-31"),
                             oos=("2025-01-01", "2026-06-04"),
                             max_signals: int = 4000) -> dict:
    """Grid-search exits on TRAIN, validate on OOS. max_signals caps sample for
    speed (random tail-sampling at SQL would be cleaner — TODO if needed)."""
    a = get_analytics_conn()
    try:
        tr = _signal_dates(a, predicates_sql, *train)
        oo = _signal_dates(a, predicates_sql, *oos)
        if max_signals and len(tr) > max_signals:
            # uniform sampling, deterministic
            step = max(1, len(tr) // max_signals)
            tr = tr[::step]
        if max_signals and len(oo) > max_signals:
            step = max(1, len(oo) // max_signals)
            oo = oo[::step]

        # train: grid
        best, best_avg = None, -1e9
        for s in STOP_ATRS:
            for t in TARGET_ATRS:
                for h in HOLD_DAYS:
                    r = _simulate(tr, a, s, t, h)
                    if r["avg_pnl"] is not None and r["avg_pnl"] > best_avg:
                        best_avg = r["avg_pnl"]
                        best = {"stop_atr": s, "target_atr": t, "hold_days": h, "train": r}
        if not best:
            return {"combo_id": combo_id, "skipped": True}

        # oos: same grid point
        oos_r = _simulate(oo, a, best["stop_atr"], best["target_atr"], best["hold_days"])
        best["oos"] = oos_r
    finally:
        a.close()
    return {"combo_id": combo_id, **best}


def optimize_passed_combos(limit: int = 50) -> dict:
    """Run exit-grid for the top-N passed combos in the catalog (by oos_hh_edge)."""
    t0 = time.time()
    j = get_journal_conn()
    try:
        passed = j.execute("""SELECT combo_id, predicates FROM combo_catalog
                              WHERE status='passed' ORDER BY oos_hh_edge DESC LIMIT ?""",
                           [limit]).fetchall()
    finally:
        j.close()

    from .enumerate import ATOMS
    done = 0
    for cid, preds in passed:
        sql = " AND ".join(f"({ATOMS[p]})" for p in preds.split(","))
        r = optimize_exits_for_combo(cid, sql)
        if r.get("skipped"):
            continue
        oos = r["oos"]
        j = get_journal_conn()
        try:
            j.execute("""UPDATE combo_catalog SET best_stop_atr=?, best_target_atr=?,
                         best_hold_days=?, realized_win=?, realized_avg=?,
                         revalidated_at=current_timestamp WHERE combo_id=?""",
                      [r["stop_atr"], r["target_atr"], r["hold_days"],
                       oos.get("win_rate"), oos.get("avg_pnl"), cid])
            j.commit()
        finally:
            j.close()
        done += 1
        log.info("  combo %s [%s]: best stop=%.1f target=%.1f hold=%d → OOS win=%.1f%% avg=%+.2f%%",
                 cid, preds, r["stop_atr"], r["target_atr"], r["hold_days"],
                 (oos.get("win_rate") or 0) * 100, oos.get("avg_pnl") or 0)
    return {"optimized": done, "duration_sec": round(time.time() - t0, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(optimize_passed_combos(limit=20))
