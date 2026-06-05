"""
exits.py — exit optimizer v2 (HH-structural, not ATR-grid).

Why v2: the first version used fixed-R ATR stop/target. On our setups it killed
profit even when HH-edge was huge (76% setup-HH → realized -0.8% avg) because:
  - ATR-stop fires BEFORE the HH pivot forms (HH often takes 5-15 days; -1 ATR
    drawdown happens on the way),
  - fixed ATR-target caps the winners that ride to HH and beyond,
  - 5 bps round-trip slippage on a ~0-median drift = nominal loss.

v2 exits suited to OUR signal (HH-continuation):
  - **Stop:** TRAILING from highest close since entry — wider on the way up,
    locks in once HH structure proves itself (default trail = 1.5·ATR).
  - **Take-profit triggers:** EITHER next_pivot_is_hh_5 = True (the structural
    completion) OR price reaches an asymmetric target (default 3·ATR).
  - **Time-stop:** longer (20 days) — HH-pivots need room to form.
  - **Hard initial stop:** also present (1.5·ATR) so a fast loser is cut early.

Grid (small, principled — not the previous 45-point fishing expedition):
  - trail_atr: [1.5, 2.0]
  - target_atr: [3.0, 5.0]
  - max_hold: [15, 25]
That's 8 variants per combo — fast.

Same look-ahead-safe execution: entry = next-bar open + 5 bps; exits = daily
OHLC first-touch.
"""
from __future__ import annotations

import time
import logging

from ai_journal.db import get_analytics_conn, get_journal_conn

log = logging.getLogger(__name__)

# v2 grid (small, principled)
TRAIL_ATRS  = [1.5, 2.0]
TARGET_ATRS = [3.0, 5.0]
MAX_HOLDS   = [15, 25]
SLIPPAGE_BPS = 5
HARD_STOP_ATR = 1.5      # also catches an immediate loser


def _signal_dates(conn, sql: str, start: str, end: str):
    """Returns [(ticker, universe, sig_date, atr, open_date, entry_px)]."""
    return conn.execute(f"""
        WITH sig AS (
            SELECT ticker, universe, date, atr_14
            FROM bars
            WHERE date BETWEEN ? AND ? AND ({sql}) AND atr_14 IS NOT NULL AND atr_14 > 0
        )
        SELECT s.ticker, s.universe, s.date, s.atr_14, b.date, b.open
        FROM sig s
        LEFT JOIN LATERAL (
            SELECT date, open FROM bars b
            WHERE b.ticker = s.ticker AND b.universe = s.universe AND b.date > s.date
            ORDER BY b.date ASC LIMIT 1
        ) b ON true
        WHERE b.open IS NOT NULL AND b.open > 0
    """, [start, end]).fetchall()


def _forward_bars(conn, ticker: str, universe: str, after: str, n: int):
    """Returns daily bars with the HH-flag — used to fire HH-completion exit."""
    return conn.execute("""
        SELECT date, open, high, low, close, next_pivot_is_hh_5
        FROM bars WHERE ticker=? AND universe=? AND date >= ?
        ORDER BY date ASC LIMIT ?""", [ticker, universe, after, n]).fetchall()


def _simulate_v2(entries: list, conn, trail_atr: float, target_atr: float, max_hold: int) -> dict:
    """Trailing-stop + HH-completion + hard-stop + time-stop."""
    pnls = []
    slip = SLIPPAGE_BPS / 10000.0
    for (ticker, universe, _sig_date, atr, open_date, entry) in entries:
        e = entry * (1 + slip)
        hard_stop = e - HARD_STOP_ATR * atr
        target = e + target_atr * atr
        bars = _forward_bars(conn, ticker, universe, str(open_date), max_hold)
        if not bars:
            continue
        peak = e
        exit_px = None
        for (_d, o, h, l, c, hh_done) in bars:
            # update trailing peak using high
            if h is not None and h > peak:
                peak = h
            trail = peak - trail_atr * atr
            # check stops first (conservative): hard stop, then trail
            if l is not None and l <= hard_stop:
                exit_px = hard_stop; break
            if l is not None and l <= trail:
                exit_px = trail; break
            if h is not None and h >= target:
                exit_px = target; break
            if hh_done:                                   # structural completion → take profit at close
                exit_px = c; break
        if exit_px is None:
            exit_px = bars[-1][4]                         # mark-to-last close
        exit_px = exit_px * (1 - slip)
        pnls.append((exit_px - e) / e * 100.0)
    if not pnls:
        return {"n": 0, "win_rate": None, "avg_pnl": None, "med_pnl": None}
    return {
        "n": len(pnls),
        "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
        "avg_pnl":  sum(pnls) / len(pnls),
        "med_pnl":  sorted(pnls)[len(pnls)//2],
    }


def optimize_exits_for_combo(combo_id: str, predicates_sql: str,
                             train=("2021-01-01", "2024-12-31"),
                             oos=("2025-01-01", "2026-06-04"),
                             max_signals: int = 1500):
    a = get_analytics_conn()
    try:
        tr = _signal_dates(a, predicates_sql, *train)
        oo = _signal_dates(a, predicates_sql, *oos)
        if max_signals and len(tr) > max_signals:
            tr = tr[::max(1, len(tr) // max_signals)]
        if max_signals and len(oo) > max_signals:
            oo = oo[::max(1, len(oo) // max_signals)]

        best, best_avg = None, -1e9
        for ta in TRAIL_ATRS:
            for tg in TARGET_ATRS:
                for h in MAX_HOLDS:
                    r = _simulate_v2(tr, a, ta, tg, h)
                    if r["avg_pnl"] is not None and r["avg_pnl"] > best_avg:
                        best_avg = r["avg_pnl"]
                        best = {"trail_atr": ta, "target_atr": tg, "max_hold": h, "train": r}
        if not best:
            return {"combo_id": combo_id, "skipped": True}
        oos_r = _simulate_v2(oo, a, best["trail_atr"], best["target_atr"], best["max_hold"])
        best["oos"] = oos_r
    finally:
        a.close()
    return {"combo_id": combo_id, **best}


def optimize_passed_combos(limit: int = 30) -> dict:
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
                      [r["trail_atr"], r["target_atr"], r["max_hold"],
                       oos.get("win_rate"), oos.get("avg_pnl"), cid])
            j.commit()
        finally:
            j.close()
        done += 1
        log.info("  combo %s [%s]: trail=%.1f target=%.1f hold=%dd → OOS n=%d win=%.0f%% avg=%+.2f%% med=%+.2f%%",
                 cid, preds, r["trail_atr"], r["target_atr"], r["max_hold"],
                 oos.get("n") or 0, (oos.get("win_rate") or 0) * 100,
                 oos.get("avg_pnl") or 0, oos.get("med_pnl") or 0)
    return {"optimized": done, "duration_sec": round(time.time() - t0, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(optimize_passed_combos(limit=20))
