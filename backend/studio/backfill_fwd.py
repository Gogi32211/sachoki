"""
studio/backfill_fwd.py — Forward-return / MFE-MAE backfill for trailing bars.

WHY: forward returns (fwd_*, mfe_*, mae_* and the hit_*/drop_* flags) are computed
ONCE at bulk import (importer._add_forward_returns). When incremental_delta appends
new daily bars, the bars near each import boundary were imported with no future yet,
so their fwd_* were stored NULL — and nothing ever revisits them once the future
arrives. That silently truncates labels near the tail (a backtest-validity gap).

This module recomputes those columns with DuckDB window functions and writes them
back, but ONLY into rows whose fwd_5d IS still NULL (i.e. previously-unlabelled
trailing bars). Rows that already carry a value are NEVER touched, so the job is
purely additive and cannot corrupt existing data. Formulas mirror importer exactly:
    fwd_Nd = (LEAD(close,N) / close - 1) * 100
    mfe_Nd = (MAX(high) over next N bars / close - 1) * 100
    mae_Nd = (MIN(low)  over next N bars / close - 1) * 100
"""
from __future__ import annotations

import logging
from studio.db import get_conn

log = logging.getLogger(__name__)

_FWD_NS  = [1, 3, 5, 10, 20, 30, 60, 90]
_MFE_NS  = [5, 10, 20, 30, 60]
_MAE_NS  = [5, 10, 20, 30]


def _select_exprs() -> str:
    parts = ["id"]
    for n in _FWD_NS:
        parts.append(f"(LEAD(close, {n}) OVER w / close - 1) * 100 AS fwd_{n}d")
    for n in _MFE_NS:
        parts.append(
            f"(MAX(high) OVER (PARTITION BY ticker ORDER BY date "
            f"ROWS BETWEEN 1 FOLLOWING AND {n} FOLLOWING) / close - 1) * 100 AS mfe_{n}d"
        )
    for n in _MAE_NS:
        parts.append(
            f"(MIN(low) OVER (PARTITION BY ticker ORDER BY date "
            f"ROWS BETWEEN 1 FOLLOWING AND {n} FOLLOWING) / close - 1) * 100 AS mae_{n}d"
        )
    return ",\n               ".join(parts)


def backfill_forward_returns(lookback_days: int = 150) -> dict:
    """Fill previously-NULL forward-return labels for recent (now-resolved) bars.

    Only rows with date >= MAX(date)-lookback_days AND fwd_5d IS NULL are updated.
    Returns {candidates, updated, max_date}.
    """
    conn = get_conn(read_only=False)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        max_date = conn.execute("SELECT MAX(date) FROM bars").fetchone()[0]
        if max_date is None:
            return {"candidates": 0, "updated": 0, "max_date": None}

        # rows that should now be labelled but aren't (trailing-gap bug)
        candidates = conn.execute(
            "SELECT COUNT(*) FROM bars "
            "WHERE fwd_5d IS NULL AND date >= (SELECT MAX(date) FROM bars) - ?",
            [lookback_days],
        ).fetchone()[0]
        if candidates == 0:
            return {"candidates": 0, "updated": 0, "max_date": str(max_date)}

        # recompute over full history (window funcs need the future bars) then
        # update only the previously-NULL recent rows.
        set_cols = []
        for n in _FWD_NS:
            if f"fwd_{n}d" in available:
                set_cols.append(f"fwd_{n}d = c.fwd_{n}d")
        for n in _MFE_NS:
            if f"mfe_{n}d" in available:
                set_cols.append(f"mfe_{n}d = c.mfe_{n}d")
        for n in _MAE_NS:
            if f"mae_{n}d" in available:
                set_cols.append(f"mae_{n}d = c.mae_{n}d")

        sql = f"""
        UPDATE bars AS b
        SET {', '.join(set_cols)}
        FROM (
            SELECT {_select_exprs()}
            FROM bars
            WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ) AS c
        WHERE b.id = c.id
          AND b.fwd_5d IS NULL
          AND b.date >= (SELECT MAX(date) FROM bars) - {int(lookback_days)}
        """
        conn.execute(sql)

        # derive hit_*/drop_* flags for the rows we just filled
        flag_updates = [
            ("hit_5pct_5d",   "mfe_5d  >= 5"),
            ("hit_10pct_5d",  "mfe_5d  >= 10"),
            ("hit_20pct_5d",  "mfe_5d  >= 20"),
            ("hit_30pct_10d", "mfe_10d >= 30"),
            ("hit_50pct_20d", "mfe_20d >= 50"),
            ("hit_2x_60d",    "mfe_60d >= 100"),
            ("drop_10pct_5d", "mae_5d  <= -10"),
            ("drop_20pct_10d","mae_10d <= -20"),
            ("drop_30pct_20d","mae_20d <= -30"),
        ]
        for col, cond in flag_updates:
            if col in available:
                src = cond.split()[0]
                conn.execute(
                    f"UPDATE bars SET {col} = ({cond}) "
                    f"WHERE {src} IS NOT NULL AND {col} IS NULL "
                    f"AND date >= (SELECT MAX(date) FROM bars) - {int(lookback_days)}"
                )

        # report how many got labelled
        remaining = conn.execute(
            "SELECT COUNT(*) FROM bars "
            "WHERE fwd_5d IS NULL AND date >= (SELECT MAX(date) FROM bars) - ?",
            [lookback_days],
        ).fetchone()[0]
        conn.commit()
        updated = candidates - remaining
        log.info("backfill_forward_returns: candidates=%d updated=%d", candidates, updated)
        return {"candidates": candidates, "updated": updated, "max_date": str(max_date)}
    finally:
        conn.close()
