"""
pnl_metric.py v2 — multi-horizon realized P&L edge per predicate.

Why multi-horizon: different signals have edge on different timescales — a volume
spike may have a 1d gap-edge but no 10d edge; a Wyckoff setup may take 10-20d to
play out. Measuring only one horizon hides this and can reject a signal that
works on a different timescale.

For each horizon H ∈ {1, 3, 5, 10} bars we compute the same asymmetric exit
metric, with stop/target SCALED by √(H/5) so that the % cap is proportional to
the expected per-period move (Brownian-motion scaling). At H=5 the caps are the
canonical -2% / +5%; at H=1 they are about -0.9% / +2.2%, at H=10 about -2.8% /
+7.1%. Baseline is recomputed per horizon (it differs across H).

One DuckDB pass per horizon, conditional-aggregates over all PREDICATES.
"""
from __future__ import annotations

import math
import time
import logging

from ai_journal.db import get_analytics_conn, get_journal_conn, ensure_schema
from ai_journal.bootstrap import PREDICATES, _POP

log = logging.getLogger(__name__)

# Canonical exit caps at H=5; scaled by √(H/5) for other horizons.
STOP_BASE   = 2.0     # % at H=5
TARGET_BASE = 5.0
DEFAULT_HORIZONS = (1, 3, 5, 10)


def _caps(h: int) -> tuple[float, float]:
    """√(h/5) Brownian scaling. → (-2%/+5%) at H=5, smaller/wider on shorter/longer."""
    f = math.sqrt(h / 5.0)
    return round(STOP_BASE * f, 3), round(TARGET_BASE * f, 3)


_SCHEMA_PNL = """
CREATE TABLE IF NOT EXISTS signal_outcomes_pnl (
    predicate     VARCHAR,
    category      VARCHAR,
    as_of_date    DATE,
    horizon       INTEGER,        -- 1, 3, 5, 10
    stop_pct      DOUBLE,         -- the cap actually applied at this horizon
    target_pct    DOUBLE,
    n             BIGINT,
    avg_raw_fwd   DOUBLE,         -- median of un-clipped fwd_H
    avg_clip      DOUBLE,         -- mean of clipped (asymmetric exit) returns
    win_rate      DOUBLE,
    big_rate      DOUBLE,         -- P(clipped >= 0.8*target)
    loss_rate     DOUBLE,         -- P(clipped <= -0.8*stop)
    base_avg_clip DOUBLE,
    base_win_rate DOUBLE,
    edge_avg_clip DOUBLE,
    edge_win      DOUBLE,         -- pp
    updated_at    TIMESTAMP,
    PRIMARY KEY (predicate, as_of_date, horizon)
);
"""


def _pop_for(h: int) -> str:
    """Population for this horizon — only rows that HAVE fwd_H (clip outliers)."""
    return f"fwd_{h}d IS NOT NULL AND fwd_{h}d BETWEEN -90 AND 500"


def _build_sql(h: int, stop: float, target: float) -> str:
    fwd = f"fwd_{h}d"
    clipped = f"greatest(-{stop}, least({target}, {fwd}))"
    big_thr = target * 0.8
    loss_thr = -stop * 0.8
    sel = [
        "count(*) AS base_n",
        f"avg({clipped}) AS base_avg_clip",
        f"avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) AS base_win",
    ]
    for name, _cat, cond in PREDICATES:
        f = f"FILTER (WHERE {cond})"
        sel += [
            f"count(*) {f} AS {name}__n",
            f"median({fwd}) {f} AS {name}__raw",
            f"avg({clipped}) {f} AS {name}__avgclip",
            f"avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) {f} AS {name}__win",
            f"avg(CASE WHEN {clipped} >= {big_thr} THEN 1.0 ELSE 0 END) {f} AS {name}__big",
            f"avg(CASE WHEN {clipped} <= {loss_thr} THEN 1.0 ELSE 0 END) {f} AS {name}__loss",
        ]
    return f"SELECT {', '.join(sel)} FROM bars WHERE {_pop_for(h)}"


def reseed_pnl_metric(horizons: tuple[int, ...] = DEFAULT_HORIZONS) -> dict:
    ensure_schema()
    t0 = time.time()
    j = get_journal_conn()
    try:
        j.execute(_SCHEMA_PNL); j.commit()
    finally:
        j.close()

    a = get_analytics_conn()
    try:
        as_of = a.execute("SELECT max(date) FROM bars").fetchone()[0]
        per_h = {}
        for h in horizons:
            stop, target = _caps(h)
            t1 = time.time()
            row = a.execute(_build_sql(h, stop, target)).fetchdf().iloc[0]
            dur = time.time() - t1
            base_avg = float(row["base_avg_clip"])
            base_win = float(row["base_win"])
            out_rows = []
            for name, cat, _cond in PREDICATES:
                n = int(row[f"{name}__n"] or 0)
                if n == 0:
                    continue
                avgclip = float(row[f"{name}__avgclip"] or 0)
                winr = float(row[f"{name}__win"] or 0)
                bigr = float(row[f"{name}__big"] or 0)
                lossr = float(row[f"{name}__loss"] or 0)
                rawmd = float(row[f"{name}__raw"] or 0)
                out_rows.append((name, cat, as_of, h, stop, target, n, rawmd,
                                 avgclip, winr, bigr, lossr,
                                 base_avg, base_win,
                                 avgclip - base_avg, (winr - base_win) * 100.0))
            per_h[h] = {"base_avg": base_avg, "base_win": base_win,
                        "base_n": int(row["base_n"]), "rows": out_rows,
                        "stop": stop, "target": target, "duration": dur}
            log.info("  H=%d stop=%.2f target=%.2f base_n=%d avg_clip=%.3f win=%.1f%% (%.1fs)",
                     h, stop, target, per_h[h]["base_n"], base_avg, base_win * 100, dur)
    finally:
        a.close()

    j = get_journal_conn()
    try:
        j.execute("DELETE FROM signal_outcomes_pnl WHERE as_of_date = ? AND horizon IN ({})".format(
            ",".join(str(h) for h in horizons)), [as_of])
        for h in horizons:
            j.executemany("""INSERT INTO signal_outcomes_pnl
                (predicate, category, as_of_date, horizon, stop_pct, target_pct, n,
                 avg_raw_fwd, avg_clip, win_rate, big_rate, loss_rate,
                 base_avg_clip, base_win_rate, edge_avg_clip, edge_win, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp)""",
                per_h[h]["rows"])
        j.commit()
    finally:
        j.close()

    log.info("multi-horizon P&L reseed done in %.1fs", time.time() - t0)
    return {"horizons": list(horizons),
            "summary": {h: {"base_n": per_h[h]["base_n"],
                            "base_avg_clip": round(per_h[h]["base_avg"], 3),
                            "base_win": round(per_h[h]["base_win"] * 100, 1),
                            "stop": per_h[h]["stop"], "target": per_h[h]["target"]}
                        for h in horizons},
            "as_of": str(as_of), "duration_sec": round(time.time() - t0, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    res = reseed_pnl_metric()
    print(res)
    j = get_journal_conn()
    try:
        df = j.execute("""
            SELECT predicate, horizon, n,
                   round(stop_pct,2) stop, round(target_pct,2) tgt,
                   round(avg_raw_fwd,2) raw,
                   round(avg_clip,3) avg_clip,
                   round(edge_avg_clip,3) e_avg,
                   round(win_rate*100,1) win,
                   round(edge_win,1) e_win
            FROM signal_outcomes_pnl
            WHERE as_of_date = (SELECT max(as_of_date) FROM signal_outcomes_pnl)
            ORDER BY edge_avg_clip DESC LIMIT 30
        """).fetchdf()
    finally:
        j.close()
    import pandas as pd
    pd.set_option("display.width", 200); pd.set_option("display.max_rows", 200)
    print("\n=== Top-30 (predicate, horizon) by realized P&L edge — across all H ===")
    print(df.to_string(index=False))
