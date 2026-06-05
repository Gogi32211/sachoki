"""
pnl_metric.py — replace Tier-1's HH-based edge with a REALIZED P&L edge.

Combo Lab proved (n=20k, Bonferroni p≈0) that HH-edge does NOT convert into
profit through tradeable exits. So Tier-1 / V3 / journal-rails are pointed at
the wrong target. Here we re-grade every predicate by a SIMPLE, FIXED, FAIR
trade simulation done in pure SQL — no per-bar loop — so it can run over the
whole DB quickly:

  entry = close of signal bar (proxy; we accept a small look-ahead leak vs the
          journal's "next-open" rule, but trade ranks remain comparable since
          the leak hits every predicate equally)
  exit  = first-touch of {stop = entry*(1-S), target = entry*(1+T), time = N days}
          where S/T are fixed % (default 2% / 5%), N=10 days. Approximated by
          fwd_5d/fwd_10d distribution percentiles — see _SQL.

For the cleanest, fairest comparison we just measure the REALIZED return given
a fixed asymmetric exit rule that mimics swing trading: capped on both sides.
Simulation per row, vectorised:

  ret_raw   = fwd_5d
  ret_clip  = max(-S, min(+T, fwd_5d))             # apply stop/target caps
  win       = ret_clip > 0
  big_win   = ret_clip >= T*0.8                     # near-target win
  loss      = ret_clip <= -S*0.8                    # near-stop loss

Aggregating these by predicate gives win_rate / avg_clipped / lift vs baseline.
We then sort by lift, store the top in signal_outcomes_pnl table for the UI/
journal to consume.
"""
from __future__ import annotations

import time
import logging

from ai_journal.db import get_analytics_conn, get_journal_conn, ensure_schema
from ai_journal.bootstrap import PREDICATES, _POP

log = logging.getLogger(__name__)

STOP_PCT   = 2.0     # %
TARGET_PCT = 5.0     # %


_SCHEMA_PNL = """
CREATE TABLE IF NOT EXISTS signal_outcomes_pnl (
    predicate     VARCHAR,
    category      VARCHAR,
    as_of_date    DATE,
    stop_pct      DOUBLE,
    target_pct    DOUBLE,
    n             BIGINT,
    avg_raw_fwd5  DOUBLE,          -- median of un-clipped fwd_5d
    avg_clip      DOUBLE,          -- mean of clipped (asymmetric exit) returns
    win_rate      DOUBLE,          -- P(ret_clip > 0)
    big_rate      DOUBLE,          -- P(ret_clip >= 0.8*target)
    loss_rate     DOUBLE,          -- P(ret_clip <= -0.8*stop)
    base_avg_clip DOUBLE,
    base_win_rate DOUBLE,
    edge_avg_clip DOUBLE,          -- avg_clip - base
    edge_win      DOUBLE,          -- (win_rate - base_win_rate)*100, pp
    updated_at    TIMESTAMP,
    PRIMARY KEY (predicate, as_of_date, stop_pct, target_pct)
);
"""


def _build_sql(stop: float, target: float) -> str:
    """One pass, conditional-aggregate over all PREDICATES."""
    s, t = stop, target
    clipped = f"greatest(-{s}, least({t}, fwd_5d))"
    big_thr = t * 0.8
    loss_thr = -s * 0.8
    sel = [
        "count(*) AS base_n",
        f"avg({clipped}) AS base_avg_clip",
        f"avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) AS base_win",
    ]
    for name, _cat, cond in PREDICATES:
        f = f"FILTER (WHERE {cond})"
        sel += [
            f"count(*) {f} AS {name}__n",
            f"median(fwd_5d) {f} AS {name}__raw5",
            f"avg({clipped}) {f} AS {name}__avgclip",
            f"avg(CASE WHEN {clipped} > 0 THEN 1.0 ELSE 0 END) {f} AS {name}__win",
            f"avg(CASE WHEN {clipped} >= {big_thr} THEN 1.0 ELSE 0 END) {f} AS {name}__big",
            f"avg(CASE WHEN {clipped} <= {loss_thr} THEN 1.0 ELSE 0 END) {f} AS {name}__loss",
        ]
    return f"SELECT {', '.join(sel)} FROM bars WHERE {_POP}"


def reseed_pnl_metric(stop_pct: float = STOP_PCT, target_pct: float = TARGET_PCT) -> dict:
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
        row = a.execute(_build_sql(stop_pct, target_pct)).fetchdf().iloc[0]
    finally:
        a.close()

    base_n = int(row["base_n"])
    base_avg = float(row["base_avg_clip"])
    base_win = float(row["base_win"])

    out = []
    for name, cat, _cond in PREDICATES:
        n = int(row[f"{name}__n"] or 0)
        if n == 0:
            continue
        avgclip = float(row[f"{name}__avgclip"] or 0)
        winr = float(row[f"{name}__win"] or 0)
        bigr = float(row[f"{name}__big"] or 0)
        lossr = float(row[f"{name}__loss"] or 0)
        out.append((name, cat, as_of, stop_pct, target_pct, n,
                    float(row[f"{name}__raw5"] or 0), avgclip, winr, bigr, lossr,
                    base_avg, base_win,
                    avgclip - base_avg, (winr - base_win) * 100.0))

    j = get_journal_conn()
    try:
        j.execute("DELETE FROM signal_outcomes_pnl WHERE as_of_date = ? AND stop_pct = ? AND target_pct = ?",
                  [as_of, stop_pct, target_pct])
        j.executemany("""INSERT INTO signal_outcomes_pnl
            (predicate, category, as_of_date, stop_pct, target_pct, n,
             avg_raw_fwd5, avg_clip, win_rate, big_rate, loss_rate,
             base_avg_clip, base_win_rate, edge_avg_clip, edge_win, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp)""", out)
        j.commit()
    finally:
        j.close()

    log.info("signal_outcomes_pnl seeded: %d preds, base n=%d avg_clip=%.2f win=%.1f%%, in %.1fs",
             len(out), base_n, base_avg, base_win * 100, time.time() - t0)
    return {"predicates": len(out), "base_n": base_n,
            "base_avg_clip": round(base_avg, 3), "base_win_rate": round(base_win * 100, 1),
            "stop_pct": stop_pct, "target_pct": target_pct,
            "as_of": str(as_of), "duration_sec": round(time.time() - t0, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    res = reseed_pnl_metric()
    print(res)
    # Show the truth — ranked by P&L edge
    j = get_journal_conn()
    try:
        df = j.execute("""
            SELECT predicate, category, n,
                   round(avg_raw_fwd5,2) raw5,
                   round(avg_clip,3) avg_clip,
                   round(edge_avg_clip,3) edge_avg,
                   round(win_rate*100,1) win,
                   round(edge_win,1) edge_win_pp,
                   round(big_rate*100,1) big,
                   round(loss_rate*100,1) loss
            FROM signal_outcomes_pnl ORDER BY edge_avg_clip DESC
        """).fetchdf()
    finally:
        j.close()
    import pandas as pd
    pd.set_option("display.width", 200); pd.set_option("display.max_rows", 100)
    print("\n=== Tier-1 reranked by REALIZED P&L (asymmetric exit -2% / +5%) ===")
    print(df.to_string(index=False))
