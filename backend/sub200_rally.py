"""
sub200_rally.py — the "1D=B" universal suppressor flag (validated 2026-07-05).

State B = daily close BELOW EMA200 while the short stack is up (e9>e20>e50):
a bear-market rally. Entering longs here is chasing the bounce — B-fires are
worse on EVERY Edge setup tested (Δmean −1.0..−3.3, era-independent: Atomic
6/6yr, Parabola/P55 5/6yr; B-state absolute mean NEGATIVE for Parabola/P55/
Atomic). Core reversal setups (GEM1/Z11/L43) almost never fire in B (they buy
state D), so this flag mainly protects the high-n scanners.

Usage (scanner-side, candidates only — cheap):
    from sub200_rally import flags_for
    flags = flags_for([r["ticker"] for r in rows])
    for r in rows:
        if flags.get(r["ticker"]):
            r["sub200_rally"] = True
            r["atoms"].append("⛔sub200")
Badge-only by design — no score change (keeps validated scan semantics).
"""
from __future__ import annotations
import duckdb
from studio.paths import ANALYTICS_DB


def flags_for(tickers: "list[str]") -> "dict[str, bool]":
    """{ticker: True if its LATEST daily bar is state B (sub-200 rally)}."""
    tks = sorted({t.upper() for t in tickers if t})
    if not tks:
        return {}
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = con.execute(f"""
            WITH u AS (
              SELECT ticker, date, close,
                     row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
              FROM bars WHERE ticker IN ({','.join('?' * len(tks))})),
            b AS (SELECT ticker, date, close FROM u WHERE rn = 1),
            r AS (SELECT ticker, date, close,
                         row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rd
                  FROM b)
            SELECT ticker, date, close FROM r WHERE rd <= 320 ORDER BY ticker, date
        """, tks).fetchdf()
    finally:
        con.close()
    out: dict[str, bool] = {}
    for tk, g in df.groupby("ticker", sort=False):
        c = g["close"]
        if len(c) < 60:                      # not enough history to trust EMA200
            out[tk] = False
            continue
        e9 = c.ewm(span=9, adjust=False).mean().iloc[-1]
        e20 = c.ewm(span=20, adjust=False).mean().iloc[-1]
        e50 = c.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = c.ewm(span=200, adjust=False).mean().iloc[-1]
        out[tk] = bool(c.iloc[-1] <= e200 and e9 > e20 > e50)
    return out
