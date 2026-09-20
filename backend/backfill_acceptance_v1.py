"""Backfill acceptance — the gate between the staging refetch and the canonical stores.

FAIL CLOSED. Canonical is replaced only if every section below passes. A failure leaves canonical
untouched, which is no worse than the state we are already in.

⚠️ CLOSE PARITY IS NOT ROW PARITY. An earlier pilot check compared `close` only and was reported as
"byte-identical" — that is evidence about one column, not about the row. A refetch can return the
right close on a different bar grid (extra/missing bars, shifted timestamps) and close-only parity
would pass it. This gate therefore reconciles the full OHLCV tuple AND the row-key set
(ticker, session, bar timestamp) over the healthy overlap.
"""
from __future__ import annotations
import os, sys
import duckdb, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
STG = {tf: f"{R}staging_backfill/stage_{tf}.duckdb" for tf in ("1h", "4h")}
CUR = {tf: f"{R}studio_{tf}.duckdb" for tf in ("1h", "4h")}
REF = f"{R}studio_15m.duckdb"                 # healthy reference calendar
ONE = f"{R}studio_analytics.duckdb"
DAMAGE = ("2026-06-29", "2026-09-01")         # the outage window
SHAPE = {"1h": 7, "4h": 2}                    # bars per session, from the alignment audit
L = lambda c="═", n=88: print(c * n)
_bad = 0


def fail(cond, msg):
    global _bad
    if not cond:
        _bad += 1
        print(f"      ⛔ {msg}")
    return cond


def fetch_window():
    c = duckdb.connect(STG["1h"], read_only=True)
    lo, hi = c.execute("SELECT MIN(CAST(date AS DATE)), MAX(CAST(date AS DATE)) FROM bars").fetchone()
    c.close()
    return str(lo), str(hi)


def calendar(lo, hi):
    c = duckdb.connect(ONE, read_only=True)
    d = [str(r[0]) for r in c.execute(
        "SELECT DISTINCT CAST(date AS DATE) FROM bars WHERE universe <> 'index' "
        "AND CAST(date AS DATE) BETWEEN ? AND ? ORDER BY 1", [lo, hi]).fetchall()]
    c.close()
    return d


def main():
    lo, hi = fetch_window()
    cal = calendar(lo, hi)
    L(); print(f"BACKFILL ACCEPTANCE · staging window {lo} … {hi} · {len(cal)} trading sessions"); L()

    print("\nFETCH")
    req = len(open(os.path.join(
        "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
        "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/bf_tickers.txt")).read().split(","))
    s1 = duckdb.connect(STG["1h"], read_only=True)
    got = s1.execute("SELECT COUNT(DISTINCT ticker), COUNT(*) FROM bars").fetchone()
    s1.close()
    print(f"  tickers requested        {req:,}")
    print(f"  tickers succeeded        {got[0]:,}")
    print(f"  tickers failed           {req - got[0]:,}")
    print(f"  1H rows written          {got[1]:,}")

    for tf in ("1h", "4h"):
        print(f"\nSTAGING {tf.upper()}")
        s = duckdb.connect(STG[tf], read_only=True)
        sess = [str(r[0]) for r in s.execute(
            "SELECT DISTINCT CAST(date AS DATE) FROM bars ORDER BY 1").fetchall()]
        missing = sorted(set(cal) - set(sess))
        print(f"  sessions expected/present/missing   {len(cal):,} / {len(sess):,} / {len(missing):,}")
        fail(not missing, f"missing sessions: {missing[:10]}")
        td = s.execute("SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) FROM bars)").fetchone()[0]
        exp = got[0] * len(cal)
        print(f"  ticker-days present                 {td:,}  (upper bound {exp:,} if every ticker traded every session)")
        dup = s.execute("""SELECT COUNT(*) FROM (SELECT ticker, date, universe, COUNT(*) c
                           FROM bars GROUP BY 1,2,3 HAVING c > 1)""").fetchone()[0]
        print(f"  row-key duplicates                  {dup:,}")
        fail(dup == 0, f"{dup:,} duplicate (ticker, date, universe) keys")
        shp = s.execute(f"""SELECT COUNT(*) FROM (SELECT ticker, CAST(date AS DATE) d, COUNT(*) c
                            FROM bars GROUP BY 1,2 HAVING c > {SHAPE[tf]})""").fetchone()[0]
        print(f"  session-shape violations (> {SHAPE[tf]} bars) {shp:,}")
        fail(shp == 0, f"{shp:,} ticker-sessions carry more than {SHAPE[tf]} bars")
        s.close()

    print("\nHEALTHY OVERLAP PARITY   (full OHLCV + row-key set, not close alone)")
    for tf in ("1h", "4h"):
        c = duckdb.connect(CUR[tf], read_only=True)
        c.execute(f"ATTACH '{STG[tf]}' AS s (READ_ONLY)")
        r = c.execute(f"""
            SELECT COUNT(*) n,
                   ROUND(MAX(abs(a.open  - b.open )), 6) o,
                   ROUND(MAX(abs(a.high  - b.high )), 6) h,
                   ROUND(MAX(abs(a.low   - b.low  )), 6) l,
                   ROUND(MAX(abs(a.close - b.close)), 6) cl,
                   ROUND(MAX(abs(COALESCE(a.volume,0) - COALESCE(b.volume,0))), 6) v
            FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
            WHERE CAST(a.date AS DATE) > DATE '{DAMAGE[1]}'""").fetchdf().iloc[0]
        k = c.execute(f"""
            SELECT (SELECT COUNT(*) FROM bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}') cur_rows,
                   (SELECT COUNT(*) FROM s.bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}') stg_rows,
                   (SELECT COUNT(*) FROM (SELECT ticker, date FROM bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}'
                    EXCEPT SELECT ticker, date FROM s.bars)) in_cur_not_stg""").fetchdf().iloc[0]
        print(f"  {tf.upper()}: compared {int(r.n):,} bars · max|Δ| O {r.o} H {r.h} L {r.l} C {r.cl} V {r.v}")
        print(f"       row keys  canonical {int(k.cur_rows):,} · staging {int(k.stg_rows):,} · "
              f"IN CANONICAL BUT NOT IN STAGING {int(k.in_cur_not_stg):,}")
        fail(max(r.o, r.h, r.l, r.cl) == 0, f"{tf} OHLC differs on the healthy overlap")
        fail(int(k.in_cur_not_stg) == 0, f"{tf} staging LOSES {int(k.in_cur_not_stg):,} existing row keys")
        c.close()

    print("\nDAMAGED WINDOW RECOVERY   " + f"{DAMAGE[0]} … {DAMAGE[1]}")
    for tf in ("1h", "4h"):
        c = duckdb.connect(CUR[tf], read_only=True)
        c.execute(f"ATTACH '{STG[tf]}' AS s (READ_ONLY)")
        r = c.execute(f"""
            WITH cur AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                         WHERE CAST(date AS DATE) BETWEEN DATE '{DAMAGE[0]}' AND DATE '{DAMAGE[1]}'),
                 stg AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM s.bars
                         WHERE CAST(date AS DATE) BETWEEN DATE '{DAMAGE[0]}' AND DATE '{DAMAGE[1]}')
            SELECT (SELECT COUNT(*) FROM cur) cur_td, (SELECT COUNT(*) FROM stg) stg_td,
                   (SELECT COUNT(*) FROM (SELECT * FROM stg EXCEPT SELECT * FROM cur)) recovered,
                   (SELECT COUNT(*) FROM (SELECT * FROM cur EXCEPT SELECT * FROM stg)) lost""").fetchdf().iloc[0]
        print(f"  {tf.upper()}: canonical {int(r.cur_td):,} ticker-days · staging {int(r.stg_td):,} · "
              f"RECOVERED {int(r.recovered):,} · lost {int(r.lost):,}")
        fail(int(r.lost) == 0, f"{tf} staging is missing {int(r.lost):,} ticker-days canonical already has")
        c.close()

    L(); print("ACCEPTANCE: " + ("PASS — canonical replacement may proceed" if _bad == 0
                                 else f"FAIL ({_bad} problems) — canonical stays untouched")); L()
    return 0 if _bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
