"""Canonical FULL-WINDOW REPLACEMENT for the repaired interval.

Why this and not the MERGE that came before it: the merge inserted the missing bars but never
touched the rows that survived the outage, and those rows carry ENRICHED columns computed on the
holed series — 41.2 % of 4H bars and 43.5 % of 1H bars in the window hold an rsi_14 that differs
from the correctly-enriched value, by up to 81 points. Repairing coverage without repairing derived
state leaves the live path (ultra_db_scan reads stored rsi_14) still broken.

A delete-and-replace is only safe because it was MEASURED safe: canonical bars in the window that
are absent from staging = 0 of 287,384 (4H) and 0 of 1,005,293 (1H). Staging is a strict superset,
so the delete gives back everything it removes, correctly enriched. That precondition is re-checked
here immediately before the write and aborts if it no longer holds.
"""
from __future__ import annotations
import os, sys
import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
WIN = ("2026-06-29", "2026-09-02")
L = lambda c="═", n=90: print(c * n)


def replace(tf: str) -> bool:
    con = duckdb.connect(f"{R}studio_{tf}.duckdb")
    con.execute(f"ATTACH '{R}staging_backfill/stage_{tf}.duckdb' AS s (READ_ONLY)")
    W = f"CAST(date AS DATE) >= DATE '{WIN[0]}' AND CAST(date AS DATE) < DATE '{WIN[1]}'"
    cc = [r[0] for r in con.execute("DESCRIBE bars").fetchall()]
    sc = set(r[0] for r in con.execute("DESCRIBE s.bars").fetchall())
    cols = [c for c in cc if c in sc and c != "id"]
    sel = ", ".join(f'"{c}"' for c in cols)

    print(f"\n── {tf.upper()} ──")
    # PRECONDITION, re-measured at the last possible moment
    only = con.execute(f"""SELECT COUNT(*) FROM (SELECT ticker, date FROM bars WHERE {W}
        EXCEPT SELECT ticker, date FROM s.bars)""").fetchone()[0]
    print(f"   precondition: canonical window bars absent from staging = {only:,}"
          f"   {'OK' if only == 0 else '⛔ ABORT'}")
    if only:
        con.close(); return False

    before_total = con.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
    before_out = con.execute(f"SELECT COUNT(*) FROM bars WHERE NOT ({W})").fetchone()[0]
    before_tk = con.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
    dmin, dmax = con.execute("SELECT MIN(date), MAX(date) FROM bars").fetchone()
    frozen = con.execute(f"SELECT COUNT(*) FROM s.bars WHERE {W}").fetchone()[0]
    print(f"   frozen staging window rows: {frozen:,}")

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM bars WHERE {W}")
        nid = con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]
        con.execute(f"""INSERT INTO bars ({sel}, "id")
            SELECT {sel}, {nid} + row_number() OVER () FROM s.bars WHERE {W}""")

        inw = con.execute(f"SELECT COUNT(*) FROM bars WHERE {W}").fetchone()[0]
        dup = con.execute("SELECT COUNT(*) FROM (SELECT ticker, date FROM bars GROUP BY 1,2 HAVING COUNT(*)>1)").fetchone()[0]
        keydiff = con.execute(f"""SELECT
            (SELECT COUNT(*) FROM (SELECT ticker, date FROM bars WHERE {W}
             EXCEPT SELECT ticker, date FROM s.bars WHERE {W}))
          + (SELECT COUNT(*) FROM (SELECT ticker, date FROM s.bars WHERE {W}
             EXCEPT SELECT ticker, date FROM bars WHERE {W}))""").fetchone()[0]
        ohlcv = con.execute(f"""SELECT ROUND(MAX(GREATEST(abs(a.open-b.open), abs(a.high-b.high),
              abs(a.low-b.low), abs(a.close-b.close), abs(COALESCE(a.volume,0)-COALESCE(b.volume,0)))),6)
            FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date WHERE {W.replace('date','a.date')}""").fetchone()[0]
        rsi = con.execute(f"""SELECT COUNT(*) FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
            WHERE {W.replace('date','a.date')} AND a.rsi_14 IS DISTINCT FROM b.rsi_14""").fetchone()[0]
        # ALL common columns, not a hand-picked few — guessing names is how the first attempt
        # aborted ("tz_sig" does not exist; the store has t_sig / z_sig / l_sig).
        cmp_cols = [c for c in cols if c not in ("ticker", "date", "universe")]
        pred = " OR ".join(f'a."{c}" IS DISTINCT FROM b."{c}"' for c in cmp_cols)
        der = con.execute(f"""SELECT COUNT(*) FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
            WHERE {W.replace('date','a.date')} AND ({pred})""").fetchone()[0]
        out = con.execute(f"SELECT COUNT(*) FROM bars WHERE NOT ({W})").fetchone()[0]
        tk = con.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
        nmin, nmax = con.execute("SELECT MIN(date), MAX(date) FROM bars").fetchone()

        checks = [
            (inw == frozen,            f"window rows {inw:,} == frozen staging {frozen:,}"),
            (dup == 0,                 f"duplicate (ticker, date) = {dup:,}"),
            (keydiff == 0,             f"row-key set identical to staging (symmetric diff {keydiff:,})"),
            (float(ohlcv or 0) == 0,   f"OHLCV exact parity vs staging (max |Δ| {ohlcv})"),
            (rsi == 0,                 f"rsi_14 parity 100 % ({rsi:,} rows differ)"),
            (der == 0,                 f"ALL {len(cmp_cols)} common columns identical to staging ({der:,} rows differ)"),
            (out == before_out,        f"healthy overlap untouched {before_out:,} -> {out:,}"),
            (tk >= before_tk,          f"tickers {before_tk:,} -> {tk:,}"),
            (str(nmin) == str(dmin) and str(nmax) == str(dmax), f"date range unchanged {str(nmin)[:10]} … {str(nmax)[:10]}"),
        ]
        for ok, msg in checks:
            print(f"   {'✓' if ok else '⛔'} {msg}")
        if all(ok for ok, _ in checks):
            con.execute("COMMIT")
            print(f"   COMMIT — window replaced, {before_total:,} -> {con.execute('SELECT COUNT(*) FROM bars').fetchone()[0]:,} rows")
            con.close(); return True
        con.execute("ROLLBACK"); print("   ROLLBACK — verification failed, canonical unchanged")
        con.close(); return False
    except Exception as e:
        con.execute("ROLLBACK"); print(f"   ROLLBACK on exception: {e}")
        con.close(); return False


if __name__ == "__main__":
    L(); print(f"CANONICAL FULL-WINDOW REPLACEMENT · {WIN[0]} … {WIN[1]}"); L()
    ok = all(replace(tf) for tf in ("1h", "4h"))
    L(); print("REPLACEMENT: " + ("COMPLETE" if ok else "ABORTED — canonical unchanged")); L()
    sys.exit(0 if ok else 1)
