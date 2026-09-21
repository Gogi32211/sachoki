"""Canonical MERGE — insert only what is missing, inside the damaged window, transactionally.

MERGE KEY = (ticker, date). NOT (ticker, date, universe), even though that is the declared PRIMARY
KEY: across 25.1M (1H) and 7.2M (4H) rows, ZERO (ticker, date) pairs carry more than one universe,
so `universe` is an ATTRIBUTE and the bar's identity is (ticker, date). Merging on the declared key
would have inserted 546 (1H) + 114 (4H) duplicate bars for the three tickers whose index membership
changed — the first duplicates these stores have ever held, and the PK would not have stopped them.

Fail closed: every verification runs INSIDE the transaction, and any mismatch rolls back.
"""
from __future__ import annotations
import os, sys
import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
WIN = ("2026-06-29", "2026-09-02")            # [from, to) — the damaged window
FROZEN = {"1h": 436_297, "4h": 168_608}       # dry-run counts, frozen before the write
L = lambda c="═", n=88: print(c * n)


def merge(tf: str) -> bool:
    con = duckdb.connect(f"{R}studio_{tf}.duckdb")
    con.execute(f"ATTACH '{R}staging_backfill/stage_{tf}.duckdb' AS s (READ_ONLY)")
    cc = [r[0] for r in con.execute("DESCRIBE bars").fetchall()]
    sc = set(r[0] for r in con.execute("DESCRIBE s.bars").fetchall())
    cols = [c for c in cc if c in sc and c != "id"]
    before = con.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
    before_out = con.execute(f"""SELECT COUNT(*) FROM bars
        WHERE CAST(date AS DATE) < DATE '{WIN[0]}' OR CAST(date AS DATE) >= DATE '{WIN[1]}'""").fetchone()[0]
    before_tk = con.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
    dmin, dmax = con.execute("SELECT MIN(date), MAX(date) FROM bars").fetchone()

    sel = ", ".join(f'"{c}"' for c in cols)
    dry = con.execute(f"""SELECT COUNT(*) FROM s.bars s
        WHERE CAST(s.date AS DATE) >= DATE '{WIN[0]}' AND CAST(s.date AS DATE) < DATE '{WIN[1]}'
          AND NOT EXISTS (SELECT 1 FROM bars b WHERE b.ticker = s.ticker AND b.date = s.date)""").fetchone()[0]
    print(f"\n── {tf.upper()} ── rows {before:,} · tickers {before_tk:,} · {str(dmin)[:10]} … {str(dmax)[:10]}")
    print(f"   dry-run {dry:,} · frozen {FROZEN[tf]:,}   "
          f"{'MATCH' if dry == FROZEN[tf] else '⛔ DRIFT — aborting'}")
    if dry != FROZEN[tf]:
        con.close(); return False

    con.execute("BEGIN TRANSACTION")
    try:
        nid = con.execute("SELECT coalesce(max(id),0) FROM bars").fetchone()[0]
        con.execute(f"""INSERT INTO bars ({sel}, "id")
            SELECT {sel}, {nid} + row_number() OVER () FROM s.bars s
            WHERE CAST(s.date AS DATE) >= DATE '{WIN[0]}' AND CAST(s.date AS DATE) < DATE '{WIN[1]}'
              AND NOT EXISTS (SELECT 1 FROM bars b WHERE b.ticker = s.ticker AND b.date = s.date)""")
        after = con.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
        ins = after - before
        dup = con.execute("""SELECT COUNT(*) FROM (SELECT ticker, date FROM bars
                             GROUP BY 1,2 HAVING COUNT(*) > 1)""").fetchone()[0]
        out = con.execute(f"""SELECT COUNT(*) FROM bars
            WHERE CAST(date AS DATE) < DATE '{WIN[0]}' OR CAST(date AS DATE) >= DATE '{WIN[1]}'""").fetchone()[0]
        tk = con.execute("SELECT COUNT(DISTINCT ticker) FROM bars").fetchone()[0]
        nmin, nmax = con.execute("SELECT MIN(date), MAX(date) FROM bars").fetchone()
        ref = duckdb.connect(f"{R}studio_15m.duckdb", read_only=True)
        want = ref.execute(f"""SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) FROM bars
            WHERE CAST(date AS DATE) >= DATE '{WIN[0]}' AND CAST(date AS DATE) < DATE '{WIN[1]}')""").fetchone()[0]
        ref.close()
        have = con.execute(f"""SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) FROM bars
            WHERE CAST(date AS DATE) >= DATE '{WIN[0]}' AND CAST(date AS DATE) < DATE '{WIN[1]}')""").fetchone()[0]

        checks = [
            (ins == FROZEN[tf],        f"inserted {ins:,} == frozen {FROZEN[tf]:,}"),
            (dup == 0,                 f"duplicate (ticker, date) = {dup:,}"),
            (out == before_out,        f"healthy overlap untouched  {before_out:,} -> {out:,}"),
            (tk >= before_tk,          f"tickers {before_tk:,} -> {tk:,} (no ticker lost)"),
            (str(nmin) == str(dmin) and str(nmax) == str(dmax), f"date range unchanged {str(nmin)[:10]} … {str(nmax)[:10]}"),
            (want - have == 0,         f"damaged-window remaining missing = {want - have:,}"),
        ]
        for ok, msg in checks:
            print(f"   {'✓' if ok else '⛔'} {msg}")
        if all(ok for ok, _ in checks):
            con.execute("COMMIT")
            print(f"   COMMIT — {ins:,} rows merged")
            con.close(); return True
        con.execute("ROLLBACK")
        print("   ROLLBACK — a verification failed, canonical unchanged")
        con.close(); return False
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"   ROLLBACK on exception: {e}")
        con.close(); return False


if __name__ == "__main__":
    L(); print(f"CANONICAL MERGE · window {WIN[0]} … {WIN[1]} · key (ticker, date)"); L()
    ok = all(merge(tf) for tf in ("1h", "4h"))
    L(); print("MERGE: " + ("COMPLETE" if ok else "ABORTED — canonical unchanged")); L()
    sys.exit(0 if ok else 1)
