"""
build_m15_dayrsi.py — (re)build data/m15_dayrsi.duckdb, the per-(ticker, ET-day)
MIN 15m RSI cache used by the 🧗 High-Base 15m-Dip edge (edge_replay + scanner).

Source: the ENRICHED studio_15m.duckdb (rsi_14 already computed per 15m bar).
One GROUP BY collapses each ET-trading-day to its lowest 15m RSI. Idempotent —
rebuilds the single `day_rsi` table from scratch each run (~seconds).

Run standalone, or nightly from update_all.sh right after the 15m enriched top-up.
READ-ONLY on the source; writes only the small cache file.
"""
from __future__ import annotations
import os
import duckdb
from studio.paths import db_path

SRC = db_path("studio_15m.duckdb")
OUT = db_path("m15_dayrsi.duckdb")


def main() -> None:
    if not os.path.exists(SRC):
        print(f"no enriched 15m DB at {SRC}"); return
    # ET day = bar timestamp (UTC) minus 5h, cast to date. min(rsi_14) per day.
    con = duckdb.connect(OUT)
    con.execute(f"ATTACH '{SRC}' AS s (READ_ONLY)")
    con.execute("DROP TABLE IF EXISTS day_rsi")
    con.execute("""
        CREATE TABLE day_rsi AS
        SELECT ticker,
               CAST(CAST(date - INTERVAL 5 HOUR AS DATE) AS VARCHAR) AS d,
               min(rsi_14) AS rsi15
        FROM s.bars
        WHERE rsi_14 IS NOT NULL
        GROUP BY 1, 2
    """)
    n, tk, dmax = con.execute(
        "SELECT count(*), count(DISTINCT ticker), max(d) FROM day_rsi").fetchone()
    con.close()
    sz = os.path.getsize(OUT) / 1e6
    print(f"m15_dayrsi rebuilt: {n:,} rows · {tk} tickers · through {dmax} · {sz:.0f}MB")


if __name__ == "__main__":
    main()
