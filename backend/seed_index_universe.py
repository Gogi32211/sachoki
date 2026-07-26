"""
seed_index_universe.py — one-shot seeding of the 'index' universe into the studio DB.

SPY/QQQ/DIA/IWM + the 11 SPDR sector ETFs + SMH, ~6.5y of daily bars, fetched
through the SAME live engine path the nightly incremental uses (main.api_bar_signals
→ full signal computation), inserted with universe='index', then enriched with the
standard enricher. After this one-shot, the 17:00 ET nightly refresh keeps it
current (universe list includes 'index').

RUN WITH THE BACKEND STOPPED (DuckDB single-writer):
  launchctl bootout gui/$(id -u)/com.sachoki.backend
  .venv/bin/python seed_index_universe.py
  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.sachoki.backend.plist

Idempotent: (ticker,date,universe) rows are deleted before insert.
"""
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("seed_index")

INDEX_TICKERS = [
    "SPY", "QQQ", "DIA", "IWM",                       # broad indices (ES/NQ/YM/RTY futures proxies)
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP",         # SPDR sectors
    "XLI", "XLB", "XLU", "XLRE", "XLC",
    "SMH",                                            # semis (used by the RS gate)
    # ── futures-market ETF proxies (2026-07-24) — non-equity futures via liquid ETFs ──
    "VIXY", "VXX", "UVXY",                            # VIX futures (VX) — volatility
    "GLD", "SLV", "USO", "UNG", "DBC", "CPER",        # commodities: gold/silver/crude/natgas/broad/copper
]
BARS = 1650   # ~6.5 years — full signal warmup + 60-month replay horizon


def main() -> None:
    from main import api_bar_signals
    from studio.incremental import _bar_dict_to_db_row
    from studio.db import get_conn
    from studio.enricher import enrich_universe
    import pandas as pd

    rows = []
    for tk in INDEX_TICKERS:
        t0 = time.time()
        try:
            bars = api_bar_signals(tk, tf="1d", bars=BARS, universe="index")
        except Exception as e:
            log.error("%s fetch failed: %s", tk, e)
            continue
        rows.extend(_bar_dict_to_db_row(b, tk, "index") for b in bars)
        log.info("%s: %d bars (%.1fs)", tk, len(bars), time.time() - t0)

    if not rows:
        log.error("nothing fetched — aborting")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    log.info("inserting %d rows…", len(df))

    conn = get_conn(read_only=False)
    try:
        conn.register("seed_tmp", df)
        conn.execute("DELETE FROM bars WHERE universe = 'index'")
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM bars").fetchone()[0]
        cols_no_id = [c for c in df.columns if c != "id"]
        cols_str = ", ".join(["id"] + cols_no_id)
        src_str = ", ".join([f"ROW_NUMBER() OVER () + {max_id} AS id"] + cols_no_id)
        conn.execute(f"INSERT INTO bars ({cols_str}) SELECT {src_str} FROM seed_tmp")
        conn.unregister("seed_tmp")
        conn.commit()
        n = conn.execute("SELECT count(*) FROM bars WHERE universe='index'").fetchone()[0]
        log.info("inserted — index universe now holds %d rows", n)
    finally:
        conn.close()

    log.info("enriching 'index'…")
    summary = enrich_universe(universe="index", max_workers=1)
    log.info("enrich done: %s", summary)


if __name__ == "__main__":
    main()
