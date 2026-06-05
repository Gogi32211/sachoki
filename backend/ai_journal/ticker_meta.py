"""
ai_journal/ticker_meta.py — one-time (refreshable) ticker metadata enrichment.

Fetches Massive reference (/v3/reference/tickers/{ticker}) for the whole universe
→ sector (via SIC code), industry (sic_description), market_cap, employees → stores
in journal.duckdb.ticker_meta. This is the data foundation for:
  - Industry Pulse (industry edges / movers / regime by sector),
  - the journal's sector cap (currently inert — no sector data),
  - market-cap buckets (avoid/segment penny-pump microcaps).

Reference is reference data → fetch once, cache, refresh occasionally. The Massive
reference endpoint is slow (~3-4s/call), so we parallelise with threads (I/O-bound).

Run:  python -m ai_journal.ticker_meta
"""
from __future__ import annotations

import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from data_massive import _sic_to_sector, _BASE       # noqa: E402
from .db import get_analytics_conn, get_journal_conn, ensure_schema  # noqa: E402

log = logging.getLogger(__name__)
_KEY = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY") or ""


def _mcap_bucket(mc: float | None) -> str:
    if not mc:
        return "unknown"
    b = mc / 1e9
    if b >= 200: return "mega"
    if b >= 10:  return "large"
    if b >= 2:   return "mid"
    if b >= 0.3: return "small"
    return "micro"


def _fetch_one(ticker: str) -> dict | None:
    try:
        r = requests.get(f"{_BASE}/v3/reference/tickers/{ticker}",
                         params={"apiKey": _KEY}, timeout=12)
        if r.status_code != 200:
            return None
        d = r.json().get("results") or {}
        sic = d.get("sic_code")
        try:
            sic = int(sic) if sic else None
        except (TypeError, ValueError):
            sic = None
        mc = d.get("market_cap")
        return {
            "ticker": ticker,
            "name": d.get("name") or ticker,
            "sector": _sic_to_sector(sic) if sic else "",
            "industry": (d.get("sic_description") or "").title(),
            "sic_code": sic,
            "market_cap": float(mc) if mc else None,
            "employees": d.get("total_employees"),
            "mcap_bucket": _mcap_bucket(float(mc) if mc else None),
        }
    except Exception:
        return None


def build_ticker_meta(universes: list[str] | None = None, workers: int = 12,
                      only_missing: bool = False) -> dict:
    ensure_schema()
    t0 = time.time()

    a = get_analytics_conn()
    try:
        if universes:
            ph = ",".join("?" * len(universes))
            tickers = [r[0] for r in a.execute(
                f"SELECT DISTINCT ticker FROM bars WHERE universe IN ({ph})", universes).fetchall()]
        else:
            tickers = [r[0] for r in a.execute("SELECT DISTINCT ticker FROM bars").fetchall()]
    finally:
        a.close()

    if only_missing:
        j = get_journal_conn(read_only=True)
        try:
            have = {r[0] for r in j.execute("SELECT ticker FROM ticker_meta").fetchall()}
        finally:
            j.close()
        tickers = [t for t in tickers if t not in have]

    log.info("ticker_meta: fetching %d tickers (%d workers)…", len(tickers), workers)
    rows, done, errors = [], 0, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_fetch_one, t) for t in tickers]
        for fut in as_completed(futs):
            r = fut.result()
            done += 1
            if r:
                rows.append(r)
            else:
                errors += 1
            if done % 250 == 0:
                log.info("  %d/%d fetched (%d ok, %d err) %.0fs",
                         done, len(tickers), len(rows), errors, time.time() - t0)

    j = get_journal_conn(read_only=False)
    try:
        for r in rows:
            j.execute("""INSERT OR REPLACE INTO ticker_meta
                (ticker, name, sector, industry, sic_code, market_cap, employees,
                 mcap_bucket, updated_at)
                VALUES (?,?,?,?,?,?,?,?, current_timestamp)""",
                [r["ticker"], r["name"], r["sector"], r["industry"], r["sic_code"],
                 r["market_cap"], r["employees"], r["mcap_bucket"]])
        j.commit()
        total = j.execute("SELECT count(*) FROM ticker_meta").fetchone()[0]
    finally:
        j.close()

    dur = time.time() - t0
    log.info("ticker_meta done: %d stored (%d err) in %.0fs; table total=%d",
             len(rows), errors, dur, total)
    return {"fetched": len(tickers), "stored": len(rows), "errors": errors,
            "table_total": total, "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import sys
    unis = sys.argv[1:] or None
    print(build_ticker_meta(universes=unis))
