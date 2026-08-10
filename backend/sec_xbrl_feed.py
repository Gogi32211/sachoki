"""SEC XBRL facts — point-in-time fundamentals, free, from an endpoint we already call.

Every fundamental dataset we could buy has the same two ways of lying, and both are invisible
once the data is in a table:

  · a figure that was later RESTATED, stored as though it had always read that way
  · a figure keyed to the END OF ITS PERIOD rather than the day it was published — a quarter
    ends 30-45 days before anyone can read it

`data.sec.gov/api/xbrl/companyfacts/CIK##########.json` avoids both by construction. Every
fact carries its own `filed` date and its own accession number, and a restatement appears as
an ADDITIONAL entry rather than a correction of the old one. So the honest design is simply
to keep what the API already gives:

    one row per (concept, period, filing)   —   append-only, never UPDATE

A restatement is a new row with a later `filed`. `as_of(ticker, concept, date)` then returns
the latest value whose `filed <= date`, which is the number the market could actually have
read that morning. Point-in-time becomes a property of the schema instead of a discipline
somebody has to remember.

Concepts are filtered during parsing rather than after. companyfacts for a large filer runs
to tens of megabytes and carries hundreds of tags; we keep twelve, chosen for one question —
whether a balance sheet is fragile — because that is what the price buckets were a proxy for.
$8-21 carries a 1.50% chance of a >25% drawdown against 0.59% at $21-89, and price is not the
cause. Dilution, cash runway and burn are.

SINGLE WRITER, like `bars`. DuckDB takes an exclusive lock for the life of the connection,
so nothing can read fundamentals.duckdb while an ingest is running. The bulk load is a
one-off of a few hours; incremental refreshes are short. Do not run two ingests at once, and
do not query the file mid-load — the error is loud, but the rule is the same one that governs
the bars writer.

Usage:
    python sec_xbrl_feed.py            # resumable; safe to interrupt and re-run
    python sec_xbrl_feed.py --limit 25 # a taste
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import duckdb
import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "..", "data")
FDB = os.path.join(DATA, "fundamentals.duckdb")
BARS_DB = os.path.join(DATA, "studio_analytics.duckdb")

# SEC requires a descriptive User-Agent with contact details and throttles at 10 req/s.
UA = {"User-Agent": "sachoki-research demetrashviligoga@gmail.com",
      "Accept-Encoding": "gzip, deflate"}
RATE = 0.11

# Twelve tags, one question: is this balance sheet fragile?
CONCEPTS = {
    # share count → dilution, the thing that turns into a reverse split
    ("dei", "EntityCommonStockSharesOutstanding"): "shares_outstanding",
    ("us-gaap", "CommonStockSharesOutstanding"): "shares_outstanding_bs",
    ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"): "shares_wavg",
    # liquidity → how many quarters of runway
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"): "cash",
    ("us-gaap", "ShortTermInvestments"): "short_investments",
    ("us-gaap", "AssetsCurrent"): "assets_current",
    ("us-gaap", "LiabilitiesCurrent"): "liabilities_current",
    # leverage
    ("us-gaap", "Liabilities"): "liabilities",
    ("us-gaap", "LongTermDebtNoncurrent"): "debt_lt",
    ("us-gaap", "StockholdersEquity"): "equity",
    # burn
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"): "cfo",
    ("us-gaap", "OperatingIncomeLoss"): "operating_income",
}

DDL = """
CREATE TABLE IF NOT EXISTS facts (
    cik           INTEGER,
    ticker        VARCHAR,
    concept       VARCHAR,     -- our name, not the raw tag
    taxonomy      VARCHAR,
    tag           VARCHAR,
    unit          VARCHAR,
    period_start  DATE,
    period_end    DATE,
    fy            INTEGER,
    fp            VARCHAR,
    form          VARCHAR,
    val           DOUBLE,
    filed         DATE,        -- THE public key. Nothing may be read before this date.
    accn          VARCHAR
);
CREATE TABLE IF NOT EXISTS ingest_log (
    ticker VARCHAR, cik INTEGER, status VARCHAR, n_facts INTEGER, ts TIMESTAMP
);
"""


def _con():
    c = duckdb.connect(FDB)
    c.execute(DDL)
    return c


def universe(limit: int | None = None) -> list[str]:
    c = duckdb.connect(BARS_DB, read_only=True)
    tks = [r[0] for r in c.execute("""
        SELECT DISTINCT ticker FROM bars
        WHERE universe <> 'index' AND close >= 5
          AND date >= current_date - INTERVAL 400 DAY
        ORDER BY ticker""").fetchall()]
    c.close()
    return tks[:limit] if limit else tks


def cik_map(s: requests.Session) -> dict:
    r = s.get("https://www.sec.gov/files/company_tickers.json", timeout=30)
    r.raise_for_status()
    return {str(v["ticker"]).upper(): int(v["cik_str"]) for v in r.json().values()}


def parse(cik: int, ticker: str, payload: dict) -> list[tuple]:
    """Flatten companyfacts into rows, keeping only our concepts and only dated facts.

    A fact without `filed` is dropped rather than defaulted — a value with no publication
    date cannot be placed in time, and guessing one is exactly the fault this file exists to
    prevent. Same for `period_end > filed`, which would mean a period closing after it was
    reported.
    """
    out = []
    facts = payload.get("facts", {})
    for (tax, tag), name in CONCEPTS.items():
        node = facts.get(tax, {}).get(tag)
        if not node:
            continue
        for unit, entries in (node.get("units") or {}).items():
            for e in entries:
                filed, end = e.get("filed"), e.get("end")
                if not filed or not end or e.get("val") is None:
                    continue
                if end > filed:                      # impossible; drop rather than repair
                    continue
                out.append((cik, ticker, name, tax, tag, unit, e.get("start"), end,
                            e.get("fy"), e.get("fp"), e.get("form"), float(e["val"]),
                            filed, e.get("accn")))
    return out


def refresh(limit: int | None = None, verbose: bool = True) -> dict:
    con = _con()
    done = {r[0] for r in con.execute(
        "SELECT ticker FROM ingest_log WHERE status IN ('ok','no-cik','empty')").fetchall()}
    tks = [t for t in universe(limit) if t not in done]
    if verbose:
        print(f"  universe {len(tks) + len(done):,} · already ingested {len(done):,} · "
              f"to fetch {len(tks):,}", flush=True)
    if not tks:
        con.close()
        return {"fetched": 0}

    s = requests.Session()
    s.headers.update(UA)
    cmap = cik_map(s)
    n_ok = n_rows = n_nocik = n_err = 0
    for i, t in enumerate(tks, 1):
        cik = cmap.get(t)
        if cik is None:
            con.execute("INSERT INTO ingest_log VALUES (?,?,?,?,now())",
                        [t, None, "no-cik", 0])
            n_nocik += 1
            continue
        try:
            r = s.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json",
                      timeout=45)
            if r.status_code != 200:
                con.execute("INSERT INTO ingest_log VALUES (?,?,?,?,now())",
                            [t, cik, f"http-{r.status_code}", 0])
                n_err += 1
                time.sleep(RATE)
                continue
            rows = parse(cik, t, r.json())
            if rows:
                con.executemany(
                    "INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            con.execute("INSERT INTO ingest_log VALUES (?,?,?,?,now())",
                        [t, cik, "ok" if rows else "empty", len(rows)])
            n_ok += 1
            n_rows += len(rows)
        except Exception as e:
            con.execute("INSERT INTO ingest_log VALUES (?,?,?,?,now())",
                        [t, cik, f"err:{str(e)[:40]}", 0])
            n_err += 1
        time.sleep(RATE)
        if verbose and i % 100 == 0:
            print(f"  {i}/{len(tks)} · {n_rows:,} facts · {n_err} errors", flush=True)
    con.close()
    if verbose:
        print(f"\n  DONE  {n_ok:,} tickers · {n_rows:,} facts · no-CIK {n_nocik} · "
              f"errors {n_err}", flush=True)
    return dict(tickers=n_ok, facts=n_rows, no_cik=n_nocik, errors=n_err)


def summary():
    con = _con()
    r = con.execute("""SELECT count(*), count(DISTINCT ticker), count(DISTINCT concept),
                              min(filed), max(filed) FROM facts""").fetchone()
    print(f"\n  facts {r[0]:,} · tickers {r[1]:,} · concepts {r[2]} · "
          f"filed {str(r[3])[:10]} → {str(r[4])[:10]}", flush=True)
    if r[0]:
        print("\n  by concept:", flush=True)
        for c, n, tk in con.execute("""SELECT concept, count(*), count(DISTINCT ticker)
                                       FROM facts GROUP BY 1 ORDER BY 2 DESC""").fetchall():
            print(f"    {c:24s} {n:>9,} rows · {tk:>5,} tickers", flush=True)
        rest = con.execute("""SELECT count(*) FROM (
            SELECT ticker, concept, period_end, count(DISTINCT accn) k
            FROM facts GROUP BY 1,2,3 HAVING k > 1)""").fetchone()[0]
        print(f"\n  restated (same period, >1 filing): {rest:,} "
              f"← kept as separate rows, which is the point", flush=True)
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--summary", action="store_true")
    a = ap.parse_args()
    if a.summary:
        summary()
    else:
        refresh(a.limit)
        summary()
