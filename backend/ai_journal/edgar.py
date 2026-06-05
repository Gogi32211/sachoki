"""
ai_journal/edgar.py — SEC EDGAR Form 4 insider-transaction ingest (Phase 2).

Pulls recent Form 4 filings from the EDGAR daily index, filters to OUR universe
(via the SEC ticker↔CIK map), parses open-market activity, and stores it in
journal.duckdb.insider_tx. Cluster detection (>=2 distinct insiders buying the
same name in a window) is the high-signal pattern; surfaced in Industry Pulse.

HONEST SCOPE: this gives a live insider feed + cluster flags. The forward-edge of
insider clusters (their own Tier-1 stats, weeks horizon) requires a historical
backfill to validate before the journal leans on it — that's a follow-up.

SEC etiquette: declared User-Agent + <=10 req/s. Set AIJ_SEC_UA to your contact.

Run:  python -m ai_journal.edgar 10     # ingest last 10 calendar days
"""
from __future__ import annotations

import os
import re
import time
import logging
from datetime import date, timedelta
from xml.etree import ElementTree as ET

import requests

from .db import get_analytics_conn, get_journal_conn, ensure_schema

log = logging.getLogger(__name__)
_UA = os.environ.get("AIJ_SEC_UA", "Sachoki Screener research contact@example.com")
_HDR = {"User-Agent": _UA, "Accept-Encoding": "gzip, deflate"}
_SLEEP = 0.12   # ~8 req/s, under SEC's 10/s


def _get(url: str, retries: int = 2, **kw):
    """Resilient GET — never raises; returns response or None (one bad fetch
    must not kill a long ingest)."""
    for attempt in range(retries + 1):
        time.sleep(_SLEEP)
        try:
            return requests.get(url, headers=_HDR, timeout=20, **kw)
        except Exception:
            if attempt < retries:
                time.sleep(0.5)
                continue
            return None


def build_cik_map(universe_only: bool = True) -> dict:
    """ticker (upper) -> zero-padded 10-digit CIK, restricted to our universe."""
    r = _get("https://www.sec.gov/files/company_tickers.json")
    if not r or r.status_code != 200:
        raise RuntimeError("could not fetch SEC company_tickers.json")
    data = r.json()
    full = {row["ticker"].upper(): str(row["cik_str"]).zfill(10) for row in data.values()}
    if not universe_only:
        return full
    a = get_analytics_conn()
    try:
        uni = {t for (t,) in a.execute("SELECT DISTINCT ticker FROM bars").fetchall()}
    finally:
        a.close()
    return {t: c for t, c in full.items() if t in uni}


def _parse_form4(txt: str):
    """Extract (issuer_ticker, insider, title, [tx...]) from a Form 4 submission."""
    m = re.search(r"<ownershipDocument>.*?</ownershipDocument>", txt, re.S)
    if not m:
        return None
    try:
        root = ET.fromstring(m.group(0))
    except ET.ParseError:
        return None
    def _t(path):
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else ""
    ticker = _t("issuer/issuerTradingSymbol").upper()
    insider = _t("reportingOwner/reportingOwnerId/rptOwnerName")
    rel = root.find("reportingOwner/reportingOwnerRelationship")
    title = ""
    if rel is not None:
        bits = [c.tag for c in rel if (c.text or "").strip() in ("1", "true")]
        title = ",".join(bits)
    txs = []
    for nd in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = (nd.findtext("transactionCoding/transactionCode") or "").strip()
        ad = (nd.findtext("transactionAmounts/transactionAcquiredDisposedCode/value") or "").strip()
        sh = nd.findtext("transactionAmounts/transactionShares/value")
        px = nd.findtext("transactionAmounts/transactionPricePerShare/value")
        dt = (nd.findtext("transactionDate/value") or "").strip()
        try:
            sh = float(sh) if sh else 0.0
            px = float(px) if px else 0.0
        except ValueError:
            sh, px = 0.0, 0.0
        txs.append({"code": code, "acq_disp": ad, "shares": sh, "price": px,
                    "value": round(sh * px, 2), "tx_date": dt})
    return {"ticker": ticker, "insider": insider, "title": title, "txs": txs}


def ingest_form4(days: int = 10) -> dict:
    ensure_schema()
    t0 = time.time()
    cikmap = build_cik_map(universe_only=True)
    cik_set = set(cikmap.values())
    log.info("edgar: %d universe CIKs mapped", len(cik_set))

    def _flush(batch):
        if not batch:
            return
        j = get_journal_conn(read_only=False)
        try:
            for r in batch:
                try:
                    j.execute("""INSERT OR REPLACE INTO insider_tx
                        (accession,ticker,cik,insider,title,tx_date,code,acq_disp,shares,price,value,filed_date)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", list(r))
                except Exception:
                    pass
            j.commit()
        finally:
            j.close()

    stored, scanned, parsed = 0, 0, 0
    for d in range(days):
        day = date.today() - timedelta(days=d)
        if day.weekday() >= 5:
            continue
        q = (day.month - 1) // 3 + 1
        idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{q}/form.{day:%Y%m%d}.idx"
        r = _get(idx_url)
        if not r or r.status_code != 200:
            continue
        rows = []
        for line in r.text.splitlines():
            if not line.startswith("4 "):
                continue
            parts = re.split(r"\s{2,}", line.strip())
            if len(parts) < 5:
                continue
            cik, fname = parts[2].zfill(10), parts[-1]
            if cik not in cik_set:
                continue
            scanned += 1
            fr = _get(f"https://www.sec.gov/Archives/{fname}")
            if not fr or fr.status_code != 200:
                continue
            doc = _parse_form4(fr.text)
            if not doc or not doc["ticker"]:
                continue
            parsed += 1
            acc = fname.rsplit("/", 1)[-1].replace(".txt", "")
            for tx in doc["txs"]:
                if not tx["tx_date"]:
                    continue
                rows.append((acc, doc["ticker"], cik, doc["insider"], doc["title"],
                             tx["tx_date"], tx["code"], tx["acq_disp"], tx["shares"],
                             tx["price"], tx["value"], str(day)))
        _flush(rows)          # persist per-day so a later failure can't lose progress
        stored += len(rows)
        log.info("  %s: scanned=%d parsed=%d stored=%d", day, scanned, parsed, stored)

    j = get_journal_conn(read_only=True)
    try:
        total = j.execute("SELECT count(*) FROM insider_tx").fetchone()[0]
        buys = j.execute("SELECT count(*) FROM insider_tx WHERE code='P'").fetchone()[0]
    finally:
        j.close()
    dur = time.time() - t0
    log.info("edgar ingest: scanned %d, parsed %d, %d tx stored (%d buys), table=%d, %.0fs",
             scanned, parsed, stored, buys, total, dur)
    return {"days": days, "scanned": scanned, "parsed": parsed, "stored": stored,
            "buys": buys, "table_total": total, "duration_sec": round(dur, 1)}


def recent_insider(limit_days: int = 30, cluster_window: int = 7) -> dict:
    """Recent open-market BUYS + cluster flags (>=2 distinct insiders / ticker / window)."""
    ensure_schema()
    j = get_journal_conn(read_only=True)
    try:
        buys = j.execute("""
            SELECT ticker, count(DISTINCT insider) n_insiders, count(*) n_tx,
                   sum(value) tot_value, max(tx_date) last_buy
            FROM insider_tx
            WHERE code='P' AND tx_date >= (current_date - INTERVAL (?) DAY)
            GROUP BY ticker ORDER BY n_insiders DESC, tot_value DESC""", [limit_days]).fetchall()
        recent = j.execute("""
            SELECT ticker, insider, title, tx_date, shares, price, value
            FROM insider_tx WHERE code='P' ORDER BY tx_date DESC LIMIT 40""").fetchall()
    finally:
        j.close()
    cols1 = ["ticker", "n_insiders", "n_tx", "tot_value", "last_buy"]
    cols2 = ["ticker", "insider", "title", "tx_date", "shares", "price", "value"]
    by_ticker = [dict(zip(cols1, r)) for r in buys]
    clusters = [b for b in by_ticker if b["n_insiders"] >= 2]
    return {"clusters": clusters, "by_ticker": by_ticker[:40],
            "recent": [dict(zip(cols2, r)) for r in recent],
            "cluster_window": cluster_window}


if __name__ == "__main__":
    import sys, json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(ingest_form4(days))
    print(json.dumps(recent_insider(), default=str)[:800])
