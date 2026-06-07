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
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree as ET

import requests

from .db import get_analytics_conn, get_journal_conn, ensure_schema

log = logging.getLogger(__name__)
_UA = os.environ.get("AIJ_SEC_UA", "Sachoki Screener research contact@example.com")
_HDR = {"User-Agent": _UA, "Accept-Encoding": "gzip, deflate"}

# GLOBAL rate limiter — caps the aggregate request rate across ALL threads so the
# parallel backfill stays under SEC's 10 req/s fair-access limit. Workers fetch
# concurrently (hiding network latency) but their request STARTS are spaced here.
import threading as _threading
_MIN_INTERVAL = float(os.environ.get("AIJ_SEC_MIN_INTERVAL", "0.11"))  # ~9 req/s
_WORKERS      = int(os.environ.get("AIJ_SEC_WORKERS", "5"))
_RATE_LOCK    = _threading.Lock()
_LAST_REQ     = [0.0]


def _rate_wait():
    with _RATE_LOCK:
        gap = _MIN_INTERVAL - (time.time() - _LAST_REQ[0])
        if gap > 0:
            time.sleep(gap)
        _LAST_REQ[0] = time.time()


def _get(url: str, retries: int = 2, **kw):
    """Resilient GET — never raises; returns response or None (one bad fetch
    must not kill a long ingest). Rate-limited globally (thread-safe)."""
    for attempt in range(retries + 1):
        _rate_wait()
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


_CIKMAP = {"map": None, "at": 0.0}


def _cikmap_cached(max_age_h: float = 24):
    """Universe ticker→CIK map, cached in-process (the SEC ticker file + DB query
    is ~1s — don't repeat it on every on-demand per-ticker load)."""
    if _CIKMAP["map"] is None or (time.time() - _CIKMAP["at"]) > max_age_h * 3600:
        _CIKMAP["map"] = build_cik_map(universe_only=True)
        _CIKMAP["at"] = time.time()
    return _CIKMAP["map"]


def _ensure_ticker_cache_table(j):
    j.execute("""CREATE TABLE IF NOT EXISTS insider_ticker_cache (
        ticker VARCHAR PRIMARY KEY, fetched_at TIMESTAMP,
        lookback_days INTEGER, n_filings INTEGER, n_tx INTEGER)""")


def fetch_ticker_form4(ticker: str, lookback_days: int = 365,
                       max_age_h: float = 24, force: bool = False) -> dict:
    """ON-DEMAND: pull ONE ticker's Form 4 history (last `lookback_days`) straight
    from the SEC per-company submissions API, store in insider_tx, and remember
    the fetch in insider_ticker_cache so repeat views are instant. Idempotent
    (INSERT OR REPLACE). This is the lazy alternative to the universe-wide scan —
    we only ever fetch tickers the user actually opens."""
    ensure_schema()
    tk = ticker.upper()

    j = get_journal_conn(read_only=False)
    try:
        _ensure_ticker_cache_table(j)
        if not force:
            fresh = j.execute("""SELECT 1 FROM insider_ticker_cache
                WHERE ticker=? AND lookback_days>=?
                  AND fetched_at > now() - INTERVAL (?) HOUR""",
                [tk, lookback_days, max_age_h]).fetchone()
            if fresh:
                return {"ticker": tk, "fetched": False, "cached": True}
    finally:
        j.close()

    cik = _cikmap_cached().get(tk)
    if not cik:
        return {"ticker": tk, "fetched": False, "error": "ticker not in universe / no CIK"}
    r = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    if not r or r.status_code != 200:
        return {"ticker": tk, "fetched": False, "error": "submissions fetch failed"}
    rec = (r.json().get("filings") or {}).get("recent") or {}
    forms = rec.get("form", []); dates = rec.get("filingDate", []); accs = rec.get("accessionNumber", [])
    cutoff = str(date.today() - timedelta(days=lookback_days))
    f4 = [accs[i] for i in range(len(forms)) if forms[i] == "4" and dates[i] >= cutoff]
    cik_int = str(int(cik))

    def _one(acc):
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc.replace('-', '')}/{acc}.txt"
        fr = _get(url)
        if not fr or fr.status_code != 200:
            return None
        doc = _parse_form4(fr.text)
        if not doc or not doc["ticker"]:
            return None
        out = []
        for tx in doc["txs"]:
            if not tx["tx_date"]:
                continue
            out.append((acc, doc["ticker"], cik, doc["insider"], doc["title"],
                        tx["tx_date"], tx["code"], tx["acq_disp"], tx["shares"],
                        tx["price"], tx["value"], str(date.today())))
        return out

    rows = []
    if f4:
        with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
            for res in ex.map(_one, f4):
                if res:
                    rows.extend(res)

    jw = get_journal_conn(read_only=False)
    try:
        for row in rows:
            try:
                jw.execute("""INSERT OR REPLACE INTO insider_tx
                    (accession,ticker,cik,insider,title,tx_date,code,acq_disp,shares,price,value,filed_date)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", list(row))
            except Exception:
                pass
        _ensure_ticker_cache_table(jw)
        jw.execute("""INSERT OR REPLACE INTO insider_ticker_cache
            (ticker, fetched_at, lookback_days, n_filings, n_tx)
            VALUES (?, now(), ?, ?, ?)""", [tk, lookback_days, len(f4), len(rows)])
        jw.commit()
    finally:
        jw.close()
    buys = sum(1 for x in rows if x[6] == "P")
    return {"ticker": tk, "fetched": True, "n_filings": len(f4),
            "n_tx": len(rows), "buys": buys}


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


# Resumable day-log + live backfill progress (for the long 1-year backfill that
# must survive server restarts and run as a background task).
import threading
_BF_LOCK = threading.Lock()
_BACKFILL = {"running": False, "total_days": 0, "done_days": 0, "current_day": None,
             "stored": 0, "buys": 0, "started_at": None, "finished_at": None, "error": None}


def _ensure_log_table(j):
    j.execute("""CREATE TABLE IF NOT EXISTS insider_ingest_log (
        day DATE PRIMARY KEY, scanned INTEGER, stored INTEGER, done_at TIMESTAMP)""")


def backfill_status() -> dict:
    with _BF_LOCK:
        return dict(_BACKFILL)


def run_backfill(days: int = 365) -> dict:
    """Thread entry-point for the long backfill. Tracks progress in _BACKFILL and
    skips days already recorded in insider_ingest_log (resumable across restarts)."""
    with _BF_LOCK:
        if _BACKFILL["running"]:
            return {"already_running": True, **_BACKFILL}
        _BACKFILL.update({"running": True, "total_days": days, "done_days": 0,
                          "current_day": None, "stored": 0, "buys": 0,
                          "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                          "finished_at": None, "error": None})
    try:
        return ingest_form4(days=days, skip_done=True, _track=True)
    except Exception as e:
        with _BF_LOCK:
            _BACKFILL["error"] = str(e)
        log.exception("insider backfill failed")
        return {"error": str(e)}
    finally:
        with _BF_LOCK:
            _BACKFILL["running"] = False
            _BACKFILL["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")


def ingest_form4(days: int = 10, skip_done: bool = False, _track: bool = False) -> dict:
    ensure_schema()
    t0 = time.time()
    cikmap = build_cik_map(universe_only=True)
    cik_set = set(cikmap.values())
    log.info("edgar: %d universe CIKs mapped", len(cik_set))

    # Resumable day-log: which ingest-days are already complete.
    j0 = get_journal_conn(read_only=False)
    try:
        _ensure_log_table(j0)
        done_set = {str(r[0])[:10] for r in j0.execute("SELECT day FROM insider_ingest_log").fetchall()} if skip_done else set()
    finally:
        j0.close()

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
        if skip_done and str(day) in done_set:
            if _track:
                with _BF_LOCK:
                    _BACKFILL["done_days"] += 1
            continue
        if _track:
            with _BF_LOCK:
                _BACKFILL["current_day"] = str(day)
        q = (day.month - 1) // 3 + 1
        idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{q}/form.{day:%Y%m%d}.idx"
        r = _get(idx_url)
        if not r or r.status_code != 200:
            continue
        day_scanned0 = scanned
        # Collect this day's universe Form-4 filings, then fetch the docs in
        # PARALLEL (the global rate limiter keeps the aggregate under SEC's cap).
        filings = []
        for line in r.text.splitlines():
            if not line.startswith("4 "):
                continue
            parts = re.split(r"\s{2,}", line.strip())
            if len(parts) < 5:
                continue
            cik, fname = parts[2].zfill(10), parts[-1]
            if cik not in cik_set:
                continue
            filings.append((cik, fname))
        scanned += len(filings)

        def _fetch_one(cf):
            cik, fname = cf
            fr = _get(f"https://www.sec.gov/Archives/{fname}")
            if not fr or fr.status_code != 200:
                return None
            doc = _parse_form4(fr.text)
            if not doc or not doc["ticker"]:
                return None
            acc = fname.rsplit("/", 1)[-1].replace(".txt", "")
            out = []
            for tx in doc["txs"]:
                if not tx["tx_date"]:
                    continue
                out.append((acc, doc["ticker"], cik, doc["insider"], doc["title"],
                            tx["tx_date"], tx["code"], tx["acq_disp"], tx["shares"],
                            tx["price"], tx["value"], str(day)))
            return out

        rows = []
        if filings:
            with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
                for res in ex.map(_fetch_one, filings):
                    if res is not None:
                        parsed += 1
                        rows.extend(res)
        _flush(rows)          # persist per-day so a later failure can't lose progress
        stored += len(rows)
        # Mark this day done so a restart resumes from here (idempotent).
        jl = get_journal_conn(read_only=False)
        try:
            _ensure_log_table(jl)
            jl.execute("""INSERT OR REPLACE INTO insider_ingest_log (day, scanned, stored, done_at)
                          VALUES (?,?,?, now())""", [day, scanned - day_scanned0, len(rows)])
            jl.commit()
        except Exception:
            pass
        finally:
            jl.close()
        if _track:
            with _BF_LOCK:
                _BACKFILL["done_days"] += 1
                _BACKFILL["stored"] = stored
        log.info("  %s: scanned=%d parsed=%d stored=%d", day, scanned, parsed, stored)

    j = get_journal_conn(read_only=True)
    try:
        total = j.execute("SELECT count(*) FROM insider_tx").fetchone()[0]
        buys = j.execute("SELECT count(*) FROM insider_tx WHERE code='P'").fetchone()[0]
    finally:
        j.close()
    if _track:
        with _BF_LOCK:
            _BACKFILL["buys"] = buys
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


def marks_for_ticker(ticker: str, from_date: str | None = None) -> dict:
    """Open-market insider BUYS (Form 4, code='P') for ONE ticker, grouped per
    transaction date — for the chart's ★ markers AND the fullscreen detail panel.
    Each mark carries a `txs` breakdown (who / title / shares / price / $value) so
    the side panel can show exactly who bought what. Optional from_date lower
    bound matches the chart's earliest visible bar."""
    ensure_schema()
    j = get_journal_conn(read_only=True)
    try:
        params: list = [ticker.upper()]
        where_date = ""
        if from_date:
            where_date = " AND tx_date >= ?"
            params.append(from_date)
        rows = j.execute(f"""
            SELECT tx_date, insider, title, shares, price, value
            FROM insider_tx
            WHERE ticker = ? AND code = 'P'{where_date}
            ORDER BY tx_date, value DESC NULLS LAST
        """, params).fetchall()
    finally:
        j.close()

    from collections import OrderedDict
    by_date: "OrderedDict[str, dict]" = OrderedDict()
    for tx_date, insider, title, shares, price, value in rows:
        d = str(tx_date)[:10]
        m = by_date.get(d)
        if m is None:
            m = {"date": d, "_insiders": set(), "n_tx": 0,
                 "total_shares": 0.0, "total_value": 0.0, "txs": []}
            by_date[d] = m
        m["_insiders"].add(insider)
        m["n_tx"] += 1
        m["total_shares"] += float(shares or 0)
        m["total_value"] += float(value or 0)
        m["txs"].append({"insider": insider or "", "title": title or "",
                         "shares": float(shares or 0), "price": float(price or 0),
                         "value": float(value or 0)})

    marks = []
    for d, m in by_date.items():
        marks.append({
            "date":         d,
            "n_insiders":   len(m["_insiders"]),
            "n_tx":         m["n_tx"],
            "total_shares": m["total_shares"],
            "total_value":  m["total_value"],
            "insiders":     ", ".join(sorted(m["_insiders"])),
            "txs":          m["txs"],
        })
    return {"ticker": ticker.upper(), "from_date": from_date,
            "count": len(marks), "marks": marks}


if __name__ == "__main__":
    import sys, json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(ingest_form4(days))
    print(json.dumps(recent_insider(), default=str)[:800])
