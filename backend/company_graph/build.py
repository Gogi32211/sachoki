"""company_graph/build.py — harvest a ticker's ecosystem, in the background.

WHY THIS IS A JOB AND NOT A REQUEST HANDLER
    A full pass over NVDA is 37 candidate filers, each needing a filing fetched (0.8–1.8s)
    and a passage read by a model (~3s). Serially that is over two minutes. No page load
    waits for that, and no amount of tuning changes the shape: the work is proportional to
    how many companies mention the target.

    So `build_graph` runs detached and writes progress as it goes, and the page reads
    whatever is already stored. First view of a new ticker shows the profile and a
    progress line; the graph fills in. Second view is instant.

CONCURRENCY WITHOUT ABUSING SEC
    Fetches run in a small thread pool, but every request still passes through the shared
    rate limiter in harvest_sec — the pool raises utilisation, not request rate. SEC's
    full-text service is free and unauthenticated, and being cut off from it would take
    the whole page down, so the limiter is deliberately well under the published ceiling.

WHICH CANDIDATES GET PROCESSED FIRST
    All of them eventually, but the order matters when a run is interrupted or capped.
    Ranking is by industry proximity first: a filer in the target's own SIC code is far
    more likely to describe a real supply or competitive relationship than one three
    industries away. Recency breaks ties, and 10-K outranks 10-Q because the annual report
    is where dependencies are actually discussed.

    The ranking is a priority, not a filter. Everything in the candidate list is processed
    unless `max_candidates` says otherwise, and when it does, the number dropped is
    recorded rather than left to be inferred from a short list.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from company_graph import harvest_sec as H
from company_graph import store
from company_graph.extract import extract_edges, extract_self_edges

log = logging.getLogger(__name__)

MAX_WORKERS = 4
DEFAULT_FORMS = ("10-K", "10-Q")
DEFAULT_LOOKBACK_DAYS = 400
PASSAGE_WINDOW = 420
MAX_PASSAGES = 4

# in-process view of what a run is doing, so the API can answer "how far along" without
# hitting the DB on every poll
_progress: dict[str, dict] = {}
_progress_lock = threading.Lock()
_running: set[str] = set()


def progress(ticker: str) -> dict:
    with _progress_lock:
        return dict(_progress.get(ticker.upper(), {}))


def is_running(ticker: str) -> bool:
    with _progress_lock:
        return ticker.upper() in _running


def _set(ticker: str, **kw) -> None:
    with _progress_lock:
        _progress.setdefault(ticker.upper(), {}).update(kw)


def _rank(candidates: list[dict], target_sic: str) -> list[dict]:
    def key(c):
        same_sic = 0 if c.get("sic") and c["sic"] == target_sic else 1
        # SIC codes share a prefix within an industry group: 3674 and 3672 are neighbours
        near_sic = 0 if (c.get("sic") or "")[:2] == (target_sic or "")[:2] else 1
        form_rank = 0 if c.get("root_form") == "10-K" else 1
        return (same_sic, near_sic, form_rank, c.get("file_date", ""))
    return sorted(candidates, key=key, reverse=False)


def _one(target: dict, c: dict, needle: str) -> dict:
    """Fetch one filer's passages and read them. Never raises — a bad document is data."""
    try:
        ex = H.extract_passages(c["url"], needle, window=PASSAGE_WINDOW,
                                max_passages=MAX_PASSAGES)
        if not ex.get("ok") or not ex.get("passages"):
            return {"ticker": c.get("ticker"), "edges": [], "rejected": [],
                    "skipped": ex.get("error") or "no passages"}
        res = extract_edges(
            target, {"name": c["name"], "ticker": c.get("ticker", ""), "cik": c["cik"]},
            ex["passages"], form=c.get("form", ""), date=c.get("file_date", ""),
            url=c["url"])
        res["ticker"] = c.get("ticker")
        res["candidate"] = c
        return res
    except Exception as exc:                                          # noqa: BLE001
        log.warning("company_graph: candidate %s failed: %s", c.get("ticker"), exc)
        return {"ticker": c.get("ticker"), "edges": [], "rejected": [],
                "skipped": f"{type(exc).__name__}: {exc}"[:160]}


# Where a company describes its own world. "Competition" is a required part of Item 1, so
# every operating company has one; the others are common section headings rather than
# guaranteed ones.
SELF_NEEDLES = ["Competition", "we compete", "our suppliers", "principal suppliers",
                "our customers", "significant customers", "sole source", "single source"]


def harvest_self(prof: dict) -> dict:
    """Read the target's OWN latest annual report for the companies IT names.

    The co-mention search only ever finds companies that wrote this one's name down. That
    is the wrong half of the world for a small company: Unusual Machines' own 10-K names
    DJI, T-Motor, Orqa, ModalAI and ARK Electronics as its competitors, none of them files
    with the SEC, and the page therefore showed zero competitors for a company that had
    just listed five.

    The target's own filings were being actively discarded — 171 of them on UMAC, dropped
    as 'self' — while the section that answers "who do you compete with" sat inside them.
    """
    cik = prof.get("cik")
    if not cik:
        return {"edges": [], "rejected": [], "error": "no cik"}
    sub = H.submissions(cik)
    if not sub:
        return {"edges": [], "rejected": [], "error": "submissions unavailable"}

    rec = (sub.get("filings") or {}).get("recent") or {}
    forms_ = rec.get("form") or []
    # the annual report, because Item 1 Business is where Competition lives; a 10-Q has
    # no such section and searching one finds only XBRL noise
    idx = next((i for i, f in enumerate(forms_) if f in ("10-K", "20-F", "40-F")), None)
    if idx is None:
        return {"edges": [], "rejected": [], "error": "no annual report on record"}

    url = H.document_url(cik, rec["accessionNumber"][idx], rec["primaryDocument"][idx])
    date = rec["filingDate"][idx]
    form = forms_[idx]

    seen, passages = set(), []
    for needle in SELF_NEEDLES:
        ex = H.extract_passages(url, needle, window=760, max_passages=2)
        for ps in ex.get("passages", []):
            # windows around different needles overlap constantly in a 10-K
            bucket = ps["offset"] // 900
            if bucket not in seen:
                seen.add(bucket)
                passages.append(ps)
        if len(passages) >= 6:
            break

    if not passages:
        return {"edges": [], "rejected": [], "url": url, "form": form, "date": date,
                "error": "no matching sections found in the annual report"}

    res = extract_self_edges({"name": prof.get("name"), "ticker": prof.get("ticker")},
                             passages, form=form, date=date, url=url)
    res.update({"url": url, "form": form, "date": date, "n_passages": len(passages)})
    return res


def build_graph(ticker: str, forms: tuple = DEFAULT_FORMS, lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                max_candidates: int = 40, pages: int = 3) -> dict:
    """Harvest one ticker end to end. Blocking; call via `build_async` from a request."""
    from datetime import date, timedelta                              # noqa: PLC0415

    ticker = ticker.upper()
    store.init()
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=lookback_days)).isoformat()
    params = {"forms": list(forms), "start": start, "end": end,
              "max_candidates": max_candidates, "pages": pages}
    run_id = store.start_run(ticker, params)
    t0 = time.time()
    _set(ticker, phase="profile", done=0, total=0, edges=0, started=t0)

    try:
        prof = H.profile(ticker)
        if not prof:
            store.finish_run(run_id, status="FAILED",
                             error=f"{ticker} is not in SEC's ticker index")
            _set(ticker, phase="failed", error="not in SEC ticker index")
            return {"ok": False, "error": f"{ticker} not found in SEC ticker index"}
        store.upsert_entity(prof, is_target=True)

        _set(ticker, phase="own filing", profile=prof.get("name"))
        self_res = harvest_self(prof)
        self_edges = self_res.get("edges") or []
        if self_edges:
            store.save_edges(ticker, self_edges, tier=0)
        _set(ticker, edges=len(self_edges), self_edges=len(self_edges),
             unlisted=len(self_res.get("unlisted") or []))

        _set(ticker, phase="searching")
        found = H.candidate_filers(prof["search_names"], prof["cik"], forms=forms,
                                   start=start, end=end, pages=pages)
        if not found.get("ok"):
            store.finish_run(run_id, status="FAILED", error=str(found.get("error"))[:400])
            _set(ticker, phase="failed", error=str(found.get("error"))[:200])
            return {"ok": False, "error": found.get("error")}

        cands = _rank(found["candidates"], str(prof.get("sic", "")))
        n_found = len(cands)
        capped = max(0, n_found - max_candidates)
        cands = cands[:max_candidates]

        n_docs = sum(v.get("documents", 0) for v in found["per_variant"])
        _set(ticker, phase="reading", total=len(cands), done=0, capped=capped,
             candidates=n_found)

        target = {"name": prof["search_names"][0] if prof["search_names"] else prof["name"],
                  "ticker": ticker}
        # search the passages for the spelling most likely to appear in prose
        needle = prof["search_names"][-1] if prof["search_names"] else prof["name"]

        all_edges, all_rej = [], list(self_res.get("rejected") or [])
        skipped, done = [], 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futs = {pool.submit(_one, target, c, needle): c for c in cands}
            for fut in as_completed(futs):
                r = fut.result()
                done += 1
                if r.get("skipped"):
                    skipped.append({"ticker": r.get("ticker"), "why": r["skipped"]})
                all_edges.extend(r.get("edges") or [])
                all_rej.extend(r.get("rejected") or [])
                if r.get("candidate"):
                    store.upsert_entity({
                        "cik": r["candidate"]["cik"], "ticker": r["candidate"].get("ticker", ""),
                        "name": r["candidate"]["name"], "sic": r["candidate"].get("sic", ""),
                        "hq_state": r["candidate"].get("biz_state", ""),
                        "hq_city": r["candidate"].get("biz_location", ""),
                        "hq_country": H.resolve_country(r["candidate"].get("biz_state", ""), ""),
                    })
                _set(ticker, done=done, edges=len(all_edges), rejected=len(all_rej))

        store.save_edges(ticker, all_edges, tier=1)
        total_edges = len(all_edges) + len(self_edges)
        store.finish_run(run_id, status="OK", n_documents=n_docs, n_candidates=n_found,
                         n_processed=len(cands), n_edges=total_edges,
                         n_rejected=len(all_rej), rejections=all_rej[:50])
        _set(ticker, phase="done", done=done, edges=total_edges,
             seconds=round(time.time() - t0, 1))

        return {"ok": True, "ticker": ticker, "run_id": run_id,
                "documents": n_docs, "candidates": n_found, "processed": len(cands),
                "capped": capped, "edges": total_edges, "rejected": len(all_rej),
                "self": {k: self_res.get(k) for k in
                         ("form", "date", "url", "n_passages", "unlisted", "error")}
                       | {"edges": len(self_edges)},
                "skipped": skipped, "per_variant": found["per_variant"],
                "dropped": found["dropped"], "seconds": round(time.time() - t0, 1)}
    except Exception as exc:                                          # noqa: BLE001
        log.exception("company_graph build failed for %s", ticker)
        store.finish_run(run_id, status="FAILED", error=f"{type(exc).__name__}: {exc}"[:400])
        _set(ticker, phase="failed", error=f"{type(exc).__name__}: {exc}"[:200])
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        with _progress_lock:
            _running.discard(ticker)


def build_async(ticker: str, **kw) -> dict:
    """Start a harvest unless one is already in flight for this ticker.

    The guard is not politeness: two concurrent harvests of the same ticker would both
    append their edges, and the pair would show up twice with identical evidence — which
    reads as corroboration from two sources when it is one source counted twice.
    """
    ticker = ticker.upper()
    with _progress_lock:
        if ticker in _running:
            return {"ok": True, "already_running": True, "progress": dict(_progress.get(ticker, {}))}
        _running.add(ticker)
        _progress[ticker] = {"phase": "queued", "done": 0, "total": 0, "edges": 0}
    threading.Thread(target=build_graph, args=(ticker,), kwargs=kw,
                     name=f"cgraph-{ticker}", daemon=True).start()
    return {"ok": True, "started": True, "ticker": ticker}


__all__ = ["build_graph", "build_async", "progress", "is_running"]
