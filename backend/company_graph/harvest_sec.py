"""company_graph/harvest_sec.py — EDGAR as the evidence spine. No AI in this file.

Everything here is deterministic retrieval. The rule that keeps the graph honest is that
this layer produces DOCUMENTS — url, date, filer, quote — and never a relationship. The
relationship is read out of the quote later, by something that can be wrong and is marked
as such.

WHY EDGAR IS THE RIGHT SPINE, AND NOT WEB SEARCH

    Resolving "some memory maker in Korea" to a ticker is the step where a supply-chain
    graph normally dies. EDGAR hands it over for free: every full-text hit carries

        display_names: ['KLA CORP  (KLAC)  (CIK 0000319201)']

    The ticker comes from the filing index, not from a model's recollection. That single
    field is why the whole page can be built on documents rather than on vibes.

WHAT THE SEARCH ACTUALLY MEANS, WHICH IS LESS THAN IT LOOKS

    A full-text hit means one company's filing contains another company's name. That is
    CO-MENTION, not dependency. Measured on NVIDIA's own 2026 hits: 47 of 133 documents
    were SIC 6189 — asset-backed securities trusts, naming NVIDIA as a building tenant.
    Nothing about a supply chain.

    So co-mention is a CANDIDATE GENERATOR. `NOISE_SICS` throws out the financial vehicles
    before anything expensive happens, and the surviving passages go to an extractor that
    is allowed to answer "no relationship here" — and mostly should.

HOW A COMPANY'S NAME IS WRITTEN DECIDES WHAT YOU FIND, AND EVERY OBVIOUS CHOICE IS WRONG

    Measured on real filings, 10-K + 10-Q, one year:

        "NVDA"                 16     the ticker — barely used in prose
        "NVIDIA CORP"           4     the legal name from SEC's own records
        "NVIDIA Corporation"   88
        "NVIDIA"              294

        "Target Corporation"  130
        "Target"                0     ← a common word, suppressed by the index

    Both defensible choices fail: the ticker loses 95% of the evidence, and the legal name
    SEC itself returns loses 98%. Neither fails loudly. `candidate_filers` searching
    "NVIDIA CORP" returned four documents, all NVIDIA's own, and reported success.

    Worse is "Target" → 0. A zero is indistinguishable from "this company has no
    relationships", which is a conclusion, not an error message.

    So no single spelling is trusted. `name_variants` produces several, the search unions
    them, and every candidate records which spelling found it — so a variant that silently
    contributes nothing is visible instead of merely absent.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any, Iterable, Optional

import requests

log = logging.getLogger(__name__)

# SEC requires a descriptive User-Agent with a contact address and asks for <= 10 req/s.
# We sit well under it: this is someone else's free service and the whole page depends on
# not being blocked from it.
_UA = os.environ.get(
    "SEC_USER_AGENT",
    "sachoki-desktop research demetrashviligoga@gmail.com")
_HEADERS = {"User-Agent": _UA, "Accept-Encoding": "gzip, deflate"}

_MAX_RPS = 5.0
_FTS = "https://efts.sec.gov/LATEST/search-index"
_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
_SUBMISSIONS = "https://data.sec.gov/submissions"
_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# Financial vehicles that name operating companies for reasons that are never supply chain:
# securitisation trusts listing tenants, blank-check shells, funds listing holdings.
NOISE_SICS = {
    "6189",  # asset-backed securities
    "6199",  # finance services
    "6221", "6770",  # commodity contracts, blank checks
    "6726",  # investment offices / funds
    "6798",  # REITs
    "6022", "6020", "6035",  # banks
}

# A 10-K's main body is the interesting document; EX-* exhibits are mostly boilerplate
# (insider trading policies, subsidiary lists) that mention everyone and mean nothing.
BODY_FILE_TYPES = {"10-K", "10-Q", "8-K", "20-F", "40-F", "S-1", "DEF 14A"}


class _RateLimiter:
    """One shared clock. Threads elsewhere in the app must not be able to burst past it."""

    def __init__(self, rps: float):
        self._min_gap = 1.0 / rps
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            gap = time.monotonic() - self._last
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            self._last = time.monotonic()


_limiter = _RateLimiter(_MAX_RPS)


def _get(url: str, params: dict | None = None, timeout: tuple = (10, 30),
         stream: bool = False, retries: int = 2):
    """Rate-limited GET with a retry on transport failure.

    sec.gov times out intermittently under normal use. Without a retry that surfaces as
    the company having no filings, which is a wrong answer rather than a failure.
    Streaming responses are never retried — the caller is already consuming the body.
    """
    last: Exception | None = None
    for attempt in range(retries + 1):
        _limiter.wait()
        try:
            return requests.get(url, params=params, headers=_HEADERS,
                                timeout=timeout, stream=stream)
        except requests.RequestException as exc:
            last = exc
            if stream or attempt == retries:
                break
            time.sleep(1.0 * (attempt + 1))
    raise last                                                        # type: ignore[misc]


# ── entity parsing ────────────────────────────────────────────────────────────
# 'NVIDIA CORP  (NVDA)  (CIK 0001045810)'  and the multi-class form
# 'AEye, Inc.  (LIDR, LIDRW)  (CIK 0001818644)'
_DISPLAY_RE = re.compile(r"^(?P<name>.+?)\s{2,}(?:\((?P<tickers>[A-Z0-9.,\s-]+)\)\s{2,})?"
                         r"\(CIK (?P<cik>\d{10})\)\s*$")


def parse_display_name(display: str) -> dict:
    """Split an EDGAR display_name into name / tickers / cik.

    Filers with no ticker are real and common — private subsidiaries, funds, foreign
    filers. They are kept with ticker='' rather than dropped: an unlisted single-source
    supplier is exactly the kind of dependency worth knowing about, and dropping it
    because it is not tradeable would be measuring the wrong thing.
    """
    m = _DISPLAY_RE.match(display.strip())
    if not m:
        return {"name": display.strip(), "tickers": [], "cik": "", "display": display}
    tk = [t.strip() for t in (m.group("tickers") or "").split(",") if t.strip()]
    return {"name": m.group("name").strip(), "tickers": tk,
            "cik": m.group("cik"), "display": display}


# ── name variants ─────────────────────────────────────────────────────────────
# Legal suffixes as EDGAR stores them (upper, unpunctuated) → how humans write them.
_SUFFIX_FORMS = {
    "CORP": "Corporation", "CORPORATION": "Corporation", "INC": "Inc.",
    "INCORPORATED": "Inc.", "CO": "Company", "COMPANY": "Company", "LTD": "Ltd.",
    "LIMITED": "Ltd.", "PLC": "plc", "NV": "N.V.", "SA": "S.A.", "AG": "AG",
    "LP": "L.P.", "LLC": "LLC", "TRUST": "Trust", "HOLDINGS": "Holdings",
    "HOLDING": "Holding", "GROUP": "Group", "SE": "SE", "AB": "AB",
}
# Bare single words the index either suppresses or floods on. Never searched alone.
_UNSAFE_BARE = {"TARGET", "APPLE", "BLOCK", "MATCH", "GAP", "AMAZON", "ORACLE", "VISA",
                "SQUARE", "SHELL", "UNITY", "ARM", "NOW", "OPEN", "PATH", "GENERAL",
                "AMERICAN", "NATIONAL", "UNITED", "FIRST", "GLOBAL", "PACIFIC"}


def name_variants(legal_name: str, max_variants: int = 3) -> list[str]:
    """Ways this company's name plausibly appears in someone else's filing.

    Ordered most-specific first. The bare base name is the highest-yield variant and also
    the most dangerous one, so it is withheld for names that are ordinary English words —
    for those the index returns 0 or floods, and neither outcome is informative.
    """
    raw = (legal_name or "").strip()
    if not raw:
        return []
    words = raw.replace(",", " ").replace(".", " ").split()
    # peel trailing legal suffixes: 'ASML HOLDING NV' → base 'ASML', suffixes [HOLDING, NV]
    base_words, tail = list(words), []
    while len(base_words) > 1 and base_words[-1].upper().strip(".") in _SUFFIX_FORMS:
        tail.insert(0, base_words.pop())

    # Tokens are kept exactly as EDGAR stores them. The index is case-insensitive
    # (verified: 'NVIDIA' / 'nvidia' / 'Nvidia' all return 133), so re-casing buys nothing
    # and costs accuracy — title-casing turns NVIDIA into Nvidia and ASML into Asml.
    out, seen = [], set()

    def add(v):
        v = v.strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)

    base = " ".join(base_words)
    if tail:
        add(f"{base} {_SUFFIX_FORMS[tail[0].upper().strip('.')]}")
    add(raw)
    if base.upper() not in _UNSAFE_BARE and len(base) >= 4:
        add(base)
    return out[:max_variants]


# ── country ───────────────────────────────────────────────────────────────────
# EDGAR puts US states and foreign countries in the SAME field. 'CA' is California, 'P7'
# is the Netherlands, and the description field repeats the state code unhelpfully for US
# filers. Reading it naively makes every American company's country 'CA' or 'MN'.
_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE",
    "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "PR", "VI", "GU", "AS", "MP",
}


def resolve_country(code: str, description: str) -> str:
    """Turn EDGAR's state-or-country field into an actual country."""
    code = (code or "").strip().upper()
    desc = (description or "").strip()
    if not code and not desc:
        return ""
    if code in _US_STATES:
        return "United States"
    if desc and desc.upper() != code:
        return desc                       # foreign filer: description is the country name
    return desc or code


def document_url(cik: str, adsh: str, filename: str) -> str:
    """Build the readable URL for a filing document from the pieces the search returns."""
    return f"{_ARCHIVES}/{int(cik)}/{adsh.replace('-', '')}/{filename}"


# ── ticker → CIK, from SEC's own file ─────────────────────────────────────────
_ticker_map: dict[str, dict] | None = None
_map_lock = threading.Lock()


def ticker_map(retries: int = 3) -> dict[str, dict]:
    """SEC's official ticker→CIK table. ~10k rows, fetched once per process.

    A failure is NOT cached. An earlier version stored whatever it got, so a single
    connection timeout to sec.gov cached an empty map and every ticker lookup for the rest
    of the process returned "not found" — a transient blip presenting as "this company
    does not exist", with no error anywhere and nothing to retry.
    """
    global _ticker_map
    with _map_lock:
        if _ticker_map:
            return _ticker_map
        for attempt in range(retries):
            try:
                r = _get(_TICKER_MAP_URL, timeout=(10, 30))
                r.raise_for_status()
                out = {}
                for row in r.json().values():
                    t = (row.get("ticker") or "").upper()
                    if t:
                        out[t] = {"ticker": t,
                                  "cik": str(row.get("cik_str", "")).zfill(10),
                                  "name": row.get("title", "")}
                if out:
                    _ticker_map = out
                    return _ticker_map
            except Exception as exc:                                  # noqa: BLE001
                log.warning("SEC ticker map attempt %d/%d failed: %s",
                            attempt + 1, retries, exc)
                time.sleep(1.5 * (attempt + 1))
        return {}                       # empty, and deliberately not remembered


def resolve(ticker: str) -> Optional[dict]:
    return ticker_map().get(ticker.upper())


# ── name → ticker ─────────────────────────────────────────────────────────────
# A company's own filing names its competitors in prose — "T-Motor, Orqa, ModalAI" — with
# no ticker and no CIK. Turning those words into a listed company is the step that must
# never be guessed, so it is a lookup against SEC's own registry, keyed by NAME.
_name_index: dict[str, dict] | None = None
_name_lock = threading.Lock()

_NAME_NOISE = re.compile(
    r"\b(inc|incorporated|corp|corporation|co|company|ltd|limited|plc|holdings?|group|"
    r"technologies|technology|the|sa|nv|ag|se|ab|lp|llc)\b\.?", re.I)


def _norm_name(s: str) -> str:
    s = (s or "").lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = _NAME_NOISE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def name_index() -> dict[str, dict]:
    global _name_index
    with _name_lock:
        if _name_index is None:
            idx: dict[str, dict] = {}
            for row in ticker_map().values():
                k = _norm_name(row.get("name", ""))
                # first registrant wins; SEC lists share classes under the same name and
                # the alternative is picking one arbitrarily on every lookup
                if k and k not in idx:
                    idx[k] = row
            _name_index = idx
        return _name_index


def resolve_name(name: str) -> Optional[dict]:
    """A company NAME to its SEC registration, or None. Exact on the normalised form only.

    Deliberately strict. Fuzzy matching here would occasionally attach a real quotation
    about one company to a different listed company's row — the most damaging error this
    page can make, and one that looks entirely credible because the citation is genuine.
    Failing to resolve merely leaves the node unlisted, which is the safe direction.
    """
    if not name:
        return None
    return name_index().get(_norm_name(name))


# ── the filer's own profile ───────────────────────────────────────────────────
def submissions(cik: str) -> Optional[dict]:
    """data.sec.gov/submissions — name, tickers, exchanges, SIC, addresses, filing list.

    This is where HQ country comes from, and it is deterministic. Note that
    `stateOfIncorporation` is usually DE and says nothing about where anything happens —
    the contract keeps those two country roles apart for this reason.
    """
    try:
        r = _get(f"{_SUBMISSIONS}/CIK{str(cik).zfill(10)}.json")
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as exc:                                          # noqa: BLE001
        log.warning("submissions %s failed: %s", cik, exc)
        return None


def profile(ticker: str) -> Optional[dict]:
    """Deterministic company identity for the target ticker. No model involved."""
    res = resolve(ticker)
    if not res:
        return None
    sub = submissions(res["cik"])
    if not sub:
        # Degraded, but the SAME SHAPE. A fallback that drops keys turns an upstream
        # timeout into a KeyError three call-frames away, where nothing mentions sec.gov.
        return {"ticker": ticker.upper(), "cik": res["cik"], "name": res["name"],
                "search_names": name_variants(res["name"]), "former_names": [],
                "sic": "", "sic_description": "", "exchanges": [], "tickers": [],
                "hq_country": "", "hq_state": "", "hq_city": "", "incorporation": "",
                "fiscal_year_end": "", "degraded": True,
                "source_url": f"{_SUBMISSIONS}/CIK{str(res['cik']).zfill(10)}.json"}
    addrs = sub.get("addresses") or {}
    # foreign private issuers sometimes carry only a mailing address (TSM has no business one)
    addr = (addrs.get("business") or {}) or (addrs.get("mailing") or {})
    legal = sub.get("name") or res["name"]
    return {
        "ticker": ticker.upper(),
        "cik": res["cik"],
        "name": legal,
        "search_names": name_variants(legal),
        "former_names": [f.get("name", "") for f in (sub.get("formerNames") or [])],
        "sic": sub.get("sic", ""),
        "sic_description": sub.get("sicDescription", ""),
        "exchanges": sub.get("exchanges") or [],
        "tickers": sub.get("tickers") or [],
        "hq_country": resolve_country(addr.get("stateOrCountry", ""),
                                      addr.get("stateOrCountryDescription", "")),
        "hq_state": addr.get("stateOrCountry", ""),
        "hq_city": addr.get("city", ""),
        "incorporation": sub.get("stateOfIncorporation", ""),
        "fiscal_year_end": sub.get("fiscalYearEnd", ""),
        "source_url": f"{_SUBMISSIONS}/CIK{str(res['cik']).zfill(10)}.json",
    }


# ── full-text search ──────────────────────────────────────────────────────────
def full_text_search(phrase: str, forms: Iterable[str] = ("10-K",),
                     start: str = "", end: str = "",
                     from_: int = 0) -> dict:
    """Exact-phrase search across EDGAR filings.

    Phrase matching is strict — `"purchase from NVIDIA"` returns zero while `"NVIDIA"`
    returns hundreds. Templated phrases therefore cannot be used to infer direction; the
    direction has to be read from the passage. Searching for the plain name and reading
    the context is the only approach that does not quietly return nothing.
    """
    params: dict[str, Any] = {"q": f'"{phrase}"'}
    if forms:
        params["forms"] = ",".join(forms)
    if start:
        params["startdt"] = start
    if end:
        params["enddt"] = end
    if from_:
        params["from"] = from_

    try:
        r = _get(_FTS, params=params)
        if r.status_code != 200:
            return {"ok": False, "error": f"EDGAR returned {r.status_code}",
                    "hits": [], "total": 0}
        d = r.json()
    except Exception as exc:                                          # noqa: BLE001
        return {"ok": False, "error": str(exc), "hits": [], "total": 0}

    hits = []
    for h in d.get("hits", {}).get("hits", []):
        s = h.get("_source", {})
        disp = (s.get("display_names") or [""])[0]
        ent = parse_display_name(disp)
        adsh = s.get("adsh", "")
        fname = h.get("_id", "").split(":", 1)[-1]
        cik = (s.get("ciks") or [ent.get("cik") or ""])[0]
        hits.append({
            "cik": cik,
            "name": ent["name"],
            "ticker": ent["tickers"][0] if ent["tickers"] else "",
            "all_tickers": ent["tickers"],
            "form": s.get("form", ""),
            "root_form": (s.get("root_forms") or [""])[0],
            "file_type": s.get("file_type", ""),
            "file_date": s.get("file_date", ""),
            "period_ending": s.get("period_ending", ""),
            "sic": (s.get("sics") or [""])[0],
            "biz_state": (s.get("biz_states") or [""])[0],
            "biz_location": (s.get("biz_locations") or [""])[0],
            "inc_state": (s.get("inc_states") or [""])[0],
            "adsh": adsh,
            "filename": fname,
            "url": document_url(cik, adsh, fname) if cik and adsh else "",
        })

    aggs = {}
    for name in ("entity_filter", "sic_filter", "biz_states_filter"):
        buckets = (d.get("aggregations", {}).get(name) or {}).get("buckets") or []
        aggs[name] = [{"key": b["key"], "count": b["doc_count"]} for b in buckets]

    return {"ok": True, "total": d.get("hits", {}).get("total", {}).get("value", 0),
            "returned": len(hits), "hits": hits, "aggregations": aggs, "phrase": phrase}


def candidate_filers(names: str | list[str], target_cik: str,
                     forms: Iterable[str] = ("10-K", "10-Q"),
                     start: str = "", end: str = "",
                     pages: int = 3, drop_noise_sics: bool = True) -> dict:
    """Every filer that names the target, minus the filer itself and the known noise.

    Searches each spelling in `names` and unions the results, because no single spelling
    is reliable (see the module docstring — the two most defensible choices both return
    almost nothing). `per_variant` reports what each spelling contributed, so a variant
    that is being suppressed by the index shows up as a zero next to a variant that
    worked, instead of as an empty result with no explanation.

    Returns CANDIDATES, not suppliers. The word matters: nothing here has read a sentence
    yet. What comes back is 'these companies wrote this name down in a legal filing' —
    the honest input to an extractor, and a dishonest thing to render as a graph.
    """
    if isinstance(names, str):
        names = [names]
    seen: dict[str, dict] = {}
    dropped = {"self": 0, "noise_sic": 0, "exhibit": 0}
    per_variant: list[dict] = []

    for name in names:
        v_total = v_kept = 0
        for page in range(pages):
            res = full_text_search(name, forms=forms, start=start, end=end, from_=page * 10)
            if not res.get("ok"):
                per_variant.append({"name": name, "error": res.get("error")})
                break
            v_total = res["total"]
            if not res["hits"]:
                break
            for h in res["hits"]:
                if h["cik"] and target_cik and int(h["cik"]) == int(target_cik):
                    dropped["self"] += 1
                    continue
                if drop_noise_sics and h["sic"] in NOISE_SICS:
                    dropped["noise_sic"] += 1
                    continue
                # exhibits mention everyone; the body is where relationships are described
                if h["file_type"] and h["file_type"].startswith("EX-"):
                    dropped["exhibit"] += 1
                    continue
                key = h["cik"] or h["name"]
                prev = seen.get(key)
                if prev is None or h["file_date"] > prev["file_date"]:
                    h = dict(h, found_by=name)
                    seen[key] = h
                    v_kept += 1
        else:
            per_variant.append({"name": name, "documents": v_total, "new_candidates": v_kept})
            continue
        per_variant.append({"name": name, "documents": v_total, "new_candidates": v_kept})

    return {"ok": True, "per_variant": per_variant,
            "candidates": sorted(seen.values(), key=lambda x: x["file_date"], reverse=True),
            "dropped": dropped, "searched_names": list(names)}


# ── pulling the quote out of a filing ─────────────────────────────────────────
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[\s ]+")
MAX_DOC_BYTES = 12 * 1024 * 1024        # a 10-K body; refuse to swallow more than this


def _strip(doc: str) -> str:
    """HTML → plain text. Entities are decoded properly, not by a hand-rolled shortlist.

    Filings are dense with &#160; and &#8239;, and a leftover entity is not cosmetic here:
    the extractor must return a quote that can be found again in this text, so a stray
    '&#160;' inside a sentence would break the verification of an otherwise honest quote.
    """
    import html as _html                                              # noqa: PLC0415
    txt = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", doc)
    txt = _TAG_RE.sub(" ", txt)
    txt = _html.unescape(txt).replace(" ", " ").replace(" ", " ")
    return _WS_RE.sub(" ", txt).strip()


def extract_passages(url: str, needle: str, window: int = 700,
                     max_passages: int = 6) -> dict:
    """Fetch a filing and return the passages around each mention of `needle`.

    These passages are the evidence. A relationship claim without one of these attached is
    not storable under the contract, so this function is the gate between "EDGAR says
    these two names appear together" and "here is the sentence that says why".

    The fetch is capped and streamed: filings run to tens of megabytes and the page must
    not be able to pull one company's whole annual report into memory by accident.
    """
    try:
        r = _get(url, stream=True, timeout=(6, 40))
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}", "passages": []}
        buf, n = [], 0
        for chunk in r.iter_content(chunk_size=262144):
            buf.append(chunk)
            n += len(chunk)
            if n >= MAX_DOC_BYTES:
                break
        r.close()
        html = b"".join(buf).decode("utf-8", errors="replace")
    except Exception as exc:                                          # noqa: BLE001
        return {"ok": False, "error": str(exc), "passages": []}

    text = _strip(html)
    low, nlow = text.lower(), needle.lower()
    out, pos = [], 0
    while len(out) < max_passages:
        i = low.find(nlow, pos)
        if i < 0:
            break
        a, b = max(0, i - window), min(len(text), i + len(needle) + window)
        out.append({"offset": i, "text": text[a:b].strip()})
        pos = i + len(needle) + window          # don't emit overlapping windows

    return {"ok": True, "url": url, "doc_chars": len(text), "truncated": n >= MAX_DOC_BYTES,
            "mentions": low.count(nlow), "passages": out}


__all__ = ["profile", "resolve", "submissions", "full_text_search", "candidate_filers",
           "extract_passages", "parse_display_name", "document_url", "ticker_map",
           "NOISE_SICS"]
