"""Massive.com data provider — primary metadata + snapshot source.

API key: MASSIVE_API_KEY env var.
Base:    https://api.massive.com  (Polygon-compatible v2/v3 API)

Priority: Massive → yfinance fallback (only if ALLOW_YFINANCE_FALLBACK=1).

Provides:
  get_ticker_info(ticker)   → name, sector, industry, market_cap, logo, description
  get_snapshot(ticker)      → today's price, change %, prev close
  get_snapshot_batch(tickers) → same for multiple tickers in one call
"""
from __future__ import annotations
import logging
import os
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_BASE        = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
_API_KEY     = os.environ.get("MASSIVE_API_KEY", "")
_ALLOW_YF    = os.environ.get("ALLOW_YFINANCE_FALLBACK", "0").strip() in ("1", "true", "True")

_INFO_TTL    = 86_400   # 24h  — company metadata rarely changes
_SNAP_TTL    = 60       # 60s  — price snapshot

_info_cache: Dict[str, tuple[Dict[str, Any], float]] = {}
_snap_cache: Dict[str, tuple[Dict[str, Any], float]] = {}

# Sentinel cached for tickers the snapshot endpoint returned no data for (delisted,
# halted, not found). Lets the cache "warm" on misses so we don't re-request the
# same dead symbols every call, while keeping them ABSENT from caller result dicts.
_MISS: Dict[str, Any] = {"__miss__": True}


# ── SIC code → GICS-like sector ──────────────────────────────────────────────
def _sic_to_sector(sic: int) -> str:
    """Map SIC code to a broad sector label."""
    if not sic:
        return ""
    # Healthcare
    if 2830 <= sic <= 2836 or 5047 <= sic <= 5047 or 8000 <= sic <= 8099:
        return "Healthcare"
    # Financials
    if 6020 <= sic <= 6199 or 6200 <= sic <= 6299 or 6300 <= sic <= 6411:
        return "Financials"
    if 6700 <= sic <= 6726:
        return "Financials"
    # Real Estate
    if 6500 <= sic <= 6552:
        return "Real Estate"
    # Utilities
    if 4900 <= sic <= 4991:
        return "Utilities"
    # Energy
    if 1311 <= sic <= 1382 or 2900 <= sic <= 2911 or 4610 <= sic <= 4619:
        return "Energy"
    # Communication Services
    if 4800 <= sic <= 4899 or 7810 <= sic <= 7819 or 7900 <= sic <= 7999:
        return "Communication Services"
    # Technology
    if (3570 <= sic <= 3579 or 3670 <= sic <= 3679 or
            3810 <= sic <= 3812 or 7370 <= sic <= 7379):
        return "Technology"
    if 3825 <= sic <= 3827 or 3812 <= sic <= 3812:
        return "Technology"
    # Consumer Staples
    if (2000 <= sic <= 2111 or 2200 <= sic <= 2299 or
            5400 <= sic <= 5499 or 5910 <= sic <= 5912):
        return "Consumer Staples"
    # Consumer Discretionary
    if 5200 <= sic <= 5999 or 7000 <= sic <= 7299 or 7510 <= sic <= 7549:
        return "Consumer Discretionary"
    if 2300 <= sic <= 2399 or 5600 <= sic <= 5699:
        return "Consumer Discretionary"
    # Materials
    if 1000 <= sic <= 1499 or 2400 <= sic <= 2899 or 3000 <= sic <= 3399:
        return "Materials"
    # Industrials
    if 3400 <= sic <= 3569 or 3580 <= sic <= 3669 or 3690 <= sic <= 3809:
        return "Industrials"
    if 4000 <= sic <= 4799 or 7380 <= sic <= 7389 or 8700 <= sic <= 8742:
        return "Industrials"
    return ""


# ── Exchange code → readable name ────────────────────────────────────────────
_EXCHANGE_MAP = {
    "XNAS": "NASDAQ", "XNYS": "NYSE", "XASE": "NYSE American",
    "ARCX": "NYSE Arca", "BATS": "CBOE BZX",
}


# ── Cache helpers ─────────────────────────────────────────────────────────────
def _cget(cache: dict, key: str, ttl: float) -> Optional[Dict]:
    entry = cache.get(key)
    if entry and (time.time() - entry[1]) < ttl:
        return entry[0]
    return None


def _cset(cache: dict, key: str, val: Dict) -> None:
    cache[key] = (val, time.time())


# ── Massive reference fetch ───────────────────────────────────────────────────
def _massive_reference(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch ticker reference from Massive /v3/reference/tickers/{ticker}."""
    if not _API_KEY:
        return None
    try:
        import requests
        url = f"{_BASE}/v3/reference/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": _API_KEY}, timeout=5)
        if r.status_code != 200:
            log.debug("Massive reference %s → %d", ticker, r.status_code)
            return None
        d = r.json().get("results") or {}
        if not d:
            return None

        sic = int(d.get("sic_code") or 0)
        sector = _sic_to_sector(sic)
        # sic_description is more specific than sector — use as industry
        industry = (d.get("sic_description") or "").title()

        # Branding logo
        branding = d.get("branding") or {}
        logo_url = branding.get("icon_url") or branding.get("logo_url") or ""

        exchange_raw = d.get("primary_exchange", "")
        exchange = _EXCHANGE_MAP.get(exchange_raw, exchange_raw)

        return {
            "ticker":       ticker,
            "name":         d.get("name") or ticker,
            "sector":       sector,
            "industry":     industry,
            "market_cap":   d.get("market_cap"),
            "float_shares": d.get("share_class_shares_outstanding"),
            "total_employees": d.get("total_employees"),
            "list_date":    d.get("list_date", ""),
            "exchange":     exchange,
            "description":  d.get("description", "") or "",
            "logo_url":     logo_url,
            "homepage_url": d.get("homepage_url", "") or "",
            "source":       "massive",
        }
    except Exception as exc:
        log.warning("Massive reference failed for %s: %s", ticker, exc)
        return None


def _yfinance_info(ticker: str) -> Dict[str, Any]:
    """Last-resort yfinance fallback. Returns same schema."""
    stub = {
        "ticker": ticker, "name": ticker, "sector": "", "industry": "",
        "market_cap": None, "float_shares": None, "total_employees": None,
        "list_date": "", "exchange": "", "description": "", "logo_url": "",
        "homepage_url": "", "source": "error",
    }
    if not _ALLOW_YF:
        return stub
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        return {
            **stub,
            "name":         info.get("shortName") or info.get("longName") or ticker,
            "sector":       info.get("sector", "") or "",
            "industry":     info.get("industry", "") or "",
            "market_cap":   info.get("marketCap"),
            "float_shares": info.get("floatShares"),
            "total_employees": info.get("fullTimeEmployees"),
            "exchange":     info.get("exchange", "") or "",
            "description":  info.get("longBusinessSummary", "") or "",
            "source":       "yfinance",
        }
    except Exception as exc:
        log.warning("yfinance info fallback failed for %s: %s", ticker, exc)
        return stub


def get_ticker_info(ticker: str) -> Dict[str, Any]:
    """Metadata: name, sector, industry, market_cap, logo, description.
    Source priority: cache → Massive /v3/reference → yfinance fallback.
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {"ticker": "", "source": "error"}

    cached = _cget(_info_cache, ticker, _INFO_TTL)
    if cached is not None:
        return cached

    result = _massive_reference(ticker) or _yfinance_info(ticker)
    _cset(_info_cache, ticker, result)
    return result


# Keep old name as alias for callers that use it
get_ticker_info_massive = get_ticker_info


# ── Snapshot (today's price + change) ────────────────────────────────────────
def get_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
    """Today's market snapshot via Massive.
    Returns: price, change_pct, prev_close, volume, vwap, open, high, low
    """
    ticker = (ticker or "").upper().strip()
    if not ticker or not _API_KEY:
        return None

    cached = _cget(_snap_cache, ticker, _SNAP_TTL)
    if cached is not None:
        return None if cached is _MISS else cached

    try:
        import requests
        url = f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        r = requests.get(url, params={"apiKey": _API_KEY}, timeout=5)
        if r.status_code != 200:
            return None
        t = (r.json().get("ticker") or {})
        day = t.get("day") or {}
        prev = t.get("prevDay") or {}

        result = {
            "ticker":     ticker,
            "price":      day.get("c"),
            "open":       day.get("o"),
            "high":       day.get("h"),
            "low":        day.get("l"),
            "volume":     day.get("v"),
            "vwap":       day.get("vw"),
            "change_pct": t.get("todaysChangePerc"),
            "change":     t.get("todaysChange"),
            "prev_close": prev.get("c"),
        }
        _cset(_snap_cache, ticker, result)
        return result
    except Exception as exc:
        log.debug("Massive snapshot failed for %s: %s", ticker, exc)
        return None


def get_snapshot_batch(tickers: List[str]) -> Dict[str, Dict[str, Any]]:
    """Batch snapshot via Massive /v2/snapshot/locale/us/markets/stocks/tickers."""
    tickers = [t.upper().strip() for t in tickers if t]
    if not tickers or not _API_KEY:
        return {}

    # Check what's already cached
    result: Dict[str, Any] = {}
    need   = []
    for t in tickers:
        cached = _cget(_snap_cache, t, _SNAP_TTL)
        if cached is _MISS:
            continue                       # known miss, still fresh — skip, don't re-request
        if cached is not None:
            result[t] = cached
        else:
            need.append(t)

    if not need:
        return result

    try:
        import requests
        joined = ",".join(need)
        url = f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers"
        r = requests.get(url, params={"tickers": joined, "apiKey": _API_KEY}, timeout=10)
        if r.status_code == 200:
            for snap in (r.json().get("tickers") or []):
                t    = snap.get("ticker", "")
                day  = snap.get("day") or {}
                prev = snap.get("prevDay") or {}
                entry = {
                    "ticker":     t,
                    "price":      day.get("c"),
                    "open":       day.get("o"),
                    "high":       day.get("h"),
                    "low":        day.get("l"),
                    "volume":     day.get("v"),
                    "vwap":       day.get("vw"),
                    "change_pct": snap.get("todaysChangePerc"),
                    "change":     snap.get("todaysChange"),
                    "prev_close": prev.get("c"),
                }
                _cset(_snap_cache, t, entry)
                result[t] = entry
            # Cache a miss sentinel for anything we asked for but didn't get back,
            # so dead/halted symbols don't trigger a full re-request every call.
            for t in need:
                if t not in result:
                    _cset(_snap_cache, t, _MISS)
    except Exception as exc:
        log.warning("Massive batch snapshot failed: %s", exc)

    return result


def get_ticker_info_batch(tickers: list[str]) -> Dict[str, Dict[str, Any]]:
    """Batch info helper."""
    return {t: get_ticker_info(t) for t in tickers}
