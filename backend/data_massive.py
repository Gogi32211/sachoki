"""Massive.com data provider — primary metadata source with yfinance fallback.

API key is read from env: MASSIVE_API_KEY. Base URL: MASSIVE_BASE_URL
(defaults to https://api.massive.com/v1).

If MASSIVE_API_KEY is unset or the API returns an error (404, 5xx, timeout),
falls back to yfinance silently. Callers should treat `source` in the result
dict to know which provider answered.

Cache: in-memory dict, 24h TTL for metadata, 5min for OHLCV (when added).
"""
from __future__ import annotations
import logging
import os
import time
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

MASSIVE_BASE = os.environ.get("MASSIVE_BASE_URL", "https://api.massive.com/v1")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY", "")

MASSIVE_CACHE_TTL_SECONDS = 86_400  # 24h for metadata
MASSIVE_OHLCV_TTL_SECONDS = 300     # 5min for price data

# Module-level in-memory cache: ticker → (data, timestamp)
_info_cache: Dict[str, tuple[Dict[str, Any], float]] = {}


def _cache_get(ticker: str) -> Optional[Dict[str, Any]]:
    entry = _info_cache.get(ticker)
    if entry is None:
        return None
    data, ts = entry
    if time.time() - ts >= MASSIVE_CACHE_TTL_SECONDS:
        return None
    return data


def _cache_set(ticker: str, data: Dict[str, Any]) -> None:
    _info_cache[ticker] = (data, time.time())


_ALLOW_YF_FALLBACK = os.environ.get("ALLOW_YFINANCE_FALLBACK", "0").strip() in ("1", "true", "True")


def _yfinance_info(ticker: str) -> Dict[str, Any]:
    """Fallback path. Used only when ALLOW_YFINANCE_FALLBACK=1.
    Returns the same schema as Massive."""
    if not _ALLOW_YF_FALLBACK:
        log.warning("data_massive: Massive unavailable for %s and "
                    "ALLOW_YFINANCE_FALLBACK is off — returning stub", ticker)
        return {
            "ticker": ticker, "name": ticker, "sector": "", "industry": "",
            "market_cap": None, "float_shares": None, "avg_volume_30d": None,
            "exchange": "", "description": "", "logo_url": "",
            "source": "error",
        }
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        return {
            "ticker":         ticker,
            "name":           info.get("shortName") or info.get("longName") or ticker,
            "sector":         info.get("sector", "") or "",
            "industry":       info.get("industry", "") or "",
            "market_cap":     info.get("marketCap"),
            "float_shares":   info.get("floatShares"),
            "avg_volume_30d": info.get("averageVolume"),
            "exchange":       info.get("exchange", "") or "",
            "description":    info.get("longBusinessSummary", "") or "",
            "logo_url":       "",
            "source":         "yfinance",
        }
    except Exception as e:
        log.warning("yfinance fallback failed for %s: %s", ticker, e)
        return {
            "ticker": ticker, "name": ticker, "sector": "", "industry": "",
            "market_cap": None, "float_shares": None, "avg_volume_30d": None,
            "exchange": "", "description": "", "logo_url": "",
            "source": "error",
        }


def get_ticker_info_massive(ticker: str) -> Dict[str, Any]:
    """Synchronous metadata fetch with Massive-first, yfinance-fallback.

    Returns a dict with keys: ticker, name, sector, industry, market_cap,
    float_shares, avg_volume_30d, exchange, description, logo_url, source.
    `source` ∈ {"massive", "yfinance", "error", "cache"}.
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {"ticker": "", "source": "error"}

    cached = _cache_get(ticker)
    if cached is not None:
        return {**cached, "source": cached.get("source", "cache")}

    # Massive primary
    if MASSIVE_API_KEY:
        try:
            import requests  # standard sync HTTP
            headers = {"Authorization": f"Bearer {MASSIVE_API_KEY}"}
            url = f"{MASSIVE_BASE.rstrip('/')}/securities/{ticker}"
            r = requests.get(url, headers=headers, timeout=5.0)
            if r.status_code == 200:
                d = r.json() or {}
                result = {
                    "ticker":         ticker,
                    "name":           d.get("name", ticker),
                    "sector":         d.get("sector", "") or "",
                    "industry":       d.get("industry", "") or "",
                    "market_cap":     d.get("market_cap"),
                    "float_shares":   d.get("float_shares"),
                    "avg_volume_30d": d.get("avg_volume_30d"),
                    "exchange":       d.get("exchange", "") or "",
                    "description":    d.get("description", "") or "",
                    "logo_url":       d.get("logo_url", "") or "",
                    "source":         "massive",
                }
                _cache_set(ticker, result)
                return result
            else:
                log.info("massive %s → %d, falling back to yfinance",
                         ticker, r.status_code)
        except Exception as e:
            log.warning("massive request failed for %s: %s — falling back", ticker, e)

    # Fallback
    fallback = _yfinance_info(ticker)
    _cache_set(ticker, fallback)
    return fallback


def get_ticker_info_batch(tickers: list[str]) -> Dict[str, Dict[str, Any]]:
    """Batch helper. Calls get_ticker_info_massive for each ticker, with cache."""
    return {t: get_ticker_info_massive(t) for t in tickers}
