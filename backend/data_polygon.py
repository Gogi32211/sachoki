"""
data_polygon.py — Massive (formerly Polygon.io) OHLCV data fetcher.

API key is read from the MASSIVE_API_KEY environment variable
(POLYGON_API_KEY also accepted for backwards compatibility).

Base URL: https://api.massive.com  (fallback: https://api.polygon.io)

Interval mapping:
  "1d"  → 1 day       "1wk" / "1w" → 1 week
  "4h"  → 4 hour      "1h"  → 1 hour
  "30m" → 30 minute   "15m" → 15 minute

Usage:
    from data_polygon import fetch_bars
    df = fetch_bars("AAPL", interval="1d", days=180)
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

log = logging.getLogger(__name__)

# Massive (new name) first, Polygon (old name) as fallback
_BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")

_SPAN = {
    "1d":  (1, "day"),
    "1wk": (1, "week"),
    "1w":  (1, "week"),
    "4h":  (4, "hour"),
    "1h":  (1, "hour"),
    "30m": (30, "minute"),
    "15m": (15, "minute"),
}

# conservative: Starter plan is unlimited but burst-friendly
_RATE_DELAY = 0.08   # ~12 req/s across workers


def _key() -> str:
    # check both env var names
    k = (os.environ.get("MASSIVE_API_KEY") or
         os.environ.get("POLYGON_API_KEY") or "")
    if not k:
        raise EnvironmentError("MASSIVE_API_KEY not set")
    return k


def fetch_bars(
    ticker: str,
    interval: str = "1d",
    days: int = 180,
) -> pd.DataFrame:
    """
    Fetch OHLCV bars from Massive/Polygon.
    Returns a DataFrame with columns open/high/low/close/volume indexed by UTC datetime.
    Raises on network error or empty result.
    """
    mult, span = _SPAN.get(interval, (1, "day"))
    now  = datetime.now(timezone.utc)
    frm  = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    to   = now.strftime("%Y-%m-%d")
    url  = f"{_BASE}/v2/aggs/ticker/{ticker}/range/{mult}/{span}/{frm}/{to}"

    params = {
        "adjusted": "true",
        "sort":     "asc",
        "limit":    50000,
        "apiKey":   _key(),
    }

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                wait = 12 * (attempt + 1)
                log.debug("Polygon rate-limit %s, waiting %ds", ticker, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            break
        except requests.RequestException as exc:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    else:
        raise RuntimeError(f"Polygon: max retries reached for {ticker}")

    results = data.get("results") or []
    if not results:
        raise ValueError(f"Polygon: no data for {ticker} ({interval})")

    df = pd.DataFrame(results)
    df = df.rename(columns={
        "o": "open", "h": "high", "l": "low",
        "c": "close", "v": "volume", "t": "timestamp",
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("timestamp")[["open", "high", "low", "close", "volume"]]
    df = df[~df.index.duplicated()].sort_index()

    time.sleep(_RATE_DELAY)  # gentle pacing across concurrent workers
    return df


def polygon_available() -> bool:
    """True if MASSIVE_API_KEY (or POLYGON_API_KEY) is set in environment."""
    return bool(os.environ.get("MASSIVE_API_KEY") or
                os.environ.get("POLYGON_API_KEY"))


def get_all_us_tickers(market: str = "stocks", limit: int = 10_000) -> list[str]:
    """
    Fetch all active US stock tickers from Polygon reference endpoint.
    Requires Starter plan+. Returns list of ticker strings.
    """
    tickers: list[str] = []
    url = f"{_BASE}/v3/reference/tickers"
    params = {
        "market":  market,
        "locale":  "us",
        "active":  "true",
        "limit":   1000,
        "apiKey":  _key(),
    }
    while url and len(tickers) < limit:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        for t in data.get("results", []):
            sym = t.get("ticker", "")
            if sym:
                tickers.append(sym)
        # pagination
        url    = data.get("next_url")
        params = {"apiKey": _key()}   # next_url already has other params
        time.sleep(0.2)

    log.info("Polygon: fetched %d tickers", len(tickers))
    return tickers[:limit]
