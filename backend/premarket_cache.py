"""
premarket_cache.py — lightweight pre-market price/change cache for Ultra scan.

Fetches pre-market data from Massive snapshot API in batches of 100.
Cache TTL: 15 minutes. Thread-safe via a Lock.

Endpoint exposed by main.py: GET /api/premarket?tickers=A,B,C,...
"""
from __future__ import annotations

import os
import time
import logging
import threading
from typing import Optional

import requests

log = logging.getLogger(__name__)

_CACHE_TTL   = 900   # 15 minutes
_BATCH_SIZE  = 100   # Massive snapshot allows up to ~300; 100 is safe
_REQUEST_TIMEOUT = (5, 20)

# ── Cache state ──────────────────────────────────────────────────────────────
_lock:        threading.Lock = threading.Lock()
_data:        dict[str, dict] = {}   # ticker → {pm_price, pm_chg_pct, fetched_at}
_fetched_at:  dict[str, float] = {}  # ticker → unix timestamp of last successful fetch

# ── Helpers ──────────────────────────────────────────────────────────────────

def _api_key() -> str:
    return (os.environ.get("MASSIVE_API_KEY") or
            os.environ.get("POLYGON_API_KEY") or "")


def _base_url() -> str:
    try:
        from data_polygon import _BASE
        return _BASE
    except Exception:
        return "https://api.massive.com"


def _fetch_batch(tickers: list[str]) -> dict[str, dict]:
    """Fetch snapshot for up to _BATCH_SIZE tickers. Returns map of ticker → raw snapshot."""
    key = _api_key()
    if not key or not tickers:
        return {}

    base = _base_url()
    result: dict[str, dict] = {}
    try:
        r = requests.get(
            f"{base}/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"tickers": ",".join(tickers), "apiKey": key},
            timeout=_REQUEST_TIMEOUT,
        )
        if r.status_code == 200:
            for item in r.json().get("tickers", []):
                sym = (item.get("ticker") or "").upper()
                if sym:
                    result[sym] = item
        else:
            log.warning("premarket_cache: snapshot batch HTTP %s", r.status_code)
    except Exception as exc:
        log.warning("premarket_cache: batch fetch error: %s", exc)
    return result


def _parse_snapshot(snap: dict) -> dict:
    """Extract pre-market price and change% from a Massive snapshot item.

    Massive API does not expose a dedicated preMarket field.
    Strategy:
      1. day.c — regular session close (populated after 9:30 AM ET open)
      2. min.c — last minute-bar close (during pre-market this IS the PM price)
    We use min.c as pre-market price when day.c is absent/zero (session not open yet).
    """
    prev_day  = snap.get("prevDay") or {}
    day       = snap.get("day")     or {}
    minute    = snap.get("min")     or {}

    prev_close = prev_day.get("c")
    day_close  = day.get("c")          # 0 or None before regular open
    min_close  = minute.get("c")       # last trade (pre-market if session not open)
    min_vol    = minute.get("v")

    # Use day close if session is open (day.c > 0), else fall back to last minute bar
    day_open   = bool(day_close and float(day_close) > 0)
    pm_price   = None if day_open else (min_close if min_close else None)
    pm_vol     = None if day_open else (int(min_vol) if min_vol else None)

    pm_chg_pct: Optional[float] = None
    if pm_price is not None and prev_close and float(prev_close) != 0:
        pm_chg_pct = round(
            (float(pm_price) - float(prev_close)) / float(prev_close) * 100, 2
        )

    # ── RT (regular-session real-time) — same snapshot, no extra API call ───────
    # day.c is the live last regular-session price (>0 once 9:30 ET opens, and the
    # final close after hours). Massive snapshot also carries `todaysChangePerc`
    # (regular-session % vs prev close) at item level — prefer it, else compute.
    rt_price = float(day_close) if day_open else None
    rt_chg_pct: Optional[float] = None
    _tcp = snap.get("todaysChangePerc")
    if _tcp is not None:
        try:
            rt_chg_pct = round(float(_tcp), 2)
        except (TypeError, ValueError):
            rt_chg_pct = None
    if rt_chg_pct is None and rt_price is not None and prev_close and float(prev_close) != 0:
        rt_chg_pct = round((rt_price - float(prev_close)) / float(prev_close) * 100, 2)

    return {
        "pm_price":    round(float(pm_price), 4) if pm_price is not None else None,
        "pm_chg_pct":  pm_chg_pct,
        "pm_vol":      pm_vol,
        "prev_close":  round(float(prev_close), 4) if prev_close is not None else None,
        "rt_price":    round(rt_price, 4) if rt_price is not None else None,
        "rt_chg_pct":  rt_chg_pct,
    }


# ── Public API ───────────────────────────────────────────────────────────────

def get_premarket(tickers: list[str]) -> dict[str, dict]:
    """Return pre-market data for the given tickers.

    Stale entries (> TTL) are refreshed; fresh entries are returned from cache.
    Returns dict keyed by ticker (uppercase). Missing/failed tickers are omitted.
    """
    if not tickers:
        return {}

    now = time.time()
    tickers_upper = [t.upper() for t in tickers if t]

    # Split into stale vs fresh
    stale = [t for t in tickers_upper
             if now - _fetched_at.get(t, 0) > _CACHE_TTL]

    if stale:
        log.info("premarket_cache: refreshing %d stale tickers", len(stale))
        raw_snaps: dict[str, dict] = {}
        for i in range(0, len(stale), _BATCH_SIZE):
            batch = stale[i : i + _BATCH_SIZE]
            raw_snaps.update(_fetch_batch(batch))

        with _lock:
            for ticker in stale:
                snap = raw_snaps.get(ticker)
                if snap:
                    _data[ticker]       = _parse_snapshot(snap)
                    _fetched_at[ticker] = now
                else:
                    # Mark attempted so we don't spam the API for missing tickers
                    _fetched_at[ticker] = now

    with _lock:
        return {t: _data[t] for t in tickers_upper if t in _data}
