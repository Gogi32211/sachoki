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
from datetime import datetime, time as _dtime
from typing import Optional

import requests

log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None


def _regular_session_open() -> bool:
    """True only during the US regular session (Mon–Fri 9:30–16:00 ET).

    RT% (todaysChangePerc / day.c) is meaningful only while today's regular
    session is live. Pre-market / overnight, Massive's snapshot still carries the
    PREVIOUS session in `day`, so RT% would show stale yesterday data — hide it.
    (Holidays aren't handled; on a holiday RT% just stays ~flat, harmless.)"""
    if _ET is None:
        return True  # no tz info → don't gate (fail open)
    now = datetime.now(_ET)
    if now.weekday() >= 5:          # Sat/Sun
        return False
    return _dtime(9, 30) <= now.time() <= _dtime(16, 0)

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

    def _f(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    prev_c = _f(prev_day.get("c"))
    day_c  = _f(day.get("c"))      # live regular-session price; 0/None before open
    min_c  = _f(minute.get("c"))   # last trade — updates in pre/post market too
    min_vol = minute.get("v")
    day_open = bool(day_c and day_c > 0)

    # Last COMPLETED regular-session close. Massive rolls `day` at the open, so
    # before that the prior close may sit in day.c (stale) OR — once rolled —
    # in prevDay.c. Either way this is the correct pre/post-market baseline.
    last_close = day_c if day_open else prev_c

    # ── PM% — extended-hours (pre/post-market) move vs last completed close ─────
    # min.c is the last trade (pre-market price when session is closed). Shown
    # only OUTSIDE the regular session (gated in get_premarket).
    pm_price = min_c
    pm_vol   = int(min_vol) if min_vol else None
    pm_chg_pct: Optional[float] = None
    if min_c is not None and last_close:
        pm_chg_pct = round((min_c - last_close) / last_close * 100, 2)

    # ── RT% — regular-session move (day.c live + todaysChangePerc). Shown only
    # DURING the regular session (gated in get_premarket). ──────────────────────
    rt_price = day_c if day_open else None
    rt_chg_pct: Optional[float] = None
    _tcp = snap.get("todaysChangePerc")
    if _tcp is not None:
        try:
            rt_chg_pct = round(float(_tcp), 2)
        except (TypeError, ValueError):
            rt_chg_pct = None
    if rt_chg_pct is None and rt_price is not None and prev_c:
        rt_chg_pct = round((rt_price - prev_c) / prev_c * 100, 2)

    return {
        "pm_price":    round(pm_price, 4) if pm_price is not None else None,
        "pm_chg_pct":  pm_chg_pct,
        "pm_vol":      pm_vol,
        "prev_close":  round(prev_c, 4) if prev_c is not None else None,
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
        out = {t: dict(_data[t]) for t in tickers_upper if t in _data}
    # Show RT% only during the regular session, PM% only outside it (pre/post
    # market) — they're complementary windows. Avoids RT% showing stale prior-
    # session data pre-market, and surfaces the live pre-market move in PM%.
    if _regular_session_open():
        for v in out.values():
            v["pm_chg_pct"] = None
            v["pm_price"]   = None
    else:
        for v in out.values():
            v["rt_chg_pct"] = None
            v["rt_price"]   = None
    return out
