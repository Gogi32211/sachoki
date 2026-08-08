"""data_options_cboe.py — FREE Cboe delayed options chains (2026-08-03).

Built for the Massive-Options-Starter cancellation plan: Cboe's public delayed-quotes CDN
serves full chains (greeks + OI + IV) for stocks AND true index options, 15-min delayed —
the same latency class as the paid Massive snapshot. This module normalizes it to the
EXACT row shape data_options.fetch_chain returns, so gex_engine.compute_gex consumes it
unchanged:  [{strike, type, expiration, dte, oi, gamma, delta, iv, volume, close}].

Endpoint (no key): https://cdn.cboe.com/api/global/delayed_quotes/options/{SYM}.json
  * stocks/ETFs: plain symbol (AAPL.json, SPY.json)
  * indices: underscore prefix (_SPX.json, _NDX.json, _RUT.json, _VIX.json)

UNIT NOTE: Cboe iv is a DECIMAL fraction (SPY ATM ≈ 0.0999) — the SAME unit the Massive
path feeds compute_gex, which does its own ×100 percent conversion. Do NOT pre-scale here
(the first parity run double-scaled to "1144%").

Unofficial CDN: no SLA, be polite (10-min cache, one request per ticker). If Cboe ever
blocks or reshapes it, gex sources fail loudly (empty chain) — the Massive path remains
in data_options.py untouched until the parity week says it can go.
"""
from __future__ import annotations
import json
import logging
import re
import time
from datetime import date, datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

_BASE = "https://cdn.cboe.com/api/global/delayed_quotes/options"
_UA = {"User-Agent": "sachoki-desktop research demetrashviligoga@gmail.com"}
_TIMEOUT = 25
_CACHE: dict = {}
_CACHE_TTL = 600            # 10 min, matches the Massive fetcher's cache
# indices Cboe serves under an underscore prefix
_INDEX = {"SPX", "NDX", "RUT", "VIX", "XSP", "DJX"}
_OCC = re.compile(r"^([A-Z.]{1,6})(\d{6})([CP])(\d{8})$")


def _sym(ticker: str) -> str:
    t = (ticker or "").upper().strip().lstrip("_")
    return f"_{t}" if t in _INDEX else t


_MIN_INTERVAL = 0.70        # ~1.4 req/s: 0.35s still drew 161 429s on the 600-ticker pass
_last_req = [0.0]           # back-to-back requests and Cboe 429'd all but 29 (2026-08-04).


def _fetch_raw(ticker: str) -> Optional[dict]:
    ck = _sym(ticker)
    now = time.time()
    hit = _CACHE.get(ck)
    if hit and (now - hit[0]) < _CACHE_TTL:
        return hit[1]
    for attempt in (1, 2):
        wait = _MIN_INTERVAL - (time.time() - _last_req[0])
        if wait > 0:
            time.sleep(wait)
        _last_req[0] = time.time()
        try:
            r = requests.get(f"{_BASE}/{ck}.json", headers=_UA, timeout=_TIMEOUT)
            if r.status_code == 429 and attempt == 1:
                time.sleep(15.0)             # one patient backoff retry on rate-limit
                continue
            if r.status_code != 200:
                log.warning("cboe chain %s → %d", ck, r.status_code)
                return None
            d = r.json().get("data") or {}
            _CACHE[ck] = (time.time(), d)
            return d
        except Exception as e:
            log.warning("cboe chain %s failed: %s", ck, e)
            return None
    return None


def spot_price_cboe(ticker: str) -> Optional[float]:
    d = _fetch_raw(ticker)
    if not d:
        return None
    for k in ("current_price", "close"):
        v = d.get(k)
        if v:
            return float(v)
    return None


def fetch_chain_cboe(ticker: str, max_dte: int = 60, today: Optional[date] = None,
                     expiration: Optional[str] = None) -> list[dict]:
    """Same contract as data_options.fetch_chain, sourced from the free Cboe CDN."""
    d = _fetch_raw(ticker)
    if not d:
        return []
    today = today or date.today()
    out: list[dict] = []
    for c in d.get("options") or []:
        m = _OCC.match(c.get("option") or "")
        if not m:
            continue
        _, ymd, cp, k8 = m.groups()
        try:
            exp = datetime.strptime(ymd, "%y%m%d").date()
        except ValueError:
            continue
        dte = (exp - today).days
        if dte < 0 or dte > max_dte:
            continue
        exp_iso = exp.isoformat()
        if expiration and exp_iso != expiration:
            continue
        oi = c.get("open_interest")
        if not oi or oi <= 0:
            continue
        # iv: Cboe serves a DECIMAL fraction — exactly what compute_gex expects (it does
        # the ×100 percent conversion itself; pre-scaling here double-counted to 1144%
        # on the first parity run). Sanity cap: deep-ITM/stale quotes carry garbage IV
        # (11.4 = "1140%") — keep the row (gamma/OI still real for GEX), null only the iv.
        iv = c.get("iv")
        iv_dec = float(iv) if iv and 0 < float(iv) <= 3.0 else None
        out.append({
            "strike": int(k8) / 1000.0,
            "type": "call" if cp == "C" else "put",
            "expiration": exp_iso, "dte": dte,
            "oi": float(oi), "gamma": c.get("gamma"), "delta": c.get("delta"),
            "iv": iv_dec,                                  # decimal, same as the Massive path
            "volume": c.get("volume"), "close": c.get("last_trade_price"),
        })
    return out


def list_expirations_cboe(ticker: str, max_dte: int = 120) -> list[dict]:
    today = date.today()
    seen: dict = {}
    for row in fetch_chain_cboe(ticker, max_dte=max_dte, today=today):
        seen.setdefault(row["expiration"], row["dte"])
    return [{"date": d, "dte": n} for d, n in sorted(seen.items())]
