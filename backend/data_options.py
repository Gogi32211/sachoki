"""data_options.py — Massive options-chain snapshot fetcher (2026-07-22).

ISOLATED, ADDITIVE module for the GEX layer. Touches nothing in the stock pipeline;
if the Options plan is cancelled the snapshot endpoint 403s and every caller degrades
gracefully (returns None / empty). Requires an Options subscription (Starter $29+):
/v3/snapshot/options/{underlying} returns per-strike open_interest + greeks (gamma,
delta, theta, vega) + implied_volatility — the raw inputs for gamma-exposure levels.

Data is 15-min delayed on Starter/Developer — fine for our EOD/swing GEX use.
"""
from __future__ import annotations
import os
import time
import logging
from datetime import date, datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

_BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
_TIMEOUT = 30
_MAX_PAGES = 24          # 24 × 250 = 6000 contracts — covers near+mid term for any name
_CACHE: dict = {}        # {(ticker, max_dte): (ts, chain_list)}
_CACHE_TTL = 600         # 10 min — GEX levels move slowly; delayed data anyway


def _key() -> str:
    k = os.environ.get("MASSIVE_API_KEY", "")
    if not k:
        for p in (".env", "../.env"):
            if os.path.exists(p):
                for line in open(p):
                    if line.startswith("MASSIVE_API_KEY"):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
    return k


def options_available() -> bool:
    """True if the current key can read the options snapshot (Options plan active)."""
    try:
        r = requests.get(f"{_BASE}/v3/snapshot/options/SPY",
                         params={"limit": 1, "apiKey": _key()}, timeout=_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def _dte(exp: str, today: date) -> int:
    try:
        return (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
    except Exception:
        return 9999


_EXP_CACHE: dict = {}       # {ticker: (ts, [{date, dte}, ...])}
_EXP_TTL = 3600            # 1h — expiration calendar changes slowly


def list_expirations(ticker: str, today: Optional[date] = None) -> list[dict]:
    """Available option expirations for `ticker` as [{date, dte}, …] (ascending),
    cheaply from /v3/reference/options/contracts (no greeks needed). Cached 1h.
    Powers the expiration dropdown so GEX can target one expiry like OptionFlow."""
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return []
    now = time.time()
    hit = _EXP_CACHE.get(ticker)
    if hit and (now - hit[0]) < _EXP_TTL:
        return hit[1]
    today = today or date.today()
    key = _key()
    exps: set = set()
    url = f"{_BASE}/v3/reference/options/contracts"
    params = {"underlying_ticker": ticker, "limit": 1000, "apiKey": key}
    pages = 0
    try:
        while url and pages < 8:
            r = requests.get(url, params=params, timeout=_TIMEOUT)
            if r.status_code != 200:
                break
            body = r.json()
            for c in body.get("results", []):
                e = c.get("expiration_date")
                if e:
                    exps.add(e)
            url = body.get("next_url")
            params = {"apiKey": key} if url else params
            pages += 1
        out = [{"date": e, "dte": _dte(e, today)} for e in sorted(exps)]
        out = [x for x in out if x["dte"] >= 0]
        _EXP_CACHE[ticker] = (now, out)
        return out
    except Exception as exc:
        log.warning("list_expirations %s failed: %s", ticker, exc)
        return []


def fetch_chain(ticker: str, max_dte: int = 60, today: Optional[date] = None,
                expiration: Optional[str] = None) -> list[dict]:
    """Options-chain snapshot for `ticker`. If `expiration` (YYYY-MM-DD) is given,
    only that expiry (efficient, one snapshot filter); else all contracts with DTE
    ≤ max_dte. Returns [{strike,type,expiration,dte,oi,gamma,delta,iv,volume,close}].
    Empty on error / no options / no plan. Cached 10 min per (ticker, max_dte, exp).
    """
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return []
    ck = (ticker, max_dte, expiration)
    now = time.time()
    hit = _CACHE.get(ck)
    if hit and (now - hit[0]) < _CACHE_TTL:
        return hit[1]

    today = today or date.today()
    key = _key()
    out: list[dict] = []
    url = f"{_BASE}/v3/snapshot/options/{ticker}"
    params = {"limit": 250, "apiKey": key}
    if expiration:
        params["expiration_date"] = expiration
    pages = 0
    try:
        while url and pages < _MAX_PAGES:
            r = requests.get(url, params=params, timeout=_TIMEOUT)
            if r.status_code != 200:
                if r.status_code in (401, 403):
                    log.warning("options snapshot %s → %d (plan?)", ticker, r.status_code)
                break
            body = r.json()
            for c in body.get("results", []):
                d = c.get("details", {}) or {}
                exp = d.get("expiration_date", "")
                dte = _dte(exp, today)
                # a specific expiration is already snapshot-filtered; otherwise cap by DTE
                if not expiration and dte > max_dte:
                    continue
                g = c.get("greeks") or {}
                oi = c.get("open_interest")
                if oi is None:
                    continue
                out.append({
                    "strike":     d.get("strike_price"),
                    "type":       d.get("contract_type"),   # 'call' | 'put'
                    "expiration": exp,
                    "dte":        dte,
                    "oi":         int(oi),
                    "gamma":      g.get("gamma"),
                    "delta":      g.get("delta"),
                    "iv":         c.get("implied_volatility"),
                    "volume":     (c.get("day") or {}).get("volume"),
                    "close":      (c.get("day") or {}).get("close"),
                })
            url = body.get("next_url")
            params = {"apiKey": key} if url else params   # next_url carries the cursor
            pages += 1
        _CACHE[ck] = (now, out)
        return out
    except Exception as exc:
        log.warning("fetch_chain %s failed: %s", ticker, exc)
        return []


def spot_price(ticker: str) -> Optional[float]:
    """Current underlying price via the stock snapshot we already use (data_massive)."""
    try:
        from data_massive import get_snapshot
        snap = get_snapshot(ticker)
        if snap:
            return snap.get("price") or snap.get("close") or snap.get("prev_close")
    except Exception:
        pass
    # fallback: last daily close from the analytics DB
    try:
        import duckdb
        db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "studio_analytics.duckdb")
        c = duckdb.connect(db, read_only=True)
        r = c.execute("SELECT close FROM bars WHERE ticker=? ORDER BY date DESC LIMIT 1",
                      [ticker.upper()]).fetchone()
        c.close()
        return float(r[0]) if r else None
    except Exception:
        return None
