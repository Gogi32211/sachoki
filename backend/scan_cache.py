"""
scan_cache.py — single-flight TTL memo for the DB-backed Edge/Ultra scans.

The scan endpoints hit the daily-updated Studio DB, which does NOT change intraday
(the nightly incremental_swap replaces it atomically). The Edge board fetches ~15
scans and React StrictMode/remount fires each 2-3× on load + again on every tab
switch — all recomputing the same 1-2s query. This collapses them:

  · TTL: within `ttl` seconds a key returns the memoized result instantly.
  · SINGLE-FLIGHT: concurrent callers for the SAME key serialize on a per-key lock,
    so the near-simultaneous page-load duplicates compute ONCE, not N times.
  · per-key locks → different scans never block each other.

Bust with `refresh=True` (skips the read, still stores the fresh result), or call
`invalidate()` after a DB swap.
"""
from __future__ import annotations
import time
import threading

_CACHE: dict[str, tuple[float, object]] = {}
_LOCKS: dict[str, threading.Lock] = {}
_GLOBAL = threading.Lock()
DEFAULT_TTL = 600.0   # 10 min — DB is static intraday; picks up the nightly swap within 10 min


def _key_lock(key: str) -> threading.Lock:
    with _GLOBAL:
        lk = _LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _LOCKS[key] = lk
        return lk


def cached(key: str, fn, ttl: float = DEFAULT_TTL, refresh: bool = False):
    """Return fn()'s result, memoized under `key` for `ttl` seconds (single-flight)."""
    if not refresh:
        hit = _CACHE.get(key)
        if hit is not None and (time.time() - hit[0]) < ttl:
            return hit[1]
    lk = _key_lock(key)
    with lk:
        if not refresh:
            hit = _CACHE.get(key)          # re-check: another thread may have filled it
            if hit is not None and (time.time() - hit[0]) < ttl:
                return hit[1]
        val = fn()
        _CACHE[key] = (time.time(), val)
        return val


def peek(key: str, ttl: float = DEFAULT_TTL):
    """Return the memoized value for `key` if it's fresh, else None — NEVER builds.
    For opportunistic enrichers that must not block on a cold, expensive scan (2026-07-16:
    the confluence/g3abs scans called the mtf-ema scan for their 📐 badge; when its TTL had
    expired that cold rebuild — 3 intraday DBs, can hit 'database is locked' after the nightly
    swap — blocked the whole Edge board for 45s+). Enrich only when the data is already there."""
    hit = _CACHE.get(key)
    if hit is not None and (time.time() - hit[0]) < ttl:
        return hit[1]
    return None


def invalidate(prefix: str | None = None) -> int:
    """Drop all entries (or those whose key starts with `prefix`). Returns count removed."""
    with _GLOBAL:
        keys = [k for k in _CACHE if (prefix is None or k.startswith(prefix))]
        for k in keys:
            _CACHE.pop(k, None)
        return len(keys)
