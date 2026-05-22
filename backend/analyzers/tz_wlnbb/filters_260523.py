"""Pure-Python 260523 enrichment + filter helpers, importable without fastapi.

Used by `backend/main.py` (turbo-scan / ultra-scan / superchart) and by tests.
"""
from __future__ import annotations
import csv as _csv
import os
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


# In-memory cache: (universe, tf, nasdaq_batch) → (mtime, {ticker: row})
_STOCK_STAT_260523_CACHE: Dict[tuple, tuple] = {}


def load_260523_stock_stat_index(
    universe: str, tf: str, nasdaq_batch: str = "",
    path_resolver=None,
) -> Dict[str, Dict[str, Any]]:
    """Load latest stock_stat CSV and return {ticker → last_row}.

    `path_resolver(universe, tf, nasdaq_batch) -> str` lets callers (main.py)
    inject their canonical path helper. If None or returns missing path,
    falls back to a couple of common naming conventions.
    """
    candidates = []
    if path_resolver is not None:
        try:
            candidates.append(path_resolver(universe, tf, nasdaq_batch))
        except Exception:
            pass
    candidates.append(f"stock_stat_tz_wlnbb_{universe}_{tf}.csv")
    candidates.append(f"stock_stat_tz_wlnbb_{tf}.csv")

    path = None
    for c in candidates:
        if c and os.path.exists(c):
            path = c
            break
    if path is None:
        return {}

    try:
        mtime = os.path.getmtime(path)
        cache_key = (universe, tf, nasdaq_batch)
        cached = _STOCK_STAT_260523_CACHE.get(cache_key)
        if cached and cached[0] == mtime:
            return cached[1]
        per_ticker: Dict[str, Dict[str, Any]] = {}
        with open(path, newline="", encoding="utf-8") as fh:
            for row in _csv.DictReader(fh):
                tk = row.get("ticker", "")
                if not tk:
                    continue
                per_ticker[tk] = row  # last row wins (oldest-first ordering)
        _STOCK_STAT_260523_CACHE[cache_key] = (mtime, per_ticker)
        return per_ticker
    except Exception as e:
        log.warning("260523 stock_stat load failed: %s", e)
        return {}


def _to_bool(v: Any) -> bool:
    return str(v).strip() in ("1", "True", "true")


def enrich_with_260523(
    results: List[Dict[str, Any]],
    universe: str, tf: str, nasdaq_batch: str = "",
    path_resolver=None,
) -> List[Dict[str, Any]]:
    """Add ad_fresh, ad_cluster, wyc_phase + wyc_* booleans to every row.
    Existing non-empty values are preserved (additive only)."""
    idx = load_260523_stock_stat_index(universe, tf, nasdaq_batch, path_resolver)
    for r in results:
        tk = r.get("ticker", "")
        ss = idx.get(tk, {}) if idx else {}
        if not r.get("ad_fresh") and r.get("ad_fresh") not in (False,):
            r["ad_fresh"] = _to_bool(ss.get("ad_fresh", ""))
        else:
            r["ad_fresh"] = bool(r.get("ad_fresh")) if "ad_fresh" in r else _to_bool(ss.get("ad_fresh", ""))
        if "ad_cluster" not in r or r.get("ad_cluster") in (None, ""):
            r["ad_cluster"] = _to_bool(ss.get("ad_cluster", ""))
        if "wyc_phase" not in r or r.get("wyc_phase") in (None, ""):
            r["wyc_phase"] = ss.get("wyc_phase", "") or "NEUTRAL"
        for k in ("wyc_spring", "wyc_sos", "wyc_acc_tr", "wyc_markup"):
            if k not in r or r.get(k) in (None, ""):
                r[k] = _to_bool(ss.get(k, ""))
    return results


def apply_260523_filters(
    results: List[Dict[str, Any]],
    ad_fresh: Optional[bool] = None,
    ad_cluster: Optional[bool] = None,
    wyc_phase: Optional[str] = None,
    wyc_spring: Optional[bool] = None,
    wyc_sos: Optional[bool] = None,
    wyc_acc_tr: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Apply optional filters. None = no constraint."""
    if ad_fresh is not None:
        results = [r for r in results if bool(r.get("ad_fresh")) == ad_fresh]
    if ad_cluster is not None:
        results = [r for r in results if bool(r.get("ad_cluster")) == ad_cluster]
    if wyc_phase is not None and wyc_phase != "":
        results = [r for r in results if r.get("wyc_phase") == wyc_phase]
    if wyc_spring is not None:
        results = [r for r in results if bool(r.get("wyc_spring")) == wyc_spring]
    if wyc_sos is not None:
        results = [r for r in results if bool(r.get("wyc_sos")) == wyc_sos]
    if wyc_acc_tr is not None:
        results = [r for r in results if bool(r.get("wyc_acc_tr")) == wyc_acc_tr]
    return results


def parse_line5_tokens(line5: str) -> Dict[str, Any]:
    """Parse bar_line5 (e.g. "VX-PB-R2X") into convenience flags."""
    if not line5:
        return {"wvf_spike": False, "vix_range": False,
                "psar_bull": False, "rsi2_token": ""}
    tokens = line5.split("-")
    rsi2 = next((t for t in tokens if t.startswith("R2")), "")
    return {
        "wvf_spike":  "VX" in tokens,
        "vix_range":  "VR" in tokens,
        "psar_bull":  "PB" in tokens,
        "rsi2_token": rsi2,
    }
