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
        # 260523 v3.2: swing classification (backward LIVE-SAFE + forward RESEARCH_ONLY)
        if "swing_type" not in r or r.get("swing_type") in (None,):
            r["swing_type"] = ss.get("swing_type", "") or ""
        for k in ("is_pivot_high", "is_pivot_low"):
            if k not in r or r.get(k) in (None, ""):
                r[k] = _to_bool(ss.get(k, ""))
        # swing_ret_from_prev = backward (live-safe)
        # fwd_swing_ret / fwd_swing_bars = forward (RESEARCH_ONLY, lookahead)
        for k in ("swing_ret_from_prev", "fwd_swing_ret", "fwd_swing_bars"):
            if k not in r or r.get(k) in (None, ""):
                v = ss.get(k, "")
                try:
                    r[k] = float(v) if v not in ("", None) else None
                except (TypeError, ValueError):
                    r[k] = None
        # 260523 v3.5 PREBREAK + WYC additional booleans
        for k in ("prebreak_prime", "prebreak_ready", "prebreak_watch",
                  "pb_lvbo", "pb_stop_cause", "pb_pp_rtv", "pb_fly_cd_c",
                  "pb_wvf_confirm", "pb_follow_confirm", "pb_macro_penalty",
                  "wyc_in_tr", "wyc_sow"):
            if k not in r or r.get(k) in (None, ""):
                r[k] = _to_bool(ss.get(k, ""))
    return results


_PREBREAK_BOOL_FILTER_KEYS = (
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "pb_lvbo", "pb_stop_cause", "pb_wvf_confirm", "pb_macro_penalty",
    "wyc_in_tr", "wyc_sow",
)


def _column_unpopulated(results: List[Dict[str, Any]], col: str) -> bool:
    """Return True if a column is missing/falsy across ALL result rows.
    Used to detect when a stock_stat CSV lacks a new 260523 column so we
    can warn the caller rather than silently returning 0 rows."""
    if not results:
        return False
    for r in results:
        v = r.get(col)
        if v not in (None, "", False, 0):
            return False
    return True


def apply_260523_filters(
    results: List[Dict[str, Any]],
    ad_fresh: Optional[bool] = None,
    ad_cluster: Optional[bool] = None,
    wyc_phase: Optional[str] = None,
    wyc_spring: Optional[bool] = None,
    wyc_sos: Optional[bool] = None,
    wyc_acc_tr: Optional[bool] = None,
    swing_type: Optional[str] = None,
    # 260523 v3.5 — PREBREAK + WYC additional bool filters
    prebreak_prime: Optional[bool] = None,
    prebreak_ready: Optional[bool] = None,
    prebreak_watch: Optional[bool] = None,
    pb_lvbo: Optional[bool] = None,
    pb_stop_cause: Optional[bool] = None,
    pb_wvf_confirm: Optional[bool] = None,
    pb_macro_penalty: Optional[bool] = None,
    wyc_in_tr: Optional[bool] = None,
    wyc_sow: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Apply optional filters. None = no constraint.
    swing_type accepts: "HH" | "LH" | "HL" | "LL" | "pivot" (any non-empty)."""
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
    if swing_type is not None and swing_type != "":
        if swing_type == "pivot":
            results = [r for r in results if (r.get("swing_type") or "") != ""]
        else:
            results = [r for r in results if (r.get("swing_type") or "") == swing_type]

    # 260523 v3.5 — PREBREAK + WYC additional bool filters
    _bool_params = {
        "prebreak_prime":   prebreak_prime,
        "prebreak_ready":   prebreak_ready,
        "prebreak_watch":   prebreak_watch,
        "pb_lvbo":          pb_lvbo,
        "pb_stop_cause":    pb_stop_cause,
        "pb_wvf_confirm":   pb_wvf_confirm,
        "pb_macro_penalty": pb_macro_penalty,
        "wyc_in_tr":        wyc_in_tr,
        "wyc_sow":          wyc_sow,
    }
    for col, want in _bool_params.items():
        if want is not None:
            results = [r for r in results if bool(r.get(col, False)) == want]
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
