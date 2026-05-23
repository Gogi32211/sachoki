"""Pure-Python 260523 enrichment + filter helpers, importable without fastapi.

Used by `backend/main.py` (turbo-scan / ultra-scan / superchart) and by tests.
"""
from __future__ import annotations
import csv as _csv
import glob as _glob
import os
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


# In-memory cache: (universe, tf, nasdaq_batch) → (mtime, {ticker: row})
_STOCK_STAT_260523_CACHE: Dict[tuple, tuple] = {}


def _resolve_stat_path(universe: str, tf: str, nasdaq_batch: str,
                       path_resolver) -> Optional[str]:
    """Pick the best available stock_stat file for 260523 enrichment.

    Priority: canonical (when present) → ULTRA-private subsets (parquet wins
    over csv, most recently modified wins). ULTRA-private files are the only
    source on local dev where the canonical batch generator hasn't been run.
    """
    candidates: list[str] = []
    if path_resolver is not None:
        try:
            candidates.append(path_resolver(universe, tf, nasdaq_batch))
        except Exception:
            pass
    candidates.append(f"stock_stat_tz_wlnbb_{universe}_{tf}.csv")
    candidates.append(f"stock_stat_tz_wlnbb_{tf}.csv")

    # Canonical first
    for c in candidates:
        if c and os.path.exists(c):
            return c

    # Fall back to ULTRA-private subsets (parquet preferred, then csv).
    # Multiple subsets can coexist for different ticker hashes — pick newest.
    ultra_patterns = [
        f"stock_stat_tz_wlnbb_ultra_{universe}_{tf}_*.parquet",
        f"stock_stat_tz_wlnbb_ultra_{universe}_{tf}_*.csv",
    ]
    matches: list[tuple[float, str]] = []
    for pat in ultra_patterns:
        for p in _glob.glob(pat):
            try:
                matches.append((os.path.getmtime(p), p))
            except OSError:
                continue
    if not matches:
        return None
    # Newest first; parquet & csv tied on mtime → parquet wins by pattern order.
    matches.sort(key=lambda t: t[0], reverse=True)
    return matches[0][1]


def _iter_stock_stat_rows(path: str):
    """Yield dict-rows from a stock_stat file (parquet or CSV) with CSV-style
    string semantics so the rest of this module's downstream `_to_bool` /
    `row.get(...)` logic stays untouched."""
    if path.lower().endswith(".parquet"):
        # Lazy import: pandas is heavy and only needed for parquet path.
        import sys as _sys
        # stat_io lives in backend/; ensure the backend dir is importable.
        _bd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # backend/analyzers/tz_wlnbb → backend/
        _bd = os.path.dirname(_bd)
        if _bd not in _sys.path:
            _sys.path.insert(0, _bd)
        from stat_io import read_stat_as_df, df_to_string_rows
        df = read_stat_as_df(path)
        for row in df_to_string_rows(df):
            yield row
        return
    with open(path, newline="", encoding="utf-8") as fh:
        for row in _csv.DictReader(fh):
            yield row

# Event-style columns: "recent" semantics — True if ANY of last EVENT_LOOKBACK
# bars had the column set. (Pivots, AD-FRESH, LVBO are events that fire on a
# single bar; checking only the latest bar misses ~all matches.)
#
# We also expose `<col>_age` — bars since most recent True (0 = current bar).
# The frontend N selector (1..10) filters on `<col>_age < N`. EVENT_LOOKBACK
# is raised to 10 so age info is available across the whole N range.
EVENT_LOOKBACK = 10
# Sentinel: no fire within the lookback window
EVENT_AGE_UNSEEN = 99
EVENT_BOOL_COLS = (
    "ad_fresh", "ad_cluster",
    "pb_lvbo", "pb_wvf_confirm", "pb_pp_rtv", "pb_fly_cd_c",
    "pb_stop_cause", "is_pivot_high", "is_pivot_low",
    "wyc_spring", "wyc_sos",
    # PREBREAK tiers + WYC additional + macro penalty: bool flags that can flip
    # bar-to-bar. The N selector should filter "fired in last N bars" the same
    # way it does for ad_fresh / wyc_spring.
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "wyc_in_tr", "wyc_sow", "pb_macro_penalty",
)
EVENT_STR_COLS = ("swing_type",)


def load_260523_stock_stat_index(
    universe: str, tf: str, nasdaq_batch: str = "",
    path_resolver=None,
) -> Dict[str, Dict[str, Any]]:
    """Load latest stock_stat CSV and return {ticker → enriched_row}.

    enriched_row is the LATEST raw row, with event-style columns
    (ad_fresh, pb_lvbo, swing_type, etc.) replaced by their "recent"
    value: True if ANY of the last EVENT_LOOKBACK bars had the column set,
    or the most recent non-empty swing_type. This matches how a trader views
    "is this ticker showing AD-FRESH recently?" rather than "did it fire
    exactly on today's bar?".

    State-style columns (wyc_phase, prebreak_*) keep latest-row semantics.
    """
    path = _resolve_stat_path(universe, tf, nasdaq_batch, path_resolver)
    if path is None:
        return {}

    try:
        mtime = os.path.getmtime(path)
        cache_key = (universe, tf, nasdaq_batch, path)
        cached = _STOCK_STAT_260523_CACHE.get(cache_key)
        if cached and cached[0] == mtime:
            return cached[1]

        # Collect per-ticker tail buffer of the last EVENT_LOOKBACK rows so we
        # can compute "recent event" semantics for swing_type / pb_lvbo / etc.
        tail: Dict[str, list] = {}
        latest: Dict[str, Dict[str, Any]] = {}
        for row in _iter_stock_stat_rows(path):
            tk = row.get("ticker", "")
            if not tk:
                continue
            # Rows arrive oldest-first → keep a rolling tail
            buf = tail.setdefault(tk, [])
            buf.append(row)
            if len(buf) > EVENT_LOOKBACK:
                buf.pop(0)
            latest[tk] = row

        # Build enriched per-ticker view.
        #
        # For every event-style column we expose two fields downstream:
        #   <col>      — True if the event fired in ANY of the last
        #                EVENT_LOOKBACK bars (back-compat with 5-bar code).
        #   <col>_age  — bars since most recent True (0 = current bar),
        #                or EVENT_AGE_UNSEEN if not seen in window.
        # The frontend N selector filters on `<col>_age < N`.
        per_ticker: Dict[str, Dict[str, Any]] = {}
        for tk, last_row in latest.items():
            enriched = dict(last_row)
            buf = tail.get(tk, [last_row])
            # buf is oldest→newest; reversed → newest first.
            buf_rev = list(reversed(buf))
            # Bool event cols
            for col in EVENT_BOOL_COLS:
                age = EVENT_AGE_UNSEEN
                for i, b in enumerate(buf_rev):
                    if _to_bool(b.get(col, "")):
                        age = i
                        break
                enriched[f"{col}_age"] = age
                if age != EVENT_AGE_UNSEEN:
                    enriched[col] = True
                # else keep the latest-row value (which is also False)
            # String event cols (swing_type): most recent non-empty + its age
            for col in EVENT_STR_COLS:
                latest_nonempty = ""
                age = EVENT_AGE_UNSEEN
                for i, b in enumerate(buf_rev):
                    v = (b.get(col) or "").strip()
                    if v:
                        latest_nonempty = v
                        age = i
                        break
                enriched[f"{col}_age"] = age
                if latest_nonempty:
                    enriched[col] = latest_nonempty
            per_ticker[tk] = enriched

        _STOCK_STAT_260523_CACHE[cache_key] = (mtime, per_ticker)
        return per_ticker
    except Exception as e:
        log.warning("260523 stock_stat load failed: %s", e)
        return {}


def _to_bool(v: Any) -> bool:
    return str(v).strip() in ("1", "True", "true")


_BOOL_COLS_FROM_INDEX = tuple(dict.fromkeys(
    EVENT_BOOL_COLS                       # all event-style + prebreak/wyc/macro
    + ("wyc_acc_tr", "wyc_markup",        # extra WYC phase booleans
       "pb_follow_confirm")               # follow-through (not yet age-tracked)
))
_FLOAT_COLS_FROM_INDEX = (
    "swing_ret_from_prev", "fwd_swing_ret", "fwd_swing_bars",
)


def enrich_with_260523(
    results: List[Dict[str, Any]],
    universe: str, tf: str, nasdaq_batch: str = "",
    path_resolver=None,
) -> List[Dict[str, Any]]:
    """Overlay 260523 columns onto every row from the stock_stat index.

    Index wins: the stock_stat file is the authoritative source for these
    columns; existing row values (numeric 0 placeholders from the turbo
    pipeline, etc.) are overwritten. If the index has no entry for the
    ticker, columns are reset to safe defaults.
    """
    idx = load_260523_stock_stat_index(universe, tf, nasdaq_batch, path_resolver)
    for r in results:
        tk = r.get("ticker", "")
        ss = idx.get(tk, {}) if idx else {}
        # Boolean columns (event + state)
        for k in _BOOL_COLS_FROM_INDEX:
            r[k] = _to_bool(ss.get(k, ""))
        # String columns
        r["wyc_phase"]   = ss.get("wyc_phase", "") or "NEUTRAL"
        r["swing_type"]  = ss.get("swing_type", "") or ""
        # Float columns
        for k in _FLOAT_COLS_FROM_INDEX:
            v = ss.get(k, "")
            try:
                r[k] = float(v) if v not in ("", None) else None
            except (TypeError, ValueError):
                r[k] = None
        # Per-event ages for the frontend N selector. UI filters as
        # `r[col] && r[col_age] < lookbackN`. Missing → EVENT_AGE_UNSEEN.
        for col in EVENT_BOOL_COLS + EVENT_STR_COLS:
            age_key = f"{col}_age"
            v = ss.get(age_key)
            try:
                r[age_key] = int(v) if v not in (None, "") else EVENT_AGE_UNSEEN
            except (TypeError, ValueError):
                r[age_key] = EVENT_AGE_UNSEEN
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
