"""
ultra_engine.py — 260308+L88 and 260315 ULTRA v2 signal engines.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from indicators import atr as _atr_hlc


def compute_260308_l88(
    df: pd.DataFrame,
    vol_mult: float = 2.0,
    delta_mult: float = 1.5,
) -> pd.DataFrame:
    c = df["close"]
    o = df["open"]
    v = df["volume"] if "volume" in df.columns else pd.Series(1.0, index=df.index)

    vol_prev   = v.shift(1).fillna(0.0)
    vol_higher = v > vol_prev
    vol_jump   = (vol_prev > 0) & (v >= vol_prev * vol_mult)
    bull_cand  = c > o

    prev_delta = (c - o).shift(1).abs()
    curr_delta = (c - o).abs()
    delta_ok   = curr_delta >= prev_delta * delta_mult

    sig_260308 = vol_higher & vol_jump & bull_cand & delta_ok

    try:
        from wlnbb_engine import compute_wlnbb
        wl   = compute_wlnbb(df)
        l34  = wl["L34"].astype(bool)
        l43  = wl["L43"].astype(bool)
        l_ctx = l34 | l43 | l34.shift(1).fillna(False) | l43.shift(1).fillna(False)
    except Exception:
        l_ctx = pd.Series(False, index=df.index)

    sig_l88 = sig_260308 & l_ctx

    return pd.DataFrame({
        "sig_260308": sig_260308.astype(bool),
        "sig_l88":    sig_l88.astype(bool),
    }, index=df.index)


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    return _atr_hlc(df["high"], df["low"], df["close"], n)


def compute_ultra_v2(
    df: pd.DataFrame,
    eb_body_mult: float = 1.5,
    eb_wick_ratio: float = 0.25,
    eb_lookback: int = 5,
    fbo_lookback: int = 10,
    vol_len: int = 20,
    hi_vol_mult: float = 1.6,
    lo_vol_mult: float = 0.80,
    atr_len: int = 14,
    narrow_mult: float = 1.0,
    clv_high: float = 0.70,
    clv_low: float = 0.30,
    vsa_test_n: int = 3,
) -> pd.DataFrame:
    c, o, h, l = df["close"], df["open"], df["high"], df["low"]
    v = df["volume"] if "volume" in df.columns else pd.Series(1.0, index=df.index)

    curr_body  = (c - o).abs()
    avg_body   = (c - o).abs().shift(1).rolling(eb_lookback, min_periods=1).mean()
    curr_top   = c.where(c >= o, o)
    curr_bot   = c.where(c <= o, o)
    upper_wick = h - curr_top
    lower_wick = curr_bot - l
    total_wick = upper_wick + lower_wick
    big_body   = curr_body >= avg_body * eb_body_mult
    little_tail = total_wick <= curr_body.replace(0, np.nan) * eb_wick_ratio
    little_tail = little_tail.fillna(False)
    eb_raw  = big_body & little_tail
    eb_bull = eb_raw & (c > o)
    eb_bear = eb_raw & (c < o)

    n_bar_high = h.shift(1).rolling(fbo_lookback, min_periods=1).max()
    n_bar_low  = l.shift(1).rolling(fbo_lookback, min_periods=1).min()
    fbo_bear = (h > n_bar_high) & (c < n_bar_high) & (c < o)
    fbo_bull = (l < n_bar_low)  & (c > n_bar_low)  & (c > o)

    bf_buy  = (c > h.shift(1)) & (c > h.shift(3))
    bf_sell = (c < l.shift(1)) & (c < l.shift(3))

    inside_bar = (h <= h.shift(1)) & (l >= l.shift(1))

    vol_ma = v.rolling(vol_len, min_periods=1).mean()
    hi_vol = v >= vol_ma * hi_vol_mult
    lo_vol = v <= vol_ma * lo_vol_mult
    spread = h - l
    atrv   = _atr(df, atr_len)
    narrow = (spread > 0) & (spread < atrv * narrow_mult)
    clv    = (c - l) / spread.replace(0, np.nan)

    up_bar = c > c.shift(1)
    dn_bar = c < c.shift(1)

    sq = hi_vol & narrow
    ns = lo_vol & (narrow | inside_bar) & dn_bar & (clv >= clv_high)
    nd = lo_vol & (narrow | inside_bar) & up_bar  & (clv <= clv_low)

    effort_recent = sq.shift(1).fillna(False) | sq.shift(2).fillna(False)
    lo_quiet      = lo_vol & (narrow | inside_bar)
    test_recent   = (ns | nd | lo_quiet).rolling(vsa_test_n, min_periods=1).max().astype(bool)
    h2_roll = h.shift(1).rolling(2, min_periods=1).max()
    l2_roll = l.shift(1).rolling(2, min_periods=1).min()
    confirm_up = (c > h2_roll) & (clv >= 0.55)
    confirm_dn = (c < l2_roll) & (clv <= 0.45)
    sig3_up = effort_recent & test_recent & confirm_up
    sig3_dn = effort_recent & test_recent & confirm_dn

    best_long  = fbo_bull & bf_buy
    best_short = fbo_bear & bf_sell

    return pd.DataFrame({
        "eb_bull":    eb_bull.astype(bool),
        "eb_bear":    eb_bear.astype(bool),
        "fbo_bull":   fbo_bull.astype(bool),
        "fbo_bear":   fbo_bear.astype(bool),
        "bf_buy":     bf_buy.astype(bool),
        "bf_sell":    bf_sell.astype(bool),
        "ultra_sq":   sq.astype(bool),
        "ultra_ns":   ns.astype(bool),
        "ultra_nd":   nd.astype(bool),
        "ultra_3up":  sig3_up.astype(bool),
        "ultra_3dn":  sig3_dn.astype(bool),
        "best_long":  best_long.astype(bool),
        "best_short": best_short.astype(bool),
    }, index=df.index)


# ─────────────────────────────────────────────────────────────────────────────
# ULTRA Screener (read-only signal aggregation)
#
# This section is independent from `compute_260308_l88` / `compute_ultra_v2`.
# It does NOT define any new score, category, or context flag.
# It simply joins existing read-only results from:
#   - Turbo  (turbo_engine.get_turbo_results)
#   - TZ/WLNBB stock_stat CSV
#   - TZ Intelligence (tz_intelligence.scanner.run_intelligence_scan)
#   - Pullback Miner (analyzers.pullback_miner.miner.run_pullback_scan)
#   - Rare Reversal (analyzers.rare_reversal.miner.run_rare_reversal_scan)
# Any source that fails to load is reported as a warning; the response is
# returned with whatever sources succeeded.
# ─────────────────────────────────────────────────────────────────────────────

import csv as _csv
import os as _os
import time as _time

# Turbo signal keys to surface as a flat "signals" list (no scoring).
# Restricted to known boolean / categorical TZ-WLNBB / VABS / Combo / B / F flags.
_TURBO_SIGNAL_KEYS: tuple = (
    # VABS
    "best_sig", "strong_sig", "vbo_up", "vbo_dn",
    "abs_sig", "climb_sig", "load_sig",
    # Wyckoff
    "ns", "nd", "sc", "bc", "sq",
    # Combo / 2809
    "buy_2809", "rocket", "sig3g", "rtv",
    "hilo_buy", "hilo_sell", "atr_brk", "bb_brk",
    "bias_up", "bias_down", "cons_atr",
    "um_2809", "svs_2809", "conso_2809",
    # B (260321)
    "b1", "b2", "b3", "b4", "b5",
    "b6", "b7", "b8", "b9", "b10", "b11",
    # F (260418)
    "f1", "f2", "f3", "f4", "f5", "f6",
    "f7", "f8", "f9", "f10", "f11", "any_f",
    # G (260410)
    "g1", "g2", "g4", "g6", "g11",
    # seq_bcont, va
    "seq_bcont", "va",
    # Volume spike
    "vol_spike_5x", "vol_spike_10x", "vol_spike_20x",
    # TZ confluences
    "ca", "cd", "cw", "tz_bull_flip", "tz_attempt",
    "tz_weak_bull", "tz_weak_bear",
    # T/Z
    "tz_sig", "tz_bull",
    # WLNBB
    "fri34", "fri43", "fri64",
    "l34", "l43", "l64", "l22", "l555", "only_l2l4",
    "blue", "cci_ready", "cci_0_retest", "cci_blue_turn",
    "bo_up", "bo_dn", "bx_up", "bx_dn",
    "be_up", "be_dn",
    "fuchsia_rh", "fuchsia_rl", "pre_pump",
    # Wick
    "wick_bull", "wick_bear",
    "x2g_wick", "x2_wick", "x1g_wick", "x1_wick", "x3_wick",
    # 260308 / ULTRA v2
    "sig_260308", "sig_l88",
    "eb_bull", "eb_bear", "fbo_bull", "fbo_bear",
    "bf_buy", "bf_sell",
    "ultra_3up", "ultra_3dn",
    "best_long", "best_short",
    # PARA / FLY
    "para_prep", "para_start", "para_plus", "para_retest",
    "fly_abcd", "fly_cd", "fly_bd", "fly_ad",
    # PREUP / PREDN
    "preup66", "preup55", "preup89", "preup3", "preup2", "preup50",
    "predn66", "predn55", "predn89", "predn3", "predn2", "predn50",
    # RGTI / SMX / RS
    "rgti_ll", "rgti_up", "rgti_upup", "rgti_upupup",
    "rgti_orange", "rgti_green", "rgti_greencirc", "smx",
    "rs_strong",
)


def _ultra_tz_batch_stat_path(universe: str, tf: str, nasdaq_batch: str = "") -> str:
    """Mirror of main._tz_batch_stat_path — kept local to avoid circular import."""
    if nasdaq_batch and nasdaq_batch != "all":
        if universe == "nasdaq":
            return f"stock_stat_tz_wlnbb_nasdaq_{nasdaq_batch}_{tf}.csv"
        if universe == "nasdaq_gt5":
            return f"stock_stat_tz_wlnbb_nasdaq_gt5_{nasdaq_batch}_{tf}.csv"
    return f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"


def _resolve_tz_wlnbb_csv(universe: str, tf: str, nasdaq_batch: str = "") -> str | None:
    """Pick the first existing stock_stat CSV path for this universe/tf/batch."""
    candidates = [
        _ultra_tz_batch_stat_path(universe, tf, nasdaq_batch),
        f"stock_stat_tz_wlnbb_{universe}_{tf}.csv",
        f"stock_stat_tz_wlnbb_{tf}.csv",
    ]
    for p in candidates:
        if _os.path.exists(p):
            return p
    return None


def _extract_turbo_signals(row: dict) -> list:
    """Return a flat list of truthy turbo signal keys for the row."""
    sigs: list = []
    for k in _TURBO_SIGNAL_KEYS:
        v = row.get(k)
        if v is None:
            continue
        try:
            if isinstance(v, bool):
                if v:
                    sigs.append(k)
            elif isinstance(v, (int, float)):
                if v:
                    sigs.append(k)
            elif isinstance(v, str):
                if v not in ("", "0", "0.0", "False", "false", "None", "null"):
                    sigs.append(k)
        except Exception:
            pass
    return sigs


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _load_turbo_block(
    universe: str, tf: str, direction: str, limit: int,
    min_score: float, min_price: float, max_price: float, min_volume: float,
) -> tuple[dict, list, str | None]:
    """Return (rows_by_ticker, ordered_tickers, last_scan_time)."""
    from turbo_engine import get_turbo_results, get_last_turbo_scan_time
    rows = get_turbo_results(
        limit=limit, min_score=min_score, direction=direction,
        tf=tf, universe=universe,
        price_min=min_price, price_max=max_price,
        vol_min=min_volume,
    )
    last_time = get_last_turbo_scan_time(tf=tf, universe=universe)
    by_ticker: dict = {}
    order: list = []
    for r in rows:
        t = r.get("ticker")
        if not t:
            continue
        if t not in by_ticker:
            by_ticker[t] = r
            order.append(t)
    return by_ticker, order, last_time


def _load_tz_wlnbb_latest(
    universe: str, tf: str, nasdaq_batch: str = "",
) -> dict:
    """Latest TZ/WLNBB row per ticker keyed by ticker. Returns {} on failure."""
    path = _resolve_tz_wlnbb_csv(universe, tf, nasdaq_batch)
    if not path:
        raise FileNotFoundError(
            f"stock_stat_tz_wlnbb CSV not found for universe={universe} tf={tf}"
        )
    rows_by_ticker: dict = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            t = row.get("ticker", "")
            if not t:
                continue
            if row.get("universe", "") and row.get("universe", "") != universe:
                continue
            rows_by_ticker.setdefault(t, []).append(row)
    latest: dict = {}
    for t, rows in rows_by_ticker.items():
        rows.sort(key=lambda r: r.get("bar_datetime") or r.get("date", ""))
        latest[t] = rows[-1]
    return latest


def _project_tz_wlnbb(row: dict) -> dict:
    return {
        "t_signal":      row.get("t_signal", "") or "",
        "z_signal":      row.get("z_signal", "") or "",
        "l_signal":      row.get("l_signal", "") or "",
        "preup_signal":  row.get("preup_signal", "") or "",
        "predn_signal":  row.get("predn_signal", "") or "",
        "lane1_label":   row.get("lane1_label", "") or "",
        "lane3_label":   row.get("lane3_label", "") or "",
        "volume_bucket": row.get("volume_bucket", "") or "",
        "wick_suffix":   row.get("wick_suffix", "") or "",
    }


def _project_tz_intel(row: dict) -> dict:
    return {
        "role":                 row.get("role", "") or "",
        "score":                row.get("score"),
        "quality":              row.get("quality", "") or "",
        "action":               row.get("action", "") or "",
        "abr_category":         row.get("abr_category", "") or "",
        "abr_med10d_pct":       row.get("abr_med10d_pct"),
        "abr_fail10d_pct":      row.get("abr_fail10d_pct"),
        "matched_status":       row.get("matched_status", "") or "",
        "matched_med10d_pct":   row.get("matched_med10d_pct"),
        "matched_fail10d_pct":  row.get("matched_fail10d_pct"),
    }


def _project_pullback(row: dict) -> dict:
    return {
        "evidence_tier":       row.get("evidence_tier", "") or "",
        "pullback_stage":      row.get("pullback_stage", "") or "",
        "pattern_key":         row.get("pattern_key", "") or "",
        "score":               row.get("score"),
        "median_10d_return":   row.get("median_10d_return"),
        "win_rate_10d":        row.get("win_rate_10d"),
        "fail_rate_10d":       row.get("fail_rate_10d"),
        "is_currently_active": bool(row.get("is_currently_active")),
    }


def _project_rare(row: dict) -> dict:
    return {
        "evidence_tier":       row.get("evidence_tier", "") or "",
        "base4_key":           row.get("base4_key", "") or "",
        "extended5_key":       row.get("extended5_key") or "",
        "extended6_key":       row.get("extended6_key") or "",
        "pattern_length":      row.get("pattern_length"),
        "score":               row.get("score"),
        "median_10d_return":   row.get("median_10d_return"),
        "fail_rate_10d":       row.get("fail_rate_10d"),
        "is_currently_active": bool(row.get("is_currently_active")),
        "current_pattern_completion": row.get("current_pattern_completion"),
    }


def _best_pattern_per_ticker(rows: list) -> dict:
    """Pick the highest-score pattern per ticker."""
    by_ticker: dict = {}
    for r in rows or []:
        t = r.get("ticker")
        if not t:
            continue
        prev = by_ticker.get(t)
        if prev is None:
            by_ticker[t] = r
            continue
        try:
            if _safe_float(r.get("score"), -1e9) > _safe_float(prev.get("score"), -1e9):
                by_ticker[t] = r
        except Exception:
            pass
    return by_ticker


def run_ultra_scan(
    universe: str = "sp500",
    tf: str = "1d",
    direction: str = "bull",
    limit: int = 500,
    min_score: float = 0,
    min_price: float = 0,
    max_price: float = 1e9,
    min_volume: float = 0,
    scan_mode: str = "latest",
    role_filter: str = "all",
    nasdaq_batch: str = "",
) -> dict:
    """ULTRA aggregator — joins existing read-only signal sources by ticker.

    Does NOT compute any new score, category, or context. If a source CSV is
    missing or its loader raises, the source is recorded as failed in `meta`
    and a human-readable string is appended to `warnings`; the rest of the
    response still returns.
    """
    started = _time.time()
    warnings: list = []
    sources: dict = {
        "turbo":           {"ok": False, "count": 0},
        "tz_wlnbb":        {"ok": False, "count": 0},
        "tz_intelligence": {"ok": False, "count": 0},
        "pullback":        {"ok": False, "count": 0},
        "rare_reversal":   {"ok": False, "count": 0},
    }

    # ── Turbo (base) ──────────────────────────────────────────────────────────
    turbo_by_ticker: dict = {}
    ordered: list = []
    last_scan: str | None = None
    try:
        turbo_by_ticker, ordered, last_scan = _load_turbo_block(
            universe=universe, tf=tf, direction=direction, limit=limit,
            min_score=min_score, min_price=min_price, max_price=max_price,
            min_volume=min_volume,
        )
        sources["turbo"] = {"ok": True, "count": len(turbo_by_ticker)}
    except Exception as exc:
        warnings.append(f"Turbo unavailable: {exc}")

    # ── TZ/WLNBB ──────────────────────────────────────────────────────────────
    tz_wlnbb_by_ticker: dict = {}
    try:
        tz_wlnbb_by_ticker = _load_tz_wlnbb_latest(universe, tf, nasdaq_batch)
        sources["tz_wlnbb"] = {"ok": True, "count": len(tz_wlnbb_by_ticker)}
    except Exception as exc:
        warnings.append(f"TZ/WLNBB unavailable: {exc}")

    # ── TZ Intelligence ───────────────────────────────────────────────────────
    tz_intel_by_ticker: dict = {}
    try:
        from tz_intelligence.scanner import run_intelligence_scan
        intel_resp = run_intelligence_scan(
            universe=universe, tf=tf, nasdaq_batch=nasdaq_batch,
            min_price=min_price, max_price=max_price, min_volume=min_volume,
            role_filter=role_filter, scan_mode=scan_mode,
            limit=max(limit, 500),
        )
        if isinstance(intel_resp, dict) and intel_resp.get("error"):
            warnings.append(f"TZ Intelligence unavailable: {intel_resp['error']}")
        else:
            for r in (intel_resp or {}).get("results", []) or []:
                t = r.get("ticker")
                if t and t not in tz_intel_by_ticker:
                    tz_intel_by_ticker[t] = r
            sources["tz_intelligence"] = {"ok": True, "count": len(tz_intel_by_ticker)}
    except Exception as exc:
        warnings.append(f"TZ Intelligence unavailable: {exc}")

    # ── Pullback Miner ────────────────────────────────────────────────────────
    pullback_by_ticker: dict = {}
    try:
        from analyzers.pullback_miner.miner import run_pullback_scan
        pb_resp = run_pullback_scan(
            universe=universe, tf=tf,
            min_price=min_price, max_price=max_price,
            limit=max(limit, 500),
        )
        if isinstance(pb_resp, dict) and pb_resp.get("error"):
            warnings.append(f"Pullback Miner unavailable: {pb_resp['error']}")
        else:
            pullback_by_ticker = _best_pattern_per_ticker(
                (pb_resp or {}).get("results", []) or []
            )
            sources["pullback"] = {"ok": True, "count": len(pullback_by_ticker)}
    except Exception as exc:
        warnings.append(f"Pullback Miner unavailable: {exc}")

    # ── Rare Reversal ─────────────────────────────────────────────────────────
    rare_by_ticker: dict = {}
    try:
        from analyzers.rare_reversal.miner import run_rare_reversal_scan
        rr_resp = run_rare_reversal_scan(
            universe=universe, tf=tf,
            min_price=min_price, max_price=max_price,
            limit=max(limit, 500),
        )
        if isinstance(rr_resp, dict) and rr_resp.get("error"):
            warnings.append(f"Rare Reversal unavailable: {rr_resp['error']}")
        else:
            rare_by_ticker = _best_pattern_per_ticker(
                (rr_resp or {}).get("results", []) or []
            )
            sources["rare_reversal"] = {"ok": True, "count": len(rare_by_ticker)}
    except Exception as exc:
        warnings.append(f"Rare Reversal unavailable: {exc}")

    # ── If Turbo failed, fall back to whatever ticker set we have ────────────
    if not ordered:
        seen: set = set()
        for src in (tz_wlnbb_by_ticker, tz_intel_by_ticker,
                    pullback_by_ticker, rare_by_ticker):
            for t in src.keys():
                if t not in seen:
                    ordered.append(t)
                    seen.add(t)

    # ── Merge by ticker ──────────────────────────────────────────────────────
    results: list = []
    for ticker in ordered:
        turbo_row = turbo_by_ticker.get(ticker)
        tzw_row   = tz_wlnbb_by_ticker.get(ticker)
        intel_row = tz_intel_by_ticker.get(ticker)
        pb_row    = pullback_by_ticker.get(ticker)
        rr_row    = rare_by_ticker.get(ticker)

        # Source-of-truth for price/volume: turbo if available, then tz_intel,
        # then tz_wlnbb CSV row.
        price = 0.0
        volume = 0.0
        if turbo_row:
            price = _safe_float(turbo_row.get("last_price"))
            volume = _safe_float(turbo_row.get("avg_vol"))
        elif intel_row:
            price = _safe_float(intel_row.get("close"))
            volume = _safe_float(intel_row.get("volume"))
        elif tzw_row:
            price = _safe_float(tzw_row.get("close"))
            volume = _safe_float(tzw_row.get("volume"))

        out_row: dict = {
            "ticker": ticker,
            "price":  price,
            "volume": volume,
            "turbo":         None,
            "tz_wlnbb":      None,
            "tz_intel":      None,
            "pullback":      None,
            "rare_reversal": None,
            "source_flags": {
                "has_turbo":         turbo_row is not None,
                "has_tz_wlnbb":      tzw_row   is not None,
                "has_tz_intel":      intel_row is not None,
                "has_pullback":      pb_row    is not None,
                "has_rare_reversal": rr_row    is not None,
            },
        }

        if turbo_row is not None:
            out_row["turbo"] = {
                "score":     _safe_float(turbo_row.get("turbo_score")),
                "direction": "bull" if turbo_row.get("tz_bull") else "bear",
                "signals":   _extract_turbo_signals(turbo_row),
                "raw":       turbo_row,
            }
        if tzw_row is not None:
            out_row["tz_wlnbb"] = _project_tz_wlnbb(tzw_row)
        if intel_row is not None:
            out_row["tz_intel"] = _project_tz_intel(intel_row)
        if pb_row is not None:
            out_row["pullback"] = _project_pullback(pb_row)
        if rr_row is not None:
            out_row["rare_reversal"] = _project_rare(rr_row)

        results.append(out_row)

    # Apply limit at the merged-row level
    if limit and limit > 0:
        results = results[:limit]

    return {
        "results":   results,
        "total":     len(results),
        "last_scan": last_scan,
        "meta": {
            "universe":    universe,
            "tf":          tf,
            "direction":   direction,
            "scan_mode":   scan_mode,
            "role_filter": role_filter,
            "nasdaq_batch": nasdaq_batch or None,
            "elapsed_ms":  int((_time.time() - started) * 1000),
            "sources":     sources,
        },
        "warnings": warnings,
    }