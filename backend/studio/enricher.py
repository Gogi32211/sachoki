"""
studio/enricher.py — Enrich existing Studio DB bars with derived columns.

Adds per-bar columns that aren't in the import CSV but are derivable from OHLC:
  • Line 2 suffixes: ne/wick/penetration/close + composite
  • Line 3 body+wick (e.g. "STB", "M")
  • Line 4 gap+range (e.g. "G1-C")
  • Line 5 VIX-Fix/PSAR/RSI2 (e.g. "PS-R2X") + parsed bool flags
  • Williams 3-3 pivots: swing_type, fwd_swing_ret, swing_ret_from_prev,
                          pct_to_next_hl/hh, bars_to_next_hl/hh
  • Williams 5-5 pivots: same as above but for major swings only
  • Individual L digit flags (sig_l1..sig_l6) from l_sig string
  • ATR(14) (used by gap/range computation, also useful downstream)

Idempotent: safe to re-run; just overwrites the enrichment columns.
"""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn, STUDIO_DB_PATH
from analyzers.tz_wlnbb.signal_extraction import compute_line5
from analyzers.tz_wlnbb.swing_classifier import classify_swings

log = logging.getLogger(__name__)

# Progress file (mirrors importer.py pattern)
PROGRESS_FILE = "/tmp/studio_enrich_progress.json"
ENRICH_VERSION = "v1.0"


# ─────────────────────────────────────────────────────────────────────────────
# Per-bar derived fields (vectorised pandas)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_suffixes(df: pd.DataFrame) -> pd.DataFrame:
    """Add ne_suffix, wick_suffix, penetration_suffix, close_suffix, full_suffix,
    composite_full_suffix.  All derived from OHLC of current vs prev bar.
    """
    o = df["open"].to_numpy()
    h = df["high"].to_numpy()
    l = df["low"].to_numpy()
    c = df["close"].to_numpy()
    n = len(df)

    if n == 0:
        for col in ["ne_suffix", "wick_suffix", "penetration_suffix",
                    "close_suffix", "full_suffix", "composite_full_suffix"]:
            df[col] = ""
        return df

    po = np.roll(o, 1); ph = np.roll(h, 1); pl = np.roll(l, 1); pc = np.roll(c, 1)
    # First bar has no prev — mask it later

    # NE suffix: E if close exceeds prev high/low, else N
    ne = np.where((c > ph) | (c < pl), "E", "N")

    # WICK suffix: U / D / B / ""
    wick_up = h > ph
    wick_dn = l < pl
    wick = np.where(wick_up & wick_dn, "B",
                    np.where(wick_up, "U",
                             np.where(wick_dn, "D", "")))

    # Penetration suffix: P / R / H / ""
    prev_body_top = np.maximum(po, pc)
    prev_body_bot = np.minimum(po, pc)
    pen_up = (h >= prev_body_top) & (h <= ph)
    pen_dn = (l <= prev_body_bot) & (l >= pl)
    pen = np.where(pen_up & pen_dn, "H",
                   np.where(pen_up, "P",
                            np.where(pen_dn, "R", "")))

    # Close suffix: A / O / I
    close_above = c > prev_body_top
    close_below = c < prev_body_bot
    close_s = np.where(close_above, "A",
                       np.where(close_below, "O", "I"))

    # First bar — clear everything
    ne[0] = ""; wick[0] = ""; pen[0] = ""; close_s[0] = ""

    # full_suffix = ne + wick + pen
    full = np.char.add(np.char.add(ne, wick), pen)

    # composite — append close suffix when the bar is "interesting"
    append_close = ((ne == "E") & (wick == "B")) | \
                   ((ne == "N") & ((wick != "") | (pen != "")))
    composite = np.where(append_close, np.char.add(full, close_s), full)

    df["ne_suffix"] = ne
    df["wick_suffix"] = wick
    df["penetration_suffix"] = pen
    df["close_suffix"] = close_s
    df["full_suffix"] = full
    df["composite_full_suffix"] = composite
    return df


def _compute_atr14(df: pd.DataFrame) -> pd.DataFrame:
    """Wilder ATR(14) matching chart's signal_extraction.compute_atr_wilder.
    No min_periods — ATR is available from bar 0 (using just h-l for that bar).
    """
    h = df["high"]; l = df["low"]; c = df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(),
                    (h - pc).abs(),
                    (l - pc).abs()], axis=1).max(axis=1)
    df["atr_14"] = tr.ewm(alpha=1.0 / 14, adjust=False).mean()
    return df


def _compute_body_wick(df: pd.DataFrame,
                      body_expand=1.5, body_min=0.5,
                      wick_heavy=0.5, wick_flat_max=0.3,
                      doji_body_ratio=0.2) -> pd.DataFrame:
    """Line 3: bar_body_wick = body_class + wick_class.
    Body: X (≥1.5x prev) / S (in between) / M (≤0.5x)
    Wick: J (doji) / TB (top-big) / BB (bottom-big) / F (flat) / ""
    """
    o = df["open"]; h = df["high"]; l = df["low"]; c = df["close"]
    body_now = (c - o).abs()
    body_prev = body_now.shift(1).replace(0, 1e-10).fillna(1e-10)
    body_ratio = body_now / body_prev

    body_class = np.where(body_ratio >= body_expand, "X",
                          np.where(body_ratio <= body_min, "M", "S"))

    bar_range = (h - l).replace(0, 1e-10)
    upper_wick = h - o.combine(c, max)
    lower_wick = o.combine(c, min) - l
    upper_frac = upper_wick / bar_range
    lower_frac = lower_wick / bar_range
    body_frac = body_now / bar_range

    wick_class = np.where(body_frac <= doji_body_ratio, "J",
                  np.where(upper_frac >= wick_heavy, "TB",
                  np.where(lower_frac >= wick_heavy, "BB",
                  np.where((upper_frac < wick_flat_max) & (lower_frac < wick_flat_max),
                           "F", ""))))

    # First bar — body_class meaningless without prev body
    body_class = body_class.copy()
    body_class[0] = "S"

    df["bar_body_wick"] = np.char.add(body_class.astype(str), wick_class.astype(str))
    return df


def _compute_gap_range(df: pd.DataFrame,
                      gap_small_atr=0.2, gap_med_atr=0.5,
                      range_vol_mult=1.5, range_contract_mult=0.5) -> pd.DataFrame:
    """Line 4: bar_gap_range = gap_class + "-" + range_class (or one or the other)."""
    o = df["open"]; h = df["high"]; l = df["low"]; c = df["close"]
    ph = h.shift(1); pl = l.shift(1); pc = c.shift(1)
    atr = df["atr_14"].replace(0, np.nan).fillna(1e-10)

    has_gap = (o > ph) | (o < pl)
    gap_atr_ratio = np.where(has_gap, (o - pc).abs() / atr, 0.0)
    gap_class = np.where(~has_gap, "",
                         np.where(gap_atr_ratio < gap_small_atr, "G1",
                         np.where(gap_atr_ratio < gap_med_atr, "G2", "G3")))

    range_ratio = (h - l) / atr
    range_class = np.where(atr.isna() | (atr <= 0), "",
                           np.where(range_ratio > range_vol_mult, "V",
                           np.where(range_ratio < range_contract_mult, "C", "N")))

    # First bar: no prev, clear
    gap_class = gap_class.copy(); gap_class[0] = ""

    df["bar_gap_class"] = gap_class
    df["bar_range_class"] = range_class
    df["bar_gap_range"] = np.where(
        (gap_class != "") & (range_class != ""),
        np.char.add(np.char.add(gap_class.astype(str), "-"), range_class.astype(str)),
        np.where(gap_class != "", gap_class, range_class)
    )
    return df


def _parse_line5_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Parse bar_line5 string into individual bool flags + rsi2_state string."""
    l5 = df["bar_line5"].fillna("").astype(str)
    df["wvf_spike"] = l5.str.contains("VX", regex=False).astype("Int8")
    df["vix_range"] = l5.str.contains("VR", regex=False).astype("Int8")
    df["psar_bull"] = l5.str.contains("PB", regex=False).astype("Int8")
    df["psar_bear"] = l5.str.contains("PS", regex=False).astype("Int8")

    def _extract_rsi2(s: str) -> str:
        for tok in s.split("-"):
            if tok.startswith("R2"):
                return tok
        return ""
    df["rsi2_state"] = l5.apply(_extract_rsi2)
    return df


def _l_digit_flags(df: pd.DataFrame) -> pd.DataFrame:
    """From `l_sig` (e.g. "L34" / "L555" / "L1L2") populate sig_l1..sig_l6."""
    lsig = df["l_sig"].fillna("").astype(str).str.replace("L", "")
    for d in range(1, 7):
        df[f"sig_l{d}"] = lsig.str.contains(str(d), regex=False).astype("Int8")
    return df


def _acc_exit_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Label each bar by its position relative to the next ACC_TR → MARKUP
    (or DIST_TR → MKDN) transition.

    Outputs:
      acc_exit_in_n  (float): bars until next MARKUP/MKDN bar (NaN if >5 ahead)
      acc_exit_class (str):
          BO_NOW   — current bar is the first MARKUP bar
          BO_1     — MARKUP starts next bar
          BO_2_3   — 2-3 bars away ⭐ sweet spot
          BO_4_5   — 4-5 bars away
          BO_LATE  — currently in ACC_TR with no MARKUP within 5 bars
          DIST_EXIT — bear-side ACC equivalent (DIST_TR → MKDN within 5 bars)
          NOT_ACC  — not in any trading range phase
    """
    n = len(df)
    phase = df.get("wyc_phase", pd.Series([""] * n)).fillna("").astype(str).values
    is_acc      = (phase == "ACC_TR")
    is_markup   = (phase == "MARKUP")
    is_dist     = (phase == "DIST_TR")
    is_mkdn     = (phase == "MKDN")

    # ── Forward distance to nearest MARKUP bar ──────────────────────────────
    dist_to_markup = np.full(n, -1, dtype=np.int32)
    last_markup = -1
    for i in range(n - 1, -1, -1):
        if is_markup[i]:
            last_markup = i
        if last_markup > i and (last_markup - i) <= 20:
            dist_to_markup[i] = last_markup - i

    # ── Forward distance to nearest MKDN bar (bear side) ────────────────────
    dist_to_mkdn = np.full(n, -1, dtype=np.int32)
    last_mkdn = -1
    for i in range(n - 1, -1, -1):
        if is_mkdn[i]:
            last_mkdn = i
        if last_mkdn > i and (last_mkdn - i) <= 20:
            dist_to_mkdn[i] = last_mkdn - i

    acc_exit_in_n = np.full(n, np.nan)
    acc_exit_class = np.array(["NOT_ACC"] * n, dtype=object)

    for i in range(n):
        if is_markup[i] and i > 0 and not is_markup[i-1]:
            # First markup bar after ACC_TR
            acc_exit_in_n[i]  = 0
            acc_exit_class[i] = "BO_NOW"
            continue
        if is_mkdn[i] and i > 0 and not is_mkdn[i-1]:
            acc_exit_in_n[i]  = 0
            acc_exit_class[i] = "DIST_EXIT"
            continue
        if is_acc[i]:
            d = dist_to_markup[i]
            if d <= 0:
                acc_exit_class[i] = "BO_LATE"
            elif d == 1:
                acc_exit_in_n[i]  = 1
                acc_exit_class[i] = "BO_1"
            elif d <= 3:
                acc_exit_in_n[i]  = d
                acc_exit_class[i] = "BO_2_3"
            elif d <= 5:
                acc_exit_in_n[i]  = d
                acc_exit_class[i] = "BO_4_5"
            else:
                acc_exit_in_n[i]  = d
                acc_exit_class[i] = "BO_LATE"
            continue
        if is_dist[i]:
            d = dist_to_mkdn[i]
            if d > 0 and d <= 5:
                acc_exit_in_n[i]  = d
                acc_exit_class[i] = "DIST_EXIT"
            else:
                acc_exit_class[i] = "BO_LATE"

    df["acc_exit_in_n"]  = acc_exit_in_n
    df["acc_exit_class"] = acc_exit_class
    return df


def _ultra_extras(df: pd.DataFrame) -> pd.DataFrame:
    """Compute ULTRA-from-DB helper columns.

    - tz_bull: 1 if t_sig non-empty (bullish TZ active)
    - avg_vol_20d: 20-bar rolling volume mean
    - rsi_14: Wilder RSI(14)
    - cci_20: CCI(20) SMA
    - change_pct: close vs prev_close %
    - sweet_spot_active: turbo_score >= 60 AND tz_bull=1
    - late_warning: turbo_score has dropped >= 10 in last 3 bars
    - profile_category: SWEET_SPOT / BUILDING / WATCH / ""
    - profile_score: composite (0-100) — turbo + tz/l confluence
    """
    c = df["close"]

    # tz_bull from t_sig
    if "t_sig" in df.columns:
        df["tz_bull"] = df["t_sig"].fillna("").astype(str).ne("").astype("Int8")
    else:
        df["tz_bull"] = 0

    # avg_vol_20d
    if "volume" in df.columns:
        df["avg_vol_20d"] = df["volume"].rolling(20, min_periods=1).mean()

    # change_pct
    pc = c.shift(1)
    df["change_pct"] = ((c / pc - 1.0) * 100.0).round(2)

    # RSI(14) — Wilder smoothing
    delta = c.diff()
    up = delta.clip(lower=0)
    dn = (-delta).clip(lower=0)
    rs_up = up.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
    rs_dn = dn.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
    rsi = 100.0 - 100.0 / (1.0 + rs_up / rs_dn.replace(0, 1e-10))
    df["rsi_14"] = rsi.round(1)

    # CCI(20)
    h = df["high"]; l = df["low"]
    tp = (h + l + c) / 3.0
    tp_sma = tp.rolling(20, min_periods=1).mean()
    md = (tp - tp_sma).abs().rolling(20, min_periods=1).mean()
    cci = (tp - tp_sma) / (0.015 * md.replace(0, 1e-10))
    df["cci_20"] = cci.round(1)

    # sweet_spot / late_warning / profile
    ts = pd.to_numeric(df.get("turbo_score", 0), errors="coerce").fillna(0)
    bull_regime = (df.get("final_regime", "").astype(str).str.lower() == "bull")
    tzb = df["tz_bull"].fillna(0).astype(int) > 0

    # Sweet Spot: turbo >= 60 AND TZ bull active (don't require explicit bull regime)
    df["sweet_spot_active"] = ((ts >= 60) & tzb).astype("Int8")

    # late_warning: turbo dropped 10+ pts in last 3 bars
    ts_prev3 = ts.shift(3).fillna(ts)
    df["late_warning"] = ((ts_prev3 - ts) >= 10).astype("Int8")

    # profile_score: combination of turbo + tz bull + L + BE confluence
    has_l = pd.to_numeric(df.get("sig_l_any", 0), errors="coerce").fillna(0) > 0
    has_be = pd.to_numeric(df.get("sig_be_any", 0), errors="coerce").fillna(0) > 0
    profile_score = (ts * 0.6
                     + bull_regime.astype(int) * 10
                     + tzb.astype(int) * 12
                     + has_l.astype(int) * 10
                     + has_be.astype(int) * 8).clip(upper=100)
    df["profile_score"] = profile_score.round(1)

    # Categorize — don't strictly require final_regime=bull (often unset)
    cat = pd.Series([""] * len(df), index=df.index)
    cat[df["sweet_spot_active"].astype(int) == 1] = "SWEET_SPOT"
    cat[(cat == "") & (profile_score >= 50) & tzb] = "BUILDING"
    cat[(cat == "") & (profile_score >= 30) & tzb] = "WATCH"
    df["profile_category"] = cat.values

    return df


def _add_hl_hh_outcomes(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Given classified pivots, compute pct_to_next_hl/hh + bars_to_next_hl/hh.

    suffix: "3" or "5" — indicates which pivot variant.
    Assumes columns: swing_type_<suffix>, is_pivot_high_<suffix>, is_pivot_low_<suffix>,
                     fwd_swing_ret_<suffix>, fwd_swing_bars_<suffix>.
    """
    st_col = f"swing_type_{suffix}"
    next_hl_col = f"next_pivot_is_hl_{suffix}"
    next_hh_col = f"next_pivot_is_hh_{suffix}"
    pct_hl = f"pct_to_next_hl_{suffix}"
    pct_hh = f"pct_to_next_hh_{suffix}"
    bars_hl = f"bars_to_next_hl_{suffix}"
    bars_hh = f"bars_to_next_hh_{suffix}"

    n = len(df)
    nxt_is_hl = np.zeros(n, dtype="int8")
    nxt_is_hh = np.zeros(n, dtype="int8")
    pct_hl_arr = np.full(n, np.nan)
    pct_hh_arr = np.full(n, np.nan)
    bars_hl_arr = np.full(n, np.nan)
    bars_hh_arr = np.full(n, np.nan)

    swings = df[st_col].fillna("").astype(str).to_numpy()
    closes = df["close"].to_numpy()
    highs  = df["high"].to_numpy()
    lows   = df["low"].to_numpy()

    # Indices of next pivot of each type
    # For each bar i, find the next bar j > i where swing_type is HL/LL or HH/LH
    # Then mark whether that pivot is HL or HH and compute pct from close[i]
    # We do this in a single backwards scan: nearest "HL" and nearest "HH"
    nearest_hl_idx = -1
    nearest_hh_idx = -1
    # Walk from end to start, tracking the nearest future pivot of each type
    for i in range(n - 1, -1, -1):
        st = swings[i]
        # Update trackers based on THIS bar's pivot
        if st in ("HL",):
            nearest_hl_idx = i
        if st in ("HH",):
            nearest_hh_idx = i
        # Now, for this bar (looking forward to find next pivot):
        # We want pivot strictly AFTER i, so we look at positions > i
        # nearest_*_idx as of THIS iteration includes i itself if i is a pivot
        # so we need to find next > i
        # Track "next" separately: nearest > i means the index of the most recent HL/HH at position > i
        # Use a different approach: iterate forward.

    # Actually, simpler: forward scan, for each i, find next j > i where swing_type matches.
    # Use a "look-ahead" array.
    next_hl_arr = np.full(n, -1, dtype=np.int64)
    next_hh_arr = np.full(n, -1, dtype=np.int64)
    last_hl = -1
    last_hh = -1
    for i in range(n - 1, -1, -1):
        next_hl_arr[i] = last_hl
        next_hh_arr[i] = last_hh
        if swings[i] == "HL":
            last_hl = i
        if swings[i] == "HH":
            last_hh = i

    for i in range(n):
        c = closes[i]
        # Next HL
        j = next_hl_arr[i]
        if j > i and c > 0:
            pct_hl_arr[i] = (lows[j] / c - 1.0) * 100.0
            bars_hl_arr[i] = float(j - i)
        # Next HH
        j = next_hh_arr[i]
        if j > i and c > 0:
            pct_hh_arr[i] = (highs[j] / c - 1.0) * 100.0
            bars_hh_arr[i] = float(j - i)

        # next_pivot_is_hl/hh: of the next pivot AFTER bar i, is it HL or HH?
        # = the closer of next_hl_arr and next_hh_arr (and the same for LH/LL but we focus on bullish HL/HH)
        nh = next_hl_arr[i] if next_hl_arr[i] > i else 10**9
        nH = next_hh_arr[i] if next_hh_arr[i] > i else 10**9
        if nh < nH and nh != 10**9:
            nxt_is_hl[i] = 1
        elif nH < nh and nH != 10**9:
            nxt_is_hh[i] = 1

    df[next_hl_col] = nxt_is_hl
    df[next_hh_col] = nxt_is_hh
    df[pct_hl]      = pct_hl_arr
    df[pct_hh]      = pct_hh_arr
    df[bars_hl]     = bars_hl_arr
    df[bars_hh]     = bars_hh_arr
    return df


def _classify_pivots(df: pd.DataFrame, pivot_lr: int, suffix: str) -> pd.DataFrame:
    """Run Williams pivot classifier with given lookback, write to *_{suffix} columns."""
    sw = classify_swings(df[["high", "low"]].copy(), pivot_left=pivot_lr, pivot_right=pivot_lr)
    df[f"swing_type_{suffix}"]          = sw["swing_type"].values
    df[f"is_pivot_high_{suffix}"]       = sw["is_pivot_high"].astype("Int8").values
    df[f"is_pivot_low_{suffix}"]        = sw["is_pivot_low"].astype("Int8").values
    df[f"fwd_swing_ret_{suffix}"]       = sw["fwd_swing_ret"].values
    df[f"fwd_swing_bars_{suffix}"]      = sw["fwd_swing_bars"].values
    df[f"swing_ret_from_prev_{suffix}"] = sw["swing_ret_from_prev"].values
    df = _add_hl_hh_outcomes(df, suffix)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Per-ticker pipeline
# ─────────────────────────────────────────────────────────────────────────────

ENRICH_COLUMNS = [
    # Suffixes
    "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix",
    "full_suffix", "composite_full_suffix",
    # Line 3 / 4 / 5
    "bar_body_wick", "bar_gap_range", "bar_gap_class", "bar_range_class",
    "bar_line5", "rsi2_state",
    # Line 5 flags
    "wvf_spike", "vix_range", "psar_bull", "psar_bear",
    # ATR
    "atr_14",
    # Williams 3-3
    "swing_type_3", "is_pivot_high_3", "is_pivot_low_3",
    "fwd_swing_ret_3", "fwd_swing_bars_3", "swing_ret_from_prev_3",
    "next_pivot_is_hl_3", "next_pivot_is_hh_3",
    "pct_to_next_hl_3", "pct_to_next_hh_3",
    "bars_to_next_hl_3", "bars_to_next_hh_3",
    # Williams 5-5
    "swing_type_5", "is_pivot_high_5", "is_pivot_low_5",
    "fwd_swing_ret_5", "fwd_swing_bars_5", "swing_ret_from_prev_5",
    "next_pivot_is_hl_5", "next_pivot_is_hh_5",
    "pct_to_next_hl_5", "pct_to_next_hh_5",
    "bars_to_next_hl_5", "bars_to_next_hh_5",
    # L digits
    "sig_l1", "sig_l2", "sig_l3", "sig_l4", "sig_l5", "sig_l6",
    # ULTRA extras
    "tz_bull", "avg_vol_20d", "sweet_spot_active", "late_warning",
    "profile_category", "profile_score",
    "rsi_14", "cci_20", "change_pct",
    # ACC Exit (Breakout Hunter)
    "acc_exit_in_n", "acc_exit_class",
    "aes_score", "aes_stage", "aes_leading", "aes_trend_5d",
    # 260308 + L88 (Pine-equivalent late-stage signals)
    "sig_260308", "sig_l88",
    # ULTRA v2 (eb_bull/bear, fbo_bull/bear, bf_buy/sell, ultra_3up/3dn, best_long/short)
    "eb_bull", "eb_bear", "fbo_bull", "fbo_bear",
    "bf_buy", "bf_sell", "ultra_3up", "ultra_3dn",
    "best_long", "best_short",
    # PARA — Parabola Start Detector (260420)
    "para_prep", "para_start", "para_plus", "para_retest",
    # FLY — ABCD EMA DP (260424)
    "fly_abcd", "fly_cd", "fly_bd", "fly_ad",
    # Delta / order-flow (260403 V2)
    "d_strong_bull", "d_strong_bear",
    "d_absorb_bull", "d_absorb_bear",
    "d_div_bull",    "d_div_bear",
    "d_cd_bull",     "d_cd_bear",
    "d_surge_bull",  "d_surge_bear",
    "d_blast_bull",  "d_blast_bear",
    "d_vd_div_bull", "d_vd_div_bear",
    "d_spring",      "d_upthrust",
    "d_flip_bull",   "d_flip_bear",   "d_orange_bull",
    "d_blast_bull_red", "d_blast_bear_grn",
    "d_surge_bull_red", "d_surge_bear_grn",
    # Version
    "enrich_version",
]


def enrich_ticker_df(df: pd.DataFrame) -> pd.DataFrame:
    """Run full enrichment pipeline on one ticker's bars (must be sorted by date)."""
    if len(df) < 2:
        return df  # need at least 2 bars for prev-bar refs

    df = df.sort_values("date").reset_index(drop=True)
    df = _compute_suffixes(df)
    df = _compute_atr14(df)
    df = _compute_body_wick(df)
    df = _compute_gap_range(df)
    df = compute_line5(df)            # adds bar_line5 from existing analyzer
    df = _parse_line5_flags(df)
    df = _l_digit_flags(df)
    df = _classify_pivots(df, pivot_lr=3, suffix="3")
    df = _classify_pivots(df, pivot_lr=5, suffix="5")
    df = _ultra_extras(df)            # tz_bull, avg_vol_20d, profile_*, rsi_14, cci_20
    df = _acc_exit_labels(df)         # acc_exit_in_n, acc_exit_class
    df = _aes_score_compute(df)       # aes_score (uses lift cache if available)
    df = _compute_pine_engines(df)    # 260308/L88 + ULTRA v2 + PARA + FLY + Delta
    df["enrich_version"] = ENRICH_VERSION
    return df


def _compute_pine_engines(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the Pine/LIVE-scan engines that aren't already in the DB:
    260308/L88, ULTRA v2, PARA, FLY, Delta. Each engine is self-contained
    (takes OHLCV + computed extras and returns a per-bar DataFrame).
    All failures default to 0 so the row is never broken by one bad engine.
    """
    out = df.copy()

    # ── 260308 + L88 ──────────────────────────────────────────────────────
    try:
        from ultra_engine import compute_260308_l88
        u308 = compute_260308_l88(df)
        out["sig_260308"] = u308["sig_260308"].astype("int8") if "sig_260308" in u308.columns else 0
        out["sig_l88"]    = u308["sig_l88"].astype("int8")    if "sig_l88"    in u308.columns else 0
    except Exception:
        out["sig_260308"] = 0
        out["sig_l88"]    = 0

    # ── ULTRA v2 ──────────────────────────────────────────────────────────
    try:
        from ultra_engine import compute_ultra_v2
        uv2 = compute_ultra_v2(df)
        for col in ("eb_bull", "eb_bear", "fbo_bull", "fbo_bear",
                    "bf_buy", "bf_sell", "ultra_3up", "ultra_3dn",
                    "best_long", "best_short"):
            out[col] = uv2[col].astype("int8") if col in uv2.columns else 0
    except Exception:
        for col in ("eb_bull", "eb_bear", "fbo_bull", "fbo_bear",
                    "bf_buy", "bf_sell", "ultra_3up", "ultra_3dn",
                    "best_long", "best_short"):
            out[col] = 0

    # ── PARA — Parabola Start Detector ─────────────────────────────────────
    try:
        from para_engine import compute_para_series
        para = compute_para_series(df, is_daily=True)
        if para is not None:
            for col in ("para_prep", "para_start", "para_plus", "para_retest"):
                out[col] = para[col].fillna(0).astype("int8") if col in para.columns else 0
        else:
            for col in ("para_prep", "para_start", "para_plus", "para_retest"):
                out[col] = 0
    except Exception:
        for col in ("para_prep", "para_start", "para_plus", "para_retest"):
            out[col] = 0

    # ── FLY — ABCD EMA DP ──────────────────────────────────────────────────
    try:
        from fly_engine import compute_fly_series
        fly = compute_fly_series(df)
        for col in ("fly_abcd", "fly_cd", "fly_bd", "fly_ad"):
            out[col] = fly[col].fillna(0).astype("int8") if col in fly.columns else 0
    except Exception:
        for col in ("fly_abcd", "fly_cd", "fly_bd", "fly_ad"):
            out[col] = 0

    # ── Delta / order-flow (260403 V2) ─────────────────────────────────────
    # 20 signal columns: strong/absorb/div/cd/surge/blast bull+bear,
    # vd_div bull+bear, spring/upthrust, flip bull+bear, orange_bull,
    # blast_bull_red / blast_bear_grn, surge_bull_red / surge_bear_grn.
    _DELTA_COLS = (
        "strong_bull", "strong_bear", "absorb_bull", "absorb_bear",
        "div_bull", "div_bear", "cd_bull", "cd_bear",
        "surge_bull", "surge_bear", "blast_bull", "blast_bear",
        "vd_div_bull", "vd_div_bear", "spring", "upthrust",
        "flip_bull", "flip_bear", "orange_bull",
        "blast_bull_red", "blast_bear_grn",
        "surge_bull_red", "surge_bear_grn",
    )
    try:
        from delta_engine import compute_delta
        ddf = compute_delta(df)
        for col in _DELTA_COLS:
            ui_col = f"d_{col}"
            if col in ddf.columns:
                out[ui_col] = ddf[col].fillna(0).astype("int8")
            else:
                out[ui_col] = 0
    except Exception:
        for col in _DELTA_COLS:
            out[f"d_{col}"] = 0

    return out


# ─────────────────────────────────────────────────────────────────────────────
# AES Score v2 (Accumulation-Exit Score) —
#   Uses per-ticker calibrated lifts when available, falls back to global.
#   Weighted to favor LEADING signals (high 2-3d lift, low close-day lift).
# ─────────────────────────────────────────────────────────────────────────────
_AES_GLOBAL_LIFT_CACHE: dict | None = None
_AES_TICKER_LIFT_CACHE: dict | None = None    # {(ticker, universe): {sig: {...}}}


def _load_aes_lift_cache():
    """Load global empirical signal lifts from DB table acc_exit_lift_v1.
    Cached in-process. If table missing, returns empty dict → AES = naive."""
    global _AES_GLOBAL_LIFT_CACHE
    if _AES_GLOBAL_LIFT_CACHE is not None:
        return _AES_GLOBAL_LIFT_CACHE
    try:
        conn = get_conn(read_only=True)
        try:
            tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
            if "acc_exit_lift_v1" not in tables:
                _AES_GLOBAL_LIFT_CACHE = {}
                return _AES_GLOBAL_LIFT_CACHE
            df = conn.execute(
                "SELECT signal, lift_close, lift_2_3, n_acc FROM acc_exit_lift_v1"
            ).fetchdf()
            _AES_GLOBAL_LIFT_CACHE = {
                row["signal"]: {
                    "lift_close":  float(row["lift_close"] or 1.0),
                    "lift_2_3":    float(row["lift_2_3"] or 1.0),
                    "n_acc":       int(row["n_acc"] or 0),
                }
                for _, row in df.iterrows()
            }
        finally:
            conn.close()
    except Exception:
        _AES_GLOBAL_LIFT_CACHE = {}
    return _AES_GLOBAL_LIFT_CACHE


def _load_per_ticker_lifts():
    """Load per-ticker calibrated lifts from ticker_signal_lift_v1.
    Returns: {(ticker, universe): {sig: {lift_blend_2_3, lift_blend_cl, n_local}}}.
    """
    global _AES_TICKER_LIFT_CACHE
    if _AES_TICKER_LIFT_CACHE is not None:
        return _AES_TICKER_LIFT_CACHE
    try:
        conn = get_conn(read_only=True)
        try:
            tables = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
            if "ticker_signal_lift_v1" not in tables:
                _AES_TICKER_LIFT_CACHE = {}
                return _AES_TICKER_LIFT_CACHE
            df = conn.execute("""
                SELECT ticker, universe, signal,
                       n_local, lift_blend_2_3, lift_blend_cl
                FROM ticker_signal_lift_v1
            """).fetchdf()
            cache: dict = {}
            for _, row in df.iterrows():
                key = (row["ticker"], row["universe"])
                cache.setdefault(key, {})[row["signal"]] = {
                    "lift_2_3":  float(row["lift_blend_2_3"] or 1.0),
                    "lift_close": float(row["lift_blend_cl"] or 1.0),
                    "n_local":   int(row["n_local"] or 0),
                }
            _AES_TICKER_LIFT_CACHE = cache
        finally:
            conn.close()
    except Exception:
        _AES_TICKER_LIFT_CACHE = {}
    return _AES_TICKER_LIFT_CACHE


def reset_aes_caches():
    """Force re-load of lift caches (call after re-mining)."""
    global _AES_GLOBAL_LIFT_CACHE, _AES_TICKER_LIFT_CACHE
    _AES_GLOBAL_LIFT_CACHE = None
    _AES_TICKER_LIFT_CACHE = None


# Comprehensive signal universe — mirror the candidate list in acc_exit_miner.
# AES is computed empirically from mined lifts; if a signal isn't in the lift
# table yet, the heuristic weight here is used as fallback.
_AES_FALLBACK_WEIGHTS = {
    # Wyckoff
    "wyc_spring":  4.0, "wyc_sos": 3.5, "wyc_in_tr": 1.0, "wyc_sow": 0.5,
    # Prebreak
    "prebreak_prime": 3.0, "prebreak_ready": 2.2, "prebreak_watch": 1.5,
    "pb_lvbo": 3.5, "pb_wvf_confirm": 2.8, "pb_stop_cause": 2.0,
    # AD
    "ad_fresh": 2.5, "ad_cluster": 3.2,
    # TZ — T family
    **{f"sig_t{n}": 1.0 for n in range(1, 13)},
    "sig_t1g": 1.5, "sig_t2g": 1.8,
    # TZ — Z family
    **{f"sig_z{n}": 0.8 for n in range(1, 13)},
    "sig_z1g": 1.0, "sig_z2g": 1.0,
    # TZ state
    "sig_tz_flip": 2.5, "sig_bias_up": 2.0, "tz_bull": 0.5,
    # L digits + WLNBB
    **{f"sig_l{d}": 1.5 for d in range(1, 7)},
    "l34": 1.5, "l43": 1.5, "l22": 1.0,
    "be_up": 1.8, "bo_up": 2.5, "bx_up": 2.5, "vbo_up": 3.0,
    "sig_fri34": 1.0, "sig_fri43": 1.0, "sig_fri64": 1.0,
    "sig_blue": 0.8, "sig_l_any": 0.5, "sig_be_any": 0.8,
    # CCI
    "sig_cci": 1.0, "sig_cci0r": 1.0, "sig_ccib": 1.0,
    # Volume
    "sig_vol_5x": 1.5, "sig_vol_10x": 2.0, "sig_vol_20x": 3.0, "sig_va": 1.0,
    # VABS
    "sig_abs": 1.0, "sig_clm": 1.5, "sig_sc": 1.0, "sig_bc": 2.5,
    "sig_best": 1.2, "sig_strong": 1.2, "sig_best_up": 1.5,
    "sig_fbo_up": 2.0, "sig_eb_up": 2.0, "sig_3up": 2.0,
    # GOG
    "sig_g1": 1.0, "sig_g2": 1.5, "sig_g4": 1.5, "sig_g6": 2.0, "sig_g11": 1.0,
    "sig_gog_plus": 2.5,
    "g1p": 2.0, "g2p": 1.5, "g3p": 1.5, "g1l": 2.0, "g2l": 1.5,
    "g1c": 2.5, "g2c": 1.5, "g3c": 1.5,
    # FLY
    "sig_fly_abcd": 1.5, "sig_fly_cd": 1.0, "sig_fly_bd": 1.0, "sig_fly_ad": 1.0,
    # WICK
    "sig_wk_up": 2.0, "sig_x1": 1.5, "sig_x2": 1.5, "sig_x1g": 1.5, "sig_x3": 1.5,
    # PREUP (often pre-EMA-cross — STRONG leading)
    "sig_p2": 3.0, "sig_p3": 3.5, "sig_p50": 2.5, "sig_p55": 4.0,
    "sig_p66": 4.5, "sig_p89": 4.0, "sig_any_p": 2.5,
    # PREDN (counter-intuitive — surprisingly leading per mining)
    "sig_d2": 1.5, "sig_d3": 1.5, "sig_d50": 1.5, "sig_d55": 2.0,
    "sig_d66": 2.5, "sig_d89": 2.0, "sig_any_d": 1.5,
    # Combo / Momentum
    "sig_buy": 1.5, "sig_3g": 4.0, "sig_conso": 1.5, "sig_svs": 1.0,
    "sig_cd": 2.5, "sig_ca": 2.0, "sig_cw": 1.5,
    "rocket": 1.5, "hilo_buy": 1.5, "three_g": 4.0,
    # Delta / CISD
    "sig_flp_up": 1.5, "sig_org_up": 1.5, "sig_dd_up_red": 1.0, "sig_d_up_red": 1.0,
    "sig_cisd_cplus": 3.0, "sig_cisd_cplus_minus": 2.0, "sig_cisd_cplus_mm": 2.0,
    # PARA
    "sig_para_prep": 2.5, "sig_para_start": 3.0,
    "sig_para_plus": 3.0, "sig_para_retest": 2.5,
    # EMA position (STRONG leading per mining)
    "price_gt_20":  1.5, "price_gt_50":  3.0, "price_gt_89":  4.0, "price_gt_200": 4.5,
    # RSI
    "rsi_ge_70": 2.5, "rsi_le_35": 1.5,
    # Line5 booleans
    "wvf_spike": 1.0, "psar_bull": 1.5,
}


def _aes_score_compute(df: pd.DataFrame) -> pd.DataFrame:
    """AES v3 — per-ticker calibrated lift score with companion "leading-bias" metric.

    AES (main score):
      For each active pre-breakout signal s, weight = max(0, lift_2_3 - 1.0).
      AES = (Σ active_s × w(s)) / Σ all w(s) × 100  (0..100 fraction-of-max)

    AES_LEADING (companion):
      Same as AES but weights are LEADING-only:
        w_lead(s) = max(0, lift_2_3 - lift_close)
      Tells us how much of the AES comes from genuinely leading signals
      (vs coincident-with-breakout signals).

    AES_TREND_5D:
      Today's AES minus AES 5 bars ago — positive = score rising, negative = fading.

    Per-ticker lookup: if ticker_signal_lift_v1 has data, use blended lift,
    else fall back to global lift.
    """
    global_lifts = _load_aes_lift_cache()
    ticker_lifts = _load_per_ticker_lifts()
    n = len(df)

    # Identify ticker for this dataframe (single-ticker chunk in enricher)
    if "ticker" in df.columns and "universe" in df.columns and len(df) > 0:
        tk = df["ticker"].iloc[0]
        uni = df["universe"].iloc[0] if "universe" in df.columns else None
        per_ticker = ticker_lifts.get((tk, uni), {})
    else:
        per_ticker = {}

    aes_main    = np.zeros(n)
    aes_leading = np.zeros(n)
    max_main    = 0.0
    max_leading = 0.0

    for sig in _AES_FALLBACK_WEIGHTS.keys():
        if sig not in df.columns:
            continue

        loc = per_ticker.get(sig)
        glb = global_lifts.get(sig)

        if loc and loc.get("n_local", 0) >= 5:
            lift_2_3 = loc["lift_2_3"]
            lift_cl  = loc["lift_close"]
        elif glb and glb.get("n_acc", 0) >= 50:
            lift_2_3 = glb["lift_2_3"]
            lift_cl  = glb["lift_close"]
        else:
            lift_2_3 = 1.0 + _AES_FALLBACK_WEIGHTS[sig] / 10.0
            lift_cl  = 1.0

        # Main weight: how much lift over baseline (1.0)
        w = max(0.0, lift_2_3 - 1.0)
        # Leading weight: lift_2_3 in excess of close-day lift
        w_lead = max(0.0, lift_2_3 - lift_cl)

        if w <= 0 and w_lead <= 0:
            continue

        active = pd.to_numeric(df[sig], errors="coerce").fillna(0).astype(int) > 0

        if w > 0:
            max_main += w
            aes_main[active] += w
        if w_lead > 0:
            max_leading += w_lead
            aes_leading[active] += w_lead

    if max_main > 0:
        aes_main = (aes_main / max_main) * 100.0
    if max_leading > 0:
        aes_leading = (aes_leading / max_leading) * 100.0

    df["aes_score"]   = np.round(aes_main, 1)
    df["aes_leading"] = np.round(aes_leading, 1)

    # 5-bar trend: today's AES minus 5-bar-ago AES
    aes_lag5 = pd.Series(aes_main).shift(5).fillna(method='bfill').fillna(0).values
    df["aes_trend_5d"] = np.round(aes_main - aes_lag5, 1)

    # Stage label combines wyc_phase + AES bucket
    phase = df.get("wyc_phase", pd.Series([""] * n)).fillna("").astype(str).values
    stage_arr = np.array([""] * n, dtype=object)
    for i in range(n):
        s = aes_main[i]
        p = phase[i]
        if p == "MARKUP":
            stage_arr[i] = "MARKUP" if s < 40 else "MARKUP★"
        elif p == "MKDN":
            stage_arr[i] = "MKDN"
        elif p == "ACC_TR":
            if   s >= 70: stage_arr[i] = "PRIME★★"
            elif s >= 50: stage_arr[i] = "READY"
            elif s >= 30: stage_arr[i] = "BUILDING"
            else:         stage_arr[i] = "ACC"
        elif p == "DIST_TR":
            stage_arr[i] = "DIST"
        elif p == "SPRING":
            stage_arr[i] = "SPRING★"
        elif p == "SOS":
            stage_arr[i] = "SOS★"
        elif p == "UTAD":
            stage_arr[i] = "UTAD"
    df["aes_stage"] = stage_arr
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Bulk runner
# ─────────────────────────────────────────────────────────────────────────────

def _write_progress(stage: str, done: int, total: int,
                    started_at: float, extra: dict | None = None) -> None:
    elapsed = time.time() - started_at
    pct = round(done / total * 100, 1) if total else 0
    eta = round(elapsed / done * (total - done)) if done > 0 else None
    payload = {
        "stage": stage, "done": done, "total": total, "pct": pct,
        "elapsed_seconds": round(elapsed, 1), "eta_seconds": eta,
    }
    if extra:
        payload.update(extra)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(payload, f)


def _enrich_one_ticker(args: tuple) -> tuple[str, int, str | None]:
    """Worker function. Takes (ticker, universe, db_path) and updates DB rows
    via its own short-lived DuckDB connection.
    Returns (ticker, n_rows_updated, error_str_or_None)."""
    ticker, universe, db_path = args
    try:
        import duckdb
        conn = duckdb.connect(db_path, read_only=False)
        try:
            df = conn.execute("""
                SELECT id, ticker, date, open, high, low, close, volume,
                       l_sig, t_sig, z_sig, final_regime, turbo_score,
                       sig_l_any, sig_be_any
                FROM bars
                WHERE ticker = ? AND universe = ?
                ORDER BY date
            """, [ticker, universe]).fetchdf()
            if len(df) < 2:
                return (ticker, 0, None)

            df = enrich_ticker_df(df)

            update_cols = [c for c in ENRICH_COLUMNS if c in df.columns]
            updates = df[["id"] + update_cols].copy()

            conn.register("upd_tmp", updates)
            set_clauses = ", ".join(f"{c} = upd_tmp.{c}" for c in update_cols)
            conn.execute(f"""
                UPDATE bars
                SET {set_clauses}
                FROM upd_tmp
                WHERE bars.id = upd_tmp.id
            """)
            conn.unregister("upd_tmp")
            conn.commit()
            return (ticker, len(df), None)
        finally:
            conn.close()
    except Exception as e:
        return (ticker, 0, f"{type(e).__name__}: {e}")


def _enrich_one_ticker_compute_only(args: tuple) -> tuple[str, pd.DataFrame | None, str | None]:
    """Worker: load + compute only, return enriched DataFrame for the coordinator to write.
    Avoids multi-process write contention on DuckDB."""
    ticker, universe, db_path = args
    try:
        import duckdb
        # Read-only connection — safe for concurrent processes
        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute("""
                SELECT id, ticker, date, open, high, low, close, l_sig
                FROM bars
                WHERE ticker = ? AND universe = ?
                ORDER BY date
            """, [ticker, universe]).fetchdf()
        finally:
            conn.close()
        if len(df) < 2:
            return (ticker, None, None)

        df = enrich_ticker_df(df)
        update_cols = [c for c in ENRICH_COLUMNS if c in df.columns]
        return (ticker, df[["id"] + update_cols].copy(), None)
    except Exception as e:
        return (ticker, None, f"{type(e).__name__}: {e}")


def enrich_universe(universe: str = "sp500", max_workers: int = 1) -> dict:
    """Enrich ALL bars for a given universe. Idempotent.

    Note: DuckDB has cross-process write-lock semantics — multi-process workers
    conflict with the uvicorn main process. Serial (max_workers=1) is the
    reliable path; ~10–15 min for 740K rows.

    Returns summary dict: {tickers, rows_updated, errors, duration}.
    """
    started = time.time()
    log.info("Enrichment starting — universe=%s, workers=%d", universe, max_workers)
    _write_progress("loading_tickers", 0, 0, started)

    # Get ticker list (open + close immediately to release lock)
    conn = get_conn(read_only=True)
    try:
        tickers = [r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM bars WHERE universe = ? ORDER BY ticker",
            [universe]
        ).fetchall()]
    finally:
        conn.close()

    total = len(tickers)
    log.info("Found %d tickers in %s", total, universe)
    _write_progress("enriching", 0, total, started)

    rows_total = 0
    errors = []
    done = 0

    # Always use serial path — robust against DuckDB lock conflicts.
    # Single long-lived RW connection for the whole job avoids open/close
    # contention with other API handlers in the same uvicorn process.
    if True or max_workers <= 1:
        conn = get_conn(read_only=False)
        try:
            for tk in tickers:
                try:
                    df = conn.execute("""
                        SELECT id, ticker, date, open, high, low, close, volume,
                               l_sig, t_sig, z_sig, final_regime, turbo_score,
                               sig_l_any, sig_be_any,
                               wyc_phase, wyc_spring, wyc_sos, wyc_in_tr, wyc_sow,
                               ad_fresh, ad_cluster,
                               prebreak_prime, prebreak_ready, prebreak_watch,
                               pb_lvbo, pb_wvf_confirm, pb_stop_cause, pb_macro_penalty,
                               sig_bias_up
                        FROM bars
                        WHERE ticker = ? AND universe = ?
                        ORDER BY date
                    """, [tk, universe]).fetchdf()
                    if len(df) < 2:
                        done += 1
                        continue

                    df = enrich_ticker_df(df)
                    update_cols = [c for c in ENRICH_COLUMNS if c in df.columns]
                    updates = df[["id"] + update_cols].copy()

                    conn.register("upd_tmp", updates)
                    set_clauses = ", ".join(f"{c} = upd_tmp.{c}" for c in update_cols)
                    conn.execute(f"""
                        UPDATE bars
                        SET {set_clauses}
                        FROM upd_tmp
                        WHERE bars.id = upd_tmp.id
                    """)
                    conn.unregister("upd_tmp")
                    rows_total += len(df)
                except Exception as e:
                    errors.append({"ticker": tk, "error": f"{type(e).__name__}: {e}"})
                done += 1
                if done % 25 == 0 or done == total:
                    conn.commit()
                    _write_progress("enriching", done, total, started,
                                    {"rows_updated": rows_total, "errors": len(errors)})
            conn.commit()
        finally:
            conn.close()
    else:
        # Parallel: workers compute (read-only) in parallel, main process writes serially.
        # Avoids DuckDB multi-process write contention.
        args = [(tk, universe, STUDIO_DB_PATH) for tk in tickers]
        writer_conn = get_conn(read_only=False)
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(_enrich_one_ticker_compute_only, a) for a in args]
                for fut in as_completed(futures):
                    tk, upd_df, err = fut.result()
                    done += 1
                    if err:
                        errors.append({"ticker": tk, "error": err})
                    elif upd_df is not None and len(upd_df) > 0:
                        update_cols = [c for c in upd_df.columns if c != "id"]
                        set_clauses = ", ".join(f"{c} = upd_tmp.{c}" for c in update_cols)
                        writer_conn.register("upd_tmp", upd_df)
                        try:
                            writer_conn.execute(f"""
                                UPDATE bars
                                SET {set_clauses}
                                FROM upd_tmp
                                WHERE bars.id = upd_tmp.id
                            """)
                            rows_total += len(upd_df)
                        finally:
                            writer_conn.unregister("upd_tmp")
                    if done % 25 == 0 or done == total:
                        writer_conn.commit()
                        _write_progress("enriching", done, total, started,
                                        {"rows_updated": rows_total, "errors": len(errors)})
            writer_conn.commit()
        finally:
            writer_conn.close()

    duration = time.time() - started
    summary = {
        "universe": universe,
        "tickers": total,
        "rows_updated": rows_total,
        "errors": len(errors),
        "duration_sec": round(duration, 1),
        "error_samples": errors[:5],
    }
    _write_progress("done", done, total, started,
                    {"rows_updated": rows_total, "errors": len(errors),
                     "summary": summary})
    log.info("Enrichment complete: %s", summary)
    return summary


def get_progress() -> dict:
    """Read latest progress file."""
    if not os.path.exists(PROGRESS_FILE):
        return {"stage": "idle", "done": 0, "total": 0, "pct": 0}
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"stage": "unknown", "done": 0, "total": 0, "pct": 0}
