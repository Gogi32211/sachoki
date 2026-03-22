"""
Signal Engine — Pine Script → pandas translation.

Each bar gets exactly ONE signal (0=NONE or the highest-priority T/Z signal).

Signal IDs
----------
Bullish (T):
  0=NONE  1=T1G  2=T1  3=T2G  4=T2  5=T3  6=T4  7=T5  8=T6
  9=T9  10=T10  11=T11

Bearish (Z):
  12=Z1G  13=Z1  14=Z2G  15=Z2  16=Z3  17=Z4  18=Z5  19=Z6
  20=Z7(doji)  21=Z8  22=Z9  23=Z10  24=Z11  25=Z12

Priority (bullish, highest → lowest):
  T4 > T6 > T1G > T2G > T1 > T2 > T9 > T10 > T3 > T11 > T5

Priority (bearish, highest → lowest):
  Z4 > Z6 > Z1G > Z2G > Z1 > Z2 > Z8 > Z9 > Z10 > Z3 > Z11 > Z5 > Z12 > Z7(doji)

Key rules:
  - Doji: body/range <= 0.05  → always Z7
  - If any bullish pattern fires → bearish not evaluated on that bar
  - Z8 only fires if NO other Z signal on the same bar
  - Z7 only fires if NO other T or Z signal on the same bar
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NONE = 0

# Bullish
T1G  = 1
T1   = 2
T2G  = 3
T2   = 4
T3   = 5
T4   = 6
T5   = 7
T6   = 8
T9   = 9
T10  = 10
T11  = 11

# Bearish
Z1G  = 12
Z1   = 13
Z2G  = 14
Z2   = 15
Z3   = 16
Z4   = 17
Z5   = 18
Z6   = 19
Z7   = 20   # doji
Z8   = 21
Z9   = 22
Z10  = 23
Z11  = 24
Z12  = 25

SIG_NAMES: dict[int, str] = {
    NONE: "NONE",
    T1G:  "T1G",  T1:  "T1",  T2G: "T2G", T2:  "T2",
    T3:   "T3",   T4:  "T4",  T5:  "T5",  T6:  "T6",
    T9:   "T9",   T10: "T10", T11: "T11",
    Z1G:  "Z1G",  Z1:  "Z1",  Z2G: "Z2G", Z2:  "Z2",
    Z3:   "Z3",   Z4:  "Z4",  Z5:  "Z5",  Z6:  "Z6",
    Z7:   "Z7",   Z8:  "Z8",  Z9:  "Z9",  Z10: "Z10",
    Z11:  "Z11",  Z12: "Z12",
}

BULLISH_SIGS = {T1G, T1, T2G, T2, T3, T4, T5, T6, T9, T10, T11}
BEARISH_SIGS = {Z1G, Z1, Z2G, Z2, Z3, Z4, Z5, Z6, Z7, Z8, Z9, Z10, Z11, Z12}

# Ordered priority lists (highest first)
BULL_PRIORITY: list[int] = [T4, T6, T1G, T2G, T1, T2, T9, T10, T3, T11, T5]
BEAR_PRIORITY: list[int] = [Z4, Z6, Z1G, Z2G, Z1, Z2, Z8, Z9, Z10, Z3, Z11, Z5, Z12, Z7]

# ---------------------------------------------------------------------------
# Helpers — vectorized bar-component series
# ---------------------------------------------------------------------------

def _components(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Return common bar components as a dict of Series."""
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    body     = (c - o).abs()
    rng      = h - l
    bull_bar = c > o
    bear_bar = c < o
    mid      = (o + c) / 2.0       # body midpoint
    rng_mid  = (h + l) / 2.0       # range midpoint (for wick/position checks)
    body_top    = np.where(bull_bar, c, o)
    body_bottom = np.where(bull_bar, o, c)
    upper_wick  = h - pd.Series(body_top,    index=df.index)
    lower_wick  = pd.Series(body_bottom, index=df.index) - l
    body_pct    = body / rng.replace(0, np.nan)   # NaN when range == 0

    prev_c = c.shift(1)
    prev_o = o.shift(1)
    prev_h = h.shift(1)
    prev_l = l.shift(1)
    prev_body    = (prev_c - prev_o).abs()
    prev_rng     = prev_h - prev_l
    prev_bull    = prev_c > prev_o
    prev_bear    = prev_c < prev_o
    prev_body_top    = np.where(prev_bull, prev_c, prev_o)
    prev_body_bottom = np.where(prev_bull, prev_o, prev_c)  # bull→o (lower), bear→c (lower)

    return dict(
        o=o, h=h, l=l, c=c,
        body=body, rng=rng,
        bull_bar=bull_bar, bear_bar=bear_bar,
        mid=mid, rng_mid=rng_mid,
        body_top=pd.Series(body_top, index=df.index),
        body_bottom=pd.Series(body_bottom, index=df.index),
        upper_wick=upper_wick,
        lower_wick=lower_wick,
        body_pct=body_pct,
        prev_c=prev_c, prev_o=prev_o, prev_h=prev_h, prev_l=prev_l,
        prev_body=prev_body, prev_rng=prev_rng,
        prev_bull=pd.Series(prev_bull, index=df.index),
        prev_bear=pd.Series(prev_bear, index=df.index),
        prev_body_top=pd.Series(prev_body_top, index=df.index),
        prev_body_bottom=pd.Series(prev_body_bottom, index=df.index),
    )

# ---------------------------------------------------------------------------
# Doji detector
# ---------------------------------------------------------------------------

def _is_doji(b: dict) -> pd.Series:
    """body/range <= 0.05 (or range == 0)."""
    return (b["body_pct"] <= 0.05) | (b["rng"] == 0)

# ---------------------------------------------------------------------------
# Bullish pattern detectors  (return boolean Series)
# ---------------------------------------------------------------------------

def _cond_T1G(b: dict) -> pd.Series:
    """
    T1G — Bullish Marubozu / strong bull bar.
    Large bull body (>= 60 % of range), small wicks (each < 20 % of range).
    Gap-up open (open > prev close) required.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & (b["body"] / rng >= 0.60)
        & (b["upper_wick"] / rng < 0.20)
        & (b["lower_wick"] / rng < 0.20)
        & (b["o"] > b["prev_c"])
    )

def _cond_T1(b: dict) -> pd.Series:
    """
    T1 — Bullish Marubozu (no gap required).
    Large bull body (>= 60 %), small wicks (each < 20 %).
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & (b["body"] / rng >= 0.60)
        & (b["upper_wick"] / rng < 0.20)
        & (b["lower_wick"] / rng < 0.20)
    )

def _cond_T2G(b: dict) -> pd.Series:
    """
    T2G — Bullish Engulfing with gap.
    Current bull bar engulfs prior bear bar body; open gaps below prev low.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & b["prev_bear"]
        & (b["c"] > b["prev_body_top"])
        & (b["o"] < b["prev_body_bottom"])
        & (b["o"] < b["prev_l"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_T2(b: dict) -> pd.Series:
    """
    T2 — Bullish Engulfing (no gap required).
    Current bull bar body engulfs prior bear bar body.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & b["prev_bear"]
        & (b["c"] > b["prev_body_top"])
        & (b["o"] < b["prev_body_bottom"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_T3(b: dict) -> pd.Series:
    """
    T3 — Hammer (bull bar).
    Small body in upper half of range; lower wick >= 2× body; tiny upper wick.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & (b["body"] / rng <= 0.35)
        & (b["lower_wick"] >= 2.0 * b["body"].replace(0, np.nan))
        & (b["upper_wick"] / rng <= 0.15)
        & (b["body_bottom"] > b["rng_mid"])
    )

def _cond_T4(b: dict) -> pd.Series:
    """
    T4 — Bullish Piercing Line / strong reversal engulfing.
    After bearish bar: bull bar opens below prev low, closes above prev mid.
    Large body (>= 50 % range).
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & b["prev_bear"]
        & (b["o"] < b["prev_l"])
        & (b["c"] > b["prev_c"] + b["prev_body"] * 0.50)
        & (b["c"] < b["prev_o"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_T5(b: dict) -> pd.Series:
    """
    T5 — Inverted Hammer (bull bar).
    Small body in lower half of range; large upper wick >= 2× body; tiny lower wick.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & (b["body"] / rng <= 0.35)
        & (b["upper_wick"] >= 2.0 * b["body"].replace(0, np.nan))
        & (b["lower_wick"] / rng <= 0.15)
        & (b["body_top"] < b["rng_mid"])
    )

def _cond_T6(b: dict) -> pd.Series:
    """
    T6 — Morning Star equivalent: strong bull bar after doji/small body.
    Prev bar: small body (<= 30 % range).
    Current: large bull bar (body >= 60 % range), closes above prev open.
    """
    prev_rng = b["prev_rng"].replace(0, np.nan)
    rng      = b["rng"].replace(0, np.nan)
    return (
        b["bull_bar"]
        & (b["prev_body"] / prev_rng <= 0.30)
        & (b["body"] / rng >= 0.60)
        & (b["c"] > b["prev_o"])
    )

def _cond_T9(b: dict) -> pd.Series:
    """
    T9 — Bullish Harami.
    Current bar (bull or bear) fully inside prior large bear bar body.
    """
    return (
        b["prev_bear"]
        & (b["prev_body"] > b["body"])
        & (b["body_top"] < b["prev_body_top"])
        & (b["body_bottom"] > b["prev_body_bottom"])
        & (b["prev_body"] / b["prev_rng"].replace(0, np.nan) >= 0.60)
    )

def _cond_T10(b: dict) -> pd.Series:
    """
    T10 — Tweezer Bottom.
    Two consecutive bars with matching lows (within 0.1 % of each other).
    """
    low_diff = (b["l"] - b["prev_l"]).abs()
    return (
        b["bull_bar"]
        & b["prev_bear"]
        & (low_diff / b["prev_l"].replace(0, np.nan) <= 0.001)
    )

def _cond_T11(b: dict) -> pd.Series:
    """
    T11 — Three-bar bullish reversal: two prior bears + current bull bar
    that closes above the first of the two prior bars' open.
    """
    prev2_c = b["c"].shift(2)
    prev2_o = b["o"].shift(2)
    prev2_bear = prev2_c < prev2_o
    return (
        b["bull_bar"]
        & b["prev_bear"]
        & prev2_bear
        & (b["c"] > prev2_o)
    )

# ---------------------------------------------------------------------------
# Bearish pattern detectors
# ---------------------------------------------------------------------------

def _cond_Z1G(b: dict) -> pd.Series:
    """
    Z1G — Bearish Marubozu with gap-down open.
    Large bear body (>= 60 % range), tiny wicks, open < prev close.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["body"] / rng >= 0.60)
        & (b["upper_wick"] / rng < 0.20)
        & (b["lower_wick"] / rng < 0.20)
        & (b["o"] < b["prev_c"])
    )

def _cond_Z1(b: dict) -> pd.Series:
    """Z1 — Bearish Marubozu (no gap)."""
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["body"] / rng >= 0.60)
        & (b["upper_wick"] / rng < 0.20)
        & (b["lower_wick"] / rng < 0.20)
    )

def _cond_Z2G(b: dict) -> pd.Series:
    """
    Z2G — Bearish Engulfing with gap.
    Current bear engulfs prior bull body; open gaps above prev high.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & b["prev_bull"]
        & (b["o"] > b["prev_body_top"])
        & (b["c"] < b["prev_body_bottom"])
        & (b["o"] > b["prev_h"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_Z2(b: dict) -> pd.Series:
    """Z2 — Bearish Engulfing (no gap)."""
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & b["prev_bull"]
        & (b["o"] > b["prev_body_top"])
        & (b["c"] < b["prev_body_bottom"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_Z3(b: dict) -> pd.Series:
    """
    Z3 — Shooting Star (bear bar).
    Small body in lower half of range; upper wick >= 2× body; tiny lower wick.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["body"] / rng <= 0.35)
        & (b["upper_wick"] >= 2.0 * b["body"].replace(0, np.nan))
        & (b["lower_wick"] / rng <= 0.15)
        & (b["body_top"] < b["rng_mid"])
    )

def _cond_Z4(b: dict) -> pd.Series:
    """
    Z4 — Dark Cloud Cover / strong bearish piercing.
    After bull bar: bear bar opens above prev high, closes below prev mid.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & b["prev_bull"]
        & (b["o"] > b["prev_h"])
        & (b["c"] < b["prev_c"] - b["prev_body"] * 0.50)
        & (b["c"] > b["prev_o"])
        & (b["body"] / rng >= 0.50)
    )

def _cond_Z5(b: dict) -> pd.Series:
    """
    Z5 — Hanging Man (bear bar).
    Small body in upper half of range; long lower wick >= 2× body; tiny upper wick.
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["body"] / rng <= 0.35)
        & (b["lower_wick"] >= 2.0 * b["body"].replace(0, np.nan))
        & (b["upper_wick"] / rng <= 0.15)
        & (b["body_bottom"] > b["rng_mid"])
    )

def _cond_Z6(b: dict) -> pd.Series:
    """
    Z6 — Evening Star: small prev body, then strong bear bar closing below prev open.
    """
    prev_rng = b["prev_rng"].replace(0, np.nan)
    rng      = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["prev_body"] / prev_rng <= 0.30)
        & (b["body"] / rng >= 0.60)
        & (b["c"] < b["prev_o"])
    )

def _cond_Z8(b: dict) -> pd.Series:
    """
    Z8 — Bearish Harami.
    Current bar fully inside prior large bull bar body.
    (Only fires when NO other Z signal — enforced in resolver.)
    """
    return (
        b["prev_bull"]
        & (b["prev_body"] > b["body"])
        & (b["body_top"] < b["prev_body_top"])
        & (b["body_bottom"] > b["prev_body_bottom"])
        & (b["prev_body"] / b["prev_rng"].replace(0, np.nan) >= 0.60)
    )

def _cond_Z9(b: dict) -> pd.Series:
    """
    Z9 — Tweezer Top.
    Two consecutive bars with matching highs (within 0.1 %).
    """
    high_diff = (b["h"] - b["prev_h"]).abs()
    return (
        b["bear_bar"]
        & b["prev_bull"]
        & (high_diff / b["prev_h"].replace(0, np.nan) <= 0.001)
    )

def _cond_Z10(b: dict) -> pd.Series:
    """
    Z10 — Three-bar bearish reversal: two prior bulls + current bear
    closing below the first of the two prior bars' open.
    """
    prev2_c = b["c"].shift(2)
    prev2_o = b["o"].shift(2)
    prev2_bull = prev2_c > prev2_o
    return (
        b["bear_bar"]
        & b["prev_bull"]
        & prev2_bull
        & (b["c"] < prev2_o)
    )

def _cond_Z11(b: dict) -> pd.Series:
    """Z11 — Bearish Abandoned Baby / gap-down doji (gap below prev low)."""
    prev_rng = b["prev_rng"].replace(0, np.nan)
    return (
        b["prev_bull"]
        & (b["prev_body"] / prev_rng >= 0.50)
        & (b["o"] < b["prev_l"])
        & (b["c"] < b["prev_l"])
    )

def _cond_Z12(b: dict) -> pd.Series:
    """
    Z12 — Weak bearish bar: bear bar with body < 30 % of range and
    close in lower half of range (mild exhaustion).
    """
    rng = b["rng"].replace(0, np.nan)
    return (
        b["bear_bar"]
        & (b["body"] / rng <= 0.30)
        & (b["c"] < b["rng_mid"])
    )

# ---------------------------------------------------------------------------
# Signal resolution — priority + special rules
# ---------------------------------------------------------------------------

# Maps signal id → condition function
_BULL_FUNCS: dict[int, callable] = {
    T1G: _cond_T1G,
    T1:  _cond_T1,
    T2G: _cond_T2G,
    T2:  _cond_T2,
    T3:  _cond_T3,
    T4:  _cond_T4,
    T5:  _cond_T5,
    T6:  _cond_T6,
    T9:  _cond_T9,
    T10: _cond_T10,
    T11: _cond_T11,
}

_BEAR_FUNCS: dict[int, callable] = {
    Z1G: _cond_Z1G,
    Z1:  _cond_Z1,
    Z2G: _cond_Z2G,
    Z2:  _cond_Z2,
    Z3:  _cond_Z3,
    Z4:  _cond_Z4,
    Z5:  _cond_Z5,
    Z6:  _cond_Z6,
    Z8:  _cond_Z8,
    Z9:  _cond_Z9,
    Z10: _cond_Z10,
    Z11: _cond_Z11,
    Z12: _cond_Z12,
}

def _resolve_signals(
    b: dict,
    doji: pd.Series,
    index: pd.Index,
) -> pd.Series:
    """
    Apply priority rules and return a Series of signal IDs (one per bar).

    Rules:
    1. Evaluate ALL bullish conditions.
    2. If any bullish fires → pick highest priority → skip bearish.
    3. If no bullish → evaluate bearish (excluding Z7/Z8 for now).
    4. Z8 only if no other Z fired.
    5. Z7 (doji) only if no T and no other Z fired.
    """
    n = len(index)
    result = np.zeros(n, dtype=np.int8)

    # --- Bullish evaluation (vectorized across all bars) ---
    # Build a (n, len(BULL_PRIORITY)) boolean matrix
    bull_matrix: dict[int, np.ndarray] = {}
    for sig in BULL_PRIORITY:
        func = _BULL_FUNCS[sig]
        cond = func(b)
        bull_matrix[sig] = np.asarray(cond.fillna(False), dtype=bool)

    # For each bar: pick highest priority bullish signal
    bull_result = np.zeros(n, dtype=np.int8)
    for sig in BULL_PRIORITY:           # highest → lowest
        mask = bull_matrix[sig] & (bull_result == 0)
        bull_result[mask] = sig

    # --- Bearish evaluation (only for bars with no bullish signal) ---
    bear_matrix: dict[int, np.ndarray] = {}
    for sig in BEAR_PRIORITY:
        if sig == Z7:                   # handled separately
            continue
        func = _BEAR_FUNCS[sig]
        cond = func(b)
        bear_matrix[sig] = np.asarray(cond.fillna(False), dtype=bool)

    bear_result = np.zeros(n, dtype=np.int8)
    for sig in BEAR_PRIORITY:
        if sig == Z7:
            continue
        if sig == Z8:
            # Z8 fires only if NO other Z has been assigned yet
            mask = (
                bear_matrix[sig]
                & (bear_result == 0)
                & (bull_result == 0)
            )
        else:
            mask = bear_matrix[sig] & (bear_result == 0) & (bull_result == 0)
        bear_result[mask] = sig

    # --- Z7 doji: only if nothing else assigned ---
    doji_arr = np.asarray(doji.fillna(False), dtype=bool)
    doji_mask = doji_arr & (bull_result == 0) & (bear_result == 0)
    bear_result[doji_mask] = Z7

    # --- Merge ---
    result = np.where(bull_result != 0, bull_result, bear_result).astype(np.int8)

    return pd.Series(result, index=index, name="sig_id", dtype=np.int8)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute T/Z signals for every bar in *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Must have columns: open, high, low, close (case-insensitive).
        Index should be a DatetimeIndex (or any ordered index).

    Returns
    -------
    pd.DataFrame with columns:
        sig_id   : int8  — signal ID (0-25)
        sig_name : str   — signal name string
        bc       : bool  — True if bullish (T) signal
        zc       : bool  — True if bearish (Z) signal
    """
    df = _normalise_columns(df)

    b    = _components(df)
    doji = _is_doji(b)

    sig_id   = _resolve_signals(b, doji, df.index)
    sig_name = sig_id.map(SIG_NAMES).fillna("NONE")
    bc       = sig_id.isin(BULLISH_SIGS)
    zc       = sig_id.isin(BEARISH_SIGS)

    return pd.DataFrame(
        {"sig_id": sig_id, "sig_name": sig_name, "bc": bc, "zc": zc},
        index=df.index,
    )

def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case column names; raise if OHLC are missing."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    required = {"open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing columns: {missing}")
    return df

# ---------------------------------------------------------------------------
# ok3 helper  (used by scanner)
# ---------------------------------------------------------------------------

def ok3(sig_series: pd.Series) -> pd.Series:
    """
    Returns a boolean Series: True where the *last 3 bars* of a window
    contain at least one non-NONE signal.

    For scanner use: call on a rolling window or the full result series.
    Equivalent to: any(sig[-3:] != NONE).
    """
    non_zero = (sig_series != NONE).astype(int)
    return non_zero.rolling(3, min_periods=1).sum() > 0
