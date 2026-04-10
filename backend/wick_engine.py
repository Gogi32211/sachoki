"""
wick_engine.py — 2-Candle Opposite-Wick Reversal (Pine Script 3112_2C).

comboSignal        = the 2-bar opposite-wick pattern is present
WICK_BULL_PATTERN  = bullish-bias pattern (big lower wick on current bar)
WICK_BEAR_PATTERN  = bearish-bias pattern (big upper wick on current bar)
WICK_BULL_CONFIRM  = pattern confirmed bullish (close > pattern high within N bars)
WICK_BEAR_CONFIRM  = pattern confirmed bearish (close < pattern low within N bars)
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_wick(
    df: pd.DataFrame,
    wick_dom_pct: float = 0.40,
    min_wick_pct: float = 0.00,
    max_body_pct_each: float = 0.55,
    atr_len: int = 14,
    min_atr_mul: float = 0.10,
    confirm_bars: int = 3,
    confirm_mode: str = "Breakout",
) -> pd.DataFrame:
    """
    Detects 2-candle opposite-wick patterns and their confirmation.
    Input df must have lowercase OHLCV columns.
    """
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    o = df["open"];  h = df["high"]; l = df["low"]; c = df["close"]
    o1 = o.shift(1); h1 = h.shift(1); l1 = l.shift(1); c1 = c.shift(1)

    r0 = h - l
    r1 = h1 - l1
    b0 = (c - o).abs()
    b1 = (c1 - o1).abs()

    # Wick sizes
    hi_body0 = pd.concat([o, c],   axis=1).max(axis=1)
    lo_body0 = pd.concat([o, c],   axis=1).min(axis=1)
    hi_body1 = pd.concat([o1, c1], axis=1).max(axis=1)
    lo_body1 = pd.concat([o1, c1], axis=1).min(axis=1)

    u0 = h  - hi_body0;  d0 = lo_body0 - l
    u1 = h1 - hi_body1;  d1 = lo_body1 - l1

    body_pct0 = (b0 / r0).where(r0 > 0, other=1.0)
    body_pct1 = (b1 / r1).where(r1 > 0, other=1.0)
    up_pct0   = (u0 / r0).where(r0 > 0, other=0.0)
    up_pct1   = (u1 / r1).where(r1 > 0, other=0.0)
    lo_pct0   = (d0 / r0).where(r0 > 0, other=0.0)
    lo_pct1   = (d1 / r1).where(r1 > 0, other=0.0)

    each_body_ok = (body_pct0 <= max_body_pct_each) & (body_pct1 <= max_body_pct_each)

    # Opposite dominant wick pairs
    prev_lower_dom = (lo_pct1 >= wick_dom_pct) & (up_pct1 >= min_wick_pct)
    cur_upper_dom  = (up_pct0 >= wick_dom_pct) & (lo_pct0 >= min_wick_pct)
    prev_upper_dom = (up_pct1 >= wick_dom_pct) & (lo_pct1 >= min_wick_pct)
    cur_lower_dom  = (lo_pct0 >= wick_dom_pct) & (up_pct0 >= min_wick_pct)

    opposite_wicks = (prev_lower_dom & cur_upper_dom) | (prev_upper_dom & cur_lower_dom)

    # Combined 2-bar range
    combo_high  = pd.concat([h1, h], axis=1).max(axis=1)
    combo_low   = pd.concat([l1, l], axis=1).min(axis=1)
    combo_range = combo_high - combo_low

    # ATR (Wilder)
    tr  = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / atr_len, adjust=False).mean()

    range_ok     = combo_range >= atr * min_atr_mul
    combo_signal = range_ok & each_body_ok & opposite_wicks

    # Direction of pattern
    wick_bull_pattern = combo_signal & cur_lower_dom   # big lower wick → expect up
    wick_bear_pattern = combo_signal & cur_upper_dom   # big upper wick → expect down

    # Confirmation: look back up to confirm_bars for a recent pattern
    bars_since_pat = _bars_since(combo_signal)
    in_window      = (bars_since_pat >= 1) & (bars_since_pat <= confirm_bars)

    combo_hi_mem = combo_high.where(combo_signal).ffill()
    combo_lo_mem = combo_low.where(combo_signal).ffill()

    if confirm_mode == "Breakout":
        WICK_BULL_CONFIRM = in_window & (c > combo_hi_mem)
        WICK_BEAR_CONFIRM = in_window & (c < combo_lo_mem)
    else:
        combo_mid = (combo_hi_mem + combo_lo_mem) / 2
        WICK_BULL_CONFIRM = in_window & (c > combo_mid) & (c > o)
        WICK_BEAR_CONFIRM = in_window & (c < combo_mid) & (c < o)

    return pd.DataFrame(
        {
            "WICK_PATTERN":      combo_signal,
            "WICK_BULL_PATTERN": wick_bull_pattern,
            "WICK_BEAR_PATTERN": wick_bear_pattern,
            "WICK_BULL_CONFIRM": WICK_BULL_CONFIRM,
            "WICK_BEAR_CONFIRM": WICK_BEAR_CONFIRM,
        },
        index=df.index,
    )


# ── 260402_WICK — X2G / X1X / X3 signals ─────────────────────────────────────

def compute_wick_x(df: pd.DataFrame, wick_mult: float = 2.0) -> pd.DataFrame:
    """
    Wick-filtered T2G/T2/T1G/T1 reversal signals from Pine Script 260402_WICK.

    X2G_WICK  — T2G or T2 base (prev bull continuation) confirmed by wick shape
    X1X_WICK  — T1G or T1 base (prev bear reversal) confirmed by wick on bar[1]
    X3_WICK   — any bullish bar whose current wick shape + prior wick shape align
                (only fires when X2G and X1X are absent)

    All use wickMult=2.0: dominant wick must be ≥ 2× the opposite wick.
    """
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    o = df["open"].values.astype(float)
    h = df["high"].values.astype(float)
    l = df["low"].values.astype(float)
    c = df["close"].values.astype(float)
    n = len(df)
    mt = 1e-10  # mintick substitute (syminfo.mintick)

    def upper_wick(h_, o_, c_):
        return h_ - np.maximum(o_, c_)

    def lower_wick(o_, c_, l_):
        return np.minimum(o_, c_) - l_

    # current bar wicks
    uw0 = upper_wick(h,       o,       c)
    lw0 = lower_wick(o,       c,       l)

    # bar[1] wicks
    h1 = np.roll(h, 1); o1 = np.roll(o, 1); c1 = np.roll(c, 1); l1 = np.roll(l, 1)
    uw1 = upper_wick(h1, o1, c1)
    lw1 = lower_wick(o1, c1, l1)

    # bar[2] wicks
    h2 = np.roll(h, 2); o2 = np.roll(o, 2); c2 = np.roll(c, 2); l2 = np.roll(l, 2)
    uw2 = upper_wick(h2, o2, c2)
    lw2 = lower_wick(o2, c2, l2)

    # ── Wick conditions ──────────────────────────────────────────────────────
    # wickCond_prev  : bar[1] bearish-wick shape (upper dominant)
    lw1_safe = np.maximum(lw1, mt)
    wc_prev  = (lw1 == 0) | (uw1 >= wick_mult * lw1_safe)

    # wickCond_prev_bull : bar[1] bullish-wick shape (lower dominant)
    uw1_safe     = np.maximum(uw1, mt)
    wc_prev_bull = (uw1 == 0) | (lw1 >= wick_mult * uw1_safe)

    # wickCond_prev2 : bar[2] bearish-wick shape
    lw2_safe = np.maximum(lw2, mt)
    wc_prev2 = (lw2 == 0) | (uw2 >= wick_mult * lw2_safe)

    # wickCond_curr : current bar bullish-wick shape (lower dominant)
    uw0_safe = np.maximum(uw0, mt)
    wc_curr  = (uw0 == 0) | (lw0 >= wick_mult * uw0_safe)

    # ── Candle direction ─────────────────────────────────────────────────────
    is_bull  = c  > o
    is_bull1 = c1 > o1
    is_bear1 = c1 < o1

    # ── X2G / X2 base logic (prev bar bullish, continuation) ─────────────────
    x2g_raw = is_bull1 & (o >= o1) & (o >  c1) & (c > c1) & is_bull
    x2_raw  = is_bull1 & (o >= o1) & (o <= c1) & (c > c1) & is_bull

    # ── X1G / X1 base logic (prev bar bearish, reversal) ─────────────────────
    x1g_raw = is_bear1 & (o > c1) & (o > o1) & (c > o1) & is_bull
    x1_raw  = is_bear1 & (o >= c1) & (o1 >= o) & (c > o1) & is_bull

    # ── Final signals ────────────────────────────────────────────────────────
    x2g_wick = ((x2g_raw | x2_raw) & wc_prev & wc_curr) | (x2g_raw & wc_prev_bull & wc_curr)
    x1x_wick = (x1g_raw | x1_raw) & wc_prev
    x3_wick  = wc_curr & (wc_prev | wc_prev2) & is_bull

    # Zero out first 2 bars (roll artifacts)
    for arr in (x2g_wick, x1x_wick, x3_wick):
        arr[:2] = False

    out = pd.DataFrame(index=df.index)
    out["x2g_wick"] = x2g_wick.astype(int)
    out["x1x_wick"] = x1x_wick.astype(int)
    # X3 only fires when neither X2G nor X1X fires (per Pine display logic)
    out["x3_wick"]  = (x3_wick & ~x2g_wick & ~x1x_wick).astype(int)
    return out


def _bars_since(cond: pd.Series) -> pd.Series:
    """Returns number of bars elapsed since last True; large int if never fired."""
    arr = cond.values
    out = np.full(len(arr), 9999, dtype=np.int32)
    last = -9999
    for i in range(len(arr)):
        if arr[i]:
            last = i
        out[i] = i - last
    return pd.Series(out, index=cond.index)
