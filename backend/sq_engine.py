"""
sq_engine.py — VSA-style signals from Pine Script 260312.
SQ  = Effort / Stopping Volume  (high vol + narrow spread)
NS  = No Supply                 (low vol + narrow/inside + down close + CLV high)
ND  = No Demand                 (low vol + narrow/inside + up close + CLV low)
3UP = Effort→Test→Confirm Up
3DN = Effort→Test→Confirm Down
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_sq(
    df: pd.DataFrame,
    vol_len: int = 20,
    hi_vol_mult: float = 1.6,
    lo_vol_mult: float = 0.80,
    atr_len: int = 14,
    narrow_mult: float = 1.0,
    clv_high: float = 0.70,
    clv_low: float = 0.30,
    test_n: int = 3,
) -> pd.DataFrame:
    """
    Compute VSA signals.  Input df must have lowercase OHLCV columns.
    Returns DataFrame with boolean columns: SQ, NS, ND, SIG3_UP, SIG3_DN.
    """
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]
    v = df.get("volume", pd.Series(0.0, index=df.index)).fillna(0)

    # ── Volume MA (Wilder RMA = EWM alpha=1/n) ────────────────────────────
    vol_ma = v.ewm(alpha=1 / vol_len, adjust=False).mean()
    hi_vol = v >= vol_ma * hi_vol_mult
    lo_vol = v <= vol_ma * lo_vol_mult

    # ── Spread / ATR ──────────────────────────────────────────────────────
    spread = h - l
    hl   = spread
    hpc  = (h - c.shift(1)).abs()
    lpc  = (l - c.shift(1)).abs()
    tr   = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    atr  = tr.ewm(alpha=1 / atr_len, adjust=False).mean()

    narrow     = (spread > 0) & (spread < atr * narrow_mult)
    inside_bar = (h <= h.shift(1)) & (l >= l.shift(1))

    # ── Close Location Value ∈ [0, 1] ────────────────────────────────────
    clv = ((c - l) / spread).where(spread > 0, other=0.5)

    up_bar = c > c.shift(1)
    dn_bar = c < c.shift(1)

    # ── Core signals ──────────────────────────────────────────────────────
    SQ = hi_vol & narrow

    NS = lo_vol & (narrow | inside_bar) & dn_bar & (clv >= clv_high)

    ND = lo_vol & (narrow | inside_bar) & up_bar  & (clv <= clv_low)

    # ── "3" composite ─────────────────────────────────────────────────────
    # Effort = SQ fired within last 2 bars
    effort_recent = SQ.shift(1).fillna(False) | SQ.shift(2).fillna(False)

    # Test = NS / ND or any low-vol narrow/inside candle in last test_n bars
    test_signal = NS | ND | (lo_vol & (narrow | inside_bar))
    test_recent = test_signal.rolling(test_n, min_periods=1).max().astype(bool)

    # Confirmation
    highest2  = h.shift(1).rolling(2, min_periods=1).max()
    lowest2   = l.shift(1).rolling(2, min_periods=1).min()
    confirm_up = (c > highest2)  & (clv >= 0.55)
    confirm_dn = (c < lowest2)   & (clv <= 0.45)

    SIG3_UP = effort_recent & test_recent & confirm_up
    SIG3_DN = effort_recent & test_recent & confirm_dn

    return pd.DataFrame(
        {
            "SQ":      SQ,
            "NS":      NS,
            "ND":      ND,
            "SIG3_UP": SIG3_UP,
            "SIG3_DN": SIG3_DN,
        },
        index=df.index,
    )
