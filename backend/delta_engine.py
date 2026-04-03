"""
delta_engine.py — Order-flow / footprint approximation (Pine 260403_Delta_New).

Volume decomposition:
    buyVol  = volume × (close − low)  / (high − low)   ← aggressive buyers
    sellVol = volume × (high − close) / (high − low)   ← aggressive sellers
    delta   = buyVol − sellVol

Bullish signals (all returned as bool Series):
    strong_bull  — N consecutive bars with ask imbalance + positive delta
    absorb_bull  — high volume + tiny body + positive delta  (absorption)
    div_bull     — new price low but delta positive           (bear trap)
    cd_bull      — red candle but delta positive              (CD conflict ↑)
    surge_bull   — |delta| > |delta[1]| × mult1              (Δ acceleration)
    blast_bull   — |delta| > |delta[1]| × mult2              (ΔΔ blast)

Bear variants: strong_bear, absorb_bear, div_bear, cd_bear, surge_bear, blast_bear
"""
from __future__ import annotations

import pandas as pd


def compute_delta(
    df: pd.DataFrame,
    imb_ratio:    float = 3.0,
    stack_len:    int   = 3,
    abs_vol_mult: float = 1.5,
    abs_body_pct: float = 0.30,
    delta_mult1:  float = 2.0,
    delta_mult2:  float = 4.0,
    div_len:      int   = 3,
) -> pd.DataFrame:
    """Return DataFrame with 13 columns (delta + 12 signal booleans)."""
    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]
    v = df["volume"]

    range_nz = (h - l).replace(0, 1e-4)
    buy_vol  = v * (c - l) / range_nz
    sell_vol = v * (h - c) / range_nz
    delta    = buy_vol - sell_vol

    # ── Imbalance stacks (vectorised cumulative counter) ──────────────────
    ask_imb = buy_vol  > sell_vol * imb_ratio
    bid_imb = sell_vol > buy_vol  * imb_ratio

    # Each time the condition breaks, start a new group; sum within group
    ask_grp   = (~ask_imb).cumsum()
    bid_grp   = (~bid_imb).cumsum()
    ask_stack = ask_imb.groupby(ask_grp).cumsum().where(ask_imb, 0)
    bid_stack = bid_imb.groupby(bid_grp).cumsum().where(bid_imb, 0)

    strong_bull = (ask_stack >= stack_len) & (delta > 0)
    strong_bear = (bid_stack >= stack_len) & (delta < 0)

    # ── Absorption ────────────────────────────────────────────────────────
    avg_vol    = v.rolling(20, min_periods=5).mean()
    body_size  = (c - o).abs()
    high_vol   = v > avg_vol * abs_vol_mult
    small_body = body_size / range_nz < abs_body_pct
    absorption = high_vol & small_body

    absorb_bull = absorption & (delta > 0)
    absorb_bear = absorption & (delta < 0)

    # ── Divergence / trap signals ─────────────────────────────────────────
    highest_h1 = h.rolling(div_len).max().shift(1)
    lowest_l1  = l.rolling(div_len).min().shift(1)

    price_hh = h > highest_h1
    price_ll = l < lowest_l1

    div_bear = price_hh & (delta < 0) & (c > o)   # bull trap
    div_bull = price_ll & (delta > 0) & (c < o)   # bear trap

    # Candle-vs-delta conflict (exclude where trap already fires)
    cd_bear = (c > o) & (delta < 0) & ~div_bear
    cd_bull = (c < o) & (delta > 0) & ~div_bull

    # ── Delta surge / blast ───────────────────────────────────────────────
    abs_d  = delta.abs()
    abs_d1 = abs_d.shift(1).fillna(0)

    blast_bull = (delta > 0) & (abs_d > abs_d1 * delta_mult2)
    blast_bear = (delta < 0) & (abs_d > abs_d1 * delta_mult2)
    surge_bull = (delta > 0) & (abs_d > abs_d1 * delta_mult1) & ~blast_bull
    surge_bear = (delta < 0) & (abs_d > abs_d1 * delta_mult1) & ~blast_bear

    return pd.DataFrame({
        "delta":        delta.round(0),
        "strong_bull":  strong_bull,
        "strong_bear":  strong_bear,
        "absorb_bull":  absorb_bull,
        "absorb_bear":  absorb_bear,
        "div_bull":     div_bull,
        "div_bear":     div_bear,
        "cd_bull":      cd_bull,
        "cd_bear":      cd_bear,
        "surge_bull":   surge_bull,
        "surge_bear":   surge_bear,
        "blast_bull":   blast_bull,
        "blast_bear":   blast_bear,
    }, index=df.index)
