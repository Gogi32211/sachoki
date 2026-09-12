"""§5 PRIMARY RESEARCH TARGET — FROZEN 2026-09-11, BEFORE ANY FEATURE SELECTION.

    PARA_T20_ATR3

        y[t] = 1   iff   ( max(high[t+1 .. t+20]) - close[t] ) / atr_14[t]  >=  3.0
        y[t] = 0   otherwise
        y[t] = UNDEFINED (row dropped) when the ticker has fewer than 20 sessions after t,
                          or atr_14[t] is missing or <= 0

EVERY DETAIL, SPELLED OUT so it cannot drift:

  · MAXIMUM FAVOURABLE EXCURSION, not a closing return. The question is "did the stock offer a
    3-ATR move within 20 sessions", which is the tradeable event; a close-to-close return would
    punish a setup that ran and gave it back.
  · The window is STRICTLY FORWARD and inclusive: sessions t+1 through t+20. Bar t itself is
    never part of its own label.
  · ATR is Wilder-14 on the same de-duplicated daily series, taken AT t (known at t's close).
    Verified point-in-time on 2026-09-11: identical under truncation at four dates on NVDA, and
    a median relative difference of 0.0000 % against a clean Wilder-14 recomputation.
  · The ATR is NOT separately normalised by price. The ratio IS the normalisation — that is the
    whole point of a volatility-normalised target, and dividing twice would re-introduce the
    price effect the book already measured ([[project_fib_price_zones]]: win rate rises with
    price, so an un-normalised target quietly becomes a price factor).
  · Sessions, not calendar days. Holidays and halts shorten the calendar window, never the
    session count.
  · The last 20 sessions of every ticker are UNLABELLED and dropped. They are not zeros.

  THRESHOLD 3.0 IS FROZEN. It comes from the brief's prior formulation (fwd20 / ATR >= 3), not
  from anything measured here. It was chosen before any feature was looked at and is not tuned.
  Secondary horizons (5D / 10D / 40D) and secondary thresholds may be REPORTED afterwards but may
  never be used to choose the primary model.

WHY A SEPARATE MODULE. So the target has one definition with one digest, and every downstream
result can name which one it used. Nothing here reads a stored fwd_/mfe_ column: those are exactly
the fields the leakage audit rejected, and re-deriving the label from raw OHLC keeps the target
independent of the enricher's own forward machinery.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

TARGET_ID = "PARA_T20_ATR3"
HORIZON = 20          # forward sessions, t+1 .. t+HORIZON
ATR_N = 14            # Wilder
THRESHOLD = 3.0       # ATR multiples of maximum favourable excursion


def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = ATR_N) -> np.ndarray:
    """Wilder's ATR, seeded on the first n true ranges. NaN until bar n."""
    pc = np.r_[np.nan, close[:-1]]
    tr = np.nanmax(np.c_[high - low, np.abs(high - pc), np.abs(low - pc)], axis=1)
    a = np.full(len(tr), np.nan)
    if len(tr) <= n:
        return a
    a[n] = np.nanmean(tr[1:n + 1])
    for i in range(n + 1, len(tr)):
        a[i] = (a[i - 1] * (n - 1) + tr[i]) / n
    return a


def label(df: pd.DataFrame, atr_col: str | None = "atr_14") -> pd.DataFrame:
    """Add `mfe_atr` and `y` to a frame sorted by (ticker, date).

    `atr_col` names a point-in-time ATR already on the frame; pass None to recompute Wilder-14
    here. Rows without a full forward window, or without a usable ATR, get y = NaN and must be
    DROPPED, never treated as negatives.
    """
    d = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    n = len(d)
    high = d["high"].to_numpy(float)
    close = d["close"].to_numpy(float)

    if atr_col and atr_col in d.columns:
        atr = d[atr_col].to_numpy(float)
    else:
        atr = np.full(n, np.nan)

    fmax = np.full(n, np.nan)
    start = 0
    for _tk, cnt in d.groupby("ticker", sort=False).size().items():
        a, b = start, start + cnt
        start = b
        h = high[a:b]
        if atr_col is None or atr_col not in d.columns:
            atr[a:b] = wilder_atr(h, d["low"].to_numpy(float)[a:b], close[a:b])
        if cnt <= HORIZON:
            continue
        # rolling max of the NEXT HORIZON highs: reverse-rolling then shift back by one bar
        fwd = (pd.Series(h[::-1]).rolling(HORIZON, min_periods=HORIZON).max().to_numpy()[::-1])
        out = np.full(cnt, np.nan)
        out[:cnt - HORIZON] = fwd[HORIZON:]          # row i gets max(h[i+1 .. i+HORIZON])
        fmax[a:b] = out

    with np.errstate(invalid="ignore", divide="ignore"):
        ok = np.isfinite(fmax) & np.isfinite(atr) & (atr > 0)
        mfe_atr = np.where(ok, (fmax - close) / np.where(atr > 0, atr, np.nan), np.nan)
    d["atr_pit"] = atr
    d["mfe_atr"] = mfe_atr
    d["y"] = np.where(np.isfinite(mfe_atr), (mfe_atr >= THRESHOLD).astype(float), np.nan)
    return d


def spec() -> dict:
    return dict(target_id=TARGET_ID, horizon_sessions=HORIZON, atr="Wilder", atr_n=ATR_N,
                threshold_atr=THRESHOLD,
                formula="( max(high[t+1..t+20]) - close[t] ) / atr_14[t] >= 3.0",
                excursion="MAXIMUM FAVOURABLE (high), not closing return",
                window="strictly forward, sessions t+1..t+20 inclusive",
                atr_normalised_by_price=False,
                undefined="fewer than 20 forward sessions, or atr <= 0 / missing — DROPPED, not 0",
                frozen_at="2026-09-11, before any feature selection",
                source="derived from raw OHLC; never reads a stored fwd_/mfe_ column")
