"""Vectorized computation of TZ/WLNBB signals over a DataFrame of OHLCV bars."""
import pandas as pd
import numpy as np
from .signal_logic import compute_tz_wlnbb_for_bar
from .config import WLNBB_MA_PERIOD, USE_WICK, MIN_BODY_RATIO, DOJI_THRESH


def _compute_psar(high_arr, low_arr, start=0.02, inc=0.02, max_af=0.2):
    """Iterative Parabolic SAR matching Pine ta.sar(start, inc, max)."""
    n = len(high_arr)
    sar = [0.0] * n
    if n == 0:
        return sar
    bull = True
    af = start
    ep = low_arr[0]
    sar[0] = high_arr[0]
    for i in range(1, n):
        prev_sar = sar[i - 1]
        if bull:
            new_sar = prev_sar + af * (ep - prev_sar)
            if i >= 2:
                new_sar = min(new_sar, low_arr[i - 1], low_arr[i - 2])
            else:
                new_sar = min(new_sar, low_arr[i - 1])
            if low_arr[i] < new_sar:
                bull = False
                new_sar = ep
                ep = low_arr[i]
                af = start
            else:
                if high_arr[i] > ep:
                    ep = high_arr[i]
                    af = min(af + inc, max_af)
        else:
            new_sar = prev_sar + af * (ep - prev_sar)
            if i >= 2:
                new_sar = max(new_sar, high_arr[i - 1], high_arr[i - 2])
            else:
                new_sar = max(new_sar, high_arr[i - 1])
            if high_arr[i] > new_sar:
                bull = True
                new_sar = ep
                ep = high_arr[i]
                af = start
            else:
                if low_arr[i] < ep:
                    ep = low_arr[i]
                    af = min(af + inc, max_af)
        sar[i] = new_sar
    return sar


def compute_line5(
    df: pd.DataFrame,
    wvf_lookback: int = 22,
    wvf_sdev_len: int = 20,
    wvf_sdev_mult: float = 2.0,
    wvf_range_len: int = 50,
    wvf_range_pct: float = 0.85,
    psar_start: float = 0.02,
    psar_inc: float = 0.02,
    psar_max: float = 0.2,
    rsi2_low: float = 20.0,
    rsi2_high: float = 80.0,
) -> pd.DataFrame:
    """Add `bar_line5` column — Pine 260521 line-5: VIX-Fix / PSAR / RSI2."""
    c = df["close"]
    h = df["high"]
    l = df["low"]

    # WVF
    hc = c.rolling(wvf_lookback, min_periods=1).max()
    wvf = (hc - l) / hc.replace(0, np.nan) * 100.0
    wvf_upper = (wvf.rolling(wvf_sdev_len, min_periods=1).mean()
                 + wvf_sdev_mult * wvf.rolling(wvf_sdev_len, min_periods=1).std(ddof=0))
    wvf_range = wvf.rolling(wvf_range_len, min_periods=1).max() * wvf_range_pct

    # PSAR
    psar_arr = _compute_psar(h.tolist(), l.tolist(), psar_start, psar_inc, psar_max)

    # RSI(2) — Wilder smoothing (alpha=0.5 for period=2)
    delta = c.diff()
    up = delta.clip(lower=0)
    dn = (-delta).clip(lower=0)
    rs_up = up.ewm(alpha=0.5, adjust=False).mean()
    rs_dn = dn.ewm(alpha=0.5, adjust=False).mean()
    rsi2 = 100.0 - 100.0 / (1.0 + rs_up / rs_dn.replace(0, np.nan))
    rsi2 = rsi2.fillna(50.0)

    tokens = []
    n = len(df)
    for i in range(n):
        wvf_v = wvf.iloc[i] if not pd.isna(wvf.iloc[i]) else 0.0
        wvf_u = wvf_upper.iloc[i] if not pd.isna(wvf_upper.iloc[i]) else float("inf")
        wvf_r = wvf_range.iloc[i] if not pd.isna(wvf_range.iloc[i]) else float("inf")

        if wvf_v >= wvf_u:
            vix_tok = "VX"
        elif wvf_v >= wvf_r:
            vix_tok = "VR"
        else:
            vix_tok = ""

        psar_tok = "PB" if float(c.iloc[i]) > psar_arr[i] else "PS"

        r2 = float(rsi2.iloc[i])
        r2_prev = float(rsi2.iloc[i - 1]) if i > 0 else 50.0
        if r2_prev < rsi2_low and r2 >= rsi2_low:
            rsi2_tok = "R2X"
        elif r2_prev > rsi2_high and r2 <= rsi2_high:
            rsi2_tok = "R2D"
        elif r2 < rsi2_low:
            rsi2_tok = "R2L"
        elif r2 > rsi2_high:
            rsi2_tok = "R2H"
        else:
            rsi2_tok = ""

        parts = [t for t in [vix_tok, psar_tok, rsi2_tok] if t]
        tokens.append("-".join(parts))

    df["bar_line5"] = tokens
    return df


def compute_emas(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMA columns to df. df must have 'close' column."""
    for p in [9, 20, 34, 50, 89, 200]:
        df[f"ema{p}"] = df["close"].ewm(span=p, adjust=False).mean()
    return df


def compute_wlnbb(df: pd.DataFrame, period: int = WLNBB_MA_PERIOD) -> pd.DataFrame:
    """Add WLNBB Bollinger Band columns (period, std=1) on volume."""
    vol = df["volume"].fillna(0.0)
    mid = vol.rolling(period).mean()
    std = vol.rolling(period).std(ddof=0)
    df["wlnbb_mid"] = mid
    df["wlnbb_up"]  = mid + std
    df["wlnbb_low"] = mid - std
    return df


def compute_atr_wilder(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Add `atr` column using Wilder smoothing (matches Pine `ta.atr`)."""
    prev_c = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_c).abs(),
        (df["low"]  - prev_c).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return df


def compute_signals_for_ticker(df: pd.DataFrame, universe: str = "sp500") -> pd.DataFrame:
    """
    Given a OHLCV DataFrame (sorted oldest-first) for a single ticker,
    compute all TZ/WLNBB signals and return an enriched DataFrame.
    Requires columns: open, high, low, close, volume.
    """
    df = df.copy().reset_index(drop=True)
    compute_emas(df)
    compute_wlnbb(df)
    compute_atr_wilder(df, period=14)
    compute_line5(df)

    results = []
    prev_is_doji = False

    for i in range(len(df)):
        if i == 0:
            results.append(_empty_result())
            prev_is_doji = False
            continue

        row = df.iloc[i]
        prev = df.iloc[i - 1]

        if pd.isna(row["close"]) or pd.isna(prev["close"]):
            results.append(_empty_result())
            prev_is_doji = False
            continue

        r = compute_tz_wlnbb_for_bar(
            o=float(row["open"]), h=float(row["high"]),
            l=float(row["low"]), c=float(row["close"]),
            v=float(row.get("volume", 0) or 0),
            prev_o=float(prev["open"]), prev_h=float(prev["high"]),
            prev_l=float(prev["low"]), prev_c=float(prev["close"]),
            prev_v=float(prev.get("volume", 0) or 0),
            ema9=float(row["ema9"]), ema20=float(row["ema20"]),
            ema34=float(row["ema34"]), ema50=float(row["ema50"]),
            ema89=float(row["ema89"]), ema200=float(row["ema200"]),
            vol_mid=float(row["wlnbb_mid"]) if not pd.isna(row["wlnbb_mid"]) else 0.0,
            vol_up=float(row["wlnbb_up"])   if not pd.isna(row["wlnbb_up"])  else 0.0,
            vol_low=float(row["wlnbb_low"]) if not pd.isna(row["wlnbb_low"]) else 0.0,
            prev_vol_mid=float(prev["wlnbb_mid"]) if not pd.isna(prev["wlnbb_mid"]) else 0.0,
            prev_vol_up=float(prev["wlnbb_up"])   if not pd.isna(prev["wlnbb_up"])  else 0.0,
            prev_vol_low=float(prev["wlnbb_low"]) if not pd.isna(prev["wlnbb_low"]) else 0.0,
            prev_is_doji=prev_is_doji,
            use_wick=USE_WICK, min_body_ratio=MIN_BODY_RATIO, doji_thresh=DOJI_THRESH,
            atr=float(row["atr"]) if not pd.isna(row.get("atr")) else 0.0,
            bar_line5=str(row.get("bar_line5") or ""),
        )
        prev_is_doji = r["is_doji"]
        results.append(r)

    result_df = pd.DataFrame(results)
    for col in result_df.columns:
        df[col] = result_df[col].values
    return df


def _empty_result() -> dict:
    """Return a zeroed-out result dict for the first bar or error cases."""
    return {
        "is_bull": False, "is_bear": False, "is_doji": False,
        "t_raw": set(), "z_raw": set(),
        "t_signal": "", "z_signal": "",
        "bull_priority_code": 0, "bear_priority_code": 0,
        "preup_signal": "", "predn_signal": "",
        "preup_raw": set(), "predn_raw": set(),
        "volume_bucket": "", "vol_down_adapted": False, "vol_up_adapted": False,
        "l1_raw": False, "l2_raw": False, "l3_raw": False,
        "l4_raw": False, "l5_raw": False, "l6_raw": False,
        "l34_active": False, "l43_active": False, "l64_active": False, "l22_active": False,
        "l_digits": "", "l_signal": "",
        "ne_suffix": "", "wick_suffix": "",
        "penetration_suffix": "",
        "close_suffix": "", "close_appended": False,
        "close_above_prev_body": False, "close_below_prev_body": False,
        "wick_penetration_upper": False, "wick_penetration_lower": False, "wick_penetration_both": False,
        "wick_ext_up": False, "wick_ext_down": False, "wick_ext_both": False,
        "prev_body_top": 0.0, "prev_body_bot": 0.0, "prev_high": 0.0, "prev_low": 0.0,
        "composite_t_label": "", "composite_z_label": "",
        "composite_primary_label": "", "composite_all_labels": "",
        "composite_core": "", "composite_suffix": "",
        "composite_full_suffix": "", "composite_full_label": "",
        "lane1_label": "", "lane3_label": "",
        "has_t_signal": False, "has_z_signal": False, "has_l_signal": False,
        "has_preup": False, "has_predn": False,
        "has_tz_l_combo": False, "has_bullish_context": False, "has_bearish_context": False,
        "bar_body_wick": "", "bar_gap_range": "", "bar_line5": "",
    }
