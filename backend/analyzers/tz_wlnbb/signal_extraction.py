"""Vectorized computation of TZ/WLNBB signals over a DataFrame of OHLCV bars."""
import pandas as pd
import numpy as np
from .signal_logic import compute_tz_wlnbb_for_bar
from .config import (
    WLNBB_MA_PERIOD, USE_WICK, MIN_BODY_RATIO, DOJI_THRESH,
    AD_FRESH_LOOKBACK, AD_FRESH_POS_THR, AD_CLUSTER_WINDOW, AD_CLUSTER_MIN,
    WYC_ATR_COMP_MULT, WYC_ATR_AVG_PERIOD, WYC_VOL_MULT, WYC_TR_LOOKBACK,
    WYC_SPRING_VOL_MULT, WYC_SPRING_CLOSE_POS,
)


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


def compute_ad_fresh(df: pd.DataFrame, cfg: dict = None):
    """
    Compute AD-FRESH + AD-CLUSTER columns (Pine 260523).

    AD-FRESH = D_signal (T4|T6|T2G|T2) AND barssince(A_signal Z1G|Z2G) <= lookback
               AND (close - low20) / (high20 - low20) < pos_thr  (fresh = lower half)
    AD-CLUSTER = rolling count of AD-FRESH in last AD_CLUSTER_WINDOW >= AD_CLUSTER_MIN

    Requires `t_signal` and `z_signal` columns already populated.
    Returns (ad_fresh: bool Series, ad_cluster: bool Series).
    """
    cfg = cfg or {}
    lookback = int(cfg.get("AD_FRESH_LOOKBACK", AD_FRESH_LOOKBACK))
    pos_thr  = float(cfg.get("AD_FRESH_POS_THR", AD_FRESH_POS_THR))
    win      = int(cfg.get("AD_CLUSTER_WINDOW", AD_CLUSTER_WINDOW))
    min_cnt  = int(cfg.get("AD_CLUSTER_MIN", AD_CLUSTER_MIN))

    n = len(df)
    if n == 0:
        empty = pd.Series([], dtype=bool)
        return empty, empty

    t_col = df["t_signal"] if "t_signal" in df.columns else pd.Series([""] * n, index=df.index)
    z_col = df["z_signal"] if "z_signal" in df.columns else pd.Series([""] * n, index=df.index)

    a_sig = z_col.isin(["Z1G", "Z2G"]).to_numpy()
    d_sig = t_col.isin(["T4", "T6", "T2G", "T2"]).to_numpy()

    # barssince(A_signal): for each i, distance to most recent True at <= i
    bars_since = np.full(n, np.iinfo(np.int32).max, dtype=np.int32)
    last_idx = -1
    for i in range(n):
        if a_sig[i]:
            last_idx = i
        if last_idx >= 0:
            bars_since[i] = i - last_idx
    a_recent = bars_since <= lookback

    h20 = df["high"].rolling(20, min_periods=1).max()
    l20 = df["low"].rolling(20, min_periods=1).min()
    rng = (h20 - l20).clip(lower=1e-10)
    pos = (df["close"] - l20) / rng
    is_fresh = (pos < pos_thr).to_numpy()

    ad_fresh = d_sig & a_recent & is_fresh
    ad_fresh_ser = pd.Series(ad_fresh, index=df.index)

    # Bug-fix v3.1: AD-CLUSTER must AND with ad_fresh on the same bar.
    # Without the AND gate, the rolling-window count stays True for several
    # bars after the second AD-FRESH fires (cluster "persists"), inflating
    # the count from ~3.5% to ~12.8% of rows on SP500 1D.
    rolling_cnt = ad_fresh_ser.rolling(win, min_periods=1).sum()
    ad_cluster_ser = (rolling_cnt >= min_cnt) & ad_fresh_ser

    return ad_fresh_ser.fillna(False), ad_cluster_ser.fillna(False)


def compute_wyc_phase(df: pd.DataFrame, cfg: dict = None) -> pd.Series:
    """
    Compute Wyckoff macro phase column (Pine 260523).

    Values: SPRING | UTAD | SOS | ACC_TR | DIST_TR | MARKUP | MKDN | NEUTRAL

    Dual TR detection: ATR-based compression (atr14 < atr14_ema50 * mult)
    OR Fourier ratio (computed elsewhere — falls back to ATR-only here).

    Spring/UTAD require T/Z confirmation (consumes t_signal/z_signal cols).
    SOS requires ad_fresh + wvf_spike (consumes ad_fresh + bar_line5 cols).

    State machine: phase persists across bars (does not reset to NEUTRAL).
    """
    cfg = cfg or {}
    atr_mult   = float(cfg.get("WYC_ATR_COMP_MULT",  WYC_ATR_COMP_MULT))
    atr_period = int(cfg.get("WYC_ATR_AVG_PERIOD", WYC_ATR_AVG_PERIOD))
    vol_mult   = float(cfg.get("WYC_VOL_MULT",     WYC_VOL_MULT))
    tr_look    = int(cfg.get("WYC_TR_LOOKBACK",    WYC_TR_LOOKBACK))

    n = len(df)
    if n == 0:
        return pd.Series([], dtype=str)

    ema50  = df["close"].ewm(span=50,  adjust=False).mean()
    ema200 = df["close"].ewm(span=200, adjust=False).mean()

    if "atr" not in df.columns:
        compute_atr_wilder(df, period=14)
    atr14 = df["atr"]
    atr14_avg = atr14.ewm(span=atr_period, adjust=False).mean()
    in_tr = (atr14 < atr14_avg * atr_mult).to_numpy()

    macro_up   = (ema50 > ema200).to_numpy()
    macro_down = (ema50 < ema200).to_numpy()

    vol_avg = df["volume"].rolling(20, min_periods=1).mean()
    vol_hi  = (df["volume"] > vol_avg * vol_mult).to_numpy()

    # Bug-fix v3.1: Spring needs STRONGER constraints — over-fires on SP500 1D
    # with the loose 1.5× vol + 0.70 ATR mult, producing inverted edge
    # (avg_5d -2.07%, win 39%). Tightening: 2× vol, close in upper 60% of bar,
    # range > ATR (expansion, not compression).
    spring_vol_mult  = float((cfg or {}).get("WYC_SPRING_VOL_MULT",  WYC_SPRING_VOL_MULT))
    spring_close_pos = float((cfg or {}).get("WYC_SPRING_CLOSE_POS", WYC_SPRING_CLOSE_POS))
    vol_spike_strong = (df["volume"] > vol_avg * spring_vol_mult).to_numpy()

    prev_sup = df["low"].rolling(tr_look, min_periods=1).min().shift(1)
    prev_res = df["high"].rolling(tr_look, min_periods=1).max().shift(1)
    prev_sup_arr = prev_sup.to_numpy()
    prev_res_arr = prev_res.to_numpy()

    close_arr = df["close"].to_numpy()
    open_arr  = df["open"].to_numpy()
    high_arr  = df["high"].to_numpy()
    low_arr   = df["low"].to_numpy()
    is_bull   = close_arr > open_arr
    is_bear   = close_arr < open_arr

    bar_range = (df["high"] - df["low"]).clip(lower=1e-8)
    close_pos_bar = ((df["close"] - df["low"]) / bar_range).to_numpy()
    spring_close_ok = close_pos_bar > spring_close_pos
    range_expanded  = (bar_range.to_numpy() > atr14.to_numpy())

    t_col = df["t_signal"] if "t_signal" in df.columns else pd.Series([""] * n, index=df.index)
    z_col = df["z_signal"] if "z_signal" in df.columns else pd.Series([""] * n, index=df.index)
    t_bull_conf = t_col.isin(["T1G", "T4", "T9"]).to_numpy()
    z_bear_conf = z_col.isin(["Z1G", "Z4"]).to_numpy()

    ad_fresh_arr = (df["ad_fresh"].to_numpy() if "ad_fresh" in df.columns
                    else np.zeros(n, dtype=bool))
    # wvf_spike from bar_line5 token starting with "VX"
    if "bar_line5" in df.columns:
        wvf_arr = df["bar_line5"].astype(str).str.startswith("VX").to_numpy()
    else:
        wvf_arr = np.zeros(n, dtype=bool)

    # Per-bar event masks — Spring now uses tightened constraints
    spring_mask = (
        (low_arr < prev_sup_arr)
        & (close_arr > prev_sup_arr)
        & is_bull
        & macro_down
        & vol_spike_strong        # 2× avg volume (was 1.5×)
        & spring_close_ok         # close in upper 60% of bar
        & range_expanded          # bar range > ATR(14)
        & t_bull_conf             # T1G / T4 / T9 confirmation
    )
    utad_mask = (
        (high_arr > prev_res_arr)
        & (close_arr < prev_res_arr)
        & is_bear & macro_up & vol_hi & z_bear_conf
    )
    sos_mask = ad_fresh_arr & macro_down & wvf_arr

    # State machine: phase persists across bars
    phase = ["NEUTRAL"] * n
    current = "NEUTRAL"
    for i in range(n):
        if spring_mask[i]:
            current = "SPRING"
        elif utad_mask[i]:
            current = "UTAD"
        elif sos_mask[i]:
            current = "SOS"
        elif macro_down[i] and in_tr[i]:
            current = "ACC_TR"
        elif macro_up[i] and in_tr[i]:
            current = "DIST_TR"
        elif macro_up[i]:
            current = "MARKUP"
        elif macro_down[i]:
            current = "MKDN"
        phase[i] = current

    return pd.Series(phase, index=df.index, dtype=str)


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

    # ── 260523: AD-FRESH / AD-CLUSTER / WYC Phase (requires t_signal/z_signal) ─
    try:
        ad_fresh, ad_cluster = compute_ad_fresh(df)
        df["ad_fresh"]   = ad_fresh.astype(bool).values
        df["ad_cluster"] = ad_cluster.astype(bool).values
    except Exception:
        df["ad_fresh"]   = False
        df["ad_cluster"] = False

    try:
        wyc = compute_wyc_phase(df)
        df["wyc_phase"]  = wyc.values
        df["wyc_spring"] = (wyc == "SPRING").values
        df["wyc_sos"]    = (wyc == "SOS").values
        df["wyc_acc_tr"] = (wyc == "ACC_TR").values
        df["wyc_markup"] = (wyc == "MARKUP").values
    except Exception:
        df["wyc_phase"]  = "NEUTRAL"
        df["wyc_spring"] = False
        df["wyc_sos"]    = False
        df["wyc_acc_tr"] = False
        df["wyc_markup"] = False

    # ── 260523 v3.1: HH/LH/HL/LL swing classification ───────────────────────
    try:
        from .swing_classifier import classify_swings
        sw = classify_swings(df)
        df["swing_type"]    = sw["swing_type"].values
        df["swing_ret"]     = sw["swing_ret"].values
        df["swing_bars"]    = sw["swing_bars"].values
        df["is_pivot_high"] = sw["is_pivot_high"].values
        df["is_pivot_low"]  = sw["is_pivot_low"].values
    except Exception:
        df["swing_type"]    = ""
        df["swing_ret"]     = np.nan
        df["swing_bars"]    = np.nan
        df["is_pivot_high"] = False
        df["is_pivot_low"]  = False

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
