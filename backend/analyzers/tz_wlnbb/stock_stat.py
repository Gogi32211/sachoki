"""Generate stock_stat_tz_wlnbb CSV with forward returns and sequence context."""
import csv
import logging
import time
import os
from datetime import datetime
from typing import Optional, Callable, List, Tuple
import pandas as pd

from .config import TZ_WLNBB_VERSION
from .build_marker import BUILD_MARKER
from .signal_extraction import compute_signals_for_ticker

log = logging.getLogger(__name__)


# Columns that REQUIRE future bars to compute → must never be used in
# live scoring rules. Documented here as the canonical list.
LOOKAHEAD_COLUMNS = [
    "ret_1d", "ret_3d", "ret_5d", "ret_10d",
    "max_high_5d", "max_high_10d",
    "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
    "max_drawdown_5d", "max_drawdown_10d",
    "clean_win_5d", "big_win_10d", "fail_5d", "fail_10d",
    "fwd_swing_ret",    # 260523 v3.2 — pivot-to-pivot forward return
    "fwd_swing_bars",   # 260523 v3.2 — bars to next opposite pivot
]

OUTPUT_COLUMNS = [
    "ticker", "date", "bar_datetime", "bar_index", "universe", "timeframe", "open", "high", "low", "close", "volume",
    "tz_wlnbb_version", "build_marker",
    "price_bucket", "is_sub_dollar", "is_penny_stock", "is_low_price", "is_high_price",
    "ema9", "ema20", "ema34", "ema50", "ema89", "ema200",
    "t_signal", "z_signal", "t_raw_signals", "z_raw_signals", "bull_priority_code", "bear_priority_code",
    "volume_bucket", "l_digits", "l_signal", "l34_active", "l43_active", "l64_active", "l22_active", "l_raw_signals",
    "preup_signal", "predn_signal", "preup_raw_signals", "predn_raw_signals",
    "ne_suffix", "wick_suffix",
    "penetration_suffix", "wick_penetration_upper", "wick_penetration_lower", "wick_penetration_both",
    "close_suffix", "close_appended",
    "full_suffix",
    "bar_body_wick", "bar_gap_range", "bar_line5",
    "ad_fresh", "ad_cluster",
    "wyc_phase", "wyc_spring", "wyc_sos", "wyc_acc_tr", "wyc_markup",
    "swing_type", "swing_ret_from_prev",
    "fwd_swing_ret", "fwd_swing_bars",          # RESEARCH_ONLY (lookahead)
    "is_pivot_high", "is_pivot_low",
    # ── 260523 v3.5: PREBREAK + WYC additional ──────────────────────────────
    "prebreak_prime", "prebreak_ready", "prebreak_watch",
    "pb_lvbo", "pb_stop_cause", "pb_pp_rtv", "pb_fly_cd_c",
    "pb_wvf_confirm", "pb_follow_confirm", "pb_macro_penalty",
    "wyc_in_tr", "wyc_sow",
    "wick_ext_up", "wick_ext_down", "wick_ext_both",
    "prev_body_top", "prev_body_bot", "prev_high", "prev_low",
    "composite_t_label", "composite_z_label", "composite_primary_label", "composite_all_labels",
    "composite_core", "composite_suffix", "composite_full_suffix", "composite_full_label",
    "lane1_label", "lane3_label", "combined_signal_text",
    "has_t_signal", "has_z_signal", "has_l_signal", "has_preup", "has_predn",
    "has_tz_l_combo", "has_bullish_context", "has_bearish_context",
    "prev_1_signal_summary", "prev_3_signal_summary", "prev_5_signal_summary",
    "t_after_z_confirmed", "z_after_t_confirmed", "l_after_z_confirmed",
    "preup_after_z_confirmed", "predn_after_t_confirmed",
    "ret_1d", "ret_3d", "ret_5d", "ret_10d",
    "max_high_5d", "max_high_10d", "max_drawdown_5d", "max_drawdown_10d",
    "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
    "clean_win_5d", "big_win_10d", "fail_5d", "fail_10d",
]


def classify_price_bucket(close) -> str:
    """Map a close price to a price-bucket label.
    LT1, 1_5, 5_20, 20_50, 50_150, 150_300, 300_PLUS — empty for invalid input.
    """
    try:
        c = float(close)
    except (TypeError, ValueError):
        return ""
    if c != c:  # NaN
        return ""
    if c < 1:    return "LT1"
    if c < 5:    return "1_5"
    if c < 20:   return "5_20"
    if c < 50:   return "20_50"
    if c < 150:  return "50_150"
    if c < 300:  return "150_300"
    return "300_PLUS"


def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must be sorted by date ascending for a single ticker.
    All returns are close-to-close percentages.
    NEVER call this across multiple tickers.
    """
    c = df["close"]
    df["ret_1d"]  = (c.shift(-1)  / c - 1) * 100
    df["ret_3d"]  = (c.shift(-3)  / c - 1) * 100
    df["ret_5d"]  = (c.shift(-5)  / c - 1) * 100
    df["ret_10d"] = (c.shift(-10) / c - 1) * 100

    highs = df["high"]
    lows  = df["low"]

    for w, wk in [(5, "5d"), (10, "10d")]:
        mfe_vals, mae_vals, maxh_vals, mind_vals = [], [], [], []
        for i in range(len(df)):
            fut_h = highs.iloc[i+1:i+w+1]
            fut_l = lows.iloc[i+1:i+w+1]
            c0 = c.iloc[i]
            if len(fut_h) > 0 and c0 > 0:
                mh = fut_h.max()
                ml = fut_l.min()
                maxh_vals.append(round((mh - c0) / c0 * 100, 4))
                mind_vals.append(round((ml - c0) / c0 * 100, 4))
                mfe_vals.append(round((mh - c0) / c0 * 100, 4))
                mae_vals.append(round((ml - c0) / c0 * 100, 4))
            else:
                maxh_vals.append(None)
                mind_vals.append(None)
                mfe_vals.append(None)
                mae_vals.append(None)
        df[f"max_high_{wk}"]     = maxh_vals
        df[f"max_drawdown_{wk}"] = mind_vals
        df[f"mfe_{wk}"]          = mfe_vals
        df[f"mae_{wk}"]          = mae_vals

    # Outcome labels
    df["clean_win_5d"] = (df["ret_5d"]  >= 3.0).astype(int)
    df["big_win_10d"]  = (df["ret_10d"] >= 5.0).astype(int)
    df["fail_5d"]      = (df["ret_5d"]  <= -3.0).astype(int)
    df["fail_10d"]     = (df["ret_10d"] <= -5.0).astype(int)

    return df


def add_sequence_context(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add prev signal summaries and sequence confirmation flags.
    df must be sorted by date ascending for a single ticker.
    """
    def primary(row_dict):
        return (row_dict.get("t_signal") or row_dict.get("z_signal") or
                row_dict.get("l_signal") or row_dict.get("preup_signal") or
                row_dict.get("predn_signal") or "")

    summaries_1, summaries_3, summaries_5 = [], [], []
    t_after_z, z_after_t, l_after_z, preup_after_z, predn_after_t = [], [], [], [], []

    rows_list = [row._asdict() for row in df.itertuples(index=False)]

    for i in range(len(rows_list)):
        # prev 1 bar summary
        if i >= 1:
            summaries_1.append(primary(rows_list[i-1]))
        else:
            summaries_1.append("")

        # prev 3 bars summary
        prev3 = [primary(rows_list[j]) for j in range(max(0, i-3), i) if primary(rows_list[j])]
        summaries_3.append("|".join(prev3) if prev3 else "")

        # prev 5 bars summary
        prev5 = [primary(rows_list[j]) for j in range(max(0, i-5), i) if primary(rows_list[j])]
        summaries_5.append("|".join(prev5) if prev5 else "")

        # sequence confirmations: look back up to 5 bars for triggering signal
        curr = rows_list[i]
        curr_t = curr.get("t_signal", "")
        curr_z = curr.get("z_signal", "")
        curr_l = curr.get("l_signal", "")
        curr_preup = curr.get("preup_signal", "")
        curr_predn = curr.get("predn_signal", "")

        has_z_in_prev5 = any(rows_list[j].get("z_signal", "") for j in range(max(0, i-5), i))
        has_t_in_prev5 = any(rows_list[j].get("t_signal", "") for j in range(max(0, i-5), i))

        t_after_z.append(1 if (curr_t and has_z_in_prev5) else 0)
        z_after_t.append(1 if (curr_z and has_t_in_prev5) else 0)
        l_after_z.append(1 if (curr_l and has_z_in_prev5) else 0)
        preup_after_z.append(1 if (curr_preup and has_z_in_prev5) else 0)
        predn_after_t.append(1 if (curr_predn and has_t_in_prev5) else 0)

    df["prev_1_signal_summary"]   = summaries_1
    df["prev_3_signal_summary"]   = summaries_3
    df["prev_5_signal_summary"]   = summaries_5
    df["t_after_z_confirmed"]     = t_after_z
    df["z_after_t_confirmed"]     = z_after_t
    df["l_after_z_confirmed"]     = l_after_z
    df["preup_after_z_confirmed"] = preup_after_z
    df["predn_after_t_confirmed"] = predn_after_t

    return df


def generate_stock_stat(
    tickers: List[str],
    fetch_ohlcv_fn: Callable,  # callable(ticker, interval, calendar_days) -> pd.DataFrame or raises
    universe: str = "sp500",
    tf: str = "1d",
    bars: int = 500,  # now calendar_days (default 500 ≈ 320+ trading days)
    output_path: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    early_stop_fn: Optional[Callable[[], bool]] = None,
    min_price: float = 0,  # skip tickers whose latest close < min_price (e.g. 5 for nasdaq_gt5)
) -> Tuple[str, dict]:
    """Generate stock_stat CSV. Returns (output_path, audit_dict)."""
    if output_path is None:
        output_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"

    t0 = time.time()
    total = len(tickers)
    audit = {
        "tickers_requested": total,
        "tickers_with_ohlcv": 0, "tickers_skipped_no_data": 0,
        "tickers_skipped_error": 0, "tickers_processed": 0,
        "rows_before_signals": 0, "rows_after_signals": 0, "rows_processed": 0,
        "rows_with_t_signal": 0, "rows_with_z_signal": 0,
        "rows_with_l_signal": 0, "rows_with_preup": 0,
        "rows_with_predn": 0, "rows_with_combos": 0,
        "skip_reasons": {},
    }

    log.info(
        "TZ_WLNBB_GENERATION_AUDIT: starting universe=%s tf=%s requested_tickers=%d output=%s",
        universe, tf, total, output_path,
    )

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)

        for ticker_idx, ticker in enumerate(tickers, start=1):
            if early_stop_fn and early_stop_fn():
                log.info("tz_wlnbb stock_stat: early stop requested after %d tickers", audit["tickers_processed"])
                break
            # Progress fires on every attempt so UI counter advances even when tickers are skipped
            if progress_callback:
                progress_callback(ticker_idx, total)
            try:
                df = fetch_ohlcv_fn(ticker, tf, bars)
                if df is None or len(df) < 2:
                    audit["tickers_skipped_no_data"] += 1
                    audit["skip_reasons"][ticker] = "no_data_or_too_short"
                    continue
                # Price filter: skip tickers below min_price (e.g. nasdaq_gt5 requires >= 5)
                if min_price > 0:
                    try:
                        latest_close = float(df["close"].iloc[-1])
                        if latest_close < min_price:
                            audit["tickers_skipped_no_data"] += 1
                            audit["skip_reasons"][ticker] = (
                                f"close_{latest_close:.2f}<min_price_{min_price}"
                            )
                            continue
                    except (KeyError, IndexError, ValueError):
                        pass  # close column unavailable — proceed, scanner will filter
                audit["tickers_with_ohlcv"] += 1
                audit["rows_before_signals"] += len(df)

                # Extract date/datetime from index BEFORE compute_signals_for_ticker
                # resets it to integer 0,1,2... via reset_index(drop=True).
                if "date" not in df.columns:
                    df["date"] = pd.to_datetime(df.index).strftime("%Y-%m-%d")
                # bar_datetime: full ISO datetime for intraday, date-only for daily
                if "bar_datetime" not in df.columns:
                    if tf in ("4h", "1h"):
                        df["bar_datetime"] = pd.to_datetime(df.index).strftime("%Y-%m-%d %H:%M")
                    else:
                        df["bar_datetime"] = df["date"]

                df = compute_signals_for_ticker(df, universe)
                # date column is now preserved as a regular column (not the index).

                # Sort chronologically using datetime parsing, not lexicographic string sort.
                df = df.sort_values(
                    by="date",
                    key=lambda s: pd.to_datetime(s, errors="coerce"),
                ).reset_index(drop=True)
                df["bar_index"] = range(len(df))

                # Add forward returns (single-ticker, close-to-close)
                df = add_forward_returns(df)

                # Add sequence context (single-ticker)
                df = add_sequence_context(df)

                audit["rows_after_signals"] += len(df)
                audit["tickers_processed"] += 1

                for _, row in df.iterrows():
                    date_val = row.get("date", "")
                    if not date_val:
                        continue
                    bar_datetime_val = row.get("bar_datetime") or date_val
                    t_raw_set = row.get("t_raw") or set()
                    z_raw_set = row.get("z_raw") or set()
                    preup_raw_set = row.get("preup_raw") or set()
                    predn_raw_set = row.get("predn_raw") or set()
                    t_raw_str = " ".join(sorted(t_raw_set)) if t_raw_set else ""
                    z_raw_str = " ".join(sorted(z_raw_set)) if z_raw_set else ""
                    preup_raw_str = " ".join(sorted(preup_raw_set)) if preup_raw_set else ""
                    predn_raw_str = " ".join(sorted(predn_raw_set)) if predn_raw_set else ""
                    l_raw_parts = []
                    for n in range(1, 7):
                        if row.get(f"l{n}_raw"):
                            l_raw_parts.append(f"L{n}")

                    def _val(v):
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return ""
                        return v

                    close_val = row.get("close")
                    try:
                        cf = float(close_val) if close_val not in (None, "") else None
                        if cf != cf: cf = None  # NaN
                    except (TypeError, ValueError):
                        cf = None
                    price_bucket = classify_price_bucket(cf) if cf is not None else ""
                    is_sub_dollar  = int(cf is not None and cf < 1)
                    is_penny_stock = int(cf is not None and cf < 5)
                    is_low_price   = int(cf is not None and cf < 20)
                    is_high_price  = int(cf is not None and cf >= 150)

                    writer.writerow([
                        ticker, date_val, bar_datetime_val, int(row.get("bar_index", 0)), universe, tf,
                        _val(row.get("open")), _val(row.get("high")),
                        _val(row.get("low")), _val(row.get("close")),
                        _val(row.get("volume")),
                        TZ_WLNBB_VERSION,
                        BUILD_MARKER,
                        price_bucket, is_sub_dollar, is_penny_stock, is_low_price, is_high_price,
                        _val(row.get("ema9")), _val(row.get("ema20")),
                        _val(row.get("ema34")), _val(row.get("ema50")),
                        _val(row.get("ema89")), _val(row.get("ema200")),
                        row.get("t_signal", ""), row.get("z_signal", ""),
                        t_raw_str, z_raw_str,
                        row.get("bull_priority_code", 0), row.get("bear_priority_code", 0),
                        row.get("volume_bucket", ""), row.get("l_digits", ""), row.get("l_signal", ""),
                        int(bool(row.get("l34_active"))), int(bool(row.get("l43_active"))),
                        int(bool(row.get("l64_active"))), int(bool(row.get("l22_active"))),
                        " ".join(l_raw_parts),
                        row.get("preup_signal", ""), row.get("predn_signal", ""),
                        preup_raw_str, predn_raw_str,
                        row.get("ne_suffix", ""), row.get("wick_suffix", ""),
                        row.get("penetration_suffix", ""),
                        int(bool(row.get("wick_penetration_upper"))),
                        int(bool(row.get("wick_penetration_lower"))),
                        int(bool(row.get("wick_penetration_both"))),
                        row.get("close_suffix", ""),
                        int(bool(row.get("close_appended"))),
                        (
                            str(row.get("ne_suffix") or "")
                            + str(row.get("wick_suffix") or "")
                            + str(row.get("penetration_suffix") or "")
                            + (str(row.get("close_suffix") or "") if row.get("close_appended") else "")
                        ),  # full_suffix (includes close_suffix only when append_close fires)
                        row.get("bar_body_wick", ""), row.get("bar_gap_range", ""), row.get("bar_line5", ""),
                        int(bool(row.get("ad_fresh"))),
                        int(bool(row.get("ad_cluster"))),
                        row.get("wyc_phase", "NEUTRAL") or "NEUTRAL",
                        int(bool(row.get("wyc_spring"))),
                        int(bool(row.get("wyc_sos"))),
                        int(bool(row.get("wyc_acc_tr"))),
                        int(bool(row.get("wyc_markup"))),
                        row.get("swing_type", "") or "",
                        ("" if (row.get("swing_ret_from_prev") is None or
                                (isinstance(row.get("swing_ret_from_prev"), float) and
                                 row.get("swing_ret_from_prev") != row.get("swing_ret_from_prev")))
                            else _val(row.get("swing_ret_from_prev"))),
                        ("" if (row.get("fwd_swing_ret") is None or
                                (isinstance(row.get("fwd_swing_ret"), float) and
                                 row.get("fwd_swing_ret") != row.get("fwd_swing_ret")))
                            else _val(row.get("fwd_swing_ret"))),
                        ("" if (row.get("fwd_swing_bars") is None or
                                (isinstance(row.get("fwd_swing_bars"), float) and
                                 row.get("fwd_swing_bars") != row.get("fwd_swing_bars")))
                            else int(row.get("fwd_swing_bars"))),
                        int(bool(row.get("is_pivot_high"))),
                        int(bool(row.get("is_pivot_low"))),
                        # 260523 v3.5 PREBREAK + WYC
                        int(bool(row.get("prebreak_prime"))),
                        int(bool(row.get("prebreak_ready"))),
                        int(bool(row.get("prebreak_watch"))),
                        int(bool(row.get("pb_lvbo"))),
                        int(bool(row.get("pb_stop_cause"))),
                        int(bool(row.get("pb_pp_rtv"))),
                        int(bool(row.get("pb_fly_cd_c"))),
                        int(bool(row.get("pb_wvf_confirm"))),
                        int(bool(row.get("pb_follow_confirm"))),
                        int(bool(row.get("pb_macro_penalty"))),
                        int(bool(row.get("wyc_in_tr"))),
                        int(bool(row.get("wyc_sow"))),
                        int(bool(row.get("wick_ext_up"))),
                        int(bool(row.get("wick_ext_down"))),
                        int(bool(row.get("wick_ext_both"))),
                        _val(row.get("prev_body_top")),
                        _val(row.get("prev_body_bot")),
                        _val(row.get("prev_high")),
                        _val(row.get("prev_low")),
                        row.get("composite_t_label", ""),
                        row.get("composite_z_label", ""),
                        row.get("composite_primary_label", ""),
                        row.get("composite_all_labels", ""),
                        row.get("composite_core", ""),
                        row.get("composite_suffix", ""),
                        row.get("composite_full_suffix", ""),
                        row.get("composite_full_label", ""),
                        row.get("lane1_label", ""), row.get("lane3_label", ""),
                        (row.get("lane1_label", "") + " " + row.get("lane3_label", "")).strip(),
                        int(bool(row.get("has_t_signal"))), int(bool(row.get("has_z_signal"))),
                        int(bool(row.get("has_l_signal"))), int(bool(row.get("has_preup"))),
                        int(bool(row.get("has_predn"))), int(bool(row.get("has_tz_l_combo"))),
                        int(bool(row.get("has_bullish_context"))), int(bool(row.get("has_bearish_context"))),
                        # sequence context
                        row.get("prev_1_signal_summary", ""),
                        row.get("prev_3_signal_summary", ""),
                        row.get("prev_5_signal_summary", ""),
                        row.get("t_after_z_confirmed", 0),
                        row.get("z_after_t_confirmed", 0),
                        row.get("l_after_z_confirmed", 0),
                        row.get("preup_after_z_confirmed", 0),
                        row.get("predn_after_t_confirmed", 0),
                        # forward returns
                        _val(row.get("ret_1d")), _val(row.get("ret_3d")),
                        _val(row.get("ret_5d")), _val(row.get("ret_10d")),
                        _val(row.get("max_high_5d")), _val(row.get("max_high_10d")),
                        _val(row.get("max_drawdown_5d")), _val(row.get("max_drawdown_10d")),
                        _val(row.get("mfe_5d")), _val(row.get("mfe_10d")),
                        _val(row.get("mae_5d")), _val(row.get("mae_10d")),
                        _val(row.get("clean_win_5d")), _val(row.get("big_win_10d")),
                        _val(row.get("fail_5d")), _val(row.get("fail_10d")),
                    ])
                    audit["rows_processed"] += 1
                    if row.get("has_t_signal"):    audit["rows_with_t_signal"] += 1
                    if row.get("has_z_signal"):    audit["rows_with_z_signal"] += 1
                    if row.get("has_l_signal"):    audit["rows_with_l_signal"] += 1
                    if row.get("has_preup"):       audit["rows_with_preup"] += 1
                    if row.get("has_predn"):       audit["rows_with_predn"] += 1
                    if row.get("has_tz_l_combo"):  audit["rows_with_combos"] += 1
            except Exception as exc:
                audit["tickers_skipped_error"] += 1
                audit["skip_reasons"][ticker] = str(exc)
                log.warning("tz_wlnbb stock_stat error for %s: %s", ticker, exc, exc_info=True)

    elapsed = round(time.time() - t0, 1)
    audit["elapsed_seconds"] = elapsed
    audit["output_path"] = output_path

    log.info(
        "TZ_WLNBB_GENERATION_AUDIT: universe=%s tf=%s "
        "requested=%d ohlcv_ok=%d skipped_no_data=%d skipped_error=%d processed=%d "
        "rows_before=%d rows_after=%d rows_written=%d "
        "t=%d z=%d l=%d preup=%d predn=%d combos=%d elapsed=%.1fs output=%s",
        universe, tf,
        audit["tickers_requested"], audit["tickers_with_ohlcv"],
        audit["tickers_skipped_no_data"], audit["tickers_skipped_error"],
        audit["tickers_processed"],
        audit["rows_before_signals"], audit["rows_after_signals"], audit["rows_processed"],
        audit["rows_with_t_signal"], audit["rows_with_z_signal"],
        audit["rows_with_l_signal"], audit["rows_with_preup"],
        audit["rows_with_predn"], audit["rows_with_combos"],
        elapsed, output_path,
    )

    if audit["rows_processed"] == 0:
        msg = (
            f"TZ_WLNBB_ANALYZER_FAILURE: stock_stat generation produced zero rows. "
            f"universe={universe} tf={tf} requested={total} "
            f"ohlcv_ok={audit['tickers_with_ohlcv']} errors={audit['tickers_skipped_error']} "
            f"no_data={audit['tickers_skipped_no_data']}. "
            f"Check ticker universe, OHLCV fetch, date range, and filters."
        )
        log.error(msg)
        # Include first few skip reasons in audit for debugging
        sample_errors = {k: v for k, v in list(audit["skip_reasons"].items())[:5]}
        audit["sample_skip_reasons"] = sample_errors

    return output_path, audit


# ──────────────────────────────────────────────────────────────────────────
# 260523 v4.9 Phase 2 — Incremental ("scan today only")
# ──────────────────────────────────────────────────────────────────────────

def _read_existing_last_dates(csv_path: str) -> dict:
    """Scan an existing stock_stat CSV and return {ticker: max_date}."""
    if not os.path.exists(csv_path):
        return {}
    try:
        import pandas as _pd
        df = _pd.read_csv(csv_path, usecols=["ticker", "date"], low_memory=False)
    except Exception as e:
        log.warning("incremental: cannot read existing CSV %s: %s", csv_path, e)
        return {}
    if len(df) == 0:
        return {}
    grp = df.groupby("ticker")["date"].max()
    return {str(t): str(d) for t, d in grp.items()}


def _read_ticker_tail(csv_path: str, ticker: str, n_bars: int = 60):
    """Read the last `n_bars` rows for `ticker` from the existing CSV.
    Used as warm-up context (so indicators have prior bars to compute on)."""
    if not os.path.exists(csv_path):
        return None
    try:
        import pandas as _pd
        df = _pd.read_csv(csv_path, low_memory=False)
        sub = df[df["ticker"] == ticker]
        if len(sub) == 0:
            return None
        # OHLCV only — the rest will be recomputed by compute_signals_for_ticker
        sub = sub[["date", "open", "high", "low", "close", "volume"]].copy()
        sub = sub.sort_values("date").tail(n_bars).reset_index(drop=True)
        return sub
    except Exception as e:
        log.warning("incremental: tail read failed for %s: %s", ticker, e)
        return None


def _append_rows(csv_path: str, rows: list) -> int:
    """Append rows (list of OUTPUT_COLUMNS-aligned lists) to existing CSV.
    Returns count appended."""
    if not rows:
        return 0
    with open(csv_path, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        for r in rows:
            w.writerow(r)
    return len(rows)


def generate_stock_stat_incremental(
    tickers: List[str],
    fetch_ohlcv_fn: Callable,
    universe: str = "sp500",
    tf: str = "1d",
    output_path: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    early_stop_fn: Optional[Callable[[], bool]] = None,
    warmup_bars: int = 60,
    max_gap_days: int = 10,
) -> Tuple[str, dict]:
    """Append only NEW bars (since last existing date) per ticker.

    For each ticker:
      1. Read existing CSV → last_date.
      2. If file doesn't exist OR last_date older than max_gap_days → fall
         back to full generate_stock_stat for that ticker (cold start).
      3. Otherwise: fetch only bars since (last_date + 1 day).
      4. Concat with warmup tail (60 bars) → compute_signals_for_ticker on
         the combined frame → take only the new rows.
      5. Append new rows to CSV.

    Returns (output_path, audit). Audit includes per-ticker stats:
      tickers_appended, tickers_skipped_no_new_bars, total_rows_added.
    """
    if output_path is None:
        output_path = f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"

    t0 = time.time()
    total = len(tickers)

    audit = {
        "mode": "incremental",
        "tickers_requested": total,
        "tickers_appended": 0,
        "tickers_skipped_no_new_bars": 0,
        "tickers_cold_started": 0,
        "tickers_error": 0,
        "rows_added": 0,
        "skip_reasons": {},
    }

    # If file doesn't exist at all, do a full run for all tickers (cold start)
    if not os.path.exists(output_path):
        log.info("incremental: %s does not exist — falling back to full scan", output_path)
        return generate_stock_stat(
            tickers=tickers, fetch_ohlcv_fn=fetch_ohlcv_fn,
            universe=universe, tf=tf, bars=500,
            output_path=output_path,
            progress_callback=progress_callback,
            early_stop_fn=early_stop_fn,
        )

    last_dates = _read_existing_last_dates(output_path)
    log.info("incremental: existing CSV covers %d tickers", len(last_dates))

    from .signal_extraction import compute_signals_for_ticker
    import pandas as _pd

    # Compute "today" cutoff to know what is fresh
    from datetime import date as _date, datetime as _dt, timedelta as _td

    cold_start_tickers: list = []

    for idx, ticker in enumerate(tickers, start=1):
        if early_stop_fn and early_stop_fn():
            log.info("incremental: early stop after %d tickers", idx - 1)
            break
        if progress_callback:
            progress_callback(idx, total)

        last_date_str = last_dates.get(ticker)
        if not last_date_str:
            cold_start_tickers.append(ticker)
            continue

        # Parse last date
        try:
            last_dt = _dt.strptime(last_date_str[:10], "%Y-%m-%d").date()
        except Exception:
            cold_start_tickers.append(ticker)
            continue

        gap = (_date.today() - last_dt).days
        if gap > max_gap_days:
            log.info("incremental: %s last bar %s is %d days old (>max_gap_days=%d) → cold start",
                     ticker, last_date_str, gap, max_gap_days)
            cold_start_tickers.append(ticker)
            continue

        # Fetch only new bars since last+1
        since_str = (last_dt + _td(days=1)).strftime("%Y-%m-%d")
        try:
            new_df = fetch_ohlcv_fn(ticker, tf, since=since_str)
        except TypeError:
            # Caller passed a fetch fn that doesn't accept `since` — fall back
            try:
                new_df = fetch_ohlcv_fn(ticker, tf, 30)  # last 30 days
                if "date" in getattr(new_df, "columns", []):
                    new_df = new_df[new_df["date"] > last_date_str]
                else:
                    new_df = new_df[new_df.index.astype(str) > last_date_str]
            except Exception as e:
                audit["tickers_error"] += 1
                audit["skip_reasons"][ticker] = f"fetch failed: {e}"
                continue
        except Exception as e:
            audit["tickers_error"] += 1
            audit["skip_reasons"][ticker] = f"fetch failed: {e}"
            continue

        if new_df is None or len(new_df) == 0:
            audit["tickers_skipped_no_new_bars"] += 1
            continue

        # Build combined warmup + new for indicator context
        warmup = _read_ticker_tail(output_path, ticker, n_bars=warmup_bars)
        if warmup is None or len(warmup) == 0:
            cold_start_tickers.append(ticker)
            continue

        # Normalise new_df to columns the signal engine expects
        new_df = new_df.copy()
        if "date" not in new_df.columns:
            new_df["date"] = new_df.index.astype(str).str[:10]
        new_df = new_df.reset_index(drop=True)
        # Keep just the columns we need
        need_cols = ["date", "open", "high", "low", "close", "volume"]
        new_df = new_df[[c for c in need_cols if c in new_df.columns]].copy()

        combined = _pd.concat([warmup, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)

        try:
            enriched = compute_signals_for_ticker(combined, universe=universe)
        except Exception as e:
            audit["tickers_error"] += 1
            audit["skip_reasons"][ticker] = f"signal compute failed: {e}"
            continue

        # Take only rows whose date is AFTER the existing last_date
        new_rows = enriched[enriched["date"].astype(str) > last_date_str]
        if len(new_rows) == 0:
            audit["tickers_skipped_no_new_bars"] += 1
            continue

        # Write rows in OUTPUT_COLUMNS order to the CSV (append mode)
        rows_to_write = []
        for _, row in new_rows.iterrows():
            rows_to_write.append([row.get(col, "") for col in OUTPUT_COLUMNS])
        n = _append_rows(output_path, rows_to_write)
        audit["tickers_appended"] += 1
        audit["rows_added"] += n

    # Cold-start tickers via full path (writes to a temp file then we merge)
    if cold_start_tickers:
        log.info("incremental: cold-starting %d tickers", len(cold_start_tickers))
        # Run full scan for cold-start tickers into a temp file, then append
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", delete=False,
                                          suffix=".csv") as tmp:
            tmp_path = tmp.name
        try:
            _, cold_audit = generate_stock_stat(
                tickers=cold_start_tickers, fetch_ohlcv_fn=fetch_ohlcv_fn,
                universe=universe, tf=tf, bars=500,
                output_path=tmp_path,
                progress_callback=None,
                early_stop_fn=early_stop_fn,
            )
            # Append all rows (skip header) of tmp into output_path
            try:
                with open(tmp_path, "r", encoding="utf-8") as src, \
                     open(output_path, "a", encoding="utf-8") as dst:
                    next(src, None)  # skip header
                    n_added = 0
                    for line in src:
                        dst.write(line)
                        n_added += 1
                audit["tickers_cold_started"] = cold_audit.get("tickers_processed", len(cold_start_tickers))
                audit["rows_added"] += n_added
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
        except Exception as e:
            log.exception("cold-start branch failed")
            audit["skip_reasons"]["__cold_start__"] = str(e)

    audit["elapsed_sec"] = round(time.time() - t0, 2)
    log.info("TZ_WLNBB_INCREMENTAL: appended=%d skipped=%d cold=%d rows=%d in %.1fs",
             audit["tickers_appended"], audit["tickers_skipped_no_new_bars"],
             audit["tickers_cold_started"], audit["rows_added"],
             audit["elapsed_sec"])
    return output_path, audit


def backfill_forward_columns(
    csv_path: str,
    days_back: int = 14,
) -> dict:
    """Recompute forward-return columns (ret_5d, ret_10d, mfe_5d, mae_5d, ...)
    on rows in the last `days_back` days that didn't have enough future bars
    at the time they were written.

    Strategy: rerun compute_signals_for_ticker on each ticker's recent tail
    (warmup + days_back rows), then UPDATE the forward columns in those rows.

    Note: this is the simplest correct implementation — it rewrites the
    affected rows. For very large CSVs this is O(N) per backfill. Future
    optimisation: write rows in DB and UPDATE in-place.
    """
    if not os.path.exists(csv_path):
        return {"ok": False, "error": f"{csv_path} not found"}

    import pandas as _pd
    from .signal_extraction import compute_signals_for_ticker

    try:
        df = _pd.read_csv(csv_path, low_memory=False)
    except Exception as e:
        return {"ok": False, "error": f"read failed: {e}"}
    if len(df) == 0:
        return {"ok": True, "rows_updated": 0, "tickers_processed": 0}

    df["date"] = df["date"].astype(str)
    from datetime import date as _date, timedelta as _td
    cutoff = (_date.today() - _td(days=days_back)).strftime("%Y-%m-%d")

    fwd_cols = [
        "ret_1d", "ret_3d", "ret_5d", "ret_10d",
        "max_high_5d", "max_high_10d",
        "max_drawdown_5d", "max_drawdown_10d",
        "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
        "clean_win_5d", "big_win_10d", "fail_5d", "fail_10d",
        "fwd_swing_ret", "fwd_swing_bars",
    ]
    fwd_cols_present = [c for c in fwd_cols if c in df.columns]

    tickers_processed = 0
    rows_updated = 0
    for ticker, grp in df.groupby("ticker"):
        if grp["date"].max() < cutoff:
            continue  # nothing recent for this ticker
        ohlcv = grp[["date", "open", "high", "low", "close", "volume"]].copy()
        ohlcv = ohlcv.sort_values("date").reset_index(drop=True)
        try:
            enriched = compute_signals_for_ticker(ohlcv, universe="sp500")
        except Exception:
            continue
        enriched["date"] = enriched["date"].astype(str)
        recent_dates = set(grp.loc[grp["date"] >= cutoff, "date"])
        for d in recent_dates:
            mask_old = (df["ticker"] == ticker) & (df["date"] == d)
            mask_new = enriched["date"] == d
            if not mask_old.any() or not mask_new.any():
                continue
            new_row = enriched[mask_new].iloc[0]
            for col in fwd_cols_present:
                df.loc[mask_old, col] = new_row.get(col)
            rows_updated += 1
        tickers_processed += 1

    # Atomic rewrite
    tmp = csv_path + ".bktmp"
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, csv_path)
    except Exception as e:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        return {"ok": False, "error": f"write failed: {e}"}

    return {
        "ok": True,
        "tickers_processed": tickers_processed,
        "rows_updated": rows_updated,
        "cutoff": cutoff,
    }
