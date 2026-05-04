"""Generate stock_stat_tz_wlnbb CSV."""
import csv
import logging
import time
import os
from datetime import datetime
from typing import Optional, Callable, List
from .config import TZ_WLNBB_VERSION
from .signal_extraction import compute_signals_for_ticker

log = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "ticker", "date", "universe", "timeframe", "open", "high", "low", "close", "volume",
    "ema9", "ema20", "ema34", "ema50", "ema89", "ema200",
    "t_signal", "z_signal", "t_raw_signals", "z_raw_signals", "bull_priority_code", "bear_priority_code",
    "volume_bucket", "l_digits", "l_signal", "l34_active", "l43_active", "l64_active", "l22_active", "l_raw_signals",
    "preup_signal", "predn_signal", "preup_raw_signals", "predn_raw_signals",
    "ne_suffix", "wick_suffix",
    "lane1_label", "lane3_label", "combined_signal_text",
    "has_t_signal", "has_z_signal", "has_l_signal", "has_preup", "has_predn",
    "has_tz_l_combo", "has_bullish_context", "has_bearish_context",
    "tz_wlnbb_version",
]


def generate_stock_stat(
    tickers: List[str],
    fetch_ohlcv_fn: Callable,  # callable(ticker, interval, bars) -> pd.DataFrame or raises
    universe: str = "sp500",
    tf: str = "1d",
    bars: int = 252,
    output_path: Optional[str] = None,
) -> str:
    """Generate stock_stat CSV. Returns output path."""
    if output_path is None:
        output_path = f"stock_stat_tz_wlnbb_{tf}.csv"

    t0 = time.time()
    audit = {
        "tickers_processed": 0, "rows_processed": 0,
        "rows_with_t_signal": 0, "rows_with_z_signal": 0,
        "rows_with_l_signal": 0, "rows_with_preup": 0,
        "rows_with_predn": 0, "rows_with_combos": 0,
    }

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)

        for ticker in tickers:
            try:
                df = fetch_ohlcv_fn(ticker, tf, bars)
                if df is None or len(df) < 2:
                    continue
                df = compute_signals_for_ticker(df, universe)
                audit["tickers_processed"] += 1

                # Add date column from index if not present
                if "date" not in df.columns:
                    try:
                        df["date"] = df.index.strftime("%Y-%m-%d")
                    except Exception:
                        df["date"] = [str(v)[:10] for v in df.index]

                for _, row in df.iterrows():
                    date_val = row.get("date", "")
                    if not date_val:
                        continue
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

                    writer.writerow([
                        ticker, date_val, universe, tf,
                        row.get("open", ""), row.get("high", ""), row.get("low", ""),
                        row.get("close", ""), row.get("volume", ""),
                        row.get("ema9", ""), row.get("ema20", ""), row.get("ema34", ""),
                        row.get("ema50", ""), row.get("ema89", ""), row.get("ema200", ""),
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
                        row.get("lane1_label", ""), row.get("lane3_label", ""),
                        (row.get("lane1_label", "") + " " + row.get("lane3_label", "")).strip(),
                        int(bool(row.get("has_t_signal"))), int(bool(row.get("has_z_signal"))),
                        int(bool(row.get("has_l_signal"))), int(bool(row.get("has_preup"))),
                        int(bool(row.get("has_predn"))), int(bool(row.get("has_tz_l_combo"))),
                        int(bool(row.get("has_bullish_context"))), int(bool(row.get("has_bearish_context"))),
                        TZ_WLNBB_VERSION,
                    ])
                    audit["rows_processed"] += 1
                    if row.get("has_t_signal"):    audit["rows_with_t_signal"] += 1
                    if row.get("has_z_signal"):    audit["rows_with_z_signal"] += 1
                    if row.get("has_l_signal"):    audit["rows_with_l_signal"] += 1
                    if row.get("has_preup"):       audit["rows_with_preup"] += 1
                    if row.get("has_predn"):       audit["rows_with_predn"] += 1
                    if row.get("has_tz_l_combo"):  audit["rows_with_combos"] += 1
            except Exception as exc:
                log.warning("tz_wlnbb stock_stat error for %s: %s", ticker, exc)

    elapsed = round(time.time() - t0, 1)
    log.info(
        "TZ_WLNBB_ANALYZER_AUDIT: universe=%s tf=%s tickers=%d rows=%d "
        "t_rows=%d z_rows=%d l_rows=%d preup=%d predn=%d combos=%d elapsed=%.1fs output=%s",
        universe, tf,
        audit["tickers_processed"], audit["rows_processed"],
        audit["rows_with_t_signal"], audit["rows_with_z_signal"],
        audit["rows_with_l_signal"], audit["rows_with_preup"],
        audit["rows_with_predn"], audit["rows_with_combos"],
        elapsed, output_path,
    )
    return output_path
