"""Pivot Swing Character Analytics Engine.

Analyzes TZ/WLNBB signal behavior at pivot zones (LOW-5..LOW+5, HIGH-5..HIGH+5),
discovers signal roles statistically, and produces CSV + Markdown output files.

CRITICAL DESIGN PRINCIPLES:
- Confirmed pivots only (pivot_right bars must close before pivot is known).
- All offset windows use bar offsets from the pivot's price bar (not confirmation bar).
- Research findings (using offset > 0) are tagged as RESEARCH_ONLY — they cannot
  be applied in live trading because they require future bars.
- Live-safe findings use only offsets ≤ 0 (pivot not yet confirmed but price known
  in hindsight) or offset analysis clearly labeled.
- No modification to signal_logic.py / signal_extraction.py / WLNBB L1–L6 logic.
"""
import os
import csv
import json
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)

# Offset window around each pivot
OFFSET_RANGE = range(-5, 6)   # -5 .. +5 inclusive

# Strict GO gate volume buckets
GO_VOLUME_BUCKETS = {"B", "VB"}

# Confidence thresholds
CONF_HIGH    = 100
CONF_MEDIUM  = 40
CONF_LOW     = 15


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_pivot_analytics(
    csv_path: str,
    output_dir: str,
    pivot_left: int = 3,
    pivot_right: int = 3,
    min_swing_return_pct: float = 3.0,
    min_swing_bars: int = 2,
    ticker: Optional[str] = None,
) -> Dict[str, str]:
    """
    Run the full pivot analytics pipeline on a stock_stat_tz_wlnbb CSV file.

    Parameters
    ----------
    csv_path           : path to stock_stat_tz_wlnbb_*.csv
    output_dir         : directory where output files are written
    pivot_left/right   : pivot detection parameters
    min_swing_return_pct / min_swing_bars : swing filtering
    ticker             : optional ticker label (inferred from filename if None)

    Returns
    -------
    dict mapping output file name → absolute path
    """
    from .pivot_detector import detect_pivots
    from .swing_builder import build_swings

    os.makedirs(output_dir, exist_ok=True)
    df = _load_csv(csv_path)
    if df is None or len(df) < 20:
        log.warning("pivot_analytics: not enough data in %s", csv_path)
        return {}

    if ticker is None:
        base = os.path.basename(csv_path)
        # stock_stat_tz_wlnbb_TICKER_... .csv
        parts = base.replace(".csv", "").split("_")
        ticker = parts[4] if len(parts) > 4 else base

    log.info("pivot_analytics: processing %s (%d bars)", ticker, len(df))

    df = detect_pivots(df, pivot_left=pivot_left, pivot_right=pivot_right)
    swings = build_swings(df, min_swing_return_pct=min_swing_return_pct,
                          min_swing_bars=min_swing_bars)

    log.info("pivot_analytics: %d swings detected for %s", len(swings), ticker)

    outputs: Dict[str, str] = {}

    # 1. Raw pivot list
    out = _write_pivot_list(df, ticker, output_dir)
    outputs.update(out)

    # 2. Raw swing list
    out = _write_swing_list(swings, ticker, output_dir)
    outputs.update(out)

    # 3. Pivot zone signal windows
    zone_records = _build_pivot_zone_records(df, swings, pivot_right)
    out = _write_pivot_zone_windows(zone_records, ticker, output_dir)
    outputs.update(out)

    # 4. Single-bar role analysis
    out = _write_single_bar_role(zone_records, ticker, output_dir)
    outputs.update(out)

    # 5. Sequence analysis (2–6 bar sequences)
    out = _write_sequence_analysis(zone_records, ticker, output_dir)
    outputs.update(out)

    # 6. Composite feature analysis
    out = _write_composite_analysis(zone_records, ticker, output_dir)
    outputs.update(out)

    # 7. Volume bucket + strict GO gate analysis
    out = _write_go_gate_analysis(zone_records, ticker, output_dir)
    outputs.update(out)

    # 8. Markdown summary reports
    out = _write_markdown_reports(zone_records, swings, ticker, output_dir)
    outputs.update(out)

    # 9. Version audit
    out = _write_version_audit(df, ticker, output_dir)
    outputs.update(out)

    log.info("pivot_analytics: wrote %d output files for %s", len(outputs), ticker)
    return outputs


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------

def _load_csv(csv_path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "date" not in df.columns and "bar_datetime" in df.columns:
            df["date"] = df["bar_datetime"]
        df = df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        return df
    except Exception as e:
        log.error("pivot_analytics: failed to load %s: %s", csv_path, e)
        return None


# ---------------------------------------------------------------------------
# Pivot zone record construction
# ---------------------------------------------------------------------------

def _build_pivot_zone_records(
    df: pd.DataFrame,
    swings: List[Dict[str, Any]],
    pivot_right: int,
) -> List[Dict[str, Any]]:
    """
    For every confirmed pivot, extract signal data in the [-5, +5] offset window.

    Each record represents ONE bar at ONE pivot's offset window:
        ticker, date, pivot_type (LOW/HIGH), pivot_idx, pivot_date, pivot_price,
        swing_direction (UP/DOWN), swing_return_pct,
        offset (-5..+5), bar_idx, bar_date,
        t_signal, z_signal, l_signal, volume_bucket,
        bar_body_wick, bar_gap_range, bar_line5,
        full_suffix, composite_full_label,
        close, high, low,
        lookahead_safe (bool: offset <= 0 is live-safe RELATIVE to pivot price bar,
                        but note pivot itself is only confirmed after pivot_right bars)
    """
    n = len(df)
    dates = df["date"].values if "date" in df.columns else np.arange(n).astype(str)

    def _get(row_idx: int, col: str, default="") -> Any:
        if 0 <= row_idx < n and col in df.columns:
            v = df[col].iloc[row_idx]
            return "" if (pd.isna(v) if isinstance(v, float) else False) else v
        return default

    # Build pivot_to_swing map: pivot_idx → swing info
    pivot_swing_map: Dict[int, Dict] = {}
    for sw in swings:
        pivot_swing_map[sw["start_pivot_idx"]] = sw
        pivot_swing_map[sw["end_pivot_idx"]] = sw

    records: List[Dict[str, Any]] = []

    for i in range(n):
        is_low = bool(df["pivot_low"].iloc[i])
        is_high = bool(df["pivot_high"].iloc[i])
        if not (is_low or is_high):
            continue

        pivot_type = "LOW" if is_low else "HIGH"
        conf_idx = int(df["pivot_low_confirmed_at"].iloc[i] if is_low
                       else df["pivot_high_confirmed_at"].iloc[i])
        pivot_price = float(df["low"].iloc[i] if is_low else df["high"].iloc[i])
        pivot_date = str(dates[i])

        sw = pivot_swing_map.get(i, {})

        for offset in OFFSET_RANGE:
            bar_idx = i + offset
            if bar_idx < 0 or bar_idx >= n:
                continue

            # lookahead_safe: offset <= 0 means bar happened before/at pivot price extreme
            # However note: the pivot is not CONFIRMED until conf_idx = i + pivot_right.
            # For live trading we can only use this at offset=(conf_idx - i) or later.
            lookahead_safe = offset <= 0
            live_bar_offset = conf_idx - i  # pivot_right

            records.append({
                "ticker": _get(i, "ticker", ""),
                "pivot_type": pivot_type,
                "pivot_idx": i,
                "pivot_date": pivot_date,
                "pivot_price": pivot_price,
                "conf_idx": conf_idx,
                "live_usable_offset": live_bar_offset,
                "swing_direction": sw.get("direction", ""),
                "swing_return_pct": sw.get("return_pct", ""),
                "offset": offset,
                "bar_idx": bar_idx,
                "bar_date": str(dates[bar_idx]),
                "t_signal": _get(bar_idx, "t_signal"),
                "z_signal": _get(bar_idx, "z_signal"),
                "l_signal": _get(bar_idx, "l_signal"),
                "volume_bucket": _get(bar_idx, "volume_bucket"),
                "bar_body_wick": _get(bar_idx, "bar_body_wick"),
                "bar_gap_range": _get(bar_idx, "bar_gap_range"),
                "bar_line5": _get(bar_idx, "bar_line5"),
                "full_suffix": _get(bar_idx, "full_suffix"),
                "composite_full_label": _get(bar_idx, "composite_full_label"),
                "composite_primary_label": _get(bar_idx, "composite_primary_label"),
                "close": _get(bar_idx, "close", np.nan),
                "high": _get(bar_idx, "high", np.nan),
                "low": _get(bar_idx, "low", np.nan),
                "ret_1d": _get(bar_idx, "ret_1d", ""),
                "ret_5d": _get(bar_idx, "ret_5d", ""),
                "ret_10d": _get(bar_idx, "ret_10d", ""),
                "mfe_5d": _get(bar_idx, "mfe_5d", ""),
                "mae_5d": _get(bar_idx, "mae_5d", ""),
                "lookahead_safe": lookahead_safe,
                "lookahead_note": (
                    "LIVE_SAFE" if lookahead_safe
                    else f"RESEARCH_ONLY (requires +{offset} future bars relative to pivot)"
                ),
            })

    return records


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def _write_pivot_list(df: pd.DataFrame, ticker: str, out_dir: str) -> Dict[str, str]:
    filename = f"pivot_list_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    pivot_rows = []
    for i in range(len(df)):
        for pt, col_flag, col_conf in [
            ("LOW", "pivot_low", "pivot_low_confirmed_at"),
            ("HIGH", "pivot_high", "pivot_high_confirmed_at"),
        ]:
            if df[col_flag].iloc[i]:
                pivot_rows.append({
                    "ticker": df["ticker"].iloc[i] if "ticker" in df.columns else ticker,
                    "pivot_type": pt,
                    "pivot_idx": i,
                    "pivot_date": df["date"].iloc[i] if "date" in df.columns else i,
                    "pivot_price": df["low"].iloc[i] if pt == "LOW" else df["high"].iloc[i],
                    "confirmed_at_idx": int(df[col_conf].iloc[i]),
                })
    _write_csv(path, pivot_rows,
               ["ticker","pivot_type","pivot_idx","pivot_date","pivot_price","confirmed_at_idx"])
    return {filename: path}


def _write_swing_list(swings: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    filename = f"swing_list_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    cols = ["direction","start_pivot_idx","end_pivot_idx","start_conf_idx","end_conf_idx",
            "start_price","end_price","return_pct","bar_count","start_date","end_date"]
    _write_csv(path, swings, cols)
    return {filename: path}


def _write_pivot_zone_windows(records: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    filename = f"pivot_zone_windows_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    cols = ["ticker","pivot_type","pivot_idx","pivot_date","pivot_price","conf_idx",
            "live_usable_offset","swing_direction","swing_return_pct",
            "offset","bar_idx","bar_date",
            "t_signal","z_signal","l_signal","volume_bucket",
            "bar_body_wick","bar_gap_range","bar_line5",
            "full_suffix","composite_full_label","composite_primary_label",
            "close","high","low","ret_1d","ret_5d","ret_10d","mfe_5d","mae_5d",
            "lookahead_safe","lookahead_note"]
    _write_csv(path, records, cols)
    return {filename: path}


def _write_single_bar_role(records: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    """
    For each (pivot_type, offset, t_signal or z_signal) combination,
    count occurrences and compute average forward returns.
    Roles are discovered statistically — T is not assumed bullish, Z not assumed bearish.
    """
    # Separate by pivot type
    agg: Dict[Tuple, Dict] = defaultdict(lambda: {
        "count": 0, "ret_1d_sum": 0.0, "ret_5d_sum": 0.0, "ret_10d_sum": 0.0,
        "mfe_5d_sum": 0.0, "mae_5d_sum": 0.0,
        "ret_1d_n": 0, "ret_5d_n": 0, "ret_10d_n": 0, "mfe_5d_n": 0, "mae_5d_n": 0,
    })

    for rec in records:
        pt = rec["pivot_type"]
        offset = rec["offset"]
        # Analyze both T and Z signals
        for sig_type in ("t_signal", "z_signal"):
            sig_val = str(rec.get(sig_type, "") or "").strip()
            if not sig_val:
                continue
            key = (pt, offset, sig_type, sig_val)
            bucket = agg[key]
            bucket["count"] += 1
            for ret_col in ("ret_1d", "ret_5d", "ret_10d", "mfe_5d", "mae_5d"):
                v = rec.get(ret_col, "")
                try:
                    fv = float(v)
                    bucket[f"{ret_col}_sum"] += fv
                    bucket[f"{ret_col}_n"] += 1
                except (TypeError, ValueError):
                    pass

    rows = []
    for (pt, offset, sig_type, sig_val), bucket in sorted(agg.items()):
        count = bucket["count"]
        conf_tier = _confidence_tier(count)
        lookahead_safe = offset <= 0
        rows.append({
            "pivot_type": pt,
            "offset": offset,
            "signal_field": sig_type,
            "signal_value": sig_val,
            "count": count,
            "confidence_tier": conf_tier,
            "avg_ret_1d": _safe_avg(bucket["ret_1d_sum"], bucket["ret_1d_n"]),
            "avg_ret_5d": _safe_avg(bucket["ret_5d_sum"], bucket["ret_5d_n"]),
            "avg_ret_10d": _safe_avg(bucket["ret_10d_sum"], bucket["ret_10d_n"]),
            "avg_mfe_5d": _safe_avg(bucket["mfe_5d_sum"], bucket["mfe_5d_n"]),
            "avg_mae_5d": _safe_avg(bucket["mae_5d_sum"], bucket["mae_5d_n"]),
            "lookahead_safe": lookahead_safe,
            "lookahead_note": "LIVE_SAFE" if lookahead_safe else f"RESEARCH_ONLY (offset={offset})",
        })

    filename = f"single_bar_role_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    cols = ["pivot_type","offset","signal_field","signal_value","count","confidence_tier",
            "avg_ret_1d","avg_ret_5d","avg_ret_10d","avg_mfe_5d","avg_mae_5d",
            "lookahead_safe","lookahead_note"]
    _write_csv(path, rows, cols)
    return {filename: path}


def _write_sequence_analysis(records: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    """
    Build 2–6 bar signal sequences within each pivot's offset window.
    A sequence is defined by consecutive offsets within the window.
    Only within-window sequences (not crossing pivot boundaries).
    """
    # Group records by (pivot_type, pivot_idx)
    by_pivot: Dict[Tuple, List[Dict]] = defaultdict(list)
    for rec in records:
        key = (rec["pivot_type"], rec["pivot_idx"])
        by_pivot[key].append(rec)

    # Sort each group by offset
    for key in by_pivot:
        by_pivot[key].sort(key=lambda r: r["offset"])

    seq_agg: Dict[Tuple, Dict] = defaultdict(lambda: {
        "count": 0, "ret_5d_sum": 0.0, "ret_5d_n": 0,
        "mfe_5d_sum": 0.0, "mfe_5d_n": 0,
    })

    for (pt, _), recs in by_pivot.items():
        # Build offset→signal map
        off_map = {r["offset"]: r for r in recs}
        offsets = sorted(off_map.keys())

        for seq_len in range(2, 7):
            for start_pos in range(len(offsets) - seq_len + 1):
                window_offsets = offsets[start_pos:start_pos + seq_len]
                # Require consecutive offsets
                if window_offsets[-1] - window_offsets[0] != seq_len - 1:
                    continue
                # Check lookahead safety: all offsets <= 0 → live_safe
                all_safe = all(o <= 0 for o in window_offsets)
                start_off = window_offsets[0]
                end_off = window_offsets[-1]

                # Build sequence token from t+z signals
                tokens = []
                for o in window_offsets:
                    r = off_map[o]
                    t = str(r.get("t_signal", "") or "")
                    z = str(r.get("z_signal", "") or "")
                    tok = t if t else (z if z else "—")
                    tokens.append(tok)
                seq_str = "|".join(tokens)

                key = (pt, start_off, end_off, seq_len, seq_str)
                bkt = seq_agg[key]
                bkt["count"] += 1
                # Use the forward return at the END bar of the sequence
                end_rec = off_map[end_off]
                for ret_col in ("ret_5d", "mfe_5d"):
                    v = end_rec.get(ret_col, "")
                    try:
                        fv = float(v)
                        bkt[f"{ret_col}_sum"] += fv
                        bkt[f"{ret_col}_n"] += 1
                    except (TypeError, ValueError):
                        pass
                bkt["lookahead_safe"] = all_safe

    rows = []
    for (pt, start_off, end_off, seq_len, seq_str), bkt in sorted(seq_agg.items()):
        count = bkt["count"]
        rows.append({
            "pivot_type": pt,
            "seq_start_offset": start_off,
            "seq_end_offset": end_off,
            "seq_len": seq_len,
            "signal_sequence": seq_str,
            "count": count,
            "confidence_tier": _confidence_tier(count),
            "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
            "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
            "lookahead_safe": bkt.get("lookahead_safe", False),
            "lookahead_note": (
                "LIVE_SAFE" if bkt.get("lookahead_safe") else
                f"RESEARCH_ONLY (end offset={end_off})"
            ),
        })

    rows.sort(key=lambda r: (-r["count"], r["pivot_type"], r["seq_start_offset"]))

    filename = f"sequence_analysis_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    cols = ["pivot_type","seq_start_offset","seq_end_offset","seq_len","signal_sequence",
            "count","confidence_tier","avg_ret_5d","avg_mfe_5d",
            "lookahead_safe","lookahead_note"]
    _write_csv(path, rows, cols)
    return {filename: path}


def _write_composite_analysis(records: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    """
    Composite feature analysis: signal + l_signal + volume_bucket + full_suffix +
    bar_body_wick + bar_gap_range + bar_line5 token combinations at each pivot offset.
    """
    agg: Dict[Tuple, Dict] = defaultdict(lambda: {
        "count": 0, "ret_5d_sum": 0.0, "ret_5d_n": 0,
        "mfe_5d_sum": 0.0, "mfe_5d_n": 0, "mae_5d_sum": 0.0, "mae_5d_n": 0,
    })

    for rec in records:
        pt = rec["pivot_type"]
        offset = rec["offset"]
        # Build composite token
        t = str(rec.get("t_signal", "") or "")
        z = str(rec.get("z_signal", "") or "")
        primary_sig = t if t else (z if z else "")
        if not primary_sig:
            continue

        composite = "|".join([
            primary_sig,
            str(rec.get("l_signal", "") or ""),
            str(rec.get("volume_bucket", "") or ""),
            str(rec.get("full_suffix", "") or ""),
            str(rec.get("bar_body_wick", "") or ""),
            str(rec.get("bar_gap_range", "") or ""),
            str(rec.get("bar_line5", "") or ""),
        ])

        key = (pt, offset, composite)
        bkt = agg[key]
        bkt["count"] += 1
        for ret_col in ("ret_5d", "mfe_5d", "mae_5d"):
            v = rec.get(ret_col, "")
            try:
                fv = float(v)
                bkt[f"{ret_col}_sum"] += fv
                bkt[f"{ret_col}_n"] += 1
            except (TypeError, ValueError):
                pass

    rows = []
    for (pt, offset, composite), bkt in sorted(agg.items()):
        count = bkt["count"]
        rows.append({
            "pivot_type": pt,
            "offset": offset,
            "composite_feature": composite,
            "count": count,
            "confidence_tier": _confidence_tier(count),
            "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
            "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
            "avg_mae_5d": _safe_avg(bkt["mae_5d_sum"], bkt["mae_5d_n"]),
            "lookahead_safe": offset <= 0,
            "lookahead_note": "LIVE_SAFE" if offset <= 0 else f"RESEARCH_ONLY (offset={offset})",
        })

    rows.sort(key=lambda r: (-r["count"], r["pivot_type"], r["offset"]))

    filename = f"composite_analysis_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    cols = ["pivot_type","offset","composite_feature","count","confidence_tier",
            "avg_ret_5d","avg_mfe_5d","avg_mae_5d","lookahead_safe","lookahead_note"]
    _write_csv(path, rows, cols)
    return {filename: path}


def _write_go_gate_analysis(records: List[Dict], ticker: str, out_dir: str) -> Dict[str, str]:
    """
    Filter records through strict GO gate (volume_bucket in [B, VB]) and
    analyze signal behavior separately for GO vs non-GO bars.
    """
    for go_filter in (True, False):
        label = "GO" if go_filter else "ALL"
        filtered = [
            r for r in records
            if (not go_filter) or (str(r.get("volume_bucket", "")) in GO_VOLUME_BUCKETS)
        ]

        agg: Dict[Tuple, Dict] = defaultdict(lambda: {
            "count": 0, "ret_5d_sum": 0.0, "ret_5d_n": 0,
            "mfe_5d_sum": 0.0, "mfe_5d_n": 0,
        })

        for rec in filtered:
            pt = rec["pivot_type"]
            offset = rec["offset"]
            for sig_type in ("t_signal", "z_signal"):
                sig_val = str(rec.get(sig_type, "") or "").strip()
                if not sig_val:
                    continue
                key = (pt, offset, sig_type, sig_val)
                bkt = agg[key]
                bkt["count"] += 1
                for ret_col in ("ret_5d", "mfe_5d"):
                    v = rec.get(ret_col, "")
                    try:
                        fv = float(v)
                        bkt[f"{ret_col}_sum"] += fv
                        bkt[f"{ret_col}_n"] += 1
                    except (TypeError, ValueError):
                        pass

        rows = []
        for (pt, offset, sig_type, sig_val), bkt in sorted(agg.items()):
            count = bkt["count"]
            rows.append({
                "gate_filter": label,
                "pivot_type": pt,
                "offset": offset,
                "signal_field": sig_type,
                "signal_value": sig_val,
                "count": count,
                "confidence_tier": _confidence_tier(count),
                "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
                "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
                "lookahead_safe": offset <= 0,
                "lookahead_note": "LIVE_SAFE" if offset <= 0 else f"RESEARCH_ONLY (offset={offset})",
            })

        rows.sort(key=lambda r: (-r["count"], r["pivot_type"], r["offset"]))

        filename = f"go_gate_analysis_{label.lower()}_{ticker}.csv"
        path = os.path.join(out_dir, filename)
        cols = ["gate_filter","pivot_type","offset","signal_field","signal_value",
                "count","confidence_tier","avg_ret_5d","avg_mfe_5d",
                "lookahead_safe","lookahead_note"]
        _write_csv(path, rows, cols)

    return {
        f"go_gate_analysis_go_{ticker}.csv": os.path.join(out_dir, f"go_gate_analysis_go_{ticker}.csv"),
        f"go_gate_analysis_all_{ticker}.csv": os.path.join(out_dir, f"go_gate_analysis_all_{ticker}.csv"),
    }


def _write_markdown_reports(
    records: List[Dict],
    swings: List[Dict],
    ticker: str,
    out_dir: str,
) -> Dict[str, str]:
    """Generate two Markdown reports: (a) full research findings, (b) live-safe rules only."""

    up_swings = [s for s in swings if s["direction"] == "UP"]
    dn_swings = [s for s in swings if s["direction"] == "DOWN"]

    # Aggregate top signals at pivot LOW offset 0 and pivot HIGH offset 0
    def _top_signals(pt: str, offset: int, top_n: int = 10):
        agg = defaultdict(int)
        for rec in records:
            if rec["pivot_type"] != pt or rec["offset"] != offset:
                continue
            for sig_type in ("t_signal", "z_signal"):
                sv = str(rec.get(sig_type, "") or "").strip()
                if sv:
                    agg[f"{sig_type}={sv}"] += 1
        return sorted(agg.items(), key=lambda x: -x[1])[:top_n]

    def _render_table(items, headers):
        rows_md = [" | ".join(headers), " | ".join(["---"] * len(headers))]
        for item in items:
            rows_md.append(" | ".join(str(v) for v in item))
        return "\n".join(rows_md)

    # Full research report
    lines = [
        f"# Pivot Swing Character Analytics — {ticker}",
        "",
        "> **IMPORTANT:** Sections marked `RESEARCH_ONLY` use future bars relative to",
        "> the pivot price bar. They are valid for backtesting but MUST NOT be applied",
        "> in live trading. Sections marked `LIVE_SAFE` use only bars at or before the",
        "> pivot price extreme (offset ≤ 0), but note that the pivot is not confirmed",
        "> until `pivot_right` bars close after the extreme.",
        "",
        "## Swing Statistics",
        "",
        f"- Total swings: **{len(swings)}**",
        f"- UP swings: {len(up_swings)}",
        f"- DOWN swings: {len(dn_swings)}",
    ]

    if swings:
        avg_ret = sum(s["return_pct"] for s in swings) / len(swings)
        avg_bars = sum(s["bar_count"] for s in swings) / len(swings)
        lines += [
            f"- Avg return: {avg_ret:.2f}%",
            f"- Avg bar count: {avg_bars:.1f}",
        ]

    lines += [
        "",
        "## Top Signals at Pivot LOW (offset=0) — `LIVE_SAFE` relative to price extreme",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("LOW", 0)],
            ["Signal", "Count"],
        ),
        "",
        "## Top Signals at Pivot HIGH (offset=0) — `LIVE_SAFE` relative to price extreme",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("HIGH", 0)],
            ["Signal", "Count"],
        ),
        "",
        "## Top Signals at Pivot LOW (offset=+1) — `RESEARCH_ONLY`",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("LOW", 1)],
            ["Signal", "Count"],
        ),
        "",
        "## Top Signals at Pivot HIGH (offset=+1) — `RESEARCH_ONLY`",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("HIGH", 1)],
            ["Signal", "Count"],
        ),
        "",
        "## Confidence Tiers",
        "",
        f"- HIGH: ≥ {CONF_HIGH} occurrences",
        f"- MEDIUM: ≥ {CONF_MEDIUM} occurrences",
        f"- LOW: ≥ {CONF_LOW} occurrences",
        f"- RESEARCH_ONLY: < {CONF_LOW} occurrences",
        "",
        "---",
        "_Generated by Pivot Swing Character Analytics Engine_",
    ]

    full_md = "\n".join(lines)
    full_filename = f"pivot_analytics_report_{ticker}.md"
    full_path = os.path.join(out_dir, full_filename)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(full_md)

    # Live-safe rules only
    live_lines = [
        f"# Live-Safe Pivot Rules — {ticker}",
        "",
        "> These rules use ONLY bars at offset ≤ 0 from the pivot price extreme.",
        "> The pivot is confirmed `pivot_right` bars after the extreme.",
        "> Do NOT enter a trade based on these rules until the pivot is confirmed.",
        "",
        "## At Pivot LOW (offset=0)",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("LOW", 0)],
            ["Signal", "Count"],
        ),
        "",
        "## At Pivot LOW (offset=-1, one bar before low)",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("LOW", -1)],
            ["Signal", "Count"],
        ),
        "",
        "## At Pivot HIGH (offset=0)",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("HIGH", 0)],
            ["Signal", "Count"],
        ),
        "",
        "## At Pivot HIGH (offset=-1, one bar before high)",
        "",
        _render_table(
            [(sig, cnt) for sig, cnt in _top_signals("HIGH", -1)],
            ["Signal", "Count"],
        ),
        "",
        "---",
        "_Generated by Pivot Swing Character Analytics Engine — Live-Safe Section Only_",
    ]

    live_md = "\n".join(live_lines)
    live_filename = f"pivot_live_rules_{ticker}.md"
    live_path = os.path.join(out_dir, live_filename)
    with open(live_path, "w", encoding="utf-8") as f:
        f.write(live_md)

    return {
        full_filename: full_path,
        live_filename: live_path,
    }


def _write_version_audit(df: pd.DataFrame, ticker: str, out_dir: str) -> Dict[str, str]:
    """Write a version audit CSV showing the tz_wlnbb_version distribution in the input data."""
    version_col = "tz_wlnbb_version"
    if version_col not in df.columns:
        rows = [{"ticker": ticker, "tz_wlnbb_version": "UNKNOWN", "bar_count": len(df)}]
    else:
        vc = df[version_col].value_counts().reset_index()
        vc.columns = ["tz_wlnbb_version", "bar_count"]
        rows = [{"ticker": ticker, **r} for r in vc.to_dict("records")]

    filename = f"version_audit_{ticker}.csv"
    path = os.path.join(out_dir, filename)
    _write_csv(path, rows, ["ticker", "tz_wlnbb_version", "bar_count"])
    return {filename: path}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: str, rows: List[Dict], cols: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _confidence_tier(count: int) -> str:
    if count >= CONF_HIGH:
        return "HIGH"
    if count >= CONF_MEDIUM:
        return "MEDIUM"
    if count >= CONF_LOW:
        return "LOW"
    return "RESEARCH_ONLY"


def _safe_avg(total: float, n: int) -> str:
    if n == 0:
        return ""
    return f"{total / n:.4f}"
