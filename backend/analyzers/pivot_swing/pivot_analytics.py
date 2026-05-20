"""Pivot Swing Character Analytics Engine.

Analyzes TZ/WLNBB signal behavior at confirmed pivot zones (offsets −5..+5),
discovers signal roles statistically, and produces a fixed set of output
files aggregated across all input tickers:

    pivot_swing_summary.csv
    pivot_low_single_signal_stats.csv
    pivot_high_single_signal_stats.csv
    pivot_low_sequence_2bar_stats.csv  …  pivot_low_sequence_6bar_stats.csv
    pivot_high_sequence_2bar_stats.csv …  pivot_high_sequence_6bar_stats.csv
    pivot_zone_offset_stats.csv
    pivot_role_map.csv
    pivot_scanner_rules_proposal.md
    pivot_engine_audit_report.md

CRITICAL DESIGN PRINCIPLES:
- Confirmed pivots only (pivot_right bars close before pivot known).
- All offset windows use bar offsets from pivot price bar (not confirmation bar).
- Rows with offset > 0 are RESEARCH_ONLY (require future bars relative to pivot).
- Rows with offset ≤ 0 are LIVE_SAFE relative to the pivot price extreme but
  the pivot itself is only confirmed at pivot_idx + pivot_right.
- Signal roles are discovered statistically — T is NOT assumed bullish,
  Z is NOT assumed bearish.
- No modification to signal_logic.py / signal_extraction.py / WLNBB L1–L6.
"""
import os
import csv
import glob
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)

OFFSET_RANGE = list(range(-5, 6))
GO_VOLUME_BUCKETS = {"B", "VB"}

CONF_HIGH    = 100
CONF_MEDIUM  = 40
CONF_LOW     = 15

SEQUENCE_LENGTHS = [2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_pivot_analytics(
    csv_path: Optional[str] = None,
    output_dir: str = "/tmp/pivot_analytics",
    pivot_left: int = 3,
    pivot_right: int = 3,
    min_swing_return_pct: float = 3.0,
    min_swing_bars: int = 2,
    ticker: Optional[str] = None,
    csv_paths: Optional[List[str]] = None,
) -> Dict[str, str]:
    """
    Run the pipeline on one or many stock_stat_tz_wlnbb CSV files and produce
    a single aggregated output set.

    Parameters
    ----------
    csv_path  : single CSV path (mutually exclusive with csv_paths)
    csv_paths : list of CSV paths to process and aggregate
    output_dir: directory for the aggregated output files

    Returns
    -------
    dict mapping output file name → absolute path
    """
    from .pivot_detector import detect_pivots
    from .swing_builder import build_swings

    os.makedirs(output_dir, exist_ok=True)

    if csv_paths is None:
        if csv_path:
            csv_paths = [csv_path]
        else:
            csv_paths = []
    if not csv_paths:
        log.warning("pivot_analytics: no input CSVs provided")
        return {}

    all_pivot_rows: List[Dict[str, Any]] = []
    all_swings: List[Dict[str, Any]] = []
    all_zone_records: List[Dict[str, Any]] = []
    tickers_processed: List[str] = []
    version_counts: Dict[str, int] = defaultdict(int)
    audit_per_ticker: List[Dict[str, Any]] = []
    files_inspected: List[str] = []

    for path in csv_paths:
        files_inspected.append(path)
        df = _load_csv(path)
        if df is None or len(df) < (pivot_left + pivot_right + 1):
            log.warning("pivot_analytics: skipping %s (insufficient bars)", path)
            audit_per_ticker.append({
                "csv_path": path, "ticker": "", "bars": 0 if df is None else len(df),
                "pivots_low": 0, "pivots_high": 0, "swings": 0, "skipped": "INSUFFICIENT_BARS",
            })
            continue

        tk = ticker or _infer_ticker_from_path(path, df)
        tickers_processed.append(tk)

        # Version distribution
        if "tz_wlnbb_version" in df.columns:
            for v in df["tz_wlnbb_version"].astype(str).fillna(""):
                version_counts[v] += 1

        df = detect_pivots(df, pivot_left=pivot_left, pivot_right=pivot_right)
        swings = build_swings(df, min_swing_return_pct=min_swing_return_pct,
                              min_swing_bars=min_swing_bars)

        # Pivot rows for summary + role map
        for i in range(len(df)):
            for pt, flag_col, conf_col in [
                ("LOW", "pivot_low", "pivot_low_confirmed_at"),
                ("HIGH", "pivot_high", "pivot_high_confirmed_at"),
            ]:
                if df[flag_col].iloc[i]:
                    all_pivot_rows.append({
                        "ticker": tk,
                        "pivot_type": pt,
                        "pivot_idx": i,
                        "pivot_date": str(df["date"].iloc[i]) if "date" in df.columns else str(i),
                        "pivot_price": float(df["low"].iloc[i] if pt == "LOW" else df["high"].iloc[i]),
                        "confirmed_at_idx": int(df[conf_col].iloc[i]),
                    })

        for sw in swings:
            sw_out = dict(sw)
            sw_out["ticker"] = tk
            all_swings.append(sw_out)

        zone_records = _build_pivot_zone_records(df, swings, tk)
        all_zone_records.extend(zone_records)

        low_pivots = sum(df["pivot_low"])
        high_pivots = sum(df["pivot_high"])
        audit_per_ticker.append({
            "csv_path": path,
            "ticker": tk,
            "bars": len(df),
            "pivots_low": int(low_pivots),
            "pivots_high": int(high_pivots),
            "swings": len(swings),
            "skipped": "",
        })

    outputs: Dict[str, str] = {}

    # 1. pivot_swing_summary.csv (one row per ticker)
    outputs.update(_write_pivot_swing_summary(audit_per_ticker, all_swings, output_dir))

    # 2. single signal stats (LOW + HIGH)
    outputs.update(_write_single_signal_stats(all_zone_records, output_dir))

    # 3. sequence stats per length (2..6)
    outputs.update(_write_sequence_stats(all_zone_records, output_dir))

    # 4. pivot_zone_offset_stats.csv (every (pivot_type, offset) bucket)
    outputs.update(_write_pivot_zone_offset_stats(all_zone_records, output_dir))

    # 5. pivot_role_map.csv (statistical role for each signal at each pivot_type)
    outputs.update(_write_pivot_role_map(all_zone_records, output_dir))

    # 6. pivot_scanner_rules_proposal.md (live-safe rules only)
    outputs.update(_write_scanner_rules_proposal(all_zone_records, output_dir))

    # 7. pivot_engine_audit_report.md (versions, ticker audit, lookahead notes)
    outputs.update(_write_engine_audit(
        audit_per_ticker, version_counts, files_inspected,
        all_swings, all_zone_records, output_dir,
        pivot_left, pivot_right, min_swing_return_pct, min_swing_bars,
    ))

    log.info("pivot_analytics: wrote %d output files to %s", len(outputs), output_dir)
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


def _infer_ticker_from_path(path: str, df: pd.DataFrame) -> str:
    if "ticker" in df.columns and len(df) > 0:
        try:
            v = str(df["ticker"].iloc[0])
            if v:
                return v
        except Exception:
            pass
    base = os.path.basename(path).replace(".csv", "")
    parts = base.split("_")
    return parts[-1] if parts else base


# ---------------------------------------------------------------------------
# Pivot-zone record construction
# ---------------------------------------------------------------------------

def _build_pivot_zone_records(
    df: pd.DataFrame,
    swings: List[Dict[str, Any]],
    ticker: str,
) -> List[Dict[str, Any]]:
    n = len(df)
    dates = df["date"].values if "date" in df.columns else np.arange(n).astype(str)

    pivot_swing_map: Dict[int, Dict] = {}
    for sw in swings:
        pivot_swing_map[sw["start_pivot_idx"]] = sw
        pivot_swing_map[sw["end_pivot_idx"]] = sw

    records: List[Dict[str, Any]] = []

    def _get(row_idx: int, col: str, default="") -> Any:
        if 0 <= row_idx < n and col in df.columns:
            v = df[col].iloc[row_idx]
            if isinstance(v, float) and pd.isna(v):
                return default
            return v
        return default

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
            records.append({
                "ticker": ticker,
                "pivot_type": pivot_type,
                "pivot_idx": i,
                "pivot_date": pivot_date,
                "pivot_price": pivot_price,
                "conf_idx": conf_idx,
                "swing_direction": sw.get("direction", ""),
                "swing_return_pct": sw.get("return_pct", ""),
                "offset": offset,
                "bar_idx": bar_idx,
                "bar_date": str(dates[bar_idx]),
                "t_signal": str(_get(bar_idx, "t_signal", "") or ""),
                "z_signal": str(_get(bar_idx, "z_signal", "") or ""),
                "l_signal": str(_get(bar_idx, "l_signal", "") or ""),
                "volume_bucket": str(_get(bar_idx, "volume_bucket", "") or ""),
                "bar_body_wick": str(_get(bar_idx, "bar_body_wick", "") or ""),
                "bar_gap_range": str(_get(bar_idx, "bar_gap_range", "") or ""),
                "bar_line5": str(_get(bar_idx, "bar_line5", "") or ""),
                "full_suffix": str(_get(bar_idx, "full_suffix", "") or ""),
                "ret_1d": _get(bar_idx, "ret_1d", ""),
                "ret_5d": _get(bar_idx, "ret_5d", ""),
                "ret_10d": _get(bar_idx, "ret_10d", ""),
                "mfe_5d": _get(bar_idx, "mfe_5d", ""),
                "mae_5d": _get(bar_idx, "mae_5d", ""),
                "lookahead_safe": offset <= 0,
            })

    return records


# ---------------------------------------------------------------------------
# 1. pivot_swing_summary.csv
# ---------------------------------------------------------------------------

def _write_pivot_swing_summary(
    audit: List[Dict], swings: List[Dict], out_dir: str,
) -> Dict[str, str]:
    by_ticker: Dict[str, Dict] = {}
    for row in audit:
        tk = row.get("ticker") or ""
        if not tk:
            continue
        by_ticker[tk] = {
            "ticker": tk,
            "bars": row["bars"],
            "pivot_lows": row["pivots_low"],
            "pivot_highs": row["pivots_high"],
            "swings": row["swings"],
            "up_swings": 0,
            "down_swings": 0,
            "avg_swing_return_pct": "",
            "avg_swing_bars": "",
            "skipped": row.get("skipped", ""),
        }

    sums: Dict[str, Dict] = defaultdict(lambda: {"ret_sum": 0.0, "bar_sum": 0, "n": 0,
                                                  "up": 0, "down": 0})
    for sw in swings:
        tk = sw.get("ticker") or ""
        s = sums[tk]
        s["n"] += 1
        s["ret_sum"] += float(sw.get("return_pct") or 0)
        s["bar_sum"] += int(sw.get("bar_count") or 0)
        if sw.get("direction") == "UP":
            s["up"] += 1
        elif sw.get("direction") == "DOWN":
            s["down"] += 1

    for tk, s in sums.items():
        if tk in by_ticker and s["n"] > 0:
            by_ticker[tk]["up_swings"] = s["up"]
            by_ticker[tk]["down_swings"] = s["down"]
            by_ticker[tk]["avg_swing_return_pct"] = round(s["ret_sum"] / s["n"], 3)
            by_ticker[tk]["avg_swing_bars"] = round(s["bar_sum"] / s["n"], 2)

    rows = sorted(by_ticker.values(), key=lambda r: r["ticker"])
    cols = ["ticker","bars","pivot_lows","pivot_highs","swings",
            "up_swings","down_swings","avg_swing_return_pct","avg_swing_bars","skipped"]
    path = os.path.join(out_dir, "pivot_swing_summary.csv")
    _write_csv(path, rows, cols)
    return {"pivot_swing_summary.csv": path}


# ---------------------------------------------------------------------------
# 2. single signal stats (LOW + HIGH)
# ---------------------------------------------------------------------------

def _write_single_signal_stats(records: List[Dict], out_dir: str) -> Dict[str, str]:
    outputs: Dict[str, str] = {}

    for pt in ("LOW", "HIGH"):
        agg: Dict[Tuple, Dict] = defaultdict(_bucket_init)
        for rec in records:
            if rec["pivot_type"] != pt:
                continue
            offset = rec["offset"]
            for sig_type in ("t_signal", "z_signal", "l_signal"):
                sv = rec.get(sig_type, "")
                if not sv:
                    continue
                key = (offset, sig_type, sv)
                _bucket_add(agg[key], rec)

        rows = []
        for (offset, sig_type, sv), bkt in sorted(agg.items()):
            count = bkt["count"]
            rows.append({
                "pivot_type": pt,
                "offset": offset,
                "signal_field": sig_type,
                "signal_value": sv,
                "count": count,
                "confidence_tier": _confidence_tier(count),
                "avg_ret_1d": _safe_avg(bkt["ret_1d_sum"], bkt["ret_1d_n"]),
                "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
                "avg_ret_10d": _safe_avg(bkt["ret_10d_sum"], bkt["ret_10d_n"]),
                "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
                "avg_mae_5d": _safe_avg(bkt["mae_5d_sum"], bkt["mae_5d_n"]),
                "lookahead_safe": offset <= 0,
                "lookahead_note": "LIVE_SAFE" if offset <= 0 else f"RESEARCH_ONLY (offset={offset})",
            })

        rows.sort(key=lambda r: (-r["count"], r["offset"]))
        cols = ["pivot_type","offset","signal_field","signal_value","count","confidence_tier",
                "avg_ret_1d","avg_ret_5d","avg_ret_10d","avg_mfe_5d","avg_mae_5d",
                "lookahead_safe","lookahead_note"]
        fname = f"pivot_{pt.lower()}_single_signal_stats.csv"
        path = os.path.join(out_dir, fname)
        _write_csv(path, rows, cols)
        outputs[fname] = path

    return outputs


# ---------------------------------------------------------------------------
# 3. sequence stats per length (2..6)
# ---------------------------------------------------------------------------

def _write_sequence_stats(records: List[Dict], out_dir: str) -> Dict[str, str]:
    by_pivot: Dict[Tuple, List[Dict]] = defaultdict(list)
    for rec in records:
        key = (rec["ticker"], rec["pivot_type"], rec["pivot_idx"])
        by_pivot[key].append(rec)
    for key in by_pivot:
        by_pivot[key].sort(key=lambda r: r["offset"])

    outputs: Dict[str, str] = {}

    for seq_len in SEQUENCE_LENGTHS:
        for pt in ("LOW", "HIGH"):
            agg: Dict[Tuple, Dict] = defaultdict(lambda: {
                **_bucket_init(),
                "lookahead_safe": True,
            })

            for (tk, p_type, _pidx), recs in by_pivot.items():
                if p_type != pt:
                    continue
                off_map = {r["offset"]: r for r in recs}
                offsets = sorted(off_map.keys())
                if len(offsets) < seq_len:
                    continue
                for start_pos in range(len(offsets) - seq_len + 1):
                    win = offsets[start_pos:start_pos + seq_len]
                    # require consecutive offsets
                    if win[-1] - win[0] != seq_len - 1:
                        continue
                    tokens = []
                    for o in win:
                        r = off_map[o]
                        t = r.get("t_signal", "")
                        z = r.get("z_signal", "")
                        tok = t if t else (z if z else "—")
                        tokens.append(tok)
                    seq_str = "|".join(tokens)
                    start_off, end_off = win[0], win[-1]
                    all_safe = all(o <= 0 for o in win)
                    key = (start_off, end_off, seq_str)
                    bkt = agg[key]
                    # use forward returns at the END bar of the sequence
                    _bucket_add(bkt, off_map[end_off])
                    bkt["lookahead_safe"] = bkt["lookahead_safe"] and all_safe

            rows = []
            for (start_off, end_off, seq_str), bkt in sorted(agg.items()):
                count = bkt["count"]
                rows.append({
                    "pivot_type": pt,
                    "seq_len": seq_len,
                    "seq_start_offset": start_off,
                    "seq_end_offset": end_off,
                    "signal_sequence": seq_str,
                    "count": count,
                    "confidence_tier": _confidence_tier(count),
                    "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
                    "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
                    "avg_mae_5d": _safe_avg(bkt["mae_5d_sum"], bkt["mae_5d_n"]),
                    "lookahead_safe": bool(bkt["lookahead_safe"]),
                    "lookahead_note": (
                        "LIVE_SAFE" if bkt["lookahead_safe"]
                        else f"RESEARCH_ONLY (end offset={end_off})"
                    ),
                })

            rows.sort(key=lambda r: (-r["count"], r["seq_start_offset"]))
            cols = ["pivot_type","seq_len","seq_start_offset","seq_end_offset",
                    "signal_sequence","count","confidence_tier",
                    "avg_ret_5d","avg_mfe_5d","avg_mae_5d",
                    "lookahead_safe","lookahead_note"]
            fname = f"pivot_{pt.lower()}_sequence_{seq_len}bar_stats.csv"
            path = os.path.join(out_dir, fname)
            _write_csv(path, rows, cols)
            outputs[fname] = path

    return outputs


# ---------------------------------------------------------------------------
# 4. pivot_zone_offset_stats.csv — counts + returns per (pivot_type, offset)
# ---------------------------------------------------------------------------

def _write_pivot_zone_offset_stats(records: List[Dict], out_dir: str) -> Dict[str, str]:
    agg: Dict[Tuple, Dict] = defaultdict(_bucket_init)
    for rec in records:
        key = (rec["pivot_type"], rec["offset"])
        _bucket_add(agg[key], rec)

    rows = []
    for (pt, offset), bkt in sorted(agg.items()):
        count = bkt["count"]
        rows.append({
            "pivot_type": pt,
            "offset": offset,
            "count": count,
            "confidence_tier": _confidence_tier(count),
            "avg_ret_1d": _safe_avg(bkt["ret_1d_sum"], bkt["ret_1d_n"]),
            "avg_ret_5d": _safe_avg(bkt["ret_5d_sum"], bkt["ret_5d_n"]),
            "avg_ret_10d": _safe_avg(bkt["ret_10d_sum"], bkt["ret_10d_n"]),
            "avg_mfe_5d": _safe_avg(bkt["mfe_5d_sum"], bkt["mfe_5d_n"]),
            "avg_mae_5d": _safe_avg(bkt["mae_5d_sum"], bkt["mae_5d_n"]),
            "lookahead_safe": offset <= 0,
            "lookahead_note": "LIVE_SAFE" if offset <= 0 else f"RESEARCH_ONLY (offset={offset})",
        })

    cols = ["pivot_type","offset","count","confidence_tier",
            "avg_ret_1d","avg_ret_5d","avg_ret_10d","avg_mfe_5d","avg_mae_5d",
            "lookahead_safe","lookahead_note"]
    path = os.path.join(out_dir, "pivot_zone_offset_stats.csv")
    _write_csv(path, rows, cols)
    return {"pivot_zone_offset_stats.csv": path}


# ---------------------------------------------------------------------------
# 5. pivot_role_map.csv — discovered statistical role per signal
# ---------------------------------------------------------------------------

def _write_pivot_role_map(records: List[Dict], out_dir: str) -> Dict[str, str]:
    """
    For each signal value, count occurrences at pivot LOW vs pivot HIGH at
    offset 0 (live-safe). Discover its statistical role: BULLISH_REVERSAL
    (more common at lows), BEARISH_REVERSAL (more common at highs), NEUTRAL.
    """
    counts: Dict[Tuple, Dict] = defaultdict(lambda: {"LOW": 0, "HIGH": 0,
                                                      "LOW_ret5_sum": 0.0, "LOW_ret5_n": 0,
                                                      "HIGH_ret5_sum": 0.0, "HIGH_ret5_n": 0})

    for rec in records:
        if rec["offset"] != 0:
            continue
        pt = rec["pivot_type"]
        for sig_type in ("t_signal", "z_signal", "l_signal"):
            sv = rec.get(sig_type, "")
            if not sv:
                continue
            key = (sig_type, sv)
            counts[key][pt] += 1
            try:
                rv = float(rec.get("ret_5d") or "")
                counts[key][f"{pt}_ret5_sum"] += rv
                counts[key][f"{pt}_ret5_n"] += 1
            except (TypeError, ValueError):
                pass

    rows = []
    for (sig_type, sv), c in sorted(counts.items()):
        low = c["LOW"]
        high = c["HIGH"]
        total = low + high
        if total == 0:
            continue
        low_pct = low / total
        high_pct = high / total
        if total < CONF_LOW:
            role = "RESEARCH_ONLY"
        elif low_pct >= 0.65:
            role = "BULLISH_REVERSAL"
        elif high_pct >= 0.65:
            role = "BEARISH_REVERSAL"
        else:
            role = "NEUTRAL"

        rows.append({
            "signal_field": sig_type,
            "signal_value": sv,
            "count_at_low": low,
            "count_at_high": high,
            "total": total,
            "low_pct": round(low_pct, 3),
            "high_pct": round(high_pct, 3),
            "discovered_role": role,
            "confidence_tier": _confidence_tier(total),
            "avg_ret5_at_low": _safe_avg(c["LOW_ret5_sum"], c["LOW_ret5_n"]),
            "avg_ret5_at_high": _safe_avg(c["HIGH_ret5_sum"], c["HIGH_ret5_n"]),
            "lookahead_safe": True,
            "lookahead_note": "LIVE_SAFE (offset=0; pivot confirmed pivot_right bars later)",
        })

    rows.sort(key=lambda r: (-r["total"], r["signal_value"]))
    cols = ["signal_field","signal_value","count_at_low","count_at_high","total",
            "low_pct","high_pct","discovered_role","confidence_tier",
            "avg_ret5_at_low","avg_ret5_at_high","lookahead_safe","lookahead_note"]
    path = os.path.join(out_dir, "pivot_role_map.csv")
    _write_csv(path, rows, cols)
    return {"pivot_role_map.csv": path}


# ---------------------------------------------------------------------------
# 6. pivot_scanner_rules_proposal.md — live-safe rules only
# ---------------------------------------------------------------------------

def _write_scanner_rules_proposal(records: List[Dict], out_dir: str) -> Dict[str, str]:
    def _top_signals(pt: str, offset: int, top_n: int = 12):
        agg: Dict[str, Dict] = defaultdict(_bucket_init)
        for rec in records:
            if rec["pivot_type"] != pt or rec["offset"] != offset:
                continue
            for sig_type in ("t_signal", "z_signal", "l_signal"):
                sv = rec.get(sig_type, "")
                if not sv:
                    continue
                key = f"{sig_type}={sv}"
                _bucket_add(agg[key], rec)
        items = sorted(agg.items(), key=lambda kv: -kv[1]["count"])[:top_n]
        return items

    def _table(items, headers):
        out = [" | ".join(headers), " | ".join(["---"] * len(headers))]
        for k, b in items:
            out.append(" | ".join([
                k, str(b["count"]), _confidence_tier(b["count"]),
                _safe_avg(b["ret_5d_sum"], b["ret_5d_n"]),
                _safe_avg(b["mfe_5d_sum"], b["mfe_5d_n"]),
            ]))
        return "\n".join(out)

    headers = ["Signal", "Count", "Confidence", "Avg Ret 5d", "Avg MFE 5d"]
    lines = [
        "# Pivot Scanner Rules Proposal — Live-Safe Only",
        "",
        "> Generated by the Pivot Swing Character Analytics Engine.",
        "> Rules below use ONLY bars at offset ≤ 0 from the pivot price extreme.",
        "> A pivot is confirmed `pivot_right` bars after the extreme — do NOT enter",
        "> a trade on these rules until the pivot is confirmed.",
        "",
        f"## Live-safe candidates at PIVOT LOW (offset=0, top {12})",
        "",
        _table(_top_signals("LOW", 0), headers),
        "",
        "## Live-safe candidates at PIVOT LOW (offset=-1, one bar before low)",
        "",
        _table(_top_signals("LOW", -1), headers),
        "",
        "## Live-safe candidates at PIVOT HIGH (offset=0)",
        "",
        _table(_top_signals("HIGH", 0), headers),
        "",
        "## Live-safe candidates at PIVOT HIGH (offset=-1, one bar before high)",
        "",
        _table(_top_signals("HIGH", -1), headers),
        "",
        "## Recommended live-scanner integration modules",
        "",
        "1. **Pivot Detector** — `pivot_left=3`, `pivot_right=3` (or 5 for stricter)",
        "2. **Pivot Confirmation Gate** — only act after `pivot_right` bars close",
        "3. **Signal Role Lookup** — `pivot_role_map.csv` mapping signal → role",
        "4. **Volume Bucket Filter** — restrict to `B` / `VB`",
        "5. **ABR + STAT_COMP Gate** — reuse existing final_normalizer gates",
        "6. **Confidence Tier Floor** — require role lookup ≥ MEDIUM (count ≥ 40)",
        "7. **Per-side Sequence Confirm** — require LOW-side bull sequence or HIGH-side bear sequence to match `pivot_*_sequence_*bar_stats.csv`",
        "",
        "## Confidence Tiers",
        f"- HIGH: ≥ {CONF_HIGH} occurrences",
        f"- MEDIUM: ≥ {CONF_MEDIUM} occurrences",
        f"- LOW: ≥ {CONF_LOW} occurrences",
        f"- RESEARCH_ONLY: < {CONF_LOW} occurrences",
        "",
        "---",
        "_Pivot Swing Character Analytics Engine — live-safe section only_",
    ]

    path = os.path.join(out_dir, "pivot_scanner_rules_proposal.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return {"pivot_scanner_rules_proposal.md": path}


# ---------------------------------------------------------------------------
# 7. pivot_engine_audit_report.md
# ---------------------------------------------------------------------------

def _write_engine_audit(
    audit_per_ticker: List[Dict],
    version_counts: Dict[str, int],
    files_inspected: List[str],
    swings: List[Dict],
    records: List[Dict],
    out_dir: str,
    pivot_left: int,
    pivot_right: int,
    min_swing_return_pct: float,
    min_swing_bars: int,
) -> Dict[str, str]:
    total_bars = sum(r.get("bars", 0) for r in audit_per_ticker)
    total_pivots = sum(r.get("pivots_low", 0) + r.get("pivots_high", 0)
                       for r in audit_per_ticker)
    total_swings = sum(r.get("swings", 0) for r in audit_per_ticker)
    skipped = [r for r in audit_per_ticker if r.get("skipped")]
    research_only_count = sum(1 for r in records if not r.get("lookahead_safe"))
    live_safe_count = sum(1 for r in records if r.get("lookahead_safe"))

    lines = [
        "# Pivot Engine Audit Report",
        "",
        "## Run parameters",
        "",
        f"- `pivot_left` = {pivot_left}",
        f"- `pivot_right` = {pivot_right}",
        f"- `min_swing_return_pct` = {min_swing_return_pct}",
        f"- `min_swing_bars` = {min_swing_bars}",
        f"- offset window = {OFFSET_RANGE[0]}..{OFFSET_RANGE[-1]}",
        "",
        "## Input files inspected",
        "",
        f"- Total CSVs: **{len(files_inspected)}**",
    ]
    for p in files_inspected:
        lines.append(f"  - `{p}`")

    lines += [
        "",
        "## Data version distribution (`tz_wlnbb_version` column)",
        "",
        "| version | bar count |",
        "| --- | --- |",
    ]
    if version_counts:
        for v, c in sorted(version_counts.items(), key=lambda kv: -kv[1]):
            lines.append(f"| `{v or '(empty)'}` | {c} |")
    else:
        lines.append("| _(no version column in input)_ | _(n/a)_ |")

    lines += [
        "",
        "## Per-ticker audit",
        "",
        "| ticker | bars | pivot_lows | pivot_highs | swings | skipped |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in audit_per_ticker:
        lines.append(
            f"| {r.get('ticker','')} | {r.get('bars',0)} | "
            f"{r.get('pivots_low',0)} | {r.get('pivots_high',0)} | "
            f"{r.get('swings',0)} | {r.get('skipped','')} |"
        )

    lines += [
        "",
        "## Aggregate totals",
        "",
        f"- Total bars: **{total_bars}**",
        f"- Total confirmed pivots: **{total_pivots}**",
        f"- Total swings: **{total_swings}**",
        f"- Zone records (LIVE_SAFE, offset ≤ 0): **{live_safe_count}**",
        f"- Zone records (RESEARCH_ONLY, offset > 0): **{research_only_count}**",
        "",
        "## Lookahead-safety policy",
        "",
        "- **LIVE_SAFE** = offset ≤ 0 relative to pivot price extreme.",
        "  Pivot still requires `pivot_right` bars to close before confirmation.",
        "  Live scanners may use these rows only AFTER the pivot is confirmed.",
        "- **RESEARCH_ONLY** = offset > 0 (uses bars after the pivot extreme).",
        "  May be used for backtesting / research but MUST NOT enter a trade rule.",
        "",
        "## Skipped tickers",
        "",
    ]
    if skipped:
        for r in skipped:
            lines.append(f"- `{r.get('ticker') or r.get('csv_path')}` — {r.get('skipped')}")
    else:
        lines.append("_None._")

    lines += [
        "",
        "## Security / scope guarantees",
        "",
        "- No modification to `signal_logic.py`, `signal_extraction.py`, WLNBB L1–L6 logic, or candle-pattern logic.",
        "- Engine consumes the existing `stock_stat_tz_wlnbb_*.csv` pipeline as read-only input.",
        "- No API keys read or written by this engine.",
        "",
        "---",
        "_Generated by the Pivot Swing Character Analytics Engine_",
    ]

    path = os.path.join(out_dir, "pivot_engine_audit_report.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return {"pivot_engine_audit_report.md": path}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bucket_init() -> Dict[str, Any]:
    d: Dict[str, Any] = {"count": 0}
    for col in ("ret_1d", "ret_5d", "ret_10d", "mfe_5d", "mae_5d"):
        d[f"{col}_sum"] = 0.0
        d[f"{col}_n"] = 0
    return d


def _bucket_add(bkt: Dict[str, Any], rec: Dict[str, Any]) -> None:
    bkt["count"] += 1
    for col in ("ret_1d", "ret_5d", "ret_10d", "mfe_5d", "mae_5d"):
        v = rec.get(col, "")
        try:
            fv = float(v)
            bkt[f"{col}_sum"] += fv
            bkt[f"{col}_n"] += 1
        except (TypeError, ValueError):
            pass


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
