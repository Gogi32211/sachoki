"""Replay analytics for TZ/WLNBB signals."""
import csv
import io
import json
import random
import re
import statistics
import zipfile
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from .config import TZ_WLNBB_VERSION, DEFAULT_LOOKBACK_TRADING_DAYS

log = logging.getLogger(__name__)


def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def _primary_signal(r: dict) -> str:
    """Get the primary signal name for a row (T > Z > L > PREUP > PREDN)."""
    return (
        r.get("t_signal") or r.get("z_signal") or
        r.get("l_signal") or r.get("preup_signal") or r.get("predn_signal") or ""
    )


def _full_label(r: dict) -> str:
    """Get suffix-aware full label for the primary signal in a row."""
    if r.get("t_signal"):
        return r.get("lane1_label") or r.get("t_signal", "")
    if r.get("z_signal"):
        return r.get("lane3_label") or r.get("z_signal", "")
    if r.get("l_signal"):
        return r.get("lane1_label") or r.get("l_signal", "")
    suf = r.get("ne_suffix", "") + r.get("wick_suffix", "") + r.get("penetration_suffix", "")
    if r.get("preup_signal"):
        return r.get("preup_signal", "") + suf
    if r.get("predn_signal"):
        return r.get("predn_signal", "") + suf
    return ""


def _signal_perf(rows: List[dict]) -> List[dict]:
    """Aggregate signal performance by signal_type + signal_name. Includes median."""
    groups: Dict[tuple, list] = {}
    signal_types = [
        ("T", "t_signal"), ("Z", "z_signal"), ("L", "l_signal"),
        ("PREUP", "preup_signal"), ("PREDN", "predn_signal"),
    ]
    for r in rows:
        uni = r.get("universe", "")
        tf  = r.get("timeframe", "")
        for sig_type, col in signal_types:
            name = r.get(col, "")
            if not name:
                continue
            key = (sig_type, name, uni, tf)
            groups.setdefault(key, []).append(r)

    result = []
    for (sig_type, name, uni, tf), grp in groups.items():
        def _avg(k, g=grp):
            vals = []
            for r in g:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals), 4) if vals else None

        def _med(k, g=grp):
            vals = sorted(v for r in g for v in [_safe_float(r.get(k))] if v is not None)
            if not vals:
                return None
            m = len(vals) // 2
            return round((vals[m - 1] + vals[m]) / 2 if len(vals) % 2 == 0 else vals[m], 4)

        def _rate(k, g=grp):
            vals = []
            for r in g:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        result.append({
            "signal_type": sig_type, "signal_name": name, "universe": uni, "timeframe": tf,
            "count": len(grp),
            "avg_ret_1d":  _avg("ret_1d"),  "avg_ret_3d": _avg("ret_3d"),
            "avg_ret_5d":  _avg("ret_5d"),  "avg_ret_10d": _avg("ret_10d"),
            "median_ret_5d": _med("ret_5d"), "median_ret_10d": _med("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"),
            "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "tz_wlnbb_version": TZ_WLNBB_VERSION,
        })
    return sorted(result, key=lambda x: -(x["count"] or 0))


def _combo_perf(rows: List[dict]) -> List[dict]:
    """Aggregate by combination of t/z/l/preup/predn/ne/wick. Includes median."""
    groups: Dict[tuple, list] = {}
    for r in rows:
        pen = r.get("penetration_suffix", "")
        key = (
            r.get("t_signal", ""), r.get("z_signal", ""), r.get("l_signal", ""),
            r.get("preup_signal", ""), r.get("predn_signal", ""),
            r.get("ne_suffix", ""), r.get("wick_suffix", ""), pen,
        )
        groups.setdefault(key, []).append(r)

    result = []
    for (ts, zs, ls, ps, ds, ne, wk, pen), grp in groups.items():
        if not any([ts, zs, ls, ps, ds]):
            continue

        def _avg(k, g=grp):
            vals = []
            for r in g:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals), 4) if vals else None

        def _med(k, g=grp):
            vals = sorted(v for r in g for v in [_safe_float(r.get(k))] if v is not None)
            if not vals:
                return None
            m = len(vals) // 2
            return round((vals[m - 1] + vals[m]) / 2 if len(vals) % 2 == 0 else vals[m], 4)

        def _rate(k, g=grp):
            vals = []
            for r in g:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        result.append({
            "t_signal": ts, "z_signal": zs, "l_signal": ls,
            "preup_signal": ps, "predn_signal": ds, "ne_suffix": ne, "wick_suffix": wk,
            "penetration_suffix": pen,
            "full_suffix": ne + wk + pen,
            "count": len(grp),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "median_ret_5d": _med("ret_5d"), "median_ret_10d": _med("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"), "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "tz_wlnbb_version": TZ_WLNBB_VERSION,
        })
    return sorted(result, key=lambda x: -(x["count"] or 0))


def _suffix_perf(rows: List[dict]) -> List[dict]:
    """
    Group by signal_family + signal_name + ne_suffix + wick_suffix + penetration_suffix.
    Answers: does penetration suffix improve/weaken signal quality?
    """
    groups: Dict[tuple, list] = {}
    signal_cols = [
        ("T", "t_signal"), ("Z", "z_signal"), ("L", "l_signal"),
        ("PREUP", "preup_signal"), ("PREDN", "predn_signal"),
    ]
    for r in rows:
        ne  = r.get("ne_suffix", "")
        wk  = r.get("wick_suffix", "")
        pen = r.get("penetration_suffix", "")
        uni = r.get("universe", "")
        tf  = r.get("timeframe", "")
        for sig_type, col in signal_cols:
            name = r.get(col, "")
            if not name:
                continue
            key = (sig_type, name, ne, wk, pen, ne + wk + pen, uni, tf)
            groups.setdefault(key, []).append(r)

    result = []
    for (sig_type, name, ne, wk, pen, full_suf, uni, tf), grp in groups.items():
        def _avg(k, g=grp):
            vals = [v for r in g for v in [_safe_float(r.get(k))] if v is not None]
            return round(sum(vals) / len(vals), 4) if vals else None

        def _med(k, g=grp):
            vals = sorted(v for r in g for v in [_safe_float(r.get(k))] if v is not None)
            if not vals: return None
            m = len(vals) // 2
            return round((vals[m-1] + vals[m]) / 2 if len(vals) % 2 == 0 else vals[m], 4)

        def _rate(k, g=grp):
            vals = [v for r in g for v in [_safe_float(r.get(k))] if v is not None]
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        result.append({
            "signal_type": sig_type, "signal_name": name,
            "ne_suffix": ne, "wick_suffix": wk, "penetration_suffix": pen,
            "full_suffix": full_suf,
            "universe": uni, "timeframe": tf, "count": len(grp),
            "avg_ret_1d": _avg("ret_1d"), "avg_ret_3d": _avg("ret_3d"),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "median_ret_5d": _med("ret_5d"), "median_ret_10d": _med("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"),
            "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
        })
    return sorted(result, key=lambda x: -(x["count"] or 0))


def _sequence_perf_expanded(rows: List[dict]) -> List[dict]:
    """
    Detect 2-bar and 3-bar sequences across all signal families.
    Rows must be sorted by ticker+date.
    """
    from .config import signal_family, sequence_family

    # Group by ticker
    by_ticker: Dict[str, list] = {}
    for r in rows:
        by_ticker.setdefault(r.get("ticker", ""), []).append(r)

    # Sort each ticker chronologically (parse dates to avoid lexicographic mis-sort)
    def _parse_date(d: str):
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            return datetime.min

    for t in by_ticker:
        by_ticker[t].sort(key=lambda x: _parse_date(x.get("date", "")))

    seq2_groups: Dict[tuple, list] = {}  # (family, pattern, lag, uni, tf) -> [outcome_rows]
    seq2_labels: Dict[tuple, tuple] = {}  # key -> (source_full_label, confirmation_full_label)
    seq3_groups: Dict[tuple, list] = {}  # (family, pattern, lag1, lag2, uni, tf) -> [outcome_rows]
    seq3_labels: Dict[tuple, tuple] = {}  # key -> (source_full_label, confirmation_full_label)

    for ticker, t_rows in by_ticker.items():
        n = len(t_rows)
        for i in range(n):
            curr = t_rows[i]
            curr_sig = _primary_signal(curr)
            if not curr_sig:
                continue
            uni = curr.get("universe", "")
            timeframe = curr.get("timeframe", "")

            # 2-bar sequences: look back 1–5 bars
            for lag in range(1, 6):
                if i - lag < 0:
                    break
                prev = t_rows[i - lag]
                prev_sig = _primary_signal(prev)
                if not prev_sig:
                    continue
                fam = sequence_family(prev_sig, curr_sig)
                if not fam:
                    continue
                pattern = f"{prev_sig}->{curr_sig}"
                key = (fam, pattern, lag, uni, timeframe)
                seq2_groups.setdefault(key, []).append(curr)
                if key not in seq2_labels:
                    seq2_labels[key] = (_full_label(prev), _full_label(curr))
                break  # only closest prev signal per current bar

            # 3-bar sequences: look back combinations
            for lag1 in range(1, 4):
                if i - lag1 < 0:
                    break
                mid = t_rows[i - lag1]
                mid_sig = _primary_signal(mid)
                if not mid_sig:
                    continue
                for lag2 in range(lag1 + 1, lag1 + 4):
                    if i - lag2 < 0:
                        break
                    prev2 = t_rows[i - lag2]
                    prev2_sig = _primary_signal(prev2)
                    if not prev2_sig:
                        break
                    fam = (f"{signal_family(prev2_sig)}_to_"
                           f"{signal_family(mid_sig)}_to_"
                           f"{signal_family(curr_sig)}")
                    pattern = f"{prev2_sig}->{mid_sig}->{curr_sig}"
                    key = (fam, pattern, lag1, lag2, uni, timeframe)
                    seq3_groups.setdefault(key, []).append(curr)
                    if key not in seq3_labels:
                        seq3_labels[key] = (_full_label(prev2), _full_label(curr))
                    break
                break

    result = []

    def _make_helpers(grp):
        def _avg(k):
            vals = []
            for r in grp:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals), 4) if vals else None

        def _med(k):
            vals = sorted(v for r in grp for v in [_safe_float(r.get(k))] if v is not None)
            if not vals:
                return None
            m = len(vals) // 2
            return round((vals[m - 1] + vals[m]) / 2 if len(vals) % 2 == 0 else vals[m], 4)

        def _rate(k):
            vals = []
            for r in grp:
                v = _safe_float(r.get(k))
                if v is not None:
                    vals.append(v)
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        return _avg, _med, _rate

    for (fam, pattern, lag, uni, tf2), grp in seq2_groups.items():
        _avg, _med, _rate = _make_helpers(grp)
        src_lbl, conf_lbl = seq2_labels.get((fam, pattern, lag, uni, tf2), ("", ""))
        result.append({
            "sequence_type": "2bar",
            "sequence_family": fam, "sequence_pattern": pattern,
            "bars_between": lag, "bars_between_1": lag, "bars_between_2": "",
            "universe": uni, "timeframe": tf2, "count": len(grp),
            "avg_ret_1d": _avg("ret_1d"), "avg_ret_3d": _avg("ret_3d"),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "median_ret_5d": _med("ret_5d"), "median_ret_10d": _med("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"), "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "source_full_label": src_lbl, "confirmation_full_label": conf_lbl,
            "source_full_suffix": src_lbl.lstrip("TZLPDtlzpd0123456789") if src_lbl else "",
            "confirmation_full_suffix": conf_lbl.lstrip("TZLPDtlzpd0123456789") if conf_lbl else "",
        })

    for (fam, pattern, lag1, lag2, uni, tf2), grp in seq3_groups.items():
        _avg, _med, _rate = _make_helpers(grp)
        src_lbl, conf_lbl = seq3_labels.get((fam, pattern, lag1, lag2, uni, tf2), ("", ""))
        result.append({
            "sequence_type": "3bar",
            "sequence_family": fam, "sequence_pattern": pattern,
            "bars_between": lag1, "bars_between_1": lag1, "bars_between_2": lag2,
            "universe": uni, "timeframe": tf2, "count": len(grp),
            "avg_ret_1d": _avg("ret_1d"), "avg_ret_3d": _avg("ret_3d"),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "median_ret_5d": _med("ret_5d"), "median_ret_10d": _med("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"), "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "source_full_label": src_lbl, "confirmation_full_label": conf_lbl,
            "source_full_suffix": src_lbl.lstrip("TZLPDtlzpd0123456789") if src_lbl else "",
            "confirmation_full_suffix": conf_lbl.lstrip("TZLPDtlzpd0123456789") if conf_lbl else "",
        })

    return sorted(result, key=lambda x: -(x["count"] or 0))


def _suspicious_patterns(seq_rows: List[dict]) -> List[dict]:
    """Patterns with avg_ret_10d > 15% and count > 30."""
    result = []
    for r in seq_rows:
        avg10 = _safe_float(r.get("avg_ret_10d"))
        cnt = r.get("count", 0)
        if avg10 is not None and avg10 > 15.0 and int(cnt) > 30:
            result.append(r)
    return sorted(result, key=lambda x: -(_safe_float(x.get("avg_ret_10d")) or 0))


def _date_forward(date_str: str, n: int) -> str:
    """Return date_str + n calendar days as YYYY-MM-DD, or '' on error."""
    try:
        return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=n)).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _return_validation_examples(rows: List[dict], n: int = 200) -> List[dict]:
    """Sample rows with forward returns for manual validation.
    Includes expected future dates (calendar) so values can be verified in a price chart.
    """
    with_returns = [r for r in rows if r.get("ret_10d") not in (None, "", "None")]
    # take spread: first 50, last 50, random middle 100
    sample = with_returns[:50]
    if len(with_returns) > 100:
        mid = with_returns[50:-50]
        sample += random.sample(mid, min(100, len(mid)))
        sample += with_returns[-50:]
    fields = ["ticker", "date", "date_plus_1", "date_plus_3", "date_plus_5", "date_plus_10",
              "close", "ret_1d", "ret_3d", "ret_5d", "ret_10d",
              "max_high_5d", "max_high_10d", "max_drawdown_5d", "max_drawdown_10d",
              "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
              "big_win_10d", "fail_10d", "t_signal", "z_signal", "l_signal"]
    result = []
    for r in sample[:n]:
        row_out = {f: r.get(f, "") for f in fields}
        d = r.get("date", "")
        row_out["date_plus_1"]  = _date_forward(d, 1)
        row_out["date_plus_3"]  = _date_forward(d, 3)
        row_out["date_plus_5"]  = _date_forward(d, 5)
        row_out["date_plus_10"] = _date_forward(d, 10)
        result.append(row_out)
    return result


def _date_order_audit(rows: List[dict]) -> List[dict]:
    """
    Verify chronological ordering of dates per ticker.
    Returns one row per ticker summarising any ordering issues.
    """
    by_ticker: Dict[str, list] = {}
    for r in rows:
        by_ticker.setdefault(r.get("ticker", ""), []).append(r)

    issues = []
    for ticker, t_rows in by_ticker.items():
        dates = [r.get("date", "") for r in t_rows]
        parsed = []
        invalid = 0
        for d in dates:
            try:
                parsed.append(datetime.strptime(d, "%Y-%m-%d"))
            except Exception:
                parsed.append(None)
                invalid += 1
        out_of_order = sum(
            1 for i in range(1, len(parsed))
            if parsed[i] is not None and parsed[i - 1] is not None and parsed[i] < parsed[i - 1]
        )
        issues.append({
            "ticker": ticker,
            "total_rows": len(t_rows),
            "invalid_date_format": invalid,
            "out_of_order_count": out_of_order,
            "first_date": dates[0] if dates else "",
            "last_date": dates[-1] if dates else "",
            "status": "OK" if out_of_order == 0 and invalid == 0 else "ISSUE",
        })

    issues.sort(key=lambda x: -(x["out_of_order_count"] + x["invalid_date_format"]))
    return issues


def _unscored_audit(rows: List[dict]) -> List[dict]:
    """
    Find any signal column values that don't appear in the known signal registry.
    L dynamic combos (e.g. L12, L346) matching ^L[1-6]+$ are considered valid.
    """
    from .config import ALL_KNOWN_SIGNALS, is_known_l_signal
    unknown: Dict[str, dict] = {}
    for r in rows:
        for col in ("t_signal", "z_signal", "l_signal", "preup_signal", "predn_signal"):
            sig = r.get(col, "")
            if not sig:
                continue
            if sig in ALL_KNOWN_SIGNALS:
                continue
            # Dynamic L combos like L12, L346 are valid
            if col == "l_signal" and is_known_l_signal(sig):
                continue
            d = unknown.setdefault(sig, {"raw_signal": sig, "normalized_signal": sig,
                                         "source_column": col, "count": 0,
                                         "tickers": [], "dates": []})
            d["count"] += 1
            if len(d["tickers"]) < 3:
                d["tickers"].append(r.get("ticker", ""))
                d["dates"].append(r.get("date", ""))
    result = []
    for sig, d in unknown.items():
        result.append({
            "raw_signal": d["raw_signal"],
            "normalized_signal": d["normalized_signal"],
            "source_column": d["source_column"],
            "count": d["count"],
            "example_tickers": "|".join(d["tickers"]),
            "example_dates": "|".join(d["dates"]),
        })
    return sorted(result, key=lambda x: -x["count"])


def _build_metadata(rows: List[dict], universe: str, tf: str,
                    ticker_count: int = 0, audit: dict = None) -> dict:
    """Build enhanced metadata dict."""
    dates = sorted(r.get("date", "") for r in rows if r.get("date"))
    start_date = dates[0] if dates else ""
    end_date   = dates[-1] if dates else ""

    # Trading days per ticker
    per_ticker: Dict[str, set] = {}
    for r in rows:
        per_ticker.setdefault(r.get("ticker", ""), set()).add(r.get("date", ""))
    counts = [len(v) for v in per_ticker.values() if v]

    rows_with_fwd = sum(1 for r in rows if r.get("ret_10d") not in (None, "", "None"))
    rows_dropped  = len(rows) - rows_with_fwd

    meta = {
        "version": TZ_WLNBB_VERSION,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "universe": universe,
        "timeframe": tf,
        "ticker_count": ticker_count or len(per_ticker),
        "rows_total": len(rows),
        "start_date": start_date,
        "end_date": end_date,
        "lookback_trading_days_requested": DEFAULT_LOOKBACK_TRADING_DAYS,
        "trading_days_per_ticker_min":    min(counts) if counts else 0,
        "trading_days_per_ticker_median": statistics.median(counts) if counts else 0,
        "trading_days_per_ticker_max":    max(counts) if counts else 0,
        "rows_with_t_signal":     sum(1 for r in rows if r.get("t_signal")),
        "rows_with_z_signal":     sum(1 for r in rows if r.get("z_signal")),
        "rows_with_l_signal":     sum(1 for r in rows if r.get("l_signal")),
        "rows_with_preup":        sum(1 for r in rows if r.get("preup_signal")),
        "rows_with_predn":        sum(1 for r in rows if r.get("predn_signal")),
        "rows_with_combo":        sum(1 for r in rows if r.get("has_tz_l_combo") in (1, "1", True)),
        "rows_with_sequence":     sum(1 for r in rows if r.get("t_after_z_confirmed") in (1, "1", True)),
        "rows_with_forward_returns_available": rows_with_fwd,
        "rows_dropped_due_to_missing_forward_returns": rows_dropped,
    }
    rows_with_pen_p = sum(1 for r in rows if r.get("penetration_suffix") == "P")
    rows_with_pen_r = sum(1 for r in rows if r.get("penetration_suffix") == "R")
    rows_with_pen_h = sum(1 for r in rows if r.get("penetration_suffix") == "H")
    rows_with_any_pen = rows_with_pen_p + rows_with_pen_r + rows_with_pen_h

    meta["rows_with_penetration_p"] = rows_with_pen_p
    meta["rows_with_penetration_r"] = rows_with_pen_r
    meta["rows_with_penetration_h"] = rows_with_pen_h
    meta["rows_with_any_penetration_suffix"] = rows_with_any_pen

    if audit:
        meta.update(audit)
    return meta


def get_config_snapshot() -> dict:
    """Return a pure-dict snapshot of current config (no pandas dependency)."""
    from .config import (
        TZ_WLNBB_VERSION, OUTPUT_SCHEMA_VERSION, DEFAULT_LOOKBACK_TRADING_DAYS,
        USE_WICK, MIN_BODY_RATIO, DOJI_THRESH, WLNBB_MA_PERIOD,
        T_PRIORITY, Z_PRIORITY, PREUP_PRIORITY, PREDN_PRIORITY,
        ALL_KNOWN_SIGNALS, SEQUENCE_FAMILIES,
    )
    return {
        "TZ_WLNBB_ANALYZER_VERSION": TZ_WLNBB_VERSION,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "default_lookback_trading_days": DEFAULT_LOOKBACK_TRADING_DAYS,
        "parameters": {
            "useWick": USE_WICK,
            "minBodyRatio": MIN_BODY_RATIO,
            "dojiThresh": DOJI_THRESH,
            "ma_period": WLNBB_MA_PERIOD,
        },
        "t_priority_order": T_PRIORITY,
        "z_priority_order": Z_PRIORITY,
        "preup_priority_order": PREUP_PRIORITY,
        "predn_priority_order": PREDN_PRIORITY,
        "wlnbb_bucket_logic": "Bollinger Bands period=20 std=1 on volume: W<low, L<mid, N<up, B<up+mid, VB=rest",
        "suffix_logic": {
            "NE": "E if close>high[1] or close<low[1] else N",
            "wick": "U if high>high[1], D if low<low[1], B if both, empty if neither"
        },
        "known_signal_registry": sorted(ALL_KNOWN_SIGNALS),
        "sequence_families_enabled": SEQUENCE_FAMILIES,
    }


def generate_replay_zip(
    rows: List[dict],
    output_path: str = "replay_tz_wlnbb_analytics.zip",
    universe: str = "",
    tf: str = "",
    ticker_count: int = 0,
    audit: dict = None,
) -> str:
    """Generate replay ZIP with all analytics CSVs. Returns output_path."""
    # rows already have forward returns baked in from stock_stat CSV
    sp = _signal_perf(rows)
    cp = _combo_perf(rows)
    sq = _sequence_perf_expanded(rows)

    top = [r for r in sp if (r.get("count") or 0) >= 30 and (_safe_float(r.get("avg_ret_10d")) or 0) > 0]
    top.sort(key=lambda x: -(_safe_float(x.get("avg_ret_10d")) or 0))

    bad = [r for r in sp if (_safe_float(r.get("avg_ret_10d")) or 0) < 0
           or (_safe_float(r.get("fail_10d_rate")) or 0) > 20]
    bad.sort(key=lambda x: (_safe_float(x.get("avg_ret_10d")) or 0))

    suspicious = _suspicious_patterns(sq)
    validation_examples = _return_validation_examples(rows)
    unscored = _unscored_audit(rows)
    date_audit = _date_order_audit(rows)
    meta = _build_metadata(rows, universe, tf, ticker_count, audit)
    config_snap = get_config_snapshot()

    # Attach date audit summary to metadata
    issue_tickers = [r["ticker"] for r in date_audit if r["status"] == "ISSUE"]
    meta["date_order_issues"] = len(issue_tickers)
    meta["date_order_issue_tickers_sample"] = issue_tickers[:10]

    def _to_csv_bytes(data: list, fields: list) -> bytes:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(data)
        return buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        sp_fields = [
            "signal_type", "signal_name", "universe", "timeframe", "count",
            "avg_ret_1d", "avg_ret_3d", "avg_ret_5d", "avg_ret_10d",
            "median_ret_5d", "median_ret_10d", "big_win_10d_rate", "fail_10d_rate",
            "avg_mfe_10d", "avg_mae_10d", "tz_wlnbb_version",
        ]
        zf.writestr("replay_tz_wlnbb_signal_perf.csv", _to_csv_bytes(sp, sp_fields))

        cp_fields = [
            "t_signal", "z_signal", "l_signal", "preup_signal", "predn_signal",
            "ne_suffix", "wick_suffix", "count",
            "avg_ret_5d", "avg_ret_10d", "median_ret_5d", "median_ret_10d",
            "big_win_10d_rate", "fail_10d_rate", "avg_mfe_10d", "avg_mae_10d",
            "tz_wlnbb_version",
        ]
        zf.writestr("replay_tz_wlnbb_combo_perf.csv", _to_csv_bytes(cp, cp_fields))

        sq_fields = [
            "sequence_type", "sequence_family", "sequence_pattern",
            "bars_between", "bars_between_1", "bars_between_2",
            "universe", "timeframe", "count",
            "avg_ret_1d", "avg_ret_3d", "avg_ret_5d", "avg_ret_10d",
            "median_ret_5d", "median_ret_10d",
            "big_win_10d_rate", "fail_10d_rate", "avg_mfe_10d", "avg_mae_10d",
        ]
        zf.writestr("replay_tz_wlnbb_sequence_perf.csv", _to_csv_bytes(sq, sq_fields))
        zf.writestr("replay_tz_wlnbb_top_patterns.csv",  _to_csv_bytes(top, sp_fields))
        zf.writestr("replay_tz_wlnbb_bad_patterns.csv",  _to_csv_bytes(bad, sp_fields))

        unscored_fields = [
            "raw_signal", "normalized_signal", "source_column",
            "count", "example_tickers", "example_dates",
        ]
        zf.writestr("replay_tz_wlnbb_unscored_or_unknown.csv",
                    _to_csv_bytes(unscored, unscored_fields))

        val_fields = [
            "ticker", "date", "date_plus_1", "date_plus_3", "date_plus_5", "date_plus_10",
            "close", "ret_1d", "ret_3d", "ret_5d", "ret_10d",
            "max_high_5d", "max_high_10d", "max_drawdown_5d", "max_drawdown_10d",
            "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
            "big_win_10d", "fail_10d", "t_signal", "z_signal", "l_signal",
        ]
        zf.writestr("replay_tz_wlnbb_return_validation_examples.csv",
                    _to_csv_bytes(validation_examples, val_fields))

        date_audit_fields = [
            "ticker", "total_rows", "invalid_date_format", "out_of_order_count",
            "first_date", "last_date", "status",
        ]
        zf.writestr("replay_tz_wlnbb_date_order_audit.csv",
                    _to_csv_bytes(date_audit, date_audit_fields))

        zf.writestr("replay_tz_wlnbb_metadata.json",
                    json.dumps(meta, indent=2))

        zf.writestr("tz_wlnbb_config_snapshot.json",
                    json.dumps(config_snap, indent=2))

        if suspicious:
            zf.writestr("replay_tz_wlnbb_suspicious_return_patterns.csv",
                        _to_csv_bytes(suspicious, sq_fields))

    return output_path
