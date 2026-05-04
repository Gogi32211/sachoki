"""Replay analytics for TZ/WLNBB signals."""
import csv
import io
import json
import zipfile
import logging
from datetime import datetime
from typing import List, Dict, Optional

from .config import TZ_WLNBB_VERSION

log = logging.getLogger(__name__)


def _add_forward_returns(rows: List[dict]) -> List[dict]:
    """Add ret_1d, ret_3d, ret_5d, ret_10d and outcome fields to rows sorted by date."""
    by_ticker: Dict[str, List] = {}
    for r in rows:
        t = r.get("ticker", "")
        by_ticker.setdefault(t, []).append(r)

    result = []
    for ticker, t_rows in by_ticker.items():
        t_rows.sort(key=lambda x: x.get("date", ""))
        closes = [float(r.get("close", 0) or 0) for r in t_rows]
        highs  = [float(r.get("high", 0)  or 0) for r in t_rows]
        lows   = [float(r.get("low", 0)   or 0) for r in t_rows]
        n = len(closes)
        for i, r in enumerate(t_rows):
            c0 = closes[i]
            r2 = dict(r)
            if c0 > 0:
                for w, key in [(1, "ret_1d"), (3, "ret_3d"), (5, "ret_5d"), (10, "ret_10d")]:
                    if i + w < n:
                        r2[key] = round((closes[i + w] - c0) / c0 * 100, 4)
                    else:
                        r2[key] = None
                for w, wk in [(5, "5d"), (10, "10d")]:
                    fut_h = highs[i + 1:i + w + 1] if i + 1 < n else []
                    fut_l = lows[i + 1:i + w + 1]  if i + 1 < n else []
                    r2[f"max_high_{wk}"]     = round((max(fut_h) - c0) / c0 * 100, 4) if fut_h else None
                    r2[f"max_drawdown_{wk}"] = round((min(fut_l) - c0) / c0 * 100, 4) if fut_l else None
                    r2[f"mfe_{wk}"]          = r2[f"max_high_{wk}"]
                    r2[f"mae_{wk}"]          = r2[f"max_drawdown_{wk}"]
                ret5  = r2.get("ret_5d")
                ret10 = r2.get("ret_10d")
                r2["clean_win_5d"] = int(ret5  is not None and ret5  >= 3.0)
                r2["big_win_10d"]  = int(ret10 is not None and ret10 >= 5.0)
                r2["fail_5d"]      = int(ret5  is not None and ret5  <= -3.0)
                r2["fail_10d"]     = int(ret10 is not None and ret10 <= -5.0)
            else:
                for k in ["ret_1d", "ret_3d", "ret_5d", "ret_10d",
                          "max_high_5d", "max_drawdown_5d", "mfe_5d", "mae_5d",
                          "max_high_10d", "max_drawdown_10d", "mfe_10d", "mae_10d",
                          "clean_win_5d", "big_win_10d", "fail_5d", "fail_10d"]:
                    r2[k] = None
            result.append(r2)
    return result


def _signal_perf(rows: List[dict]) -> List[dict]:
    """Aggregate signal performance by signal_type + signal_name."""
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
            vals = [r[k] for r in g if r.get(k) is not None]
            return round(sum(vals) / len(vals), 4) if vals else None

        def _med(k, g=grp):
            vals = sorted(r[k] for r in g if r.get(k) is not None)
            if not vals:
                return None
            m = len(vals) // 2
            return round((vals[m - 1] + vals[m]) / 2 if len(vals) % 2 == 0 else vals[m], 4)

        def _rate(k, g=grp):
            vals = [r[k] for r in g if r.get(k) is not None]
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
    """Aggregate by combination of t/z/l/preup/predn/ne/wick."""
    groups: Dict[tuple, list] = {}
    for r in rows:
        key = (
            r.get("t_signal", ""), r.get("z_signal", ""), r.get("l_signal", ""),
            r.get("preup_signal", ""), r.get("predn_signal", ""),
            r.get("ne_suffix", ""), r.get("wick_suffix", ""),
        )
        groups.setdefault(key, []).append(r)

    result = []
    for (ts, zs, ls, ps, ds, ne, wk), grp in groups.items():
        if not any([ts, zs, ls, ps, ds]):
            continue

        def _avg(k, g=grp):
            vals = [r[k] for r in g if r.get(k) is not None]
            return round(sum(vals) / len(vals), 4) if vals else None

        def _rate(k, g=grp):
            vals = [r[k] for r in g if r.get(k) is not None]
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        result.append({
            "t_signal": ts, "z_signal": zs, "l_signal": ls,
            "preup_signal": ps, "predn_signal": ds, "ne_suffix": ne, "wick_suffix": wk,
            "count": len(grp),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"), "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "tz_wlnbb_version": TZ_WLNBB_VERSION,
        })
    return sorted(result, key=lambda x: -(x["count"] or 0))


def _sequence_perf(rows: List[dict]) -> List[dict]:
    """Analyze 2-bar and 3-bar signal sequences."""
    by_ticker: Dict[str, list] = {}
    for r in rows:
        t = r.get("ticker", "")
        by_ticker.setdefault(t, []).append(r)

    seq_groups: Dict[tuple, list] = {}

    for ticker, t_rows in by_ticker.items():
        t_rows.sort(key=lambda x: x.get("date", ""))
        for i in range(len(t_rows)):
            curr = t_rows[i]
            curr_sig = _primary_signal(curr)
            if not curr_sig:
                continue
            for lag in range(1, 6):
                if i - lag < 0:
                    break
                prev = t_rows[i - lag]
                prev_sig = _primary_signal(prev)
                if not prev_sig:
                    continue
                pattern = f"{prev_sig}->{curr_sig}@{lag}"
                key = (pattern, lag, curr.get("universe", ""), curr.get("timeframe", ""))
                seq_groups.setdefault(key, []).append(curr)
                break

    result = []
    for (pattern, lag, uni, tf), grp in seq_groups.items():
        def _avg(k, g=grp):
            vals = [r[k] for r in g if r.get(k) is not None]
            return round(sum(vals) / len(vals), 4) if vals else None

        def _rate(k, g=grp):
            vals = [r[k] for r in g if r.get(k) is not None]
            return round(sum(vals) / len(vals) * 100, 2) if vals else None

        result.append({
            "sequence_pattern": pattern, "bars_between": lag,
            "universe": uni, "timeframe": tf, "count": len(grp),
            "avg_ret_1d": _avg("ret_1d"), "avg_ret_3d": _avg("ret_3d"),
            "avg_ret_5d": _avg("ret_5d"), "avg_ret_10d": _avg("ret_10d"),
            "big_win_10d_rate": _rate("big_win_10d"), "fail_10d_rate": _rate("fail_10d"),
            "avg_mfe_10d": _avg("mfe_10d"), "avg_mae_10d": _avg("mae_10d"),
            "tz_wlnbb_version": TZ_WLNBB_VERSION,
        })
    return sorted(result, key=lambda x: -(x["count"] or 0))


def _primary_signal(r: dict) -> str:
    """Get the primary signal name for a row (T > Z > L > PREUP > PREDN)."""
    return (
        r.get("t_signal") or r.get("z_signal") or
        r.get("l_signal") or r.get("preup_signal") or r.get("predn_signal") or ""
    )


def generate_replay_zip(rows: List[dict], output_path: str = "replay_tz_wlnbb_analytics.zip") -> str:
    """Generate replay ZIP with all analytics CSVs."""
    rows = _add_forward_returns(rows)

    sp = _signal_perf(rows)
    cp = _combo_perf(rows)
    sq = _sequence_perf(rows)

    top = [r for r in sp if (r.get("count") or 0) >= 30 and (r.get("avg_ret_10d") or 0) > 0]
    top.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))

    bad = [r for r in sp if (r.get("avg_ret_10d") or 0) < 0 or (r.get("fail_10d_rate") or 0) > 20]
    bad.sort(key=lambda x: (x.get("avg_ret_10d") or 0))

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
            "ne_suffix", "wick_suffix", "count", "avg_ret_5d", "avg_ret_10d",
            "big_win_10d_rate", "fail_10d_rate", "avg_mfe_10d", "avg_mae_10d", "tz_wlnbb_version",
        ]
        zf.writestr("replay_tz_wlnbb_combo_perf.csv", _to_csv_bytes(cp, cp_fields))

        sq_fields = [
            "sequence_pattern", "bars_between", "universe", "timeframe", "count",
            "avg_ret_1d", "avg_ret_3d", "avg_ret_5d", "avg_ret_10d",
            "big_win_10d_rate", "fail_10d_rate", "avg_mfe_10d", "avg_mae_10d", "tz_wlnbb_version",
        ]
        zf.writestr("replay_tz_wlnbb_sequence_perf.csv", _to_csv_bytes(sq, sq_fields))
        zf.writestr("replay_tz_wlnbb_top_patterns.csv",  _to_csv_bytes(top, sp_fields))
        zf.writestr("replay_tz_wlnbb_bad_patterns.csv",  _to_csv_bytes(bad, sp_fields))

        meta = {
            "tz_wlnbb_version": TZ_WLNBB_VERSION,
            "generated_at": datetime.utcnow().isoformat(),
            "rows_total": len(rows),
        }
        zf.writestr("replay_tz_wlnbb_metadata.json", json.dumps(meta, indent=2))

    return output_path
