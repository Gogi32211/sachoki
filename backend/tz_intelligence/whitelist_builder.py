"""Build composite, seq4, and composite+seq4 whitelist/blacklist CSVs from stock_stat.

Run via:  python -m backend.tz_intelligence.whitelist_builder --stat-path stock_stat_tz_wlnbb_sp500_1d.csv

Output files (written to current directory):
  composite_whitelist.csv
  composite_blacklist.csv
  seq4_whitelist.csv
  seq4_blacklist.csv
  composite_seq4_whitelist.csv
  composite_seq4_blacklist.csv
  aio_suffix_performance.csv
"""
from __future__ import annotations
import argparse
import csv
import os
import statistics
from collections import defaultdict
from typing import Optional

from .stat_engine import compute_stat_status, compute_sample_confidence

_SIGNAL_COL = ("t_signal", "z_signal", "l_signal")


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> Optional[int]:
    try:
        return int(float(v)) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _bar_sig(row: dict) -> str:
    return (row.get("t_signal") or row.get("z_signal") or
            row.get("l_signal") or "—")


def _summarise(returns: list[float]) -> dict:
    n = len(returns)
    if not n:
        return {"count": 0, "avg_10d": None, "median_10d": None,
                "win_rate": None, "fail_rate": None}
    wins  = sum(1 for r in returns if r > 0)
    fails = sum(1 for r in returns if r <= -3.0)  # fail = -3% or worse
    s = sorted(returns)
    return {
        "count":     n,
        "avg_10d":   round(sum(returns) / n, 3),
        "median_10d": round(statistics.median(returns), 3),
        "win_rate":  round(wins  / n * 100, 2),
        "fail_rate": round(fails / n * 100, 2),
    }


def _build_stat_row(key: dict, stats: dict) -> dict:
    s = _summarise(stats)
    n   = s["count"]
    med = s["median_10d"]
    fail = s["fail_rate"]
    status     = compute_stat_status(n, med, fail)
    confidence = compute_sample_confidence(n)
    return {**key, **s, "status": status, "confidence": confidence}


def build_whitelists(stat_path: str, output_dir: str = ".") -> dict:
    """Read stock_stat CSV and write whitelist/blacklist files.

    Returns dict with counts per file.
    """
    if not os.path.exists(stat_path):
        return {"error": f"File not found: {stat_path}"}

    # Buckets: composite → [ret_10d], seq4 → [ret_10d], (comp,seq4) → [ret_10d]
    comp_rets: dict[str, list[float]] = defaultdict(list)
    seq4_rets: dict[str, list[float]] = defaultdict(list)
    cs_rets:   dict[tuple, list[float]] = defaultdict(list)

    # For AIO suffix performance
    aio_rets: dict[tuple, list[float]] = defaultdict(list)  # (base_comp, aio) → [ret]

    # Build seq4 on the fly: need sorted rows per ticker
    ticker_rows: dict[str, list] = defaultdict(list)

    with open(stat_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ticker = row.get("ticker", "")
            if ticker:
                ticker_rows[ticker].append(row)

    for ticker, rows in ticker_rows.items():
        # Sort by bar_datetime or date
        rows.sort(key=lambda r: r.get("bar_datetime") or r.get("date", ""))

        for i, row in enumerate(rows):
            ret10 = _safe_float(row.get("ret_10d"))
            if ret10 is None:
                continue

            composite = (row.get("composite_full_label") or "").strip()
            fail10    = _safe_float(row.get("fail_10d"))  # binary 0/1

            if composite:
                comp_rets[composite].append(ret10)

            # seq4: use 3 prior bars + current
            history = rows[max(0, i - 3):i]
            seq4_parts = [_bar_sig(b) for b in history] + [_bar_sig(row)]
            if len(seq4_parts) == 4:
                seq4 = "|".join(seq4_parts)
                seq4_rets[seq4].append(ret10)
                if composite:
                    cs_rets[(composite, seq4)].append(ret10)

            # AIO suffix performance
            close_sfx   = (row.get("close_suffix")   or "").strip()
            close_appended = str(row.get("close_appended") or "0").strip()
            if composite and close_sfx and close_appended in ("1", "True", "true"):
                # base_comp = composite without the final A/I/O character
                if composite.endswith(close_sfx):
                    base_comp = composite[:-len(close_sfx)]
                else:
                    base_comp = composite
                aio_rets[(base_comp, close_sfx)].append(ret10)

    try:
        from ..analyzers.tz_wlnbb.replay import parse_composite_label
    except ImportError:
        try:
            from analyzers.tz_wlnbb.replay import parse_composite_label
        except ImportError:
            def parse_composite_label(label):
                return {"t_signal": "", "z_signal": "", "l_signal": "", "composite_core": label, "full_suffix": ""}

    # ── Write composite whitelists ─────────────────────────────────────────────
    comp_rows = []
    for comp, rets in comp_rets.items():
        try:
            parsed = parse_composite_label(comp)
        except Exception:
            parsed = {"t_signal": "", "z_signal": "", "l_signal": "", "full_suffix": ""}
        signal = parsed.get("t_signal") or parsed.get("z_signal") or ""
        row_out = _build_stat_row({
            "composite": comp,
            "signal": signal,
            "l_pattern": parsed.get("l_signal", ""),
            "suffix": parsed.get("full_suffix", ""),
        }, rets)
        comp_rows.append(row_out)

    _write_csv(
        os.path.join(output_dir, "composite_whitelist.csv"),
        [r for r in comp_rows if r["status"] in ("STRONG", "GOOD")],
        ["composite","signal","l_pattern","suffix","count","median_10d","avg_10d",
         "win_rate","fail_rate","status","confidence"],
    )
    _write_csv(
        os.path.join(output_dir, "composite_blacklist.csv"),
        [r for r in comp_rows if r["status"] in ("WEAK", "REJECT")],
        ["composite","signal","l_pattern","suffix","count","median_10d","avg_10d",
         "win_rate","fail_rate","status","confidence"],
    )

    # ── Write seq4 whitelists ──────────────────────────────────────────────────
    seq4_rows = [
        _build_stat_row({"seq4": seq4}, rets)
        for seq4, rets in seq4_rets.items()
    ]
    _write_csv(
        os.path.join(output_dir, "seq4_whitelist.csv"),
        [r for r in seq4_rows if r["status"] in ("STRONG", "GOOD")],
        ["seq4","count","median_10d","avg_10d","win_rate","fail_rate","status","confidence"],
    )
    _write_csv(
        os.path.join(output_dir, "seq4_blacklist.csv"),
        [r for r in seq4_rows if r["status"] in ("WEAK", "REJECT")],
        ["seq4","count","median_10d","avg_10d","win_rate","fail_rate","status","confidence"],
    )

    # ── Write composite+seq4 whitelists ────────────────────────────────────────
    cs_rows = [
        _build_stat_row({"composite": cs[0], "seq4": cs[1]}, rets)
        for cs, rets in cs_rets.items()
    ]
    _write_csv(
        os.path.join(output_dir, "composite_seq4_whitelist.csv"),
        [r for r in cs_rows if r["status"] in ("STRONG", "GOOD")],
        ["composite","seq4","count","median_10d","avg_10d","win_rate","fail_rate","status","confidence"],
    )
    _write_csv(
        os.path.join(output_dir, "composite_seq4_blacklist.csv"),
        [r for r in cs_rows if r["status"] in ("WEAK", "REJECT")],
        ["composite","seq4","count","median_10d","avg_10d","win_rate","fail_rate","status","confidence"],
    )

    # ── Comprehensive (composite, seq4) stats: every observed pair ─────────────
    # Lookup the normalizer reads to populate statistical_status_composite_seq4
    # for arbitrary rows. Includes LOW_SAMPLE rows (count < 20) so the
    # downstream normalizer can distinguish "small sample" from "never seen".
    cs_all = [r for r in cs_rows if r["count"] >= 1]
    _write_csv(
        os.path.join(output_dir, "composite_seq4_stats.csv"),
        cs_all,
        ["composite","seq4","count","median_10d","avg_10d","win_rate","fail_rate","status","confidence"],
    )

    # ── Write AIO suffix performance ───────────────────────────────────────────
    base_comps: set[str] = {k[0] for k in aio_rets}
    aio_out = []
    for base in sorted(base_comps):
        a_rets = aio_rets.get((base, "A"), [])
        i_rets = aio_rets.get((base, "I"), [])
        o_rets = aio_rets.get((base, "O"), [])

        def _med(r): return round(statistics.median(r), 3) if r else None
        def _fail(r): return round(sum(1 for x in r if x <= -3.0) / len(r) * 100, 2) if r else None

        variants = {
            "A": (_med(a_rets), _fail(a_rets), len(a_rets)),
            "I": (_med(i_rets), _fail(i_rets), len(i_rets)),
            "O": (_med(o_rets), _fail(o_rets), len(o_rets)),
        }
        best = max(variants, key=lambda v: (variants[v][0] or -99, -(variants[v][1] or 99)))
        worst = min(variants, key=lambda v: (variants[v][0] or -99, -(variants[v][1] or 99)))

        aio_out.append({
            "base_composite": base,
            "A_variant":      base + "A", "A_count": variants["A"][2],
            "A_median_10d":   variants["A"][0], "A_fail_rate": variants["A"][1],
            "I_variant":      base + "I", "I_count": variants["I"][2],
            "I_median_10d":   variants["I"][0], "I_fail_rate": variants["I"][1],
            "O_variant":      base + "O", "O_count": variants["O"][2],
            "O_median_10d":   variants["O"][0], "O_fail_rate": variants["O"][1],
            "best_variant":   best + " (" + (str(variants[best][0]) or "?") + ")",
            "worst_variant":  worst + " (" + (str(variants[worst][0]) or "?") + ")",
            "recommendation": (
                f"Use {best} suffix for {base}: highest median return among A/I/O variants"
                if variants[best][2] >= 20
                else f"LOW_SAMPLE: insufficient data for {base} A/I/O recommendation"
            ),
        })
    _write_csv(
        os.path.join(output_dir, "aio_suffix_performance.csv"),
        aio_out,
        ["base_composite","A_variant","A_count","A_median_10d","A_fail_rate",
         "I_variant","I_count","I_median_10d","I_fail_rate",
         "O_variant","O_count","O_median_10d","O_fail_rate",
         "best_variant","worst_variant","recommendation"],
    )

    return {
        "composite_whitelist": len([r for r in comp_rows if r["status"] in ("STRONG", "GOOD")]),
        "composite_blacklist": len([r for r in comp_rows if r["status"] in ("WEAK", "REJECT")]),
        "seq4_whitelist":       len([r for r in seq4_rows if r["status"] in ("STRONG", "GOOD")]),
        "seq4_blacklist":       len([r for r in seq4_rows if r["status"] in ("WEAK", "REJECT")]),
        "composite_seq4_whitelist": len([r for r in cs_rows if r["status"] in ("STRONG", "GOOD")]),
        "composite_seq4_blacklist": len([r for r in cs_rows if r["status"] in ("WEAK", "REJECT")]),
        "composite_seq4_stats":     len(cs_all),
        "aio_suffix_performance":   len(aio_out),
        "total_composites": len(comp_rows),
        "total_seq4": len(seq4_rows),
        "total_composite_seq4": len(cs_rows),
    }


def _write_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    rows_sorted = sorted(rows, key=lambda r: (-(r.get("count") or 0)))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_sorted)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build whitelist/blacklist CSVs from stock_stat")
    parser.add_argument("--stat-path", required=True, help="Path to stock_stat_tz_wlnbb_*.csv")
    parser.add_argument("--output-dir", default=".", help="Directory for output CSVs")
    args = parser.parse_args()
    result = build_whitelists(args.stat_path, args.output_dir)
    for k, v in result.items():
        print(f"  {k}: {v}")
