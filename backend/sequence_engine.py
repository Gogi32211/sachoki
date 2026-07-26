"""
sequence_engine.py — universe-wide N-bar T/Z sequence analyzer.

Reads the canonical TZ/WLNBB stock_stat CSV that
``backend/analyzers/tz_wlnbb/stock_stat.py`` already writes — does NOT
re-fetch OHLCV. Each row in that CSV contains:
  • ticker / date / bar_datetime
  • t_signal / z_signal      — current bar's T or Z label (may be empty)
  • ret_1d / ret_3d / ret_5d / ret_10d / mfe_* / mae_*  — pre-computed
                                                          forward returns

For every ticker we collect the rows where (t_signal or z_signal) belongs
to the standard pool (T7/T8/Z8 are excluded by spec), order them
chronologically, and slide an N-bar window. The forward-return measured
for the sequence is the ``ret_1d`` of the LAST bar in the window — i.e.
"if you saw this sequence and entered at the close of the last bar,
what was your 1-day return".

Ranking: ``score = win_rate × log1p(count)`` — same balance as the
existing ``robust_score`` in tz_wlnbb/replay.py.
"""
from __future__ import annotations

import csv
import logging
import math
import os
from collections import defaultdict
from typing import Callable, Iterable

log = logging.getLogger(__name__)

# Canonical CSV path candidates.
#   • TZ/WLNBB stock_stat   — has t_signal / z_signal / ret_1d already
#   • Bulk Stock Stat (Admin/api_stock_stat_trigger) — has compact T / Z
#     columns with full labels (e.g. T="T4", Z="Z3") and `close` for
#     forward-return derivation.
def _stat_path(universe: str, tf: str, nasdaq_batch: str = "") -> str:
    if nasdaq_batch and nasdaq_batch != "all":
        if universe == "nasdaq":
            return f"stock_stat_tz_wlnbb_nasdaq_{nasdaq_batch}_{tf}.csv"
        if universe == "nasdaq_gt5":
            return f"stock_stat_tz_wlnbb_nasdaq_gt5_{nasdaq_batch}_{tf}.csv"
    return f"stock_stat_tz_wlnbb_{universe}_{tf}.csv"


def _bulk_stat_path(universe: str, tf: str) -> str:
    """Bulk Stock Stat CSV path (Admin tab → /api/stock-stat/trigger)."""
    return os.path.join("stock_stat_output", f"stock_stat_{universe}_{tf}.csv")


def _candidate_paths(universe: str, tf: str, nasdaq_batch: str = "") -> list[str]:
    """Ordered list of CSV paths the engine may use. Preferred first.

    The engine will try each in order; if the first existing path yields
    zero ticker rows (stale / empty file from a prior session), it falls
    through to the next. This keeps things working even when a leftover
    empty TZ/WLNBB CSV would otherwise shadow the freshly-generated bulk
    Stock Stat CSV.
    """
    return [
        _stat_path(universe, tf, nasdaq_batch),
        f"stock_stat_tz_wlnbb_{universe}_{tf}.csv",
        f"stock_stat_tz_wlnbb_{tf}.csv",
        _bulk_stat_path(universe, tf),
    ]


def _resolve_stat_path(universe: str, tf: str, nasdaq_batch: str = "") -> str | None:
    """First existing candidate path, or None."""
    for p in _candidate_paths(universe, tf, nasdaq_batch):
        if os.path.exists(p):
            return p
    return None


def _read_rows_grouped(path: str) -> dict[str, list]:
    """Read a stock_stat CSV grouped by ticker. Returns {} on read errors
    or when the file has no usable rows."""
    grouped: dict[str, list] = defaultdict(list)
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                t = r.get("ticker", "")
                if t:
                    grouped[t].append(r)
    except (OSError, csv.Error) as exc:
        log.warning("sequence_engine: cannot read %s: %s", path, exc)
        return {}
    return dict(grouped)


def _read_rows_grouped_db(universe: str, tf: str) -> dict[str, list]:
    """Build the same {ticker: [row,...]} structure directly from the DuckDB
    `bars` table (no Stock Stat CSV needed) — routes to the tf DB (1d=analytics,
    else studio_<tf>.duckdb). 'all_us' = sp500+nasdaq+russell2k deduped to ONE
    canonical row per (ticker,date) by universe priority. Returns {} if the DB
    is missing/locked. The engine derives forward returns from `close`."""
    import os as _os
    from collections import defaultdict as _dd
    try:
        import duckdb as _duck
        from studio.paths import db_path as _dbp, ANALYTICS_DB as _ANA
    except Exception:
        return {}
    tf = (tf or "1d").lower()
    dbp = _ANA if tf == "1d" else _dbp(tf)
    if not _os.path.exists(dbp):
        return {}
    unis = (["sp500", "nasdaq", "russell2k"]
            if universe in ("all_us", "all", "") else [universe])
    ph = ",".join("?" * len(unis))
    try:
        con = _duck.connect(dbp, read_only=True)
    except Exception as exc:
        log.warning("sequence_engine DB read: cannot open %s: %s", dbp, exc)
        return {}
    try:
        df = con.execute(f"""
            WITH r AS (
              SELECT ticker, CAST(date AS VARCHAR) AS date, close, rsi_14,
                     coalesce(t_sig,'') AS t_signal, coalesce(z_sig,'') AS z_signal,
                     coalesce(l_sig,'') AS l_signal,
                     row_number() OVER (PARTITION BY ticker, date ORDER BY
                       CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2
                                     WHEN 'russell2k' THEN 3 ELSE 9 END) AS rn
              FROM bars WHERE universe IN ({ph}) AND close > 0)
            SELECT ticker, date, close, rsi_14, t_signal, z_signal, l_signal
            FROM r WHERE rn = 1 ORDER BY ticker, date
        """, unis).fetchdf()
    except Exception as exc:
        log.warning("sequence_engine DB read failed (%s): %s", dbp, exc)
        return {}
    finally:
        con.close()
    grouped: dict[str, list] = _dd(list)
    for rec in df.to_dict("records"):
        grouped[rec["ticker"]].append(rec)
    return dict(grouped)


# ─────────────────────────────────────────────────────────────────────────────
# Standard sequence pool — excludes T7, T8, Z8 per spec.
# ─────────────────────────────────────────────────────────────────────────────

BULL_SIGNALS = (
    "T1G", "T1", "T2G", "T2", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12",
)
BEAR_SIGNALS = (
    "Z1G", "Z1", "Z2G", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "Z9", "Z10", "Z11", "Z12",
)
_BULL_SET = frozenset(BULL_SIGNALS)
_BEAR_SET = frozenset(BEAR_SIGNALS)
ALLOWED_SIGNALS = _BULL_SET | _BEAR_SET   # T7, T8, Z8 excluded


def _classify(t_sig: str, z_sig: str) -> tuple[str, str] | None:
    """Return (type_letter, full_label) for the bar, or None if the bar
    has no allowed signal (or only T7/T8/Z8 — explicitly excluded)."""
    t = (t_sig or "").strip()
    z = (z_sig or "").strip()
    if t in _BULL_SET:
        return ("T", t)
    if z in _BEAR_SET:
        return ("Z", z)
    return None


def _safe_float(v) -> float | None:
    try:
        if v is None or v == "":
            return None
        f = float(v)
        return None if f != f else f  # NaN
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Row → (t_signal, z_signal) extraction — supports BOTH CSV layouts.
#
#   • TZ/WLNBB stock_stat: row['t_signal'] / row['z_signal'] (lowercase)
#   • Bulk Stock Stat:     row['T'] / row['Z'] (uppercase compact strings,
#                           one full label per cell or empty)
# Bulk Stock Stat may also store the label as the only token in the cell, or
# multiple space-separated tokens — we take the first allowed token.
# ─────────────────────────────────────────────────────────────────────────────

def _first_allowed(text: str, pool: frozenset) -> str:
    if not text:
        return ""
    for tok in str(text).replace(",", " ").split():
        u = tok.strip().upper()
        if u in pool:
            return u
    return ""


def _extract_tz_for_row(row: dict) -> tuple[str, str]:
    """Return (t_signal, z_signal) for a row from either CSV layout.

    Returns ("", "") if the bar has no allowed T/Z signal.
    """
    # TZ/WLNBB layout
    t = (row.get("t_signal") or "").strip()
    z = (row.get("z_signal") or "").strip()
    # Bulk Stock Stat layout (uppercase columns)
    if not t:
        t = _first_allowed(row.get("T", ""), _BULL_SET)
    if not z:
        z = _first_allowed(row.get("Z", ""), _BEAR_SET)
    return t, z


# ─────────────────────────────────────────────────────────────────────────────
# Main scan
# ─────────────────────────────────────────────────────────────────────────────

# Forward-return horizons aggregated per sequence. 1d remains the
# canonical measure used for the score; 3d/5d/9d are additive context.
_HORIZONS = (1, 3, 5, 9)


def run_sequence_scan(
    universe:    str = "sp500",
    tf:          str = "1d",
    seq_len:     int = 4,
    min_count:   int = 10,
    mode:        str = "type",        # "type"  → TZTZ  |  "full" → T4|Z2|T1G|Z3
    nasdaq_batch: str = "",
    progress_cb: Callable[[int, int], None] | None = None,
    years:       list | None = None,  # restrict entry bar (window's last bar) to these years
    months:      list | None = None,  # ... and/or these months (1-12)
    source:      str = "auto",        # "auto" = CSV then DB · "db" = force DuckDB
    rsi_min:     float | None = None, # entry-bar RSI-14 band
    rsi_max:     float | None = None,
    l_sigs:      list | None = None,  # entry-bar l_sig must be one of these (e.g. ['L3','L12'])
    price_min:   float | None = None, # entry-bar close-price band
    price_max:   float | None = None,
    robust:      bool = False,        # compute 2022-aware cross-period stability verdict
) -> dict:
    """Read the existing TZ/WLNBB stock_stat CSV, slide an N-bar window over
    each ticker's chronologically-ordered T/Z signal events, aggregate
    per-sequence stats across the whole universe, and return a sorted result
    list.

    Returns a dict so the caller can distinguish 'no CSV yet' (status='no_data')
    from 'CSV present but no sequences match min_count' (status='ok',
    results=[]).
    """
    if seq_len < 2 or seq_len > 6:
        return {"status": "error", "error": f"seq_len must be 2..6 (got {seq_len})"}
    if mode not in ("type", "full", "full_l"):
        return {"status": "error", "error": f"mode must be 'type'|'full'|'full_l' (got {mode!r})"}

    # Year/month filter sets (applied to the window's ENTRY bar = last bar).
    try:
        _yrs = {int(y) for y in (years or []) if str(y).strip()}
    except (TypeError, ValueError):
        _yrs = set()
    try:
        _mos = {int(m) for m in (months or []) if str(m).strip() and 1 <= int(m) <= 12}
    except (TypeError, ValueError):
        _mos = set()
    _lset = {str(x).strip().upper() for x in (l_sigs or []) if str(x).strip()}
    def _fnum(v):
        try:
            return float(v) if v is not None and str(v) != "" else None
        except (TypeError, ValueError):
            return None
    _rmin, _rmax = _fnum(rsi_min), _fnum(rsi_max)
    _pmin, _pmax = _fnum(price_min), _fnum(price_max)
    _entry_filter = bool(_yrs or _mos or _lset or _rmin is not None or _rmax is not None
                         or _pmin is not None or _pmax is not None)

    # Data source: DuckDB directly (any universe incl. all_us, any TF, exact
    # year/month filtering) OR the legacy Stock Stat CSV. 'db' forces DB; 'auto'
    # uses DB for all_us / when a year-month filter is set, else tries CSV first
    # and falls back to DB when no CSV exists (so all_us & fresh universes work).
    # DuckDB is the primary source (full history, all universes, all TFs, and the
    # only source that can serve RSI/L/price/year filters). The legacy Stock Stat
    # CSV is a fallback ONLY when the DB is unavailable AND no entry filter is set
    # (its recent-snapshot semantics can't answer a filtered query anyway). Using
    # DB always keeps counts consistent — adding a filter narrows, never jumps.
    rows_by_ticker: dict[str, list] = {}
    stat_path: str | None = None
    tried: list = []
    rows_by_ticker = _read_rows_grouped_db(universe, tf)
    if rows_by_ticker:
        stat_path = f"DB:{tf}:{universe}"
    elif not _entry_filter and source != "db":
        candidates = _candidate_paths(universe, tf, nasdaq_batch)
        for p in candidates:
            if not os.path.exists(p):
                continue
            tried.append(p)
            rows_by_ticker = _read_rows_grouped(p)
            if rows_by_ticker:
                stat_path = p
                break
            log.info("sequence_engine: %s exists but contains 0 ticker rows; "
                     "trying next candidate", p)
    if not stat_path:
        if tried:
            return {
                "status": "no_data",
                "error": (
                    "Stock Stat CSV(s) found but contain 0 ticker rows: "
                    + ", ".join(tried) + ". Re-run Stock Stat."
                ),
                "tried_paths": tried,
                "results": [],
            }
        return {
            "status": "no_data",
            "error": (
                f"No Stock Stat CSV for universe={universe} tf={tf}. "
                "Run Admin → Stock Stat or TZ/WLNBB → Generate Stock Stat first."
            ),
            "tried_paths": [],
            "results": [],
        }

    tickers = list(rows_by_ticker.keys())
    total = len(tickers)
    if progress_cb:
        progress_cb(0, total)

    seq_map: dict[str, dict] = defaultdict(
        lambda: {
            "wins": 0,
            "count": 0,
            # Per-horizon return lists. 1d is canonical; 3/5/9d are extras.
            "rets_by_h": {n: [] for n in _HORIZONS},
            "tickers": set(),
            # Robustness (only populated when robust=True): per-YEAR next-day
            # win counters. 1d win-rate is outlier- AND drift-immune (unlike the
            # 9d win rate, which just measures market up-drift), so it's the right
            # cross-period stability metric. O(1) memory (counters, ≤6 years).
            "yr_n":  defaultdict(int),
            "yr_w1": defaultdict(int),
        }
    )

    for idx, ticker in enumerate(tickers):
        rows = rows_by_ticker[ticker]
        rows.sort(key=lambda r: r.get("bar_datetime") or r.get("date", ""))

        # Pre-compute close-derived returns for every horizon a row lacks.
        # TZ/WLNBB CSV already populates ret_1d/ret_3d/ret_5d/ret_10d; we
        # still need to derive ret_9d for it and all four horizons for the
        # bulk Stock Stat CSV.
        for i, r in enumerate(rows):
            c0 = _safe_float(r.get("close"))
            if c0 is None or c0 <= 0:
                continue
            for n in _HORIZONS:
                key = f"ret_{n}d"
                if _safe_float(r.get(key)) is not None:
                    continue
                if i + n >= len(rows):
                    continue
                cn = _safe_float(rows[i + n].get("close"))
                if cn is None or cn <= 0:
                    continue
                r[key] = (cn / c0 - 1) * 100   # match TZ/WLNBB units (%)

        # Reduce to bars whose signal is in the allowed pool.
        events = []
        for r in rows:
            t_sig, z_sig = _extract_tz_for_row(r)
            cls = _classify(t_sig, z_sig)
            if cls is None:
                continue
            ret1 = _safe_float(r.get("ret_1d"))
            if ret1 is None:
                continue        # last few bars with no forward bar — skip
            # Other horizons may legitimately be None for events near the
            # end of the dataset (e.g. ret_9d on the last 9 bars).
            rets_by_h = {n: _safe_float(r.get(f"ret_{n}d")) for n in _HORIZONS}
            events.append((cls[0], cls[1], rets_by_h, r.get("date", ""),
                           _safe_float(r.get("rsi_14")),
                           (r.get("l_signal") or r.get("l_sig") or "").strip().upper(),
                           _safe_float(r.get("close"))))

        # Slide window. Forward returns are taken from the LAST bar of the
        # window (entry-at-close, exit-N-bars-later).
        for i in range(len(events) - seq_len + 1):
            window = events[i : i + seq_len]
            # entry-bar filters (window's last bar): year/month/RSI/L-signal/price.
            # Keeps sequences that CROSS a boundary but END inside the selection.
            if _entry_filter:
                ent = window[-1]
                _ed = str(ent[3])                 # entry date 'YYYY-MM-DD...'
                if _yrs and (not _ed[:4].isdigit() or int(_ed[:4]) not in _yrs):
                    continue
                if _mos and (len(_ed) < 7 or not _ed[5:7].isdigit() or int(_ed[5:7]) not in _mos):
                    continue
                _ersi = ent[4] if len(ent) > 4 else None
                if _rmin is not None and (_ersi is None or _ersi < _rmin):
                    continue
                if _rmax is not None and (_ersi is None or _ersi > _rmax):
                    continue
                if _lset and (len(ent) <= 5 or ent[5] not in _lset):
                    continue
                _ecl = ent[6] if len(ent) > 6 else None
                if _pmin is not None and (_ecl is None or _ecl < _pmin):
                    continue
                if _pmax is not None and (_ecl is None or _ecl > _pmax):
                    continue
            last_rets = window[-1][2]
            if mode == "type":
                key = "".join(w[0] for w in window)
            elif mode == "full_l":
                # full label WITH each bar's L signal appended, e.g. T1GL3|Z2GL12
                key = "|".join((w[1] + (w[5] if len(w) > 5 and w[5] else "")) for w in window)
            else:
                key = "|".join(w[1] for w in window)
            entry = seq_map[key]
            entry["count"] += 1
            for n in _HORIZONS:
                v = last_rets.get(n)
                if v is None:
                    continue
                entry["rets_by_h"][n].append(v)
            entry["tickers"].add(ticker)
            # 1d-derived counters retained for backward compat with the
            # canonical 'wins' column and ranking.
            r1 = last_rets.get(1)
            if r1 is not None and r1 > 0:
                entry["wins"] += 1
            # per-year 1d win counters for the robustness verdict
            if robust and r1 is not None:
                _yr = str(window[-1][3])[:4]
                if _yr.isdigit():
                    entry["yr_n"][_yr] += 1
                    if r1 > 0:
                        entry["yr_w1"][_yr] += 1

        if progress_cb:
            progress_cb(idx + 1, total)

    def _stats(xs: list) -> tuple[float | None, float | None, float | None, float | None]:
        """Return (avg, median, std, win_rate) for a list of returns; (None,…)
        if empty."""
        n = len(xs)
        if n == 0:
            return (None, None, None, None)
        avg = sum(xs) / n
        srt = sorted(xs)
        med = (srt[n // 2] if n % 2 else (srt[n // 2 - 1] + srt[n // 2]) / 2)
        if n > 1:
            std = math.sqrt(sum((x - avg) ** 2 for x in xs) / (n - 1))
        else:
            std = 0.0
        wins = sum(1 for x in xs if x > 0)
        return (avg, med, std, wins / n)

    # Build result list.
    results = []
    for key, d in seq_map.items():
        if d["count"] < min_count:
            continue

        # 1d remains the canonical horizon for the headline win_rate / wins
        # / score (so existing sort_by=score stays comparable across runs).
        rets1 = d["rets_by_h"][1]
        avg1, med1, std1, wr1 = _stats(rets1)
        wr1 = wr1 if wr1 is not None else (d["wins"] / d["count"])

        if mode in ("full", "full_l"):
            type_seq = "".join("T" if p[:1] == "T" else "Z" for p in key.split("|"))
        else:
            type_seq = key
        score = round(wr1 * math.log1p(d["count"]), 4)

        out = {
            "sequence":     key,
            "type_seq":     type_seq,
            "count":        d["count"],
            "wins":         d["wins"],
            "win_rate":     round(wr1, 4),
            "avg_ret_1d":   round(avg1, 6) if avg1 is not None else None,
            "med_ret_1d":   round(med1, 6) if med1 is not None else None,
            "std_ret":      round(std1, 6) if std1 is not None else None,
            "ticker_count": len(d["tickers"]),
            "score":        score,
        }
        # 3d / 5d / 9d additive context. None when no events had that
        # horizon populated (typical near the end of the dataset).
        for n in (3, 5, 9):
            xs = d["rets_by_h"][n]
            avg, med, _std, wr = _stats(xs)
            out[f"win_rate_{n}d"] = round(wr,  4) if wr  is not None else None
            out[f"avg_ret_{n}d"]  = round(avg, 6) if avg is not None else None
            out[f"med_ret_{n}d"]  = round(med, 6) if med is not None else None
            out[f"count_{n}d"]    = len(xs)

        # ── Robustness verdict (2022-aware cross-period stability) ──────────────
        # 2022 is a bear year that drags nearly every long setup negative, so we
        # DON'T require it — stability is judged on the non-2022 years, and 2022
        # is a separate "bear stress" flag. Metric = per-year 1d WIN-RATE (>52% =
        # a real next-day directional edge, immune to the drift/outlier illusion
        # that makes 9d win-rates look great on random entries).
        if robust:
            _MIN_YR_N = 5
            yr_win = {y: d["yr_w1"][y] / d["yr_n"][y]
                      for y in d["yr_n"] if d["yr_n"][y] >= _MIN_YR_N}
            non22 = {y: w for y, w in yr_win.items() if y != "2022"}
            good  = sum(1 for w in non22.values() if w >= 0.52)   # beats coin-flip
            total = len(non22)
            bear_ok = ("2022" in yr_win) and (yr_win["2022"] >= 0.50)
            out["pos_years"]  = good
            out["tot_years"]  = total
            out["bear_ok"]    = bool(bear_ok)
            out["yr_win"]     = {y: round(w, 3) for y, w in sorted(yr_win.items())}
            out["is_robust"]  = bool(total >= 3 and good / max(total, 1) >= 0.6)
            # robust_score ranks the stable ones: fraction of good years × log(n),
            # + a bear-survivor bonus. Non-robust rows keep their normal score.
            out["robust_score"] = round((good / max(total, 1)) * math.log1p(d["count"])
                                        + (0.5 if bear_ok else 0), 4)
        results.append(out)

    results.sort(key=lambda x: (-x["score"], -x["count"]))
    return {
        "status":       "ok",
        "stat_path":    stat_path,
        "universe":     universe,
        "tf":           tf,
        "seq_len":      seq_len,
        "mode":         mode,
        "min_count":    min_count,
        "tickers_seen": total,
        "results":      results,
    }
