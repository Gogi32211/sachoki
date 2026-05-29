"""
studio/seq_lab.py — TZ Sequence Lab.

Exploratory ranking of N-bar T/Z sequences by their forward outcome, straight
from the enriched `bars` table. Powers the Studio "Seq Lab" tab.

Two sequence modes:
  - color : each bar is T (close>open) / Z (close<open) — e.g. "ZZZT"
  - signal: each bar is its t_sig/z_sig label (sparse) — e.g. "T9|Z3|·|·|T2G"

Metrics per sequence: n, win% (forward return > 0), avg forward return, avg mfe_20d.
A baseline row (same filters, no sequence condition) is always returned so you can
judge real edge vs the unconditional rate.

IMPORTANT (lesson baked in): for pivot/reversal work prefer horizon
'fwd_swing_ret_3' (move to the next swing pivot) over fixed-day 'fwd_1d' — swings
are not day-bounded. Also watch avg_ret + mfe_20d together: a high win% with a
tiny avg_ret/mfe is a weak edge, not a signal.

All user inputs are whitelisted/escaped (no SQL injection).
"""
from __future__ import annotations

import math
import re
from typing import Optional

from studio.db import get_conn
from studio.signal_stats import _safe_universe, _q


def _f(v):
    """Float for JSON, mapping NaN/Inf → None (DuckDB AVG over illiquid names can
    yield NaN/Inf, which FastAPI's JSON encoder rejects → 500)."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None

_HORIZONS = {
    "fwd_1d": "fwd_1d", "fwd_3d": "fwd_3d", "fwd_5d": "fwd_5d",
    "fwd_10d": "fwd_10d", "fwd_20d": "fwd_20d",
    "fwd_swing_ret_3": "fwd_swing_ret_3", "fwd_swing_ret_5": "fwd_swing_ret_5",
}
_PHASES = {"MARKUP", "MKDN", "ACC_TR", "DIST_TR", "SPRING", "UTAD", "SOS", "SOW", "NEUTRAL"}
_SORTS = {"win": "win DESC", "avg": "avg_ret DESC", "mfe": "mfe20 DESC",
          "n": "n DESC", "win_lo": "win ASC", "avg_lo": "avg_ret ASC"}
_PREFIX_RE = re.compile(r"^[TZ\-|A-Z0-9·]{0,40}$")


def _token_expr(mode: str) -> str:
    if mode == "signal":
        return "COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '.')"
    # color (default)
    return "CASE WHEN close > open THEN 'T' WHEN close < open THEN 'Z' ELSE '-' END"


def seq_lab(
    universe:  Optional[str] = None,
    n_bars:    int = 4,
    mode:      str = "color",
    horizon:   str = "fwd_1d",
    min_occ:   int = 500,
    wyc_phase: Optional[str] = None,
    prefix:    Optional[str] = None,
    sort:      str = "win",
    limit:     int = 25,
    by_phase:  bool = False,
) -> dict:
    """Rank N-bar T/Z sequences by forward outcome. Returns {baseline, rows, params}."""
    # ── validate / clamp ──────────────────────────────────────────────────────
    n_bars  = max(2, min(6, int(n_bars)))
    mode    = "signal" if mode == "signal" else "color"
    hcol    = _HORIZONS.get(horizon, "fwd_1d")
    min_occ = max(20, min(200_000, int(min_occ)))
    limit   = max(1, min(100, int(limit)))
    order   = _SORTS.get(sort, "win DESC")
    uni     = _safe_universe(universe)
    phase   = wyc_phase if wyc_phase in _PHASES else None
    pref    = prefix.strip().upper() if prefix and _PREFIX_RE.match(prefix.strip().upper()) else None

    sep = "|" if mode == "signal" else ""
    tok = _token_expr(mode)

    base_clauses = [f"{hcol} IS NOT NULL"]
    if uni:
        base_clauses.append(f"universe = '{uni}'")
    if phase:
        base_clauses.append(f"wyc_phase = '{phase}'")
    base_where = " AND ".join(base_clauses)

    conn = get_conn(read_only=True)
    try:
        # ── baseline (same filters, no sequence) ──────────────────────────────
        b = conn.execute(f"""
            SELECT COUNT(*) n,
                   ROUND(AVG(CASE WHEN {hcol} > 0 THEN 1.0 ELSE 0 END)*100, 1) win,
                   ROUND(AVG({hcol}), 3) avg_ret,
                   ROUND(AVG(mfe_20d), 2) mfe20
            FROM bars WHERE {base_where}
        """).fetchone()
        baseline = {"n": int(b[0] or 0), "win": _f(b[1]), "avg_ret": _f(b[2]), "mfe20": _f(b[3])}

        # ── windowed sequence build ───────────────────────────────────────────
        lag_parts = [f"LAG(tk, {k}) OVER w" for k in range(n_bars - 1, 0, -1)] + ["tk"]
        seq_concat = f" || '{sep}' || ".join(lag_parts) if sep else " || ".join(lag_parts)

        outer = ["seq IS NOT NULL"]
        if mode == "color":
            outer.append("seq NOT LIKE '%-%'")          # drop doji bars
        else:
            outer.append("seq NOT LIKE '%.%'")          # require a signal on every bar
        if pref:
            outer.append(f"seq LIKE '{_q(pref)}%'")
        outer_where = " AND ".join(outer)

        grp = "seq, wyc_phase" if by_phase else "seq"
        sel_phase = ", wyc_phase" if by_phase else ""

        rows = conn.execute(f"""
            WITH s AS (
                SELECT ticker, date, wyc_phase,
                       {tok} AS tk, {hcol} AS ret, mfe_20d
                FROM bars WHERE {base_where}
            ),
            seqd AS (
                SELECT *, {seq_concat} AS seq
                FROM s WINDOW w AS (PARTITION BY ticker ORDER BY date)
            )
            SELECT seq{sel_phase}, COUNT(*) n,
                   ROUND(AVG(CASE WHEN ret > 0 THEN 1.0 ELSE 0 END)*100, 1) win,
                   ROUND(AVG(ret), 3) avg_ret,
                   ROUND(AVG(mfe_20d), 2) mfe20
            FROM seqd
            WHERE {outer_where}
            GROUP BY {grp}
            HAVING COUNT(*) >= {min_occ}
            ORDER BY {order}
            LIMIT {limit}
        """).fetchdf()

        recs = []
        for _, r in rows.iterrows():
            rec = {
                "seq":     str(r["seq"]),
                "n":       int(r["n"]),
                "win":     _f(r["win"]),
                "avg_ret": _f(r["avg_ret"]),
                "mfe20":   _f(r["mfe20"]),
            }
            if by_phase:
                rec["wyc_phase"] = str(r["wyc_phase"])
            recs.append(rec)

        return {
            "baseline": baseline,
            "rows": recs,
            "params": {
                "universe": uni or "all", "n_bars": n_bars, "mode": mode,
                "horizon": hcol, "min_occ": min_occ, "wyc_phase": phase or "all",
                "prefix": pref or "", "sort": sort, "by_phase": by_phase,
            },
        }
    finally:
        conn.close()
