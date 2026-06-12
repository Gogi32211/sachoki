"""
studio/signal_stats.py — Signal forward-return statistics engine.

Core idea: given a set of signal conditions, compute historical outcome
statistics directly from the bars table (forward returns already stored).

Two main functions:
  query_combo(signals, filters)  → outcome stats for a specific signal combo
  rank_signals(filters)          → rank ALL single signals by a chosen metric
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import pandas as pd

from studio.db import get_conn, UNIVERSE_PRIORITY_SQL

log = logging.getLogger(__name__)

# ── SQL-safety helpers ────────────────────────────────────────────────────────
# Several query paths build window-function SQL that can't easily use bound
# params, so user-supplied values are interpolated into string literals. These
# helpers make that safe WITHOUT changing behaviour for legitimate inputs:
#   - universe is a known enum → allowlist (invalid value drops the filter)
#   - all other free-text values → escape embedded single quotes (legit signal /
#     suffix / line5 tokens contain none, so the emitted SQL is byte-identical).
_UNIVERSE_ALLOW = {"sp500", "nasdaq", "russell2k"}


def _safe_universe(u: Optional[str]) -> Optional[str]:
    """Return a whitelisted universe name, or None (= no filter) if not recognised."""
    if not u:
        return None
    s = str(u).strip().lower()
    return s if s in _UNIVERSE_ALLOW else None


def _q(v) -> str:
    """Escape a value for use inside a single-quoted SQL literal (no-op for clean tokens)."""
    return str(v).replace("'", "''")

# ── All mineable signal columns ───────────────────────────────────────────────
ALL_SIGNALS = [
    # T signals
    "sig_t1g","sig_t2g",
    "sig_t1","sig_t2","sig_t3","sig_t4","sig_t5","sig_t6",
    "sig_t9","sig_t10","sig_t11","sig_t12",
    # Z signals
    "sig_z1g","sig_z2g",
    "sig_z1","sig_z2","sig_z3","sig_z4","sig_z5","sig_z6",
    "sig_z9","sig_z10","sig_z11","sig_z12",
    # TZ state
    "sig_tz_flip","sig_bias_up",
    # Volume / VABS
    "sig_abs","sig_clm","sig_sc","sig_bc",
    "sig_fbo_up","sig_eb_up","sig_3up","sig_best_up",
    "sig_fbo_dn","sig_eb_dn","sig_vbo_dn",
    "sig_vol_5x","sig_vol_10x","sig_vol_20x",
    # L / WLNBB
    "sig_fri34","sig_fri43","sig_fri64",
    "sig_l555","sig_l2l4","sig_blue",
    "sig_cci","sig_cci0r","sig_ccib",
    "sig_rl","sig_rh","sig_pp",
    "sig_l_any","sig_be_any",
    "l34","l43","l22","be_up","bo_up","bx_up","vbo_up",
    # GOG
    "sig_g1","sig_g2","sig_g4","sig_g6","sig_g11","sig_gog_plus",
    "g1p","g2p","g3p","g1l","g2l","g1c","g2c","g3c",
    # FLY
    "sig_fly_abcd","sig_fly_cd","sig_fly_bd","sig_fly_ad",
    # WICK
    "sig_wk_up","sig_wk_dn","sig_x1","sig_x2","sig_x1g","sig_x3",
    # Combo / Momentum
    "sig_buy","sig_3g","sig_conso","sig_svs",
    "sig_cd","sig_ca","sig_cw","sig_seq_bcont",
    "sig_va","rocket","hilo_buy","sq",
    # EMA / PREUP
    "sig_p55","sig_p66","sig_p89","sig_any_p",
    # Delta
    "sig_flp_up","sig_org_up","sig_dd_up_red","sig_d_up_red",
    # CISD / PARA
    "sig_cisd_cplus","sig_para_prep","sig_para_start","sig_para_plus","sig_para_retest",
    # Meta
    "sig_not_ext","already_extended_flag",
    # Wyckoff / AD / Prebreak
    "ad_fresh","ad_cluster","wyc_spring","wyc_sos","wyc_in_tr",
    "prebreak_prime","prebreak_ready","prebreak_watch","pb_lvbo","pb_wvf_confirm",
    # EMA position
    "price_gt_89","price_gt_200","price_lt_89","price_lt_200",
    "rsi_le_35","rsi_ge_70",
]

# Timeframes available for analysis
TIMEFRAMES = [
    ("1d",  "fwd_1d",  None,    None),
    ("3d",  "fwd_3d",  None,    None),
    ("5d",  "fwd_5d",  "hit_5pct_5d",   "mae_5d"),
    ("10d", "fwd_10d", "hit_10pct_10d", "mae_10d"),
    ("20d", "fwd_20d", "hit_50pct_20d", "mae_20d"),
]

SORT_METRICS = [
    "win_5d", "avg_5d", "hit5_5d",
    "win_10d", "avg_10d", "hit10_10d",
    "expectancy_5d", "expectancy_10d",
    "win_1d", "avg_1d",
]


def _clean(v):
    """Return None for NaN/Inf floats, else the value."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 3)
    except (TypeError, ValueError):
        return None


def _outcome_stats(df: pd.DataFrame, min_n: int = 10) -> dict:
    """
    Compute forward-return statistics from a filtered DataFrame.
    Returns a dict with per-timeframe metrics.
    """
    n = len(df)
    if n < min_n:
        return {"n": n, "insufficient": True}

    result: dict = {"n": n, "insufficient": False}

    for label, fwd_col, hit_col, mae_col in TIMEFRAMES:
        if fwd_col not in df.columns:
            continue
        fwd = pd.to_numeric(df[fwd_col], errors="coerce").dropna()
        if len(fwd) == 0:
            continue

        wins    = (fwd > 0).sum()
        avg_ret = fwd.mean()
        avg_win = fwd[fwd > 0].mean() if wins > 0 else 0
        avg_los = fwd[fwd <= 0].mean() if (len(fwd) - wins) > 0 else 0
        win_r   = wins / len(fwd)
        loss_r  = 1 - win_r
        expect  = win_r * avg_win + loss_r * avg_los

        result[f"win_{label}"]   = _clean(win_r * 100)
        result[f"avg_{label}"]   = _clean(avg_ret * 100)
        result[f"exp_{label}"]   = _clean(expect * 100)
        result[f"n_{label}"]     = int(len(fwd))

        if hit_col and hit_col in df.columns:
            hit_vals = pd.to_numeric(df[hit_col], errors="coerce").fillna(0)
            result[f"hit_{label}"] = _clean(hit_vals.mean() * 100)

        if mae_col and mae_col in df.columns:
            mae_vals = pd.to_numeric(df[mae_col], errors="coerce").dropna()
            if len(mae_vals):
                result[f"mae_{label}"] = _clean(mae_vals.mean() * 100)

    return result


def _build_where(
    signals:    list[str],
    universe:   Optional[str],
    regime:     Optional[str],
    date_from:  Optional[str],
    date_to:    Optional[str],
    turbo_min:  Optional[float],
    turbo_max:  Optional[float],
    available:  list[str],
) -> tuple[str, list]:
    """Build SQL WHERE clause and params list."""
    clauses: list[str] = []
    params:  list      = []

    # Signal conditions (AND logic — all must be present)
    for sig in signals:
        col = sig.lower()
        if col in available:
            clauses.append(f"{col} = 1")
        else:
            log.warning("Signal column not in DB: %s", col)

    if universe:
        clauses.append("universe = ?")
        params.append(universe)
    if regime:
        clauses.append("final_regime = ?")
        params.append(regime)
    if date_from:
        clauses.append("date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("date <= ?")
        params.append(date_to)
    if turbo_min is not None:
        clauses.append("turbo_score >= ?")
        params.append(turbo_min)
    if turbo_max is not None:
        clauses.append("turbo_score <= ?")
        params.append(turbo_max)

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def query_combo(
    signals:    list[str],
    universe:   Optional[str]  = None,
    regime:     Optional[str]  = None,
    date_from:  Optional[str]  = None,
    date_to:    Optional[str]  = None,
    turbo_min:  Optional[float] = None,
    turbo_max:  Optional[float] = None,
    min_n:      int             = 5,
) -> dict:
    """
    Compute outcome statistics for a specific signal combo.
    Also returns baseline stats (same filters, no signals) for comparison.
    """
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()

        # ── Combo stats ──────────────────────────────────────────────────────
        where, params = _build_where(
            signals, universe, regime, date_from, date_to,
            turbo_min, turbo_max, available,
        )
        fwd_cols = [c for _, c, _, _ in TIMEFRAMES if c in available]
        hit_cols = [c for _, _, c, _ in TIMEFRAMES if c and c in available]
        mae_cols = [c for _, _, _, c in TIMEFRAMES if c and c in available]
        extra    = ["final_regime", "universe"]
        sel      = ", ".join(fwd_cols + hit_cols + mae_cols + extra)

        df = conn.execute(
            f"SELECT {sel} FROM bars WHERE {where}",
            params,
        ).fetchdf()

        combo_stats = _outcome_stats(df, min_n)
        combo_stats["signals"] = signals

        # ── Baseline (no signal filter, same context) ────────────────────────
        base_where, base_params = _build_where(
            [], universe, regime, date_from, date_to,
            turbo_min, turbo_max, available,
        )
        base_df = conn.execute(
            f"SELECT {sel} FROM bars WHERE {base_where} USING SAMPLE 50000",
            base_params,
        ).fetchdf()
        baseline = _outcome_stats(base_df, min_n=100)

        # ── Regime breakdown (optional enrichment) ───────────────────────────
        regime_breakdown: dict = {}
        if "final_regime" in df.columns and len(df) >= 30:
            for reg, grp in df.groupby("final_regime"):
                if grp is not None and len(grp) >= 10:
                    s = _outcome_stats(grp, min_n=10)
                    s["regime"] = reg
                    regime_breakdown[str(reg)] = s

        return {
            "combo":    combo_stats,
            "baseline": baseline,
            "regime_breakdown": regime_breakdown,
        }

    finally:
        conn.close()


def rank_signals(
    universe:   Optional[str]  = None,
    regime:     Optional[str]  = None,
    date_from:  Optional[str]  = None,
    date_to:    Optional[str]  = None,
    turbo_min:  Optional[float] = None,
    turbo_max:  Optional[float] = None,
    sort_by:    str             = "win_5d",
    min_n:      int             = 30,
    top_n:      int             = 60,
) -> dict:
    """
    Rank all single signals by a chosen metric.
    Returns list of {signal, label, n, win_5d, avg_5d, hit5_5d, win_10d, ...}
    sorted descending by sort_by.
    """
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        sig_cols  = [s for s in ALL_SIGNALS if s in available]

        fwd_cols = [c for _, c, _, _ in TIMEFRAMES if c in available]
        hit_cols = [c for _, _, c, _ in TIMEFRAMES if c and c in available]
        mae_cols = [c for _, _, _, c in TIMEFRAMES if c and c in available]
        sel      = ", ".join(sig_cols + fwd_cols + hit_cols + mae_cols)

        # Context filter (no signal yet)
        base_where, base_params = _build_where(
            [], universe, regime, date_from, date_to,
            turbo_min, turbo_max, available,
        )
        df = conn.execute(
            f"SELECT {sel} FROM bars WHERE {base_where}",
            base_params,
        ).fetchdf()

        log.info("rank_signals: loaded %d base rows, computing %d signals", len(df), len(sig_cols))

        rows: list[dict] = []
        for col in sig_cols:
            if col not in df.columns:
                continue
            mask = pd.to_numeric(df[col], errors="coerce").fillna(0) >= 1
            sub  = df[mask]
            if len(sub) < min_n:
                continue
            stats = _outcome_stats(sub, min_n=min_n)
            if stats.get("insufficient"):
                continue
            stats["signal"] = col
            rows.append(stats)

        # Sort
        def sort_key(r):
            v = r.get(sort_by)
            return v if v is not None else -9999

        rows.sort(key=sort_key, reverse=True)
        return {
            "total":   len(rows),
            "sort_by": sort_by,
            "rows":    rows[:top_n],
        }

    finally:
        conn.close()


def query_tz_sequence(
    sequence:  list[str | None],
    universe:  Optional[str]  = None,
    regime:    Optional[str]  = None,
    min_n:     int            = 5,
) -> dict:
    """
    Query Studio DuckDB for next-bar T/Z signal distribution after a sequence.

    sequence : list of signal names ("T1", "Z4", etc.) or None (wildcard).
               sequence[0] = oldest bar, sequence[-1] = most recent bar.
               Returns what T/Z signal appears on the NEXT bar after sequence[-1].

    Uses LEAD/LAG window functions on the bars table (fast on DuckDB columnar store).
    """
    n = len(sequence)
    if n == 0:
        return {"total_matches": 0, "top_outcomes": [], "sequence_label": ""}

    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        if "t_sig" not in available:
            return {"total_matches": 0, "top_outcomes": [],
                    "sequence_label": " → ".join(s or "?" for s in sequence),
                    "error": "t_sig column not available — re-import CSV"}

        # ── Base filter (universe / regime) ──────────────────────────────────
        base_clauses: list[str] = []
        _uni = _safe_universe(universe)
        if _uni:
            base_clauses.append(f"universe = '{_uni}'")
        base_where = f"WHERE {' AND '.join(base_clauses)}" if base_clauses else ""

        # ── LAG expressions — we need n positions ────────────────────────────
        # sequence[-1] → LAG 0 (sig_now / current row)
        # sequence[-2] → LAG 1 (1 bar ago)
        # sequence[0]  → LAG (n-1)
        lag_exprs = []
        for lag in range(1, n):          # LAG(1) .. LAG(n-1)
            lag_exprs.append(
                f"LAG(tz_sig, {lag}) OVER w AS sig_{lag}ago"
            )
        lag_select = (",\n             " + ",\n             ".join(lag_exprs)) if lag_exprs else ""

        # ── Outer WHERE conditions ────────────────────────────────────────────
        seq_clauses: list[str] = []
        if regime:
            seq_clauses.append(f"regime = '{_q(regime)}'")
        for i, sig in enumerate(sequence):
            lag = n - 1 - i         # sequence[0] → LAG(n-1), sequence[-1] → LAG(0)
            col = "sig_now" if lag == 0 else f"sig_{lag}ago"
            if sig is not None and sig.upper() not in ("NONE", "—", "ANY", ""):
                # Exact match
                seq_clauses.append(f"{col} = '{_q(sig)}'")
        outer_where = f"WHERE {' AND '.join(seq_clauses)}" if seq_clauses else ""

        sql = f"""
        WITH tz AS (
          SELECT ticker, date,
                 COALESCE(t_sig, z_sig) AS tz_sig,
                 final_regime           AS regime
          FROM bars {base_where}
        ),
        lagged AS (
          SELECT
            LEAD(tz_sig, 1) OVER w AS next_sig,
            tz_sig                  AS sig_now,
            regime{lag_select}
          FROM tz
          WINDOW w AS (PARTITION BY ticker ORDER BY date)
        )
        SELECT COALESCE(next_sig, 'NONE') AS next_signal,
               COUNT(*) AS cnt
        FROM lagged
        {outer_where}
        GROUP BY next_signal
        ORDER BY cnt DESC
        LIMIT 20
        """

        rows = conn.execute(sql).fetchdf()
        total = int(rows["cnt"].sum()) if len(rows) > 0 else 0

        top = []
        for _, r in rows.iterrows():
            sig = str(r["next_signal"])
            cnt = int(r["cnt"])
            top.append({
                "sig_name": sig,
                "count":    cnt,
                "pct":      round(cnt / total * 100) if total > 0 else 0,
                "is_bull":  sig.startswith("T"),
                "is_bear":  sig.startswith("Z"),
            })

        seq_label = " → ".join(s if s else "?" for s in sequence)
        return {
            "total_matches":  total,
            "top_outcomes":   top,
            "sequence_label": seq_label,
            "n_bars":         n,
        }
    except Exception as exc:
        log.exception("query_tz_sequence failed")
        return {"total_matches": 0, "top_outcomes": [],
                "sequence_label": " → ".join(s or "?" for s in sequence),
                "error": str(exc)}
    finally:
        conn.close()


# ── Confluence signal groups ──────────────────────────────────────────────────
CONFLUENCE_GROUPS: dict[str, list[str]] = {
    "wlnbb": ["l34", "l43", "l22", "be_up", "bo_up", "bx_up",
              "sig_fri34", "sig_fri43", "sig_fri64", "sig_blue", "sig_rl", "sig_rh", "sig_l_any"],
    "wick":  ["sig_wk_up", "sig_wk_dn", "sig_x1", "sig_x2", "sig_x3"],
    "gog":   ["sig_g1", "sig_g2", "sig_g4", "sig_g6", "sig_g11", "sig_gog_plus"],
    "para":  ["sig_cisd_cplus", "sig_para_prep", "sig_para_start", "sig_para_plus",
              "prebreak_prime", "prebreak_ready"],
    "vabs":  ["sig_abs", "sig_clm", "sig_sc"],
}
CONFLUENCE_LEVEL_LABELS = {
    "wlnbb": "WLNBB / L-signals",
    "wick":  "Wick signals",
    "gog":   "GOG",
    "para":  "PARA / Prebreak",
    "vabs":  "VABS",
}
# Default ordering of levels shown in the funnel
CONFLUENCE_LEVEL_ORDER = ["wlnbb", "wick", "gog", "para", "vabs"]


def query_confluence_sequence(
    bars:     list[str | None],   # T/Z signal per bar (None = wildcard). bars[0]=oldest, bars[-1]=most recent
    universe: Optional[str] = None,
) -> dict:
    """
    Count how many times an N-bar T/Z sequence appears at each confluence level.

    Level 0: baseline — total bars in the (filtered) universe
    Level 1: T/Z sequence only
    Level 2: Level 1 + at least one WLNBB signal across any bar in the sequence
    Level 3: + at least one Wick signal
    Level 4: + at least one GOG signal
    Level 5: + at least one PARA signal
    Level 6: + at least one VABS signal

    bars : list of T/Z signal names ("T1", "Z4", etc.) or None (wildcard).
           bars[0] = oldest, bars[-1] = most recent (current bar).
    """
    n = len(bars)
    if n == 0:
        return {"levels": [], "baseline": 0, "sequence_label": ""}

    conn = get_conn(read_only=True)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        if "t_sig" not in available:
            return {"levels": [], "baseline": 0, "sequence_label": "",
                    "error": "t_sig column missing — re-import CSV"}

        # ── Universe filter ───────────────────────────────────────────────────
        _uni = _safe_universe(universe)
        base_where = f"WHERE universe = '{_uni}'" if _uni else ""

        # ── Per-bar boolean expressions for each group ────────────────────────
        group_exprs: dict[str, str] = {}
        for grp_key in CONFLUENCE_LEVEL_ORDER:
            cols = [c for c in CONFLUENCE_GROUPS[grp_key] if c in available]
            if not cols:
                group_exprs[grp_key] = "FALSE"
            else:
                parts = " OR ".join(f"COALESCE({c}::BOOLEAN, FALSE)" for c in cols)
                group_exprs[grp_key] = f"({parts})"

        # ── CTE1: base — compute per-bar group flags ──────────────────────────
        grp_select = "\n    ".join(
            f"{group_exprs[g]} AS {g}_flag,"
            for g in CONFLUENCE_LEVEL_ORDER
        )
        # ── CTE2: lagged — LAG window for tz_sig and each group flag ─────────
        tz_lags  = ["tz_sig AS tz_0"]
        grp_ors: dict[str, list[str]] = {g: [f"{g}_flag"] for g in CONFLUENCE_LEVEL_ORDER}

        for lag in range(1, n):
            tz_lags.append(f"LAG(tz_sig, {lag}) OVER w AS tz_{lag}")
            for g in CONFLUENCE_LEVEL_ORDER:
                grp_ors[g].append(f"LAG({g}_flag, {lag}) OVER w")

        tz_lag_select = ",\n    ".join(tz_lags)
        grp_lag_select = ",\n    ".join(
            f"({' OR '.join(parts)}) AS has_{g}"
            for g, parts in grp_ors.items()
        )

        # ── Outer WHERE: T/Z sequence conditions ─────────────────────────────
        seq_clauses: list[str] = []
        for i, sig in enumerate(bars):
            lag = n - 1 - i          # bars[0] → LAG(n-1), bars[-1] → LAG(0)
            col = f"tz_{lag}"
            if sig is not None and sig.upper() not in ("NONE", "—", "ANY", ""):
                seq_clauses.append(f"{col} = '{_q(sig)}'")
        outer_where = ("WHERE " + " AND ".join(seq_clauses)) if seq_clauses else ""

        # ── Progressive FILTER SELECT ─────────────────────────────────────────
        active_groups: list[str] = []
        filter_counts: list[str] = ["COUNT(*) AS cnt_1"]
        for i, g in enumerate(CONFLUENCE_LEVEL_ORDER, start=2):
            active_groups.append(f"has_{g}")
            cond = " AND ".join(active_groups)
            filter_counts.append(f"COUNT(*) FILTER (WHERE {cond}) AS cnt_{i}")

        filter_select = ",\n    ".join(filter_counts)

        sql = f"""
        WITH base AS (
          SELECT ticker, date,
            COALESCE(t_sig, z_sig) AS tz_sig,
            {grp_select}
            final_regime AS regime
          FROM bars {base_where}
        ),
        lagged AS (
          SELECT
            {tz_lag_select},
            {grp_lag_select}
          FROM base
          WINDOW w AS (PARTITION BY ticker ORDER BY date)
        )
        SELECT
          {filter_select}
        FROM lagged
        {outer_where}
        """

        row   = conn.execute(sql).fetchone()
        total = conn.execute(
            f"SELECT COUNT(*) FROM bars {base_where}"
        ).fetchone()[0]

        seq_label = " → ".join(s if s else "?" for s in bars)

        # Build levels list
        levels = []
        prev_count: int | None = None
        for idx, (grp, cnt_raw) in enumerate(
            [("tz_only", row[0])] +
            [(g, row[i + 1]) for i, g in enumerate(CONFLUENCE_LEVEL_ORDER)]
        ):
            cnt = int(cnt_raw) if cnt_raw is not None else 0
            pct_total = round(cnt / total * 100, 4) if total > 0 else 0.0
            pct_prev  = (round(cnt / prev_count * 100, 1)
                         if prev_count and prev_count > 0 else 100.0)
            if grp == "tz_only":
                label = "T/Z sequence"
            else:
                label = f"+ {CONFLUENCE_LEVEL_LABELS.get(grp, grp)}"

            levels.append({
                "level":       idx + 1,
                "group":       grp,
                "label":       label,
                "count":       cnt,
                "pct_total":   pct_total,
                "pct_prev":    pct_prev,
            })
            prev_count = cnt

        return {
            "levels":         levels,
            "baseline":       int(total),
            "sequence_label": seq_label,
            "n_bars":         n,
        }

    except Exception as exc:
        log.exception("query_confluence_sequence failed")
        return {"levels": [], "baseline": 0,
                "sequence_label": " → ".join(s or "?" for s in bars),
                "error": str(exc)}
    finally:
        conn.close()


def _parse_tz_wild(tz_only: str) -> tuple[str, str]:
    """Like _parse_tz but supports '*' wildcard (converted to SQL LIKE '%' pattern).

    Examples:
      'T*'  → ('T%', '')    — any T signal
      'T2*' → ('T2%', '')   — any T2-prefix signal
      'Z*'  → ('', 'Z%')    — any Z signal
      '*'   → ('', '')      — skip TZ condition (match anything)
    """
    if not tz_only:
        return ('', '')
    su = tz_only.strip().upper()
    if '*' not in su:
        return _parse_tz(su)
    if su == '*':
        return ('', '')           # bare wildcard → skip condition
    if su.startswith('T'):
        return (su.replace('*', '%'), '')
    if su.startswith('Z'):
        return ('', su.replace('*', '%'))
    return ('', '')


def _sql_cond(col_alias: str, value: str) -> str:
    """Return SQL equality or LIKE condition based on whether value contains '%' (LIKE wildcard).
    Value is quote-escaped (no-op for clean tokens) so user input can't break out of the literal."""
    safe = _q(value)
    if '%' in value:
        return f"{col_alias} LIKE '{safe}'"
    return f"{col_alias} = '{safe}'"


def _multi_cond(col_alias: str, value: str):
    """Build a per-field condition that supports space-separated tokens and a '!'
    NOT prefix. Positives are OR'd (a "one of" set), negatives AND'd:
      'L34'        -> lsig = 'L34'
      'L% !L34'    -> (lsig LIKE 'L%') AND NOT (lsig = 'L34')    # any L but not L34
      'T2 T3'      -> (t = 'T2' OR t = 'T3')
      '!T1'        -> NOT (t = 'T1')
    Returns a SQL fragment or None if no usable token. ('%' already substituted
    for '*' upstream; '!' survives .strip()/.upper()/_wild.)"""
    if not value:
        return None
    pos, neg = [], []
    for tok in value.split():
        if tok.startswith('!'):
            t = tok[1:]
            if t:
                neg.append(_sql_cond(col_alias, t))
        else:
            pos.append(_sql_cond(col_alias, tok))
    parts = []
    if pos:
        parts.append("(" + " OR ".join(pos) + ")")
    parts.extend(f"NOT ({c})" for c in neg)
    return " AND ".join(parts) if parts else None


def _tz_multi_cond(t_alias: str, z_alias: str, raw: str):
    """Like _multi_cond but for the TZ field, routing each token to the t_sig or
    z_sig column (Z-prefixed → z, else t), with the same '!' NOT support.
      'T% !T1'  -> (t LIKE 'T%') AND NOT (t = 'T1')
      'Z6'      -> z = 'Z6'"""
    if not raw:
        return None
    pos_t, neg_t, pos_z, neg_z = [], [], [], []
    for tok in raw.split():
        neg = tok.startswith('!')
        v = tok[1:] if neg else tok
        if not v:
            continue
        if v[0] == 'Z':
            (neg_z if neg else pos_z).append(_sql_cond(z_alias, v))
        else:
            (neg_t if neg else pos_t).append(_sql_cond(t_alias, v))
    parts = []
    if pos_t:
        parts.append("(" + " OR ".join(pos_t) + ")")
    parts.extend(f"NOT ({c})" for c in neg_t)
    if pos_z:
        parts.append("(" + " OR ".join(pos_z) + ")")
    parts.extend(f"NOT ({c})" for c in neg_z)
    return " AND ".join(parts) if parts else None


def _parse_tz(tz_only: str) -> tuple[str, str]:
    """Parse a TZ-only string like 'T2G' or 'Z4' into (t_sig, z_sig).
    Returns ('', '') for empty/None input.
    """
    if not tz_only:
        return ("", "")
    import re
    s = str(tz_only).strip().upper()
    if not s or s in ("NONE", "—", "?"):
        return ("", "")
    m = re.match(r"^(T\d+G?)$", s)
    if m:
        return (m.group(1), "")
    m = re.match(r"^(Z\d+G?)$", s)
    if m:
        return ("", m.group(1))
    return ("", "")


def _parse_l_digits(l_only: str) -> list[int]:
    """Parse an L-only string like 'L3', 'L34', 'L555' into a list of digit ints.
    Returns [] for empty/None/non-L input.
    """
    if not l_only:
        return []
    import re
    s = str(l_only).strip().upper()
    if not s or s in ("NONE", "—", "?"):
        return []
    m = re.match(r"^L(\d+)$", s)
    if not m:
        return []
    return sorted({int(c) for c in m.group(1) if c.isdigit() and int(c) in range(1, 7)})


def _parse_tz_l(combined: str) -> tuple[str, str, list[int]]:
    """Legacy: parse a combined "T2GL3" / "Z2GL5" string into (t_sig, z_sig, l_digits).
    Kept for backward compatibility — new code should use _parse_tz + _parse_l_digits.
    """
    if not combined:
        return ("", "", [])
    import re
    s = str(combined).strip().upper()
    if not s or s in ("NONE", "—", "?"):
        return ("", "", [])
    t = ""; z = ""; l_digits: list[int] = []
    m = re.match(r"^(T\d+G?)", s)
    if m:
        t = m.group(1); s = s[len(t):]
    else:
        m = re.match(r"^(Z\d+G?)", s)
        if m:
            z = m.group(1); s = s[len(z):]
    if s.startswith("L"):
        m = re.match(r"^L(\d+)", s)
        if m:
            l_digits = sorted({int(c) for c in m.group(1) if c.isdigit() and int(c) in range(1, 7)})
    return (t, z, l_digits)


def query_exact_sequence(
    bars:       list[dict],            # each: {tz, l, suffix, body_wick, gap_range, line5}
    universe:   Optional[str] = None,
    strictness: dict | None  = None,   # which lines to match
    pivot_lr:   int = 3,               # 3 or 5 — Williams pivot variant for HL/HH stats
    conn=None,                          # optional shared read-only connection (reused by callers
                                        # that loop — e.g. edge scanner — to avoid re-opening the
                                        # DB per ticker, which is the main cost on a 2.4GB file)
) -> dict:
    """
    Exact-match N-bar sequence query with HL/HH outcome statistics.

    Inputs:
      bars[0] = oldest, bars[-1] = most recent (current).
      Each bar dict keys (all optional — empty means wildcard for that line):
        tz          — TZ only,             e.g. "T2G", "Z4"
        l           — L code only,         e.g. "L3", "L34", "L555"
        suffix      — full_suffix,         e.g. "EU", "NURA"
        body_wick   — bar_body_wick,       e.g. "STB", "M"
        gap_range   — bar_gap_range,       e.g. "G1-C", "N"
        line5       — bar_line5,           e.g. "PS-R2X", "PB"

      strictness keys (defaults: line1, line2 = True; others = False):
        line1 = TZ · line2 = L · line3 = suffix
        line4 = body_wick · line5 = gap_range · line6 = bar_line5

      Backward compat: if bar dict has only "tz" key with combined "T2GL3" format,
      it auto-splits into TZ + L.

      pivot_lr = 3 or 5 — which Williams variant for HL/HH outcomes.

    Returns:
      matches, sequence_label, baseline (universe row count),
      outcomes: { hl_count, hl_pct, hh_count, hh_pct,
                  avg_pct_to_hl, avg_pct_to_hh,
                  avg_bars_to_hl, avg_bars_to_hh,
                  avg_fwd_5d, avg_fwd_10d, avg_fwd_20d, win_5d, win_10d, win_20d }
    """
    n = len(bars)
    if n == 0:
        return {"matches": 0, "sequence_label": "", "outcomes": {}}

    strict = {**{"line1": True, "line2": True, "line3": False,
                 "line4": False, "line5": False, "line6": False, "line7": False,
                 "line8": False, "line9": False},
              **(strictness or {})}
    pivot_lr = 3 if pivot_lr not in (3, 5) else pivot_lr
    P = str(pivot_lr)

    # Parse each bar — support both new (tz/l separate) and legacy (combined tz='T2GL3')
    # '*' wildcards are supported in all fields: T* → LIKE 'T%', L* → LIKE 'L%', etc.
    def _wild(v: str) -> str:
        """Replace '*' with SQL LIKE '%' wildcard in a string field."""
        return v.replace('*', '%') if v else v

    def _parse_range(v: str):
        """Parse a numeric range field like '20-35' → (20.0, 35.0). Open ends:
        '20-' → (20, None) [>=20], '-35' → (None, 35) [<=35], '20' → (20, None).
        Returns (None, None) if blank/malformed (line is then a no-op)."""
        v = (v or "").strip()
        if not v:
            return (None, None)
        parts = v.split('-')
        try:
            lo = float(parts[0]) if parts[0].strip() else None
            hi = float(parts[1]) if len(parts) > 1 and parts[1].strip() else None
            return (lo, hi)
        except Exception:
            return (None, None)

    parsed = []
    for b in bars:
        b = b or {}
        tz_raw = (b.get("tz") or "").strip()
        l_raw  = (b.get("l")  or "").strip()
        if l_raw:
            # New schema — tz + l separate
            t, z = _parse_tz_wild(tz_raw)
            if '*' in l_raw:
                l_digits = []
                l_str = l_raw.upper().replace('*', '%')
            else:
                l_digits = _parse_l_digits(l_raw)
                l_str = l_raw.upper()
        elif tz_raw and ("L" in tz_raw.upper()) and not l_raw:
            # Legacy: combined "T2GL3" format with no separate l field
            t, z, l_digits = _parse_tz_l(tz_raw)
            l_str = ("L" + "".join(str(d) for d in l_digits)) if l_digits else ""
        else:
            # tz-only input, no L
            t, z = _parse_tz_wild(tz_raw)
            l_digits = []
            l_str = ""
        parsed.append({
            "t": t, "z": z, "l_digits": l_digits, "l_str": l_str,
            # raw wildcarded TZ field (for negation/multi-token line1 + label);
            # tz_legacy = combined "T2GL3" path, which keeps the old positive-only parse.
            "tz_raw":    _wild(tz_raw.upper()),
            "tz_legacy": bool(tz_raw and ("L" in tz_raw.upper()) and not l_raw),
            "suffix":    _wild((b.get("suffix") or "").strip().upper()),
            "body_wick": _wild((b.get("body_wick") or "").strip().upper()),
            "gap_range": _wild((b.get("gap_range") or "").strip().upper()),
            "line5":     _wild((b.get("line5") or "").strip().upper()),
            "vol":       _wild((b.get("vol") or "").strip().upper()),   # line7 = volume bucket (W/L/N/B/VB)
            "ema":       _wild((b.get("ema") or "").strip().upper()),   # line8 = EMA-cross P/D code (P2..P89/D2..D89, P*/D*/*)
            "rsi_rng":   _parse_range(b.get("rsi") or ""),              # line9 = rsi_14 numeric range (lo, hi)
        })

    _own_conn = conn is None
    if _own_conn:
        conn = get_conn(read_only=True)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        needed = {"bar_body_wick", "bar_gap_range", "bar_line5", "full_suffix",
                  f"next_pivot_is_hl_{P}", f"next_pivot_is_hh_{P}",
                  f"pct_to_next_hl_{P}",   f"pct_to_next_hh_{P}"}
        missing = needed - available
        if missing:
            return {"matches": 0, "sequence_label": "", "outcomes": {},
                    "error": f"Enrichment columns missing: {sorted(missing)} — run /api/studio/enrich"}

        # line8 = EMA-cross. Derive a single code from the per-bar P/D boolean
        # flags (priority: fast cross first). 'P*'/'D*'/'*' wildcards handle "any".
        # If the columns aren't enriched yet, ema_code degrades to '' (line8 is a
        # no-op) rather than erroring.
        _pd_cols = ["sig_p2", "sig_p3", "sig_p50", "sig_p55", "sig_p66", "sig_p89",
                    "sig_d2", "sig_d3", "sig_d50", "sig_d55", "sig_d66", "sig_d89"]
        if all(c in available for c in _pd_cols):
            ema_case = (
                "CASE WHEN sig_p2=1 THEN 'P2' WHEN sig_p3=1 THEN 'P3' WHEN sig_p50=1 THEN 'P50' "
                "WHEN sig_p55=1 THEN 'P55' WHEN sig_p66=1 THEN 'P66' WHEN sig_p89=1 THEN 'P89' "
                "WHEN sig_d2=1 THEN 'D2' WHEN sig_d3=1 THEN 'D3' WHEN sig_d50=1 THEN 'D50' "
                "WHEN sig_d55=1 THEN 'D55' WHEN sig_d66=1 THEN 'D66' WHEN sig_d89=1 THEN 'D89' "
                "ELSE '' END")
        else:
            ema_case = "''"

        _uni = _safe_universe(universe)
        base_where = f"WHERE universe = '{_uni}'" if _uni else ""
        # Dedup to ONE row per (ticker, date). Without a universe filter the same
        # ticker-date lives in several universe rows (e.g. AAPL in sp500+nasdaq);
        # the LAG/LEAD window (PARTITION BY ticker ORDER BY date) would otherwise
        # treat those duplicates as consecutive bars — inflating match counts and,
        # critically, making LEAD("next bar") point at the same date's other-universe
        # row (identical codes), which produced impossible "T4 → T4" predictions.
        base_cte = (f"SELECT *, {ema_case} AS ema_code FROM bars {base_where} "
                    f"QUALIFY ROW_NUMBER() OVER "
                    f"(PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1")

        # ── Build LAG SELECT clause ─────────────────────────────────────────
        # current bar = lag 0, oldest = lag n-1
        select_cols = []
        # T/Z + l_sig (chart-format L) + line3/4/5 strings + digit flags fallback.
        # NOTE: suffix uses composite_full_suffix (chart's display = ne+wick+pen+close
        # when "interesting"). DB has both full_suffix and composite_full_suffix —
        # we use composite to match chart exactly (e.g. "NURA" not "NUR", "NDPO" not "NDP").
        for lag in range(n):
            base_cols = ["t_sig", "z_sig", "l_sig", "composite_full_suffix",
                         "bar_body_wick", "bar_gap_range", "bar_line5", "vol_bucket",
                         "ema_code", "rsi_14"]
            short_map = {"t_sig":"t","z_sig":"z","l_sig":"lsig",
                         "composite_full_suffix":"s","bar_body_wick":"bw",
                         "bar_gap_range":"gr","bar_line5":"l5","vol_bucket":"vb",
                         "ema_code":"em","rsi_14":"rsi"}
            for col in base_cols:
                short = short_map[col]
                if lag == 0:
                    select_cols.append(f"{col} AS {short}_0")
                else:
                    select_cols.append(f"LAG({col}, {lag}) OVER w AS {short}_{lag}")
            # Individual L digit flags — use "d" prefix; fallback for legacy data
            for d in range(1, 7):
                if lag == 0:
                    select_cols.append(f"sig_l{d} AS d{d}_0")
                else:
                    select_cols.append(f"LAG(sig_l{d}, {lag}) OVER w AS d{d}_{lag}")
        # Outcome cols (only for current bar)
        for col in (f"next_pivot_is_hl_{P}", f"next_pivot_is_hh_{P}",
                    f"pct_to_next_hl_{P}",   f"pct_to_next_hh_{P}",
                    f"bars_to_next_hl_{P}",  f"bars_to_next_hh_{P}",
                    "fwd_5d", "fwd_10d", "fwd_20d"):
            select_cols.append(col)
        # Next bar (the bar AFTER the sequence ends) — for "most likely next signal"
        select_cols.append("LEAD(t_sig, 1) OVER w AS nxt_t")
        select_cols.append("LEAD(z_sig, 1) OVER w AS nxt_z")

        # ── Build WHERE conditions per bar ────────────────────────────────────
        # Every categorical line goes through _multi_cond, which supports '%'
        # wildcards (from user '*'), space-separated OR-sets, and a '!' NOT prefix
        # (e.g. 'T* !T1' = any T but not T1).  line9 (RSI) is numeric → handled
        # separately further down.
        conds = []
        for i, p in enumerate(parsed):
            lag = n - 1 - i   # bars[0] → LAG(n-1), bars[-1] → LAG(0)
            # line1 = TZ (t_sig / z_sig). Legacy combined "T2GL3" keeps the old
            # positive-only parse; the modern tz field supports negation/multi-token.
            if strict["line1"]:
                if p.get("tz_legacy"):
                    if p["t"]:
                        conds.append(_sql_cond(f"t_{lag}", p["t"]))
                    if p["z"]:
                        conds.append(_sql_cond(f"z_{lag}", p["z"]))
                else:
                    c = _tz_multi_cond(f"t_{lag}", f"z_{lag}", p.get("tz_raw", ""))
                    if c:
                        conds.append(c)
            # line2 = L (chart-format) — l_sig codes like "L34", "BO↑", "FRI34"
            if strict["line2"] and p["l_str"]:
                c = _multi_cond(f"lsig_{lag}", p["l_str"]);  conds.append(c) if c else None
            # line3 = suffix
            if strict["line3"] and p["suffix"]:
                c = _multi_cond(f"s_{lag}", p["suffix"]);    conds.append(c) if c else None
            # line4 = body/wick
            if strict["line4"] and p["body_wick"]:
                c = _multi_cond(f"bw_{lag}", p["body_wick"]); conds.append(c) if c else None
            # line5 = gap/range
            if strict["line5"] and p["gap_range"]:
                c = _multi_cond(f"gr_{lag}", p["gap_range"]); conds.append(c) if c else None
            # line6 = bar_line5 (VIX/PSAR/RSI2)
            if strict["line6"] and p["line5"]:
                c = _multi_cond(f"l5_{lag}", p["line5"]);     conds.append(c) if c else None
            # line7 = volume bucket (W/L/N/B/VB)
            if strict["line7"] and p["vol"]:
                c = _multi_cond(f"vb_{lag}", p["vol"]);       conds.append(c) if c else None
            # line8 = EMA-cross code (P2..P89 / D2..D89; 'P*'/'D*'/'*' wildcards, '!' NOT)
            if strict.get("line8") and p["ema"]:
                c = _multi_cond(f"em_{lag}", p["ema"]);       conds.append(c) if c else None
            # line9 = rsi_14 numeric range (lo/hi are validated floats — injection-safe)
            if strict.get("line9"):
                _lo, _hi = p["rsi_rng"]
                if _lo is not None:
                    conds.append(f"rsi_{lag} >= {_lo}")
                if _hi is not None:
                    conds.append(f"rsi_{lag} <= {_hi}")

        outer_where = ("WHERE " + " AND ".join(conds)) if conds else ""

        sql = f"""
        WITH base AS (
          {base_cte}
        ),
        lagged AS (
          SELECT
            {", ".join(select_cols)}
          FROM base
          WINDOW w AS (PARTITION BY ticker ORDER BY date)
        )
        SELECT
          COUNT(*) AS matches,
          SUM(CASE WHEN next_pivot_is_hl_{P} = 1 THEN 1 ELSE 0 END) AS hl_count,
          SUM(CASE WHEN next_pivot_is_hh_{P} = 1 THEN 1 ELSE 0 END) AS hh_count,
          AVG(CASE WHEN pct_to_next_hl_{P} IS NOT NULL THEN pct_to_next_hl_{P} END)  AS avg_pct_to_hl,
          AVG(CASE WHEN pct_to_next_hh_{P} IS NOT NULL THEN pct_to_next_hh_{P} END)  AS avg_pct_to_hh,
          AVG(CASE WHEN bars_to_next_hl_{P} IS NOT NULL THEN bars_to_next_hl_{P} END) AS avg_bars_to_hl,
          AVG(CASE WHEN bars_to_next_hh_{P} IS NOT NULL THEN bars_to_next_hh_{P} END) AS avg_bars_to_hh,
          AVG(fwd_5d)  AS avg_fwd_5d,
          AVG(fwd_10d) AS avg_fwd_10d,
          AVG(fwd_20d) AS avg_fwd_20d,
          SUM(CASE WHEN fwd_5d  > 0 THEN 1 ELSE 0 END) AS win_5d_n,
          SUM(CASE WHEN fwd_10d > 0 THEN 1 ELSE 0 END) AS win_10d_n,
          SUM(CASE WHEN fwd_20d > 0 THEN 1 ELSE 0 END) AS win_20d_n,
          COUNT(fwd_5d)  AS fwd_5d_n,
          COUNT(fwd_10d) AS fwd_10d_n,
          COUNT(fwd_20d) AS fwd_20d_n
        FROM lagged
        {outer_where}
        """

        row = conn.execute(sql).fetchone()
        matches    = int(row[0] or 0)
        hl_count   = int(row[1] or 0)
        hh_count   = int(row[2] or 0)
        next_pivot_known = hl_count + hh_count
        baseline = conn.execute(f"SELECT COUNT(*) FROM ({base_cte})").fetchone()[0]

        # ── Next-bar signal distribution — what T/Z fires on the bar AFTER the
        # matched sequence. Excludes end-of-data rows (no next bar exists). ──────
        next_bar, next_bar_total = [], 0
        if matches > 0:
            nb_conds = conds + ["(nxt_t IS NOT NULL OR nxt_z IS NOT NULL)"]
            nb_where = "WHERE " + " AND ".join(nb_conds)
            nb_sql = f"""
            WITH base AS (
              {base_cte}
            ),
            lagged AS (
              SELECT
                {", ".join(select_cols)}
              FROM base
              WINDOW w AS (PARTITION BY ticker ORDER BY date)
            )
            SELECT COALESCE(NULLIF(nxt_t, ''), NULLIF(nxt_z, ''), 'NONE') AS nsig,
                   COUNT(*) AS cnt
            FROM lagged
            {nb_where}
            GROUP BY nsig
            ORDER BY cnt DESC
            """
            nb_rows = conn.execute(nb_sql).fetchall()
            next_bar_total = sum(int(c or 0) for _, c in nb_rows)
            for nsig, cnt in nb_rows[:12]:
                s = str(nsig or "NONE")
                next_bar.append({
                    "sig":     s,
                    "count":   int(cnt or 0),
                    "pct":     round(int(cnt or 0) / next_bar_total * 100, 1) if next_bar_total else 0,
                    "is_bull": s.startswith("T"),
                    "is_bear": s.startswith("Z"),
                })

        def _round(v, nd=2):
            return None if v is None else round(float(v), nd)

        def _pct(n, d):
            return round(n / d * 100, 1) if d > 0 else None

        outcomes = {
            "hl_count":         hl_count,
            "hh_count":         hh_count,
            "next_pivot_known": next_pivot_known,
            "hl_pct":           _pct(hl_count, next_pivot_known),
            "hh_pct":           _pct(hh_count, next_pivot_known),
            "avg_pct_to_hl":    _round(row[3]),
            "avg_pct_to_hh":    _round(row[4]),
            "avg_bars_to_hl":   _round(row[5], 1),
            "avg_bars_to_hh":   _round(row[6], 1),
            "avg_fwd_5d":       _round(row[7]),
            "avg_fwd_10d":      _round(row[8]),
            "avg_fwd_20d":      _round(row[9]),
            "win_5d_pct":       _pct(int(row[10] or 0), int(row[13] or 0)),
            "win_10d_pct":      _pct(int(row[11] or 0), int(row[14] or 0)),
            "win_20d_pct":      _pct(int(row[12] or 0), int(row[15] or 0)),
            "fwd_5d_n":         int(row[13] or 0),
            "fwd_10d_n":        int(row[14] or 0),
            "fwd_20d_n":        int(row[15] or 0),
        }

        # Build sequence label — show TZ and L separately
        def _bar_label(p):
            tz_str = p.get("tz_raw") or p["t"] or p["z"] or "?"
            l_label = p["l_str"] or "—"
            parts = [tz_str, l_label]
            for k in ("suffix", "body_wick", "gap_range", "line5"):
                parts.append(p[k] or "—")
            if p.get("ema"):
                parts.append(p["ema"])
            _lo, _hi = p.get("rsi_rng", (None, None))
            if _lo is not None or _hi is not None:
                parts.append(f"RSI{_lo if _lo is not None else ''}-{_hi if _hi is not None else ''}")
            return " / ".join(parts)
        seq_label = "  ➜  ".join(_bar_label(p) for p in parsed)

        return {
            "matches":         matches,
            "baseline":        int(baseline),
            "sequence_label":  seq_label,
            "outcomes":        outcomes,
            "next_bar":        next_bar,
            "next_bar_total":  next_bar_total,
            "pivot_lr":        pivot_lr,
            "n_bars":          n,
            "strictness":      strict,
        }

    except Exception as exc:
        log.exception("query_exact_sequence failed")
        return {"matches": 0, "sequence_label": "", "outcomes": {},
                "error": str(exc)}
    finally:
        if _own_conn:
            conn.close()


def get_available_filters() -> dict:
    """Return distinct values for regime and universe filters."""
    conn = get_conn(read_only=True)
    try:
        universes = [r[0] for r in conn.execute(
            "SELECT DISTINCT universe FROM bars WHERE universe IS NOT NULL ORDER BY universe"
        ).fetchall()]
        regimes = [r[0] for r in conn.execute(
            "SELECT DISTINCT final_regime FROM bars WHERE final_regime IS NOT NULL ORDER BY final_regime"
        ).fetchall()]
        date_range = conn.execute(
            "SELECT MIN(date), MAX(date) FROM bars"
        ).fetchone()
        return {
            "universes":  universes,
            "regimes":    regimes,
            "date_min":   str(date_range[0]) if date_range[0] else None,
            "date_max":   str(date_range[1]) if date_range[1] else None,
            "sort_metrics": SORT_METRICS,
        }
    finally:
        conn.close()
