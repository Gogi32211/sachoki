"""Pivot Sequence + Suffix Analytics — what appears AT confirmed pivot bars.

For every bar where `swing_type` is HL/LL/HH/LH, analyses 6 views:
  1. Suffix (`full_suffix`)
  2. Body/Wick class (`bar_body_wick`, line3)
  3. Line5 token (`bar_line5`, VIX/PSAR/RSI2)
  4. PREUP/PREDN (`preup_signal` / `predn_signal`)
  5. 2-bar sequence (previous bar core | this pivot core)
  6. Composite + suffix combo (`composite_core` + `full_suffix`)

All forward metrics use `fwd_swing_ret` (pivot → next opposite pivot).
**RESEARCH_ONLY** — `fwd_swing_ret` requires future bars.

Win-rate convention:
  HL / LL  (pivot LOWs)  → fwd_swing_ret > 0 = win
  HH / LH  (pivot HIGHs) → fwd_swing_ret < 0 = win

Empirical highlights (SP500 1D, 165K rows):
  Best suffix at pivot LOW: EBA (+8.32%/95.8%), ED (+7.79%/98.3%)
  Best body/wick at pivot LOW: XF (+7.89%), MBB (+7.81% — pin bar)
  Best line5 at pivot LOW: VX-PS-R2X (+7.87%, VIX spike + RSI2 reclaim)
  Best composite+suffix at pivot LOW: T12L46+ED (+10.68%, 98.8%, n=172)
  Best 2-bar seq at pivot LOW: Z3L46|Z2GL46 (+10.19%, 100%, n=60)
"""
from __future__ import annotations
from typing import Dict, Tuple
import pandas as pd
import numpy as np


# ──────────────────────────────────────────────────────────────────────────
# Generic aggregator
# ──────────────────────────────────────────────────────────────────────────

def _agg(
    df: pd.DataFrame,
    group_col: str,
    fwd_col: str = "fwd",
    min_n: int = 30,
    ascending_med: bool = False,
) -> pd.DataFrame:
    """Aggregate `fwd_col` by `group_col` with direction-aware win_rate.

    ascending_med=False → pivot LOW context (best fwd > 0; sort descending by median)
    ascending_med=True  → pivot HIGH context (best fwd < 0; sort ascending  by median)

    Schema:
        <group_col>, count, avg_fwd, med_fwd, pct25, pct75, win_rate
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=[group_col, "count", "avg_fwd",
                                     "med_fwd", "pct25", "pct75", "win_rate"])

    is_low = (ascending_med is False)
    if is_low:
        win_fn = lambda x: (x > 0).mean() * 100.0
    else:
        win_fn = lambda x: (x < 0).mean() * 100.0

    agg = (
        df.groupby(group_col)[fwd_col]
        .agg(
            count="count",
            avg_fwd="mean",
            med_fwd="median",
            pct25=lambda x: x.quantile(0.25),
            pct75=lambda x: x.quantile(0.75),
        )
        .reset_index()
    )

    win_series = (
        df.groupby(group_col)[fwd_col].apply(win_fn).reset_index()
    )
    win_series.columns = [group_col, "win_rate"]
    agg = agg.merge(win_series, on=group_col)
    agg = agg[agg["count"] >= min_n].copy()
    agg = agg.sort_values("med_fwd", ascending=ascending_med).reset_index(drop=True)
    # Round metrics for stable downstream CSVs
    for c in ("avg_fwd", "med_fwd", "pct25", "pct75", "win_rate"):
        if c in agg.columns:
            agg[c] = agg[c].astype(float).round(4)
    return agg


def _pivot_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to pivot bars with non-null fwd_swing_ret."""
    if df is None or len(df) == 0:
        return df
    out = df[(df["swing_type"].astype(str) != "") &
             df["fwd_swing_ret"].notna()].copy()
    out["fwd"] = pd.to_numeric(out["fwd_swing_ret"], errors="coerce")
    return out


# ──────────────────────────────────────────────────────────────────────────
# 1. Suffix
# ──────────────────────────────────────────────────────────────────────────

def compute_suffix_perf(df: pd.DataFrame, min_n: int = 30
                        ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    p = _pivot_frame(df)
    if len(p) == 0:
        empty = pd.DataFrame()
        return empty, empty
    p["suf"] = p["full_suffix"].fillna("") if "full_suffix" in p.columns else ""
    low  = p[p["swing_type"].isin(["HL", "LL"])]
    high = p[p["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "suf", "fwd", min_n, ascending_med=False),
        _agg(high, "suf", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# 2. Body / Wick (line3)
# ──────────────────────────────────────────────────────────────────────────

def compute_body_wick_perf(df: pd.DataFrame, min_n: int = 50
                           ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    p = _pivot_frame(df)
    if len(p) == 0:
        empty = pd.DataFrame()
        return empty, empty
    p["bw"] = p["bar_body_wick"].fillna("") if "bar_body_wick" in p.columns else ""
    low  = p[p["swing_type"].isin(["HL", "LL"])]
    high = p[p["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "bw", "fwd", min_n, ascending_med=False),
        _agg(high, "bw", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# 3. Line5 (VIX-Fix / PSAR / RSI2)
# ──────────────────────────────────────────────────────────────────────────

def compute_line5_perf(df: pd.DataFrame, min_n: int = 50
                       ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    p = _pivot_frame(df)
    if len(p) == 0:
        empty = pd.DataFrame()
        return empty, empty
    p["l5"] = p["bar_line5"].fillna("") if "bar_line5" in p.columns else ""
    low  = p[p["swing_type"].isin(["HL", "LL"])]
    high = p[p["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "l5", "fwd", min_n, ascending_med=False),
        _agg(high, "l5", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# 4. PREUP at pivot LOW / PREDN at pivot HIGH
# ──────────────────────────────────────────────────────────────────────────

def compute_preup_predn_perf(df: pd.DataFrame, min_n: int = 20
                             ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    p = _pivot_frame(df)
    if len(p) == 0:
        empty = pd.DataFrame()
        return empty, empty
    p["preup"] = p["preup_signal"].fillna("") if "preup_signal" in p.columns else ""
    p["predn"] = p["predn_signal"].fillna("") if "predn_signal" in p.columns else ""
    low  = p[p["swing_type"].isin(["HL", "LL"])]
    high = p[p["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "preup", "fwd", min_n, ascending_med=False),
        _agg(high, "predn", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# 5. 2-bar sequence (prev_core | cur_core)
# ──────────────────────────────────────────────────────────────────────────

def compute_sequence2_perf(df: pd.DataFrame, min_n: int = 30
                           ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """`prev_core|cur_core` where core = composite_core (signal + L digits)."""
    if df is None or len(df) == 0 or "swing_type" not in df.columns:
        empty = pd.DataFrame()
        return empty, empty

    df2 = df.copy()
    # Ensure ordering for the per-ticker shift
    sort_cols = ["ticker"]
    if "date" in df2.columns:
        sort_cols.append("date")
    df2 = df2.sort_values(sort_cols).reset_index(drop=True)

    core_col = "composite_core" if "composite_core" in df2.columns else None
    if core_col is None:
        # Fall back to T/Z + L concatenation
        t = df2.get("t_signal", "").fillna("") if "t_signal" in df2.columns else ""
        z = df2.get("z_signal", "").fillna("") if "z_signal" in df2.columns else ""
        l = df2.get("l_signal", "").fillna("") if "l_signal" in df2.columns else ""
        df2["__core"] = (t.astype(str) + z.astype(str) + l.astype(str)).replace("", "?")
        core_col = "__core"

    df2["prev_core"] = df2.groupby("ticker")[core_col].shift(1).fillna("?").astype(str)
    df2["cur_core"]  = df2[core_col].fillna("?").astype(str)
    df2["seq2"]      = df2["prev_core"] + "|" + df2["cur_core"]
    df2["fwd"]       = pd.to_numeric(df2["fwd_swing_ret"], errors="coerce")

    pivot_rows = df2[(df2["swing_type"].astype(str) != "") & df2["fwd"].notna()]
    low  = pivot_rows[pivot_rows["swing_type"].isin(["HL", "LL"])]
    high = pivot_rows[pivot_rows["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "seq2", "fwd", min_n, ascending_med=False),
        _agg(high, "seq2", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# 6. Composite core + suffix combo  (e.g. "T12L46+ED")
# ──────────────────────────────────────────────────────────────────────────

def compute_composite_suffix_perf(df: pd.DataFrame, min_n: int = 30
                                  ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    p = _pivot_frame(df)
    if len(p) == 0:
        empty = pd.DataFrame()
        return empty, empty
    p["core"]     = p["composite_core"].fillna("?") if "composite_core" in p.columns else "?"
    p["suf"]      = p["full_suffix"].fillna("")     if "full_suffix" in p.columns     else ""
    p["core_suf"] = p["core"].astype(str) + "+" + p["suf"].astype(str)
    low  = p[p["swing_type"].isin(["HL", "LL"])]
    high = p[p["swing_type"].isin(["HH", "LH"])]
    return (
        _agg(low,  "core_suf", "fwd", min_n, ascending_med=False),
        _agg(high, "core_suf", "fwd", min_n, ascending_med=True),
    )


# ──────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────

def _tag_side(df: pd.DataFrame, side: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    df = df.copy()
    df["pivot_side"] = side
    return df


def run_pivot_sequence_analytics(
    df: pd.DataFrame,
    min_n_seq:     int = 30,
    min_n_suffix:  int = 30,
    min_n_bw:      int = 50,
    min_n_l5:      int = 50,
    min_n_preup:   int = 20,
) -> Dict[str, pd.DataFrame]:
    """Compute all six pivot-bar analytics views.

    Returns a dict keyed by canonical output filename.
    Every output DataFrame has a `pivot_side` column with values 'low' / 'high'.
    """
    results: Dict[str, pd.DataFrame] = {}

    # 1. Suffix
    suf_low, suf_hi = compute_suffix_perf(df, min_n_suffix)
    results["replay_tz_wlnbb_pivot_suffix_perf.csv"] = pd.concat(
        [_tag_side(suf_low, "low"), _tag_side(suf_hi, "high")], ignore_index=True,
    )

    # 2. Body / Wick
    bw_low, bw_hi = compute_body_wick_perf(df, min_n_bw)
    results["replay_tz_wlnbb_pivot_body_wick_perf.csv"] = pd.concat(
        [_tag_side(bw_low, "low"), _tag_side(bw_hi, "high")], ignore_index=True,
    )

    # 3. Line5
    l5_low, l5_hi = compute_line5_perf(df, min_n_l5)
    results["replay_tz_wlnbb_pivot_line5_perf.csv"] = pd.concat(
        [_tag_side(l5_low, "low"), _tag_side(l5_hi, "high")], ignore_index=True,
    )

    # 4. PREUP / PREDN
    pu_low, pd_hi = compute_preup_predn_perf(df, min_n_preup)
    pu_low_tagged = _tag_side(pu_low, "low")
    pd_hi_tagged  = _tag_side(pd_hi,  "high")
    # Different group_col names (preup vs predn) — normalise into a single
    # `signal` column for a unified output
    if pu_low_tagged is not None and len(pu_low_tagged):
        pu_low_tagged = pu_low_tagged.rename(columns={"preup": "signal"})
    if pd_hi_tagged is not None and len(pd_hi_tagged):
        pd_hi_tagged = pd_hi_tagged.rename(columns={"predn": "signal"})
    results["replay_tz_wlnbb_pivot_preup_predn_perf.csv"] = pd.concat(
        [pu_low_tagged, pd_hi_tagged], ignore_index=True,
    )

    # 5. 2-bar sequences
    seq2_low, seq2_hi = compute_sequence2_perf(df, min_n_seq)
    results["replay_tz_wlnbb_pivot_seq2_perf.csv"] = pd.concat(
        [_tag_side(seq2_low, "low"), _tag_side(seq2_hi, "high")], ignore_index=True,
    )

    # 6. Composite + suffix
    cs_low, cs_hi = compute_composite_suffix_perf(df, min_n_seq)
    results["replay_tz_wlnbb_pivot_composite_suffix_perf.csv"] = pd.concat(
        [_tag_side(cs_low, "low"), _tag_side(cs_hi, "high")], ignore_index=True,
    )

    return results
