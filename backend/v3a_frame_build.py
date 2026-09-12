"""v3a_frame_build.py — the WIDE SAFE frame for major-breakout discovery. 2026-09-12.

WHY A NEW FRAME. `breakout_frame_v1.parquet` has 97 columns that I chose for the B1-B4/R1 state
machines. All five failed. Reusing that frame would silently re-privilege the same narratives —
K_ACT, TURN, FLY, E2 — which is exactly what the V3-A brief forbids. This builds the frame from
the SAFE registry instead, with no hypothesis in the selection.

THE 2026 LOCKOUT IS PHYSICAL, NOT A PROMISE. `load(max_date=...)` drops every row beyond the
date and asserts it. Discovery and validation code call it with max_date="2025-12-31" and
therefore cannot see 2026 even by accident. The holdout is opened by a separate module that
verifies a frozen registry hash first.

FAMILY 18 IS NEW AND IS COMPUTED HERE. `pct_change_*`, `pct_from_20d_high/low`, `dist_20d_high`,
`vol_ratio_20d`, `dollar_volume`, `gap_pct`, `atr_pct`, distance-to-EMA — the block that the app's
own export path leaves permanently empty (CSV_SIGNAL_COLUMNS.md BLOCK 7). Trivially causal,
trivially computable, and the most direct candidates for an 8-ATR tail.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = os.path.join(DATA, "v3a_frame.parquet")
UNIVERSES = ("sp500", "nasdaq")
DATE_LO, DATE_HI = "2021-01-01", "2026-09-10"
LOCK_DATE = "2025-12-31"          # discovery + validation may never see beyond this

FORBIDDEN_PREFIX = ("fwd_", "mfe_", "mae_", "hit_", "drop_", "bars_to_", "pct_to_",
                    "ret_to_next_", "max_high_", "max_low_", "min_low_", "tt_up", "tt_dn",
                    "vbo_w", "gog_w", "rank_", "is_pivot", "next_pivot", "fwd_swing",
                    "swing_ret_from", "turbo_score", "ultra_score", "prebreak_v", "prebreak_score",
                    "prebreak_prime", "beta_", "aes_", "acc_exit")
FORBIDDEN_EXACT = {
    "swing_type", "swing_type_3", "swing_type_5", "seq34_win", "edge_gold",
    "final_bull_score", "final_regime", "buy_score", "gog_score", "profile_score",
    "profile_category", "rtb_total", "id", "enrich_version", "all_signals_text",
    # measured DEAD — never fire on 8.9M bars
    "sig_x1", "sig_x2", "sig_x1g", "sig_x3", "sig_l2l4", "sig_ns_delta", "sig_nd_delta",
    "wyc_sow", "already_extended_flag", "atr_brk",
}
KEEP_ALWAYS = ["ticker", "date", "universe", "sector", "open", "high", "low", "close",
               "volume", "avg_vol_20d", "atr_14"]


def _safe_columns(db_cols) -> list:
    out = []
    for c in db_cols:
        lc = c.lower()
        if lc in KEEP_ALWAYS:
            continue
        if lc in FORBIDDEN_EXACT or lc.startswith(FORBIDDEN_PREFIX):
            continue
        out.append(c)
    return out


def _price_derived(df: pd.DataFrame, bnd: np.ndarray) -> pd.DataFrame:
    """FAMILY 18 — the block the export path never fills. All strictly backward-looking."""
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float)
    lo = df["low"].to_numpy(float); o = df["open"].to_numpy(float)
    v = df["volume"].to_numpy(float); av = df["avg_vol_20d"].to_numpy(float)
    atr = df["atr_14"].to_numpy(float)
    n = len(df)
    out = {k: np.full(n, np.nan) for k in
           ("pct_change_1d", "pct_change_3d", "pct_change_5d", "pct_change_10d",
            "pct_from_20d_high", "pct_from_20d_low", "pct_from_60d_high",
            "dist_ema20_atr", "dist_ema50_atr", "dist_ema200_atr",
            "range_pct", "body_pct", "upper_wick_pct", "lower_wick_pct",
            "atr_pct", "atr_pct_chg_20d", "vol_ratio_20d", "dollar_volume",
            "gap_pct", "consec_up", "hi20_age", "close_pos_20d")}
    for a, b in zip(bnd[:-1], bnd[1:]):
        s = slice(a, b)
        cc = pd.Series(c[s]); hh = pd.Series(h[s]); ll = pd.Series(lo[s])
        for k, lag in (("pct_change_1d", 1), ("pct_change_3d", 3), ("pct_change_5d", 5),
                       ("pct_change_10d", 10)):
            out[k][s] = (cc / cc.shift(lag) - 1).to_numpy() * 100
        hi20 = hh.shift(1).rolling(20, min_periods=20).max()
        lo20 = ll.shift(1).rolling(20, min_periods=20).min()
        hi60 = hh.shift(1).rolling(60, min_periods=60).max()
        out["pct_from_20d_high"][s] = (cc / hi20 - 1).to_numpy() * 100
        out["pct_from_20d_low"][s] = (cc / lo20 - 1).to_numpy() * 100
        out["pct_from_60d_high"][s] = (cc / hi60 - 1).to_numpy() * 100
        out["close_pos_20d"][s] = ((cc - lo20) / (hi20 - lo20)).to_numpy()
        for k, span in (("dist_ema20_atr", 20), ("dist_ema50_atr", 50), ("dist_ema200_atr", 200)):
            e = cc.ewm(span=span, adjust=False).mean()
            out[k][s] = ((cc - e) / pd.Series(atr[s])).to_numpy()
        rng = hh - ll
        out["range_pct"][s] = (rng / cc).to_numpy() * 100
        out["body_pct"][s] = ((cc - pd.Series(o[s])).abs() / rng.replace(0, np.nan)).to_numpy() * 100
        top = np.maximum(o[s], c[s]); bot = np.minimum(o[s], c[s])
        out["upper_wick_pct"][s] = ((h[s] - top) / np.where(rng > 0, rng, np.nan)) * 100
        out["lower_wick_pct"][s] = ((bot - lo[s]) / np.where(rng > 0, rng, np.nan)) * 100
        ap = pd.Series(atr[s]) / cc * 100
        out["atr_pct"][s] = ap.to_numpy()
        out["atr_pct_chg_20d"][s] = (ap / ap.shift(20) - 1).to_numpy() * 100
        out["vol_ratio_20d"][s] = (v[s] / np.where(av[s] > 0, av[s], np.nan))
        out["dollar_volume"][s] = c[s] * v[s]
        out["gap_pct"][s] = (o[s] / cc.shift(1).to_numpy() - 1) * 100
        up = (cc > cc.shift(1)).to_numpy()
        run = np.zeros(b - a); k_ = 0
        for i in range(b - a):
            k_ = k_ + 1 if up[i] else 0
            run[i] = k_
        out["consec_up"][s] = run
        # bars since the 20-day high was last set
        newhi = (hh >= hh.shift(1).rolling(20, min_periods=20).max()).to_numpy()
        idx = np.arange(b - a)
        last = np.maximum.accumulate(np.where(newhi, idx, -1))
        out["hi20_age"][s] = np.where(last >= 0, idx - last, np.nan)
    return pd.DataFrame(out, index=df.index)


def build(log=print) -> pd.DataFrame:
    import duckdb
    from studio.db import tf_db_path
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    db_cols = [r[0] for r in con.execute("DESCRIBE bars").fetchall()]
    safe = _safe_columns(db_cols)
    log(f"  DB columns {len(db_cols)} → SAFE candidates {len(safe)}")
    cols = ", ".join(KEEP_ALWAYS[2:] + safe)      # ticker/date handled separately
    df = con.execute(f"""
        SELECT ticker, substr(CAST(date AS VARCHAR),1,10) AS date, {cols}
        FROM bars WHERE universe IN {UNIVERSES} AND date >= ? AND date <= ?
        QUALIFY row_number() OVER (PARTITION BY ticker, date ORDER BY
          CASE universe WHEN 'sp500' THEN 1 ELSE 2 END) = 1
        ORDER BY ticker, date""", [DATE_LO, DATE_HI]).fetchdf()
    con.close()
    log(f"  bars {len(df):,} × {df.shape[1]} · {df['ticker'].nunique():,} tickers")

    tk = df["ticker"].to_numpy()
    bnd = np.r_[np.flatnonzero(np.r_[True, tk[1:] != tk[:-1]]), len(tk)]
    pdv = _price_derived(df, bnd)
    df = pd.concat([df, pdv], axis=1)
    log(f"  + FAMILY 18 price-derived: {pdv.shape[1]} columns")

    for f, ren in (("lbal_signals", {"udn": "LBAL_UDN", "star": "LBAL_STAR"}),
                   ("lvx_signals", {"tier": "LVX_TIER"}),
                   ("vol7_signals", {"mr": "VOL7_MR", "sg": "VOL7_SG", "cons": "VOL7_CONS"}),
                   ("shapectx_signals", {"code": "SHAPE_CODE", "touches": "SHAPE_TOUCHES",
                                         "lstup_veto": "SHAPE_LSTUP_VETO", "grade": "SHAPE_GRADE"}),
                   ("ovdmap_signals", {"tokens": "OVD_TOKENS"})):
        p = os.path.join(DATA, f"{f}.parquet")
        if not os.path.exists(p):
            continue
        t = pd.read_parquet(p, columns=["ticker", "date"] + list(ren))
        t["date"] = t["date"].astype(str).str[:10]
        if f == "lvx_signals":
            t = t.sort_values("tier", ascending=False)
        t = t.drop_duplicates(subset=["ticker", "date"]).rename(columns=ren)
        before = len(df)
        df = df.merge(t, on=["ticker", "date"], how="left")
        assert len(df) == before
    log(f"  + display layers → {df.shape[1]} columns total")

    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    assert df.duplicated(subset=["ticker", "date"]).sum() == 0
    tmp = OUT + ".tmp"; df.to_parquet(tmp, index=False); os.replace(tmp, OUT)
    log(f"  wrote {OUT} · {len(df):,} × {df.shape[1]}")
    return df


def load(max_date: str | None = LOCK_DATE, log=print) -> pd.DataFrame:
    """THE LOCKOUT. Discovery and validation call this with the default and physically cannot
    receive a 2026 row. Opening the holdout requires v3a_holdout.py and a frozen registry hash."""
    df = pd.read_parquet(OUT)
    bad = [c for c in df.columns if c.lower() in FORBIDDEN_EXACT
           or c.lower().startswith(FORBIDDEN_PREFIX)]
    assert not bad, f"FORBIDDEN COLUMNS IN THE V3-A FRAME: {bad}"
    if max_date is not None:
        df = df[df["date"] <= max_date].reset_index(drop=True)
        assert df["date"].max() <= max_date, "LOCKOUT FAILED"
        log(f"  loaded {len(df):,} rows · LOCKED at {max_date} (max seen {df['date'].max()})")
    else:
        log(f"  loaded {len(df):,} rows · UNLOCKED to {df['date'].max()}")
    return df


if __name__ == "__main__":
    build()
