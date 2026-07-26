"""
qlib_lab/data.py — build a feature+label matrix from the Studio DuckDB.

The label is the qlib-style next-bar forward return:

    label[T] = close[T+2] / close[T+1] - 1        (qlib: Ref($close,-2)/Ref($close,-1)-1)

i.e. the signal is seen at the close of bar T, the trade is entered at the
*next* bar's close (T+1) and held one bar to T+2. This is point-in-time safe:
features use only data up to bar T; the label uses only future prices. We never
use bar T's own close as the entry price (that would be a fill you couldn't get)
and we never feed a forward-return column as a feature.
"""

from __future__ import annotations

import hashlib
import logging
import os

import numpy as np
import pandas as pd

from studio.db import get_conn
from .columns import validate_features, FORBIDDEN_RE

log = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".sachoki", "qlib_lab")

VALID_UNIVERSES = ("sp500", "nasdaq", "russell2k")


def _cache_key(universe: str, features: list[str], date_from, date_to, min_bars: int, horizon: int, lookback: int) -> str:
    raw = "|".join([universe, ",".join(sorted(features)), str(date_from), str(date_to),
                    str(min_bars), f"h{horizon}", f"lb{lookback}"])
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def cache_path(universe: str, features: list[str], date_from, date_to, min_bars: int, horizon: int = 1, lookback: int = 0) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{universe}_{_cache_key(universe, features, date_from, date_to, min_bars, horizon, lookback)}.parquet")


def load_dataset(
    universe: str,
    features: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    min_bars: int = 250,
    horizon: int = 1,
    lookback: int = 0,
    log_fn=None,
) -> tuple[pd.DataFrame, dict]:
    """Query `bars` read-only and return (df, meta).

    df columns: ['ticker', 'date', <features...>, 'label']  — label rows that
    cannot be computed (last 2 bars per ticker) are dropped.

    meta carries diagnostics for the UI (row/ticker counts, dropped tickers,
    features that were entirely null, categorical feature names).
    """
    def _log(m):
        log.info(m)
        if log_fn:
            log_fn(m)

    if universe not in VALID_UNIVERSES:
        raise ValueError(f"unknown universe {universe!r}; expected one of {VALID_UNIVERSES}")
    horizon = int(horizon)
    if horizon < 1:
        raise ValueError("horizon must be >= 1 bar")
    lookback = int(lookback)
    if lookback < 0 or lookback > 10:
        raise ValueError("lookback must be between 0 and 10 bars")
    features = validate_features(features)
    if not features:
        raise ValueError("no features selected")

    # Defensive: re-assert no forbidden column slipped through into the SQL.
    for f in features:
        if FORBIDDEN_RE.match(f):
            raise ValueError(f"refusing to read outcome column as feature: {f}")

    col_list = ", ".join(f'"{c}"' for c in features)
    where = ["universe = ?"]
    params: list = [universe]
    if date_from:
        where.append("date >= ?")
        params.append(date_from)
    if date_to:
        where.append("date <= ?")
        params.append(date_to)
    sql = (
        f"SELECT ticker, date, close, {col_list} "
        f"FROM bars WHERE {' AND '.join(where)} ORDER BY ticker, date"
    )
    _log(f"querying bars: universe={universe} features={len(features)} "
         f"range={date_from or 'min'}..{date_to or 'max'}")

    con = get_conn(read_only=True)
    try:
        df = con.execute(sql, params).fetchdf()
    finally:
        con.close()

    if df.empty:
        raise ValueError("query returned 0 rows — check universe/date range")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # ── label: forward return, enter T+1 close, hold `horizon` bars ──────────
    #   label[T] = close[T+1+horizon] / close[T+1] - 1   (qlib: horizon=1 →
    #   Ref($close,-2)/Ref($close,-1)-1). Entry is always the *next* bar, never
    #   the signal bar's own close — point-in-time safe for any horizon.
    g = df.groupby("ticker", sort=False)["close"]
    entry = g.shift(-1)            # close[T+1]
    exit_ = g.shift(-(1 + horizon))  # close[T+1+horizon]
    df["label"] = exit_ / entry - 1.0
    df.loc[entry <= 0, "label"] = np.nan   # guard against bad/zero prices

    # ── drop short tickers ───────────────────────────────────────────────────
    counts = df.groupby("ticker", sort=False).size()
    keep = counts[counts >= min_bars].index
    dropped = sorted(set(counts.index) - set(keep))
    if dropped:
        _log(f"dropping {len(dropped)} ticker(s) with < {min_bars} bars")
    df = df[df["ticker"].isin(keep)]

    # ── feature hygiene: drop features that are entirely null in this slice ──
    null_features = [c for c in features if df[c].isna().all()]
    usable = [c for c in features if c not in null_features]
    if null_features:
        _log(f"dropping {len(null_features)} all-null feature(s): {null_features}")
    if not usable:
        raise ValueError("all selected features are null in this slice")

    # ── categorical encoding (string signals -> pandas category codes) ───────
    categorical: list[str] = []
    for c in usable:
        if df[c].dtype == object or str(df[c].dtype).startswith("category"):
            df[c] = df[c].astype("category").cat.codes.replace(-1, np.nan)
            categorical.append(c)

    # ── lookback: add each feature's value from the previous k bars so the
    #    model sees the recent SEQUENCE/path, not just the signal bar. Backward
    #    shift per ticker on the contiguous series → point-in-time safe. Done
    #    BEFORE dropping the label-less tail so the shifts don't span a gap. ──
    feat_cols = list(usable)
    lag_cols: list[str] = []
    if lookback > 0:
        gb = df.groupby("ticker", sort=False)
        for c in usable:
            for k in range(1, lookback + 1):
                name = f"{c}_lag{k}"
                df[name] = gb[c].shift(k)
                lag_cols.append(name)
                if c in categorical:
                    categorical.append(name)
        feat_cols += lag_cols
        _log(f"lookback={lookback}: added {len(lag_cols)} lag feature(s) "
             f"({len(usable)} base × {lookback} prior bars)")

    # rows without a computable label (last 1+horizon bars / bad price) are useless
    n_before = len(df)
    df = df.dropna(subset=["label"]).reset_index(drop=True)
    _log(f"dropped {n_before - len(df)} rows without a forward label")

    out = df[["ticker", "date"] + feat_cols + ["label"]].copy()
    meta = {
        "universe": universe,
        "rows": int(len(out)),
        "tickers": int(out["ticker"].nunique()),
        "date_min": str(out["date"].min().date()),
        "date_max": str(out["date"].max().date()),
        "features_used": feat_cols,
        "base_features": usable,
        "features_dropped_null": null_features,
        "categorical_features": categorical,
        "tickers_dropped_short": dropped,
        "horizon": horizon,
        "lookback": lookback,
        "n_lag_features": len(lag_cols),
        "label": (f"close[T+{1+horizon}]/close[T+1]-1  (enter next-bar close, hold {horizon} bar"
                  f"{'s' if horizon != 1 else ''})"),
    }
    _log(f"dataset ready: {meta['rows']:,} rows × {len(feat_cols)} features "
         f"({len(usable)} base + {len(lag_cols)} lag), {meta['tickers']} tickers, "
         f"{meta['date_min']}..{meta['date_max']}")
    return out, meta


def build_dataset(
    universe: str,
    features: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    min_bars: int = 250,
    horizon: int = 1,
    lookback: int = 0,
    log_fn=None,
) -> dict:
    """Materialise the feature+label matrix to a parquet cache so `train` is fast.
    Returns meta + the cache path."""
    df, meta = load_dataset(universe, features, date_from, date_to, min_bars, horizon, lookback, log_fn=log_fn)
    path = cache_path(universe, features, date_from, date_to, min_bars, horizon, lookback)
    df.to_parquet(path, index=False)
    meta["cache_path"] = path
    meta["cache_mb"] = round(os.path.getsize(path) / 1e6, 2)
    if log_fn:
        log_fn(f"cached dataset -> {path} ({meta['cache_mb']} MB)")
    return meta
