"""
qlib_lab/train.py — time-ordered train / valid / test, fit, evaluate.

Splits are by DATE (never shuffled): the model only ever trains on bars that
occurred before the validation/test bars, so the IC we report is genuinely
out-of-sample.
"""

from __future__ import annotations

import logging
import os

import numpy as np
import pandas as pd

from .data import load_dataset, build_dataset, cache_path
from .models import make_model
from .metrics import compute_ic, quantile_spread

log = logging.getLogger(__name__)

# Default split: train 2021-2024, validate 2025, test 2026 (the prompt's split).
DEFAULT_SPLITS = {
    "train": ["2021-01-01", "2024-12-31"],
    "valid": ["2025-01-01", "2025-12-31"],
    "test":  ["2026-01-01", "2099-12-31"],
}

SCATTER_MAX = 2500


def _slice(df: pd.DataFrame, bounds) -> pd.DataFrame:
    lo, hi = pd.Timestamp(bounds[0]), pd.Timestamp(bounds[1])
    return df[(df["date"] >= lo) & (df["date"] <= hi)]


def run_training(
    universe: str,
    features: list[str],
    model: str = "lightgbm",
    splits: dict | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    min_bars: int = 250,
    horizon: int = 1,
    lookback: int = 0,
    log_fn=None,
) -> dict:
    def _log(m):
        log.info(m)
        if log_fn:
            log_fn(m)

    splits = splits or DEFAULT_SPLITS
    for k in ("train", "valid", "test"):
        if k not in splits:
            raise ValueError(f"splits missing '{k}'")

    # Use the cached parquet if build already ran for this exact slice, else load.
    df = None
    try:
        cp = cache_path(universe, features, date_from, date_to, min_bars, horizon, lookback)
        if os.path.exists(cp):
            df = pd.read_parquet(cp)
            df["date"] = pd.to_datetime(df["date"])
            # cache might have been built with a subset of usable features
            feats_in_cache = [c for c in df.columns if c not in ("ticker", "date", "label")]
            meta = {"features_used": feats_in_cache, "rows": len(df),
                    "tickers": int(df["ticker"].nunique()),
                    "categorical_features": [c for c in feats_in_cache
                                             if str(df[c].dtype).startswith(("object", "category"))],
                    "source": "cache"}
            _log(f"using cached dataset {cp} ({len(df):,} rows)")
    except Exception as e:  # noqa: BLE001 — cache is best-effort
        _log(f"cache miss/ignored ({e}); loading fresh")
        df = None

    if df is None:
        df, meta = load_dataset(universe, features, date_from, date_to, min_bars, horizon, lookback, log_fn=log_fn)
        meta["source"] = "fresh"

    feats = meta["features_used"]
    categorical = meta.get("categorical_features") or "auto"

    tr, va, te = _slice(df, splits["train"]), _slice(df, splits["valid"]), _slice(df, splits["test"])
    _log(f"split sizes — train={len(tr):,}  valid={len(va):,}  test={len(te):,}")
    if len(tr) < 500:
        raise ValueError(f"train split too small ({len(tr)} rows) — widen the date range")
    if len(te) < 50:
        raise ValueError(f"test split too small ({len(te)} rows) — adjust the split dates")

    mdl = make_model(model)
    _log(f"fitting {mdl.label} on {len(feats)} features …")
    mdl.fit(tr, va if len(va) else None, feats, categorical=categorical)

    preds = mdl.predict(te)
    _log("scoring test set …")
    ic = compute_ic(te["date"].values, preds, te["label"].values)
    qs = quantile_spread(preds, te["label"].values)
    importance = mdl.feature_importance()

    # scatter sample (pred vs realised return) — downsampled for the browser
    n = len(te)
    idx = np.arange(n)
    if n > SCATTER_MAX:
        idx = np.random.RandomState(0).choice(n, SCATTER_MAX, replace=False)
    scatter = [
        {"pred": round(float(preds[i]), 6), "actual": round(float(te["label"].values[i]), 6)}
        for i in idx
    ]

    _log(f"done — IC={ic['ic']}  RankIC={ic['rank_ic']}  ICIR={ic['icir']}  over {ic['n_days']} test days")

    return {
        "universe": universe,
        "model": mdl.name,
        "model_label": mdl.label,
        "horizon": horizon,
        "lookback": lookback,
        "n_base_features": len(meta.get("base_features", feats)),
        "best_iteration": getattr(mdl, "best_iteration", None),
        "features_used": feats,
        "n_features": len(feats),
        "splits": splits,
        "split_sizes": {"train": int(len(tr)), "valid": int(len(va)), "test": int(len(te))},
        "metrics": ic,
        "quantile_spread": qs,
        "feature_importance": [{"feature": k, "importance": round(v, 5)} for k, v in importance.items()],
        "scatter": scatter,
        "label_def": (f"close[T+{1+horizon}]/close[T+1]-1  (enter next-bar close, hold "
                      f"{horizon} bar{'s' if horizon != 1 else ''})"),
        "data_source_note": (
            "The DuckDB price feed differs slightly from TradingView, so treat "
            "absolute returns as indicative — IC / Rank IC (relative ranking) are "
            "the trustworthy signals here."
        ),
    }
