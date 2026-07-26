"""
qlib_lab/search.py — automatic feature-combination search.

Two complementary searches, both scored on the VALIDATION split so the TEST split
stays a true holdout (reported once, for the final chosen set):

  1. Leaderboard — train each candidate feature ALONE, rank by validation Rank IC.
     Answers "which signals individually predict the next move?".
  2. Greedy forward selection — start empty; at each step add the feature that most
     improves validation Rank IC; stop when nothing helps (or max_features hit).
     Finds a good subset without the combinatorial blow-up / overfit of trying
     every subset.

Ranking uses fast native LightGBM (short boosting) regardless of the chosen model;
the final honest TEST evaluation of the greedy-chosen set uses the requested model.

WHY validation, not test: searching over many combinations and picking the best by
TEST score overfits the holdout — the winner looks good partly by luck. Selecting on
validation and reporting TEST once keeps the TEST number honest.
"""

from __future__ import annotations

import logging

import numpy as np

from .data import load_dataset
from .train import _slice, DEFAULT_SPLITS
from .metrics import compute_ic
from .models import make_model

log = logging.getLogger(__name__)

SEARCH_BOOST = 150          # short boosting for fast ranking fits
GREEDY_POOL_CAP = 25        # greedy only considers the top-N leaderboard features
MIN_IMPROVEMENT = 1e-4      # stop greedy when valid Rank IC gain falls below this


def _quick_fit_valid(tr, va, cols, categorical):
    """Fast native-LightGBM fit on `cols`, scored on the validation slice.
    Returns (valid_rank_ic, valid_ic)."""
    import lightgbm as lgb

    cat = [c for c in (categorical or []) if c in cols] or "auto"
    dtr = lgb.Dataset(tr[cols], label=tr["label"], categorical_feature=cat)
    params = {
        "objective": "regression", "metric": "l2", "num_leaves": 31,
        "learning_rate": 0.05, "feature_fraction": 0.8, "bagging_fraction": 0.8,
        "bagging_freq": 5, "min_data_in_leaf": 200, "verbose": -1, "num_threads": 0,
    }
    booster = lgb.train(params, dtr, num_boost_round=SEARCH_BOOST,
                        callbacks=[lgb.log_evaluation(0)])
    pred = booster.predict(va[cols])
    ic = compute_ic(va["date"].values, pred, va["label"].values)
    return ic["rank_ic"], ic["ic"]


def _r(v):
    return None if v is None else round(float(v), 4)


def run_search(
    universe: str,
    features: list[str],
    model: str = "lightgbm",
    splits: dict | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    min_bars: int = 250,
    horizon: int = 1,
    lookback: int = 0,
    max_features: int = 6,
    log_fn=None,
) -> dict:
    def _log(m):
        log.info(m)
        if log_fn:
            log_fn(m)

    splits = splits or DEFAULT_SPLITS
    max_features = max(1, min(int(max_features), 10))

    df, meta = load_dataset(universe, features, date_from, date_to, min_bars, horizon, lookback, log_fn=log_fn)
    pool = meta["features_used"]                 # base + lag columns
    categorical = meta.get("categorical_features") or []
    tr, va, te = _slice(df, splits["train"]), _slice(df, splits["valid"]), _slice(df, splits["test"])
    if len(tr) < 500 or len(va) < 50:
        raise ValueError("train/valid split too small for search — widen the date range")

    _log(f"search pool: {len(pool)} feature column(s); ranking on validation "
         f"({len(va):,} rows), holdout test = {len(te):,} rows")

    # ── 1) single-feature leaderboard (validation) ───────────────────────────
    leaderboard = []
    for i, c in enumerate(pool, 1):
        ric, ic = _quick_fit_valid(tr, va, [c], categorical)
        leaderboard.append({"feature": c, "valid_rank_ic": _r(ric), "valid_ic": _r(ic)})
        if i % 10 == 0 or i == len(pool):
            _log(f"leaderboard {i}/{len(pool)} features scored")
    leaderboard.sort(key=lambda d: (d["valid_rank_ic"] if d["valid_rank_ic"] is not None else -9), reverse=True)
    _log(f"top single feature: {leaderboard[0]['feature']} "
         f"(valid RankIC {leaderboard[0]['valid_rank_ic']})")

    # ── 2) greedy forward selection (validation Rank IC) ─────────────────────
    greedy_candidates = [d["feature"] for d in leaderboard[:GREEDY_POOL_CAP]]
    chosen: list[str] = []
    path = []
    best = -np.inf
    remaining = list(greedy_candidates)
    for step in range(1, max_features + 1):
        scored = []
        for c in remaining:
            ric, _ = _quick_fit_valid(tr, va, chosen + [c], categorical)
            scored.append((ric if ric is not None else -9, c))
        scored.sort(reverse=True, key=lambda t: t[0])
        gain_ric, c = scored[0]
        if gain_ric <= best + MIN_IMPROVEMENT:
            _log(f"greedy: no feature improves valid RankIC beyond {round(best,4)} — stop at {len(chosen)}")
            break
        best = gain_ric
        chosen.append(c)
        remaining.remove(c)
        path.append({"step": step, "added": c, "valid_rank_ic": _r(gain_ric), "features": list(chosen)})
        _log(f"greedy step {step}: +{c} → valid RankIC {round(gain_ric,4)} ({len(chosen)} features)")
        if not remaining:
            break

    if not chosen:
        chosen = [leaderboard[0]["feature"]]

    # ── 3) honest TEST evaluation of the greedy-chosen set (requested model) ──
    _log(f"final holdout TEST eval on {len(chosen)} chosen feature(s) with {model} …")
    mdl = make_model(model)
    mdl.fit(tr, va if len(va) else None, chosen, categorical=[c for c in categorical if c in chosen])
    pred = mdl.predict(te)
    test_ic = compute_ic(te["date"].values, pred, te["label"].values)
    importance = mdl.feature_importance()
    _log(f"DONE — chosen {len(chosen)} feats, TEST IC={test_ic['ic']} RankIC={test_ic['rank_ic']} ICIR={test_ic['icir']}")

    return {
        "universe": universe,
        "model": model,
        "horizon": horizon,
        "lookback": lookback,
        "pool_size": len(pool),
        "ranking_model": "lightgbm (fast, validation-ranked)",
        "leaderboard": leaderboard[:30],
        "greedy_path": path,
        "chosen": chosen,
        "final_test": {
            "n_features": len(chosen),
            "metrics": test_ic,
            "feature_importance": [{"feature": k, "importance": round(v, 5)} for k, v in importance.items()],
        },
        "note": ("Combinations were ranked on the VALIDATION split; the final_test "
                 "numbers are the untouched holdout for the greedy-chosen set — the "
                 "only honest out-of-sample read here. Searching inflates in-sample "
                 "scores, so trust final_test, not the leaderboard magnitudes."),
    }
