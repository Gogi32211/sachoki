"""
qlib_lab/metrics.py — IC / Rank IC / ICIR, the qlib way.

IC here is the *cross-sectional* information coefficient: on each date we
correlate the model's prediction against the realised next-bar return across all
tickers, then average those daily correlations over the test period. Rank IC is
the same with Spearman (rank) correlation. ICIR = mean(daily IC) / std(daily IC)
— a t-stat-like measure of how consistent the signal is day to day.

A positive, stable IC (say > 0.02 with ICIR > 0.3) means the features carry real
cross-sectional predictive power. IC near 0 means no edge — that's a valid,
honest result, not a bug.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr


def _safe(x):
    return None if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))) else float(x)


def compute_ic(dates, preds, labels, min_names_per_day: int = 5) -> dict:
    """Cross-sectional IC / Rank IC / ICIR plus per-day series for charting."""
    df = pd.DataFrame({"date": np.asarray(dates), "pred": np.asarray(preds, float),
                       "label": np.asarray(labels, float)}).dropna()
    daily_ic, daily_ric, day_index = [], [], []
    for d, grp in df.groupby("date", sort=True):
        if len(grp) < min_names_per_day or grp["pred"].nunique() < 2 or grp["label"].nunique() < 2:
            continue
        ic = pearsonr(grp["pred"], grp["label"])[0]
        ric = spearmanr(grp["pred"], grp["label"])[0]
        if np.isnan(ic) or np.isnan(ric):
            continue
        daily_ic.append(ic)
        daily_ric.append(ric)
        day_index.append(pd.Timestamp(d).strftime("%Y-%m-%d"))

    n_days = len(daily_ic)
    ic_arr = np.array(daily_ic) if n_days else np.array([])
    ric_arr = np.array(daily_ric) if n_days else np.array([])

    mean_ic = ic_arr.mean() if n_days else np.nan
    std_ic = ic_arr.std(ddof=1) if n_days > 1 else np.nan
    icir = (mean_ic / std_ic) if (n_days > 1 and std_ic not in (0, np.nan)) else np.nan

    # pooled fallback (thin cross-sections) — correlate everything at once
    pooled_ic = pearsonr(df["pred"], df["label"])[0] if df["pred"].nunique() > 1 else np.nan
    pooled_ric = spearmanr(df["pred"], df["label"])[0] if df["pred"].nunique() > 1 else np.nan

    return {
        "ic": _safe(mean_ic),
        "rank_ic": _safe(ric_arr.mean() if n_days else np.nan),
        "icir": _safe(icir),
        "ic_std": _safe(std_ic),
        "n_days": n_days,
        "avg_names_per_day": _safe(len(df) / n_days) if n_days else None,
        "pooled_ic": _safe(pooled_ic),
        "pooled_rank_ic": _safe(pooled_ric),
        "positive_day_pct": _safe((ic_arr > 0).mean() * 100) if n_days else None,
        # series for the UI (cumulative IC chart)
        "ic_series": {
            "dates": day_index,
            "ic": [round(float(v), 4) for v in ic_arr.tolist()],
        },
    }


def quantile_spread(preds, labels, n_q: int = 5) -> dict:
    """Mean realised return of the top vs bottom prediction quantile — an
    intuitive 'does ranking by the model actually sort returns?' check."""
    df = pd.DataFrame({"pred": np.asarray(preds, float), "label": np.asarray(labels, float)}).dropna()
    if len(df) < n_q * 10 or df["pred"].nunique() < n_q:
        return {"available": False}
    try:
        df["q"] = pd.qcut(df["pred"].rank(method="first"), n_q, labels=False)
    except ValueError:
        return {"available": False}
    means = df.groupby("q")["label"].mean()
    return {
        "available": True,
        "n_quantiles": n_q,
        "by_quantile": [round(float(means.get(i, np.nan)) * 100, 3) for i in range(n_q)],  # pct
        "top_minus_bottom_pct": round(float(means.iloc[-1] - means.iloc[0]) * 100, 3),
    }
