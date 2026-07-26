"""
overfit_stats.py — formal backtest-overfitting statistics for the Edge setups.

Implements (pure numpy/scipy, no pypbo dependency — it pins statsmodels 0.8):
  · PSR  — Probabilistic Sharpe Ratio  (Bailey & López de Prado 2012)
  · DSR  — Deflated Sharpe Ratio       (Bailey & López de Prado 2014):
           PSR evaluated against SR*, the expected max SR of N random trials
           (the "false strategy theorem" benchmark).
  · PBO  — Probability of Backtest Overfitting via CSCV
           (Bailey, Borwein, López de Prado & Zhu 2015): split the T×N
           period-returns matrix into S blocks, for every C(S, S/2) IS/OOS
           combination find the IS-best strategy and its OOS relative rank;
           PBO = share of combinations where the IS winner is below-median OOS.

These formalize what we already do by hand (per-year, TRAIN/TEST, '22-survival):
DSR answers "is this SR real GIVEN we tried ~N variants", PBO answers "does
in-sample selection transfer out-of-sample AT ALL for this family".
"""
from __future__ import annotations
from itertools import combinations
import numpy as np
from scipy.stats import norm, skew, kurtosis

EULER_GAMMA = 0.5772156649015329


def sharpe(rets: np.ndarray) -> float:
    """Plain (non-annualized) per-period Sharpe of a return series."""
    r = np.asarray(rets, float)
    if len(r) < 2 or r.std(ddof=1) == 0:
        return 0.0
    return float(r.mean() / r.std(ddof=1))


def psr(rets: np.ndarray, sr_benchmark: float = 0.0) -> float:
    """Probabilistic Sharpe Ratio: P(true SR > sr_benchmark), adjusted for
    the skew/kurtosis of the return distribution (fat tails widen the CI)."""
    r = np.asarray(rets, float)
    n = len(r)
    if n < 3:
        return 0.5
    sr = sharpe(r)
    g3 = skew(r)                      # sample skewness
    g4 = kurtosis(r, fisher=False)    # PEARSON kurtosis (normal = 3)
    denom = 1.0 - g3 * sr + (g4 - 1.0) / 4.0 * sr ** 2
    if denom <= 0:                    # extreme-tail degenerate case
        denom = 1e-9
    z = (sr - sr_benchmark) * np.sqrt(n - 1) / np.sqrt(denom)
    return float(norm.cdf(z))


def expected_max_sr(n_trials: int, var_trial_sr: float) -> float:
    """SR* of the false-strategy theorem: the expected MAX Sharpe among
    n_trials strategies whose true SR is 0, given the cross-trial SR variance."""
    if n_trials <= 1 or var_trial_sr <= 0:
        return 0.0
    n = float(n_trials)
    z1 = norm.ppf(1.0 - 1.0 / n)
    z2 = norm.ppf(1.0 - 1.0 / (n * np.e))
    return float(np.sqrt(var_trial_sr) * ((1.0 - EULER_GAMMA) * z1 + EULER_GAMMA * z2))


def dsr(rets: np.ndarray, trial_srs: "list[float]", n_trials: int = None) -> dict:
    """Deflated Sharpe Ratio: PSR of `rets` against SR* derived from the family
    of tried variants. `trial_srs` = per-period Sharpes of every variant tested
    (the setups family); `n_trials` can override N upward to be honest about
    variants tried-and-discarded that never made it into the family."""
    srs = np.asarray(trial_srs, float)
    N = int(n_trials or len(srs))
    var_sr = float(srs.var(ddof=1)) if len(srs) > 1 else 0.0
    sr_star = expected_max_sr(N, var_sr)
    return {"sr": round(sharpe(rets), 4), "sr_star": round(sr_star, 4),
            "n_trials": N, "dsr": round(psr(rets, sr_star), 4)}


def pbo_cscv(M: np.ndarray, S: int = 8, metric=sharpe) -> dict:
    """CSCV Probability of Backtest Overfitting.
    M: T×N matrix — T periods (rows, e.g. months) × N strategies (columns).
    S: number of blocks (even). Uses all C(S, S/2) IS/OOS splits.
    Returns PBO plus the logit distribution and IS→OOS rank-degradation stats."""
    M = np.asarray(M, float)
    T, N = M.shape
    if T < S or N < 2:
        return {"pbo": None, "n_splits": 0}
    blocks = np.array_split(np.arange(T), S)
    half = S // 2
    logits, pairs = [], []
    for c in combinations(range(S), half):
        is_idx = np.concatenate([blocks[i] for i in c])
        oos_idx = np.concatenate([blocks[i] for i in range(S) if i not in c])
        perf_is = np.array([metric(M[is_idx, j]) for j in range(N)])
        perf_oos = np.array([metric(M[oos_idx, j]) for j in range(N)])
        j_star = int(np.argmax(perf_is))
        # relative rank of the IS winner within the OOS distribution, ω ∈ (0,1)
        omega = (np.sum(perf_oos < perf_oos[j_star]) + 0.5) / N
        omega = min(max(omega, 1e-6), 1 - 1e-6)
        logits.append(np.log(omega / (1.0 - omega)))
        pairs.append((float(perf_is[j_star]), float(perf_oos[j_star])))
    logits = np.asarray(logits)
    is_v = np.array([p[0] for p in pairs]); oos_v = np.array([p[1] for p in pairs])
    return {
        "pbo": round(float(np.mean(logits <= 0)), 3),
        "n_splits": len(logits),
        "logit_mean": round(float(logits.mean()), 3),
        "is_best_sr_mean": round(float(is_v.mean()), 3),
        "oos_of_is_best_sr_mean": round(float(oos_v.mean()), 3),
        # slope<1 with intercept≈0 ⇒ OOS retains that share of IS performance
        "oos_is_ratio": round(float(oos_v.mean() / is_v.mean()), 3) if is_v.mean() != 0 else None,
    }


if __name__ == "__main__":
    # sanity: pure-noise family → DSR near 0.5-, PBO ≈ 0.5 ; real edge → DSR high
    rng = np.random.default_rng(7)
    noise = rng.normal(0, 0.05, size=(120, 20))          # 120 months × 20 fake setups
    fam = [sharpe(noise[:, j]) for j in range(20)]
    best = int(np.argmax(fam))
    print("noise family:  best trial DSR =", dsr(noise[:, best], fam)["dsr"],
          "| PBO =", pbo_cscv(noise)["pbo"])
    edge = rng.normal(0.02, 0.05, size=120)              # true positive-mean strategy
    print("real edge:     DSR =", dsr(edge, fam + [sharpe(edge)])["dsr"])
