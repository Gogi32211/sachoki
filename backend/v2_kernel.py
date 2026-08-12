"""3B.1 — the computation, moved and not touched.

Two claims live in this extraction and they are not the same strength, so they are not made in
the same words:

    Frozen, support_hash, verdict    SOURCE-MECHANICAL EXTRACTION
                                     the computational lines and branches are unchanged; only
                                     the place of definition moved, and dependencies that were
                                     already read are now passed in.

    boot                             NOT IN THIS FILE. It keys its RNG on (world, delta, rep) —
                                     coordinates that exist only because the sealed experiment
                                     planted a needle. Moving it would carry the capability
                                     experiment's geometry into a kernel a historical run will
                                     also call. It gets its own commit and its own proof, which
                                     rests on an RNG and resampling trace rather than on the
                                     source being identical.

WHAT WAS FORBIDDEN WHILE WRITING THIS, and none of it happened: no dict became a set, nothing
gained a `sorted()` it did not have, no dtype moved, no loops merged, nothing was vectorised or
cached, no exception handling changed, and no RNG call order moved. `_lower_median` is still
reached through `E`, the median-by-weight branch still returns `np.nan` on an unrepresented arm
rather than raising, and `support_hash` still formats weights with `:.12f`. A better-looking
line here would invalidate the only claim this commit makes.

THE SEALED RUN BECOMES THE FIRST CLIENT rather than the extraction becoming a second engine.
`v2_decision_run` re-exports what moved, so the frozen core oracle is not edited at all — its
reproduction after this commit is therefore evidence that the move was invisible to the
computation, not evidence that the oracle was adjusted to match.
"""
from __future__ import annotations

import hashlib

import numpy as np

import combolab_v2 as E
from studio_verdict import Estimate, decide


def support_hash(sup: E.Support, cell: str) -> str:
    h = hashlib.sha256()
    for (s, i, j), w in zip(sup.strata[cell], sup.weights[cell]):
        h.update(f"{s}|{len(i)}|{len(j)}|{w:.12f}".encode())
    return h.hexdigest()


class Frozen:
    """Precomputed layout for one class, so the bootstrap only resamples dates."""

    def __init__(self, sup: E.Support, cell: str, gi: np.ndarray):
        self.cell, self.w = cell, sup.weights[cell]
        self.idx, self.seg, self.arm = [], [], []
        for k, (_, i, j) in enumerate(sup.strata[cell]):
            for arm, ix in ((0, i), (1, j)):
                self.idx.append(ix)
                self.seg.append(k)
                self.arm.append(arm)
        self.sizes = np.array([len(x) for x in self.idx])
        self.flat = np.concatenate(self.idx)
        self.gflat = gi[self.flat]
        self.bounds = np.r_[0, np.cumsum(self.sizes)]
        self.seg = np.asarray(self.seg)
        self.arm = np.asarray(self.arm)
        self.hash = support_hash(sup, cell)

    def theta(self, y: np.ndarray, w_date: np.ndarray | None = None) -> float:
        """Weighted within-stratum median differences, aggregated with FROZEN weights."""
        vals = y[self.flat]
        order = np.empty(len(vals), dtype=np.int64)
        med = np.empty(len(self.sizes))
        for k in range(len(self.sizes)):
            a, b = self.bounds[k], self.bounds[k + 1]
            v = vals[a:b]
            if w_date is None:
                med[k] = E._lower_median(v)
            else:
                ww = w_date[self.gflat[a:b]]
                o = np.argsort(v, kind="stable")
                c = np.cumsum(ww[o])
                if c[-1] <= 0:
                    return np.nan                  # arm unrepresented → replication INVALID
                med[k] = v[o[np.searchsorted(c, c[-1] / 2.0)]]
        d = med[self.arm == 0] - med[self.arm == 1]
        return float(self.w @ d)


def verdict(e, cell, meta, delta_star) -> str:
    """The decision, unchanged. `meta` and `delta_star` were closure reads and are now arguments.

    The inventory recorded this function's free names as exactly {DELTA_STAR, decide, meta} —
    no capability coordinate — which is what made it movable as it stands. `delta_star` is the
    materiality threshold a verdict is read against, and is not the planted δ; the two names are
    close enough that the distinction was asserted by a test rather than trusted to a reader.
    """
    return decide(branch="return", effect=e, delta_star=delta_star,
                  direction="positive",
                  support_ok=bool(meta.loc[cell, "support_fraction"] >= 0.50),
                  setups_ok=bool(meta.loc[cell, "eligible_setups"] >= 5)).status


def bootstrap_cell(y, f, cell, rng_stream, n_dates, n_boot, meta, estimand):
    """3B.2 · the same bootstrap, with the stream handed in instead of keyed here.

    ONE STREAM, CONSUMED IN ORDER. The old closure built a single generator from
    `(world, delta, cell, rep)` and drew from it `n_boot` times inside one comprehension. Keying
    a fresh generator per iteration would be tidier, more obviously reproducible, and a DIFFERENT
    RNG geometry — the sequence of draws would no longer come from one advancing state. That is
    outside this extraction's claim, so the interface takes a stream and consumes it exactly as
    before.

    What left: `world`, `delta`, `rep` — coordinates that exist only because a needle was
    planted. They now live in the provider that opened the stream, which is where the sealed
    experiment's geometry belongs. What did not leave: `y`, `meta`, `n_dates` and the estimand
    label, because removing those would mean changing computation structure rather than
    inverting a dependency.
    """
    p = np.full(n_dates, 1 / n_dates)
    d = np.array([f.theta(y, rng_stream.multinomial(n_dates, p).astype(float))
                  for _ in range(n_boot)])
    good = d[np.isfinite(d)]
    lo, hi = np.percentile(good, [2.5, 97.5])
    est = f.theta(y)
    return Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo),
                    ci_high=float(hi), level=0.95, estimand=estimand,
                    method="clustered bootstrap", cluster_unit="trading_date",
                    n_raw=int(meta.loc[cell, "eligible_cell_opportunities"]),
                    n_eff=int(meta.loc[cell, "eligible_dates"]))
