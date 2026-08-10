"""ComboLab — the search layer, built against an acceptance test it has never seen.

`combolab_spec.py` was frozen and committed first: 46 claims derived from a manifest, the
exposure metric, the delta grid, three definitions of "found", and acceptance seeds that do not
exist until this file's freeze commit hash does. Nothing here may widen that space. If the
search ever produces a 47th selectable result, `assert_search_space()` raises rather than
letting k drift from 46 to whatever was actually tried.

Two implementation facts are worth stating because neither is arbitrary.

MULTIPLICITY IS THE PERMUTATION BAND, not a separate correction bolted on afterwards. The null
distribution of the BEST cell across all 46 is computed directly, so the threshold a claim must
clear already accounts for having looked 46 times. A Bonferroni or BH layer on top would be
double-counting the same search.

THE PERMUTATION SHUFFLES OUTCOMES, NOT LABELS. `studio_gates.chance_band()` permutes cell labels
within a date, which is exact when cells partition the rows — every row has one label to
exchange. Our cells overlap by construction: a row can be in `¬conso`, in `rsi<35`, and in
`rsi<35&conso` at once, so there is no single label to permute. Permuting the OUTCOME within
each date is the generalisation: it destroys any relationship between membership and return
while leaving date structure, cell sizes, and the overlap geometry exactly as they are. On a
partition the two are the same operation.

Debugged against SMOKE and DEVELOPMENT seeds only. The acceptance set is not runnable from here.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combolab_spec as SPEC                                        # noqa: E402
from studio_verdict import Estimate, decide                         # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
N_PERM = 120            # permutations for the max-statistic band
N_BOOT = 300            # clustered bootstrap draws, selected cells only
DELTA_STAR = 0.50       # pp — materiality policy for the final verdict, declared here


# ── the population and the frozen membership ─────────────────────────────────
def load_base(verbose: bool = True) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Base population exactly as preregistered. Membership is fixed before any injection."""
    O = pd.read_parquet(os.path.join(ROOT, "data", "opportunities.parquet"))
    O = O.dropna(subset=["ret", "sig_close"]).drop_duplicates("dup_group")
    O = O[(O["sig_close"] >= 21) & (O["sig_close"] <= 89)].reset_index(drop=True)
    v = O["ret"].to_numpy(float) * 100.0
    d = O["sig_date"].astype(str).str[:10].to_numpy()
    if verbose:
        print(f"  base population {len(O):,} trades · {len(np.unique(d)):,} dates", flush=True)
    return O, v, d


def build_masks(O: pd.DataFrame) -> dict:
    """Evaluate the 46 membership rules. The search never sees anything not built here."""
    masks = {}
    for c in SPEC.MANIFEST:
        m = eval(c.membership_rule, {"O": O, "np": np, "pd": pd})       # noqa: S307
        masks[c.cell_id] = np.asarray(m, bool)
    return masks


# ── the ranking statistic ────────────────────────────────────────────────────
def _lower_median(a: np.ndarray) -> float:
    """The k-th order statistic at k = n//2 — one convention, used everywhere.

    np.median averages the two middle values on even n; a selection-based lower median does not.
    The difference is ~0.007pp here, which is nothing on its own and everything if the ranking
    uses one convention and the permutation band the other: the band would then be measuring a
    slightly different statistic from the one being tested against it.
    """
    k = len(a) // 2
    return float(np.partition(a, k)[k])


def _diffs(v: np.ndarray, idx) -> np.ndarray:
    """median(cell) − median(complement) for all 46, from precomputed index arrays.

    A first version did this from one sort plus a cumsum over a 46×N matrix, on the assumption
    that the clever route was the fast one. Measured, it was 0.375s against 0.187s for simply
    taking the two selections — the matrix allocation cost more than the sorting saved. Kept the
    boring one.
    """
    out = np.empty(len(idx))
    for k, (i, j) in enumerate(idx):
        out[k] = _lower_median(v[i]) - _lower_median(v[j])
    return out


# ── permutation worker, at module level so fork can reach it ─────────────────
_PERM: dict = {}


def _perm_once(p: int) -> float:
    """One within-date reshuffle of OUTCOMES, returning the best of the 46.

    Each permutation draws from its own generator seeded by (search_seed, p) rather than from a
    single shared stream. That makes the band identical whatever the worker count — a parallel
    speed-up must not be able to change a number.
    """
    vs, starts, idx, seed = _PERM["vs"], _PERM["starts"], _PERM["idx"], _PERM["seed"]
    rng = np.random.default_rng([seed, p])
    vv = vs.copy()
    for a, b in zip(starts[:-1], starts[1:]):
        if b - a > 1:
            vv[a:b] = rng.permutation(vv[a:b])
    return float(np.nanmax(_diffs(vv, idx)))


class ComboLab:
    """One search over the frozen 46. Rank → permutation band → bootstrap → verdict."""

    def __init__(self, O: pd.DataFrame, dates: np.ndarray, masks: dict):
        self.ids = [c.cell_id for c in SPEC.MANIFEST]
        self.claim_of = {c.cell_id: c.claim_id for c in SPEC.MANIFEST}
        self.masks = masks
        self.M = np.stack([masks[i] for i in self.ids]).astype(np.uint8)
        self.dates = dates
        self.n = len(dates)
        order = np.argsort(dates, kind="stable")
        self.date_order = order
        ds = dates[order]
        self.date_starts = np.r_[0, np.flatnonzero(ds[1:] != ds[:-1]) + 1, self.n]
        uq, gi = np.unique(dates, return_inverse=True)
        self.n_dates, self.gi = len(uq), gi
        self.sizes = {i: int(masks[i].sum()) for i in self.ids}
        # index arrays, in natural order and in date-sorted order
        self.idx = [(np.flatnonzero(masks[i]), np.flatnonzero(~masks[i])) for i in self.ids]
        Md = self.M[:, order].astype(bool)
        self.idx_date = [(np.flatnonzero(m), np.flatnonzero(~m)) for m in Md]

    # ranking ---------------------------------------------------------------
    def estimates(self, v: np.ndarray) -> np.ndarray:
        """median(cell) − median(complement), the one preregistered ranking statistic."""
        return _diffs(v, self.idx)

    # multiplicity ----------------------------------------------------------
    def chance_band(self, v: np.ndarray, seed: int, n_perm: int = N_PERM,
                    workers: int = 0) -> dict:
        """Null distribution of the BEST of 46, outcomes permuted within date.

        The p95 of the maximum is the threshold a claim must clear, and because it is the
        maximum over the whole space it already carries the multiplicity of having looked 46
        times. No second correction is applied on top of it — a Bonferroni or BH layer here
        would be charging the same search twice.
        """
        global _PERM
        _PERM = dict(vs=v[self.date_order], starts=self.date_starts,
                     idx=self.idx_date, seed=int(seed))
        if workers and workers > 1:
            import multiprocessing as mp
            with mp.get_context("fork").Pool(workers) as pool:
                best = np.asarray(pool.map(_perm_once, range(n_perm), chunksize=4))
        else:
            best = np.asarray([_perm_once(p) for p in range(n_perm)])
        return dict(band_p95=float(np.percentile(best, 95)),
                    band_median=float(np.median(best)), n_perm=int(n_perm),
                    band_max=float(best.max()))

    # uncertainty for the selected few --------------------------------------
    def bootstrap(self, v: np.ndarray, cell_id: str, rng, n_boot: int = N_BOOT) -> Estimate:
        """Date-clustered bootstrap for one cell. Only selected cells pay this cost."""
        m = self.masks[cell_id]
        a, b = v[m], v[~m]
        ga, gb = self.gi[m], self.gi[~m]
        oa, ob = np.argsort(a, kind="stable"), np.argsort(b, kind="stable")
        a_s, b_s, ga_s, gb_s = a[oa], b[ob], ga[oa], gb[ob]
        p = np.full(self.n_dates, 1 / self.n_dates)
        d = np.empty(n_boot)
        for k in range(n_boot):
            w = rng.multinomial(self.n_dates, p).astype(float)      # one draw, both arms
            d[k] = _wmed(a_s, w[ga_s]) - _wmed(b_s, w[gb_s])
        lo, hi = np.percentile(d[np.isfinite(d)], [2.5, 97.5])
        est = _lower_median(a) - _lower_median(b)
        est = min(max(est, float(lo)), float(hi))
        deff = max(1.0, (len(a) / max(len(np.unique(ga)), 1)))
        return Estimate(estimate=est, ci_low=float(lo), ci_high=float(hi), level=0.95,
                        estimand=SPEC.ESTIMAND, method="clustered bootstrap (multinomial)",
                        cluster_unit="trading_date", n_raw=len(a),
                        n_eff=int(len(a) / deff))

    # the whole search ------------------------------------------------------
    def search(self, v: np.ndarray, search_seed: int, rng_boot, *, top_k: int = SPEC.TOP_K,
               n_perm: int = N_PERM, n_boot: int = N_BOOT,
               workers: int = 8) -> tuple[pd.DataFrame, dict]:
        est = self.estimates(v)
        R = pd.DataFrame({"cell_id": self.ids,
                          "claim_id": [self.claim_of[i] for i in self.ids],
                          "n": [self.sizes[i] for i in self.ids],
                          "estimate": est})
        SPEC.assert_search_space(R["claim_id"])          # k stays 46 or this raises
        R = R.sort_values("estimate", ascending=False).reset_index(drop=True)
        R["rank"] = np.arange(1, len(R) + 1)

        band = self.chance_band(v, search_seed, n_perm=n_perm, workers=workers)
        R["selected"] = R["rank"] <= top_k
        R["clears_band"] = R["estimate"] > band["band_p95"]

        R["promoted"] = False
        R["verdict"] = ""
        for i in R.index[R["selected"]]:
            e = self.bootstrap(v, R.at[i, "cell_id"], rng_boot, n_boot=n_boot)
            R.at[i, "ci_low"], R.at[i, "ci_high"] = e.ci_low, e.ci_high
            promoted = bool(R.at[i, "clears_band"] and e.ci_low > 0)
            R.at[i, "promoted"] = promoted
            if promoted:
                dec = decide(branch="return", effect=e, delta_star=DELTA_STAR,
                             direction="positive",
                             n_eff_ok=bool(e.n_eff >= 80),
                             cluster_ok=bool(R.at[i, "n"] >= 200))
                R.at[i, "verdict"] = (dec.status if dec.blocking_layer is None
                                      else f"{dec.status}:{dec.blocking_layer}")
            else:
                R.at[i, "verdict"] = "NOT_PROMOTED"
        return R, band


def _wmed(v_sorted, w):
    c = np.cumsum(w)
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)]) if c[-1] > 0 else np.nan
