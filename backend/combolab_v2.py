"""The incremental estimator, and the synthetic worlds that know their own truth.

Frozen spec in `combolab_v2_spec.py`. This file implements it and nothing beyond it.

    Δ_cs = median(Y | Cell_c=1, S=s) − median(Y | Cell_c=0, S=s)
    θ_c  = Σ_{s ∈ E_c} w_cs · Δ_cs        w_cs = n_cell,cs / Σ_r n_cell,cr

`E_c` and `w_cs` come from X and membership only, and are built ONCE before any outcome exists.
Recomputing them per world — or per bootstrap draw — would let the outcome choose its own
support set, which is the quiet version of the same mistake as picking a threshold after seeing
the result.

GROUND TRUTH IS COMPUTED, NOT DERIVED. For the planted cell, translation equivariance of the
median gives θ + δ exactly, because every one of its treated rows inside every eligible stratum
receives δ. For a cell that merely OVERLAPS the planted one, no closed form exists: only some of
its treated rows are injected, and a median does not pass through a fraction. But the synthetic
world is fully known, so the truth for all 46 is obtained by applying the estimand to the entire
synthetic population with no resampling. That is exact, and it is strictly better than v1's
three-way TRUE/OVERLAP/UNRELATED classification — every claim gets a number, so bias is
measurable rather than categorised.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2_spec as V2                                       # noqa: E402


def _lower_median(a: np.ndarray) -> float:
    k = len(a) // 2
    return float(np.partition(a, k)[k])


class Support:
    """E_c and w_cs, built from membership and dates only. No outcome is visible here."""

    def __init__(self, O: pd.DataFrame, dates: np.ndarray, masks: dict, verbose: bool = True):
        fam = O["family"].astype(str).to_numpy()
        el = V2.ELIGIBILITY
        self.cells, self.strata, self.weights = [], {}, {}
        self.meta = []
        for cid, m in masks.items():
            keep = []
            for s in np.unique(fam):
                f = fam == s
                ic, io = m & f, (~m) & f
                nc, no = int(ic.sum()), int(io.sum())
                if nc < el["n_min"] or no < el["n_min"]:
                    continue
                dc = np.unique(dates[ic], return_counts=True)[1]
                do = np.unique(dates[io], return_counts=True)[1]
                if len(dc) < el["dates_min"] or len(do) < el["dates_min"]:
                    continue
                if dc.max() / nc > el["max_single_date_share"] or \
                        do.max() / no > el["max_single_date_share"]:
                    continue
                keep.append((s, np.flatnonzero(ic), np.flatnonzero(io), nc))
            if not keep:
                continue
            tot = sum(k[3] for k in keep)
            self.cells.append(cid)
            self.strata[cid] = [(s, i, j) for s, i, j, _ in keep]
            self.weights[cid] = np.array([k[3] / tot for k in keep])
            # SupportCoverage_c — defined literally: the share of the CELL's own opportunities
            # that entered the estimand. Not a share of setup rows, not a count of setups.
            self.meta.append(dict(cell=cid, eligible_setups=len(keep),
                                  eligible_cell_opportunities=tot,
                                  eligible_dates=len(np.unique(np.concatenate(
                                      [dates[i] for _, i, _ in self.strata[cid]]))),
                                  support_fraction=tot / int(m.sum())))
        self.meta = pd.DataFrame(self.meta)
        if verbose:
            print(f"  support · {len(self.cells)}/{len(masks)} cells selectable · "
                  f"setups per cell min {self.meta.eligible_setups.min()} "
                  f"median {self.meta.eligible_setups.median():.0f} "
                  f"max {self.meta.eligible_setups.max()} · "
                  f"coverage min {self.meta.support_fraction.min():.1%}", flush=True)
        floor = V2.SUPPORT_FLOOR
        bad = self.meta[(self.meta.support_fraction < floor["min_coverage_of_cell_opportunities"])
                        | (self.meta.eligible_setups < floor["min_eligible_setups"])]
        self.below_floor = list(bad["cell"])

    def theta(self, y: np.ndarray) -> pd.Series:
        """θ_c for every selectable cell. Exact on a known population, no resampling."""
        out = {}
        for cid in self.cells:
            w = self.weights[cid]
            d = np.array([_lower_median(y[i]) - _lower_median(y[j])
                          for _, i, j in self.strata[cid]])
            out[cid] = float(w @ d)
        return pd.Series(out)

    def marginal(self, y: np.ndarray, masks: dict) -> pd.Series:
        """v1's estimand, on the same rows — kept so the two generations can be contrasted."""
        return pd.Series({cid: _lower_median(y[masks[cid]]) - _lower_median(y[~masks[cid]])
                          for cid in self.cells})


# ── synthetic worlds ─────────────────────────────────────────────────────────
def composition_world(O: pd.DataFrame, dates: np.ndarray, *, seed: int, noise: bool,
                      date_effect: bool = True) -> np.ndarray:
    """Y = μ_setup [+ γ_date + ε]. Cell carries NO information inside a setup, by construction.

    The setup effect is what makes the marginal estimand legitimately non-zero: cells that
    happen to contain strong setups will show a marginal difference, and v1 is entitled to see
    it. The whole point of v2 is to refuse to call that an effect of the context.

    The date component matters. With i.i.d. noise the world would be far easier than the real
    one: a market day moves many opportunities together, and dropping that would flatter both
    the interval and the search. Each component draws from its own named substream so adding one
    later cannot shift the others.
    """
    fam = O["family"].astype(str).to_numpy()
    us, si = np.unique(fam, return_inverse=True)
    mu = np.random.default_rng([seed, 1]).normal(0, 4.0, size=len(us))     # setup_effect_rng
    y = mu[si]
    if date_effect:
        ud, di = np.unique(dates, return_inverse=True)
        y = y + np.random.default_rng([seed, 2]).normal(0, 3.0, size=len(ud))[di]  # date_rng
    if noise:
        y = y + np.random.default_rng([seed, 3]).normal(0, 6.0, size=len(y))       # idio_rng
    return y


def inject(y: np.ndarray, sup: Support, cell: str, delta: float) -> np.ndarray:
    """δ added to the planted cell's treated rows INSIDE its frozen eligible strata.

    Restricting the injection to the eligible support is what makes the truth exact: every row
    the estimand looks at in that arm is shifted, so each Δ_cs moves by exactly δ and the fixed
    weights sum to one.
    """
    out = y.copy()
    for _, i, _ in sup.strata[cell]:
        out[i] += delta
    return out
