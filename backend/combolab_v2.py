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


class IncompatibleNullFamilyError(AssertionError):
    """Two claims needing different null mechanisms were put in one max-band family."""


def null_family(cell_id: str, date_level_features=("sig_macro_vix_up",)) -> str:
    """Which null can actually break this claim's association — a property of the CLAIM.

    N0's finding in one function. A predictor constant within a trading date cannot be nulled by
    permuting outcomes inside (date × family): the whole block sits in one arm. Such a claim
    belongs to a different null family, and mixing the two inside one max-statistic band is what
    produced FWER 0.685 against a nominal 0.05.
    """
    return "DAY_LEVEL" if any(f in cell_id or f.replace("sig_", "") in cell_id
                              for f in date_level_features) else "OPPORTUNITY_LEVEL"


def assert_one_null_family(cells) -> str:
    """A max-band family must be homogeneous in its null mechanism."""
    fams = {c: null_family(c) for c in cells}
    kinds = set(fams.values())
    if len(kinds) > 1:
        bad = sorted(c for c, f in fams.items() if f == "DAY_LEVEL")
        raise IncompatibleNullFamilyError(
            f"a single max-band family cannot mix null mechanisms: {sorted(kinds)}. "
            f"{len(bad)} DAY_LEVEL claim(s) present, e.g. {bad[:3]}. Two separately calibrated "
            f"5% bands do not compose into 5% over their union, and N0 measured what happens "
            f"when they are mixed: FWER 0.685 against a nominal 0.05.")
    return kinds.pop()


class SearchSpaceDegeneracyError(AssertionError):
    """Two selectable hypotheses share a frozen membership signature."""


def equivalence_classes(masks: dict, *, verbose: bool = True):
    """46 names → 37 selectable classes, with every name kept as an alias.

    All seven duplicate groups are STRUCTURAL, provable from the engine rather than observed on
    this window:

        edge_replay.py:749   lead_in_lag = rs_flag & sector_lag      (rs_flag IS rs_intact)
        edge_replay.py:1107  h1_dr       = (h1_today | h1_yest) & rs_intact

    so conjoining either with rs narrows nothing, and the remaining groups follow. None needs an
    `equivalent_on_H` caveat — they are theorems about the feature contract, not facts about
    2021-2026.

    The names survive because "46 formulations were written and nine turned out redundant" is
    provenance. Silently rewriting history into "there were always 37" would erase the fact that
    the space was designed with a blind spot in it.
    """
    by_sig: dict = {}
    for cid, m in masks.items():
        by_sig.setdefault(m.tobytes(), []).append(cid)
    classes = []
    for sig, names in by_sig.items():
        classes.append(dict(class_id=f"EQ_{len(classes):02d}", representative=names[0],
                            aliases=tuple(names), n=int(masks[names[0]].sum())))
    if verbose:
        red = sum(len(c["aliases"]) - 1 for c in classes)
        print(f"  search space · {len(masks)} manifest entries → {len(classes)} selectable "
              f"classes · {red} redundant aliases", flush=True)
        for c in classes:
            if len(c["aliases"]) > 1:
                print(f"      {c['class_id']} n={c['n']:>7,d}  "
                      + " ≡ ".join(c["aliases"]), flush=True)
    return classes


def assert_no_degeneracy(cells, masks) -> None:
    """A duplicate must be an error at construction, never a tie the ranking interprets later."""
    seen = {}
    for cid in cells:
        sig = masks[cid].tobytes()
        if sig in seen:
            raise SearchSpaceDegeneracyError(
                f"{cid!r} and {seen[sig]!r} have identical frozen membership. Two selectable "
                f"hypotheses with the same support are not two hypotheses — they occupy two "
                f"top-K slots, crowd other claims out, and produce a tie the ranking then has "
                f"to break on sort order rather than on data.")
        seen[sig] = cid


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
        if not len(self.meta):
            # No cell cleared the eligibility floors, so there is no frame to filter. Historically
            # unreachable — every cell survived on six years of rows — and the forward path
            # reaches it on week one, where "nothing is eligible yet" is the expected answer
            # rather than an error. Only this branch is new; with any eligible cell the code
            # below runs exactly as before, which is why the frozen oracle still reproduces.
            self.below_floor = []
            return
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
