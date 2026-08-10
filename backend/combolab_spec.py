"""The frozen search space, written BEFORE ComboLab exists.

Ordinarily a positive control is added after the algorithm works, and its behaviour is then
explained backwards. Here the order is inverted, which is the whole point:

    claim about the required capability
        → frozen test
            → implementation
                → sealed evaluation

Everything in this file is fixed before a single line of the search layer is written. It
declares what a claim IS, how many there are, how they overlap, what counts as finding the
needle, and which seeds are sealed. The search layer will import it and must not extend it —
if the implementation ever produces a selectable result that is not in the manifest,
`assert_search_space()` raises rather than letting `k` drift.

Three separate things are being frozen, and conflating them is how positive controls quietly
turn into development sets:

    SMOKE        does the code run at all — tiny, disposable, look freely
    DEVELOPMENT  visible seeds, run as often as needed while debugging the architecture
    ACCEPTANCE   SEALED. Not to be run, or looked at, until the implementation is frozen and
                 its git hash is recorded. Opened once.

The seal is enforced mechanically, not by memory: the acceptance runner refuses to start
without an IMPLEMENTATION_FROZEN marker naming the git hash of the search layer, and it writes
that hash into its results. Editing the engine after seeing acceptance numbers leaves a
mismatch that is visible in the ledger.

WHAT IS NOT CLAIMED HERE. The delta grid's upper points (3.00, 6.00 pp) are preregistered
sensitivity probes. They carry no claim of corresponding to √46 scaling, to a known search-layer
MDE, or to any measured quantity. Cells overlap, date mass is wildly unequal, and 59 families
coexist; none of the assumptions behind such scaling hold. The search-layer MDE is UNKNOWN and
measuring it is the point of the curve.
"""
from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# ── the estimand, singular and frozen ────────────────────────────────────────
ESTIMAND = "median_return_difference_pp"
HORIZON = "path_sim_exit"          # the table's own `ret`; one horizon, deliberately
CONTROL = "complement_within_base_population"
BASE_POPULATION = "price_21_89 · deduplicated by dup_group · finite ret and sig_close"

# One estimand × one horizon × one control definition means each cell contributes exactly ONE
# selectable claim. That is what keeps k honest: k counts distinct claims the search may
# inspect or select, NOT the validity gates each claim passes through afterwards. A claim that
# survives evidence → materiality → stability → OOS → DSR is one hypothesis with several
# acceptance conditions, not five trials. Had the search been allowed to pick the best of
# several horizons or several exit rules, each of those would have been a separate claim.

DELTA_GRID = (0.00, 0.60, 1.50, 3.00, 6.00)
DELTA_PROVENANCE = {
    0.00: "false-discovery control — NO needle exists; recall and rank are undefined here",
    0.60: "near_estimator_MDE — frozen ref engine_return_v1: estimator resolved 95.8%, "
          "verdict_v2 MATERIALITY passed 26%",
    1.50: "above_estimator_MDE — frozen ref harness_power: MDE@80% ≤ 1.20pp (pooled arm)",
    3.00: "preregistered upper sensitivity probe — no scaling claim",
    6.00: "preregistered upper sensitivity probe — no scaling claim",
}

RNG_STREAMS = ("needle_location", "needle_effect", "search", "bootstrap")

SEEDS = {
    "smoke":       tuple(range(900_000, 900_003)),
    "development": tuple(range(100_000, 100_040)),
}
N_ACCEPTANCE = 120

# ── the seal, and why a plaintext seed list is not one ───────────────────────
# The first version of this file listed 120 acceptance seeds in the open. That commits to WHICH
# CODE the acceptance set saw, but it does not seal the set: freeze A → open acceptance → see
# the numbers → change ComboLab → freeze B leaves a trace in the ledger and a contaminated test.
# A secret file next to the repo is no better, because I can read it.
#
# So the seeds are derived from something that CANNOT EXIST until the implementation is frozen:
# the git commit hash of the freeze commit itself. That hash is determined by the frozen code
# and is unknowable while the code is still being written — including to me. What is committed
# now is the DERIVATION RULE; the seeds materialise at freeze and are verifiable afterwards by
# anyone holding the commit.


def acceptance_seeds(freeze_commit_sha: str, n: int = N_ACCEPTANCE) -> tuple[int, ...]:
    """Derived from the freeze commit — not knowable, by anyone, before the freeze."""
    if not freeze_commit_sha or len(freeze_commit_sha) < 40:
        raise SearchSpaceContractError(
            "acceptance seeds require the full git hash of the IMPLEMENTATION_FROZEN commit; "
            "there are no acceptance seeds before the implementation is frozen, which is the "
            "entire point of the seal")
    h = hashlib.sha256(f"combolab-acceptance:{freeze_commit_sha}".encode()).digest()
    rng = np.random.default_rng(int.from_bytes(h[:8], "big"))
    return tuple(int(x) for x in rng.integers(1, 2**31 - 1, size=n))


SEED_COMMITMENT = hashlib.sha256(
    json.dumps({"visible": {k: list(v) for k, v in SEEDS.items()},
                "acceptance_rule": "sha256('combolab-acceptance:' + freeze_commit_sha) "
                                   "→ default_rng → 120 integers",
                "n_acceptance": N_ACCEPTANCE}, sort_keys=True).encode()).hexdigest()


class SearchSpaceContractError(AssertionError):
    """The implementation produced a claim the preregistration does not contain."""


# ── what a claim IS ──────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Claim:
    """One selectable hypothesis. Everything the search may see about a cell lives here."""
    cell_id: str
    family_id: str
    membership_rule: str
    estimand: str = ESTIMAND
    horizon: str = HORIZON
    control: str = CONTROL

    @property
    def claim_id(self) -> str:
        return f"{self.cell_id}::{self.estimand}@{self.horizon}"


def _flag(name: str, want: bool) -> tuple[str, str]:
    return (f"{'' if want else '¬'}{name}", f"O['{name}'] == {want}")


FLAGS = ("sig_rs_intact", "sig_conso", "sig_lead_in_lag", "sig_h1_dr", "sig_macro_vix_up")
RSI_BANDS = (("rsi<35", "O['sig_rsi_14'] < 35"),
             ("rsi35-50", "(O['sig_rsi_14'] >= 35) & (O['sig_rsi_14'] < 50)"),
             ("rsi50-65", "(O['sig_rsi_14'] >= 50) & (O['sig_rsi_14'] < 65)"),
             ("rsi>=65", "O['sig_rsi_14'] >= 65"))
ADX_LEVELS = ((-1, "adx-1"), (0, "adx0"), (1, "adx1"), (2, "adx2"), (3, "adx3"))


def build_manifest() -> tuple[Claim, ...]:
    """The 46 claims, constructed deterministically so k is derived and never typed.

    Deliberately overlapping: singles, their negations, pairs and a few triples of the same
    flags, plus RSI bands and ADX levels. Real search spaces overlap, and pretending otherwise
    would make the needle far easier to find than it is in practice.
    """
    out: list[Claim] = []
    for f in FLAGS:                                        # 5 singles + 5 negations = 10
        for want in (True, False):
            nm, rule = _flag(f, want)
            out.append(Claim(nm, "flag_single", rule))
    for a, b in itertools.combinations(FLAGS, 2):          # C(5,2) = 10 pairs
        out.append(Claim(f"{a}+{b}", "flag_pair",
                         f"(O['{a}'] == True) & (O['{b}'] == True)"))
    for a, b, c in itertools.combinations(FLAGS[:4], 3):   # C(4,3) = 4 triples
        out.append(Claim(f"{a}+{b}+{c}", "flag_triple",
                         f"(O['{a}'] == True) & (O['{b}'] == True) & (O['{c}'] == True)"))
    for nm, rule in RSI_BANDS:                             # 4
        out.append(Claim(nm, "rsi_band", rule))
    for lvl, nm in ADX_LEVELS:                             # 5
        out.append(Claim(nm, "adx_regime", f"O['sig_adx_regime'] == {lvl}"))
    for f in FLAGS[:4]:                                    # 4 · RS-conditioned variants
        out.append(Claim(f"{f}&rs", "flag_rs_cond",
                         f"(O['{f}'] == True) & (O['sig_rs_intact'] == True)"))
    for nm, rule in RSI_BANDS[:3]:                         # 3 · RSI × RS
        out.append(Claim(f"{nm}&rs", "rsi_rs_cond",
                         f"({rule}) & (O['sig_rs_intact'] == True)"))
    for nm, rule in RSI_BANDS[:3]:                         # 3 · RSI × CONSO
        out.append(Claim(f"{nm}&conso", "rsi_conso_cond",
                         f"({rule}) & (O['sig_conso'] == True)"))
    for lvl, nm in ADX_LEVELS[2:5]:                        # 3 · ADX × RS
        out.append(Claim(f"{nm}&rs", "adx_rs_cond",
                         f"(O['sig_adx_regime'] == {lvl}) & (O['sig_rs_intact'] == True)"))
    return tuple(out)


MANIFEST: tuple[Claim, ...] = build_manifest()
DECLARED_K = len(MANIFEST)          # derived, never typed
N_CELLS = DECLARED_K


def assert_search_space(produced_claim_ids) -> None:
    """The implementation may not invent a 47th selectable result."""
    want = {c.claim_id for c in MANIFEST}
    got = set(produced_claim_ids)
    if got != want:
        extra, missing = sorted(got - want), sorted(want - got)
        raise SearchSpaceContractError(
            f"declared claims: {len(want)} · actual selectable claims: {len(got)}"
            + (f"\n  not in manifest: {extra[:5]}" if extra else "")
            + (f"\n  missing: {missing[:5]}" if missing else ""))


# ── overlap, frozen before injection ─────────────────────────────────────────
def exposure_matrix(masks: dict) -> pd.DataFrame:
    """E[j,i] = |Cj ∩ Ci| / |Cj| — the fraction of cell j's rows that also lie in cell i.

    Asymmetric, and the orientation is the substance. Because the injection adds δ to EVERY row
    of the needle cell, `E[j, needle]` is not a similarity between rules — it is literally the
    share of cell j's observations that carry the injected outcome. A cell built from 40%
    injected rows will move for an entirely legitimate reason, and calling its promotion a false
    discovery would blame the search layer for the geometry of the space it was given.

    Frozen before injection, and before any implementation, so that "cell_29 came up next to the
    needle, its overlap is 14%, let us call that contaminated" is not available after the fact.
    """
    ids = list(masks)
    M = np.zeros((len(ids), len(ids)))
    for a, j in enumerate(ids):
        mj = masks[j]
        nj = mj.sum()
        for b, i in enumerate(ids):
            M[a, b] = (mj & masks[i]).sum() / nj if nj else 0.0
    return pd.DataFrame(M, index=ids, columns=ids)


EXPOSURE_THRESHOLD = 0.20   # ≥20% of the promoted cell's rows carry the injected outcome


def classify_promotion(cell_id: str, needle_cell: str | None, exposure: pd.DataFrame) -> str:
    """TRUE_NEEDLE · OVERLAP_AFFECTED · UNRELATED_PROMOTION. Only the last is a false discovery."""
    if needle_cell is None:
        return "NULL_PROMOTION"
    if cell_id == needle_cell:
        return "TRUE_NEEDLE"
    if exposure.loc[cell_id, needle_cell] >= EXPOSURE_THRESHOLD:
        return "OVERLAP_AFFECTED"
    return "UNRELATED_PROMOTION"


# ── what "found" means — three definitions, never one ────────────────────────
TOP_K = 5

FOUND_DEFINITIONS = {
    "RANK_FOUND": f"the true cell appears in the preregistered top-{TOP_K} by ranking statistic",
    "STATISTICALLY_PROMOTED": "the true cell survives the chance band and the multiplicity "
                              "policy",
    "FINAL_ACCEPTED": "the true cell receives a positive final verdict from verdict_v2",
}

# A cell can rank 1st, be promoted, and still end at REJECT:VALIDITY. That is a legitimate
# governance refusal and must not be recorded as a search failure — the same attribution
# discipline that separated L2 from the interval, and the estimator from the gates.

OUTCOMES = ("conditional_rank_recall", "conditional_search_recall",
            "conditional_final_acceptance", "rank_distribution",
            "unrelated_false_promotions", "overlap_affected_promotions")

RANK_QUANTILES = ("p_rank_1", "p_rank_le_3", "p_rank_le_5", "median_rank", "p90_rank")
# The distribution, not the average: a median rank of 2 is compatible with 70% at rank 1 and
# 30% at rank 35, and for a search engine those are entirely different machines.

SAMPLING = {
    "kind": "finite_population_subsample",
    "conditioned_on": "realized_history_2021_2026",
    "unit": "trading_date",
    "note": "every outcome above is CONDITIONAL on this history. sampling_target.py refuses to "
            "let any of them be reported as `power`.",
}


def spec_digest() -> str:
    """One hash over everything that must not move after freeze."""
    payload = json.dumps({
        "estimand": ESTIMAND, "horizon": HORIZON, "control": CONTROL,
        "base": BASE_POPULATION, "delta_grid": list(DELTA_GRID),
        "claims": [c.claim_id for c in MANIFEST], "k": DECLARED_K,
        "top_k": TOP_K, "exposure_threshold": EXPOSURE_THRESHOLD,
        "exposure_metric": "E[j,i] = |Cj n Ci| / |Cj| (share of j's rows carrying injection)",
        "seed_commitment": SEED_COMMITMENT, "streams": list(RNG_STREAMS),
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


if __name__ == "__main__":
    print(f"claims declared      {DECLARED_K}")
    print(f"families             {len({c.family_id for c in MANIFEST})}")
    print(f"delta grid           {DELTA_GRID}")
    print(f"seed commitment      {SEED_COMMITMENT[:16]}…")
    print(f"acceptance seeds     NOT YET DERIVABLE — need the freeze commit hash")
    print(f"SPEC DIGEST          {spec_digest()}")
    for fam in sorted({c.family_id for c in MANIFEST}):
        n = sum(c.family_id == fam for c in MANIFEST)
        print(f"  {fam:<16s} {n:>3d}")
