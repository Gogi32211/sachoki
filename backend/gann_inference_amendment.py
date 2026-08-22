"""GANN_INFERENCE_AMENDMENT_V1 — the phase shift moves from null to placebo.

WHY. The X-only qualification showed that phi = 0 is not exchangeable with a uniform
shifted phase in event GEOMETRY: at phi = 0 the ASC and CONFLUENCE event counts exceed ALL
999 registered shifted-phase worlds. That is not noise, it is construction — the integer
lattice is anchored at L and M itself comes from H/L, so phi = 0 is a distinguished point
of the family rather than one more draw from it. A null that swaps phi = 0 for phi ~ U[0,1)
therefore changes the event population, the direction population and the pre-treatment
mixture along with the hypothesis, and blocks control part of that without proving
exchangeability.

SO THE TWO QUESTIONS ARE SEPARATED.

    GATE A  primary inferential null — outcome permutation on the frozen phi = 0
            memberships. Answers: is there a directional excursion association at all,
            conditional on the registered structure?

    GATE B  registered shifted-lattice PLACEBO reference — the 999 phase worlds, kept
            exactly as sealed. Answers: is any such association specific to the anchored
            integer phase, or would an equally dense shifted lattice do as well?

    A PASS / B FAIL  association exists, integer-phase specificity NOT established
    A FAIL           no registered directional association established
    A PASS / B PASS  association exists AND is concentrated at the anchored lattice
                     relative to same-geometry phase shifts

The phase tail probability is NOT called an exact phase-exchangeability p-value any more.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

N_PERM = 999
N_PHASE = 999
DELTAS_PP = [0.5, 1.0, 2.0, 3.0, 4.0, 5.0]
CLAIMS = ["GANN_ASC_TOUCH_V1", "GANN_DESC_TOUCH_V1", "GANN_CONFLUENCE_TOUCH_V1"]


def amendment():
    q = json.load(open("GANN_PHASE_X_QUALIFICATION_V1.json"))
    geo = q["phase_geometry"]
    body = dict(
        spec_id="GANN_INFERENCE_AMENDMENT_V1", status="FROZEN",
        amends=["GANN_PHASE_NULL_V1", "GANN_CAPABILITY_PROTOCOL_V1"],
        frozen_before="the first GANN outcome read",
        trigger=dict(
            finding="phi = 0 is structurally non-exchangeable with uniform shifted phases "
                    "in event geometry",
            evidence={k: dict(observed=geo[k]["observed"], null_max=geo[k]["null_max"],
                              percentile=geo[k]["observed_percentile"])
                      for k in ("ASC.events", "CONF.events",
                                "CONF.directional_valid")},
            reading="the anchored lattice touches more often than every one of the 999 "
                    "shifted worlds; blocks control part of that difference but do not "
                    "establish exchangeability"),
        gate_A=dict(
            name="PRIMARY INFERENTIAL NULL — outcome permutation",
            memberships="the frozen phi = 0 X memberships, unchanged",
            unit="the direction-neutral PAIR (MFE_LONG_10D, MFE_SHORT_10D) permuted "
                 "jointly",
            blocks=["decision_date", "pre_liquidity", "pre_volatility"],
            direction_not_a_block="direction is derived from X, so it must not be a "
                                  "permutation dimension; it stays a block of the "
                                  "comparison",
            per_permutation="select LONG/SHORT by the frozen phi = 0 X direction, compute "
                            "all three registered Z_rank statistics, persist "
                            "maxZ_perm = max(Z_ASC, Z_DESC, Z_CONF)",
            permutations=N_PERM,
            promotion="Z_obs(claim) > p95(maxZ_perm)  — family-wise, strict",
            answers="is there a directional excursion association at all, conditional on "
                    "the registered structure?"),
        gate_B=dict(
            name="REGISTERED SHIFTED-LATTICE FAMILY-WISE PLACEBO REFERENCE",
            worlds=N_PHASE,
            mechanism="the already sealed ticker-local shared-phase worlds, retained "
                      "verbatim — one phi per ticker, both families, every date",
            statistic="for every shifted world w, maxZ_shift[w] = max(Z_ASC_shift[w], "
                      "Z_DESC_shift[w], Z_CONF_shift[w]) — the FAMILY-WISE maximum, "
                      "exactly as gate A takes its own",
            threshold="GATE B PASS for claim c iff Z_phi0[c] > p95(maxZ_shift), strictly",
            why_family_wise="three claims are compared at once, so a claim-specific "
                            "threshold would let 'which of the three beat the shifted "
                            "reference?' be answered after looking. Multiplicity is a "
                            "cross-cutting rule here and gate B does not get an exemption.",
            claim_specific_percentile="may be reported DESCRIPTIVELY as "
                                      "claim_specific_shifted_percentile, but it can never "
                                      "upgrade the interpretation — the strong "
                                      "GANN-SPECIFIC label requires the family-wise "
                                      "threshold",
            wording_forbidden=["exact phase-exchangeability p-value",
                               "randomization p-value",
                               "probability that the Gann construction is false"],
            wording_required="registered shifted-lattice placebo reference",
            answers="is any association specific to the anchored integer phase, or would "
                    "an equally dense shifted lattice do as well?"),
        interpretation_table={
            "A PASS, B PASS": "association exists AND is concentrated at the anchored "
                              "integer lattice relative to same-geometry phase shifts",
            "A PASS, B FAIL": "a price-reaction association exists, but it is NOT specific "
                              "to the Gann integer phase",
            "A FAIL": "no registered directional association is established; gate B is not "
                      "interpreted"},
        capability=dict(
            qualifies="GATE A only",
            per_world=["jointly block-nullize the LONG/SHORT outcome pair",
                       "inject delta only into the component matching the frozen phi = 0 "
                       "treated direction",
                       f"run the registered {N_PERM}-permutation max-Z engine"],
            detection="injected claim Z > p95(maxZ_perm)",
            acceptance=dict(at_1pp="each of the 3 claims >= 16 / 20",
                            at_2pp="each of the 3 claims >= 19 / 20",
                            at_0_5pp="descriptive sensitivity only"),
            phase_placebo_in_capability="NOT a blocking criterion; its behaviour under the "
                                        "injected worlds may be characterized later",
            matrix=dict(claims=CLAIMS, deltas=DELTAS_PP, worlds_per_cell=20,
                        total=len(CLAIMS) * len(DELTAS_PP) * 20)),
        retained=dict(
            phase_x_qualification=dict(
                digest=ART.file_digest("GANN_PHASE_X_QUALIFICATION_V1.json"),
                status="VALID AND SEALED — its geometry findings are not superseded; they "
                       "are the REASON for this amendment"),
            x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
            estimand=ART.file_digest("GANN_ESTIMAND_V1.json"),
            rank_code=ART.file_digest("gann_rank.py")),
        outcome_spec_provenance=dict(
            active=dict(spec_id="GANN_OUTCOME_SPEC_V1",
                        digest=ART.file_digest("GANN_OUTCOME_SPEC_V1.json"),
                        status="ACTIVE / SEALED",
                        content="direction-neutral: MFE_LONG_10D, MFE_SHORT_10D, "
                                "MFE_ATR_LONG_10D, MFE_ATR_SHORT_10D"),
            superseded=dict(digest="5a40ebc562ca5262",
                            status="SUPERSEDED",
                            content="the earlier single DIR_MFE_10D materialised through "
                                    "the phi = 0 direction",
                            sidecar="GANN_OUTCOME_SPEC_V1.json.superseded."
                                    "5a40ebc562ca5262.json"),
            note="an earlier progress message labelled the ACTIVE digest as superseded; "
                 "that was a reporting error, not an artifact error — the file has carried "
                 "the direction-neutral definition since it was re-sealed"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    return ART.seal(body, "GANN_INFERENCE_AMENDMENT_V1.json",
                    required=("spec_id", "gate_A", "gate_B", "interpretation_table"),
                    supersede=os.path.exists("GANN_INFERENCE_AMENDMENT_V1.json"))


def reseal_phase_null(amend_digest):
    d = json.load(open("GANN_PHASE_NULL_V1.json"))
    d["role"] = ("REGISTERED SHIFTED-LATTICE FAMILY-WISE PLACEBO REFERENCE "
                 "(gate B)")
    d["amended_by"] = amend_digest
    d["not_the_primary_null"] = ("the primary inferential null is the outcome permutation "
                                 "of GANN_INFERENCE_AMENDMENT_V1 gate A; phi = 0 is not "
                                 "exchangeable with uniform shifted phases in event "
                                 "geometry, so this distribution is a placebo reference")
    d["H0_restated"] = ("conditional on the frozen anchors, spacing, slopes and "
                        "pre-treatment structure, any directional association found at "
                        "phi = 0 is no stronger than at a ticker-local shifted phase of "
                        "the same geometry")
    d["p_value_wording"] = ("a shifted-lattice placebo tail probability — NEVER an exact "
                            "or randomization p-value")
    d["promotion"] = ("gate B: Z_phi0(claim) > p95(maxZ_shift) where maxZ_shift[w] = "
                      "max over the three claims in shifted world w — family-wise, strict")
    d["family_wise"] = "maxZ_shift[w] = max(Z_ASC_shift[w], Z_DESC_shift[w], Z_CONF_shift[w])"
    return ART.seal(d, "GANN_PHASE_NULL_V1.json",
                    required=("spec_id", "H0", "mechanism"), supersede=True)


def reseal_capability(amend_digest):
    d = json.load(open("GANN_CAPABILITY_PROTOCOL_V1.json"))
    d["amended_by"] = amend_digest
    d["qualifies"] = "GATE A — the outcome-permutation inferential null"
    d["nullization"]["engine"] = (f"{N_PERM} joint block permutations of "
                                  "(MFE_LONG_10D, MFE_SHORT_10D)")
    d["detection"] = ("Z_injected_claim > p95(maxZ_perm) from the registered "
                      f"{N_PERM}-permutation max-Z engine — the same machinery the "
                      "historical gate A will use")
    d["phase_placebo"] = ("not a blocking capability criterion; the shifted-lattice "
                          "reference is evaluated on the historical result, not required "
                          "to detect an injected effect")
    d["matrix"]["phase_worlds_per_capability_world"] = 0
    d["matrix"]["outcome_permutations_per_capability_world"] = N_PERM
    return ART.seal(d, "GANN_CAPABILITY_PROTOCOL_V1.json",
                    required=("spec_id", "the_invariant", "nullization", "acceptance"),
                    supersede=True)


if __name__ == "__main__":
    a = amendment()
    print(f"GANN_INFERENCE_AMENDMENT_V1   · {a}")
    print(f"GANN_PHASE_NULL_V1 (re-sealed) · {reseal_phase_null(a)}")
    print(f"GANN_CAPABILITY_PROTOCOL_V1    · {reseal_capability(a)}")
    print(f"GANN_OUTCOME_SPEC_V1 ACTIVE    · {ART.file_digest('GANN_OUTCOME_SPEC_V1.json')}")
