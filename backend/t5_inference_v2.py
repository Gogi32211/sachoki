"""SEQUENCE_INFERENCE_V2 — freeze the semantics. No compute, no data, no outcome.

V1 IS NOT OVERWRITTEN

    SEQUENCE_INFERENCE_V1   RETIRED FOR PROMOTION
    reason                  unstudentized raw-max detector failed empirical sensitivity
                            qualification

What was NOT demonstrated is as important as what was. Raw theta was never shown to fail as an
EFFECT-SIZE ESTIMAND. What failed is its use as the search-wide decision statistic. Historical
theta has still never been computed, so its own sampling and post-selection properties are a
separate question that this spec does not settle.

WHAT CHANGES AND WHAT DOES NOT

    population, blocks, grammar, k=840, weights     IDENTICAL TO SEALED V1
    economic effect size, theta_s in MFE pp         UNCHANGED
    selection statistic                             CHANGED: bounded within-block AUC

The estimand is not being replaced. A statistic that decides WHICH claims are worth reporting
is being separated from the statistic that says HOW LARGE the effect is, because the evidence
now says one estimator cannot do both jobs here:

    scale pathology   analytic studentization      max-null 28.6pp -> 7.36 sd units
    shape pathology   bounded block score          marginal-tail excess +3.88 -> +0.38

THETA CANNOT RESCUE A RANK FAILURE

Promotion is decided by max Z_rank against its registered band. A large raw theta on a claim
whose rank statistic does not clear the band is NOT an alternative route to promotion. If that
door were left open the whole multiplicity control would be void.

WHAT THIS SPEC DOES NOT CLAIM

Null geometry is not power. The bounded score removed the heavy tail by discarding magnitude,
and that same discarding acts on the numerator too: a block where the treated episode beats
its controls by +400% scores identically to one where it beats them by +4%. Whether an
additive raw-MFE effect converts into rank displacement fast enough to be detected is unknown
until the V2 capability run measures it.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

OUT = os.path.join(HERE, "SEQUENCE_INFERENCE_V2.json")
RET = os.path.join(HERE, "SEQUENCE_INFERENCE_V1_RETIREMENT.json")

SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
PAIRED = json.load(open("T5_CAPABILITY_SPEC_V1_PAIRED.json"))
SHAPE = json.load(open("T5_NULL_SHAPE_DIAGNOSTIC_V1.json"))
RANK = json.load(open("T5_RANK_NULL_GEOMETRY_DIAGNOSTIC_V1.json"))
VARD = json.load(open("T5_NULL_VARIANCE_DECOMPOSITION_V1.json"))


def main():
    ret = dict(
        spec_id="SEQUENCE_INFERENCE_V1_RETIREMENT",
        v1_status="RETIRED FOR PROMOTION",
        retained_for="effect-size reporting and every frozen artifact built under it",
        reason="unstudentized raw-max detector failed empirical sensitivity qualification",
        evidence=dict(
            capability="T5_CAPABILITY_Q50_RESULT_V1 — median-support needle, +0.5..+5.0pp "
                       "injections detected 0/20 at every delta",
            mechanism_scale="NULL_VARIANCE_DECOMPOSITION_V1 — Var(theta)=sum w^2 Var(Delta_b) "
                            "reconstructs measured null sd at R2 0.98; block tail scale, not "
                            "weight or arm geometry, separates the raw-max dominators",
            mechanism_shape="NULL_SHAPE_DIAGNOSTIC_V1 — after exact studentization the "
                            "standardized family is still not pivotal: median excess kurtosis "
                            "5.35, P(Z>3) 17.8x gaussian, max-Z p95 7.36 vs 3.84 independent"),
        not_demonstrated="raw theta was NOT shown to fail as an effect-size estimand. Only its "
                         "use as the search-wide decision statistic failed. Historical theta "
                         "has never been computed; its sampling and post-selection properties "
                         "remain a separate, open question.",
        superseded_by="SEQUENCE_INFERENCE_V2")
    json.dump(ret, open(RET, "w"), indent=2)

    body = dict(
        spec_id="SEQUENCE_INFERENCE_V2",
        status="FROZEN_SEMANTICS — capability not yet qualified, historical Y not exposed",
        supersedes="SEQUENCE_INFERENCE_V1",

        inherited_unchanged=dict(
            estimand_seal=SEAL["seal_digest"],
            claim_order_hash=SEAL["claim_order_hash"],
            population_hash=SEAL["population_hash"],
            block_assignment_hash=SEAL["block_assignment_hash"],
            k=840, population=118849, blocks=5010,
            grammar="SEQUENCE_GRAMMAR_V1",
            weights="w_sb = treated_n_sb / total treated, identical to V1",
            note="nothing about which episodes are comparable to which changes. Only the "
                 "function that turns a block contrast into a number changes."),

        effect_size=dict(
            statistic="theta_s = sum_b w_sb [ median(Y|S=1,b) - median(Y|S=0,b) ]",
            units="MFE_10D percentage points",
            outcome="mfe_10d, entry NEXT_SESSION_OPEN_V1",
            role="ECONOMIC MAGNITUDE ONLY — never a promotion criterion",
            unchanged_from_v1=True),

        selection_statistic=dict(
            block_score="U_sb = [ #(Y_T > Y_C) + 0.5 #(Y_T = Y_C) ] / (n_t n_c)",
            centred="R_sb = U_sb - 1/2",
            claim="R_s = sum_b w_sb R_sb",
            null_moments="E_0[U_sb] = 1/2 exactly; "
                         "Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c), "
                         "N = n_t + n_c, tie groups t from the block's own Y multiset",
            variance="V_s = sum_b w_sb^2 Var_0(U_sb) — blocks independent under within-block "
                     "permutation",
            standardized="Z_rank_s = R_s / sqrt(V_s)",
            denominator_rule="REGISTERED ONCE from the frozen population. Never re-estimated "
                             "per delta, per world, or from an injected vector — all deltas "
                             "must share one calibrated ruler.",
            moments_verified="exact against exhaustive enumeration, with and without ties"),

        direction="one-sided positive",

        multiplicity=dict(
            statistic="max_s Z_rank_s over all k=840 claims",
            mapping="one permutation mapping applied to all 840 claims simultaneously, so the "
                    "dependence among claims is carried by the null rather than modelled",
            band="p95 of the search-wide max-null"),

        promotion_rule=dict(
            rule="observed Z_rank_s > registered p95( max-null Z_rank )",
            theta_cannot_rescue="a large raw theta on a claim whose Z_rank does not clear the "
                                "band is NOT an alternative route to promotion",
            no_second_chance="no per-family, per-support or step-down variant may be "
                             "substituted after seeing the result"),

        injection_semantics=dict(
            rule="delta is injected on the RAW outcome, never on U or R",
            path="base null world -> Y_delta = Y + delta * needle_membership -> recompute "
                 "within-block midranks -> Z_rank[840]",
            why="adding an artificial effect directly to the bounded score would measure "
                "sensitivity to rank displacement, not to an MFE effect in percentage points, "
                "and the delta grid would stop meaning pp",
            band_per_delta="the injection enters the inner-null world, so each delta carries "
                           "its own band — identical to frozen V1 capability semantics"),

        reporting_contract=dict(
            selection_evidence="Z_rank with search-wide maxT-adjusted status",
            economic_magnitude="raw theta in MFE pp",
            uncertainty_on_theta="descriptive bootstrap interval, EXPLICITLY NOT a "
                                 "selection-adjusted 95% interval. A claim chosen by the rank "
                                 "statistic has no post-selection coverage guarantee unless a "
                                 "simultaneous or selective interval is separately built.",
            secondary="MFE_ATR_10D, characterization only",
            two_columns="evidence_rank (by Z_rank) and effect_size (theta pp) are reported as "
                        "SEPARATE columns. Silent substitution of one for the other is a "
                        "reporting defect."),

        supporting_diagnostics=dict(
            variance_decomposition=dict(
                reconstruction_R2=VARD["reconstruction_R2_log"],
                finding="block tail scale separates raw-max dominators; weight and arm "
                        "geometry do not"),
            raw_shape=dict(max_Z_p95=SHAPE["max_Z"]["observed_p95"],
                           gaussianized_p95=SHAPE["max_Z"]["gaussianized_p95"],
                           median_excess_kurtosis=SHAPE["marginal_shape"]["excess_kurtosis"]["p50"]),
            rank_shape=dict(max_Z_p95=RANK["max_Z"]["observed_p95"],
                            gaussianized_p95=RANK["max_Z"]["gaussianized_p95"],
                            median_excess_kurtosis=RANK["excess_kurtosis"]["p50"],
                            sd_Z_p50=RANK["sd_Z"]["p50"]),
            status="NULL GEOMETRY ONLY — these qualify the instrument, not its power"),

        needles=PAIRED["what_is_inherited_unchanged"]["needles"],
        delta_grid_pp=[0.5, 1.0, 2.0, 3.0, 4.0, 5.0],
        delta_grid_rationale="identical to V1 so V1 and V2 sensitivity are directly comparable",

        forbidden_claims=[
            "the rank statistic is more powerful (a lower band is not more power; the "
            "numerator changed too)",
            "V1's estimand was wrong (only its decision statistic failed)",
            "T5 sequence miner is well powered"],

        y_status="HISTORICAL RANKING NOT COMPUTED, NOT EXPOSED",
        next_gate="RANK_CAPABILITY_ENGINE_QUALIFICATION — equivalence and cost only, no "
                  "capability statistic produced")

    body["spec_digest"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(body, open(OUT, "w"), indent=2, ensure_ascii=False)

    print(f"SEQUENCE_INFERENCE_V1  → RETIRED FOR PROMOTION")
    print(f"  retained for effect-size reporting; estimand NOT invalidated")
    print(f"SEQUENCE_INFERENCE_V2  {body['spec_digest']}  FROZEN_SEMANTICS")
    print(f"  inherits  seal {SEAL['seal_digest']} · k=840 · population 118,849 · blocks 5,010")
    print(f"  selection Z_rank = R_s / sqrt(V_s), V_s registered once")
    print(f"  effect    theta in MFE pp, unchanged")
    print(f"  needles   " + " · ".join(
        f"{k} {v['claim']} n={v['treated_overlap']}"
        for k, v in body["needles"].items()))
    print(f"\n  WROTE {OUT}\n        {RET}")


if __name__ == "__main__":
    main()
