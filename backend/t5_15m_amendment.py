"""T5_15M_INFERENCE_AMENDMENT_V2 — swap the 15m decision layer, touch nothing else.

WHY NOW AND NOT AFTER THE 1H RESULT

The 15m spec was frozen under V1 semantics, whose decision statistic has since been rejected
on instrument grounds — before any historical outcome was read. If the amendment waited until
the 1H historical ranking were visible, a fully legitimate correction would carry the shadow
of post-exposure adaptation. It is made now so its trigger is unambiguous and checkable:

    triggered by   pre-exposure failure of the V1 detector (T5_CAPABILITY_Q50_RESULT_V1)
                   and successful pre-exposure qualification of V2 (T5_CAPABILITY_RANK_V2)
    NOT triggered by any observed 1H or 15m historical ranking, which do not exist yet

THIS IS NOT A REDESIGN

Window, position anchoring, tokens, lengths, support floors, population rules, estimand,
blocks, multiplicity family and the forbidden-pattern clause all stand exactly as frozen at
2871358f25d62f47. Only the layer that converts a block contrast into a promotion decision
changes. The 15m study remains un-enumerated with no Y access.

THE 15M FAMILY IS STILL SEPARATE

The amendment does not pool 15m with 1H, does not let a 15m result confirm a 1H claim, and
does not relax the forbidden pattern. Its own capability must be qualified before it runs;
the 1H rank capability qualifies the 1H needles only.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

OUT = os.path.join(HERE, "T5_15M_INFERENCE_AMENDMENT_V2.json")
BASE = json.load(open("T5_15M_OPENING_HOUR_SPEC_V1.json"))
V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))
CAP = json.load(open("T5_CAPABILITY_RANK_V2.json"))
Q50 = json.load(open("T5_CAPABILITY_Q50_RESULT_V1.json"))


def main():
    if BASE["spec_digest"] != "2871358f25d62f47":
        raise RuntimeError(f"15m base spec digest drift: {BASE['spec_digest']}")

    body = dict(
        spec_id="T5_15M_INFERENCE_AMENDMENT_V2",
        status="FROZEN — inferential layer only. 15m remains UN-ENUMERATED with NO Y access.",
        amends="T5_15M_OPENING_HOUR_V1",
        amends_digest=BASE["spec_digest"],
        reason="PRE-HISTORICAL-EXPOSURE INSTRUMENT QUALIFICATION",
        trigger="Amendment triggered by pre-exposure failure of the V1 detector and "
                "successful pre-exposure qualification of V2; NOT by any observed 1H or 15m "
                "historical ranking.",
        trigger_evidence=dict(
            v1_failure=dict(artifact="T5_CAPABILITY_Q50_RESULT_V1",
                            surface=Q50["surface"],
                            reading="median-support needle, +0.5..+5.0pp injections, 0/20 at "
                                    "every delta"),
            v2_qualification=dict(artifact="T5_CAPABILITY_RANK_V2",
                                  digest=CAP["spec_digest"],
                                  inference_digest=V2["spec_digest"])),

        supersedes="raw-theta unstudentized maxT decision layer",

        unchanged=dict(
            window=BASE["window"], position_anchored=BASE["position_anchored"],
            sequences=BASE["sequences"], adjacency=BASE["adjacency"], element=BASE["element"],
            occurrence_unit=BASE["occurrence_unit"], support=BASE["support"],
            outcome=BASE["outcome"], estimand_comparator=BASE["estimand"],
            multiplicity_family=BASE["multiplicity"],
            forbidden_pattern=BASE["forbidden_pattern"],
            depends_on=BASE["depends_on"], source=BASE["source"],
            note="tokens, lengths, support floors, population rules, blocks and the "
                 "separate-family multiplicity role all stand exactly as frozen"),

        changed=dict(
            selection_statistic=dict(
                block_score="U_sb = [ #(Y_T > Y_C) + 0.5 #(Y_T = Y_C) ] / (n_t n_c)",
                centred="R_sb = U_sb - 1/2",
                claim="R_s = sum_b w_sb R_sb",
                variance="V_s = sum_b w_sb^2 Var_0(U_sb), "
                         "Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c)",
                standardized="Z_rank_s = R_s / sqrt(V_s)",
                denominator_rule="registered once from the frozen 15m population; never "
                                 "re-estimated per delta or from an injected vector"),
            multiplicity_statistic="max_s Z_rank_s over the 15m family's own k, with one "
                                   "permutation mapping applied to all claims",
            promotion_rule="observed Z_rank_s > registered p95( max-null Z_rank ), one-sided "
                           "positive; a large raw theta cannot substitute",
            effect_size_reporting="raw theta in MFE_10D percentage points, reported as a "
                                  "SEPARATE column from evidence_rank; never interchanged"),

        still_required_before_15m_runs=[
            "the 15m family's OWN rank capability qualification — the 1H needles qualify the "
            "1H family only, and the 15m population, blocks and k all differ",
            "1H primary result sealed, per the unchanged y_exposure clause"],

        y_exposure=BASE["y_exposure"],
        y_status_15m="NOT ENUMERATED, NO Y ACCESS",
        y_status_1h="HISTORICAL RANKING NOT COMPUTED, NOT EXPOSED at the time of this "
                    "amendment")

    body["amendment_digest"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(body, open(OUT, "w"), indent=2, ensure_ascii=False)

    print("T5_15M_INFERENCE_AMENDMENT_V2  " + body["amendment_digest"])
    print(f"  amends  T5_15M_OPENING_HOUR_V1  {BASE['spec_digest']}  (digest verified)")
    print("  changed selection statistic + multiplicity statistic + promotion rule")
    print("  UNCHANGED window · anchoring · tokens · lengths · support · population · "
          "estimand · family role · forbidden pattern")
    print("  15m still NOT ENUMERATED · NO Y ACCESS")
    print("  1H historical ranking NOT COMPUTED at the time of this amendment")
    print(f"\n  WROTE {OUT}")


if __name__ == "__main__":
    main()
