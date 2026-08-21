"""T5_15M_SEARCH_SCOPE_V2 — reduce the primary 15m search to 2-bar. Freeze only.

WHAT THIS IS NOT

It is not a support-floor change. Raising 300 until the surviving count looks manageable
would be a threshold fitted to the observed X distribution — outcome-free, but still tuned to
the data it is applied to. The floors stay exactly as frozen.

It is not a verdict on 3-bar composition. Those families are DEFERRED_FROM_PRIMARY_INFERENCE
on feasibility grounds and remain in the record with their measured counts.

It is not hierarchical 2->3 testing. Screening 3-bar claims only under surviving 2-bar parents
would buy the compute back at the price of a strong-heredity assumption: that a real
`A->B->C` effect must show a detectable `A->B` parent. That is not guaranteed — `A->B` can be
null while `A->B->C` shifts the distribution — and it is not an assumption worth importing
just to save compute.

WHAT IT IS

A structural complexity rule. The primary localization question the 15m study exists to ask —
in which quarter-hour of the opening hour does the change occur — is answered by the direct
transition. Higher-order 3-bar composition becomes a separate future study with its own
design.

    syntactic support-qualified   1,084,896  ->  32,450     (~33x)
    of which 3-bar                1,052,446  =  97.0%

k_15m_2bar_final is NOT 32,450 and must not be quoted as such. It is produced only after the
same estimand population, overlap restriction and exact re-equivalence that produced the full
number.

PROVENANCE, STATED PRECISELY AND PERMANENTLY

The 15m grammar and positional design predated 1H outcome exposure. The final inferential
scope was reduced POST-1H-exposure but PRE-15m-Y, solely on documented 15m X-only
computational and multiplicity feasibility grounds. Any future write-up must say that, not
the shorter and false "the 15m test was fully specified before the 1H result".
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

OUT = os.path.join(HERE, "T5_15M_SEARCH_SCOPE_V2.json")
BASE = json.load(open("T5_15M_OPENING_HOUR_SPEC_V1.json"))
AMEND = json.load(open("T5_15M_INFERENCE_AMENDMENT_V2.json"))
ENUM = json.load(open("T5_15M_X_ONLY_ENUMERATION.json"))


def main():
    if BASE["spec_digest"] != "2871358f25d62f47":
        raise RuntimeError("15m base spec digest drift")
    if AMEND["amendment_digest"] != "a3d0152180a7938c":
        raise RuntimeError("15m amendment digest drift")
    if not all(ENUM["gates"].values()):
        raise RuntimeError("the full enumeration's own gates did not all pass; a scope "
                           "revision may not be built on an unverified measurement")
    fam = ENUM["by_position_family"]
    two = sum(v for k, v in fam.items() if k.count("→") == 1)
    three = sum(v for k, v in fam.items() if k.count("→") == 2)

    body = dict(
        spec_id="T5_15M_SEARCH_SCOPE_V2",
        status="POST_1H_EXPOSURE · PRE_15M_Y · X-ONLY FEASIBILITY REVISION",
        supersedes_scope_of="T5_15M_OPENING_HOUR_V1",
        base_spec_digest=BASE["spec_digest"],
        amendment_digest=AMEND["amendment_digest"],

        decision_type="DOCUMENTED PRE-Y FEASIBILITY DECISION — not a formal gate. No numeric "
                      "feasibility threshold was registered in advance, so this artifact does "
                      "NOT claim that a preregistered cutoff was crossed.",
        feasibility_finding="The FULL search scope was judged operationally infeasible after "
                            "X-only enumeration because the realized hypothesis space implied "
                            "prohibitive memory, permutation and capability cost relative to "
                            "the qualified compute architecture.",
        trigger=dict(
            reason="the frozen enumeration produced ~1.03M membership-distinct final claims, "
                   "~97% of them arising from the 3-bar grammar",
            evidence=dict(source="T5_15M_X_ONLY_ENUMERATION",
                          syntactic_support_qualified=ENUM["syntactic_claims"],
                          two_bar=two, three_bar=three,
                          three_bar_share=round(three / (two + three), 4),
                          k_15m_provisional=ENUM["k_15m_provisional"],
                          k_15m_final=ENUM["k_15m_final"],
                          class_digest=ENUM["class_digest"]),
            not_a_trigger=["1H survivor identities", "1H ranking", "1H theta magnitude",
                           "1H motif clusters", "any 15m outcome"]),

        primary_search=dict(
            families=["M1→M2", "M2→M3", "M3→M4"],
            rationale="the primary localization question — in which quarter-hour of the "
                      "opening hour does the change occur — is answered by the direct "
                      "position-anchored transition"),

        deferred=dict(
            families=["M1→M2→M3", "M2→M3→M4"],
            status="DEFERRED_FROM_PRIMARY_INFERENCE",
            reason="combinatorial / compute feasibility",
            explicitly_not="deleted, judged, or found wanting on evidence",
            no_y="no outcome value and no descriptive outcome ranking may be produced for "
                 "these families under this spec",
            future_paths=["a separate 3-bar research family on a different period or dataset",
                          "a pre-specified smaller token vocabulary justified on domain "
                          "grounds — never derived from 1H winners",
                          "an inference architecture built for ~1M-hypothesis search"]),

        unchanged=dict(
            support_floors=BASE["support"], tokens="unchanged", positions="unchanged",
            window=BASE["window"], estimand=BASE["estimand"],
            occurrence_unit=BASE["occurrence_unit"], adjacency=BASE["adjacency"],
            selection_statistic="per T5_15M_INFERENCE_AMENDMENT_V2 — bounded blockwise "
                                "Mann-Whitney, analytic null variance, search-wide max Z_rank",
            multiplicity_family=BASE["multiplicity"],
            note="no threshold was fitted to the observed X distribution"),

        rejected_alternatives=dict(
            raise_support_floor="would fit a threshold to the observed X distribution to reach "
                                "a convenient k; outcome-free but still data-tuned",
            hierarchical_2_then_3="would assume strong heredity — that a real 3-bar effect "
                                  "must have a detectable 2-bar parent. A->B can be null "
                                  "while A->B->C shifts the distribution."),

        may_not_use=["1H survivor identity", "1H ranking", "1H theta magnitude",
                     "1H motif clusters"],

        provenance_statement="The 15m grammar and positional design predated 1H outcome "
                             "exposure. The final inferential scope was reduced "
                             "post-1H-exposure but pre-15m-Y, solely on documented 15m X-only "
                             "computational and multiplicity feasibility grounds.",
        forbidden_phrasing="the 15m test was fully specified before the 1H result",

        mobility_rule="mobility is recomputed on the 2-bar-only final population and reported. "
                      "No mobility floor is invented now; none was registered in advance, so "
                      "min retention below 0.90 is reported, not failed.",

        next_steps=["recompute the 2-bar-only funnel, exact re-equivalence, k_15m_2bar_final, "
                    "mobility and gates",
                    "STOP",
                    "then: sealed claim order, q10/q50/q90 needles chosen by the frozen "
                    "support rule from THAT order, 15m's own rank capability qualification"],
        y_status="NO 15m Y ACCESS")

    body["scope_digest"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(body, open(OUT, "w"), indent=2, ensure_ascii=False)

    print(f"T5_15M_SEARCH_SCOPE_V2  {body['scope_digest']}")
    print(f"  trigger  {ENUM['k_15m_final']:,} final classes · 3-bar share "
          f"{three/(two+three):.1%} · enumeration gates all PASS")
    print(f"  primary  M1→M2 · M2→M3 · M3→M4          syntactic {two:,}")
    print(f"  deferred M1→M2→M3 · M2→M3→M4           syntactic {three:,}  "
          f"DEFERRED_FROM_PRIMARY_INFERENCE")
    print(f"  unchanged support floors · tokens · positions · window · estimand")
    print(f"  rejected raise-the-floor (data-tuned) · hierarchical 2→3 (strong heredity)")
    print(f"\n  WROTE {OUT}")


if __name__ == "__main__":
    main()
