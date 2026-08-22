"""T15M_ELIGIBILITY_RULING_V1 — adopt the frozen predicate surface, and the census behind it.

The ruling is a reconciliation of WHERE an already-registered predicate is evaluated, not a
change to what is searched. Grammar, token set and position families are untouched; the
generated family size stays in provenance and never disappears.

    k_generated_membership_distinct   the whole search grammar burden
    k_final_inferential               the claims a future statistic could actually inspect
                                      or select — the k that family-wise max-Z uses

Two operations that must never be conflated, and are reported separately per family:

    membership deduplication   S0-distinct memberships that coincide once restricted
    eligibility filtering      classes the registered floors reject on the final surface

The funnel is renamed. FULL_M1_M4_COMPLETE, ANY_REGISTERED_PAIR_ANALYZABLE and
ESTIMAND_POPULATION are different AXES, not nested stages of one pipeline, and calling them
a funnel invites exactly the reading that made the numbers look non-monotone. It is a
population census.

Failure reasons OVERLAP — a claim can fail the treated floor and the date-share floor at
once — so all_reasons is a non-exclusive tally that must not be summed, and primary_reason
is assigned by the registered evaluation order.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import importlib, json, math, os, sys, time                            # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

OUT = "T15M_ELIGIBILITY_RULING_V1.json"
FAMS = ("T9", "T3", "T1")
REASON_ORDER = ["TREATED_FLOOR", "CONTROL_FLOOR", "TICKER_FLOOR", "DATE_FLOOR", "DATE_SHARE"]


def census(fam: str) -> dict:
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    C = pd.read_parquet(os.path.join(D.ROOT, "data", f"{fam}_15m_k_closure.parquet"))
    KC = json.load(open(f"{F}_15M_K_CLOSURE_V1.json"))
    S0 = C[C.s0_ok]
    dropped = S0[~S0.s2_ok]

    all_reasons = {r: int(dropped.s2_fail.str.split(",").apply(lambda xs: r in xs).sum())
                   for r in REASON_ORDER}
    primary = dropped.s2_fail.str.split(",").apply(
        lambda xs: next(r for r in REASON_ORDER if r in xs))
    cls_ok = S0.groupby("s2_hash").s2_ok
    return dict(
        k_generated_membership_distinct=int(S0.s2_hash.nunique()),
        k_final_inferential=int(S0[S0.s2_ok].s2_hash.nunique()),
        support_qualified_syntactic_names=int(len(S0)),
        aliases=int(len(S0) - S0.s2_hash.nunique()),
        s0_distinct_memberships=KC["restriction_accounting"]["s0_distinct_memberships"],
        restriction_merges=KC["restriction_accounting"]["restriction_merges"],
        restriction_splits=KC["restriction_accounting"]["restriction_splits"],
        whole_classes_removed=int((~cls_ok.any()).sum()),
        partial_class_removals=int((cls_ok.any() & ~cls_ok.all()).sum()),
        names_dropped=int(len(dropped)),
        names_admitted_only_on_final_surface=int((C.s2_ok & ~C.s0_ok).sum()),
        all_reasons=all_reasons,
        primary_reason={str(k): int(v) for k, v in primary.value_counts().items()},
        reason_arithmetic="all_reasons is NON-EXCLUSIVE and must not be summed — a claim "
                          "can fail several floors at once. primary_reason IS exclusive and "
                          "sums to names_dropped; it is assigned by the registered "
                          f"evaluation order {REASON_ORDER}",
        two_operations=dict(
            membership_deduplication=f"{KC['restriction_accounting']['s0_distinct_memberships']}"
                                     f" -> {S0.s2_hash.nunique()}",
            eligibility_filtering=f"{S0.s2_hash.nunique()} -> {S0[S0.s2_ok].s2_hash.nunique()}",
            note="deduplication is not filtering; they are reported separately and never "
                 "netted against each other"))


def amend_closure(fam: str, cen: dict) -> str:
    F = fam.upper()
    d = json.load(open(f"{F}_15M_K_CLOSURE_V1.json"))
    d["population_census"] = dict(d.pop("funnel"))
    d["population_census"]["naming"] = (
        "renamed from 'funnel'. FULL_M1_M4_COMPLETE, ANY_REGISTERED_PAIR_ANALYZABLE and "
        "ESTIMAND_POPULATION are different AXES over the same episodes, not nested stages "
        "of one pipeline; 'funnel' invited a monotonicity that was never claimed")
    d["reason_census"] = cen
    d["ruling"] = "ADOPT_FROZEN_PREDICATE_SURFACE — see T15M_ELIGIBILITY_RULING_V1"
    d["k_generated_membership_distinct"] = cen["k_generated_membership_distinct"]
    d["k_final_inferential"] = cen["k_final_inferential"]
    d["amended_at"] = time.strftime("%Y-%m-%d %H:%M %Z")
    return ART.seal(d, f"{F}_15M_K_CLOSURE_V1.json",
                    required=("spec_id", "surfaces", "population_census", "reason_census"),
                    supersede=True)


def main():
    cens = {f: census(f.lower()) for f in FAMS}
    for f in FAMS:
        c = cens[f]
        print(f"{f}: generated {c['k_generated_membership_distinct']:,} -> final "
              f"{c['k_final_inferential']:,} · merges {c['restriction_merges']} · splits "
              f"{c['restriction_splits']} · whole removed {c['whole_classes_removed']} · "
              f"partial {c['partial_class_removals']}")
        print(f"      primary reason {c['primary_reason']} · all (non-exclusive) "
              f"{ {k: v for k, v in c['all_reasons'].items() if v} }")
        amend = amend_closure(f.lower(), c)
        print(f"      {f}_15M_K_CLOSURE_V1 (amended) · {amend}")

    k = {f: cens[f]["k_final_inferential"] for f in FAMS}
    needles = {f: {f"q{int(q*100)}": math.ceil(q * k[f]) - 1 for q in (.1, .5, .9)}
               for f in FAMS}
    d = ART.seal(dict(
        spec_id="T15M_ELIGIBILITY_RULING_V1", status="FROZEN",
        ruling="ADOPT_FROZEN_PREDICATE_SURFACE",
        eligibility_surface="FINAL_ESTIMAND_POPULATION",
        registered_predicate=["support_eligible(t_ov, c_ov)", "ticker floor",
                              "date floor", "max-date-share floor"],
        predicate_source="t9/t3/t1_sequence_estimand — the same predicate these three "
                         "families are already sealed under at 1H, evaluated on the same "
                         "overlap-restricted surface",
        grammar="UNCHANGED", token_set="UNCHANGED", position_families="UNCHANGED",
        outcome_exposure="NONE",
        rationale="implementation reconciliation to the already registered eligibility "
                  "semantics; NOT a multiplicity-driven narrowing. The generated runner "
                  "measured support on the broader opening-hour surface and never evaluated "
                  "the registered control floor at all, so retaining it would have kept "
                  "claims that cannot pass eligibility on the very population the "
                  "registered estimand and inference live on.",
        k_semantics=dict(
            k_generated_membership_distinct="the whole search grammar burden; stays in "
                                            "provenance and is never dropped",
            k_final_inferential="the claims a future statistic could actually inspect or "
                                "select — the k family-wise max-Z uses",
            which_k_is_used="k = k_final_inferential"),
        families={f: dict(
            k_generated_membership_distinct=cens[f]["k_generated_membership_distinct"],
            k_final_inferential=cens[f]["k_final_inferential"],
            needles_zero_based=needles[f],
            k_closure=ART.file_digest(f"{f}_15M_K_CLOSURE_V1.json"),
            x_freeze=ART.file_digest(f"{f}_15M_X_ONLY_V1.json")) for f in FAMS},
        claim_order_rule=dict(
            order_by="t_ov ASCENDING — the FINAL estimand surface treated support",
            tie_break="the sealed deterministic claim/j order",
            forbidden="ordering by the broader S0 opening-hour support; needle quantiles "
                      "and eligibility must stand on ONE support definition or the "
                      "semantic split reopens",
            needle_rule="nearest rank, i_q = ceil(q*k) - 1, zero-based"),
        reason_census_note="each family's machine-readable census lives in its "
                           "{F}_15M_K_CLOSURE_V1 under reason_census; all_reasons is "
                           "non-exclusive and must not be summed",
        sequencing="family by family — T9 fully closed to capability, then T3, then T1. "
                   "Shared code may be ready for all three; the evidentiary run is not.",
        historical_y="CLOSED for all three families",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        OUT, required=("spec_id", "ruling", "eligibility_surface", "families"),
        supersede=os.path.exists(OUT))
    print(f"\nT15M_ELIGIBILITY_RULING_V1 · {d}")
    for f in FAMS:
        print(f"  {f}  k = {k[f]:,}  needles "
              + " / ".join(f"{v:,}" for v in needles[f].values()))


if __name__ == "__main__":
    main()
