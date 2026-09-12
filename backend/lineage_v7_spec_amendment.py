"""MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1 — two repair classes, and one invariant that must change.

The amendment itself is mechanical: bind the eleven evidence-qualified regular-way starts,
bind the three when-issued boundary corrections, keep them in separate taxonomies, and
reconcile the session counts by recomputing them from the per-security boundaries rather than
copying 345 and 3 forward.

BUT WRITING IT SURFACED A PROBLEM THAT WOULD HAVE BROKEN CANDIDATE V2, and it is worth
stating plainly because it is not in the gate's instructions.

The three WI corrections move each security's regular-way start ONTO the session its
when-issued line was trading:

    GEHC   GEHCV when-issued 2023-01-03..01-04   regular-way now starts 2023-01-04
    SOLV   SOLVw when-issued 2024-04-01          regular-way now starts 2024-04-01
    VLTO   VLTOw when-issued 2023-10-02          regular-way now starts 2023-10-02

Both facts are true at once — the when-issued LINE and the regular-way LINE traded on the
same session under different tickers. So after this amendment each of those securities has
two ticker intervals that overlap in time.

The V7 spec's interval invariant says intervals within (security_key_v1, attribute) must not
overlap. Applied as written, candidate V2 would flag all three as defects and fail its own
validation — not because the lineage is wrong, but because the invariant was written before
a case existed where one security legitimately had two simultaneous trading lines.

So the invariant is amended: for ticker_intervals, non-overlap is scoped WITHIN a
lineage_state. WHEN_ISSUED and REGULAR_WAY may coexist on a session. Across any single
lineage_state the old rule stands unchanged.

This is the narrowest change that admits the evidence, and it is deliberately not a loosening
of the when-issued policy itself: the WI line stays excluded from canonical coverage exactly
as before. What changes is only that its existence no longer forces the regular-way line to
start a session later.

THE 348 NEW SESSIONS ARE COVERAGE, NOT EVIDENCE. The anomaly audit saw bars through
diagnostic requests. That says the vendor serves them today; it says nothing about whether
original-vintage canonical raw exists. RAW_CANONICAL_COVERAGE_STATUS is recorded as
TO_BE_VERIFIED and the question is pushed to its own gate, because answering it here by
assuming would repeat exactly the mistake this programme has spent eight gates avoiding.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

BASE = "MASSIVE_SECURITY_LINEAGE_V7_SPEC.json"
BASE_DIGEST = "f858ee0f3be05cc7"
QUAL = "MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1.json"
QUAL_DIGEST = "e49f0925ed37e025"
CAND = "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json"
CAND_DIGEST = "e454381d12c66441"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
BOUND = "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1.json"
EIGHT = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]


def main():
    binding = {}
    for f, want in ((BASE, BASE_DIGEST), (QUAL, QUAL_DIGEST), (CAND, CAND_DIGEST)):
        got = ART.file_digest(f)
        binding[f.replace(".json", "")] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            print(f"HOLD — {f} digest {got} != {want}")
            return 1

    q = json.load(open(QUAL))
    v6 = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    aud = json.load(open(BOUND))
    wi_v6 = {x["ticker"]: x for x in json.load(open(LINEAGE))["when_issued_intervals"]}

    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")

    def n_between(a, b):
        """sessions in [a, b) — recomputed, never copied from the qualification artifact."""
        return len(cal.sessions_in_range(pd.Timestamp(a), pd.Timestamp(b))) - 1

    A_rows, B_rows, a_total, b_total = [], [], 0, 0
    for c in q["candidates"]:
        tk = c["ticker"]
        n = n_between(c["authoritative_regular_way_start"], c["v6_regular_way_start"])
        row = dict(
            security_key_v1=c["security_key_v1"], ticker=tk,
            classification=c["classification"],
            v6_regular_way_start=c["v6_regular_way_start"],
            v7_regular_way_start=c["authoritative_regular_way_start"],
            boundary_precision=c["boundary_precision"],
            listing_mechanism=c["listing_mechanism"],
            primary_evidence=c["primary_evidence"],
            quoted_statements=c["quoted_statements"],
            restored_sessions_recomputed=n,
            restored_sessions_as_qualified=c["proposed_lost_sessions"],
            reconciles=n == c["proposed_lost_sessions"],
            massive_agreement_role="CROSS_CHECK_ONLY",
            precision_not_upgraded=True)
        if c["candidate_class"] == "ANCHOR_DELAY":
            row["v6_start_on_anchor_grid"] = c["v6_start_on_anchor_grid"]
            row["repair_rule"] = ("use the evidence-qualified REGULAR-WAY start; NOT an "
                                  "anchor shift, NOT the earliest Massive bar")
            row["limitations"] = c.get("limitations", [])
            A_rows.append(row); a_total += n
        else:
            row["v6_when_issued_interval"] = c["v6_when_issued_interval"]
            row["v6_when_issued_ticker"] = c["v6_when_issued_ticker"]
            row["correction_class"] = "WI_TEMPORAL_APPLICATION"
            row["policy_unchanged"] = True
            B_rows.append(row); b_total += n

    if not all(r["reconciles"] for r in A_rows + B_rows):
        print("HOLD — per-security reconciliation failed")
        return 1
    if a_total != 345 or b_total != 3:
        print(f"HOLD — recomputed A={a_total} B={b_total}; expected 345 / 3. "
              f"Not forcing the expected total.")
        return 1

    eight_total = sum(r["vendor_aggregate_access"]["sessions_with_bars"]
                      for r in aud["securities"])

    # status invariant: the WI ticker intervals survive, so the counts must not move
    ex = json.load(open(CAND))
    status_now = dict(SINGLE_TICKER_WHOLE_WINDOW=0, LISTED_LATER=0, RENAMED_IN_WINDOW=0)
    canon_now = dict(status_now)
    for s in v6.values():
        status_now[s["status"]] += 1
        canon_now[s["canonical_status"]] += 1
    status_ok = (status_now == dict(SINGLE_TICKER_WHOLE_WINDOW=462, LISTED_LATER=19,
                                    RENAMED_IN_WINDOW=22)
                 and canon_now == dict(SINGLE_TICKER_WHOLE_WINDOW=462, LISTED_LATER=23,
                                       RENAMED_IN_WINDOW=18))

    overlap_cases = [dict(ticker=r["ticker"], when_issued_ticker=r["v6_when_issued_ticker"],
                          when_issued_sessions=r["v6_when_issued_interval"],
                          regular_way_start=r["v7_regular_way_start"],
                          overlapping_session=r["v7_regular_way_start"])
                     for r in B_rows]

    p = dict(
        spec_id="MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1",
        status="MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1_FROZEN",
        task_class="SPECIFICATION_AMENDMENT_ONLY",
        amends=dict(base_spec="MASSIVE_SECURITY_LINEAGE_V7_SPEC",
                    base_spec_digest=BASE_DIGEST,
                    base_overwritten=False,
                    supersedes_only="the affected V7 coverage rules and one interval "
                                    "invariant; every other base rule stands unchanged"),
        authoritative_inputs=dict(binding=binding, all_verified=True,
                                  qualification_artifact=QUAL_DIGEST,
                                  candidate_audit_artifact=CAND_DIGEST),

        repair_class_taxonomy=dict(
            classes=["SECURITY_LEVEL_COVERAGE_REPAIR", "WI_BOUNDARY_CORRECTION"],
            binding="these are DIFFERENT defect classes and must never share a repair rule",
            security_level_coverage_repair=dict(
                original_known_eight=EIGHT,
                new_branch_a=[r["ticker"] for r in A_rows],
                total=len(EIGHT) + len(A_rows)),
            wi_boundary_correction=dict(securities=[r["ticker"] for r in B_rows],
                                        total=len(B_rows)),
            unique_securities_changed_vs_v6=len(EIGHT) + len(A_rows) + len(B_rows),
            do_not_report="repair_set = 22 without preserving the two semantic classes"),

        branch_a_repairs=dict(
            count=len(A_rows), classification="V6_ANCHOR_DELAY_CONFIRMED",
            rule="use the evidence-qualified REGULAR-WAY start from the qualification "
                 "artifact, per security, with its own precision",
            forbidden_rules=['"move an anchor backward"',
                             '"earliest Massive bar = start"',
                             '"if V6 start is on the quarterly grid, use the previous '
                             'session"'],
            precision_preserved_verbatim=True,
            bounded_not_upgraded=[r["ticker"] for r in A_rows
                                  if "BOUNDED" in r["boundary_precision"]],
            massive_agreement="CROSS_CHECK_ONLY — never grounds to upgrade precision",
            restored_sessions_recomputed=a_total,
            restored_sessions_expected=345,
            reconciled=a_total == 345,
            reconciliation_method="recomputed per security from XNYS sessions in "
                                  "[v7_start, v6_start); the expected total was NOT forced",
            rows=A_rows),

        branch_b_corrections=dict(
            count=len(B_rows),
            classification="REGULAR_WAY_STARTED_ON_WI_DATE_CONFIRMED",
            nature="corrects the TEMPORAL APPLICATION of the frozen WI policy, not the "
                   "policy",
            what_was_wrong="V6's when-issued interval extended one session too far and "
                           "consumed the first authoritatively confirmed REGULAR_WAY "
                           "session",
            bound_starts={r["ticker"]: r["v7_regular_way_start"] for r in B_rows},
            restored_sessions_recomputed=b_total, restored_sessions_expected=3,
            reconciled=b_total == 3,
            no_generalisation="no additional WI dates are inferred from these three",
            rows=B_rows),

        wi_policy_invariant=dict(
            unchanged=True,
            statement="WHEN_ISSUED intervals remain EXCLUDED from canonical regular-way "
                      "research coverage",
            explicitly_not_adopted='"an ordinary-ticker bar during a WI period means '
                                   'regular-way"',
            why_these_three_are_accepted="authoritative filed evidence states regular-way "
                                         "trading began on those dates; each was qualified "
                                         "individually",
            globally_loosened=False),

        interval_invariant_amendment=dict(
            severity="REQUIRED — candidate V2 would fail its own validation without it",
            discovered="while writing this amendment; not present in the gate instructions",
            problem="after these corrections each of GEHC, SOLV and VLTO has a WHEN_ISSUED "
                    "ticker interval and a REGULAR_WAY ticker interval that overlap on one "
                    "session, because the when-issued LINE and the regular-way LINE really "
                    "did both trade that day under different tickers",
            base_rule="intervals within (security_key_v1, attribute) must not overlap",
            amended_rule="for ticker_intervals, non-overlap is scoped WITHIN a "
                         "lineage_state; WHEN_ISSUED and REGULAR_WAY may coexist on a "
                         "session. Within any single lineage_state the base rule is "
                         "unchanged.",
            scope="ticker_intervals only; all other interval datasets keep the base rule",
            not_a_loosening_of="the when-issued exclusion policy — the WI line stays out of "
                               "canonical coverage exactly as before; it simply no longer "
                               "forces the regular-way line to start a session later",
            affected_cases=overlap_cases),

        gev_control=dict(
            security="GEV", disposition="UNCHANGED",
            v6_regular_way_start="2024-04-02", remains_correct=True,
            classification="KNOWN_NON_ANOMALY / DESCRIPTIVE_NEGATIVE_CONTROL",
            forbidden="modifying GEV to match GEHC/SOLV/VLTO",
            why="GE Vernova's distribution completed 2024-04-02, so its comparable WI "
                "session genuinely had no regular-way trading"),

        structural_vs_canonical_status=dict(
            preserved=True,
            structural=status_now, canonical=canon_now,
            expected_structural=dict(SINGLE_TICKER_WHOLE_WINDOW=462, LISTED_LATER=19,
                                     RENAMED_IN_WINDOW=22),
            expected_canonical=dict(SINGLE_TICKER_WHOLE_WINDOW=462, LISTED_LATER=23,
                                    RENAMED_IN_WINDOW=18),
            unchanged_by_this_amendment=status_ok,
            why="the WI ticker intervals survive the correction, so nothing that produced "
                "the divergence has moved; only the regular-way START shifts",
            four_intentional_differences=["GEHC", "GEV", "SOLV", "VLTO"],
            forbidden="automatically reconciling 19 vs 23 or 22 vs 18"),

        restoration_accounting=dict(
            original_known_eight=eight_total,
            original_known_eight_expected=5069,
            branch_a=a_total, branch_b=b_total,
            newly_qualified_total=a_total + b_total,
            combined_expected_v7_vs_v6=eight_total + a_total + b_total,
            combined_expected_stated=5417,
            reconciled=(eight_total == 5069 and a_total + b_total == 348
                        and eight_total + a_total + b_total == 5417),
            caveat="this is the EXPECTED restoration IF an amended candidate later "
                   "implements all repairs correctly. This spec task creates no rows."),

        raw_evidence_consequence=dict(
            NEWLY_EXPECTED_COVERAGE_SESSIONS=a_total + b_total,
            RAW_CANONICAL_COVERAGE_STATUS="TO_BE_VERIFIED",
            binding="coverage and raw-evidence inventory are SEPARATE. The anomaly audit saw "
                    "bars through diagnostic requests; that says the vendor serves them "
                    "today and says nothing about original-vintage canonical raw.",
            must_be_resolved_before="the amended V7 candidate is used by the derived builder",
            per_session_question=["A. original-vintage canonical raw already exists",
                                  "B. only later/new-vintage diagnostic evidence exists",
                                  "C. no retrievable evidence exists"],
            not_solved_here=True,
            expected_next_gate="MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1",
            if_new_vintage_only="those bytes need their own quarantine preservation, exactly "
                                "as the earlier 5,069 sessions did — and the mixed-vintage "
                                "problem then extends beyond the original eight"),

        no_diagnostic_promotion=dict(
            sec_documents_archived=31,
            sec_documents_role="authoritative lineage/listing evidence",
            massive_probes_role="diagnostic source evidence",
            binding="neither promotes market bars into canonical raw. No diagnostic "
                    "aggregate response becomes canonical merely because its first session "
                    "matches the SEC-supported regular-way start.",
            promotions=0),

        candidate_versioning=dict(
            existing_candidate=CAND, existing_digest=CAND_DIGEST, immutable=True,
            historically_true=dict(CANDIDATE_IMPLEMENTATION="PASS",
                                   FULL_503_LINEAGE_AUDIT="HOLD"),
            patched=False,
            next_candidate="MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V2",
            built_from="base V7 spec + this amendment",
            from_scratch=True,
            not_rebuilt_automatically=True),

        implementation_provenance=dict(
            superseded="63d8e03cdafb257f",
            superseded_class="IMPLEMENTATION_DEFECT_NOT_A_LINEAGE_ANOMALY",
            superseded_defect="next_session() off-by-one on non-trading effective_to",
            corrected="e81cb9367d3e37eb",
            binding="the implementation bug must never be mixed with the newly qualified "
                    "lineage defects"),

        extended_negative_guards=[
            "anchor date moved backward without an evidence-qualified listing start",
            "earliest Massive bar automatically treated as the listing date",
            "BOUNDED boundary silently promoted to EXACT",
            "GEHC/SOLV/VLTO first regular-way session left inside WHEN_ISSUED_EXCLUDED",
            "WI policy globally loosened because three boundaries were wrong",
            "ordinary-ticker bar on a WI date automatically treated as regular-way",
            "GEV modified to match the other three",
            "345 Branch-A restored sessions not reconciled",
            "3 WI restored sessions not reconciled",
            "348 diagnostic sessions automatically promoted to canonical raw"],
        guards_total_after_amendment=24,

        acceptance={
            "branch_a_11_bound": len(A_rows) == 11,
            "branch_a_reconciles_345": a_total == 345,
            "branch_b_3_bound": len(B_rows) == 3,
            "branch_b_reconciles_3": b_total == 3,
            "wi_policy_unchanged": True,
            "gev_unchanged": True,
            "structural_canonical_preserved": status_ok,
            "original_eight_rules_unchanged": True,
            "original_eight_restoration_5069": eight_total == 5069,
            "combined_restoration_5417": eight_total + a_total + b_total == 5417,
            "raw_status_left_unresolved": True,
            "no_v7_rows_written": True,
            "no_candidate_mutation": True,
            "no_canonical_mutation": True},

        mutations=dict(base_spec=0, v6=0, v7_candidate=0, security_key_v1=0, universe=0,
                       canonical_raw=0, supplemental_promotions=0, derived_writes=0,
                       v7_rows_written=0, canonical_v7_sealed=False),
        canonical_v7_eligible=False,
        limitations=[
            "RDDT and PSKY boundaries are BOUNDED_BY_LISTING_CERTIFICATION and are not "
            "upgraded here",
            "CEG's boundary follows from a stated rule plus a completion announcement",
            "no sweep was run for WI-adjacent boundaries outside the frozen four",
            "the raw-evidence status of the 348 newly expected sessions is unresolved by "
            "design"],
        next_step="MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1, then "
                  "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V2. The candidate is NOT "
                  "rebuilt automatically.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    ok = all(p["acceptance"].values())
    p["status"] = ("MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1_FROZEN" if ok
                   else "HOLD")
    d = ART.seal(p, "MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1.json",
                 required=("spec_id", "status", "amends", "authoritative_inputs",
                           "repair_class_taxonomy", "branch_a_repairs",
                           "branch_b_corrections", "wi_policy_invariant",
                           "interval_invariant_amendment", "restoration_accounting",
                           "raw_evidence_consequence", "acceptance", "mutations"),
                 supersede=os.path.exists(
                     "MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1.json"))
    print(f"MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  Branch A  {len(A_rows)} repairs   recomputed {a_total} sessions "
          f"(expected 345) -> {a_total == 345}")
    print(f"  Branch B  {len(B_rows)} corrections recomputed {b_total} sessions "
          f"(expected 3)  -> {b_total == 3}")
    print(f"  coverage repair set  8 + {len(A_rows)} = {8 + len(A_rows)} · "
          f"WI corrections {len(B_rows)} · unique securities {8+len(A_rows)+len(B_rows)}")
    print(f"  restoration  {eight_total:,} + {a_total} + {b_total} = "
          f"{eight_total + a_total + b_total:,} (expected 5,417)")
    print(f"  status counts preserved: {status_ok} · guards {p['guards_total_after_amendment']}")
    print(f"  RAW_CANONICAL_COVERAGE_STATUS: TO_BE_VERIFIED for {a_total + b_total} sessions")
    print(f"  acceptance: {'ALL PASS' if ok else p['acceptance']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
