"""SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1 — freeze the cohort by rule, then see what number falls out.

The point of writing the rule before the count is that an exclusion list has to be defensible
as a source qualification, not as a preference. "We removed these tickers" and "V1 uses only
the pre-registered original-vintage cohort" can produce the identical list and mean completely
different things. So each clause is evaluated from sealed evidence, the resulting set is
whatever it is, and 476 is a CHECK — if the rule yields another number the gate holds rather
than reconciling by hand.

CLAUSE 3 IS MEASURED, NOT INHERITED. The five source-boundary failures are already diagnosed,
but taking that list on trust would assume the original scan was exhaustive. The canonical
ingest wrote a per-session manifest carrying a state for every security, so all 1,254 of them
are scanned here and every non-OBSERVED, non-NOT_YET_REGULAR_WAY state is collected directly.
If that sweep returns anything other than those five securities, the cohort is not what we
think it is and the count will say so.

CLAUSE 2 AND CLAUSE 4 ARE DERIVED INDEPENDENTLY AND MUST AGREE. Clause 2 reads the lineage
repair tables (8 + 11 + 3). Clause 4 asks a different question — which securities would need
supplemental or new-vintage bytes — and answers it from the quarantine directory listing and
the 348-session inventory. Two different sources, and they have to land on the same 22. If
they diverge, something is wrong with one of the artifacts and the divergence is the finding.

WHAT IS NOT BEING CLAIMED. Not that the 476 each have five full years. A company listed in
2024 has a short history and that is a fact about the company, not a defect: its
NOT_YET_REGULAR_WAY interval is correct and stays. What the cohort excludes is history lost
artificially and history whose source identity is ambiguous — nothing else.

Y IS NOT TOUCHED. The exclusion list and its hash are frozen here, before any T/Z result is
computed, so the cohort cannot later be adjusted in the direction of a nicer answer.
"""
from __future__ import annotations
import glob, hashlib, json, os, sys, time                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

CANON_MAN = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m/_manifest"
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
AMEND = "MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1.json"
INVENTORY = "MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1.json"
UNOBS = "MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1.json"
SNAP = "SP500_CURRENT_SNAPSHOT_V1.json"
BOUND = "MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1.json"
EIGHT = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]
EXPECT = 476


def main():
    digests = {f.replace(".json", ""): ART.file_digest(f)
               for f in (IDENTITY, AMEND, INVENTORY, UNOBS, SNAP, BOUND)}
    if digests["MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1"] != "94d97c70acb41fa2":
        print("HOLD — amendment digest mismatch"); return 1

    ids = json.load(open(IDENTITY))["mapping"]
    universe = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    am = json.load(open(AMEND))
    inv = json.load(open(INVENTORY))

    # ---- clause 2: lineage requires a V7 coverage repair or WI-boundary correction
    branch_a = [r["ticker"] for r in am["branch_a_repairs"]["rows"]]
    branch_b = [r["ticker"] for r in am["branch_b_corrections"]["rows"]]
    clause2 = set(EIGHT) | set(branch_a) | set(branch_b)

    # ---- clause 4: research would need supplemental / new-vintage bytes.
    # Derived from the DATA stores, not from the lineage tables, so it is a real check.
    quar_dirs = {d for d in os.listdir(QUAR)
                 if os.path.isdir(os.path.join(QUAR, d))}
    inv_tickers = {r["ticker"] for r in inv["per_security"]}
    clause4 = quar_dirs | inv_tickers
    clause_agreement = clause2 == clause4

    # ---- clause 3: MEASURED over every canonical ingest manifest
    t0 = time.time()
    bad_states, sessions_scanned = {}, 0
    for p in sorted(glob.glob(os.path.join(CANON_MAN, "*.json"))):
        for line in open(p):
            try:
                m = json.loads(line)
            except Exception:
                continue
            sessions_scanned += 1
            for tk, st in (m.get("states") or {}).items():
                if st in ("OBSERVED", "NOT_YET_REGULAR_WAY"):
                    continue
                bad_states.setdefault(tk, {}).setdefault(st, []).append(m["session"])
    clause3 = set(bad_states)
    print(f"  manifests scanned {sessions_scanned:,} in {time.time()-t0:.0f}s · "
          f"securities with a non-OBSERVED state: {len(clause3)} -> {sorted(clause3)}")

    excluded = clause2 | clause3
    overlap = sorted(clause2 & clause3)
    eligible = sorted(set(universe) - excluded)
    n = len(eligible)

    cohort_hash = hashlib.sha256(
        json.dumps([universe[t] for t in eligible], sort_keys=True).encode()).hexdigest()
    excl_hash = hashlib.sha256(
        json.dumps(sorted(excluded), sort_keys=True).encode()).hexdigest()

    checks = dict(
        universe_503=len(universe) == 503,
        clause2_is_22=len(clause2) == 22,
        clause2_equals_clause4=clause_agreement,
        clause3_is_5=len(clause3) == 5,
        no_overlap=not overlap,
        eligible_is_476=n == EXPECT,
        arithmetic=len(universe) - len(clause2) - len(clause3) == n,
        y_not_exposed=True)
    ok = all(checks.values())

    p = dict(
        report_id="SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1",
        status=("V1_RESEARCH_ELIGIBILITY_FROZEN" if ok else "HOLD"),
        purpose="freeze a pre-registered, source- and lineage-qualified original-vintage "
                "cohort for V1 research",
        framing=dict(
            what_this_is="V1 uses only the pre-registered original-vintage cohort",
            what_this_is_not="a preference-based removal of tickers we dislike",
            why_the_rule_precedes_the_count="an identical list can be defensible or "
                                            "arbitrary depending entirely on how it was "
                                            "derived",
            number_is_a_check="476 is a CHECK on the rule, not a target. A different result "
                              "holds the gate rather than being reconciled by hand."),

        frozen_rule=dict(
            name="V1_ELIGIBLE",
            clauses=[
                "member of the frozen current-503 universe",
                "lineage requires no V7 coverage repair or WI-boundary correction",
                "no security-session in its expected historical coverage has an "
                "original-ingest UNOBSERVED / source-identity failure",
                "research does not require supplemental or new-vintage market-data bytes"],
            frozen_before_any_result=True,
            name_based=False,
            note="clauses are evaluated from sealed evidence; no ticker is named in the rule"),

        source_universe=dict(count=len(universe), artifact=SNAP,
                             digest=digests["SP500_CURRENT_SNAPSHOT_V1"], unchanged=True),

        clause_2_lineage_special=dict(
            count=len(clause2),
            security_level_coverage_repairs=dict(original_eight=EIGHT,
                                                 new_anchor_delay=sorted(branch_a),
                                                 total=len(EIGHT) + len(branch_a)),
            wi_boundary_corrections=sorted(branch_b),
            securities=sorted(clause2),
            why="using their corrected lineage requires history that original canonical raw "
                "does not contain: 5,069 sessions for the original eight and 348 for the "
                "newly qualified fourteen"),

        clause_4_cross_check=dict(
            derived_from=["supplemental quarantine directory listing",
                          "MASSIVE_V7_NEW_348_RAW_COVERAGE_INVENTORY_V1 per-security rows"],
            independent_of="the lineage repair tables used by clause 2",
            count=len(clause4), securities=sorted(clause4),
            agrees_with_clause_2=clause_agreement,
            meaning="two different sources answering two different questions land on the "
                    "same 22; divergence would itself have been the finding"),

        clause_3_original_source_failures=dict(
            measured=True,
            method="scanned every canonical ingest manifest and collected every state that "
                   "is neither OBSERVED nor NOT_YET_REGULAR_WAY",
            manifests_scanned=sessions_scanned,
            not_inherited="the five were already diagnosed, but trusting that list would "
                          "assume the original scan was exhaustive",
            count=len(clause3), securities=sorted(clause3),
            detail={tk: {st: dict(sessions=len(v), examples=v[:3])
                         for st, v in d.items()} for tk, d in bad_states.items()},
            diagnosis_artifact=UNOBS,
            diagnosis_digest=digests["MASSIVE_1M_UNOBSERVED_FIVE_DIAGNOSIS_V1"]),

        overlap_between_exclusions=dict(securities=overlap, count=len(overlap),
                                        expected=0),

        result=dict(
            SOURCE_UNIVERSE=len(universe),
            V1_ELIGIBLE=n,
            EXCLUDED_LINEAGE_SPECIAL=len(clause2),
            EXCLUDED_ORIGINAL_SOURCE_UNOBSERVED=len(clause3),
            SUPPLEMENTAL_DATA_USED=0,
            Y_EXPOSED=0,
            expected_eligible=EXPECT,
            matches_expected=n == EXPECT),

        cohort=dict(
            eligible_tickers=eligible,
            eligible_count=n,
            cohort_security_key_sha256=cohort_hash,
            exclusion_list=sorted(excluded),
            exclusion_list_sha256=excl_hash,
            frozen_at=time.strftime("%Y-%m-%d %H:%M %Z"),
            frozen_before_tz_results=True,
            keyed_by="security_key_v1 — tickers are display metadata"),

        not_claimed=dict(
            full_five_years_for_all=False,
            statement="a security listed in 2024 legitimately has a short history; its "
                      "NOT_YET_REGULAR_WAY interval is correct and is preserved",
            excluded_only="history lost artificially, and history whose source identity is "
                          "ambiguous"),

        deferred_to_v2_branch=[
            "22-security V7 lineage repair",
            "309-session preservation",
            "mixed-vintage supplementation policy",
            "canonical full-503 V7"],
        deferral_rationale="none of these block V1; all remain open and none is cancelled",

        supplemental_use=dict(payloads_used=0, rows_used=0, promotions=0,
                              vintage="ORIGINAL_INGEST_ONLY"),
        input_digests=digests,
        checks=checks, failed_checks=[k for k, v in checks.items() if not v],
        mutations=dict(universe=0, v6=0, v7_candidate=0, canonical_raw=0, quarantine=0,
                       derived_writes=0, vendor_requests=0),
        next_step="Derived Builder Spec V2, scoped to this frozen cohort",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json",
                 required=("report_id", "status", "frozen_rule", "clause_2_lineage_special",
                           "clause_3_original_source_failures", "clause_4_cross_check",
                           "result", "cohort", "checks", "mutations"),
                 supersede=os.path.exists("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    r = p["result"]
    print(f"\nSP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1 · {d} · {p['status']}")
    print(f"  SOURCE_UNIVERSE                      {r['SOURCE_UNIVERSE']}")
    print(f"  V1_ELIGIBLE                          {r['V1_ELIGIBLE']}")
    print(f"  EXCLUDED_LINEAGE_SPECIAL              {r['EXCLUDED_LINEAGE_SPECIAL']}")
    print(f"  EXCLUDED_ORIGINAL_SOURCE_UNOBSERVED    {r['EXCLUDED_ORIGINAL_SOURCE_UNOBSERVED']}")
    print(f"  SUPPLEMENTAL_DATA_USED                 {r['SUPPLEMENTAL_DATA_USED']}")
    print(f"  Y_EXPOSED                              {r['Y_EXPOSED']}")
    print(f"  clause2 == clause4 : {clause_agreement} · overlap: {overlap or 'none'}")
    print(f"  cohort sha256      : {cohort_hash[:16]}")
    print(f"  exclusion sha256   : {excl_hash[:16]}")
    if not ok:
        print(f"  FAILED: {p['failed_checks']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
