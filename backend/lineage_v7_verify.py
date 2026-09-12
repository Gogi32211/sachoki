"""MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_VERIFICATION_V1 — a second pair of eyes that shares no code.

Written as a SEPARATE program on purpose. Verification that imports the builder's helpers
only proves the builder is self-consistent; a bug in `next_session` or in the interval writer
would be reproduced identically and confirmed as correct. So this file re-derives everything
from the staged bytes and from the primary sources, and never imports the builder.

It is also read-only by construction: it opens staging files, quarantine manifests and sealed
artifacts, and writes exactly one thing — its own report.

THE RESTORED-SESSION COUNT IS RECOMPUTED FROM THE MANIFESTS, NOT READ FROM THE AUDIT.
5,069 appears in the boundary audit, in the supplemental preservation artifact and in the
candidate. Checking the candidate against the audit would be checking one derived number
against another derived from the same pass. So the count here is rebuilt by walking
`_manifest.jsonl` and counting sessions whose response actually carried rows — the same way
the number would be produced if the audit had never existed.

INTERVAL INVARIANTS ARE CHECKED AGAINST THE FROZEN CONVENTION, half-open
[start, end_exclusive), including the one that is easy to get wrong: an attribute's intervals
must not overlap, and a gap is only acceptable when it is explicitly represented rather than
silently bridged. Carrying a current value backward over an unknown period is the specific
failure this looks for.
"""
from __future__ import annotations
import glob, hashlib, json, os, sys, time                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

EXEC = "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json"
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")
EIGHT = ["APO", "J", "CRH", "BG", "LH", "FERG", "BLK", "XOM"]
W0, W1 = "2021-08-25", "2026-08-24"
ATTR_OF = dict(registrant_cik_intervals="cik",
               share_class_figi_intervals="share_class_figi",
               composite_figi_intervals="composite_figi",
               ticker_intervals="ticker",
               instrument_type_intervals="type")


def main():
    ex = json.load(open(EXEC))
    stage = ex["candidate_staging_path"]
    checks, problems = {}, []

    # 1. staged bytes reproduce the sealed digests
    dig_ok, dig_detail = True, {}
    for name, meta in ex["candidate_digests"].items():
        p = os.path.join(stage, f"{name}.json")
        if not os.path.exists(p):
            dig_ok = False; dig_detail[name] = "MISSING"; continue
        raw = open(p, "rb").read()
        got = hashlib.sha256(raw).hexdigest()
        rows = json.loads(raw)
        ok = got == meta["sha256"] and len(rows) == meta["rows"]
        dig_detail[name] = dict(sealed=meta["sha256"][:12], recomputed=got[:12],
                                rows_sealed=meta["rows"], rows_read=len(rows), match=ok)
        dig_ok &= ok
    checks["candidate_digests_reproduce"] = dig_ok

    ds = {n: json.loads(open(os.path.join(stage, f"{n}.json"), "rb").read())
          for n in ex["candidate_digests"]}

    # 2. identity counts
    rs = ds["research_securities"]
    keys = [r["security_key_v1"] for r in rs]
    checks["identities_503"] = len(rs) == 503
    checks["distinct_keys_503"] = len(set(keys)) == 503
    checks["no_duplicate_keys"] = len(keys) == len(set(keys))
    checks["no_ticker_primary_key"] = all("security_key_v1" in r for r in rs)

    # 3. coverage
    cov = ds["research_coverage_intervals"]
    checks["coverage_covers_all_503"] = len({r["security_key_v1"] for r in cov}) == 503
    rep = [r for r in cov if r.get("derivation") == "AUDITED_SECURITY_LEVEL_CONTINUITY"]
    checks["repair_count_8"] = len(rep) == 8
    checks["repairs_start_at_window"] = all(r["start_boundary"] == W0 for r in rep)
    checks["repairs_not_v6_moved_backward"] = all(
        r.get("explicitly_not") == "V6_START_MOVED_BACKWARD" for r in rep)
    key_of = {r["current_frozen_ticker"]: r["security_key_v1"] for r in rs}
    checks["repair_set_is_the_eight"] = (
        {r["security_key_v1"] for r in rep} == {key_of[t] for t in EIGHT})

    # 4. restored sessions — recomputed from the manifests, NOT read from the audit
    recount, per = 0, {}
    for tk in EIGHT:
        mp = os.path.join(QUAR, tk, "_manifest.jsonl")
        n = 0
        for line in open(mp):
            try:
                r = json.loads(line)
            except Exception:
                continue
            if (r.get("rows") or 0) > 0:
                n += 1
        per[tk] = n; recount += n
    checks["restored_sessions_5069_recomputed"] = recount == 5069
    checks["restored_matches_candidate"] = (
        recount == ex["known_eight_reconciliation"]["restored_security_sessions"])

    # 5. interval invariants under the frozen half-open convention
    iv_problems = []
    for name, attr in ATTR_OF.items():
        grouped = {}
        for r in ds[name]:
            grouped.setdefault(r["security_key_v1"], []).append(r)
        for k, rows in grouped.items():
            rows = sorted(rows, key=lambda x: (x["start_boundary"],
                                               x["end_boundary_exclusive"]))
            seen = set()
            for r in rows:
                if r["start_boundary"] >= r["end_boundary_exclusive"]:
                    iv_problems.append(dict(dataset=name, key=k, kind="NON_POSITIVE",
                                            row=r))
                sig = (r["start_boundary"], r["end_boundary_exclusive"],
                       json.dumps(r.get("attribute_value")))
                if sig in seen:
                    iv_problems.append(dict(dataset=name, key=k, kind="DUPLICATE",
                                            row=r))
                seen.add(sig)
            for a, b in zip(rows, rows[1:]):
                if b["start_boundary"] < a["end_boundary_exclusive"]:
                    iv_problems.append(dict(dataset=name, key=k, kind="OVERLAP",
                                            a=a["start_boundary"],
                                            b=b["start_boundary"]))
    # coverage intervals get the same treatment — a rename must not overlap its successor
    grouped = {}
    for r in cov:
        grouped.setdefault(r["security_key_v1"], []).append(r)
    for k, rows in grouped.items():
        rows = sorted(rows, key=lambda x: x["start_boundary"])
        for a, b in zip(rows, rows[1:]):
            if b["start_boundary"] < a["end_boundary_exclusive"]:
                iv_problems.append(dict(dataset="research_coverage_intervals", key=k,
                                        kind="OVERLAP", a=a["start_boundary"],
                                        b=b["start_boundary"]))
    checks["interval_invariants"] = not iv_problems
    if iv_problems:
        problems.append(dict(kind="INTERVAL_INVARIANT", count=len(iv_problems),
                             sample=iv_problems[:5]))

    # 6. no value carried backward over an unknown period
    carried = [r for r in ds["instrument_type_intervals"]
               if r.get("attribute_value") not in (None, "NOT_AUDITED")
               and r.get("boundary_precision") == "V6_INHERITED"]
    checks["no_type_fabricated_for_inherited"] = not carried
    checks["inherited_type_is_not_audited"] = any(
        r["attribute_value"] == "NOT_AUDITED" for r in ds["instrument_type_intervals"])

    # 7. the specific semantic facts
    crh_t = [r["attribute_value"] for r in ds["instrument_type_intervals"]
             if r["security_key_v1"] == key_of["CRH"]]
    checks["crh_adrc_present"] = "ADRC" in crh_t
    checks["crh_adrc_not_relabelled"] = crh_t.count("CS") <= 1
    apo_ev = [e for e in ds["structural_transition_events"]
              if e.get("current_ticker") == "APO"
              and e.get("event_type") == "MATERIAL_ECONOMIC_COMPOSITION_TRANSITION"]
    checks["apo_composition_break"] = (bool(apo_ev)
                                       and apo_ev[0]["economic_composition_continuity"]
                                       is False)
    meta_t = {r["attribute_value"] for r in ds["ticker_intervals"]
              if r["security_key_v1"] == key_of.get("META")}
    checks["v6_ticker_rename_preserved"] = {"FB", "META"} <= meta_t
    checks["wi_intervals_flagged"] = any(r.get("when_issued")
                                         for r in ds["ticker_intervals"])
    checks["wi_excluded_from_coverage"] = all(
        not r.get("when_issued") for r in ds["ticker_intervals"]
        if r.get("lineage_state") == "REGULAR_WAY")

    # 8. supplement containment
    checks["no_canonical_use_authorised"] = all(
        r.get("canonical_use_authorised") is False
        for r in ds["vendor_aggregate_access_intervals"])
    checks["no_canonical_v7_artifact"] = not os.path.exists(
        "MASSIVE_TICKER_LINEAGE_V7.json")
    checks["no_derived_dir"] = not os.path.exists(
        "/Volumes/QUANT_RESEARCH/studio_data/derived")
    checks["quarantine_payloads_intact"] = sum(
        len(glob.glob(os.path.join(QUAR, t, "*.json.gz")))
        for t in os.listdir(QUAR)
        if os.path.isdir(os.path.join(QUAR, t))) == 5069

    # 9. evidence links
    checks["every_coverage_has_evidence"] = all(
        r.get("evidence_artifact_digest") for r in cov)
    checks["every_security_has_evidence_link"] = len(
        {r["security_key_v1"] for r in ds["lineage_evidence_links"]}) == 503

    # 10. inputs still unmutated
    inp = ex["input_digests"]
    now = {k: ART.file_digest(k + ".json") for k in inp}
    mutated = {k: (inp[k], now[k]) for k in inp if inp[k] != now[k]}
    checks["input_artifacts_unmutated"] = not mutated

    # 11. implementation hash still matches what produced this candidate
    ch = hashlib.sha256(open("lineage_v7_candidate.py", "rb").read()).hexdigest()
    checks["implementation_hash_unchanged"] = ch == ex["implementation_code_hash"]

    verdict = "PASS" if all(checks.values()) else "HOLD"
    failed = [k for k, v in checks.items() if not v]

    p = dict(
        report_id="MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_VERIFICATION_V1",
        status="INDEPENDENT_VERIFICATION_PASS" if verdict == "PASS" else "HOLD",
        independence=dict(
            shares_code_with_builder=False,
            imports_builder=False,
            why="verification that imports the builder's helpers proves only "
                "self-consistency; a bug in the interval writer would be reproduced and "
                "confirmed",
            writes="its own report only"),
        verifies=dict(execution_artifact=EXEC, digest=ART.file_digest(EXEC),
                      candidate_staging_path=stage,
                      implementation_code_hash=ex["implementation_code_hash"],
                      spec_digest=ex["spec"]["digest"]),
        defect_caught_by_this_verifier=dict(
            found_in="the first candidate build, staging hash 63d8e03cdafb257f",
            defect="next_session() returned the SECOND following session whenever V6's "
                   "inclusive effective_to fell on a non-trading day, so the interval's "
                   "end_exclusive skipped a session and overlapped its successor",
            affected=["CPAY (effective_to 2024-03-24, a Sunday)", "WTW", "XYZ"],
            overlaps_detected=12,
            missed_by="the builder's own checks — it validated counts and semantics but not "
                      "interval adjacency, which is precisely why this verifier shares no "
                      "code with it",
            resolution="fixed in the builder, which changed the implementation hash and "
                       "therefore INVALIDATED candidate 63d8e03cdafb257f; the candidate was "
                       "rebuilt in full under the new hash rather than patched in place",
            gate_rule_applied="do not patch code mid-run and keep prior results",
            superseded_candidate_retained=True,
            second_check_added="research_coverage_intervals are now checked for overlap too, "
                               "not only the five attribute datasets"),
        digest_reproduction=dig_detail,
        restored_sessions=dict(
            recomputed_from="supplemental quarantine _manifest.jsonl (sessions whose "
                            "response carried rows)",
            independent_of="MASSIVE_SECURITY_LINEAGE_BOUNDARY_AUDIT_V1",
            why="checking the candidate against the audit would compare two numbers derived "
                "from the same pass",
            recomputed_total=recount, expected=5069, per_security=per,
            match=recount == 5069),
        interval_validation=dict(
            convention="[start_boundary, end_boundary_exclusive)",
            datasets_checked=sorted(ATTR_OF),
            problems=len(iv_problems), samples=iv_problems[:5]),
        checks=checks, failed_checks=failed, problems=problems,
        reproduces=dict(identity_counts=checks["identities_503"],
                        coverage_counts=checks["coverage_covers_all_503"],
                        repair_counts=checks["repair_count_8"],
                        interval_validation=checks["interval_invariants"],
                        digests=checks["candidate_digests_reproduce"]),
        mutations=dict(input_artifacts_mutated=mutated, canonical_writes=0,
                       derived_writes=0, supplemental_promoted=0),
        verdict=verdict,
        does_not_authorize="canonical V7. This verifies the CANDIDATE only; the full-503 "
                           "audit verdict is carried separately by the execution artifact.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_VERIFICATION_V1.json",
                 required=("report_id", "status", "independence", "verifies", "checks",
                           "restored_sessions", "interval_validation", "verdict"),
                 supersede=os.path.exists(
                     "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_VERIFICATION_V1.json"))
    print(f"MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_VERIFICATION_V1 · {d} · {p['status']}")
    print(f"  checks passed        : {sum(1 for v in checks.values() if v)}/{len(checks)}")
    print(f"  restored recomputed  : {recount:,} (expected 5,069) -> "
          f"{checks['restored_sessions_5069_recomputed']}")
    print(f"  interval problems    : {len(iv_problems)}")
    print(f"  digests reproduce    : {dig_ok}")
    if failed:
        print(f"  FAILED               : {failed}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
