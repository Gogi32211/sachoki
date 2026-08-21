"""Provenance closure for the 15m capability run. Two artifacts, no statistics.

WHY THE ATTESTATION HAD TO BE WIDENED

V1 attested "the rows already written". But the running process carries the pre-patch code, so
every checkpoint it writes from now on is ALSO in the legacy format. An attestation scoped to
creation time would leave the later rows uncovered while looking as though provenance were
closed. V2 scopes it to the whole process-run instead.

The scope is checkable rather than asserted: the first launch (pid 58204) raised at the
preflight gate, which sits before the cell loop, so it wrote no ledger at all. Every row in
t5_15m_capability_ledger.parquet therefore belongs to pid 58567.

WHAT THE PATCH DID AND DID NOT TOUCH

The mid-run patch is provenance-only. It added ledger columns, a resume vintage check, and
ART.file_digest. It did not touch the kernel, the SE computation, the injection path, the
permutation stream, the RNG seeds, the delta grid, the worlds or the detection rule — so
legacy and post-patch rows are statistically the same object, differing only in the columns
that record where they came from.

THE CLOSURE BINDS THE RESULT TO THE LEDGER THAT PRODUCED IT

A capability surface with no link to its ledger can be re-derived from a different ledger and
nobody would know. The closure records the final ledger's content digest alongside the
vintage attestation, the four identity hashes and the runner file's hash.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import pandas as pd                                                   # noqa: E402
import t5_artifact as ART, t5_dna as D                                # noqa: E402

DATA = os.path.join(D.ROOT, "data")
LEDGER = os.path.join(DATA, "t5_15m_capability_ledger.parquet")
V1 = "T5_15M_CAPABILITY_DATA_VINTAGE.json"
V2 = "T5_15M_CAPABILITY_DATA_VINTAGE_V2.json"
AUDIT = "T5_15M_CAPABILITY_AUDIT.json"
CLOSURE = "T5_15M_CAPABILITY_PROVENANCE_CLOSURE.json"
RESULT = "T5_15M_CAPABILITY_RANK_V2_RESULT.json"
RUN_PID = 58567


def main():
    ART.smoke_test(verbose=False)
    ORD = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
    ydig = ART.file_digest(os.path.join(DATA, "t5_episode_outcomes.parquet"))
    v1 = json.load(open(V1))
    if v1["outcome_source_digest"] != ydig:
        raise RuntimeError(f"outcome sidecar changed since V1 was sealed: "
                           f"{v1['outcome_source_digest']} -> {ydig}")

    # ── V2 · widen the attestation to the whole process-run ────────────────
    v2 = dict(v1)
    v2.update(
        spec_id="T5_15M_CAPABILITY_DATA_VINTAGE_V2",
        supersedes="T5_15M_CAPABILITY_DATA_VINTAGE", supersedes_digest=ART.file_digest(V1),
        applies_to=f"EVERY row produced by pid {RUN_PID}, not merely the rows that existed "
                   f"when V1 was sealed. The running process carries the pre-patch code, so "
                   f"its later checkpoints are also legacy-format.",
        scope_is_checkable=f"the earlier launch (pid 58204) raised at the preflight gate, "
                           f"which precedes the cell loop, and wrote no ledger; every row in "
                           f"{os.path.basename(LEDGER)} therefore belongs to pid {RUN_PID}",
        legacy_row_format="no inline outcome_source_digest / population_hash / "
                          "block_assignment_hash",
        patch_scope=dict(
            touched=["ledger columns", "resume vintage check", "ART.file_digest"],
            not_touched=["kernel", "analytic SE", "injection path", "permutation stream",
                         "RNG seeds", "delta grid", "worlds", "detection rule"],
            consequence="legacy and post-patch rows are the same statistical object"),
        legacy_row_attribution_method="EXTERNAL_PROCESS_EXCLUSIVITY_ASSERTION",
        legacy_row_attribution_verifiable_from_row=False,
        sole_writer_pid=RUN_PID, known_nonwriter_pid=58204,
        resume_contract=dict(
            legacy_row_eligibility=[
                "a valid data-vintage attestation exists",
                "the attested outcome_source_digest equals the current source digest",
                "population_hash, block_assignment_hash, claim_order_hash and the spec "
                "digest all match",
                "the row lies within the ledger region covered by the attestation",
                "process exclusivity for that region is established externally: pid 58204 "
                "produced no rows and pid 58567 was the sole writer"],
            attribution_caveat="A legacy row carries no embedded run_id and therefore cannot "
                               "be cryptographically or field-wise attributed to a specific "
                               "process. The process attribution in item 5 is an AUDITED "
                               "PROVENANCE ASSERTION, not a property verified from the legacy "
                               "row itself.",
            new_row="must carry outcome_source_digest, population_hash and "
                    "block_assignment_hash inline, and those ARE checked from the row"))
    d2 = ART.seal(v2, V2, required=("spec_id", "outcome_source_digest", "applies_to"))

    # ── audit · the two defects, classified separately ─────────────────────
    audit = dict(
        spec_id="T5_15M_CAPABILITY_AUDIT",
        defects=[
            dict(id="DATA_VINTAGE_PROVENANCE_GAP",
                 found="mid-run, before any resume occurred",
                 description="ledger rows carried no outcome_source_digest, so a resume could "
                             "have joined rows computed on two different outcome vintages "
                             "without any check noticing",
                 effect_on_statistic="NONE — the running process reads the sidecar once and "
                                     "holds it in memory for every world",
                 effect_on_resumability_and_auditability="YES",
                 remediation="vintage attestation (V1, widened to V2) plus inline provenance "
                             "on every row written by the patched runner"),
            dict(id="SELF_DIGEST_FIELD_RESUME_BUG",
                 found="at the preflight gate, before the first cell",
                 description="the runner looked for a spec_digest field inside the spec JSON. "
                             "ART.seal returns the digest and does not store it, because a "
                             "field holding its own content hash cannot be consistent, so the "
                             "lookup returned None",
                 effect_on_statistic="NONE — the current process was never restarted, so no "
                                     "Z, permutation or detection depends on it",
                 effect_on_resume="completed worlds would have compared against None and been "
                                  "recomputed from scratch",
                 remediation="ART.file_digest() — the artifact's identity is the hash of its "
                             "bytes")],
        neither_defect_altered=["Z_rank", "permutations", "detections", "bands"],
        cpu_utilization_note="427% on an 8-thread engine is not a threading fault: a world "
                             "carries substantial serial work outside the kernel — permutation "
                             "generation, rank setup, orchestration — so whole-process average "
                             "utilisation sits well below 8x by construction.")
    da = ART.seal(audit, AUDIT, required=("spec_id", "defects"))

    # ── closure · bind the result to the ledger that produced it ───────────
    L = pd.read_parquet(LEDGER)
    ldig = ART.file_digest(LEDGER)
    worlds = sorted(set(int(w) for w in L.world_id))
    body = dict(
        spec_id="T5_15M_CAPABILITY_PROVENANCE_CLOSURE",
        run_pid=RUN_PID,
        ledger=os.path.basename(LEDGER), ledger_final_digest=ldig,
        ledger_rows=int(len(L)),
        needles_present=sorted(set(L.needle)),
        first_world=worlds[0], last_world=worlds[-1],
        n_completed_cells=int(len(L.groupby(["needle", "world_id"]))),
        expected_cells=60,
        complete=bool(len(L.groupby(["needle", "world_id"])) == 60),
        outcome_source_digest=ydig,
        vintage_attestation=V2, vintage_attestation_digest=d2,
        audit=AUDIT, audit_digest=da,
        population_hash=ORD["population_hash"],
        block_assignment_hash=ORD["block_assignment_hash"],
        claim_order_hash=ORD["claim_order_hash"],
        capability_spec_digest=ART.file_digest("T5_15M_CAPABILITY_RANK_V2.json"),
        legacy_rows_have_embedded_run_identity=False,
        legacy_row_scope_basis="sole-writer process exclusivity + ledger boundary + vintage "
                               "attestation",
        ledger_rows_before_run=0,
        ledger_rows_before_run_basis="the ledger file did not exist; the only earlier launch "
                                     "(pid 58204) raised at the preflight gate, which precedes "
                                     "the cell loop",
        ledger_rows_after_run=int(len(L)),
        runner_file_digest_at_closure=ART.file_digest("t5_15m_capability.py"),
        runner_hash_caveat="running process pid 58567 loaded the PRE-PATCH module image; this "
                           "digest identifies the on-disk runner at closure time, NOT a "
                           "byte-exact attestation of the code image already loaded by the "
                           "running process.",
        pre_patch_runner_digest=None,
        pre_patch_runner_digest_unavailable="the file was untracked in git and overwritten in "
                                            "place, so its bytes cannot be recovered. The "
                                            "patch scope is enumerated in the V2 attestation "
                                            "and touches no statistical path, but that is a "
                                            "documented assertion, not a recomputable hash.",
        db_maintenance_overlap=dict(
            nightly_job="com.sachoki.dbupdate, 03:00 Tbilisi",
            overlapped=False,
            overlap_avoided_by="com.sachoki.dbupdate was launchctl-unloaded at 18:30 on "
                               "2026-08-20, before the run reached 03:00, so no DB "
                               "maintenance competed with the capability at any point",
            db_currency_at_run="bars current through 2026-08-19; the 2026-08-20 update was "
                               "deferred to a manual run and the agent must be reloaded",
            affects_statistical_semantics="NO — the run reads the outcome sidecar once into "
                                          "memory and never touches the DuckDB stores the "
                                          "nightly job writes",
            affects_wall_clock_and_cpu="YES",
            consequence="world timings recorded after 03:00 must NOT be used to judge engine "
                        "performance as if the resource environment were unchanged"),
        timing_reference=dict(
            kernel_qualification_t6_ms=402.0,
            note="402 ms is the KERNEL qualification metric. The production wall-clock "
                 "reference is the empirical per-world runtime measured here, which carries "
                 "the serial orchestration the kernel figure excludes."),
        result_artifact=RESULT if os.path.exists(RESULT) else "NOT YET WRITTEN",
        result_digest=ART.file_digest(RESULT) if os.path.exists(RESULT) else None,
        sealed_at=time.strftime("%Y-%m-%d %H:%M:%S %z"))
    dc = ART.seal(body, CLOSURE, required=("spec_id", "ledger_final_digest",
                                           "outcome_source_digest"))

    print(f"V2 attestation  {d2}   scope: every row from pid {RUN_PID}")
    print(f"audit           {da}   2 defects, both with NO effect on the statistic")
    print(f"closure         {dc}")
    print(f"  ledger {os.path.basename(LEDGER)} · {len(L):,} rows · digest {ldig}")
    print(f"  cells {body['n_completed_cells']}/60 · complete {body['complete']}")
    print(f"  result {body['result_artifact']} · {body['result_digest']}")


if __name__ == "__main__":
    main()
