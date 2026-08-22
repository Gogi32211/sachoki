"""FORWARD_1H_SOURCE_QUALIFICATION_V1 — what a source must BE before it can carry evidence.

This is the target, sealed before the infrastructure exists, so that "qualified" cannot
later be redefined into whatever the store happens to support. The current store fails it;
that is the point of writing it down now rather than after a fix.

THE INVARIANT EVERYTHING ELSE SERVES

    a forward episode must be reconstructable, exactly, from the source version it was
    generated under

Cleaning the mutable DuckDB in place and declaring it qualified does NOT satisfy this: a
store that can be rewritten cannot testify about what it said last Tuesday. Qualification
needs either an immutable daily snapshot or append-only versioned partitions.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

OUT = "FORWARD_1H_SOURCE_QUALIFICATION_V1.json"


def main():
    cur = json.load(open("FORWARD_1H_SOURCE_V1.json"))
    body = dict(
        spec_id="FORWARD_1H_SOURCE_QUALIFICATION_V1", status="FROZEN",
        purpose="the definition of a QUALIFIED forward 1H source, sealed before the "
                "infrastructure exists so the bar cannot be lowered to fit what is built",
        the_invariant="a forward episode must be reconstructable, exactly, from the source "
                      "version it was generated under",
        why_cleaning_is_not_enough="repairing the mutable store in place and declaring it "
                                   "qualified does not satisfy the invariant — a store "
                                   "that can be rewritten cannot testify about what it "
                                   "said last week. Either immutable daily snapshots or "
                                   "append-only versioned partitions are required.",
        required_record_per_session=dict(
            source_version_id="stable identifier of the immutable state the rows came from",
            ingestion_batch_id="which write produced them",
            session_date="the ET session the rows belong to",
            ingested_at="when the batch landed",
            availability_cutoff="the instant from which a decision may legitimately use "
                                "this session — a decision earlier than this could not "
                                "have seen the data",
            row_count="rows in the batch",
            ticker_count="distinct tickers in the batch",
            expected_bars_digest="the session-length mapping in force, by digest",
            duplicate_digest="proof of the (ticker, timestamp, universe) grain",
            revision_status="ORIGINAL | REVISED | SUPERSEDED, with a pointer to what it "
                            "revised",
            content_manifest="hash manifest over the batch's rows"),
        acceptance=dict(
            grain="0 duplicate (ticker, timestamp, universe)",
            completeness="every session at or above the registered ticker-coverage floor, "
                         "with any shortfall recorded rather than silently accepted",
            reconstructability="a named past session can be re-read byte-identically from "
                               "its source_version_id",
            revision_policy="a revision NEVER edits a used version in place; it lands as a "
                            "new version, and every episode records which version it used",
            availability="every session carries an availability_cutoff, so a forward "
                         "decision can be SHOWN to have used only what existed then",
            provenance="the writer, the batch and the code that produced them are named"),
        evidence_rule=dict(
            eligible="forward_evidence_eligible = decision_time > protocol_sealed_at AND "
                     "decision_time >= source_qualified_at AND source_version_is_qualified",
            during_hold="episodes generated while the source is not qualified may be kept "
                        "operationally but are NEVER promoted to evidence afterwards",
            no_retroactive_qualification="fixing the source later does not reach back"),
        current_state=dict(
            verdict=cur["verdict"],
            source_gate_digest=ART.file_digest("FORWARD_1H_SOURCE_V1.json"),
            failing=[k for k, v in cur["checks"].items() if not v],
            not_established=["revision policy", "availability timestamps"],
            blocked=["T5 forward accrual", "T9 forward accrual", "T3 forward accrual",
                     "T1 forward accrual"],
            note="one infrastructure fix unblocks all four families at once"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    d = ART.seal(body, OUT, required=("spec_id", "the_invariant",
                                      "required_record_per_session", "acceptance"),
                 supersede=os.path.exists(OUT))
    print(f"FORWARD_1H_SOURCE_QUALIFICATION_V1 · {d} · current verdict {cur['verdict']}")
    print(f"  failing now: {body['current_state']['failing']}")


if __name__ == "__main__":
    main()
