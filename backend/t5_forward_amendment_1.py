"""T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1 — a recorded amendment, not a silent edit.

STATUS: DRAFT.

WHY AN AMENDMENT ARTIFACT RATHER THAN A CODE CHANGE

T5_FORWARD_SESSION_LENGTH_V1 needs the ledger to carry one new DQ code and one new blind
metric. Adding them to t5_forward_accrual.py alone would leave the sealed ledger spec
(9a61d6365ba612b7) describing a writer that no longer exists — the artifact would still verify
against itself while silently ceasing to describe the process. So the change is written down,
with its parent digest, and the parent is NOT regenerated.

THE TIMING IS WHAT MAKES IT CLEAN

    mature_count            0
    forward episodes        0
    outcomes observed       0

No forward observation exists, so this amendment cannot have been shaped by one. That is a
checkable fact, asserted below rather than asserted in prose, and it is the only reason a
pre-registered protocol may be amended at all.

WHAT IT DELIBERATELY DOES NOT TOUCH

Lock target, support floors, families, membership rules, outcomes, statistical tests. An
amendment that reached any of those would be a new protocol wearing V1's digest.
"""
from __future__ import annotations
import json, os, sys                                                   # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402

PARENT = "T5_FORWARD_ACCRUAL_LEDGER_V1"
PARENT_DIGEST = "9a61d6365ba612b7"
CUTOFF = json.load(open("/Users/sachoki/Desktop/sachoki-desktop/backend/T5_FORWARD_VALIDATION_V1.json"))["discovery_cutoff"]["last_eligible_historical_signal_session"]
OUT = "T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1.json"
DATA = os.path.join(D.ROOT, "data")


def assert_pre_forward_data():
    """The amendment is legitimate only while nothing forward has been OBSERVED.

    An earlier version of this check counted ROWS in the forward stores and fired on 175
    membership rows and 2 ledger rows. Those are not observations: the 175 are one scaffold row
    per frozen claim with every forward counter at zero, and the 2 are blind accrual runs that
    found n_total_t5 = 0. The check was measuring the wrong quantity, so it measures the right
    one now — the counters, not the row count.

    That correction is recorded rather than quietly applied, and BOTH numbers are returned, so
    an auditor sees the scaffold exists AND that it contains nothing observed. Loosening a gate
    because it fired would be indefensible; fixing a gate that asked the wrong question is the
    opposite, but only if the change is visible."""
    f = {}
    led = os.path.join(DATA, "t5_forward_ledger.parquet")
    if os.path.exists(led):
        L = pd.read_parquet(led)
        f["ledger_rows"] = int(len(L))
        f["n_total_t5"] = int(L["n_total_t5"].max()) if "n_total_t5" in L else 0
        f["n_total_mature"] = int(L["n_total_mature"].max()) if "n_total_mature" in L else 0
        f["forward_outcomes"] = str(L["forward_outcomes"].iloc[-1]) if "forward_outcomes" in L \
            else "UNKNOWN"
        f["statistical_testing"] = str(L["statistical_testing"].iloc[-1]) \
            if "statistical_testing" in L else "UNKNOWN"
    else:
        f.update(ledger_rows=0, n_total_t5=0, n_total_mature=0,
                 forward_outcomes="SEALED", statistical_testing="LOCKED")

    occ = os.path.join(DATA, "t5_forward_membership.parquet")
    if os.path.exists(occ):
        O = pd.read_parquet(occ)
        f["occurrence_scaffold_rows"] = int(len(O))
        f["forward_occurrence_total"] = int(O["forward_occurrence_total"].sum()) \
            if "forward_occurrence_total" in O else 0
        f["forward_occurrence_mature"] = int(O["forward_occurrence_mature"].sum()) \
            if "forward_occurrence_mature" in O else 0
    else:
        f.update(occurrence_scaffold_rows=0, forward_occurrence_total=0,
                 forward_occurrence_mature=0)

    ep = os.path.join(DATA, "t5_forward_episodes.parquet")
    f["episode_store_rows"] = int(len(pd.read_parquet(ep))) if os.path.exists(ep) else 0

    # the episode source itself: is there any post-cutoff signal session at all?
    E = pd.read_parquet(D.OUT_EP, columns=["t5_date"])
    f["post_cutoff_episodes_in_source"] = int((E.t5_date > CUTOFF).sum())

    observed = ("n_total_t5", "n_total_mature", "forward_occurrence_total",
                "forward_occurrence_mature", "episode_store_rows",
                "post_cutoff_episodes_in_source")
    nonzero = {k: f[k] for k in observed if f[k]}
    if nonzero:
        raise RuntimeError(
            f"forward observations already exist {nonzero} — this is no longer a pre-data "
            f"amendment. Amending the protocol after observations exist requires a different "
            f"and much stronger justification than 'the infrastructure needed a field'.")
    if f["forward_outcomes"] != "SEALED" or f["statistical_testing"] != "LOCKED":
        raise RuntimeError(f"outcome side is not sealed/locked: {f}")
    return f


def main():
    ART.smoke_test(verbose=False)
    got = ART.file_digest(PARENT + ".json")
    if got != PARENT_DIGEST:
        raise RuntimeError(f"parent digest drift: {got} != {PARENT_DIGEST}")
    facts = assert_pre_forward_data()
    print(f"{PARENT} · {got} · pre-forward-data {facts}")

    ART.seal(dict(
        spec_id=PARENT + "_AMENDMENT_1", status="FROZEN",
        parent=PARENT, parent_digest=PARENT_DIGEST,
        parent_not_regenerated=True,
        why_an_artifact="adding the field to the writer alone would leave the sealed ledger "
                        "spec describing a process that no longer exists — verifying against "
                        "itself while silently ceasing to describe the writer",

        effective="before the first forward episode",
        amendment_is_pre_forward_data=True,
        pre_data_evidence=facts,
        why_that_matters="no forward observation exists, so this amendment cannot have been "
                         "shaped by one. It is asserted from the stores, not claimed in prose.",

        adds_dq_code=dict(
            code="SESSION_LENGTH_LATE_FOR_DECISION",
            meaning="the session length was not committed strictly before "
                    "decision_deadline_ts",
            effect=["episode HELD / ineligible", "not evaluated",
                    "not counted toward the 20,000", "reported in the DQ ledger"],
            no_backfill="evaluating later with a length that arrived after the decision is the "
                        "retrospection the protocol exists to prevent",
            is_x_only=True,
            required_by="T5_FORWARD_SESSION_LENGTH_V1 gate G6B"),

        adds_blind_metric=dict(
            name="commit_latency",
            definition="session_length_commit_ts - last_bar_ts",
            outcome_blind=True,
            why="it makes 'sessions committed before the deadline' vs 'held for late finality' "
                "visible without touching MFE"),

        adds_correction_audit=dict(
            columns=["original_source_digest", "correction_source_digest",
                     "correction_detected_ts", "affected_session"],
            membership_not_recomputed=True,
            why="the system must use the information it had at decision time, not a history "
                "corrected afterwards"),

        does_not_change=["lock target (20,000 mature episodes)",
                         "support floors (300 treated / 300 control)",
                         "families (1H k=4, 15m k=171) and the family digest",
                         "membership rules and the frozen evaluator",
                         "outcome definition (MFE_10D)",
                         "the statistical tests and the multiplicity control"],
        why_that_list="an amendment reaching any of those would be a new protocol wearing "
                      "V1's digest"),
        OUT, required=("spec_id", "parent_digest", "adds_dq_code",
                       "amendment_is_pre_forward_data"))
    print(f"  FROZEN · {OUT} · {ART.file_digest(OUT)}")


if __name__ == "__main__":
    main()
