"""T9_15M_EXECUTION_PAUSE_V1 — a resource-management pause, recorded as a fact.

The T9 15m capability process was SIGSTOPped so the app and the GANN capability could have
the machine. This is scheduling, not science: a stopped process keeps its memory, its RNG
objects and its already-read outcome vector exactly as they were, and resumes on SIGCONT at
the next instruction. Nothing about the experiment changes.

It is written as its OWN artifact rather than added to the result, because the process that
will write the result is already running: its code is imported and in memory, so a field
added to the source now would never reach the artifact it produces. After the result seals,
a binding amendment ties the two together.

WHAT A RESUME MUST STILL PROVE (the identity check this artifact exists to enable):

    the outcome sidecar still hashes to the digest recorded here — if anything rebuilt
    t9_outcomes.parquet during the pause, the in-memory y is a different vintage from the
    file and the run may not be continued

No Y value is read here; the outcome digest is a file hash, not a value.
"""
from __future__ import annotations
import json, os, subprocess, sys, time                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
import t9_sequence_estimand as E9                                       # noqa: E402

PID = int(sys.argv[1]) if len(sys.argv) > 1 else 94877
LEDGER = os.path.join(os.path.dirname(HERE), "data",
                      "t9_15m_capability_ledger.parquet")


def state(pid: int) -> str:
    r = subprocess.run(["ps", "-o", "state=", "-p", str(pid)],
                       capture_output=True, text=True)
    return (r.stdout.strip() or "GONE")


def main():
    st = state(PID)
    ledger_exists = os.path.exists(LEDGER)
    n_cells = 0
    if ledger_exists:
        import pandas as pd
        L = pd.read_parquet(LEDGER, columns=["needle", "world_id"])
        n_cells = int(L.groupby(["needle", "world_id"]).ngroups)
    y_digest = ART.file_digest(E9.OC)
    d = ART.seal(dict(
        spec_id="T9_15M_EXECUTION_PAUSE_V1", status="OPERATIONAL_RECORD", family="T9",
        execution_pause=dict(
            method="SIGSTOP/SIGCONT",
            reason="resource isolation — the app and the GANN capability needed the machine",
            RNG_state_changed="NO",
            outcome_vintage_changed="NO",
            protocol_changed="NO",
            completed_checkpoints_rewritten="NO",
            process_restarted="NO",
            reseeded="NO"),
        why_it_is_inert=(
            "SIGSTOP suspends scheduling only. The process keeps its address space, so every "
            "Generator object, the loaded rank state and the already-read outcome vector are "
            "byte-identical on SIGCONT. Scheduling and memory bandwidth move runtime, never "
            "a value — the same reason thread count is engineering rather than protocol."),
        process=dict(pid=PID, state_at_seal=st,
                     state_legend="T = stopped; TN = stopped, niced",
                     suspended_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        progress_at_pause=dict(
            ledger_exists=ledger_exists, completed_needle_world_cells=n_cells,
            note="checkpoints are written per (needle, world); a cell in flight when the "
                 "pause began is simply finished on resume, from the same in-memory state"),
        resume_identity_check=dict(
            required_before_continuing=[
                "the outcome sidecar still hashes to outcome_source_digest below",
                "the ledger, if any, carries only that same digest",
                "the process is still alive and in state T/TN — a dead process may not be "
                "SIGCONTed and would have to be re-run from the ledger instead"],
            outcome_source=os.path.basename(E9.OC),
            outcome_source_digest=y_digest,
            outcome_source_mtime=time.strftime(
                "%Y-%m-%d %H:%M %Z", time.localtime(os.path.getmtime(E9.OC))),
            why="if anything rebuilt the outcome sidecar during the pause, the in-memory y "
                "is a different vintage from the file on disk and the run may not continue"),
        sequencing="GANN capability seals FIRST; then SIGCONT here, provided the identity "
                   "check above still passes",
        governing=dict(
            capability_protocol=ART.file_digest("T9_15M_CAPABILITY_PROTOCOL_V1.json"),
            engine_state=ART.file_digest("T9_15M_ENGINE_STATE_V1.json"),
            world_identity=ART.file_digest("T9_15M_CAPABILITY_WORLD_IDENTITY_V1.json")),
        hard_state={"T9 15M OBSERVED Z": "NOT COMPUTED",
                    "T9 15M theta": "NOT COMPUTED",
                    "T9 15M WINNERS/SURVIVORS": "UNKNOWN"},
        outcome_exposure="NOT_EXPOSED — a file hash is recorded, never a value",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "T9_15M_EXECUTION_PAUSE_V1.json",
        required=("spec_id", "execution_pause", "resume_identity_check"),
        supersede=os.path.exists("T9_15M_EXECUTION_PAUSE_V1.json"))
    print(f"T9_15M_EXECUTION_PAUSE_V1 · {d}")
    print(f"  pid {PID} state {st} · completed cells {n_cells} · outcome digest {y_digest}")


if __name__ == "__main__":
    main()
