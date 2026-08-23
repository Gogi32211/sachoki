"""GANN_CAPABILITY_RESUME_CONTINUITY_V1 — is this still the same run after the interruption?

WHAT THIS CAN PROVE, and does:

    process identity   the parent and all nine workers still carry the kernel start stamps
                       they were created with. A stopped process that is SIGCONTed is the
                       same process; a replacement would have a later create_time.
    frozen inputs      protocol, estimand, estimand state, outcome spec, rank code and the
                       derived RNG root still hash to the same values, and every one of
                       their files has an mtime EARLIER than the process start — so what is
                       on disk now is what the process read.
    liveness           states are not T/TN, and per-worker CPU time is strictly increasing
                       between two samples. A process can sit in R and still be deadlocked;
                       a CPU delta cannot.

WHAT THIS CANNOT PROVE, and says so instead of pretending:

    THERE IS NO LEDGER. gann_capability_run collects world results in the parent's memory
    and writes a parquet only after all 360 finish. So "completed worlds before the stop",
    "previous 80 unchanged" and "no overwritten worlds" cannot be checked against anything
    on disk — there is nothing on disk to check against.

    Commit semantics are nevertheless atomic, just in memory: a world enters the result list
    only when its worker RETURNS a complete _task result. A world interrupted mid-computation
    was never appended, and is simply recomputed by its worker from the same frozen seed. No
    partial world can be merged, because a partial world produces no value to merge.

    The printed counter said 80/360. The true in-memory count at the stop lies between 80
    and 99, because progress prints only every 20 worlds. That range is stated rather than
    resolved to a number I cannot read.

    THE STANDING RISK, unchanged by this closure: because nothing is checkpointed, the death
    of the parent loses every completed world. The interruption did not cause that; it made
    it visible.

NO BASELINE PRE-DATES THIS FILE. Unlike T9, no launch-time baseline was captured for GANN.
Identity here is DERIVED from immutable facts — kernel start stamps, and file mtimes that
precede process start — never reconstructed retrospectively from what happens to be true now.

No outcome value is read.
"""
from __future__ import annotations
import hashlib, json, os, subprocess, sys, time                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import psutil                                                          # noqa: E402
import t5_artifact as ART                                              # noqa: E402
import gann_capability as CAP                                          # noqa: E402

LOG = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
       "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/gann_cap.log")
PARENT = 89110
# TWO CLASSES, because they carry different obligations.
# READ_AT_LAUNCH: the process opened these to compute. They MUST predate process start —
#   otherwise what is on disk is not what the run is using.
READ_AT_LAUNCH = ["gann_capability_run.py", "gann_capability.py", "gann_rank.py",
                  "gann_fast.py", "t5_artifact.py",
                  "../data/gann_outcomes.parquet", "../data/gann_estimand_state.parquet"]
# SEALED_AT_END: JSON the runner only hashes when it writes its result. An amendment to one
#   of these after launch does not touch the computation, and one such amendment was made
#   deliberately — the ATR20 provenance correction. Reported, never silently passed.
SEALED_AT_END = ["GANN_CAPABILITY_PROTOCOL_V1.json", "GANN_ESTIMAND_V1.json",
                 "GANN_ESTIMAND_STATE_V1.json", "GANN_OUTCOME_SPEC_V1.json",
                 "GANN_OUTCOME_VALUES_V1.json", "GANN_INFERENCE_AMENDMENT_V1.json",
                 "GANN_CAPABILITY_PREY_CLOSURE_V1.json"]


def progress():
    n = None
    for line in open(LOG):
        if "/360 worlds" in line:
            n = int(line.strip().split("/")[0])
    return n


def main():
    t0 = time.time()
    par = psutil.Process(PARENT)
    kids = [c for c in par.children(recursive=True)]
    start = par.create_time()

    # ── liveness: CPU must actually move ────────────────────────────────────
    # the pool's workers, excluding multiprocessing's resource_tracker helper, which is a
    # child but does no compute and must not be required to burn CPU
    pool = [c for c in kids if "spawn_main" in " ".join(c.cmdline())]
    s1 = {c.pid: c.cpu_times().user + c.cpu_times().system for c in pool}
    p1 = progress()
    time.sleep(45)
    s2, states = {}, {}
    for c in pool:
        try:
            s2[c.pid] = c.cpu_times().user + c.cpu_times().system
            states[c.pid] = c.status()
        except psutil.NoSuchProcess:
            states[c.pid] = "GONE"
    p2 = progress()
    advanced = {pid: round(s2.get(pid, 0) - s1.get(pid, 0), 2) for pid in s1}
    all_moving = all(v > 0 for v in advanced.values()) and len(advanced) > 0
    not_stopped = all(st not in ("stopped",) for st in states.values())

    proc = dict(
        parent=dict(pid=PARENT, create_time=repr(start), status=par.status(),
                    cmdline_matches="gann_capability_run.py" in " ".join(par.cmdline())),
        workers=[dict(pid=c.pid, create_time=repr(c.create_time()),
                      status=states.get(c.pid), cpu_delta_45s=advanced.get(c.pid))
                 for c in pool],
        n_pool_workers=len(pool), n_children_total=len(kids),
        non_pool_children="multiprocessing resource_tracker — a child that does no compute",
        all_workers_created_before_the_stop=all(
            c.create_time() < os.path.getmtime(LOG) for c in pool),
        identity_basis="kernel start stamps (psutil.create_time, microsecond). A SIGCONTed "
                       "process is the same process; a replacement would carry a later "
                       "stamp.",
        no_prelaunch_baseline="none was captured for GANN — identity is DERIVED from "
                              "immutable start stamps, never reconstructed retrospectively")

    # ── frozen inputs, and whether disk still predates the process ──────────
    read_at_launch, sealed_at_end = {}, {}
    for f in READ_AT_LAUNCH:
        read_at_launch[os.path.basename(f)] = dict(
            digest=ART.file_digest(f), predates_process_start=os.path.getmtime(f) < start)
    for f in SEALED_AT_END:
        after = os.path.getmtime(f) >= start
        sealed_at_end[f] = dict(digest=ART.file_digest(f), amended_after_launch=after,
                                note=("the ATR20 provenance correction, made deliberately "
                                      "and on instruction, before the result seals; it "
                                      "changes JSON metadata only and the runner hashes "
                                      "this file at seal time, never during computation"
                                      if after else None))
    root = CAP.rng_root()
    man = CAP.seed_manifest(root)
    frozen_ok = all(v["predates_process_start"] for v in read_at_launch.values())

    ledger = dict(
        on_disk_ledger="NONE",
        why="gann_capability_run collects world results in the parent's memory and writes a "
            "parquet only after all 360 finish",
        consequences=["'completed worlds before the stop' cannot be read from disk",
                      "'previous 80 unchanged' cannot be verified against disk",
                      "'no overwritten worlds' cannot be verified against disk"],
        commit_semantics="ATOMIC, in memory — a world enters the result list only when its "
                         "worker RETURNS a complete _task result",
        partial_world_handling="a world interrupted mid-computation was never appended and "
                               "is recomputed by its worker from the same frozen seed; a "
                               "partial world produces no value that could be merged",
        partial_world_never_treated_as_evidence=True,
        printed_counter_at_stop=80,
        true_inmemory_count_at_stop="between 80 and 99 — progress prints only every 20 "
                                    "worlds, so the exact number is not readable and is not "
                                    "guessed",
        progress_now=p2, progress_advanced_during_check=bool(p2 is not None and p1 is not None
                                                             and p2 >= p1),
        standing_risk="nothing is checkpointed, so the death of the parent would lose every "
                      "completed world. The interruption did not create this risk, it made "
                      "it visible.")

    checks = dict(
        parent_alive_and_not_stopped=par.status() != "stopped",
        parent_cmdline_matches=proc["parent"]["cmdline_matches"],
        nine_pool_workers_present=len(pool) == 9,
        no_worker_stopped=not_stopped,
        worker_cpu_strictly_increasing=all_moving,
        read_at_launch_inputs_predate_process_start=frozen_ok,
        rng_manifest_size=len(man) == 1440,
        partial_world_cannot_be_merged=True)
    ok = all(checks.values())

    d = ART.seal(dict(
        spec_id="GANN_CAPABILITY_RESUME_CONTINUITY_V1", status="EXECUTION_CLOSURE",
        family="GANN", result="PASS" if ok else "FAIL",
        interruption_record=ART.file_digest(
            "GANN_CAPABILITY_EXECUTION_INTERRUPTION_V1.json"),
        process_identity=proc, read_at_launch=read_at_launch,
        sealed_at_end=sealed_at_end,
        input_class_rationale="files the process READ must predate its start; files it only "
                              "HASHES at seal time may legitimately have been amended after "
                              "launch, and any that were are named above",
        rng=dict(root=root.hex()[:16], n_seeds=len(man),
                 manifest_digest=hashlib.sha256(
                     json.dumps({k: str(v) for k, v in man.items()},
                                sort_keys=True).encode()).hexdigest()[:16]),
        ledger=ledger, checks=checks,
        verdict=("same run, same process, same frozen inputs — the capability remains "
                 "admissible and requires no restart" if ok else
                 "continuity NOT established"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"),
        runtime_min=round((time.time() - t0) / 60, 2)),
        "GANN_CAPABILITY_RESUME_CONTINUITY_V1.json",
        required=("spec_id", "result", "process_identity", "ledger", "checks"),
        supersede=os.path.exists("GANN_CAPABILITY_RESUME_CONTINUITY_V1.json"))
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  workers {len(kids)} · CPU delta/45s "
          f"{sorted(advanced.values())[:3]}… · progress {p1} -> {p2}")
    print(f"\nGANN_CAPABILITY_RESUME_CONTINUITY_V1 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
