"""GANN_CAPABILITY_RESULT time provenance — run AFTER the result seals.

The runner records one time field, `runtime_min`, taken from its own t0. That clock ran
through the 11.8-hour execution interruption, so it is NOT active compute duration and must
never be read as one. It is left exactly as the runner wrote it — its meaning is not
rewritten after the fact — and the four honest fields are ADDED beside it:

    wall_elapsed                          the same clock the runner used
    stopped_wall                          time the run spent in state T/TN
    active_wall                           wall_elapsed - stopped_wall
    aggregate_worker_cpu_last_sample      summed across the 9 pool workers

The CPU figure is a LOWER BOUND, and is named so. It comes from a sampler that ran WHILE the
pool was alive, because worker CPU is unreadable once the pool tears down. Pool workers are
never recycled here, so cumulative CPU only grows — but up to 5 minutes can elapse between
the last sample and teardown, so with 9 workers at most ~45 worker-minutes go unobserved.
The lag is measured against a TEARDOWN marker rather than assumed.

runtime_min is NEVER reassigned. `runtime_min := active_wall` would be a post-hoc semantic
rewrite, so the runner's field is left untouched and carries a warning instead.

No outcome value is read.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

SCRATCH = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
           "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad")
CPU_TSV = os.path.join(SCRATCH, "gann_worker_cpu.tsv")
RESULT = "GANN_CAPABILITY_RESULT_V1.json"
# There are now TWO pauses, and stopped_wall is their SUM — not a single constant.
#   V1  accidental, cause never established, 22:06 -> 09:54          708 min
#   V2  deliberate, user-directed: T9 runs to completion first       measured at resume
# A hardcoded 708 would silently under-report the second one.
PAUSE_V1_MIN = 708.0
PAUSE_V2 = "GANN_CAPABILITY_EXECUTION_PAUSE_V2.json"
AT_RESUME_V2 = "GANN_CAPABILITY_RESUME_IDENTITY_AT_RESUME_V2.json"


def main():
    if not os.path.exists(RESULT):
        raise SystemExit(f"{RESULT} does not exist yet — this module runs AFTER the seal.")
    d = json.load(open(RESULT))
    if "time_provenance" in d:
        print("time provenance already recorded"); return

    rows = [l.split("\t") for l in open(CPU_TSV).read().strip().split("\n") if l]
    teardown = next((int(r[1]) for r in rows if r[0] == "TEARDOWN"), None)
    samples = [r for r in rows if r[0] != "TEARDOWN"]
    last_ts, n_workers, cpu_s = int(samples[-1][0]), int(samples[-1][1]), float(samples[-1][2])
    counts = sorted({int(r[1]) for r in samples})
    wall = float(d.get("runtime_hours", 0)) * 60.0 or float(d.get("runtime_min", 0))
    # DERIVED from two immutable events, never stored inside either of them
    v2 = json.load(open(PAUSE_V2))
    if not os.path.exists(AT_RESUME_V2):
        raise SystemExit(f"{AT_RESUME_V2} does not exist — GANN has not been resumed, so the "
                         "second pause has no end and stopped_wall cannot be computed.")
    ar = json.load(open(AT_RESUME_V2))
    if not ar.get("sigcont_issued_at"):
        raise SystemExit("AT_RESUME_V2 records no sigcont_issued_at — the resume was "
                         "authorised but not performed; stopped_wall is not yet defined.")
    fmt = "%Y-%m-%d %H:%M:%S"
    t0_ = time.mktime(time.strptime(v2["pause"]["suspended_at"].rsplit(" ", 1)[0], fmt))
    t1_ = time.mktime(time.strptime(ar["sigcont_issued_at"].rsplit(" ", 1)[0], fmt))
    v2_min = (t1_ - t0_) / 60.0
    stopped = PAUSE_V1_MIN + v2_min

    stamp = (lambda t: time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(t))
             if t else None)
    d["time_provenance"] = dict(
        wall_elapsed_min=round(wall, 1),
        stopped_wall_min=round(stopped, 1),
        stopped_wall_breakdown={
            "unexpected_interruption_wall_min": PAUSE_V1_MIN,
            "deliberate_pause_v2_wall_min": round(float(v2_min), 1),
            "stopped_wall_total_min": round(stopped, 1),
            "components_kept": "the total is correct either way, but an audit still needs to "
                               "see WHY the process stood still — one cause was never "
                               "established, the other was instructed",
            "V2_derivation": "PAUSE_V2.suspended_at -> AT_RESUME_V2.sigcont_issued_at; both "
                             "artifacts immutable, duration derived and never stored in "
                             "either"},
        active_wall_min=round(wall - stopped, 1),
        aggregate_worker_cpu_last_sample_min=round(cpu_s / 60.0, 1),
        sampling_interval_min=5,
        n_pool_workers=n_workers,
        worker_counts_seen=counts,
        last_sample_at=stamp(last_ts),
        pool_teardown_at=stamp(teardown),
        lag_to_teardown_min=(round((teardown - last_ts) / 60.0, 1)
                             if teardown else None),
        interpretation=(
            "LOWER-BOUND observation of aggregate worker CPU consumption. NOT an exact "
            "final CPU total. Maximum sampling lag is 5 minutes per worker, so with 9 "
            "workers the unobserved remainder is at most ~45 worker-minutes, and only if "
            "every worker computed through the entire final interval."),
        max_unobserved_worker_min=45,
        cpu_source="sampled while the pool was alive. Worker CPU is unreadable once the "
                   "pool exits, and pool workers are never recycled here, so cumulative CPU "
                   "only grows.",
        worker_filter="cmdline contains 'spawn_main' — exact: it matches the 9 pool workers "
                      "and NOT multiprocessing's resource_tracker, the sampler script, or "
                      "any other child. Verified against the live pool before arming.",
        stopped_wall_sources=[
            ART.file_digest("GANN_CAPABILITY_EXECUTION_INTERRUPTION_V1.json"),
            ART.file_digest(PAUSE_V2)],
        four_concepts_are_separate="wall_elapsed, stopped_wall, active_wall and "
                                   "aggregate_worker_cpu_last_sample are distinct and none "
                                   "is derived into runtime_min")
    d["runtime_min_warning"] = (
        "runtime_min / runtime_hours as written by the runner INCLUDES the execution "
        "interruption and MUST NOT be interpreted as active compute duration. The field is "
        "left exactly as the runner wrote it — it is a legacy/raw execution field. It is "
        "explicitly NOT reassigned: runtime_min := active_wall was NOT performed, because "
        "that would be a post-hoc semantic rewrite. Use time_provenance instead.")
    d["amended_by"] = ("gann_time_provenance.py — additive only; no value written by the "
                       "runner was changed")
    dg = ART.seal(d, RESULT, required=("spec_id", "detections", "integrity", "hard_state"),
                  supersede=True)
    tp = d["time_provenance"]
    print(f"  wall {tp['wall_elapsed_min']:.0f}m · stopped {tp['stopped_wall_min']:.0f}m · "
          f"active {tp['active_wall_min']:.0f}m")
    print(f"  worker CPU (last sample, LOWER BOUND) "
          f"{tp['aggregate_worker_cpu_last_sample_min']:.0f}m over "
          f"{tp['n_pool_workers']} workers · lag to teardown "
          f"{tp['lag_to_teardown_min']}m · counts seen {tp['worker_counts_seen']}")
    print(f"GANN_CAPABILITY_RESULT_V1 (time provenance added) · {dg}")


if __name__ == "__main__":
    main()
