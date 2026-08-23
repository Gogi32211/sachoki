"""GANN_CAPABILITY_RESULT time provenance — run AFTER the result seals.

The runner records one time field, `runtime_min`, taken from its own t0. That clock ran
through the 11.8-hour execution interruption, so it is NOT active compute duration and must
never be read as one. It is left exactly as the runner wrote it — its meaning is not
rewritten after the fact — and the four honest fields are ADDED beside it:

    wall_elapsed          the same clock the runner used
    stopped_wall          time the run spent in state T/TN
    active_wall           wall_elapsed - stopped_wall
    aggregate_worker_cpu  summed across the 9 pool workers

aggregate_worker_cpu comes from a sampler that ran WHILE the pool was alive: worker CPU time
is unreadable once the pool tears down, and pool workers are never recycled here, so their
cumulative CPU only grows and the last sample before teardown is the honest aggregate. It is
labelled as a last-sample figure, not claimed as an exact total.

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
STOPPED_WALL_MIN = 708.0          # 22:06 -> 09:54, established by the interruption record


def main():
    if not os.path.exists(RESULT):
        raise SystemExit(f"{RESULT} does not exist yet — this module runs AFTER the seal.")
    d = json.load(open(RESULT))
    if "time_provenance" in d:
        print("time provenance already recorded"); return

    rows = [l.split("\t") for l in open(CPU_TSV).read().strip().split("\n") if l]
    n_workers, cpu_s = int(rows[-1][1]), float(rows[-1][2])
    wall = float(d.get("runtime_hours", 0)) * 60.0 or float(d.get("runtime_min", 0))
    active = wall - STOPPED_WALL_MIN

    d["time_provenance"] = dict(
        wall_elapsed_min=round(wall, 1),
        stopped_wall_min=STOPPED_WALL_MIN,
        active_wall_min=round(active, 1),
        aggregate_worker_cpu_min=round(cpu_s / 60.0, 1),
        n_pool_workers=n_workers,
        cpu_source="sampled every 5 min while the pool was alive; the last sample before "
                   "teardown. Worker CPU is unreadable once the pool exits, and pool workers "
                   "are never recycled here, so cumulative CPU only grows.",
        cpu_caveat="a last-sample figure, not claimed as an exact total",
        stopped_wall_source=ART.file_digest(
            "GANN_CAPABILITY_EXECUTION_INTERRUPTION_V1.json"))
    d["runtime_min_warning"] = (
        "runtime_min / runtime_hours as written by the runner INCLUDES the execution "
        "interruption and MUST NOT be interpreted as active compute duration. The field is "
        "left exactly as the runner wrote it; use time_provenance instead.")
    d["amended_by"] = ("gann_time_provenance.py — additive only; no value written by the "
                       "runner was changed")
    dg = ART.seal(d, RESULT, required=("spec_id", "detections", "integrity", "hard_state"),
                  supersede=True)
    tp = d["time_provenance"]
    print(f"  wall {tp['wall_elapsed_min']:.0f}m · stopped {tp['stopped_wall_min']:.0f}m · "
          f"active {tp['active_wall_min']:.0f}m · worker CPU "
          f"{tp['aggregate_worker_cpu_min']:.0f}m over {tp['n_pool_workers']} workers")
    print(f"GANN_CAPABILITY_RESULT_V1 (time provenance added) · {dg}")


if __name__ == "__main__":
    main()
