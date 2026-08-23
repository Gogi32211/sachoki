"""GANN_CAPABILITY_RESUME_IDENTITY_V2 — the gate before GANN's SIGCONT.

Same discipline as T9's, adapted to a pool: the parent AND all nine workers must still be
the processes that were paused, proven by kernel start stamps recorded in PAUSE_V2 while
they were alive.

    parent PID / create_time            SAME
    9 worker PID / create_time          SAME
    all workers stopped before resume   PASS   — resuming a partly-running pool would mean
                                                 something else had already touched it
    launch-time input digests           SAME
    protocol / estimand / RNG root      SAME
    no restart · no reseed              PASS
    SIGCONT issued ONLY after PASS

CHECKPOINT CONTINUITY CANNOT BE CHECKED, and is not claimed. GANN writes nothing until all
360 worlds finish, so there is no on-disk state to compare. The limitation is already
recorded; it is repeated here so a PASS is not mistaken for more than it is.

This module also COMPLETES the pause record: a pause has no duration until it ends, so
resumed_at and duration_min are written back into PAUSE_V2 at resume. Without that,
gann_time_provenance refuses to run — by design, so the second pause cannot be silently
dropped from stopped_wall.

    usage:  python gann_resume_identity.py [--issue-sigcont]
"""
from __future__ import annotations
import json, os, signal, sys, time                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import psutil                                                          # noqa: E402
import t5_artifact as ART                                              # noqa: E402
import gann_capability as CAP                                          # noqa: E402

PAUSE_V2 = "GANN_CAPABILITY_EXECUTION_PAUSE_V2.json"
READ_AT_LAUNCH = ["gann_capability_run.py", "gann_capability.py", "gann_rank.py",
                  "gann_fast.py", "t5_artifact.py",
                  "../data/gann_outcomes.parquet", "../data/gann_estimand_state.parquet"]


def main():
    issue = "--issue-sigcont" in sys.argv
    P = json.load(open(PAUSE_V2))
    st = P["state_at_pause"]
    ppid = st["parent_pid"]

    try:
        par = psutil.Process(ppid)
        par_ct, par_state = repr(par.create_time()), par.status()
        alive = True
    except psutil.NoSuchProcess:
        par_ct, par_state, alive = None, "GONE", False

    workers, all_same, all_stopped = [], True, True
    for pid, ct in zip(st["worker_pids"], st["worker_create_times"]):
        try:
            w = psutil.Process(pid)
            now_ct, now_st = repr(w.create_time()), w.status()
        except psutil.NoSuchProcess:
            now_ct, now_st = None, "GONE"
        same = now_ct == ct
        stopped = now_st == "stopped"
        all_same &= same
        all_stopped &= stopped
        workers.append(dict(pid=pid, create_time_at_pause=ct, create_time_now=now_ct,
                            identity_matches=same, status=now_st, was_stopped=stopped))

    inputs = {os.path.basename(f): ART.file_digest(f) for f in READ_AT_LAUNCH}
    launch_same = all(
        inputs[os.path.basename(f)] ==
        json.load(open("GANN_CAPABILITY_RESUME_CONTINUITY_V1.json"))["read_at_launch"]
        [os.path.basename(f)]["digest"] for f in READ_AT_LAUNCH)
    root = CAP.rng_root().hex()[:16]
    rng_same = root == json.load(
        open("GANN_CAPABILITY_RESUME_CONTINUITY_V1.json"))["rng"]["root"]
    y_same = (ART.file_digest("../data/gann_outcomes.parquet")
              == P["resume_identity_check"]["outcome_source_digest"])

    checks = dict(
        parent_alive=alive,
        parent_stopped_before_resume=par_state == "stopped",
        parent_create_time_matches=par_ct == st["parent_create_time"],
        nine_workers_accounted=len(workers) == 9,
        all_worker_identities_match=all_same,
        all_workers_stopped_before_resume=all_stopped,
        launch_time_inputs_unchanged=launch_same,
        outcome_source_digest_unchanged=y_same,
        rng_root_unchanged=rng_same)
    ok = all(checks.values())

    issued_at, duration_min = None, None
    if issue:
        if not ok:
            raise SystemExit("identity gate did not pass — SIGCONT NOT issued")
        os.kill(ppid, signal.SIGCONT)
        for w in st["worker_pids"]:
            try:
                os.kill(w, signal.SIGCONT)
            except ProcessLookupError:
                pass
        issued_at = time.strftime("%Y-%m-%d %H:%M:%S %Z")
        t_susp = time.mktime(time.strptime(
            P["pause"]["suspended_at"].rsplit(" ", 1)[0], "%Y-%m-%d %H:%M:%S"))
        duration_min = round((time.time() - t_susp) / 60.0, 1)
        # a pause has no duration until it ends — complete the record
        P["pause"]["resumed_at"] = issued_at
        P["pause"]["duration_min"] = duration_min
        P["pause"]["completed_by"] = "GANN_CAPABILITY_RESUME_IDENTITY_V2"
        ART.seal(P, PAUSE_V2, required=("spec_id", "pause", "state_at_pause"),
                 supersede=True)
        time.sleep(3)
        print(f"  SIGCONT issued at {issued_at} · pause lasted {duration_min:.0f} min")

    d = ART.seal(dict(
        spec_id="GANN_CAPABILITY_RESUME_IDENTITY_V2", status="EXECUTION_CLOSURE",
        family="GANN", result="PASS" if ok else "FAIL",
        pause_artifact=ART.file_digest(PAUSE_V2),
        parent=dict(pid=ppid, create_time_at_pause=st["parent_create_time"],
                    create_time_now=par_ct, status=par_state),
        workers=workers, launch_time_inputs=inputs, rng_root=root,
        checks=checks,
        authorized_sigcont=bool(ok and issue),
        sigcont_issued_at=issued_at,
        pause_duration_min=duration_min,
        sigcont_semantics="authorized_sigcont records that this gate passed AND the signal "
                          "was sent in the same transaction; a PASS without an issuance "
                          "timestamp means the resume was authorised but not performed",
        checkpoint_continuity=dict(
            checkable=False,
            why="GANN writes nothing until all 360 worlds finish, so there is no on-disk "
                "state to compare across the pause",
            not_claimed="a PASS here says the PROCESSES are identical, not that any "
                        "intermediate result was verified — there are none to verify"),
        no_restart=True, no_reseed=True,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "GANN_CAPABILITY_RESUME_IDENTITY_V2.json",
        required=("spec_id", "result", "parent", "workers", "checks"),
        supersede=os.path.exists("GANN_CAPABILITY_RESUME_IDENTITY_V2.json"))
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"\nGANN_CAPABILITY_RESUME_IDENTITY_V2 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
