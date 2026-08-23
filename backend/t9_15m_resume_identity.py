"""T9_15M_RESUME_IDENTITY_V1 — the gate that must PASS before SIGCONT.

A paused process is only safe to continue if it is still THE SAME process, still holding the
same in-memory outcome, and still governed by the same frozen digests it launched under.
Each of those is checked mechanically here; none is assumed.

    PROCESS      the pid is alive, still stopped, its command line is this run, and its
                 START TIME is unchanged. Start time is what closes PID reuse: a recycled
                 pid necessarily starts AFTER the original died, so a start time earlier
                 than the pause seal cannot belong to an impostor.

    OUTCOME      t9_outcomes.parquet still hashes to what the process read. If anything
                 rebuilt it during the pause, the in-memory y is a different vintage from
                 the file and the run may not continue.

    LEDGER       every row carries that one outcome digest, there are no duplicate
                 (needle, world_id, delta_pp) rows, and the checkpoint that existed at the
                 pause is still exactly the same cell.

    FROZEN       protocol, estimand, claim order, engine state and the derived RNG root all
                 still hash to what they hashed at launch.

    CODE         the working tree may have moved on; the RUNNING process cannot re-read it.
                 So provenance binds LAUNCH-TIME module digests, captured while the process
                 was still the only reader of them, and this gate reports per module whether
                 disk still matches. A changed file is recorded, never silently adopted.

NO DETECTION VALUE IS READ. The ledger is opened on identity columns only — needle,
world_id, delta_pp, outcome_source_digest. needle_Z, band_p95, band_median, band_max and
detected are never selected.
"""
from __future__ import annotations
import json, os, subprocess, sys, time                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import pandas as pd, psutil                                             # noqa: E402
import t5_artifact as ART                                               # noqa: E402
import t15m_gates as G                                                  # noqa: E402
import t9_sequence_estimand as E9                                       # noqa: E402

BASELINE = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
            "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/t9_launch_baseline.json")
LEDGER = os.path.join(os.path.dirname(HERE), "data", "t9_15m_capability_ledger.parquet")
IDENT_COLS = ["needle", "world_id", "delta_pp", "outcome_source_digest"]


def _ps(pid, fmt):
    return subprocess.run(["ps", "-o", fmt, "-p", str(pid)],
                          capture_output=True, text=True).stdout.strip()


def main():
    t0 = time.time()
    # PRE_CHECK is a dry run while the process is still paused; AT_RESUME is the operative
    # seal, taken immediately before SIGCONT. The distinction is recorded so a PASS taken
    # hours earlier can never be mistaken for the gate that actually authorised the resume.
    occasion = (sys.argv[1] if len(sys.argv) > 1 else "PRE_CHECK").upper()
    assert occasion in ("PRE_CHECK", "AT_RESUME"), occasion
    B = json.load(open(BASELINE))
    PAUSE = json.load(open("T9_15M_EXECUTION_PAUSE_V1.json"))
    pid = B["pid"]

    # ── PROCESS ─────────────────────────────────────────────────────────────
    lstart, state, cmd = _ps(pid, "lstart="), _ps(pid, "state="), _ps(pid, "command=")
    # OS-level start identity: the kernel's own microsecond start stamp. ps lstart is only
    # second-resolution, so two processes could in principle share it; proc_pidinfo's
    # pbi_start_tvsec/tvusec (what psutil.create_time reads on macOS, the equivalent of
    # Linux /proc/<pid>/stat start ticks) closes PID reuse far more tightly.
    osid = B.get("os_start_identity") or {}
    try:
        _p = psutil.Process(pid)
        ct_now, ppid_now, pstatus = _p.create_time(), _p.ppid(), _p.status()
    except Exception:
        ct_now, ppid_now, pstatus = None, None, "GONE"
    proc = dict(
        os_start_identity=dict(
            expected=osid.get("create_time"), observed=repr(ct_now),
            matches=(ct_now is not None
                     and repr(ct_now) == osid.get("create_time")),
            resolution="microsecond", ppid_at_launch=osid.get("ppid"), ppid_now=ppid_now,
            psutil_status=pstatus,
            source=osid.get("source")),
        pid=pid, alive=bool(state), state=state,
        start_time_now=lstart, start_time_at_launch=B["lstart"],
        start_time_matches=lstart == B["lstart"],
        still_stopped=state.startswith("T"),
        command_matches=("t15m_capability.py" in cmd and " t9" in f" {cmd} "),
        pid_reuse_excluded_because="a recycled pid necessarily starts after the original "
                                   "died; an unchanged start time cannot be an impostor")

    # ── OUTCOME ─────────────────────────────────────────────────────────────
    expect_y = PAUSE["resume_identity_check"]["outcome_source_digest"]
    y_now = ART.file_digest(E9.OC)
    outcome = dict(expected=expect_y, observed=y_now, matches=y_now == expect_y,
                   source=os.path.basename(E9.OC))

    # ── LEDGER (identity columns only) ──────────────────────────────────────
    L = pd.read_parquet(LEDGER, columns=IDENT_COLS)
    keys = sorted({(r.needle, int(r.world_id)) for r in L.itertuples()})
    exp_keys = [(c["needle"], c["world_id"]) for c in
                B["checkpoint_at_pause"]["checkpoint_key"]]
    ledger = dict(
        rows=int(len(L)),
        completed_cells=len(keys),
        checkpointed_at_pause=B["checkpoint_at_pause"]["checkpointed_at_pause"],
        checkpoint_key=[{"needle": n, "world_id": w} for n, w in keys],
        checkpoint_identity_reproduced=keys == exp_keys,
        duplicate_needle_world_delta=int(
            len(L) - len(L.drop_duplicates(["needle", "world_id", "delta_pp"]))),
        all_rows_carry_expected_outcome_digest=bool(
            set(L.outcome_source_digest) == {expect_y}),
        distinct_outcome_digests=sorted(set(L.outcome_source_digest)),
        detection_values_read="NO",
        ledger_payload_read="identity columns only — " + ", ".join(IDENT_COLS),
        note="the earlier progress message that said 0 checkpoints was written before this "
             "cell landed; the correct count at the pause is recorded here and in the "
             "baseline, so that report stays traceable rather than silently corrected")

    # ── FROZEN DIGESTS ──────────────────────────────────────────────────────
    frozen = {}
    for a, was in B["artifacts"].items():
        frozen[a] = dict(at_launch=was, now=ART.file_digest(a),
                         matches=ART.file_digest(a) == was)
    rng_now = G.rng_root("T9").hex()[:16]
    frozen["rng_root"] = dict(at_launch=B["rng_root"], now=rng_now,
                              matches=rng_now == B["rng_root"])

    # ── CODE PROVENANCE ─────────────────────────────────────────────────────
    code = {}
    for m, v in B["modules"].items():
        now = ART.file_digest(m) if os.path.exists(m) else None
        code[m] = dict(launch_time_digest=v["digest"], working_tree_now=now,
                       working_tree_unchanged=now == v["digest"])
    code_note = ("provenance binds the LAUNCH-TIME digests above. The running process cannot "
                 "re-read the working tree, so a file that changed during the pause does not "
                 "alter this run's semantics — but it is recorded, never silently adopted.")

    checks = dict(
        process_alive_and_stopped=proc["alive"] and proc["still_stopped"],
        process_start_time_matches=proc["start_time_matches"],
        os_start_identity_matches=proc["os_start_identity"]["matches"],
        process_command_matches=proc["command_matches"],
        outcome_digest_unchanged=outcome["matches"],
        ledger_single_outcome_vintage=ledger["all_rows_carry_expected_outcome_digest"],
        no_duplicate_cells=ledger["duplicate_needle_world_delta"] == 0,
        checkpoint_identity_reproduced=ledger["checkpoint_identity_reproduced"],
        frozen_digests_unchanged=all(v["matches"] for v in frozen.values()))
    ok = all(checks.values())

    print(f"T9_15M_RESUME_IDENTITY [{occasion}] · pid {pid} · state {state or 'GONE'}")
    print(f"  start time  launch {B['lstart']} · now {lstart or '—'} · "
          f"{'MATCH' if proc['start_time_matches'] else 'MISMATCH'}")
    _o = proc["os_start_identity"]
    print(f"  kernel stamp {_o['expected']} vs {_o['observed']} · "
          f"{'MATCH' if _o['matches'] else 'MISMATCH'} (microsecond)")
    print(f"  outcome     {expect_y} · {'MATCH' if outcome['matches'] else 'MISMATCH'}")
    print(f"  ledger      {ledger['rows']} rows · {ledger['completed_cells']} cell(s) "
          f"{ledger['checkpoint_key']} · dup {ledger['duplicate_needle_world_delta']}")
    for m, v in code.items():
        if not v["working_tree_unchanged"]:
            print(f"  CODE CHANGED SINCE LAUNCH (not adopted by the running process): {m}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")

    d = ART.seal(dict(
        spec_id="T9_15M_RESUME_IDENTITY_V1", status="EXECUTION_CLOSURE", family="T9",
        result="PASS" if ok else "FAIL",
        gate_occasion=occasion,
        occasion_meaning=("dry run while still paused — NOT the gate that authorises SIGCONT"
                          if occasion == "PRE_CHECK" else
                          "taken immediately before SIGCONT — this is the operative gate"),
        purpose="the gate that must pass before SIGCONT",
        pause_artifact=ART.file_digest("T9_15M_EXECUTION_PAUSE_V1.json"),
        process=proc, outcome=outcome, ledger=ledger, frozen_digests=frozen,
        code_provenance=dict(modules=code, binding=code_note,
                             baseline_captured_at=B["captured_at"]),
        checks=checks,
        on_pass="SIGCONT -> same process -> same in-memory Y -> same RNG progression -> "
                "the resume skips exactly the checkpointed work",
        outcome_exposure="NOT_EXPOSED — file hashes and identity columns only; no detection "
                         "value was selected from the ledger",
        hard_state={"T9 15M OBSERVED Z": "NOT COMPUTED",
                    "T9 15M theta": "NOT COMPUTED",
                    "T9 15M WINNERS/SURVIVORS": "UNKNOWN"},
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"),
        runtime_min=round((time.time() - t0) / 60, 2)),
        "T9_15M_RESUME_IDENTITY_V1.json",
        required=("spec_id", "result", "process", "outcome", "ledger", "checks"),
        supersede=os.path.exists("T9_15M_RESUME_IDENTITY_V1.json"))
    print(f"\nT9_15M_RESUME_IDENTITY_V1 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
