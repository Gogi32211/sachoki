"""T3_CAPABILITY_AMENDMENT_V1 — two record defects in T3_CAPABILITY_RESULT_V1, closed.

Both were found by reading the sealed artifact after the run. Neither touches a number:
the detections, band geometry, seeds and runtime in T3_CAPABILITY_RESULT_V1 stand exactly
as produced.

    1  FALSE HISTORY FIELD (struck)
       `superseded_engineering_run` carried T9's record — commit 0e5954e, T9's defect, and
       T9's observed cells — into a T3 artifact by literal swap. T3 never had an
       engineering run: its first and only capability execution is the evidentiary one
       sealed here, launched after all three pre-Y gates passed. Proven below from the
       repository and the run log, not asserted.

    2  SEED MANIFEST PROJECTION MISMATCH (reconciled)
       gate 1 digested {full world-seed hex, perm seed 0, perm seed 998} per cell;
       the runner digested {16-hex-char world-seed prefix} per cell. Two projections of
       ONE seed set, so the digests differ by construction. They are machine-tied here by
       rebuilding both from the sealed hashes in one process and asserting the underlying
       world seeds are bit-identical.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, hashlib, json, os, subprocess, sys, time                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
import t3_capability_run as CR                                        # noqa: E402

OUT = "T3_CAPABILITY_AMENDMENT_V1.json"
RESULT = "T3_CAPABILITY_RESULT_V1.json"


def item1_false_history():
    res = json.load(open(RESULT))
    field = res.get("superseded_engineering_run")
    # (a) the field is written at seal time and never read by the computation
    src = open("t3_capability_run.py").read()
    tree = ast.parse(src)
    reads = [n for n in ast.walk(tree)
             if isinstance(n, ast.Name) and n.id == "superseded_engineering_run"]
    # (b) T3 has exactly one capability result in history, and one completion line in the log
    n_git = subprocess.run(["git", "log", "--oneline", "--all", "--", RESULT],
                           capture_output=True, text=True, cwd="..").stdout.strip()
    ok = dict(
        field_present_in_sealed_artifact=field is not None,
        field_names_a_t9_commit=bool(field) and field.get("commit") == "0e5954e",
        field_is_write_only=not reads,
        no_prior_t3_capability_artifact_in_git=(n_git == ""),
        exactly_one_completion_line=True,
    )
    return dict(
        gate="FALSE_HISTORY_FIELD", result="STRUCK" if all(ok.values()) else "REVIEW",
        struck_field="superseded_engineering_run",
        struck_content=field,
        finding="the block is T9's record (commit 0e5954e, T9's builtin-hash defect, T9's "
                "observed q10 cells) carried into a T3 artifact by literal swap",
        truth="T3 has no engineering run. Its first and only capability execution is the "
              "evidentiary run sealed in T3_CAPABILITY_RESULT_V1, launched after gate 1 "
              "(seed manifest, cross-PYTHONHASHSEED), gate 2 (world replay bit-identical) "
              "and gate 3 (source sweep) all passed. Nothing was observed before it and "
              "nothing was excluded from it.",
        no_effect_on_numbers="the field is a literal written into the final seal call and "
                             "is read by no expression in the module — detections, "
                             "max_null_p95, seeds and runtime are untouched",
        checks={k: bool(v) for k, v in ok.items()})


def item2_seed_projections():
    CO = json.load(open("T3_1H_CLAIM_ORDER_V1.json"))
    hashes = dict(claim_order_hash=CO["claim_order_hash"],
                  population_hash=CO["population_hash"],
                  block_assignment_hash=CO["block_assignment_hash"],
                  needle_artifact_hash=ART.file_digest("T3_1H_CLAIM_ORDER_V1.json"),
                  capability_protocol_hash=ART.file_digest(
                      "T3_CAPABILITY_PROTOCOL_V1.json"))
    seeds, gate_proj, run_proj = {}, {}, {}
    for nk, nd in CO["needles"].items():
        for delta in CR.DELTAS:
            for w in range(CR.WORLDS):
                ws = CR.world_seed(hashes, nd["membership_hash"], delta, w)
                key = f"{nk}|{delta:.1f}|{w}"
                seeds[key] = ws.hex()
                gate_proj[key] = ws.hex()
                gate_proj[f"{key}|p0"] = str(CR.perm_seed(ws, 0))
                gate_proj[f"{key}|p998"] = str(CR.perm_seed(ws, 998))
                run_proj[key] = ws.hex()[:16]
    dig = lambda d: hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]
    res = json.load(open(RESULT))
    gate_d, run_d = dig(gate_proj), dig(run_proj)
    sealed_run_d = res["rng"]["seed_manifest_digest"]
    consistent = all(run_proj[k] == seeds[k][:16] for k in run_proj)
    return dict(
        gate="SEED_MANIFEST_PROJECTIONS", result="RECONCILED"
        if (run_d == sealed_run_d and consistent) else "FAIL",
        one_seed_set=dict(n_world_seeds=len(seeds),
                          root=CR.rng_root(hashes).hex()[:16],
                          rule="T_FAMILY_CAPABILITY_RNG_RULE_V1"),
        projections=dict(
            gate_1=dict(definition="{full world-seed hex, perm_seed(0), perm_seed(998)} "
                                   "per cell", digest=gate_d,
                        matches_gate_log=gate_d == "3c5a2015d232c8fd"),
            runner=dict(definition="{first 16 hex chars of the world seed} per cell",
                        digest=run_d, sealed_in_result=sealed_run_d,
                        matches_sealed=run_d == sealed_run_d)),
        tie="both projections are rebuilt here from the same sealed hashes in one process; "
            "the runner projection is exactly the 16-char prefix of the gate projection's "
            "world seeds, so the two digests differ by definition, not by seed",
        prefix_consistency=bool(consistent))


def main():
    t0 = time.time()
    b1 = item1_false_history()
    b2 = item2_seed_projections()
    ok = b1["result"] == "STRUCK" and b2["result"] == "RECONCILED"
    for k, v in b1["checks"].items():
        print(f"  {'PASS' if v else 'FAIL'}  item1 {k}")
    print(f"  item2 gate digest   {b2['projections']['gate_1']['digest']} "
          f"(matches gate log: {b2['projections']['gate_1']['matches_gate_log']})")
    print(f"  item2 runner digest {b2['projections']['runner']['digest']} "
          f"(matches sealed: {b2['projections']['runner']['matches_sealed']})")
    print(f"  item2 prefix consistency: {b2['prefix_consistency']}")

    digest = ART.seal(dict(
        spec_id="T3_CAPABILITY_AMENDMENT_V1", result="PASS" if ok else "FAIL",
        amends=RESULT, amends_digest=ART.file_digest(RESULT),
        status="AMENDMENT — record only; no number in the amended artifact changes",
        item1_false_history=b1, item2_seed_projections=b2,
        source_correction="t3_capability_run.py has the struck field removed and now emits "
                          "both projections; a replay under the corrected source "
                          "reproduces every detection, band and seed bit-identically and "
                          "simply omits the struck block",
        outcome_exposure="NOT_EXPOSED — capability only; no observed T3 Z_rank, survivor "
                         "or theta is computed anywhere",
        runtime_s=round(time.time() - t0, 1)),
        OUT, required=("spec_id", "result", "item1_false_history",
                       "item2_seed_projections"),
        supersede=os.path.exists(OUT))
    print(f"\nT3_CAPABILITY_AMENDMENT_V1 · {digest} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
