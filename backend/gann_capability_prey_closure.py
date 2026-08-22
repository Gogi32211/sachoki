"""GANN_CAPABILITY_PREY_CLOSURE_V1 — qualify the capability ENGINE before any real Y.

Four PASS, all on SYNTHETIC outcomes:

    A  RNG MANIFEST      every seed derived; PYTHONHASHSEED 0 and 12345 agree; no builtin
                         hash() on a stochastic path
    B  FULL WORLD REPLAY two fresh runs of one world must agree bit-for-bit on the
                         nullized pair, the injected pair, all three Z, the whole 999-long
                         max-Z vector, the p95 and the detection — not just the verdict
    C  PAIRED PERMUTATION the (LONG, SHORT) pair moves as one indivisible row. A fixture is
                         built that would FAIL if the components were permuted separately,
                         and direction is asserted absent from the nullization block key
    D  FORBIDDEN REACH   from the capability entrypoint, the observed-statistic runner,
                         winner ranking, survivor selection and gate-B evaluation are
                         unreachable in the import graph

No real outcome is read: the closure calls synthetic_outcomes() and never load_outcomes().
"""
from __future__ import annotations
import ast, hashlib, importlib, json, os, subprocess, sys, time      # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_capability as CAP                                        # noqa: E402

OUT = "GANN_CAPABILITY_PREY_CLOSURE_V1.json"
SAMPLE_ROWS = 40000          # enough to exercise every path; the closure tests the ENGINE

MANIFEST_SNIPPET = r'''
import os, sys, json, hashlib
os.chdir(%r); sys.path.insert(0, %r)
import gann_capability as CAP
man = CAP.seed_manifest(CAP.rng_root())
print(hashlib.sha256(json.dumps({k: str(v) for k, v in man.items()},
                                sort_keys=True).encode()).hexdigest()[:16])
'''


def gate_a():
    digests = []
    for phs in ("0", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=phs)
        r = subprocess.run([".venv/bin/python", "-c", MANIFEST_SNIPPET % (HERE, HERE)],
                           capture_output=True, text=True, env=env, cwd=HERE, timeout=300)
        assert r.returncode == 0, r.stderr[-400:]
        digests.append(r.stdout.strip().splitlines()[-1])
    src = open("gann_capability.py").read()
    import re
    bad = [f"{i}: {l.strip()[:60]}" for i, l in enumerate(src.split("\n"), 1)
           if re.search(r"(?<![\w.])hash\(", l.split("#")[0])
           and "membership_hash" not in l]
    ok = digests[0] == digests[1] and not bad
    n_seeds = len(CAP.seed_manifest(CAP.rng_root()))
    print(f"A RNG MANIFEST      {'PASS' if ok else 'FAIL'} · PYTHONHASHSEED 0 -> "
          f"{digests[0]} · 12345 -> {digests[1]} · seeds {n_seeds:,} · builtin hash() "
          f"{len(bad)}")
    return ok, dict(seed_manifest_digest=digests[0], cross_process_identical=digests[0]
                    == digests[1], n_seeds=n_seeds, builtin_hash_hits=bad)


def gate_b(state, root):
    lo, sh = CAP.synthetic_outcomes(state["n"], seed=11)
    r1 = CAP.run_world(state, lo, sh, 2.0, root, "ASC", 0)
    r2 = CAP.run_world(state, lo, sh, 2.0, root, "ASC", 0)
    same = dict(
        nullized_long=bool(np.array_equal(r1["nullized_long"], r2["nullized_long"])),
        nullized_short=bool(np.array_equal(r1["nullized_short"], r2["nullized_short"])),
        injected_long=bool(np.array_equal(r1["injected_long"], r2["injected_long"])),
        injected_short=bool(np.array_equal(r1["injected_short"], r2["injected_short"])),
        z_obs=bool(r1["z_obs"] == r2["z_obs"]),
        maxz_vector=bool(np.array_equal(r1["maxz"], r2["maxz"])),
        p95=bool(r1["p95"] == r2["p95"]),
        detection=bool(r1["detected"] == r2["detected"]))
    ok = all(same.values())
    dig = hashlib.sha256(np.concatenate([r1["nullized_long"], r1["nullized_short"],
                                         r1["injected_long"], r1["injected_short"],
                                         r1["maxz"]]).tobytes()).hexdigest()[:16]
    print(f"B FULL WORLD REPLAY {'PASS' if ok else 'FAIL'} · " +
          " · ".join(f"{k}={v}" for k, v in same.items()))
    return ok, dict(bit_identical=same, replay_digest=dig, z_obs=r1["z_obs"],
                    p95=r1["p95"], detected=r1["detected"],
                    perm_vector_length=len(r1["maxz"]))


def gate_c(state, root):
    """A fixture that FAILS if LONG and SHORT are permuted independently."""
    n = state["n"]
    # LONG = k, SHORT = 10*k: the pair identity is recoverable from the ratio alone
    k = np.arange(1, n + 1, dtype=float)
    lo, sh = k, 10.0 * k
    nl, ns, idx = CAP.nullize_pair(lo, sh, state["nullblock"], state["n_nullblock"],
                                   CAP._seed(CAP.NULL_NS, root, "FIXTURE", 0, 0))
    ratio_ok = bool(np.allclose(ns, 10.0 * nl))
    moved = int((idx != np.arange(n)).sum())
    # the negative control: independent permutation must break the fixture
    rng = np.random.default_rng(5)
    broken = bool(np.allclose(sh[rng.permutation(n)], 10.0 * nl))
    # direction must not be a nullization key
    src = open("gann_capability.py").read()
    tree = ast.parse(src)
    fn = [x for x in ast.walk(tree) if isinstance(x, ast.FunctionDef)
          and x.name == "load_state"][0]
    nullkey = [s.value for s in ast.walk(fn) if isinstance(s, ast.Constant)
               and isinstance(s.value, str) and "|" in s.value]
    dir_in_key = any("dir" in str(x).lower() for x in nullkey)
    checks = dict(pair_ratio_preserved=ratio_ok, rows_actually_moved=moved > 0,
                  independent_permutation_would_break_it=not broken,
                  direction_not_a_nullization_block=not dir_in_key)
    ok = all(checks.values())
    print(f"C PAIRED PERMUTATION {'PASS' if ok else 'FAIL'} · pair ratio preserved "
          f"{ratio_ok} · rows moved {moved:,} · independent-permutation control "
          f"{'breaks as expected' if not broken else 'DID NOT BREAK'} · direction in "
          f"null key {dir_in_key}")
    return ok, dict(checks=checks, rows_moved=moved,
                    nullization_block_key="date | liq_half | vol_half — no direction",
                    fixture="LONG = k, SHORT = 10k; the pair identity is recoverable from "
                            "the ratio, so an independent permutation of the components "
                            "is detected exactly")


def gate_d():
    """Static reachability from the capability entrypoint."""
    forbidden_modules = {"gann_historical", "gann_expose", "gann_survivor",
                         "gann_phase_qualification", "gann_phase"}
    seen, stack = set(), ["gann_capability"]
    edges = {}
    while stack:
        mod = stack.pop()
        if mod in seen or not os.path.exists(f"{mod}.py"):
            continue
        seen.add(mod)
        tree = ast.parse(open(f"{mod}.py").read())
        deps = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                deps |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.module:
                deps.add(n.module.split(".")[0])
        local = {d for d in deps if os.path.exists(f"{d}.py")}
        edges[mod] = sorted(local)
        stack += list(local)
    reachable = seen
    hits = sorted(reachable & forbidden_modules)
    # identifiers only. The words "survivor" and "ranking" appear in the module's own
    # PROHIBITION text; a docstring saying what must not happen is not a code path.
    tree = ast.parse(open("gann_capability.py").read())
    idents = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name):
            idents.add(n.id.lower())
        elif isinstance(n, ast.Attribute):
            idents.add(n.attr.lower())
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            idents.add(n.name.lower())
    banned_terms = sorted(i for i in idents
                          if any(t in i for t in ("historical", "winner", "survivor",
                                                  "ranking", "gate_b", "placebo",
                                                  "expose")))
    ok = not hits and not banned_terms
    print(f"D FORBIDDEN REACH   {'PASS' if ok else 'FAIL'} · modules reachable "
          f"{sorted(reachable)} · forbidden hits {hits or 0} · banned terms "
          f"{banned_terms or 0}")
    return ok, dict(reachable_modules=sorted(reachable), import_edges=edges,
                    forbidden_modules=sorted(forbidden_modules),
                    forbidden_reachable=hits,
                    banned_identifiers_in_code=banned_terms,
                    identifier_scope="AST identifiers only — the module's own prohibition "
                                     "docstring names these concepts and must not be read "
                                     "as a code path",
                    note="the historical statistic lives in a module the capability engine "
                         "does not import; there is also no code path here that computes Z "
                         "before nullization")


def main():
    t0 = time.time()
    root = CAP.rng_root()
    state = CAP.load_state("ASC", limit_rows=SAMPLE_ROWS)
    print(f"synthetic qualification on {state['n']:,} rows · treated "
          f"{int(state['treated'].sum()):,} · blocks {state['nb']:,} · nullization blocks "
          f"{state['n_nullblock']:,}\n", flush=True)
    okA, A = gate_a()
    okB, B = gate_b(state, root)
    okC, C = gate_c(state, root)
    okD, D = gate_d()
    ok = okA and okB and okC and okD
    d = ART.seal(dict(
        spec_id="GANN_CAPABILITY_PREY_CLOSURE_V1", result="PASS" if ok else "FAIL",
        scope="engine qualification on SYNTHETIC outcomes; no real Y is read anywhere",
        capability_code=ART.file_digest("gann_capability.py"),
        rank_code=ART.file_digest("gann_rank.py"),
        estimand_state=ART.file_digest("GANN_ESTIMAND_STATE_V1.json"),
        protocol=ART.file_digest("GANN_CAPABILITY_PROTOCOL_V1.json"),
        inference_amendment=ART.file_digest("GANN_INFERENCE_AMENDMENT_V1.json"),
        rng_root=root.hex()[:16],
        sample=dict(rows=state["n"], treated=int(state["treated"].sum()),
                    comparison_blocks=state["nb"],
                    nullization_blocks=state["n_nullblock"]),
        A_rng_manifest=A, B_full_world_replay=B, C_paired_permutation=C,
        D_forbidden_reachability=D,
        outcome_exposure="NOT_EXPOSED — synthetic outcomes only",
        runtime_min=round((time.time() - t0) / 60, 1)),
        OUT, required=("spec_id", "result", "A_rng_manifest", "B_full_world_replay",
                       "C_paired_permutation", "D_forbidden_reachability"),
        supersede=os.path.exists(OUT))
    print(f"\nGANN_CAPABILITY_PREY_CLOSURE_V1 · {d} · {'PASS' if ok else 'FAIL'} · "
          f"{(time.time()-t0)/60:.1f} min")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
