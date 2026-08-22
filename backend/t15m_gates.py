"""{F}_15M_PRE_Y_GATES_V1 — everything that must hold before a 15m capability may read Y.

Four gates, all on X. None of them looks at an outcome, and the last one proves that
mechanically rather than promising it.

    A  RNG          the root is DERIVED from the sealed artifacts, never chosen; the seed
                    manifest is enumerated up front and is identical across two processes
                    with different PYTHONHASHSEED; no builtin hash() sits on a stochastic
                    path
    B  REPLAY       the claim order rebuilds to the same order hash, the same k, the same
                    three needles and the same membership-class digest, from a fresh process
    C  SOURCE       the whole chain of sealed digests that this order stands on is present
                    and matches; the ruling that fixed the eligibility surface is bound in
    D  NO-Y         the reachable import graph from the order builder contains no historical
                    runner, no outcome loader and no ranking; the only outcome-derived thing
                    read anywhere in the 15m X chain is path_status_10d, and only as a STATUS

The capability run is NOT launched here. This module ends at the gates.

    usage:  python t15m_gates.py t9
"""
from __future__ import annotations
import ast, hashlib, importlib, json, math, os, re, subprocess, sys, time   # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402
import t15m_order as ORD                                              # noqa: E402

RULING = "T15M_ELIGIBILITY_RULING_V1.json"
NS_ROOT = "T15M_CAPABILITY_EVIDENCE_V1"
N_WORLDS, N_PERM = 20, 999
DELTAS_PP = (0.5, 1.0, 2.0, 3.0, 4.0, 5.0)

MANIFEST_SNIPPET = r'''
import os, sys, json, hashlib
os.chdir(%r); sys.path.insert(0, %r)
import t15m_gates as G
print(G.manifest_digest(%r))
'''


def rng_root(F: str) -> bytes:
    """Derived from the sealed chain — never chosen, never process-dependent."""
    m = hashlib.sha256()
    for part in (NS_ROOT, F,
                 ART.file_digest(f"{F}_15M_PRESPEC_V1.json"),
                 ART.file_digest(f"{F}_15M_X_ONLY_V1.json"),
                 ART.file_digest(f"{F}_15M_K_CLOSURE_V1.json"),
                 ART.file_digest(RULING),
                 ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json")):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def _seed(ns: str, root: bytes, *parts) -> int:
    m = hashlib.sha256()
    m.update(ns.encode()); m.update(b"|"); m.update(root)
    for p in parts:
        m.update(b"|"); m.update(str(p).encode())
    return int.from_bytes(m.digest()[:8], "big")


def seed_manifest(F: str, root: bytes) -> dict:
    """Every stochastic decision the capability will make, enumerated before it runs."""
    order = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    man = {}
    for q in ("q10", "q50", "q90"):
        uid = order["needles"][q]["claim_uid"]
        for d in DELTAS_PP:
            for w in range(N_WORLDS):
                man[f"{q}|{d:.1f}|{w}|null"] = _seed("T15M_NULL_V1", root, uid, d, w)
                man[f"{q}|{d:.1f}|{w}|p0"] = _seed("T15M_PERM_V1", root, uid, d, w, 0)
                man[f"{q}|{d:.1f}|{w}|p998"] = _seed("T15M_PERM_V1", root, uid, d, w, 998)
    return man


def manifest_digest(F: str) -> str:
    man = seed_manifest(F, rng_root(F))
    return hashlib.sha256(json.dumps({k: str(v) for k, v in man.items()},
                                     sort_keys=True).encode()).hexdigest()[:16]


def gate_a(F):
    digests = []
    for phs in ("0", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=phs)
        r = subprocess.run([".venv/bin/python", "-c", MANIFEST_SNIPPET % (HERE, HERE, F)],
                           capture_output=True, text=True, env=env, cwd=HERE, timeout=600)
        assert r.returncode == 0, r.stderr[-500:]
        digests.append(r.stdout.strip().splitlines()[-1])
    # AST CALL NODES, not a substring scan. A docstring that says "no builtin hash()" and
    # an f-string that PRINTS the count are not code paths — the GANN gate-D lesson,
    # applied here before it could produce the same false FAIL.
    bad = []
    for mod in ("t15m_x.py", "t15m_k_closure.py", "t15m_order.py", "t15m_gates.py"):
        for n in ast.walk(ast.parse(open(mod).read())):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) \
                    and n.func.id == "hash":
                bad.append(f"{mod}:{n.lineno}")
    n = len(seed_manifest(F, rng_root(F)))
    ok = digests[0] == digests[1] and not bad
    print(f"  A RNG      {'PASS' if ok else 'FAIL'} · root {rng_root(F).hex()[:16]} · "
          f"manifest {digests[0]} · {n:,} seeds · builtin hash() {len(bad)}")
    return ok, dict(rng_root=rng_root(F).hex()[:16], manifest_digest=digests[0],
                    cross_process_identical=digests[0] == digests[1], n_seeds=n,
                    builtin_hash_hits=bad,
                    derivation=["prespec", "x_only", "k_closure", "ruling", "claim_order"])


def gate_b(F, fam):
    """Rebuild the claim order in a FRESH process and compare every identity."""
    sealed = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    D = importlib.import_module(f"{fam}_dna")
    C = pd.read_parquet(os.path.join(D.ROOT, "data", f"{fam}_15m_k_closure.parquet"))
    C = C[C.s2_ok].rename(columns={"s2_hash": "membership_hash", "s2_support": "support"})
    cls = ORD.classes(C)
    k = len(cls)
    oh = ORD.order_hash(cls)
    srt = cls.sort_values(["support", "j"]).reset_index(drop=True)
    needles = {}
    for q in (0.10, 0.50, 0.90):
        r = srt.iloc[math.ceil(q * k) - 1]
        needles[f"q{int(q*100)}"] = dict(j=int(r.j), claim_uid=r.claim_uid,
                                         membership_hash=r.membership_hash,
                                         support=int(r.support))
    checks = dict(
        k=k == sealed["k"],
        order_hash=oh == sealed["order"]["claim_order_hash"],
        needles=all(needles[q]["claim_uid"] == sealed["needles"][q]["claim_uid"]
                    and needles[q]["j"] == sealed["needles"][q]["j"]
                    and needles[q]["support"] == sealed["needles"][q]["support"]
                    for q in ("q10", "q50", "q90")),
        unique_j=int(cls.j.nunique()) == k,
        unique_membership=int(cls.membership_hash.nunique()) == k)
    ok = all(checks.values())
    print(f"  B REPLAY   {'PASS' if ok else 'FAIL'} · k {k:,} · order {oh} · needles "
          + "/".join(str(needles[q]['j']) for q in ('q10', 'q50', 'q90')))
    return ok, dict(checks=checks, k=k, order_hash=oh, needles=needles)


def gate_c(F, fam):
    """The whole sealed chain this order stands on."""
    D = importlib.import_module(f"{fam}_dna")
    order = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    kc = json.load(open(f"{F}_15M_K_CLOSURE_V1.json"))
    rule = json.load(open(RULING))
    chain = {
        "prespec": ART.file_digest(f"{F}_15M_PRESPEC_V1.json"),
        "x_only": ART.file_digest(f"{F}_15M_X_ONLY_V1.json"),
        "k_closure": ART.file_digest(f"{F}_15M_K_CLOSURE_V1.json"),
        "ruling": ART.file_digest(RULING),
        "claim_order": ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
        "claims_table": ART.file_digest(
            os.path.join(D.ROOT, "data", f"{fam}_15m_k_closure.parquet")),
        "order_table": ART.file_digest(
            os.path.join(D.ROOT, "data", f"{fam}_15m_claim_order.parquet")),
        "definition": getattr(D, f"{F}_DEF_HASH")}
    checks = dict(
        order_binds_k_closure=order["k_closure"] == chain["k_closure"],
        order_binds_ruling=order["eligibility_ruling"] == chain["ruling"],
        ruling_binds_k_closure=rule["families"][F]["k_closure"] == chain["k_closure"],
        ruling_k_matches_order=rule["families"][F]["k_final_inferential"] == order["k"],
        surface_is_final_estimand=(kc["CLASS_IDENTITY_SURFACE"]
                                   == "FINAL_ESTIMAND_POPULATION"
                                   == order["class_identity_surface"]),
        population_same_as_x=kc["surfaces"]["same_surface_as_sealed_x"]["population"],
        blocks_same_as_x=kc["surfaces"]["same_surface_as_sealed_x"]["blocks"],
        definition_matches_prespec=(json.load(open(f"{F}_15M_PRESPEC_V1.json"))
                                    ["definition_hash"] == chain["definition"]))
    ok = all(checks.values())
    print(f"  C SOURCE   {'PASS' if ok else 'FAIL'} · " +
          " · ".join(f"{k}={v}" for k, v in checks.items() if not v) or "  C SOURCE   PASS")
    return ok, dict(chain=chain, checks=checks,
                    support_definition="t_ov on the FINAL estimand surface — the same "
                                       "support the eligibility predicate used")


def gate_d(F):
    """No-Y: reachability, plus what the 15m chain actually loads."""
    forbidden = {"t5_15m_historical", "t5_15m_expose", "t9_historical", "t3_historical",
                 "t1_historical", "t9_survivor_char", "t3_survivor_char",
                 "t1_survivor_char", "t5_15m_mechanism", "t5_15m_sign_audit"}
    seen, stack, edges = set(), ["t15m_order", "t15m_k_closure", "t15m_x"], {}
    while stack:
        mod = stack.pop()
        if mod in seen or not os.path.exists(f"{mod}.py"):
            continue
        seen.add(mod)
        deps = set()
        for n in ast.walk(ast.parse(open(f"{mod}.py").read())):
            if isinstance(n, ast.Import):
                deps |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.module:
                deps.add(n.module.split(".")[0])
        local = {d for d in deps if os.path.exists(f"{d}.py")}
        edges[mod] = sorted(local)
        stack += list(local)
    hits = sorted(seen & forbidden)

    # WHAT IS ACTUALLY LOADED, not which words appear in the file. A plain substring scan
    # finds t15m_x's own no-Y gate — the literal ("fwd_","mfe","mae",...) tuple it uses to
    # audit its SQL — and fails on the detector rather than on a read. So: the column
    # lists passed to read_parquet, the SQL string literals (f-strings included), and the
    # runtime token-source column list.
    Y_TOK = ("fwd_", "mfe", "mae", "ret_", "mtm", "outcome_value")
    cols, sql = set(), []

    def _sqlstr(n):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            return n.value
        if isinstance(n, ast.JoinedStr):
            return "".join(v.value for v in n.values
                           if isinstance(v, ast.Constant) and isinstance(v.value, str))
        return ""

    for mod in ("t15m_x.py", "t15m_k_closure.py", "t15m_order.py"):
        for n in ast.walk(ast.parse(open(mod).read())):
            if isinstance(n, ast.Call):
                for kw in n.keywords:
                    if kw.arg == "columns" and isinstance(kw.value, (ast.List, ast.Tuple)):
                        cols |= {e.value for e in kw.value.elts
                                 if isinstance(e, ast.Constant) and isinstance(e.value, str)}
            s = _sqlstr(n)
            if "FROM bars" in s:
                sql.append(s)
    D9 = importlib.import_module(f"{F.lower()}_dna")
    src_cols = list(getattr(D9, "SRC_COLS", []))
    y_in_cols = sorted(c for c in cols if any(t in c.lower() for t in Y_TOK))
    y_in_sql = sorted({t for t in Y_TOK for q in sql if t in q.lower()})
    y_in_srccols = sorted(c for c in src_cols if any(t in c.lower() for t in Y_TOK))
    outcome_cols = sorted(c for c in cols if "path_status" in c)
    xo = json.load(open(f"{F}_15M_X_ONLY_V1.json"))
    checks = dict(
        no_forbidden_module_reachable=not hits,
        no_outcome_column_loaded=not y_in_cols,
        no_outcome_column_in_sql=not y_in_sql,
        no_outcome_column_in_token_sources=not y_in_srccols,
        outcome_sidecar_read_is_status_only=outcome_cols == ["path_status_10d"],
        x_only_gates_all_passed=all(xo["gates"].values()),
        claim_order_declares_no_y=json.load(
            open(f"{F}_15M_CLAIM_ORDER_V1.json"))["y_status"].startswith("NO OUTCOME"))
    ok = all(checks.values())
    print(f"  D NO-Y     {'PASS' if ok else 'FAIL'} · reachable {len(seen)} modules · "
          f"forbidden {hits or 0} · columns loaded {len(cols)} · outcome-derived "
          f"{outcome_cols} · SQL literals {len(sql)}")
    return ok, dict(checks=checks, reachable_modules=sorted(seen), import_edges=edges,
                    forbidden_modules=sorted(forbidden), forbidden_reachable=hits,
                    columns_loaded=sorted(cols), outcome_derived_columns=outcome_cols,
                    token_source_columns=len(src_cols), sql_literals=len(sql),
                    method="AST column lists + SQL literals + the runtime token-source "
                           "column list — NOT a substring scan of the source text, which "
                           "matches this chain's own no-Y auditor and fails on the detector",
                    note="path_status_10d is read as a STATUS — whether a 10-day path "
                         "EXISTS, never what it did")


def run(fam: str):
    t0 = time.time()
    F = fam.upper()
    print(f"{F} 15m pre-Y gates", flush=True)
    okA, A = gate_a(F)
    okB, B = gate_b(F, fam)
    okC, C = gate_c(F, fam)
    okD, D = gate_d(F)
    ok = okA and okB and okC and okD
    order = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    d = ART.seal(dict(
        spec_id=f"{F}_15M_PRE_Y_GATES_V1", status="X_ONLY_PRE_Y", family=F,
        result="PASS" if ok else "FAIL",
        A_rng=A, B_replay=B, C_source=C, D_no_y=D,
        k=order["k"], needles={q: order["needles"][q] for q in ("q10", "q50", "q90")},
        capability_matrix=dict(needles=3, deltas=list(DELTAS_PP), worlds=N_WORLDS,
                               total_worlds=3 * len(DELTAS_PP) * N_WORLDS,
                               permutations_per_world=N_PERM),
        not_launched="the capability run is NOT started here; these gates end before Y",
        hard_state={f"{F} 15M HISTORICAL Y": "CLOSED",
                    f"{F} 15M OBSERVED Z": "NOT COMPUTED",
                    f"{F} 15M SURVIVORS": "UNKNOWN"},
        outcome_exposure="NOT_EXPOSED",
        runtime_min=round((time.time() - t0) / 60, 1)),
        f"{F}_15M_PRE_Y_GATES_V1.json",
        required=("spec_id", "result", "A_rng", "B_replay", "C_source", "D_no_y"),
        supersede=os.path.exists(f"{F}_15M_PRE_Y_GATES_V1.json"))
    print(f"  {F}_15M_PRE_Y_GATES_V1 · {d} · {'PASS' if ok else 'FAIL'} · "
          f"{(time.time()-t0)/60:.1f} min")
    if not ok:
        raise SystemExit(1)
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9"]):
        run(f)
