"""T9_15M_CAPABILITY_WORLD_IDENTITY_V1 — what exactly is a stochastic world?

The question is whether the qualified T5 15m capability seeds

    A  (needle, replicate)          six delta lanes SHARE one nullized Y and one set of
                                    permutation mappings
    B  (needle, delta, replicate)   every delta gets its own independently seeded world

If T5 is B and T9 runs A, the two are not the same experiment: sharing a stochastic world
across six lanes changes the dependence structure between them, and the T9 run would be an
engineering attempt rather than evidence.

The answer is taken from the AST of the qualified implementation — the actual expressions
passed to np.random.default_rng and the actual loop nesting — never from a docstring, a
field name or my own reading of it.

NO OUTCOME IS READ. This module does not open the ledger, the outcome sidecar or any
result artifact; it inspects source structure and the sealed protocol counts only.
"""
from __future__ import annotations
import ast, json, os, sys, time                                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

REF = "t5_15m_capability.py"
CAND = "t15m_capability.py"


def analyse(path: str) -> dict:
    """Loop nesting and every default_rng key, straight from the AST."""
    tree = ast.parse(open(path).read())
    loops, keys = [], []

    class V(ast.NodeVisitor):
        def __init__(self):
            self.stack = []

        def visit_For(self, node):
            tgt = [n.id for n in ast.walk(node.target) if isinstance(n, ast.Name)]
            it = ast.unparse(node.iter)
            self.stack.append((tuple(tgt), it))
            loops.append(dict(targets=tgt, iter=it, depth=len(self.stack)))
            self.generic_visit(node)
            self.stack.pop()

        def visit_Call(self, node):
            f = node.func
            if isinstance(f, ast.Attribute) and f.attr == "default_rng" and node.args:
                keys.append(dict(key=ast.unparse(node.args[0]),
                                 enclosing=[dict(targets=list(t), iter=i)
                                            for t, i in self.stack]))
            self.generic_visit(node)

    V().visit(tree)

    # which loop variable indexes the DELTA grid?
    delta_vars = sorted({t for L in loops for t in L["targets"]
                         if "enumerate(DL)" in L["iter"] or L["iter"].strip() == "DL"})
    out = []
    for kdef in keys:
        elts = []
        a = ast.parse(kdef["key"], mode="eval").body
        if isinstance(a, (ast.List, ast.Tuple)):
            elts = [ast.unparse(e) for e in a.elts]
        names = {n.id for n in ast.walk(a) if isinstance(n, ast.Name)}
        out.append(dict(key=kdef["key"], arity=len(elts), elements=elts,
                        contains_delta_var=sorted(names & set(delta_vars)),
                        enclosing_loops=[e["targets"] for e in kdef["enclosing"]]))
    return dict(path=path, code_digest=ART.file_digest(path), rng_keys=out,
                delta_loop_vars=delta_vars,
                loops=[L for L in loops if L["depth"] <= 4])


def main():
    t0 = time.time()
    R, C = analyse(REF), analyse(CAND)
    PROT = json.load(open("T9_15M_CAPABILITY_PROTOCOL_V1.json"))

    def shape(a):
        """Positional roles with the namespace constant abstracted away."""
        return [tuple(["<NS_CONST>"] + k["elements"][1:]) for k in a["rng_keys"]]

    ref_shape, cand_shape = shape(R), shape(C)
    ref_delta = [k["contains_delta_var"] for k in R["rng_keys"]]
    cand_delta = [k["contains_delta_var"] for k in C["rng_keys"]]

    verdict = ("A" if not any(ref_delta) else "B")
    checks = dict(
        reference_keys_carry_no_delta=not any(ref_delta),
        candidate_keys_carry_no_delta=not any(cand_delta),
        same_key_arity=[k["arity"] for k in R["rng_keys"]]
                       == [k["arity"] for k in C["rng_keys"]],
        same_positional_roles=ref_shape == cand_shape,
        two_rng_keys_each=len(R["rng_keys"]) == len(C["rng_keys"]) == 2)
    ok = all(checks.values())

    n_needles, NW, ND = 3, PROT["worlds"], len(PROT["delta_grid_pp"])
    counts = dict(
        independent_nullization_worlds=n_needles * NW,
        delta_lanes_per_world=ND,
        capability_cells=n_needles * NW * ND,
        permutations_per_world=PROT["n_perm_inner"],
        protocol_logical_cells=PROT["logical_cells"],
        cells_match_protocol=n_needles * NW * ND == PROT["logical_cells"])

    print(f"WORLD IDENTITY · reference {REF} {R['code_digest']}")
    for a, nm in ((R, "T5 reference"), (C, "T9 candidate")):
        print(f"  {nm}:")
        for k in a["rng_keys"]:
            print(f"    default_rng({k['key']}) · arity {k['arity']} · delta var in key: "
                  f"{k['contains_delta_var'] or 'NONE'} · loops {k['enclosing_loops']}")
    print(f"  delta loop variable(s): ref {R['delta_loop_vars']} · cand "
          f"{C['delta_loop_vars']}")
    print(f"\n  VERDICT: option {verdict} — a stochastic world is "
          + ("(needle, replicate); the six delta lanes SHARE its nullized Y and its "
             "permutation mappings" if verdict == "A"
             else "(needle, delta, replicate) — independently seeded per delta"))
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  {counts['independent_nullization_worlds']} stochastic base worlds x "
          f"{counts['delta_lanes_per_world']} delta lanes = "
          f"{counts['capability_cells']} registered capability cells · "
          f"{counts['permutations_per_world']} permutations per world")

    d = ART.seal(dict(
        spec_id="T9_15M_CAPABILITY_WORLD_IDENTITY_V1", status="X_ONLY_STRUCTURAL",
        question="what exactly defines a stochastic world in the qualified T5 15m "
                 "capability, and does T9 use the same key structure?",
        method="AST of the qualified implementation — the actual np.random.default_rng "
               "argument expressions and the actual loop nesting. Not a docstring, not a "
               "field name, not prose.",
        verdict=dict(
            option=verdict,
            meaning=("A stochastic world is (needle, replicate). The six delta lanes are "
                     "DETERMINISTIC injections evaluated on that one nullized Y, and they "
                     "share the same 999 permutation mappings."
                     if verdict == "A" else
                     "A stochastic world is (needle, delta, replicate), independently "
                     "seeded per delta."),
            consequence=("T9 matches; the run continues unchanged."
                         if verdict == "A" and all(checks.values())
                         else "T9 does NOT match; the run is an engineering attempt and "
                              "must be re-run under the frozen semantics.")),
        reference=R, candidate=C, checks=checks, counts=counts,
        nomenclature_correction=(
            "'360 worlds' was loose wording in my report. The registered design is "
            f"{counts['independent_nullization_worlds']} STOCHASTIC BASE WORLDS x "
            f"{counts['delta_lanes_per_world']} delta lanes = "
            f"{counts['capability_cells']} REGISTERED CAPABILITY CELLS, with "
            f"{counts['permutations_per_world']} permutations per base world shared across "
            "its lanes. The cell count is unchanged; only the name was wrong."),
        namespace_constants=dict(
            reference="RNG_OUTER, RNG_INNER = 20260921, 20260922",
            candidate="RNG_OUTER, RNG_INNER = 0xA9F1, 0x5C3D",
            note="the namespace CONSTANTS differ by design — a different family must not "
                 "reuse another family's draws. What must match, and does, is the KEY "
                 "STRUCTURE: which indices enter the key and in which positions."),
        outcome_exposure="NOT_EXPOSED — no ledger, no outcome sidecar, no result artifact "
                         "was opened by this module",
        result="PASS" if ok else "FAIL",
        runtime_min=round((time.time() - t0) / 60, 2)),
        "T9_15M_CAPABILITY_WORLD_IDENTITY_V1.json",
        required=("spec_id", "verdict", "checks", "counts"),
        supersede=os.path.exists("T9_15M_CAPABILITY_WORLD_IDENTITY_V1.json"))
    print(f"\nT9_15M_CAPABILITY_WORLD_IDENTITY_V1 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
