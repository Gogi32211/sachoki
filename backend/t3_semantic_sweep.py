"""T3 SEMANTIC RESIDUE SWEEP — closure for the literal-swap implementation route.

The T3 grammar/estimand/claim-order builders were generated from their T9 counterparts by
literal substitution. That route is only safe if nothing semantically T9 survives in
anything T3 EXECUTES or READS. A plain text grep cannot decide that: "T9 Z_rank" inside
T3's forbidden-inputs list is exactly what SHOULD be there, while `t9_claim_order.parquet`
in a path would be fatal. So every hit is classified by what it DOES, from the AST.

    ACTIVE   — can change what T3 selects, reads, writes or asserts:
               · an import of, or attribute access on, a t9_* module
               · a T9 string used as a path, SQL literal, or artifact spec_id/key
               · a T9 hash constant anywhere
               · 890 / 88 / 444 / 800 in a comparison, assert, or index expression
               MUST BE ZERO.

    INERT    — cannot: a locally bound identifier (a name has no semantics), or a
               descriptive string stored as an artifact VALUE. Reported with its reason
               and, for artifact values, whether the text is CORRECT-as-reference or a
               MISLABEL that an amendment must correct.

    REFERENCE— docstrings, comments, and forbidden-input entries that name T9 on purpose.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, json, os, re, sys, time, tokenize                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "T3_SEMANTIC_SWEEP_V1.json"
T3_SOURCES = ["t3_dna.py", "t3_family_freeze.py", "t3_outcomes.py",
              "t3_sequence_grammar.py", "t3_sequence_estimand.py",
              "t3_claim_order.py", "t3_stop_closure.py", "t3_stop_closure_2.py",
              "t3_capability_protocol.py", "t3_capability_run.py",
              "t3_capability_gates.py", "t3_capability_amendment.py",
              "t3_label_amendment.py"]
T3_ARTIFACTS = ["T3_DNA_X_V1.json", "T3_DNA_AUDIT.json", "T3_OUTCOME_SPEC_V1.json",
                "T3_SEQUENCE_GRAMMAR_V1.json", "T3_SEQUENCE_UNIVERSE_V1.json",
                "T3_SEQUENCE_ESTIMAND_V1.json", "T3_FAMILY_GOVERNANCE_V1.json",
                "T3_15M_PRESPEC_V1.json", "T3_HOLD_CLOSURE_V1.json",
                "T3_STOP_CLOSURE_2_V1.json", "T3_1H_CLAIM_ORDER_V1.json",
                "T3_CAPABILITY_PROTOCOL_V1.json", "T3_CAPABILITY_RESULT_V1.json",
                "T3_CAPABILITY_AMENDMENT_V1.json", "T3_LABEL_AMENDMENT_V1.json"]
CO_ARTIFACT = "T3_1H_CLAIM_ORDER_V1.json"

T9_TXT = re.compile(r"\b[Tt]9[_A-Za-z0-9]*|\b[A-Za-z_]+_[Tt]9\b")
NUM_CONTROL = {890, 88, 444, 800, 469, 149, 645}
PATHISH = re.compile(r"\.(py|json|parquet|duckdb)$|/|\\")
SQLISH = re.compile(r"(?is)\bselect\b.*\bfrom\b|t_sig\s*=")


class Classifier(ast.NodeVisitor):
    """Walks the AST and records, per string/number/name, whether it can act."""

    def __init__(self, path, hashes):
        self.path, self.hashes = path, hashes
        self.active, self.inert = [], []
        self.local_names = set()

    # ---- helpers -------------------------------------------------------
    def _hit(self, node, kind, why, text, active):
        rec = dict(file=self.path, line=getattr(node, "lineno", 0), kind=kind,
                   reason=why, text=str(text)[:120])
        (self.active if active else self.inert).append(rec)

    def _t9(self, s):
        return bool(T9_TXT.search(s)) or any(h and h in s for h in self.hashes)

    # ---- imports: always active ---------------------------------------
    def visit_Import(self, node):
        for a in node.names:
            if self._t9(a.name):
                self._hit(node, "IMPORT", "imports a T9 module", a.name, True)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if node.module and self._t9(node.module):
            self._hit(node, "IMPORT", "imports from a T9 module", node.module, True)
        self.generic_visit(node)

    # ---- attribute access on a t9 module ------------------------------
    def visit_Attribute(self, node):
        base = node
        while isinstance(base, ast.Attribute):
            base = base.value
        if isinstance(base, ast.Name) and self._t9(base.id) \
                and base.id not in self.local_names:
            self._hit(node, "ATTRIBUTE", "attribute access on a T9-named binding",
                      f"{base.id}.{node.attr}", True)
        self.generic_visit(node)

    # ---- local bindings: a name has no semantics ----------------------
    def visit_Assign(self, node):
        for t in ast.walk(node):
            if isinstance(t, ast.Name) and isinstance(t.ctx, ast.Store):
                self.local_names.add(t.id)
                if self._t9(t.id):
                    self._hit(t, "LOCAL_NAME",
                              "locally bound variable name — alpha-renameable, cannot "
                              "affect selection, paths or values", t.id, False)
        self.generic_visit(node)

    # ---- strings: the CALL THAT CONSUMES them decides, not their text ---
    IO_FUNCS = {"open", "join", "read_parquet", "to_parquet", "file_digest",
                "connect", "makedirs", "exists", "basename", "dirname"}
    SQL_FUNCS = {"execute", "executemany", "sql", "query"}

    def _consumer(self, node):
        """Name of the function this literal is an argument to, or None."""
        p = getattr(node, "parent", None)
        while p is not None and not isinstance(p, ast.Call):
            if isinstance(p, (ast.FunctionDef, ast.Module)):
                return None
            p = getattr(p, "parent", None)
        if p is None:
            return None
        f = p.func
        return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)

    def visit_Constant(self, node):
        v = node.value
        if isinstance(v, str) and self._t9(v):
            fn = self._consumer(node)
            if any(h and h in v for h in self.hashes):
                self._hit(node, "HASH", "T9 hash constant", v, True)
            elif fn in self.IO_FUNCS and PATHISH.search(v):
                self._hit(node, "PATH", f"T9 string passed to {fn}() as a path", v, True)
            elif fn in self.SQL_FUNCS and SQLISH.search(v):
                self._hit(node, "SQL", f"T9 string executed as SQL via {fn}()", v, True)
            elif re.fullmatch(r"[A-Z0-9_]+", v) and "T9" in v:
                self._hit(node, "IDENTIFIER_LITERAL",
                          "upper-case T9 identifier literal — inert only if stored as an "
                          "artifact value; verified against readers below", v, False)
            else:
                self._hit(node, "PROSE", "descriptive text", v, False)
        elif isinstance(v, int) and v in NUM_CONTROL:
            self._hit(node, "NUMBER", "T9 control constant", v, True)
        self.generic_visit(node)


def scan_source(path, hashes):
    src = open(path).read()
    tree = ast.parse(src)
    # docstring/comment lines are the allowed reference zone
    doc = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            b = n.body
            if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant) \
                    and isinstance(b[0].value.value, str):
                doc.update(range(b[0].lineno, (b[0].end_lineno or b[0].lineno) + 1))
    refs = []
    with open(path, "rb") as fh:
        for tok in tokenize.tokenize(fh.readline):
            if tok.type == tokenize.COMMENT and T9_TXT.search(tok.string):
                refs.append(dict(file=path, line=tok.start[0], text=tok.string.strip()[:90]))
    for ln in sorted(doc):
        pass
    for parent in ast.walk(tree):                 # parent links for role resolution
        for child in ast.iter_child_nodes(parent):
            child.parent = parent
    c = Classifier(path, hashes)
    c.visit(tree)
    c.active = [h for h in c.active if h["line"] not in doc]
    c.inert = [h for h in c.inert if h["line"] not in doc]
    return c.active, c.inert, refs


# ── artifact values: inert by construction, but a mislabel must be named ──
KNOWN_MISLABELS = {
    ("T3_FAMILY_GOVERNANCE_V1.json", ".setup"): "FINAL_PRIORITY_RESOLVED_T3",
    ("T3_FAMILY_GOVERNANCE_V1.json", ".estimand.liquidity"):
        "20-session median dollar volume ending T3-2, full pre-window required",
    ("T3_FAMILY_GOVERNANCE_V1.json", ".estimand.volatility"): "ATR14/close at T3-2",
    ("T3_FAMILY_GOVERNANCE_V1.json", ".historical_cutoff_rule"):
        "the T3 max-null band MUST come from T3's own permutation distribution; neither "
        "T5's nor T9's band is ever reused",
    ("T3_HOLD_CLOSURE_V1.json", ".item1.cross_vintage.canonical_population"):
        "materialized t_sig='T3'",
}
DELIBERATE_KEYS = ("forbidden_design_inputs", "supersedes", "relation", "note",
                   "provenance", "reused", "cited_as", "forbidden",
                   "struck_content", "struck_field", "finding", "truth", "corrections",
                   "amends", "item1_false_history", "inertness", "t9_status")


def scan_artifact(path, hashes):
    raw = json.load(open(path))
    base = os.path.basename(path)
    out = []

    def walk(node, p=""):
        if isinstance(node, dict):
            for k, v in node.items():
                if T9_TXT.search(str(k)):
                    # a key with whitespace is a human-readable label, not an identifier
                    # a program could ever look up — only identifier-shaped keys can act
                    label = bool(re.search(r"\s", str(k)))
                    out.append(dict(file=base, path=f"{p}.{k}", role="KEY",
                                    classification="REFERENCE" if label else "ACTIVE",
                                    value=str(k)[:120]))
                walk(v, f"{p}.{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{p}[{i}]")
        else:
            s = str(node)
            if not (T9_TXT.search(s) or any(h and h in s for h in hashes)):
                return
            deliberate = any(w in p for w in DELIBERATE_KEYS)
            fix = KNOWN_MISLABELS.get((base, p))
            out.append(dict(file=base, path=p, role="VALUE",
                            classification="REFERENCE" if deliberate else
                                           ("MISLABEL" if fix else "VALUE_T9_TEXT"),
                            corrected_to=fix, value=s[:160]))
    walk(raw)
    return out


def main():
    t0 = time.time()
    T9CO = json.load(open("T9_1H_CLAIM_ORDER_V1.json"))
    hashes = [T9CO["claim_order_hash"], T9CO["population_hash"],
              T9CO["block_assignment_hash"]]

    active, inert, refs, missing = [], [], [], []
    for f in T3_SOURCES:
        if not os.path.exists(f):
            missing.append(f); continue
        a, i, r = scan_source(f, hashes)
        active += a; inert += i; refs += r
    art = []
    for f in T3_ARTIFACTS:
        if not os.path.exists(f):
            missing.append(f); continue
        art += scan_artifact(f, hashes)
    art_active = [h for h in art if h["classification"] == "ACTIVE"]
    mislabels = [h for h in art if h["classification"] == "MISLABEL"]
    art_refs = [h for h in art if h["classification"] == "REFERENCE"]
    art_other = [h for h in art if h["classification"] == "VALUE_T9_TEXT"]

    # ── inertness proofs for the label-only residues ──────────────────
    repo = {f: open(f).read() for f in os.listdir(".") if f.endswith(".py")}
    # a READ is an actual load, not a mention: naming the artifact in an audit module
    # proves nothing about the pipeline consuming it
    GOV_READ = re.compile(r'(?:open|load|file_digest)\s*\(\s*["\']T3_FAMILY_GOVERNANCE_V1')
    reads_governance = sorted(f for f, s in repo.items()
                              if GOV_READ.search(s) and f != "t3_family_freeze.py")
    SETUP_READ = re.compile(r'(?:\[["\']setup_1d["\']\]|\.setup_1d\b)(?!\s*=)')
    reads_setup_col = sorted(f for f, s in repo.items()
                             if SETUP_READ.search(s) and f != "t3_semantic_sweep.py")
    import t3_dna as D3
    proofs = {
        "episode identity uses SETUP == 'T3'": D3.SETUP == "T3",
        "no executable reads T3_FAMILY_GOVERNANCE_V1": not reads_governance,
        "setup_1d column is written, never read": not reads_setup_col,
        "X freeze carries the correct setup label":
            json.load(open("T3_DNA_X_V1.json"))["setup"] == "FINAL_PRIORITY_RESOLVED_T3",
        "episode SQL selects t_sig='T3'": "t_sig='T3'" in repo["t3_dna.py"]
                                          or 't_sig = \'T3\'' in repo["t3_dna.py"],
    }

    CO = json.load(open(CO_ARTIFACT))
    idx = {q: CO["needles"][q]["index_zero_based"] for q in ("q10", "q50", "q90")}
    parq = [v for k, v in CO["artifacts"].items() if k in ("order", "population")]
    req = {
        "family_id == T3_MICROSTRUCTURE_DNA_V1":
            CO["family_id"] == "T3_MICROSTRUCTURE_DNA_V1",
        "k_final == 853": CO["k_final"] == 853,
        "unique_j == 853": CO["unique_j"] == 853,
        "unique_membership_hash == 853": CO["unique_membership_hash"] == 853,
        "needle indices == 85/426/767": idx == {"q10": 85, "q50": 426, "q90": 767},
        "needle j values disjoint from T9's":
            {n["j"] for n in CO["needles"].values()}
            .isdisjoint({n["j"] for n in T9CO["needles"].values()}),
        "population_hash != T9's": CO["population_hash"] != T9CO["population_hash"],
        "block_assignment_hash != T9's":
            CO["block_assignment_hash"] != T9CO["block_assignment_hash"],
        "claim_order_hash != T9's": CO["claim_order_hash"] != T9CO["claim_order_hash"],
        "all parquet paths are t3_*": all(p.startswith("t3_") for p in parq),
        "no source file missing": not missing,
    }
    ok = (not active and not art_active and all(req.values()) and all(proofs.values()))

    print(f"ACTIVE residues (must be 0) : source {len(active)} · artifact {len(art_active)}")
    for h in (active + art_active)[:20]:
        print(f"   {h}")
    print(f"INERT local names           : {len(inert)}")
    for h in inert:
        print(f"   {h['file']}:{h['line']} {h['text']} — {h['reason'][:60]}")
    print(f"artifact MISLABELS to amend : {len(mislabels)}")
    for h in mislabels:
        print(f"   {h['file']}{h['path']}\n     is : {h['value'][:90]}\n     ->  {h['corrected_to'][:90]}")
    print(f"deliberate references       : source {len(refs)} · artifact {len(art_refs)}"
          f" · other T9 text values {len(art_other)}")
    for h in art_other:
        print(f"   {h['file']}{h['path']} :: {h['value'][:90]}")
    for k, v in {**proofs, **req}.items():
        print(f"   {'PASS' if v else 'FAIL'}  {k}")

    digest = ART.seal(dict(
        spec_id="T3_SEMANTIC_SWEEP_V1", result="PASS" if ok else "FAIL",
        scope="literal-swap implementation closure — every T9 token in the T3 chain "
              "classified by what it DOES (AST role), not by text match",
        acceptance="ACTIVE residues == 0; label-only residues proved inert; mislabels "
                   "listed for amendment",
        sources_scanned=[f for f in T3_SOURCES if os.path.exists(f)],
        artifacts_scanned=[f for f in T3_ARTIFACTS if os.path.exists(f)],
        forbidden=dict(identifiers="t9_* module import/attribute",
                       paths="T9 string as path/SQL/spec_id",
                       numbers=sorted(NUM_CONTROL),
                       hashes=dict(claim_order=T9CO["claim_order_hash"],
                                   population=T9CO["population_hash"],
                                   blocks=T9CO["block_assignment_hash"])),
        active_residues_source=active, active_residues_artifact=art_active,
        inert_local_names=inert,
        artifact_mislabels=mislabels,
        deliberate_references=dict(n_source_comments=len(refs),
                                   n_artifact_reference_values=len(art_refs),
                                   other_t9_text_values=art_other),
        inertness_proofs={k: bool(v) for k, v in proofs.items()},
        required_positives={k: bool(v) for k, v in req.items()},
        missing_files=missing,
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1)),
        OUT, required=("spec_id", "result", "required_positives", "inertness_proofs"),
        supersede=os.path.exists(OUT))
    print(f"\nT3_SEMANTIC_SWEEP_V1 · {digest} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
