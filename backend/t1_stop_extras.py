"""T1 STOP extras — coverage-bias diagnostics, the no-future rerun, prespec provenance,
and the foreign-family semantic sweep. Everything the STOP report needs that is not in
T1_STOP_CLOSURE_V1.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, json, os, re, sys, time, tokenize                          # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t1_dna as D1                                # noqa: E402

OUT = "T1_STOP_EXTRAS_V1.json"

# ── the foreign families whose residue must be inert in the T1 chain ─────
T1_SOURCES = ["t1_dna.py", "t1_family_freeze.py", "t1_outcomes.py",
              "t1_sequence_grammar.py", "t1_sequence_estimand.py",
              "t1_stop_closure.py"]   # the scanner itself declares the
                                      # forbidden constants and is excluded
T1_ARTIFACTS = ["T1_DNA_X_V1.json", "T1_DNA_AUDIT.json", "T1_OUTCOME_SPEC_V1.json",
                "T1_SEQUENCE_GRAMMAR_V1.json", "T1_SEQUENCE_UNIVERSE_V1.json",
                "T1_SEQUENCE_ESTIMAND_V1.json", "T1_FAMILY_GOVERNANCE_V1.json",
                "T1_15M_PRESPEC_V1.json"]
FOREIGN = re.compile(r"\b[Tt][359][_A-Za-z0-9]*|\b[A-Za-z_]+_[Tt][359]\b")
# T5 is the DECLARED shared-infrastructure ancestor: these imports are registered in the
# governance artifact, so they are infrastructure, not residue. T3/T9 have no such licence.
SHARED_T5 = {"t5_artifact", "t5_dna", "t5_sequence_estimand", "t5_outcomes",
             "t5_sequence_grammar"}
FOREIGN_K = {853, 890, 863}
PATHISH = re.compile(r"\.(py|json|parquet|duckdb)$|/|\\")
SQLISH = re.compile(r"(?is)\bselect\b.*\bfrom\b|t_sig\s*=")


def foreign_hashes():
    h = []
    for f in ("T9_1H_CLAIM_ORDER_V1.json", "T3_1H_CLAIM_ORDER_V1.json"):
        if os.path.exists(f):
            d = json.load(open(f))
            h += [d["claim_order_hash"], d["population_hash"],
                  d["block_assignment_hash"]]
    return [x for x in h if x]


class Classifier(ast.NodeVisitor):
    IO = {"open", "join", "read_parquet", "to_parquet", "file_digest", "connect"}
    SQL = {"execute", "executemany", "sql", "query"}

    def __init__(self, path, hashes):
        self.path, self.hashes = path, hashes
        self.active, self.inert, self.shared = [], [], []
        self.local = set()

    def _hit(self, node, kind, why, text, bucket):
        bucket.append(dict(file=self.path, line=getattr(node, "lineno", 0), kind=kind,
                           reason=why, text=str(text)[:110]))

    def _f(self, s):
        return bool(FOREIGN.search(s)) or any(h in s for h in self.hashes)

    def visit_Import(self, node):
        for a in node.names:
            if a.name in SHARED_T5:
                self._hit(node, "IMPORT", "registered shared infrastructure (governance "
                          "relationship_to_t5.reused)", a.name, self.shared)
            elif self._f(a.name):
                self._hit(node, "IMPORT", "imports a foreign family module", a.name,
                          self.active)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        m = node.module or ""
        if m in SHARED_T5:
            self._hit(node, "IMPORT", "registered shared infrastructure", m, self.shared)
        elif self._f(m):
            self._hit(node, "IMPORT", "imports from a foreign family module", m,
                      self.active)
        self.generic_visit(node)

    def visit_Assign(self, node):
        for t in ast.walk(node):
            if isinstance(t, ast.Name) and isinstance(t.ctx, ast.Store):
                self.local.add(t.id)
                if self._f(t.id):
                    self._hit(t, "LOCAL_NAME", "locally bound name — alpha-renameable",
                              t.id, self.inert)
        self.generic_visit(node)

    def _consumer(self, node):
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
        if isinstance(v, str) and self._f(v):
            fn = self._consumer(node)
            if any(h in v for h in self.hashes):
                self._hit(node, "HASH", "foreign hash constant", v, self.active)
            elif any(m in v for m in SHARED_T5):
                self._hit(node, "SHARED", "names registered shared infrastructure", v,
                          self.shared)
            elif fn in self.IO and PATHISH.search(v):
                self._hit(node, "PATH", f"foreign string passed to {fn}() as a path", v,
                          self.active)
            elif fn in self.SQL and SQLISH.search(v):
                self._hit(node, "SQL", f"foreign string executed as SQL via {fn}()", v,
                          self.active)
            else:
                self._hit(node, "PROSE", "descriptive text", v, self.inert)
        elif isinstance(v, int) and v in FOREIGN_K:
            self._hit(node, "NUMBER", "another family's k", v, self.active)
        self.generic_visit(node)


def sweep():
    hashes = foreign_hashes()
    active, inert, shared, refs = [], [], [], 0
    for f in T1_SOURCES:
        if not os.path.exists(f):
            continue
        src = open(f).read()
        tree = ast.parse(src)
        doc = set()
        for n in ast.walk(tree):
            if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                              ast.AsyncFunctionDef)):
                b = n.body
                if b and isinstance(b[0], ast.Expr) \
                        and isinstance(b[0].value, ast.Constant) \
                        and isinstance(b[0].value.value, str):
                    doc.update(range(b[0].lineno, (b[0].end_lineno or b[0].lineno) + 1))
        with open(f, "rb") as fh:
            for tok in tokenize.tokenize(fh.readline):
                if tok.type == tokenize.COMMENT and FOREIGN.search(tok.string):
                    refs += 1
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                child.parent = parent
        c = Classifier(f, hashes)
        c.visit(tree)
        active += [h for h in c.active if h["line"] not in doc]
        inert += [h for h in c.inert if h["line"] not in doc]
        shared += c.shared
    art_active = []
    for f in T1_ARTIFACTS:
        raw = json.load(open(f))

        def walk(node, p=""):
            if isinstance(node, dict):
                for k, v in node.items():
                    if re.search(r"\b[Tt][39][_A-Za-z0-9]*", str(k)) \
                            and not re.search(r"\s", str(k)):
                        art_active.append(dict(file=f, path=f"{p}.{k}", role="KEY"))
                    walk(v, f"{p}.{k}")
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    walk(v, f"{p}[{i}]")
            else:
                s = str(node)
                if any(h in s for h in hashes):
                    art_active.append(dict(file=f, path=p, role="FOREIGN_HASH",
                                           value=s[:80]))
        walk(raw)
    return dict(gate="FOREIGN_FAMILY_SEMANTIC_SWEEP",
                result="PASS" if not active and not art_active else "FAIL",
                scope="T3/T9 residue is forbidden anywhere active; T5 modules named in the "
                      "governance reuse list are registered shared infrastructure and are "
                      "reported separately, not as residue",
                foreign_k_constants=sorted(FOREIGN_K),
                foreign_hashes_checked=len(hashes),
                active_residues_source=active, active_residues_artifact=art_active,
                registered_shared_infrastructure=shared,
                inert_mentions=len(inert), comment_references=refs), \
        (not active and not art_active)


def coverage_bias():
    """Covered vs missing on price, liquidity, ATR% and rv20 — at the T1-2 anchor."""
    E = pd.read_parquet(D1.OUT_EP, columns=["episode_id", "ticker", "t1_date",
                                            "n_t1_day_1h_bars", "n_prev_day_1h_bars",
                                            "t1_close"])
    E["covered"] = (E.n_t1_day_1h_bars.fillna(0) > 0) & (E.n_prev_day_1h_bars.fillna(0) > 0)
    conn = duckdb.connect(D1.DB1D, read_only=True)
    conn.register("ep", E[["episode_id", "ticker", "t1_date"]])
    q = """
    WITH b AS (
      SELECT * FROM (
        SELECT ticker, date, close, volume, atr_14,
               row_number() OVER (PARTITION BY ticker,date ORDER BY
                 CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
        FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')) WHERE rn=1
    ), r AS (
      SELECT ticker, date, close, atr_14,
             median(close*volume) OVER (PARTITION BY ticker ORDER BY date
                 ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) dv20,
             CASE WHEN close > 0 AND lag(close) OVER (PARTITION BY ticker ORDER BY date) > 0
                  THEN ln(close / lag(close) OVER (PARTITION BY ticker ORDER BY date)) END lr
      FROM b
    ), v AS (
      SELECT ticker, date, close, atr_14, dv20,
             stddev_samp(lr) OVER (PARTITION BY ticker ORDER BY date
                 ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) rv20 FROM r
    ), a AS (
      SELECT e.episode_id, v.close price, v.dv20 liquidity,
             v.atr_14/nullif(v.close,0) atr_pct, v.rv20,
             row_number() OVER (PARTITION BY e.episode_id ORDER BY v.date DESC) k
      FROM ep e JOIN v ON v.ticker=e.ticker AND v.date < CAST(e.t1_date AS DATE)
      QUALIFY k = 2)
    SELECT episode_id, price, liquidity, atr_pct, rv20 FROM a"""
    A = conn.execute(q).fetchdf()
    conn.close()
    F = E.merge(A, on="episode_id", how="left")
    out = {}
    for col, lab in (("price", "price"), ("liquidity", "dollar_volume_20d"),
                     ("atr_pct", "atr_pct"), ("rv20", "rv20")):
        cov = F.loc[F.covered, col].median()
        mis = F.loc[~F.covered, col].median()
        out[lab] = dict(covered_median=None if pd.isna(cov) else round(float(cov), 6),
                        missing_median=None if pd.isna(mis) else round(float(mis), 6))
    return dict(gate="COVERAGE_BIAS", n_covered=int(F.covered.sum()),
                n_missing=int((~F.covered).sum()), medians=out,
                role="OBSERVED_1H_SUBPOPULATION — the 1H-covered set is liquidity- and "
                     "price-selected, so every T1 1H result describes THAT subpopulation "
                     "and is never generalized to the full canonical T1 population")


def prespec():
    p = json.load(open("T1_15M_PRESPEC_V1.json"))
    checks = {
        "opening hour only": p["primary_scope"] == "OPENING HOUR ONLY",
        "2-bar position families exactly M1->M2, M2->M3, M3->M4":
            p["position_families"] == ["M1→M2", "M2→M3", "M3→M4"],
        "2-bar grammar only": "2-bar" in p["grammar"],
        "3-bar excluded from V1": "3-bar" in p["excluded"],
        "frozen before the T1 historical outcome": "historical outcome exposure"
                                                   in p["frozen_before"],
        "sealed before the grammar": os.path.getmtime("T1_15M_PRESPEC_V1.json")
                                     < os.path.getmtime("T1_SEQUENCE_GRAMMAR_V1.json"),
        "sealed before the X freeze": os.path.getmtime("T1_15M_PRESPEC_V1.json")
                                      < os.path.getmtime("T1_DNA_X_V1.json"),
    }
    return dict(gate="PRESPEC_PROVENANCE",
                result="PASS" if all(checks.values()) else "FAIL",
                digest=ART.file_digest("T1_15M_PRESPEC_V1.json"),
                definition_hash=p["definition_hash"],
                sealed_at=time.strftime("%Y-%m-%d %H:%M",
                                        time.localtime(os.path.getmtime(
                                            "T1_15M_PRESPEC_V1.json"))),
                grammar_sealed_at=time.strftime("%Y-%m-%d %H:%M",
                                                time.localtime(os.path.getmtime(
                                                    "T1_SEQUENCE_GRAMMAR_V1.json"))),
                checks={k: bool(v) for k, v in checks.items()}), all(checks.values())


def main():
    t0 = time.time()
    sw, ok1 = sweep()
    print(f"SWEEP     {sw['result']} · active source {len(sw['active_residues_source'])} · "
          f"active artifact {len(sw['active_residues_artifact'])} · shared-infra "
          f"{len(sw['registered_shared_infrastructure'])} · inert {sw['inert_mentions']}",
          flush=True)
    cb = coverage_bias()
    print(f"COVERAGE  covered {cb['n_covered']:,} vs missing {cb['n_missing']:,} · " +
          " · ".join(f"{k} {v['covered_median']} vs {v['missing_median']}"
                     for k, v in cb["medians"].items()), flush=True)
    ps, ok3 = prespec()
    print(f"PRESPEC   {ps['result']} · {ps['digest']} · sealed {ps['sealed_at']} "
          f"(grammar {ps['grammar_sealed_at']})", flush=True)
    ok = ok1 and ok3
    d = ART.seal(dict(spec_id="T1_STOP_EXTRAS_V1", result="PASS" if ok else "FAIL",
                      semantic_sweep=sw, coverage_bias=cb, prespec=ps,
                      outcome_exposure="NOT_EXPOSED — no outcome value read",
                      runtime_s=round(time.time() - t0, 1)),
                 OUT, required=("spec_id", "result", "semantic_sweep", "coverage_bias"),
                 supersede=os.path.exists(OUT))
    print(f"\nT1_STOP_EXTRAS_V1 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
