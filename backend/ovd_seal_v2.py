"""OPENING_VOLUME_DYNAMICS — PRE-OUTCOME DESIGN AMENDMENT V2 · checkpoint + seal + execution authority.

V1 (SEAL.json) is immutable provenance and is marked SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION only through
a NEW external record (V1_STATUS.json). ONLY SEAL_V2.json may unlock outcome execution.

Run sealing as a STANDALONE process. No _pathsim call exists in this module or in any OVD module.
"""
from __future__ import annotations
import os, sys, re, json, hashlib, time, glob, inspect, subprocess      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")
V1_SEAL_SHA256 = "18b52e65a4fead1db34f0df104b0e572ea9bcc27f54a98ddd9b1733ba8885bf0"
OVD_MODULES = ["ovd_build.py", "ovd_build_v2.py", "ovd_registry.py", "ovd_registry_v2.py", "ovd_seal.py", "ovd_seal_v2.py",
               "ovd_diagnostics.py", "ovd_canonical_1d.py", "ovd_canonical_derive.py", "ovd_canonical_audit.py",
               "ovd_checkpoint1b.py", "ovd_checkpoint1c.py", "ovd_checkpoint1d.py"]
FIXTURE_FILES = ("tests/test_ovd_guards.py", "tests/test_ovd_canonical.py", "tests/test_ovd_preseal.py", "tests/test_ovd_v2.py")
CODE_FILES = ("ovd_build_v2.py", "ovd_registry_v2.py", "ovd_seal_v2.py", "ovd_diagnostics.py", "ovd_build.py")
from ovd_build import HardStop                                          # noqa: E402  one HardStop class family-wide


def _dig(p: str, n: int = 16) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# ── governance guards ──────────────────────────────────────────────────────────────────────
def assert_v1_immutable() -> dict:
    """Fixture 25: V1 SEAL.json bytes and every artifact it binds must be unchanged."""
    p = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(p):
        raise HardStop("V1 SEAL.json missing")
    sha = hashlib.sha256(open(p, "rb").read()).hexdigest()
    if sha != V1_SEAL_SHA256:
        raise HardStop(f"V1 SEAL.json bytes changed: {sha[:16]} != {V1_SEAL_SHA256[:16]}")
    s = json.load(open(p))
    bound = {"SEARCH_REGISTRY_V1.json": s["registry_sha256_16"], "FEATURE_SPEC_V1.json": s["feature_spec_sha256_16"],
             "MULTIPLICITY_UNIVERSE_V1.json": s["multiplicity_sha256_16"], "discovery_phase1.json": s["discovery_sha256_16"]}
    for name, d in bound.items():
        if _dig(os.path.join(FAMILY_DIR, name)) != d:
            raise HardStop(f"V1-bound artifact changed: {name}")
    x1 = os.path.join(FAMILY_DIR, "runs", s["x_run"], "X.parquet")
    if _dig(x1) != s["x_parquet_sha256_16"]:
        raise HardStop("V1 X.parquet changed")
    return dict(v1_seal_sha256=sha, sealed_at=s["sealed_at"], x_run=s["x_run"], registry_sha256_16=s["registry_sha256_16"], k_total=s["k_total"])


def assert_no_outcome_access() -> dict:
    """Fixtures 26/27: no outcome-bearing file, no result table, no _pathsim call site anywhere in the
    family's code; the _pathsim binding recorded at discovery is unchanged."""
    # the family's own PRE-outcome governance records (PRE_OUTCOME_DESIGN_AMENDMENT_*.json) are not
    # outcome-bearing; everything else whose name says outcome / pathsim / result / forward is.
    bad = [os.path.join(r, f) for r, _, fs in os.walk(FAMILY_DIR) for f in fs
           if not f.upper().startswith("PRE_OUTCOME_")
           and any(k in f.lower() for k in ("outcome", "pathsim", "result", "edge_table", "forward"))]
    if bad:
        raise HardStop(f"outcome-bearing / result files present: {bad[:5]}")
    calls = {}
    for m in OVD_MODULES:
        p = os.path.join(HERE, m)
        if os.path.exists(p):
            src = open(p).read()
            if re.search(r"_pathsim\s*\(", src):
                calls[m] = True
    if calls:
        raise HardStop(f"_pathsim call site found in {list(calls)}")
    import edge_replay as E
    src_sha = hashlib.sha256(inspect.getsource(E._pathsim).encode()).hexdigest()[:16]
    disc = json.load(open(os.path.join(FAMILY_DIR, "discovery_phase1.json")))
    if src_sha != disc["pathsim_binding"]["src_sha256_16"]:
        raise HardStop("_pathsim source changed since discovery")
    return dict(outcome_bearing_files=0, result_tables=0, pathsim_call_sites_in_ovd_code=0,
                pathsim_call_count="0 (no call site has ever existed in this family's code; no outcome artifact exists)",
                pathsim_src_sha256_16=src_sha, verified_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


# the seal chain, newest first: the first existing seal is the CURRENT execution authority
SEAL_CHAIN = (("SEAL_V2_1.json", "OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1"),
              ("SEAL_V2.json", "OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_DESIGN_AMENDMENT_V2"))


def current_seal() -> tuple[str, str, dict]:
    for fname, amendment in SEAL_CHAIN:
        p = os.path.join(FAMILY_DIR, fname)
        if os.path.exists(p):
            return fname, amendment, json.load(open(p))
    raise HardStop("no current execution authority: no V2 / V2.1 seal exists (V1 is superseded for execution)")


def execution_authority(presented: dict | None = None, check_code: bool = True) -> dict:
    """Fixtures 24 / V2.1-6 / V2.1-7: the outcome runner must call this. V1 -> HARD STOP; a superseded
    V2 seal (once V2.1 exists) -> HARD STOP; the on-disk harness code must match the digests bound in
    the current seal (harness drift -> HARD STOP)."""
    fname, amendment, s = current_seal()
    if presented is not None:
        if presented.get("family") == "OPENING_VOLUME_DYNAMICS_V1" and "amendment" not in presented:
            raise HardStop("SUPERSEDED_EXECUTION_AUTHORITY: V1 SEAL.json is immutable provenance, not the current "
                           f"execution authority (SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION) — present {fname}")
        if presented.get("amendment") != amendment:
            raise HardStop(f"SUPERSEDED_EXECUTION_AUTHORITY: {presented.get('amendment')} is not the current authority — present {fname}")
    assert_v1_immutable()
    if s.get("parent_v1_seal_sha256") != V1_SEAL_SHA256:
        raise HardStop(f"{fname} is not bound to the immutable V1 seal")
    if fname == "SEAL_V2_1.json":
        v2p = os.path.join(FAMILY_DIR, "SEAL_V2.json")
        if hashlib.sha256(open(v2p, "rb").read()).hexdigest() != s.get("parent_v2_seal_sha256"):
            raise HardStop("SEAL_V2.json bytes changed — V2 must remain immutable provenance")
    if check_code:
        for f, d in s.get("code", {}).items():
            p = os.path.join(HERE, f)
            if os.path.exists(p) and _dig(p) != d:
                raise HardStop(f"HARNESS_CODE_DRIFT: {f} on disk ({_dig(p)}) != digest bound in {fname} ({d}) — reseal required")
    return s


def latest_v2_run() -> str | None:
    from ovd_build import RunSpace
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OVDV2_*")), reverse=True):
        if RunSpace.is_current(r, "X_v2.parquet"):
            st = json.load(open(os.path.join(r, "STATUS.json"))) if os.path.exists(os.path.join(r, "STATUS.json")) else {}
            if st.get("status") == "CANONICAL_1D_BASIS_V2" and st.get("parent_v1_seal_sha256") == V1_SEAL_SHA256:
                return r
    return None


def _run_fixtures() -> dict:
    import tempfile, xml.etree.ElementTree as ET
    paths = [os.path.join(HERE, f) for f in FIXTURE_FILES]
    for p in paths:
        if not os.path.exists(p):
            raise HardStop(f"fixture file missing: {p}")
    fd, xml_p = tempfile.mkstemp(prefix="ovd_v2_fixtures_", suffix=".xml"); os.close(fd)
    try:
        r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={xml_p}", *paths],
                           cwd=HERE, capture_output=True, text=True)
        root = ET.parse(xml_p).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        tests = sum(int(s.get("tests", 0)) for s in suites)
        bad = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
        skipped = sum(int(s.get("skipped", 0)) for s in suites)
    finally:
        try:
            os.remove(xml_p)
        except OSError:
            pass
    passed = tests - bad - skipped
    if r.returncode != 0 or bad or passed == 0:
        raise HardStop(f"fixtures not green — tests {tests} failed/errors {bad} rc {r.returncode}\n{r.stdout[-3000:]}\n{r.stderr[-1000:]}")
    return dict(passed=passed, tests=tests, skipped=skipped, files={f: _dig(p) for f, p in zip(FIXTURE_FILES, paths)})


# ── seal ───────────────────────────────────────────────────────────────────────────────────
def ovd_seal_v2() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL_V2.json")):
        raise HardStop("REFUSED: SEAL_V2.json already exists")
    v1 = assert_v1_immutable()
    access = assert_no_outcome_access()
    import ovd_registry_v2 as R2, ovd_build_v2 as B2
    seal1, reg1 = R2.v1_sealed()
    enum = R2.enumerate_v2(reg1)
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2.draft.json")))
    mult = json.load(open(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V2.draft.json")))
    spec_md = open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2.draft.md")).read()
    # registry exactness: the draft must equal a fresh enumeration; k frozen; no interactions added
    if reg["cells"] != enum["cells"] or reg["k"]["total"] != enum["k_v2"] or mult["k_total"] != enum["k_v2"] \
            or reg["registry_sha256_16"] != mult["registry_sha256_16"]:
        raise HardStop("REFUSED: V2 registry draft != exact enumeration / k inconsistent")
    if reg["v1_registry"]["cells"] != reg1["cells"] or reg["k"]["v1"] != seal1["k_total"]:
        raise HardStop("REFUSED: embedded V1 registry differs from the sealed V1")
    if len(reg["new_claims"]) != 6 or not reg.get("no_new_interactions"):
        raise HardStop("REFUSED: V2 must add at most the six enumerated claims and no interactions")
    R2.assert_registry_covers(list(R2.CLAIM_COLUMNS), reg["cells"])
    for n in R2.DESCRIPTIVE_ONLY:
        if any(c["feature"] == n for c in reg["cells"]):
            raise HardStop(f"REFUSED: DESCRIPTIVE_ONLY {n} entered the claim set")
    # canonical identity unchanged; X_v2 run bound to V1 X and to the canonical run
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    if cur["run_id"] != seal1["canonical_1d"]["run_id"] or _dig(cur["canonical_parquet"]) != seal1["canonical_1d"]["canonical_sha256_16"] \
            or _dig(cur["derived_parquet"]) != seal1["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("REFUSED: canonical 1D identity changed since V1 seal")
    run = latest_v2_run()
    if not run:
        raise HardStop("REFUSED: no COMPLETED CANONICAL_1D_BASIS_V2 build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if rep["x1_sha256_16"] != seal1["x_parquet_sha256_16"] or rep["rows"] != seal1["observation_window_authority"]["rows"] \
            or any(v for v in rep["conservation"].values() if isinstance(v, int) and v and v != rep["rows"]):
        raise HardStop("REFUSED: X_v2 does not reconcile with the V1 X / conservation")
    for name, col in R2.CLAIM_COLUMNS.items():
        if name not in rep["claims"] or rep["claim_columns"].get(name) != col:
            raise HardStop(f"REFUSED: claim {name} not materialised in X_v2")
        B2.assert_entry_offset(name, R2.ENTRY_OFFSET[name])
    if rep["source_15m"].get("h1_store_attached") is not False:
        raise HardStop("REFUSED: the 1H store must not be a source of any 30m/60m window")
    # A10 diagnostic unchanged (module digest as bound in V1's FEATURE_SPEC_V1.json)
    spec1 = json.load(open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V1.json")))
    if _dig(os.path.join(HERE, "ovd_diagnostics.py")) != spec1["diagnostics"]["sha256_16"]:
        raise HardStop("REFUSED: ovd_diagnostics.py changed since V1 seal (A10 must remain unchanged)")
    a10 = json.load(open(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")))
    if _dig(a10["table"]) != a10["table_sha256_16"]:
        raise HardStop("REFUSED: the frozen A10 exposure set changed")
    fx = _run_fixtures()
    sealed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for name, obj in (("SEARCH_REGISTRY_V2.json", reg), ("MULTIPLICITY_UNIVERSE_V2.json", mult)):
        obj["status"] = "SEALED"; obj["sealed_at"] = sealed_at
        json.dump(obj, open(os.path.join(FAMILY_DIR, name), "w"), indent=1, default=str)
    code = {f: _dig(os.path.join(HERE, f)) for f in CODE_FILES}
    spec = dict(spec_id="OPENING_VOLUME_DYNAMICS_FEATURE_SPEC_V2", status="SEALED", sealed_at=sealed_at,
                parent_v1=dict(seal_sha256=V1_SEAL_SHA256, feature_spec_sha256_16=seal1["feature_spec_sha256_16"]),
                markdown=spec_md, markdown_sha256_16=hashlib.sha256(spec_md.encode()).hexdigest()[:16], code=code)
    json.dump(spec, open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2.json"), "w"), indent=1)
    amendment = dict(amendment_id="OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_DESIGN_AMENDMENT_V2", sealed_at=sealed_at,
                     provenance=["POST_V1_SEAL", "PRE_OUTCOME", "PRE_PATHSIM", "USER_DIRECTED_HYPOTHESIS_EXPANSION"],
                     reason="The design changed after V1 sealing but before any outcome exposure: the user added two volume "
                            "reversal / participation-handoff logics (CLOSE60_DOMINANCE_REVERSAL, CLOSE_TO_NEXT_OPEN_HANDOFF) and a "
                            "canonical 30-minute resolution layer across all four logics. V1 stays immutable provenance.",
                     parent_v1=v1, outcome_access_verification=access,
                     v1_execution_status="SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION (external record V1_STATUS.json; V1 bytes untouched)",
                     k=dict(v1=enum["v1_k"], added=enum["added"], deduplicated=enum["deduplicated"], v2=enum["k_v2"]))
    json.dump(amendment, open(os.path.join(FAMILY_DIR, "PRE_OUTCOME_DESIGN_AMENDMENT_V2.json"), "w"), indent=1, default=str)
    seal = dict(family="OPENING_VOLUME_DYNAMICS_V1", amendment="OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_DESIGN_AMENDMENT_V2",
                sealed_at=sealed_at, parent_v1_seal_sha256=V1_SEAL_SHA256, parent_v1_sealed_at=v1["sealed_at"],
                amendment_reason=amendment["reason"],
                amendment_sha256_16=_dig(os.path.join(FAMILY_DIR, "PRE_OUTCOME_DESIGN_AMENDMENT_V2.json")),
                x_run=os.path.basename(run), x_v2_parquet_sha256_16=_dig(os.path.join(run, "X_v2.parquet")),
                build_report_sha256_16=_dig(os.path.join(run, "build_report.json")),
                x1_run=seal1["x_run"], x1_parquet_sha256_16=seal1["x_parquet_sha256_16"],
                canonical_1d=seal1["canonical_1d"],
                source_15m=rep["source_15m"], early_close_sessions=rep["early_close_sessions"],
                observation_window_authority=seal1["observation_window_authority"],
                feature_spec_sha256_16=_dig(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2.json")),
                registry_sha256_16=_dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2.json")),
                multiplicity_sha256_16=_dig(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V2.json")),
                k_total=enum["k_v2"], k_v1=enum["v1_k"], k_added=enum["added"], deduplicated=enum["deduplicated"],
                k_by_family=mult["k_by_family"], cells_sha256_16=enum["v2_cells_sha256_16"],
                confirmatory_claims=[c["name"] for c in R2.CLAIMS], entry_offset_from_D=R2.ENTRY_OFFSET,
                descriptive_only=list(R2.DESCRIPTIVE_ONLY.keys()), code=code,
                report_only_diagnostics=dict(names=["POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5"], module_sha256_16=code["ovd_diagnostics.py"],
                                             exposure_set_sha256_16=a10["table_sha256_16"], unchanged_from_v1=True),
                limitations=reg1["limitations"], fixtures=fx,
                outcome_access_count=0, pathsim_call_count=0, outcome_access_verification=access,
                pathsim=seal1["pathsim"],
                execution_rule="ONLY SEAL_V2 unlocks outcome execution (ovd_seal_v2.execution_authority); V1 = immutable provenance, "
                               "SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION",
                outcome_phase="NOT OPENED by this command — _pathsim is a SEPARATE, explicit, user-authorised subsequent action")
    json.dump(seal, open(os.path.join(FAMILY_DIR, "SEAL_V2.json"), "w"), indent=1, default=str)
    json.dump(dict(v1_seal_sha256=V1_SEAL_SHA256, status="SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION", at=sealed_at,
                   superseded_by=dict(seal="SEAL_V2.json", sha256=hashlib.sha256(open(os.path.join(FAMILY_DIR, "SEAL_V2.json"), "rb").read()).hexdigest()),
                   note="V1 bytes are untouched; V1 remains immutable historical provenance; it is no longer current for execution"),
              open(os.path.join(FAMILY_DIR, "V1_STATUS.json"), "w"), indent=1)
    return seal


if __name__ == "__main__":
    if "--seal" in sys.argv:
        print(json.dumps(ovd_seal_v2(), indent=1, default=str))
    else:
        print(json.dumps(dict(v1=assert_v1_immutable(), access=assert_no_outcome_access(), v2_run=latest_v2_run()), indent=1))
