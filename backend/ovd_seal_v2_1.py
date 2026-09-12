"""OPENING_VOLUME_DYNAMICS — PRE-OUTCOME RESEAL V2.1.

Provenance: POST_V2_SEAL · PRE_OUTCOME · PRE_PATHSIM.
Change classes (both disclosed; this is NOT "harness correction only"):
  1. HARNESS_CORRECTION — corrected zero-outcome guard; seal-chain execution authority; current code-digest binding.
  2. SAMPLE_AUTHORITY_BINDING_CORRECTION — OPEN30_SUSTAINED_BUILD_3D inherits the exact V1 parent "AB" authority as two
     distinct cells A and B; FULL_OPENING_RECLAIM_30 inherits the exact V1 parent "B". USER DECISION A: exact parent
     binding accepted; global k = 300 by exact enumeration; nothing narrowed to preserve 299.
Wording: V2 feature definitions, thresholds, causal timestamps and entry rules are unchanged. V2.1 corrects sample-
authority binding for two 30m resolution-extension states to reproduce their sealed V1 parent sample authorities exactly.

SEAL_V2 and SEAL_V1 are immutable (never rewritten). Run as a STANDALONE process. No _pathsim call exists here or
anywhere in the family's code.
"""
from __future__ import annotations
import os, sys, json, hashlib, time                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ovd_seal_v2 as S2                                                # noqa: E402
from ovd_build import HardStop                                          # noqa: E402

FAMILY_DIR = S2.FAMILY_DIR
CANON_DIR = S2.CANON_DIR
V1_SEAL_SHA256 = S2.V1_SEAL_SHA256
V2_SEAL_SHA256 = "f05151387120d8069ea8f76c213f8c33d828385f0e80ba9745367c5135d9d639"
AMENDMENT_ID = "OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1"
CODE_FILES = ("ovd_seal_v2.py", "ovd_seal_v2_1.py", "ovd_build_v2.py", "ovd_registry_v2.py", "ovd_diagnostics.py", "ovd_build.py")
FIXTURE_FILES = ("tests/test_ovd_guards.py", "tests/test_ovd_canonical.py", "tests/test_ovd_preseal.py", "tests/test_ovd_v2.py",
                 "tests/test_ovd_v2_1.py")
_dig = S2._dig


def assert_v2_immutable() -> dict:
    """SEAL_V2.json bytes and every artifact it binds must be unchanged."""
    p = os.path.join(FAMILY_DIR, "SEAL_V2.json")
    if not os.path.exists(p):
        raise HardStop("SEAL_V2.json missing")
    sha = hashlib.sha256(open(p, "rb").read()).hexdigest()
    if sha != V2_SEAL_SHA256:
        raise HardStop(f"SEAL_V2.json bytes changed: {sha[:16]} != {V2_SEAL_SHA256[:16]}")
    s = json.load(open(p))
    for name, d in (("SEARCH_REGISTRY_V2.json", s["registry_sha256_16"]), ("FEATURE_SPEC_V2.json", s["feature_spec_sha256_16"]),
                    ("MULTIPLICITY_UNIVERSE_V2.json", s["multiplicity_sha256_16"]), ("PRE_OUTCOME_DESIGN_AMENDMENT_V2.json", s["amendment_sha256_16"])):
        if _dig(os.path.join(FAMILY_DIR, name)) != d:
            raise HardStop(f"V2-bound artifact changed: {name}")
    return dict(v2_seal_sha256=sha, sealed_at=s["sealed_at"], x_run=s["x_run"], k_total=s["k_total"], code=s["code"])


def latest_v2_1_run(bindings_sha: str) -> str | None:
    """Newest COMPLETED V2 build whose STATUS carries the V2.1 sample bindings."""
    import glob
    from ovd_build import RunSpace
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OVDV2_*")), reverse=True):
        if not RunSpace.is_current(r, "X_v2.parquet"):
            continue
        st = json.load(open(os.path.join(r, "STATUS.json"))) if os.path.exists(os.path.join(r, "STATUS.json")) else {}
        if st.get("status") == "CANONICAL_1D_BASIS_V2" and st.get("parent_v1_seal_sha256") == V1_SEAL_SHA256 \
                and st.get("registry_version") == "V2_1" and st.get("bindings_sha256_16") == bindings_sha:
            return r
    return None


def _run_fixtures() -> dict:
    """Same junit-based runner as V2, over the five fixture files."""
    import subprocess, tempfile, xml.etree.ElementTree as ET
    paths = [os.path.join(HERE, f) for f in FIXTURE_FILES]
    for p in paths:
        if not os.path.exists(p):
            raise HardStop(f"fixture file missing: {p}")
    fd, xml_p = tempfile.mkstemp(prefix="ovd_v2_1_fixtures_", suffix=".xml"); os.close(fd)
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


def ovd_seal_v2_1() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL_V2_1.json")):
        raise HardStop("REFUSED: SEAL_V2_1.json already exists")
    v1 = S2.assert_v1_immutable()
    v2 = assert_v2_immutable()
    access = S2.assert_no_outcome_access()
    import ovd_registry_v2 as R2, ovd_build_v2 as B2
    seal1, reg1 = R2.v1_sealed()
    enum = R2.enumerate_v2(reg1)
    # ── SEAL GATES ──
    if enum["hard_stop_for_review"] or enum["k"] != R2.EXPECTED_K or enum["deduplicated"]:
        raise HardStop(f"HARD STOP FOR REVIEW: exact enumeration k = {enum['k']} (expected {R2.EXPECTED_K}), duplicates {enum['deduplicated']}")
    if enum["added"] != 7 or enum["states"] != 6:
        raise HardStop(f"REFUSED: expected 7 cells from 6 states, got {enum['added']} from {enum['states']}")
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2_1.draft.json")))
    mult = json.load(open(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V2_1.draft.json")))
    if reg["cells"] != enum["cells"] or reg["k"]["total"] != enum["k"] or mult["k_total"] != enum["k"] \
            or reg["registry_sha256_16"] != mult["registry_sha256_16"] or reg["status"] != "DRAFT_PRE_SEAL" \
            or reg["amendment"] != AMENDMENT_ID or "HARNESS_CORRECTION_ONLY" in json.dumps(reg):
        raise HardStop("REFUSED: V2.1 registry draft != exact enumeration / k / status / amendment id / provenance wording")
    if reg["v1_registry"]["cells"] != reg1["cells"] or reg["k"]["v1"] != seal1["k_total"]:
        raise HardStop("REFUSED: embedded V1 registry differs from the sealed V1")
    if len(reg["new_claims"]) != 6 or not reg.get("no_new_interactions"):
        raise HardStop("REFUSED: V2.1 must carry exactly the six V2 states and no interactions")
    # exact A/B/B/C/C/C/C bindings, two DISTINCT Logic-1 cells (never collapsed into 'AB')
    want = {("OPEN30_SUSTAINED_BUILD_3D", "A"), ("OPEN30_SUSTAINED_BUILD_3D", "B"), ("FULL_OPENING_RECLAIM_30", "B"),
            ("CLOSE60_DOMINANCE", "C"), ("CLOSE30_DOMINANCE", "C"), ("CLOSE60_TO_NEXT_OPEN60_HANDOFF", "C"), ("CLOSE30_TO_NEXT_OPEN30_HANDOFF", "C")}
    got = {(b["claim"], b["family"]) for b in enum["bindings"]}
    if got != want or any(len(b["family"]) != 1 for b in enum["bindings"]):
        raise HardStop(f"REFUSED: sample bindings {sorted(got)} != exact parent bindings {sorted(want)}")
    if R2.parent_families(reg1, "OPEN1H_RAMP_3D") != "AB" or R2.parent_families(reg1, "PRIOR_HV_VOLUME_RECLAIM") != "B":
        raise HardStop("REFUSED: V1 parent authorities are not what decision A recorded")
    # the six V2 states: feature definitions, thresholds, causal timestamps and entry rules unchanged from the sealed V2
    reg2 = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2.json")))
    strip = lambda p: p.split(" [the >= 1.0 term")[0]
    v2_def = {c["name"]: (strip(c["predicate"]), c["feature_available"], c["entry"], c["entry_offset_from_D"]) for c in reg2["new_claims"]}
    v21_def = {c["name"]: (strip(c["predicate"]), c["feature_available"], c["entry"], c["entry_offset_from_D"]) for c in reg["new_claims"]}
    if v2_def != v21_def or B2.HANDOFF_BAND != (0.80, 1.25) or B2.DECLINE_LAG != 5:
        raise HardStop("REFUSED: a V2 state definition / threshold / as-of / entry changed — V2.1 may only correct sample bindings")
    for n in R2.DESCRIPTIVE_ONLY:
        if any(c["feature"] == n for c in reg["cells"]):
            raise HardStop(f"REFUSED: DESCRIPTIVE_ONLY {n} entered the claim set")
    R2.assert_registry_covers(list(R2.CLAIM_COLUMNS), reg["cells"])
    # canonical identity unchanged; X_v2 run bound to V1 X, the canonical run AND the V2.1 bindings
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    if cur["run_id"] != seal1["canonical_1d"]["run_id"] or _dig(cur["canonical_parquet"]) != seal1["canonical_1d"]["canonical_sha256_16"] \
            or _dig(cur["derived_parquet"]) != seal1["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("REFUSED: canonical 1D identity changed since V1 seal")
    run = latest_v2_1_run(enum["bindings_sha256_16"])
    if not run:
        raise HardStop("REFUSED: no COMPLETED V2 build carrying the V2.1 sample bindings")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if rep["x1_sha256_16"] != seal1["x_parquet_sha256_16"] or rep["rows"] != seal1["observation_window_authority"]["rows"] \
            or rep["cells_sha256_16"] != enum["cells_sha256_16"] \
            or any(v for k, v in rep["conservation"].items() if k != "rows" and v):
        raise HardStop("REFUSED: X_v2 does not reconcile with V1 X / V2.1 bindings / conservation")
    if rep["source_15m"].get("h1_store_attached") is not False:
        raise HardStop("REFUSED: the 1H store must not be a source of any 30m/60m window")
    seal2 = json.load(open(os.path.join(FAMILY_DIR, "SEAL_V2.json")))
    if rep["source_15m"]["slots_extract_sha256_16"] != seal2["source_15m"]["slots_extract_sha256_16"] \
            or rep["early_close_sessions"] != seal2["early_close_sessions"]:
        raise HardStop("REFUSED: 15m slot extract / early-close list differ from the V2 seal (conservation must be unchanged)")
    for name in R2.CLAIM_COLUMNS:
        B2.assert_entry_offset(name, R2.ENTRY_OFFSET[name])
        if not rep["claims"][name]["cells"]:
            raise HardStop(f"REFUSED: no sample-bound cell census for {name}")
    spec1 = json.load(open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V1.json")))
    if _dig(os.path.join(HERE, "ovd_diagnostics.py")) != spec1["diagnostics"]["sha256_16"]:
        raise HardStop("REFUSED: ovd_diagnostics.py changed since V1 seal (A10 must remain unchanged)")
    a10 = json.load(open(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")))
    if _dig(a10["table"]) != a10["table_sha256_16"]:
        raise HardStop("REFUSED: the frozen A10 exposure set changed")
    hc_p = os.path.join(FAMILY_DIR, "HARNESS_CORRECTIONS.json")
    if not os.path.exists(hc_p):
        raise HardStop("REFUSED: HARNESS_CORRECTIONS.json missing")
    hc = json.load(open(hc_p))
    if (hc if isinstance(hc, list) else [hc])[-1]["on_disk_digest_after_correction"] != _dig(os.path.join(HERE, "ovd_seal_v2.py")):
        raise HardStop("REFUSED: HARNESS_CORRECTIONS.json does not name the current on-disk ovd_seal_v2.py digest")
    fx = _run_fixtures()
    # ── write ──
    sealed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for name, obj in (("SEARCH_REGISTRY_V2_1.json", reg), ("MULTIPLICITY_UNIVERSE_V2_1.json", mult)):
        obj["status"] = "SEALED"; obj["sealed_at"] = sealed_at
        json.dump(obj, open(os.path.join(FAMILY_DIR, name), "w"), indent=1, default=str)
    code = {f: _dig(os.path.join(HERE, f)) for f in CODE_FILES}
    spec2 = json.load(open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2.json")))
    amend_md = open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2_1.amendment.md")).read()
    spec = dict(spec_id="OPENING_VOLUME_DYNAMICS_FEATURE_SPEC_V2_1", status="SEALED", sealed_at=sealed_at,
                parent_v2=dict(seal_sha256=V2_SEAL_SHA256, feature_spec_sha256_16=seal2["feature_spec_sha256_16"],
                               markdown_sha256_16=spec2["markdown_sha256_16"]),
                wording=R2.WORDING_V2_1, change_classes=R2.CHANGE_CLASSES_V2_1,
                amendment_markdown=amend_md, amendment_markdown_sha256_16=hashlib.sha256(amend_md.encode()).hexdigest()[:16], code=code)
    json.dump(spec, open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2_1.json"), "w"), indent=1)
    v1_parent_authority = dict(registry="SEARCH_REGISTRY_V1.json", registry_sha256_16=seal1["registry_sha256_16"],
                               samples=reg1["samples"], entry=reg1["entry"],
                               parents={"OPEN30_SUSTAINED_BUILD_3D": dict(parent="OPEN1H_RAMP_3D", families=R2.parent_families(reg1, "OPEN1H_RAMP_3D")),
                                        "FULL_OPENING_RECLAIM_30": dict(parent="PRIOR_HV_VOLUME_RECLAIM", families=R2.parent_families(reg1, "PRIOR_HV_VOLUME_RECLAIM"))})
    new_cells = [c for c in enum["cells"] if c.get("kind") == "state"]
    cell_ids = [dict(claim=c["feature"], family=c["family"], bin=c["bin"], sample=c["sample"], sample_authority=c["sample_authority"],
                     as_of=c["as_of"], entry=c["entry"], status_column=c["status_column"],
                     identity_sha256_16=hashlib.sha256(R2.identity(c, reg1["samples"], reg1["entry"], reg1["entry"]).encode()).hexdigest()[:16])
                for c in new_cells]
    reseal = dict(amendment_id=AMENDMENT_ID, sealed_at=sealed_at, provenance=R2.PROVENANCE_V2_1, change_classes=R2.CHANGE_CLASSES_V2_1,
                  wording=R2.WORDING_V2_1, user_decision=R2.USER_DECISION,
                  why_299_became_300="the Logic-1 parent OPEN1H_RAMP_3D is registered on 'AB' in the sealed V1 registry; the exact transfer "
                                     "yields two distinct cells (A, B) for OPEN30_SUSTAINED_BUILD_3D; 6 states -> 7 cells; 293 + 7 = 300",
                  parent_v2=v2, parent_v1=v1, v1_parent_authority=v1_parent_authority,
                  harness_corrections_sha256_16=_dig(hc_p), outcome_access_verification=access,
                  k=dict(v1=enum["v1_k"], states=enum["states"], cells_added=enum["added"], deduplicated=enum["deduplicated"], v2_1=enum["k"]),
                  new_cell_identities=cell_ids)
    json.dump(reseal, open(os.path.join(FAMILY_DIR, "PRE_OUTCOME_RESEAL_V2_1.json"), "w"), indent=1, default=str)
    seal = dict(family="OPENING_VOLUME_DYNAMICS_V1", amendment=AMENDMENT_ID, sealed_at=sealed_at,
                provenance=R2.PROVENANCE_V2_1, change_classes=R2.CHANGE_CLASSES_V2_1, wording=R2.WORDING_V2_1, user_decision=R2.USER_DECISION,
                parent_v2_seal_sha256=V2_SEAL_SHA256, parent_v2_sealed_at=v2["sealed_at"], parent_v1_seal_sha256=V1_SEAL_SHA256,
                v1_parent_authority=v1_parent_authority,
                reseal_record_sha256_16=_dig(os.path.join(FAMILY_DIR, "PRE_OUTCOME_RESEAL_V2_1.json")),
                harness_corrections_sha256_16=_dig(hc_p), code=code, code_bound_in_v2=v2["code"],
                x_run=os.path.basename(run), x_v2_parquet_sha256_16=_dig(os.path.join(run, "X_v2.parquet")),
                build_report_sha256_16=_dig(os.path.join(run, "build_report.json")),
                x1_run=seal1["x_run"], x1_parquet_sha256_16=seal1["x_parquet_sha256_16"], canonical_1d=seal1["canonical_1d"],
                source_15m=rep["source_15m"], early_close_sessions=rep["early_close_sessions"],
                observation_window_authority=seal1["observation_window_authority"],
                feature_spec_sha256_16=_dig(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V2_1.json")),
                feature_spec_v2_markdown_sha256_16=spec2["markdown_sha256_16"],
                registry_sha256_16=_dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2_1.json")),
                multiplicity_sha256_16=_dig(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V2_1.json")),
                k_total=enum["k"], k_v1=enum["v1_k"], k_added=enum["added"], deduplicated=enum["deduplicated"],
                k_by_family=mult["k_by_family"], cells_sha256_16=enum["cells_sha256_16"], bindings_sha256_16=enum["bindings_sha256_16"],
                new_cell_identities=cell_ids, confirmatory_states=[c["name"] for c in R2.CLAIMS], entry_offset_from_D=R2.ENTRY_OFFSET,
                descriptive_only=list(R2.DESCRIPTIVE_ONLY.keys()),
                close60_threshold="CLOSE60/OPEN60 >= 1.0 unchanged (broad; median ~2.07 is a pre-outcome property)",
                report_only_diagnostics=dict(names=["POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5"], module_sha256_16=code["ovd_diagnostics.py"],
                                             exposure_set_sha256_16=a10["table_sha256_16"], unchanged_from_v1=True),
                limitations=reg1["limitations"], fixtures=fx, outcome_access_count=0, pathsim_call_count=0,
                outcome_access_verification=access, pathsim=seal1["pathsim"],
                execution_rule="ONLY SEAL_V2_1 unlocks outcome execution (ovd_seal_v2.execution_authority, seal chain); V2 and V1 = immutable "
                               "provenance, SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION",
                outcome_phase="NOT OPENED by this command — _pathsim is a SEPARATE, explicit, user-authorised subsequent action")
    json.dump(seal, open(os.path.join(FAMILY_DIR, "SEAL_V2_1.json"), "w"), indent=1, default=str)
    json.dump(dict(v2_seal_sha256=V2_SEAL_SHA256, status="SUPERSEDED_PRE_OUTCOME_FOR_EXECUTION", at=sealed_at,
                   superseded_by=dict(seal="SEAL_V2_1.json", sha256=hashlib.sha256(open(os.path.join(FAMILY_DIR, "SEAL_V2_1.json"), "rb").read()).hexdigest()),
                   note="V2 bytes are untouched; V2 remains immutable provenance; it is no longer current for execution"),
              open(os.path.join(FAMILY_DIR, "V2_STATUS.json"), "w"), indent=1)
    return seal


if __name__ == "__main__":
    if "--seal" in sys.argv:
        print(json.dumps(ovd_seal_v2_1(), indent=1, default=str))
    else:
        import ovd_registry_v2 as R2
        _, reg1 = R2.v1_sealed()
        e = R2.enumerate_v2(reg1)
        print(json.dumps(dict(v1=S2.assert_v1_immutable(), v2=assert_v2_immutable(), access=S2.assert_no_outcome_access(),
                              k=e["k"], hard_stop_for_review=e["hard_stop_for_review"], bindings=e["bindings"],
                              v2_1_run=latest_v2_1_run(e["bindings_sha256_16"])), indent=1, default=str))
