"""V2.1 fixtures (PRE-OUTCOME HARNESS RESEAL + sample-binding correction).

 1  OPEN30_SUSTAINED_BUILD_3D on a row outside its V1 parent sample -> NOT_ELIGIBLE (not a family-C FALSE)
 2  FULL_OPENING_RECLAIM_30 outside its V1 parent sample -> NOT_ELIGIBLE
 3  Logic 3 valid decline-context row outside BASE/BREAKOUT -> eligible under family C
 4  Logic 4 valid decline-context handoff outside BASE/BREAKOUT -> eligible under family C
 5  identical feature/bin but different sample authority -> different claim identity
 6  V2 seal presented as current after V2.1 exists -> SUPERSEDED_EXECUTION_AUTHORITY
 7  corrected on-disk harness digest differs from the V2 seal but matches V2.1 -> PASS
 +  bindings are recovered from the SEALED V1 registry (never prose); enumeration is exact and never forced
"""
import os, sys, json, hashlib
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_build_v2 as V                                                # noqa: E402
import ovd_registry_v2 as R                                             # noqa: E402
import ovd_seal_v2 as S                                                 # noqa: E402
from ovd_build import HardStop                                          # noqa: E402


def _bind(name):
    _, reg1 = R.v1_sealed()
    return [b for b in R.bindings(reg1) if b["name"] == name]


def test_01_logic1_outside_parent_sample_not_eligible():
    b = _bind("OPEN30_SUSTAINED_BUILD_3D")
    assert all(x["family"] in ("A", "B") for x in b) and all(x["family"] != "C" for x in b)
    # row with a valid TRUE state but base=False (neither A nor B) -> NOT_ELIGIBLE, never FALSE
    for x in b:
        in_sample = {"A": False, "B": False}[x["family"]]
        assert V.claim_status(in_sample, True) == "NOT_ELIGIBLE"
    assert V.claim_status(True, True) == "TRUE" and V.claim_status(True, False) == "FALSE" and V.claim_status(True, None) == "UNAVAILABLE"


def test_02_logic2_outside_parent_sample_not_eligible():
    b = _bind("FULL_OPENING_RECLAIM_30")
    assert [x["family"] for x in b] == list(R.parent_families(R.v1_sealed()[1], "PRIOR_HV_VOLUME_RECLAIM"))
    assert V.claim_status(False, True) == "NOT_ELIGIBLE"


def test_03_logic3_outside_base_breakout_stays_eligible():
    for n in ("CLOSE60_DOMINANCE", "CLOSE30_DOMINANCE"):
        b = _bind(n)
        assert len(b) == 1 and b[0]["family"] == "C" and b[0]["sample_sql"] == "TRUE"
    assert V.claim_status(True, V.close60_dominance(True, 1.4, 120, 100)) == "TRUE"     # decline context, not BASE/BREAKOUT


def test_04_logic4_outside_base_breakout_stays_eligible():
    for n in ("CLOSE60_TO_NEXT_OPEN60_HANDOFF", "CLOSE30_TO_NEXT_OPEN30_HANDOFF"):
        b = _bind(n)
        assert len(b) == 1 and b[0]["family"] == "C"
    assert V.claim_status(True, V.handoff(True, 1.3, 1.1, 1.0)) == "TRUE"


def test_05_same_feature_bin_different_sample_is_different_identity():
    _, reg1 = R.v1_sealed()
    s, e = reg1["samples"], reg1["entry"]
    a = dict(kind="state", family="A", feature="OPEN30_SUSTAINED_BUILD_3D", bin="TRUE", sample=s["A"], as_of="10:30 ET D", entry="D+1 open")
    c = dict(a, family="C", sample=R.FAMILY_C["sample"])
    assert R.identity(a, s, e, e) != R.identity(c, s, e, e)
    # V1 cells carry the registry-level sample / as-of / entry in their identity
    v1 = reg1["cells"][0]
    assert json.loads(R.identity(v1, s, e, e))["sample"] == s[v1["family"]]


def test_06_v2_seal_superseded_once_v2_1_exists(tmp_path, monkeypatch):
    v2 = dict(family="OPENING_VOLUME_DYNAMICS_V1", amendment="OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_DESIGN_AMENDMENT_V2")
    (tmp_path / "SEAL_V2.json").write_text(json.dumps(v2))
    (tmp_path / "SEAL_V2_1.json").write_text(json.dumps(dict(family="OPENING_VOLUME_DYNAMICS_V1",
                                                                 amendment="OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1")))
    monkeypatch.setattr(S, "FAMILY_DIR", str(tmp_path))
    with pytest.raises(HardStop, match="SUPERSEDED_EXECUTION_AUTHORITY"):
        S.execution_authority(v2)
    with pytest.raises(HardStop, match="SUPERSEDED_EXECUTION_AUTHORITY"):
        S.execution_authority(dict(family="OPENING_VOLUME_DYNAMICS_V1"))          # V1 still rejected


def test_07_harness_digest_differs_from_v2_but_matches_current_seal():
    fam = S.FAMILY_DIR
    v2 = json.load(open(os.path.join(fam, "SEAL_V2.json")))
    on_disk = S._dig(os.path.join(S.HERE, "ovd_seal_v2.py"))
    assert on_disk != v2["code"]["ovd_seal_v2.py"]                                   # the disclosed drift
    hc = json.load(open(os.path.join(fam, "HARNESS_CORRECTIONS.json")))
    entries = hc if isinstance(hc, list) else [hc]
    assert entries[-1]["sealed_digest_in_SEAL_V2"] == v2["code"]["ovd_seal_v2.py"]
    p21 = os.path.join(fam, "SEAL_V2_1.json")
    if os.path.exists(p21):
        s21 = json.load(open(p21))
        assert s21["code"]["ovd_seal_v2.py"] == on_disk                             # V2.1 binds the corrected code
        assert S.execution_authority()["amendment"].endswith("V2_1")
    else:
        assert entries[-1]["on_disk_digest_after_correction"] == on_disk           # pre-seal: the record names the current bytes
        with pytest.raises(HardStop, match="HARNESS_CODE_DRIFT"):
            S.execution_authority()                                                 # V2 is current but its bound code drifted


def test_08_bindings_from_sealed_registry_and_exact_enumeration():
    _, reg1 = R.v1_sealed()
    assert R.parent_families(reg1, "OPEN1H_RAMP_3D") == reg1["features"]["OPEN1H_RAMP_3D"][2]
    assert R.parent_families(reg1, "PRIOR_HV_VOLUME_RECLAIM") == reg1["features"]["PRIOR_HV_VOLUME_RECLAIM"][2]
    with pytest.raises(HardStop):
        R.parent_families(reg1, "NOT_A_V1_FEATURE")
    e = R.enumerate_v2(reg1)
    assert e["v1_k"] == 293 and e["k"] == 293 + e["added"] and e["states"] == 6
    assert e["hard_stop_for_review"] == (e["k"] != R.EXPECTED_K)                  # never forced
