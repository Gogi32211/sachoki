"""V2 negative fixtures (PRE-OUTCOME DESIGN AMENDMENT V2) — numbered as in the user's gate.

 1-4  OPEN30_A / OPEN30_B / CLOSE30_A / CLOSE30_B = exact sums of their two 15m bars
 5-6  the stored 15:30 1H bar as CLOSE60 / as CLOSE30_B source -> source-identity FAIL
 7    one required 15m slot missing -> the 30m value is UNAVAILABLE (never 0)
 8-9  OPEN60 != OPEN30_A+OPEN30_B / CLOSE60 != CLOSE30_A+CLOSE30_B -> HARD FAIL
 10   early-close session emits CLOSE30/CLOSE60 -> must be UNAVAILABLE
 11   current session inside a 30m/60m RVOL denominator -> FAIL
 12   wrong historical slot in the denominator -> FAIL
 13-15 Logic 3 FALSE cases (60m and 30m)
 16-17 missing D+1 OPEN60 / OPEN30_A in Logic 4 -> UNAVAILABLE, not FALSE
 18-19 valid handoffs inside 0.80-1.25 -> TRUE
 20   ratio outside the band -> FALSE only with all inputs valid
 21-22 Logic 4 entry at D+1 -> causal-time FAIL (60m and 30m)
 23   confirmatory claim without registry-k update -> HARD FAIL
 24   V1 seal presented to the outcome runner -> SUPERSEDED_EXECUTION_AUTHORITY
 25   V1 SEAL.json bytes changed -> HARD FAIL
 26   a _pathsim call site in OVD code -> HARD FAIL (static)      27 outcome-bearing file -> HARD FAIL
 28   DESCRIPTIVE_ONLY state enters promotion logic -> HARD FAIL
"""
import os, sys, json, re
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_build_v2 as V                                                # noqa: E402
import ovd_registry_v2 as R                                             # noqa: E402
import ovd_seal_v2 as S                                                 # noqa: E402
from ovd_build import HardStop                                          # noqa: E402

b15 = lambda start, vol: dict(store="15m", tf="15m", minutes=15, start_ny=start, volume=vol)


def test_01_04_half_hours_are_exact_sums():
    assert V.window_from_bars([b15("09:30", 100), b15("09:45", 50)], ("09:30", "09:45")) == 150
    assert V.window_from_bars([b15("10:00", 7), b15("10:15", 8)], ("10:00", "10:15")) == 15
    assert V.window_from_bars([b15("15:00", 1), b15("15:15", 2)], ("15:00", "15:15")) == 3
    assert V.window_from_bars([b15("15:30", 40), b15("15:45", 60)], ("15:30", "15:45")) == 100


def test_05_stored_1h_bar_as_close60_fails():
    with pytest.raises(HardStop, match="SOURCE_IDENTITY"):
        V.assert_slot_source(dict(store="1h", tf="1h", minutes=30, start_ny="15:30", volume=100))


def test_06_stored_1h_bar_as_close30b_source_fails():
    with pytest.raises(HardStop, match="SOURCE_IDENTITY"):
        V.window_from_bars([dict(store="1h", tf="1h", minutes=30, start_ny="15:30", volume=100), b15("15:45", 1)], ("15:30", "15:45"))


def test_07_missing_slot_is_unavailable_never_zero():
    assert V.window_from_bars([b15("09:30", 100)], ("09:30", "09:45")) is None
    assert V.window_from_bars([b15("09:30", 100), b15("09:45", None)], ("09:30", "09:45")) is None
    assert V.hour_from_halves(None, 5) is None
    with pytest.raises(HardStop, match="duplicate"):
        V.window_from_bars([b15("09:30", 100), b15("09:30", 100)], ("09:30", "09:45"))   # a bar can never be doubled


def test_08_09_hour_conservation_hard_fail():
    V.assert_hour_conservation(150, 100, 50, "OPEN60")
    with pytest.raises(HardStop):
        V.assert_hour_conservation(151, 100, 50, "OPEN60")
    with pytest.raises(HardStop):
        V.assert_hour_conservation(3, 1, 3, "CLOSE60")
    with pytest.raises(HardStop):
        V.assert_hour_conservation(10, None, 10, "CLOSE60")


def test_10_early_close_emits_no_close_windows():
    assert V.close_windows(True, 100, 200) == (None, None, None)
    assert V.close_windows(False, 100, 200) == (100, 200, 300)


def test_11_current_session_in_denominator_fails():
    from ovd_build import assert_no_self_in_denominator
    with pytest.raises(HardStop):
        assert_no_self_in_denominator("rvol", "OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)")
    assert "1 PRECEDING" in V.rvol_sql("open30_a")


def test_12_wrong_historical_slot_in_denominator_fails():
    with pytest.raises(HardStop, match="same-slot"):
        V.assert_denominator_slot("close30_b", "open30_b")
    V.assert_denominator_slot("close60", "close60")


def test_13_15_logic3_false_cases():
    assert V.close60_dominance(True, 0.9, 120, 100) is False     # close >= open but RVOL_CLOSE60 <= 1
    assert V.close60_dominance(True, 1.4, 90, 100) is False      # RVOL > 1 but CLOSE60 < OPEN60
    assert V.close30_dominance(True, 0.8, 120, 100) is False     # 30m analog: CLOSE30_B >= OPEN30_A but RVOL <= 1
    assert V.close60_dominance(False, 1.4, 120, 100) is False    # no decline context
    assert V.close60_dominance(True, 1.4, 120, 100) is True
    assert V.close60_dominance(True, None, 120, 100) is None     # unavailable input -> UNAVAILABLE


def test_16_17_missing_next_open_is_unavailable_not_false():
    assert V.handoff(True, 1.5, None, None) is None              # missing D+1 OPEN60 -> no RVOL, no ratio
    assert V.handoff(True, 1.5, 1.2, None) is None               # missing D+1 OPEN30_A ratio


def test_18_19_valid_handoffs_true():
    assert V.handoff(True, 1.3, 1.1, 0.80) is True
    assert V.handoff(True, 1.3, 1.1, 1.25) is True
    assert V.handoff(True, 2.0, 1.5, 1.0) is True


def test_20_outside_band_false_only_with_valid_inputs():
    assert V.handoff(True, 1.3, 1.1, 1.26) is False
    assert V.handoff(True, 1.3, 1.1, 0.79) is False
    assert V.handoff(True, 1.3, None, 0.79) is None
    assert V.HANDOFF_BAND == (0.80, 1.25)


def test_21_22_logic4_entry_at_d1_fails_causal_time():
    with pytest.raises(HardStop, match="CAUSAL_TIME"):
        V.assert_entry_offset("CLOSE60_TO_NEXT_OPEN60_HANDOFF", 1)
    with pytest.raises(HardStop, match="CAUSAL_TIME"):
        V.assert_entry_offset("CLOSE30_TO_NEXT_OPEN30_HANDOFF", 1)
    V.assert_entry_offset("CLOSE60_TO_NEXT_OPEN60_HANDOFF", 2)
    V.assert_entry_offset("CLOSE60_DOMINANCE", 1)


def test_23_claim_without_registry_update_fails():
    cells = R.new_cells(R.v1_sealed()[1])
    R.assert_registry_covers(list(R.CLAIM_COLUMNS), cells)
    with pytest.raises(HardStop, match="not in the registry"):
        R.assert_registry_covers(list(R.CLAIM_COLUMNS) + ["CLOSE30_LATE_SPIKE_REVERSAL"], cells)


def test_24_v1_seal_rejected_by_outcome_runner():
    v1 = json.load(open(os.path.join(S.FAMILY_DIR, "SEAL.json")))
    with pytest.raises(HardStop, match="SUPERSEDED_EXECUTION_AUTHORITY"):
        S.execution_authority(v1)


def test_25_v1_seal_bytes_changed_fails(tmp_path, monkeypatch):
    fam = tmp_path / "fam"; fam.mkdir()
    (fam / "SEAL.json").write_text('{"family": "OPENING_VOLUME_DYNAMICS_V1", "tampered": true}')
    monkeypatch.setattr(S, "FAMILY_DIR", str(fam))
    with pytest.raises(HardStop, match="bytes changed"):
        S.assert_v1_immutable()
    monkeypatch.undo()
    assert S.assert_v1_immutable()["v1_seal_sha256"] == S.V1_SEAL_SHA256   # the real V1 is intact


def test_26_pathsim_call_site_is_a_hard_fail(tmp_path, monkeypatch):
    # static guard: a module with a _pathsim( call site must trip the scan
    bad = tmp_path / "ovd_build_v2.py"; bad.write_text("x = _pathsim(df)\n")
    monkeypatch.setattr(S, "HERE", str(tmp_path)); monkeypatch.setattr(S, "FAMILY_DIR", str(tmp_path))
    with pytest.raises(HardStop, match="_pathsim call site"):
        S.assert_no_outcome_access()
    monkeypatch.undo()
    for m in S.OVD_MODULES:                                             # and the real modules are clean
        p = os.path.join(S.HERE, m)
        if os.path.exists(p):
            assert not re.search(r"_pathsim\s*\(", open(p).read()), m


def test_27_outcome_bearing_file_is_a_hard_fail(tmp_path, monkeypatch):
    (tmp_path / "outcome_table.parquet").write_bytes(b"x")
    monkeypatch.setattr(S, "FAMILY_DIR", str(tmp_path))
    with pytest.raises(HardStop, match="outcome-bearing"):
        S.assert_no_outcome_access()
    monkeypatch.undo()
    if os.path.exists(os.path.join(S.FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")):
        with pytest.raises(HardStop, match="outcome-bearing"):          # the family is EXPOSED: the guard must trip
            S.assert_no_outcome_access()
    else:
        assert S.assert_no_outcome_access()["outcome_bearing_files"] == 0   # pre-outcome: the real family is clean


def test_28_descriptive_only_cannot_be_promoted():
    with pytest.raises(HardStop, match="DESCRIPTIVE_ONLY"):
        R.assert_not_promotable("CLOSE30_SHAPE")
    with pytest.raises(HardStop):
        R.assert_not_promotable("NEXT_OPEN30_PERSISTENCE")
    R.assert_not_promotable("CLOSE60_DOMINANCE")


def test_29_logic1_logic2_predicates():
    assert V.open30_sustained_build_3d(0.8, 0.9, 1.1, 0.7, 0.9, 1.0) is True
    assert V.open30_sustained_build_3d(0.8, 0.9, 1.1, 0.9, 0.9, 1.0) is False    # second half not strictly rising
    assert V.open30_sustained_build_3d(0.8, 0.9, 1.1, None, 0.9, 1.0) is None
    assert V.full_opening_reclaim_30(1.0, 1.2) is True and V.full_opening_reclaim_30(0.99, 1.2) is False
    assert V.full_opening_reclaim_30(None, 1.2) is None


def test_30_enumeration_is_exact_and_dedup_aware():
    seal, reg1 = R.v1_sealed()
    e = R.enumerate_v2(reg1)
    n_bind = len(R.bindings(reg1))
    assert e["v1_k"] == seal["k_total"] == 293 and e["k"] == 293 + e["added"] and e["states"] == 6
    assert e["added"] + len(e["deduplicated"]) == n_bind                      # one cell per (state, bound family)
    # a claim whose FULL identity (feature, bin, family, sample, as-of, entry) already exists is NOT duplicated
    dup = [c for c in R.new_cells(reg1) if c["feature"] == "CLOSE60_DOMINANCE"][0]
    fake = dict(reg1); fake["cells"] = reg1["cells"] + [dup]
    fake["k"] = dict(reg1["k"], total=reg1["k"]["total"] + 1)
    e2 = R.enumerate_v2(fake)
    assert e2["added"] == e["added"] - 1 and e2["deduplicated"][0]["claim"] == "CLOSE60_DOMINANCE" and e2["k"] == e["k"]
