"""Outcome-runner engineering fixtures (first historical execution).

  wrong seal / V1 / V2 presented   -> reject (SUPERSEDED_EXECUTION_AUTHORITY)
  k != 300                          -> reject
  missing / extra / duplicate cell  -> reject
  wrong entry offset                -> reject
  local _pathsim implementation     -> reject
  stale / partial output            -> NOT CURRENT
  ledger never resets               -> reject a second record for the same run; count only increments
  bin parser partitions the line    -> right-open, closed-edge aware
  leave-one-out control is exact
"""
import os, sys, json
import numpy as np, pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_outcome_v1 as O                                              # noqa: E402
import ovd_seal_v2 as S                                                 # noqa: E402
from ovd_build import HardStop, RunSpace                                # noqa: E402


def test_01_wrong_v1_v2_seals_rejected():
    fam = S.FAMILY_DIR
    for name in ("SEAL.json", "SEAL_V2.json"):
        with pytest.raises(HardStop, match="SUPERSEDED_EXECUTION_AUTHORITY"):
            S.execution_authority(json.load(open(os.path.join(fam, name))))
    with pytest.raises(HardStop):
        S.execution_authority(dict(family="OPENING_VOLUME_DYNAMICS_V1", amendment="SOMETHING_ELSE"))


def _cells(n):
    return [dict(claim_id=f"C|F|{i}", feature="F", entry_offset_from_D=1) for i in range(n)]


def test_02_k_and_cell_reconciliation():
    O.assert_registry_exact(_cells(300), 300)
    with pytest.raises(HardStop, match="!= k"):
        O.assert_registry_exact(_cells(299), 300)
    cells = _cells(300)
    O.assert_result_reconciles([c["claim_id"] for c in cells], cells)
    with pytest.raises(HardStop, match="missing"):
        O.assert_result_reconciles([c["claim_id"] for c in cells[:-1]], cells)
    with pytest.raises(HardStop, match="extra"):
        O.assert_result_reconciles([c["claim_id"] for c in cells] + ["C|F|extra"], cells)
    with pytest.raises(HardStop, match="duplicate"):
        O.assert_result_reconciles([c["claim_id"] for c in cells] + [cells[0]["claim_id"]], cells)
    dup = cells + [cells[0]]
    with pytest.raises(HardStop, match="duplicate"):
        O.assert_registry_exact(dup, 301)


def test_03_wrong_entry_offset_rejected():
    with pytest.raises(HardStop, match="wrong entry offset"):
        O.assert_entry_offset_ok(dict(claim_id="C|CLOSE60_TO_NEXT_OPEN60_HANDOFF|TRUE", feature="CLOSE60_TO_NEXT_OPEN60_HANDOFF", entry_offset_from_D=1))
    O.assert_entry_offset_ok(dict(claim_id="x", feature="CLOSE60_TO_NEXT_OPEN60_HANDOFF", entry_offset_from_D=2))
    O.assert_entry_offset_ok(dict(claim_id="x", feature="OPEN1H_RVOL_LEVEL", entry_offset_from_D=1))


def test_04_local_pathsim_rejected_and_sacred_digest_bound():
    with pytest.raises(HardStop, match="local path-simulation"):
        O.assert_no_local_pathsim("def _pathsim_local(grp):\n    pass\n")
    with pytest.raises(HardStop, match="exit-engine arithmetic"):
        O.assert_no_local_pathsim("x = 1\ntrail_level = pk * (1 - t)\n")
    O.assert_no_local_pathsim(open(O.__file__).read())            # the runner itself is clean
    ps, slip = O.sacred_pathsim()
    assert ps.__module__ == "edge_replay" and abs(slip - 0.0015) < 1e-12


def test_05_stale_and_partial_output_not_current(tmp_path):
    r = RunSpace(family_dir=str(tmp_path), run_id="OUT_TEST")
    p = os.path.join(r.dir, "results_300.parquet"); open(p, "wb").write(b"partial")
    assert not RunSpace.is_current(r.dir, "results_300.parquet")     # no COMPLETED marker -> NOT CURRENT
    r.complete("results_300.parquet")
    assert RunSpace.is_current(r.dir, "results_300.parquet")
    open(p, "wb").write(b"rewritten")                                # stale: digest no longer matches
    assert not RunSpace.is_current(r.dir, "results_300.parquet")


def test_06_ledger_only_increments(tmp_path, monkeypatch):
    monkeypatch.setattr(O, "LEDGER", str(tmp_path / "OUTCOME_ACCESS_LEDGER.json"))
    assert O.ledger_read()["outcome_access_count"] == 0
    led = O.ledger_record_access("OUT_A", "sha")
    assert led["outcome_access_count"] == 1 and led["events"][0]["count_before"] == 0
    with pytest.raises(HardStop, match="already records"):
        O.ledger_record_access("OUT_A", "sha")
    assert O.ledger_record_access("OUT_B", "sha")["outcome_access_count"] == 2   # never resets


def test_07_bin_parser_partitions():
    bins = ["<0.75", "0.75-1.0", "1.0-1.5", "1.5-2.5", ">2.5"]
    s = pd.Series([0.5, 0.75, 1.0, 1.5, 2.5, 9.0, np.nan])
    m = np.array([O.bin_mask(s, bins, b) for b in bins])
    assert m.sum(axis=0).tolist() == [1, 1, 1, 1, 1, 1, 0]           # every non-null value in exactly one bin
    assert O.bin_mask(s, bins, "0.75-1.0")[1] and not O.bin_mask(s, bins, "<0.75")[1]
    b2 = ["<=0", "0-0.1", "0.1-0.25", ">0.25"]
    s2 = pd.Series([0.0, 0.05, 0.1, 0.25, -1.0])
    m2 = np.array([O.bin_mask(s2, b2, b) for b in b2])
    assert m2.sum(axis=0).tolist() == [1, 1, 1, 1, 1] and O.bin_mask(s2, b2, "<=0")[0]
    b3 = ["<=-0.10", "(-0.10,0.10)", ">=0.10"]
    s3 = pd.Series([-0.1, 0.0, 0.1])
    assert [O.bin_mask(s3, b3, b).tolist() for b in b3] == [[True, False, False], [False, True, False], [False, False, True]]
    assert O.bin_mask(pd.Series([0, 1, 2]), ["0", "1", "2", "3", "4"], "1").tolist() == [False, True, False]
    assert O.bin_mask(pd.Series(["MIXED", "STRICT_RISING"]), ["STRICT_RISING", "STRICT_FALLING", "MIXED"], "MIXED").tolist() == [True, False]


def test_08_leave_one_out_control_exact():
    df = pd.DataFrame(dict(date_in=["d"] * 5 + ["e"] * 2, ret=[1.0, 2.0, 3.0, 4.0, 100.0, 5.0, 7.0]))
    ctrl, noth = O.loo_control(df)
    assert noth.tolist() == [4, 4, 4, 4, 4, 1, 1]
    # removing 100 -> median(1,2,3,4)=2.5 ; removing 1 -> median(2,3,4,100)=3.5
    assert abs(ctrl[4] - 2.5) < 1e-9 and abs(ctrl[0] - 3.5) < 1e-9 and abs(ctrl[5] - 7.0) < 1e-9


def test_09_classification_gates_written_before_outcomes():
    g = O.GATES
    assert g["build"]["pos_years_ge"] == 4 and g["build"]["worst_year_ge"] == -2.0 and g["build"]["dsr_ge"] == 0.95
    stat = dict(n_days=200, n_obs=1000, median_edge=1.0, day_win=60.0, per_year={2022: 1.0, 2023: 0.5, 2024: 0.7, 2025: 0.2, 2026: -0.5},
                days_per_year={2022: 40, 2023: 40, 2024: 40, 2025: 40, 2026: 40})
    assert O.classify(stat, 0.99, 0.01) == "BUILD_CANDIDATE"
    assert O.classify(stat, 0.50, 0.01) == "NULL"
    assert O.classify(dict(stat, n_days=10), 0.99, 0.01) == "DESCRIPTIVE_ONLY"
    neg = dict(stat, median_edge=-1.0, day_win=40.0, per_year={y: -v for y, v in stat["per_year"].items()})
    assert O.classify(neg, 0.01, 0.99) == "VETO_CANDIDATE"
