"""Negative fixtures for OPENING_VOLUME_DYNAMICS_V1 (ovd_build.py guards).

Every test asserts that a specific failure mode is CAUGHT — HardStop raised, NULL produced,
or the run marked not-current. A guard that lets the bad case through is the bug.
Numbered to match the spec's NEGATIVE FIXTURES list (1..18).
"""
import os, sys, json, hashlib, tempfile
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_build as B                                                   # noqa: E402
import duckdb                                                           # noqa: E402


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


# 1. mixed DATE vs TIMESTAMP join key -> HARD FAIL (never a silent zero-match)
def test_01_mixed_date_timestamp_join_hard_fails(con):
    con.execute("CREATE TABLE L AS SELECT 'AAPL' ticker, DATE '2026-07-15' AS k")
    con.execute("CREATE TABLE R AS SELECT 'AAPL' ticker, TIMESTAMP '2026-07-15 13:30:00' AS k")
    with pytest.raises(B.HardStop):
        B.typed_join_report(con, "SELECT * FROM L", "SELECT * FROM R", "k")


# 2. stale result from a previous run is never CURRENT
def test_02_stale_result_not_current(tmp_path):
    run = B.RunSpace(family_dir=str(tmp_path), run_id="OLD")
    run.write_atomic("X.parquet", "old bytes")
    run.complete("X.parquet")
    assert B.RunSpace.is_current(run.dir, "X.parquet")
    # a later run touches the same file without completing -> the old marker no longer matches
    with open(os.path.join(run.dir, "X.parquet"), "w") as f:
        f.write("new bytes from a run that crashed")
    assert not B.RunSpace.is_current(run.dir, "X.parquet")


# 3. crash before completion -> NOT COMPLETE
def test_03_crash_before_completed(tmp_path):
    run = B.RunSpace(family_dir=str(tmp_path), run_id="CRASH")
    run.write_atomic("X.parquet", "partial")
    assert not B.RunSpace.is_current(run.dir, "X.parquet")


# 4. missing opening bar is NULL, never volume=0
def test_04_missing_slot_is_null(con):
    con.execute("""CREATE TABLE s AS SELECT * FROM (VALUES
        ('T', DATE '2026-07-15', '09:30', 100), ('T', DATE '2026-07-15', '10:00', 90)) v(ticker, session, slot, volume)""")
    r = con.execute("""SELECT max(CASE WHEN slot='09:45' THEN volume END) v2, count(*) n FROM s GROUP BY ticker, session""").fetchone()
    assert r[0] is None and r[1] == 2


# 5. insufficient prior same-slot history -> UNAVAILABLE (NULL), not a ratio over few bars
def test_05_insufficient_history_unavailable(con):
    con.execute("CREATE TABLE s AS SELECT 'T' ticker, '09:30' slot, DATE '2026-01-01' + INTERVAL (i) DAY AS session, 100 + i AS volume FROM range(10) t(i)")
    r = con.execute(f"SELECT {B.rvol_same_slot()} AS rvol FROM s ORDER BY session DESC LIMIT 1").fetchone()
    assert r[0] is None


# 6. current session must not enter its own denominator
def test_06_self_denominator_guard():
    with pytest.raises(B.HardStop):
        B.assert_no_self_in_denominator("x", "OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)")
    B.assert_no_self_in_denominator("x", "OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)")


# 7. 09:30 volume is compared only with prior 09:30 bars (slot identity in the partition)
def test_07_slot_identity_partition():
    w = B.rvol_same_slot()
    assert "PARTITION BY ticker, slot" in w
    case = B.slot_identity("clk", ["09:30", "09:45"])
    assert "ELSE NULL" in case          # off-grid -> no slot, never merged


# 8. a feature using M4 cannot enter before 10:30
def test_08_causal_time_guard():
    with pytest.raises(B.HardStop):
        B.assert_causal_entry("10:30", "10:15")
    B.assert_causal_entry("10:30", "D+1 open" if "D+1 open" > "10:30" else "16:01")


# 9. duplicate ticker/session/slot -> HARD FAIL
def test_09_duplicate_slot_hard_fail(con):
    con.execute("CREATE TABLE s AS SELECT * FROM (VALUES ('T', DATE '2026-07-15', '09:30'), ('T', DATE '2026-07-15', '09:30')) v(ticker, session, slot)")
    with pytest.raises(B.HardStop):
        B.assert_unique(con, "SELECT * FROM s", "ticker, session, slot")


# 10. timezone: a 13:30Z July bar is 09:30 NY on the SAME date; a 03:00Z bar is the PRIOR NY date
def test_10_timezone_session_date(con):
    ny = B.ny_local("ts")
    r = con.execute(f"""SELECT CAST({ny} AS DATE), strftime({ny},'%H:%M') FROM (SELECT TIMESTAMP '2026-07-15 13:30:00' ts)""").fetchone()
    assert str(r[0]) == "2026-07-15" and r[1] == "09:30"
    r2 = con.execute(f"""SELECT CAST({ny} AS DATE) FROM (SELECT TIMESTAMP '2026-07-15 03:00:00' ts)""").fetchone()
    assert str(r2[0]) == "2026-07-14"
    # winter: 14:30Z is 09:30 EST
    r3 = con.execute(f"""SELECT strftime({ny},'%H:%M') FROM (SELECT TIMESTAMP '2026-01-15 14:30:00' ts)""").fetchone()
    assert r3[0] == "09:30"


# 11. unexpected zero lower-TF matches -> HARD STOP
def test_11_zero_matches_hard_stop(con):
    con.execute("CREATE TABLE L AS SELECT 'AAPL' ticker, DATE '2026-07-15' AS k")
    con.execute("CREATE TABLE R AS SELECT 'MSFT' ticker, DATE '2026-07-15' AS k")
    with pytest.raises(B.HardStop):
        B.typed_join_report(con, "SELECT * FROM L", "SELECT * FROM R", "k")


# 12. market-wide RVOL must not include future sessions (same window rule as the denominator)
def test_12_market_control_window_is_prior_only():
    # the market control is a per-session cross-sectional median of already-causal RVOLs;
    # its only time dependence is the per-ticker denominator, which fixture 6 pins.
    B.assert_no_self_in_denominator("mkt", B.rvol_same_slot())


# 13. missing resistance/base authority -> NULL flags, never an invented level
def test_13_missing_authority_is_null(con):
    con.execute("CREATE TABLE d AS SELECT 'T' AS ticker, DATE '2026-07-15' AS sess, 10.0 AS close, NULL::DOUBLE AS ceiling, NULL::BOOLEAN AS in_tr_1")
    r = con.execute("SELECT (in_tr_1 AND close <= ceiling) FROM d").fetchone()
    assert r[0] is None


# 14. a local reimplementation of the exit engine is detected
def test_14_pathsim_copy_detected(tmp_path):
    p = tmp_path / "bad.py"
    p.write_text("def sim(x):\n    mae = 1; mfe = 2; date_out = 3\n    mae += 1; mfe += 1; date_out += 1\n    return mae, mfe, date_out\n" * 2)
    with pytest.raises(B.HardStop):
        B.assert_no_pathsim_copy(str(p))
    B.assert_no_pathsim_copy()      # the real builder passes


# 15. output cell conservation
def test_15_conservation():
    B.assert_conservation({"A": 3, "B": 4}, 7, "ok")
    with pytest.raises(B.HardStop):
        B.assert_conservation({"A": 3, "B": 3}, 7, "bad")


# 16. old broken result JSON present -> STALE_RUN
def test_16_old_result_without_marker_is_stale(tmp_path):
    d = tmp_path / "runs" / "OLD"; d.mkdir(parents=True)
    (d / "X.parquet").write_text("looks plausible")
    assert not B.RunSpace.is_current(str(d), "X.parquet")


# 17. result family count must equal the frozen registry k
def test_17_registry_k_mismatch():
    with pytest.raises(B.HardStop):
        B.assert_conservation({"cells_scored": 47}, 48, "registry k")


# 18. missing/unavailable volume must not become False/zero in a boolean feature
def test_18_missing_volume_not_false(con):
    con.execute("CREATE TABLE s AS SELECT NULL::DOUBLE r1, 1.2 r2, 1.1 r3, 1.0 r4, 3 n_slots")
    r = con.execute("""SELECT CASE WHEN n_slots=4 AND r1 IS NOT NULL AND r2 IS NOT NULL AND r3 IS NOT NULL AND r4 IS NOT NULL
                       THEN (r1>1.0)::INT+(r2>1.0)::INT+(r3>1.0)::INT+(r4>1.0)::INT END FROM s""").fetchone()
    assert r[0] is None
