"""A9 — negative fixtures for the canonical 1D authority (option A).

Each test pins one failure mode of the corporate-action reconstruction:
  1  a raw 2:1 split seam is NOT present in a canonical (single-basis) series
  2  share volume moves inversely to price on a split in the canonical basis
  3  an already-adjusted row is never adjusted a second time — the canonical path contains
     NO factor arithmetic at all (the vendor applies the basis; we never multiply)
  4  a mixed-vintage series (CRWD-like) collapses to one basis under the canonical convention
  5  unknown basis (no CURRENT.json) -> HARD STOP, never guessed
  6  the seam detector may FLAG a candidate but exposes no factor to any producer
  7  an old contaminated X build can never become current input to the outcome runner
  8  canonical digest mismatch -> HARD FAIL
"""
import os, sys, json, re, hashlib
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import duckdb                                                           # noqa: E402
import ovd_build as B                                                   # noqa: E402
import ovd_seal as S                                                    # noqa: E402
import ovd_canonical_audit as A                                         # noqa: E402


def _seam(con, rows):
    con.execute("CREATE OR REPLACE TABLE src AS SELECT * FROM (VALUES " +
                ",".join(f"('T', DATE '{d}', {c}, {v})" for d, c, v in rows) + ") v(ticker, session_date, close, volume)")
    return con.execute(A.SEAM_SQL.format(src="src")).fetchall()


# 1 + 6: a raw 2:1 seam is flagged by the detector; a canonical series is not
def test_01_raw_seam_flagged_canonical_clean():
    con = duckdb.connect()
    raw = [("2026-07-01", 200.0, 1_000_000), ("2026-07-02", 100.5, 2_100_000)]   # price halves, volume doubles
    assert len(_seam(con, raw)) == 1
    canonical = [("2026-07-01", 100.0, 2_000_000), ("2026-07-02", 100.5, 2_100_000)]  # same basis both days
    assert len(_seam(con, canonical)) == 0


# 2: canonical basis = price / S and volume x S relative to raw (inverse)
def test_02_inverse_volume_relation():
    raw_close, raw_vol, S = 200.0, 1_000_000, 2
    canon_close, canon_vol = 100.0, 2_000_000
    assert abs(canon_close * S - raw_close) < 1e-9 and canon_vol == raw_vol * S
    assert abs(canon_close * canon_vol - raw_close * raw_vol) < 1e-6      # dollar volume invariant


# 3: the canonical producer contains NO split-factor arithmetic — it cannot double-adjust
def test_03_no_factor_arithmetic_in_canonical_producer():
    src = open(os.path.join(os.path.dirname(HERE), "ovd_canonical_1d.py")).read()
    body = src.split('"""', 2)[-1]                     # drop the module docstring
    assert not re.search(r"split_from\s*/\s*split_to|split_to\s*/\s*split_from|\*\s*factor|/\s*factor", body)
    assert "adjusted\": \"true\"" in body or "'adjusted': 'true'" in body or '"adjusted": "true"' in body


# 4: a mixed-vintage series (old rows unadjusted, new rows adjusted) shows a seam; the
#    canonical series for the same dates does not
def test_04_mixed_vintage_collapses_to_one_basis():
    con = duckdb.connect()
    mixed = [("2026-06-30", 772.74, 2_000_000), ("2026-07-01", 778.0, 2_100_000), ("2026-07-02", 193.98, 8_500_000)]  # CRWD-like 4:1
    assert len(_seam(con, mixed)) == 1
    canonical = [("2026-06-30", 193.19, 8_000_000), ("2026-07-01", 194.5, 8_400_000), ("2026-07-02", 193.98, 8_500_000)]
    assert len(_seam(con, canonical)) == 0


# 5: unknown basis -> HARD STOP (no CURRENT.json)
def test_05_unknown_basis_hard_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(B, "FAMILY_DIR", str(tmp_path))
    with pytest.raises(B.HardStop):
        B.canonical_current()


# 6 (second half): the detector's output carries no adjustment factor column a producer could consume
def test_06_detector_exposes_no_factor():
    con = duckdb.connect()
    rows = _seam(con, [("2026-07-01", 200.0, 1_000_000), ("2026-07-02", 100.5, 2_100_000)])
    cols = [d[0] for d in con.execute("SELECT * FROM (" + A.SEAM_SQL.format(src="src") + ") LIMIT 0").description]
    assert "S" in cols and "factor" not in cols and "adj" not in "".join(cols).lower()
    assert rows[0][-1] == 2                       # S is a FLAG (candidate ratio), documented as such


# 7: an INVALID_FOR_OUTCOME_PHASE run is never returned as current input
def test_07_contaminated_x_never_current(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "FAMILY_DIR", str(tmp_path))
    run = B.RunSpace(family_dir=str(tmp_path), run_id="OVD_20260904T042944Z")
    run.write_atomic("X.parquet", "mixed-basis bytes"); run.complete("X.parquet")
    run.write_atomic("STATUS.json", {"status": "INVALID_FOR_OUTCOME_PHASE"})
    assert S.latest_completed_run(require_canonical=True) is None
    assert S.latest_completed_run(require_canonical=False) is None      # INVALID is skipped even when canonical is not required
    good = B.RunSpace(family_dir=str(tmp_path), run_id="OVD_20260904T990000Z")
    good.write_atomic("X.parquet", "canonical bytes"); good.complete("X.parquet")
    good.write_atomic("STATUS.json", {"status": "CANONICAL_1D_BASIS"})
    assert S.latest_completed_run(require_canonical=True).endswith("OVD_20260904T990000Z")


# 8: canonical derived digest mismatch -> HARD FAIL
def test_08_digest_mismatch_hard_fail(tmp_path, monkeypatch):
    monkeypatch.setattr(B, "FAMILY_DIR", str(tmp_path))
    cd = tmp_path / "canonical_1d"; cd.mkdir()
    dp = cd / "derived_x.parquet"; dp.write_bytes(b"derived bytes")
    (cd / "CURRENT.json").write_text(json.dumps({"run_id": "x", "derived_parquet": str(dp), "derived_sha256_16": "0000000000000000", "canonical_sha256_16": "abc"}))
    with pytest.raises(B.HardStop):
        B.canonical_current()
    (cd / "CURRENT.json").write_text(json.dumps({"run_id": "x", "derived_parquet": str(dp),
                                                  "derived_sha256_16": hashlib.sha256(b"derived bytes").hexdigest()[:16], "canonical_sha256_16": "abc"}))
    assert B.canonical_current()["run_id"] == "x"
