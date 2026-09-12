"""INTRADAY_EFFORT_BALANCE_V1 fixtures.

 1  the L-code SQL reproduces the Pine WLNBB labels on a synthetic 15m series (exact digit sets)
 2  exact-label counting: every coded bar is exactly one label; L1-alone / L6-alone impossible
 3  balance formula + UNAVAILABLE below 3 effort bars; bin edges frozen
 4  OOS windows are disjoint, ordered and cover 2021-09-07..2026-09-03
 5  registry enumerates exactly k = 16 unique cells; 1H replication is not in k
 6  classification uses MINE gates first and requires VERIFY replication for BUILD
 7  seal refuses when an outcome-bearing file exists (OOS reserved pre-outcome)
"""
import os, sys, json
import numpy as np, pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import duckdb                                                            # noqa: E402
import ieb_family as F                                                   # noqa: E402
from ovd_build import HardStop                                           # noqa: E402


def _codes(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE src AS SELECT * FROM (VALUES " + ",".join(
        f"('T', TIMESTAMP '{d}', {o}, {h}, {l}, {c}, {v})" for d, o, h, l, c, v in rows) + ") t(ticker, date, open, high, low, close, volume)")
    return con.execute(F.lcode_sql("src") + " ORDER BY date").df()


def test_01_lcode_matches_pine_semantics():
    base = "2024-03-04 14:"
    rows = []
    # 20 warm-up bars: flat price, volume 100 (bucket stable)
    for i in range(22):
        rows.append((f"2024-03-01 {13 + i // 4:02d}:{(i % 4) * 15:02d}:00", 10.0, 10.1, 9.9, 10.0, 100))
    # bar A: volume 130 (same bucket or up), close up, close <= prev high (10.1)  -> L34
    rows.append((base + "00:00", 10.0, 10.1, 9.95, 10.05, 130))
    # bar B: volume 160 (up), close down, close <= prev high -> L46
    rows.append((base + "15:00", 10.05, 10.06, 9.9, 9.95, 160))
    # bar C: volume 120 (down), close up, close >= prev low -> L12
    rows.append((base + "30:00", 9.95, 10.0, 9.93, 9.98, 120))
    # bar D: volume 90 (down), close down but >= prev low (9.93) -> L25
    rows.append((base + "45:00", 9.98, 9.99, 9.94, 9.95, 90))
    df = _codes(rows)
    got = df.set_index("date")["code"].to_dict()
    import datetime as dt
    assert got[dt.datetime(2024, 3, 4, 14, 0)] == F.CODE_OF["L34"]
    assert got[dt.datetime(2024, 3, 4, 14, 15)] == F.CODE_OF["L46"]
    assert got[dt.datetime(2024, 3, 4, 14, 30)] == F.CODE_OF["L12"]
    assert got[dt.datetime(2024, 3, 4, 14, 45)] == F.CODE_OF["L25"]


def test_02_exact_label_partition():
    # any code is one of the 8 exact labels or 0; 1 and 32 never occur
    assert set(F.CODE_OF.values()) == {3, 18, 12, 40, 2, 4, 8, 16}
    assert 1 not in F.CODE_OF.values() and 32 not in F.CODE_OF.values()


def test_03_balance_and_bins():
    assert F.balance(4, 1, 1) == pytest.approx(4 / 6)
    assert F.balance(1, 0, 1) is None                       # 2 effort bars < 3 -> UNAVAILABLE
    assert F.balance(0, 0, 3) == -1.0
    assert F.bin_of(-0.33) == "B-" and F.bin_of(-0.3299) == "B0" and F.bin_of(0.33) == "B+" and F.bin_of(None) is None


def test_04_oos_windows():
    m, v = F.OOS["MINE"], F.OOS["VERIFY"]
    assert m[0] == "2021-09-07" and m[1] < v[0] and v[1] == "2026-09-03"


def test_05_registry_k16():
    cs = F.cells()
    assert len(cs) == 16 and len({c["claim_id"] for c in cs}) == 16 and F.K_EXPECTED == 16
    assert sum(1 for c in cs if c["kind"] == "conditional") == 4 and all("REPL" not in c["claim_id"] for c in cs)


def test_06_classification_mine_then_verify():
    mine = dict(n_days=200, n_obs=1000, median_edge=1.0, day_win=60.0, positive_years=3, years_counted=4, worst_year=-0.5, best_year=2.0)
    ver_ok = dict(median_edge=0.5, day_win=55.0, worst_year=-1.0, best_year=1.0)
    ver_bad = dict(median_edge=-0.5, day_win=45.0, worst_year=-3.0, best_year=0.5)
    assert F.classify(mine, ver_ok, 0.99, 0.0) == "BUILD_CANDIDATE"
    assert F.classify(mine, ver_bad, 0.99, 0.0) == "MINE_ONLY_NOT_REPLICATED"
    assert F.classify(mine, ver_ok, 0.50, 0.0) == "NULL"                     # DSR gate
    assert F.classify(dict(mine, n_days=10), ver_ok, 0.99, 0.0) == "DESCRIPTIVE_ONLY"
    neg = dict(mine, median_edge=-1.0, day_win=40.0, positive_years=1, worst_year=-3.0, best_year=0.5)
    assert F.classify(neg, dict(median_edge=-0.4, day_win=44.0, worst_year=-2.0, best_year=0.3), 0.0, 0.99) == "VETO_CANDIDATE"


def test_07_seal_refuses_outcome_files(tmp_path, monkeypatch):
    fam = tmp_path / "fam"; (fam / "runs").mkdir(parents=True)
    (fam / "outcome_x.json").write_text("{}")
    monkeypatch.setattr(F, "FAMILY_DIR", str(fam))
    with pytest.raises(HardStop, match="outcome-bearing"):
        F.seal()
