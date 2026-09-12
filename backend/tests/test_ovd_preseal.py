"""Pre-seal gate fixtures (spec A9–A11): observation-window authority + the report-only diagnostic.

  1  distribution AFTER the actual simulated exit -> FALSE (even inside maxh)   [user's worked example]
  2  ex-date ON the exit session -> TRUE (the ex-open gap is inside the simulated path)
  3  ex-date ON the entry session -> FALSE (entry at the open already trades ex)
  4  no exit (outcome UNAVAILABLE) -> NULL, never False
  5  no X producer imports the diagnostic
  6  the diagnostic is not a feature / cell; k = 293 is unchanged
  7  un-retracted '72 months' wording is refused; a retracting line passes
  8  observation-window mismatch (sessions / range) is refused
  9  the materiality table reproduces the persisted census (>=5% events / tickers in window)
"""
import os, sys, json, datetime as dt
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import ovd_diagnostics as D                                             # noqa: E402
import ovd_seal as S                                                    # noqa: E402
import ovd_registry as R                                                # noqa: E402

d = dt.date
F = D.post_entry_distribution_exposure_ge5


def test_01_distribution_after_actual_exit_is_false():
    # entry D+1 open (03-01), exits after 8 bars (03-13), distribution at bar 20 (03-29) -> FALSE
    assert F(d(2024, 3, 1), d(2024, 3, 13), [d(2024, 3, 29)]) is False


def test_02_ex_date_on_exit_session_is_true():
    assert F(d(2024, 3, 1), d(2024, 3, 13), [d(2024, 3, 13)]) is True
    assert F(d(2024, 3, 1), d(2024, 3, 13), [d(2024, 3, 5)]) is True


def test_03_ex_date_on_entry_session_is_false():
    assert F(d(2024, 3, 1), d(2024, 3, 13), [d(2024, 3, 1)]) is False
    assert F(d(2024, 3, 1), d(2024, 3, 13), [d(2024, 2, 28)]) is False


def test_04_no_exit_is_null_never_false():
    assert F(d(2024, 3, 1), None, [d(2024, 3, 5)]) is None
    with pytest.raises(ValueError):
        F(None, d(2024, 3, 13), [])


def test_05_not_used_by_x_builder():
    D.assert_not_used_by_x_builder()


def test_06_diagnostic_not_in_claims_k_unchanged():
    cells = R.enumerate_cells()
    assert D.NAME not in R.FEATURES
    assert not any("DISTRIBUTION" in c["feature"] for c in cells)
    assert D.NAME in R.REPORT_ONLY_DIAGNOSTICS and "must_not" in R.REPORT_ONLY_DIAGNOSTICS[D.NAME]
    assert len(cells) == 293 and sum(c["kind"] == "single" for c in cells) == 115


def test_07_window_wording_guard():
    S.assert_window_wording({"ok": "The old '72 months' wording is RETRACTED.", "ok2": "build(months=72) is a lookback"})
    with pytest.raises(SystemExit):
        S.assert_window_wording({"bad": "rows (eligible 1D ticker-sessions, 72 months)"})
    with pytest.raises(SystemExit):
        S.assert_window_wording({"bad": "a 72-month robustness rule"})


def test_08_window_mismatch_refused():
    owa = dict(canonical_1d_available_range=["2021-09-07", "2026-09-03"], eligible_sessions=1254)
    ok = dict(first_session="2021-09-07", last_session="2026-09-03", eligible_sessions=1254)
    S.assert_observation_window(ok, ["2021-09-07", "2026-09-03"], owa)
    with pytest.raises(SystemExit):
        S.assert_observation_window({**ok, "eligible_sessions": 1253}, ["2021-09-07", "2026-09-03"], owa)
    with pytest.raises(SystemExit):
        S.assert_observation_window({**ok, "first_session": "2021-05-26"}, ["2021-05-26", "2026-09-03"], owa)
    with pytest.raises(SystemExit):
        S.assert_observation_window(ok, ["2021-09-08", "2026-09-03"], owa)


def test_09_ge5_table_matches_frozen_a10_census_and_reconciles_to_per_event():
    cur = json.load(open(os.path.join(S.CANON_DIR, "CURRENT.json")))
    cen = json.load(open(os.path.join(S.CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS.json")))
    a10 = json.load(open(os.path.join(S.CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")))
    df = D.ge5_ex_date_table(cen["dividends_parquet"], cur["canonical_parquet"], *cur["session_range"])
    # frozen summed-per-ex-date set (the diagnostic's authority) — bound pre-seal
    assert [len(df), int(df.ticker.nunique())] == list(a10["events_tickers"]) == [282, 147]
    assert a10["canonical_run"] == cur["run_id"]
    # reconciliation: the per-vendor-row census (261/139) is contained in the summed set; the gap is
    # exactly the ex-dates that reach 5% only when same-day distributions are summed
    rec = D.per_event_ge5_count(cen["dividends_parquet"], cur["canonical_parquet"], *cur["session_range"])
    assert rec["per_event_events_tickers"] == list(cen["yield_ge_5pct"]) == [261, 139]
    assert rec["per_event_pairs_missing_from_summed_table"] == 0
    assert int((df.max_single_yld < D.THRESHOLD).sum()) == len(df) - 261 == a10["only_by_summing"] == 21
    assert (df.n_distributions[df.max_single_yld < D.THRESHOLD] > 1).all()


def test_10_duplicate_reference_rows_never_double_count():
    # two identical vendor rows for one 3% dividend must NOT sum to 6%
    import duckdb, tempfile, pandas as pd
    tmp = tempfile.mkdtemp()
    div = os.path.join(tmp, "div.parquet"); can = os.path.join(tmp, "canon.parquet")
    pd.DataFrame([dict(ticker="T", ex_date="2024-03-05", pay_date="2024-03-20", cash=0.3, dtype="CD", freq=4, currency="USD")] * 2
                 + [dict(ticker="T", ex_date="2024-06-05", pay_date="2024-06-20", cash=0.3, dtype="CD", freq=4, currency="USD"),
                    dict(ticker="T", ex_date="2024-06-05", pay_date="2024-06-20", cash=0.3, dtype="SC", freq=0, currency="USD")]).to_parquet(div)
    pd.DataFrame([dict(ticker="T", session_date=dt.date(2024, 3, 4), close=10.0), dict(ticker="T", session_date=dt.date(2024, 6, 4), close=10.0)]).to_parquet(can)
    df = D.ge5_ex_date_table(div, can)
    assert list(df.ex_date.astype(str)) == ["2024-06-05"]          # duplicate 3% -> 3% (out); regular 3% + special 3% -> 6% (in)
    assert df.n_distributions.iloc[0] == 2 and abs(df.yld.iloc[0] - 0.06) < 1e-9
