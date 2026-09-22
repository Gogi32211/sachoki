"""A --dual night that damages the store must not exit 0, and an unmeasured check must not read PASS.

The 2026 intraday outage lasted three months because the nightly job printed "✅ DONE: +87,000 new
rows" every time it destroyed a session. Counts were never the right question — the writer counted
the rows it inserted, not the rows it deleted, and the two were different.

NIGHTLY INTRADAY V2 therefore ships a VERDICT, not a row count. These tests pin it:

  · key_difference < 0 on either TF          -> NOT PASS   (deleted more than it restored)
  · any ticker lost rows                     -> NOT PASS   (a net sum hides a per-ticker loss)
  · any PARTIAL vendor response              -> NOT PASS   (a short frame looks complete)
  · RSI parity not measured, or off the gate -> NOT PASS   (an unmeasured check is not a passed one)

and the vendor telemetry those rules read, including the partial flag that the cursor walk raises
when it is rate-limited mid-page. Hermetic: no network, no vendor key, no store.
"""
from __future__ import annotations
import os, sys, types
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _acc(deleted=1000, reinserted=1000, lossy=None):
    return dict(deleted=deleted, reinserted=reinserted, ins=reinserted, lossy=list(lossy or []))


def _clean_acc():
    return {"1h": _acc(), "4h": _acc()}


def _clean_fetch(**kw):
    f = dict(requested=10, succeeded=10, failed=0, partial=0, vendor_errors=0,
             calls=10, rows=5000, bytes=1_000_000, retries=0, http_429=0,
             partial_tickers=[], failed_tickers=[])
    f.update(kw)
    return f


def _parity(p95=0.0, mx=0.1):
    return dict(n=200, tickers=25, median=0.0, p95=p95, mx=mx, within_quantum=1.0)


def _clean_parity():
    return {"1h": _parity(), "4h": _parity()}


@pytest.fixture(scope="module")
def uid():
    import update_intraday_db as m
    return m


# ── the verdict ───────────────────────────────────────────────────────────────────────────────
def test_clean_run_passes(uid):
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), _clean_parity())
    assert ok and bad == []


def test_negative_key_difference_fails(uid):
    acc = _clean_acc()
    acc["4h"] = _acc(deleted=1200, reinserted=1000)      # the outage's signature
    ok, bad = uid.dual_verdict(acc, _clean_fetch(), _clean_parity())
    assert not ok
    assert any("key_difference" in b and "4h" in b for b in bad)


def test_per_ticker_loss_fails_even_when_the_net_is_positive(uid):
    """+6,200 net on 1h hid APGE/HLX losing their 2026-08-31 bars. Judge each ticker on its own."""
    acc = _clean_acc()
    acc["1h"] = _acc(deleted=1000, reinserted=7200, lossy=[("APGE", 40, 2), ("HLX", 38, 1)])
    ok, bad = uid.dual_verdict(acc, _clean_fetch(), _clean_parity())
    assert not ok
    assert any("lost rows" in b for b in bad)


def test_partial_vendor_response_fails(uid):
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(partial=2, partial_tickers=["AAPL", "NVDA"]),
                               _clean_parity())
    assert not ok
    assert any("partial" in b.lower() for b in bad)


def test_unmeasured_parity_is_not_a_pass(uid):
    par = _clean_parity()
    par["4h"] = None
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), par)
    assert not ok
    assert any("NOT TESTED" in b for b in bad)


def test_parity_off_the_gate_fails(uid):
    """15-day 4H warm-up measured median 9.6 / max 27.8 — the defect this gate exists to catch."""
    par = _clean_parity()
    par["4h"] = _parity(p95=12.4, mx=18.6)
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), par)
    assert not ok
    assert any("parity" in b for b in bad)


def test_one_storage_unit_still_passes(uid):
    """rsi_14 is a DOUBLE rounded to 1 dp: 0.1 is the quantum, not an error.

    And it arrives with binary residue — abs(round(w,1) - stored) on a 4H bar one unit apart is
    0.10000000000000142, which a bare `> 0.1` rejects. The first live rehearsal came back NOT PASS
    on exactly that, with every measured value at the quantum and none above it.
    """
    par = {"1h": _parity(0.0, 0.0), "4h": _parity(0.1, 0.1)}
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), par)
    assert ok, bad
    residue = abs(round(46.7, 1) - 46.6)                     # 0.10000000000000142
    assert residue > 0.1
    par = {"1h": _parity(0.0, 0.0), "4h": _parity(residue, residue)}
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), par)
    assert ok, bad


def test_two_storage_units_still_fails(uid):
    """The tolerance must not swallow a genuine disagreement: the next step up is rejected."""
    par = {"1h": _parity(0.0, 0.0), "4h": _parity(0.2, 0.2)}
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), par)
    assert not ok and any("parity" in b for b in bad)


# ── the telemetry the verdict reads ───────────────────────────────────────────────────────────
class _Resp:
    def __init__(self, payload, status=200, body=b"x" * 1024):
        self._p, self.status_code, self.content = payload, status, body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def json(self):
        return self._p


def _dp(monkeypatch, responses):
    import data_polygon as dp
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr(dp.time, "sleep", lambda *_a, **_k: None)
    it = iter(responses)
    monkeypatch.setattr(dp.requests, "get", lambda *_a, **_k: next(it))
    return dp


def test_complete_fetch_reports_calls_pages_rows_bytes(monkeypatch):
    bar = {"o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "t": 1_700_000_000_000}
    dp = _dp(monkeypatch, [_Resp({"results": [bar, dict(bar, t=1_700_001_800_000)]}, body=b"y" * 2048)])
    dp.fetch_bars("AAPL", interval="30m", days=90)
    st = dp.LAST_FETCH
    assert st["calls"] == 1 and st["pages"] == 1 and st["rows"] == 2
    assert st["bytes"] == 2048 and st["partial"] is False and st["error"] is None


def test_rate_limited_mid_cursor_is_flagged_partial(monkeypatch):
    """Page 1 lands, page 2 is 429 on every attempt: the frame comes back SHORT but well-formed."""
    bar = {"o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "t": 1_700_000_000_000}
    first = _Resp({"results": [bar], "next_url": "https://api.massive.com/next"})
    dp = _dp(monkeypatch, [first] + [_Resp({}, status=429) for _ in range(6)])
    df = dp.fetch_bars("AAPL", interval="30m", days=90)
    assert len(df) == 1                       # looks like a perfectly ordinary frame …
    assert dp.LAST_FETCH["partial"] is True   # … and only the flag says it is not
    assert dp.LAST_FETCH["http_429"] == 6


def test_429_with_no_data_raises_and_records_the_error(monkeypatch):
    dp = _dp(monkeypatch, [_Resp({}, status=429) for _ in range(6)])
    with pytest.raises(RuntimeError):
        dp.fetch_bars("AAPL", interval="30m", days=90)
    assert "429" in (dp.LAST_FETCH["error"] or "")


# ── the 2026-09-22 lessons: what the writer's gate may and may not be blamed for ───────────────
def test_live_vendor_failure_fails_the_run(uid):
    """A ticker that is still trading and returns nothing is a real failure."""
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(failed=1, live_failed=1,
                                                          live_failed_tickers=["AAPL"]), _clean_parity())
    assert not ok
    assert any("KNOWN_INACTIVE" in b and "AAPL" in b for b in bad)


def test_known_inactive_failures_do_not_fail_the_run(uid):
    """ATVI, PXD, WRK, SQ… were acquired or renamed. 52 dead tickers are not 52 vendor failures,
    and failing every night for a reason nobody can act on teaches the log to be ignored."""
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(failed=52, known_inactive=52, live_failed=0),
                               _clean_parity())
    assert ok, bad


def test_store_history_parity_does_not_vote(uid):
    """The verdict reads ONLY the fresh-frame parity. The whole-store comparison asserts the stored
    close series is basis-consistent back to 2021 — a claim about corporate actions, not about
    tonight's write. Letting it vote made 2026-09-22 NOT PASS over a x10 split step in NXXT."""
    ok, bad = uid.dual_verdict(_clean_acc(), _clean_fetch(), _clean_parity())
    assert ok and bad == []      # dual_verdict takes no store-history argument at all
    import inspect
    assert "parity" in inspect.signature(uid.dual_verdict).parameters
    assert len(inspect.signature(uid.dual_verdict).parameters) == 3


def test_parity_sample_is_deterministic_and_not_python_hash(uid):
    """python hash() is salted per process, so the 'same tickers every night' promise would break
    across runs — the control-density defect all over again."""
    import hashlib
    a = [t for t in ("AAPL", "NVDA", "MSFT", "TSLA", "AMD", "INTC", "F", "T", "KO", "PEP")
         if uid.parity_sampled(t)]
    b = [t for t in ("AAPL", "NVDA", "MSFT", "TSLA", "AMD", "INTC", "F", "T", "KO", "PEP")
         if uid.parity_sampled(t)]
    assert a == b
    tk = "AAPL"
    expect = int(hashlib.sha256(tk.encode()).hexdigest()[:8], 16) % uid.PARITY_SAMPLE_MOD == 0
    assert uid.parity_sampled(tk) is expect


def _mem_db(rows):
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE bars (ticker VARCHAR, date TIMESTAMP, close DOUBLE, rsi_14 DOUBLE)")
    con.executemany("INSERT INTO bars VALUES (?, ?, ?, ?)", rows)
    return con


def test_basis_seam_ticker_is_not_measurable_not_a_failure(uid):
    """NXXT fell x10 on 2026-09-02 and came back x10 on 09-08. A Wilder recomputed across that step
    is meaningless, so the ticker is reported NOT MEASURABLE and counted — never silently dropped,
    and never charged to the writer."""
    import pandas as pd
    from mtf_rev_build import wilder_rsi14
    n = 300
    base = pd.Series([10 + (i % 7) * 0.5 for i in range(n)])
    seam = base.copy(); seam.iloc[200:] = seam.iloc[200:] / 10.0      # the split step
    days = pd.date_range("2025-01-01", periods=n, freq="D")
    rows = []
    for tk, ser in (("CLEAN", base), ("SEAM", seam)):
        r = wilder_rsi14(ser)
        rows += [(tk, d, float(c), float(v)) for d, c, v in zip(days, ser, r)]
    con = _mem_db(rows)
    out = uid.store_history_parity(con, ["CLEAN", "SEAM"], n_tickers=2)
    assert out["not_measurable"] == 1 and out["seam_tickers"] == ["SEAM"]
    assert out["tickers"] == 1 and out["mx"] == 0.0


def test_fresh_frame_parity_reads_the_row_back_out_of_the_database(uid):
    """The gate must prove the enriched value ARRIVED, not merely that it was computed."""
    import pandas as pd
    from mtf_rev_build import wilder_rsi14
    n = 150
    ser = pd.Series([20 + (i % 11) * 0.3 for i in range(n)])
    days = pd.date_range("2026-05-01", periods=n, freq="D")
    r = wilder_rsi14(ser)
    con = _mem_db([("XYZ", d, float(c), float(v)) for d, c, v in zip(days, ser, r)])
    en = pd.DataFrame(dict(date=days, close=ser.to_numpy(), rsi_14=r.to_numpy()))
    good = uid.fresh_frame_parity(con, {"XYZ": en})
    assert good["n"] == uid.PARITY_TAIL_BARS and good["mx"] == 0.0 and good["tickers"] == 1
    con.execute("UPDATE bars SET rsi_14 = rsi_14 + 9.6 WHERE date >= ?", [days[-3]])
    bad = uid.fresh_frame_parity(con, {"XYZ": en})
    assert bad["mx"] >= 9.5, bad          # the 15-day warm-up defect's own magnitude


def test_missing_inactive_list_is_fail_closed(uid, monkeypatch, tmp_path):
    """No frozen list -> no ticker is excused. An absent artifact must never widen what passes."""
    monkeypatch.setattr(uid, "INACTIVE_PATH", str(tmp_path / "nope.json"))
    s, man = uid.load_inactive()
    assert s == set() and man == {}
