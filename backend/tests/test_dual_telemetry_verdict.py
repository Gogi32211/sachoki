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
