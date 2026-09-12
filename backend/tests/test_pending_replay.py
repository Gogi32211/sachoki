"""brain/pending.py must process a pullback order AT MOST ONCE per fire event.

THE DEFECT these pin (audited 2026-09-12). `check_fills()` re-reads the daily bars from each
order's `fire_date` on every run, and its only duplicate guard is

    held = {p["ticker"] for p in journal.open_positions()}

which covers a CURRENTLY OPEN position and nothing else. Once the trade is closed the ticker
leaves `doc["positions"]`, so an old `pending.json` — restored from a backup, synced from another
machine, rolled back by hand — re-opens the same historical fire at the historical close.

WHY AGE ALONE IS NOT THE FIX. Rejecting orders whose `fire_date` is outside the 5-bar window
looks sufficient until you notice a restore can land INSIDE that window, where the age check
passes and the duplicate still happens. Event identity is the contract; age is at best a cheap
second filter.

IDENTITY: (ticker, edge, fire_date). `fire_date` is a plain YYYY-MM-DD (live.py builds it as
str(d)[:10]), and `edge` is structurally guaranteed — spine.decide has exactly one BUY return and
it always sets `edge: best["id"]` from the registry, where 0 of 61 edge findings lack an id. An
edge-less order therefore means a broken caller, so place() refuses it rather than inventing a
sentinel that would make two edge-less fires on one day collide.

HERMETIC. The four JSON paths are redirected to a tmp dir and duckdb.connect is replaced with a
fake that serves synthetic bars, so nothing here touches the real book, the real request inbox or
the research database.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from brain import journal, learn, pending, requests as brain_requests   # noqa: E402

FIRE = "2026-09-01"
BELOW = 10.0


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    """Point every file the flow writes at tmp_path. journal.open_position also reaches into
    requests and learn, so those get redirected too or the test would edit the real inbox."""
    monkeypatch.setattr(journal, "_PATH", str(tmp_path / "book.json"))
    monkeypatch.setattr(pending, "_PATH", str(tmp_path / "pending.json"))
    monkeypatch.setattr(brain_requests, "_PATH", str(tmp_path / "requests.json"))
    monkeypatch.setattr(learn, "_LOG", str(tmp_path / "learning.json"))
    return tmp_path


def _decision(ticker="AAA", edge="g3abs"):
    return {"ticker": ticker, "edge": edge, "edge_title": "⚡G3-Abs", "tier": "core",
            "shares": 10, "entry": 10.5, "stop": 9.0, "target": 12.5, "sector": "Tech",
            "risk_dollars": 100.0, "log": ["synthetic"]}


def _bars(dip_on_day=1, n=5, start="2026-09-02"):
    """Rows shaped like check_fills' query: (date, open, high, low, close).
    The bar at index `dip_on_day` undercuts BELOW and closes green — a dip-and-reclaim."""
    import datetime as dt
    d0 = dt.date.fromisoformat(start)
    out = []
    for i in range(n):
        d = (d0 + dt.timedelta(days=i)).isoformat()
        if i == dip_on_day:
            out.append((d, 10.2, 11.0, BELOW - 0.5, 10.8))      # low < below, close > open
        else:
            out.append((d, 10.6, 10.9, 10.4, 10.5))             # no undercut
    return out


@pytest.fixture
def fake_bars(monkeypatch):
    """Replace duckdb.connect for the duration of one call. check_fills imports duckdb inside the
    function, so patching the module attribute is enough and no database is opened."""
    import duckdb

    store: dict = {}

    class _Con:
        def execute(self, _sql, params):
            self._rows = store.get(params[0], [])
            return self

        def fetchall(self):
            return self._rows

        def close(self):
            pass

    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: _Con())
    return store


# ── 1 · a fill records the event identity, and it survives the close ────────
def test_fill_records_the_fire_event_on_the_position_and_keeps_it_when_closed(isolated, fake_bars):
    fake_bars["AAA"] = _bars()
    pending.place(_decision(), fire_date=FIRE, below=BELOW)
    res = pending.check_fills(apply=True)

    assert len(res["filled"]) == 1, res
    pos = journal.open_positions()
    assert len(pos) == 1
    assert pos[0]["fire_date"] == FIRE and pos[0]["below"] == BELOW, pos[0]
    assert pending.list_pending() == []           # the order left the queue

    journal.close_position("AAA", exit_price=11.0, reason="test")
    closed = journal.closed_trades()
    assert len(closed) == 1
    assert closed[0]["fire_date"] == FIRE, (
        "close_position must carry the fire event into `closed` — it is the only evidence that "
        "this event was already processed")


# ── 2 · THE REGRESSION. A restored order must not re-open a closed trade ────
def test_stale_pending_does_not_reopen_a_closed_position(isolated, fake_bars):
    fake_bars["AAA"] = _bars()
    order = _decision()
    pending.place(order, fire_date=FIRE, below=BELOW)
    assert pending.check_fills(apply=True)["filled"], "setup: the first fill must happen"
    journal.close_position("AAA", exit_price=11.0, reason="test")
    assert journal.open_positions() == [] and len(journal.closed_trades()) == 1

    # the restore: the same order is back in the queue, the bars have not changed
    pending.place(order, fire_date=FIRE, below=BELOW)
    res = pending.check_fills(apply=True)

    assert res["filled"] == [], (
        f"STALE REPLAY: the order re-filled a fire event already traded — {res['filled']}")
    assert journal.open_positions() == [], "a duplicate position was opened from a stale order"
    assert len(journal.closed_trades()) == 1, "the closed history changed"
    assert any(e.get("why", "").startswith("already processed") for e in res["expired"]), res


# ── 3 · and it must NOT suppress a genuine later fire of the same edge ──────
def test_a_later_fire_of_the_same_edge_still_fills(isolated, fake_bars):
    fake_bars["AAA"] = _bars()
    pending.place(_decision(), fire_date=FIRE, below=BELOW)
    assert pending.check_fills(apply=True)["filled"]
    journal.close_position("AAA", exit_price=11.0, reason="test")

    later = "2026-10-01"
    fake_bars["AAA"] = _bars(start="2026-10-02")
    pending.place(_decision(), fire_date=later, below=BELOW)
    res = pending.check_fills(apply=True)

    assert len(res["filled"]) == 1, (
        f"FALSE SUPPRESSION: a different fire_date is a different event and must be allowed — {res}")
    assert journal.open_positions()[0]["fire_date"] == later


# ── 4 · an edge-less order is a caller bug, not a case to paper over ────────
def test_place_refuses_an_order_without_an_edge(isolated):
    """spine.decide's only BUY path always sets `edge`, so a missing one means the order did not
    come from the decider. Accepting it would put two edge-less fires on one ticker and one day
    under the same identity — fail loudly instead."""
    bad = _decision()
    bad.pop("edge")
    with pytest.raises(ValueError, match="edge"):
        pending.place(bad, fire_date=FIRE, below=BELOW)
    assert pending.list_pending() == []

    with pytest.raises(ValueError, match="fire_date"):
        pending.place(_decision(), fire_date="", below=BELOW)
