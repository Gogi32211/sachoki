"""The nightly intraday overlap must delete exactly what it puts back.

THE DEFECT (found 2026-09-13). update_intraday_db re-writes the last OVERLAP days of every ticker:
it DELETEs them and INSERTs the freshly fetched rows. The two sides used different comparisons —

    fresh  = en[en["_dt_key"] > cutoff]                  # _dt_key is NORMALISED to midnight
    DELETE ... WHERE date > 'YYYY-MM-DD'                 # date is the raw TIMESTAMP

so a bar at 2026-08-24 13:30 was deleted (13:30 > 00:00) and never restored (its normalised key is
not > 2026-08-24). **The cutoff day was destroyed on every single run.** Nothing failed: the script
printed "✅ DONE: +87,000 new rows" and exited 0, so update_all.sh's `|| echo ... failed` never
fired and no log line ever recorded it.

THE OBSERVABLE SIGNATURE. With OVERLAP = 3 and the launchd Tue-Sat schedule the cutoff lands on a
different weekday each run:

    Tue run  old_max=Fri  -> cutoff Tue   DESTROYED
    Wed run  old_max=Mon  -> cutoff Fri   DESTROYED
    Thu run  old_max=Tue  -> cutoff Sat   not a trading day
    Fri run  old_max=Wed  -> cutoff Sun   not a trading day
    Sat run  old_max=Thu  -> cutoff Mon   DESTROYED

Measured in the stores over 2026-06-25..2026-09-02: Mon 10/10 damaged, Tue 9/10, Fri 8/9, and
Wed 0/10, Thu 0/10. The weekday pattern predicted by the code semantics matched the data exactly,
which is what identified the bug — no vendor or network hypothesis was needed.

THE CONTRACT. The overlap is a CALENDAR-DAY window, not a timestamp window, so both sides are
date-level INCLUSIVE. Hermetic: an in-memory DuckDB, no network, no vendor, no real store.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_intraday_db import DELETE_PREDICATE, OVERLAP, overlap_cutoff   # noqa: E402

TK, UNI = "TEST", "sp500"
SESSION_HOURS = (13, 17)                     # the 4H grid the alignment audit established


def _bars(days: list[str]) -> pd.DataFrame:
    """Two intraday bars per session, stamped mid-session like the real stores."""
    rows = [{"ticker": TK, "universe": UNI,
             "date": pd.Timestamp(f"{d} {h:02d}:30:00"), "close": 100.0 + i}
            for i, d in enumerate(days) for h in SESSION_HOURS]
    return pd.DataFrame(rows)


def _con(days: list[str]):
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE bars (ticker VARCHAR, universe VARCHAR, date TIMESTAMP, close DOUBLE)")
    c.register("seed", _bars(days))
    c.execute("INSERT INTO bars SELECT * FROM seed")
    return c


def _run_update(c, fetched_days: list[str], old_max: str) -> tuple[int, int]:
    """One ticker's slice of the nightly re-write. Returns (deleted, re-inserted)."""
    en = _bars(fetched_days)
    en["_dt_key"] = pd.to_datetime(en["date"]).dt.normalize()
    cutoff = overlap_cutoff(old_max, OVERLAP)
    fresh = en[en["_dt_key"] >= cutoff].drop(columns=["_dt_key"])
    before = c.execute("SELECT count(*) FROM bars").fetchone()[0]
    c.execute(f"DELETE FROM bars WHERE ticker=? AND universe=? AND {DELETE_PREDICATE}",
              [TK, UNI, cutoff.strftime("%Y-%m-%d")])
    deleted = before - c.execute("SELECT count(*) FROM bars").fetchone()[0]
    c.register("ins", fresh)
    c.execute("INSERT INTO bars SELECT ticker, universe, date, close FROM ins")
    c.unregister("ins")
    return deleted, len(fresh)


def _sessions(c) -> list[str]:
    return [str(r[0]) for r in c.execute(
        "SELECT DISTINCT CAST(date AS DATE) FROM bars ORDER BY 1").fetchall()]


# ── the four cases ───────────────────────────────────────────────────────────
WEEK = ["2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21"]   # Mon..Fri


def test_the_cutoff_day_is_deleted_and_comes_back():
    """THE REGRESSION. old_max = Thu, OVERLAP = 3 -> cutoff = Mon. Under the old code Monday's bars
    were deleted and never re-inserted; Monday had to survive intact."""
    c = _con(WEEK)
    cutoff = overlap_cutoff("2026-08-20", OVERLAP)
    assert cutoff == pd.Timestamp("2026-08-17"), cutoff
    _run_update(c, WEEK, old_max="2026-08-20")
    assert "2026-08-17" in _sessions(c), (
        "the cutoff day was destroyed — the DELETE range and the INSERT range disagree again")
    assert c.execute("SELECT count(*) FROM bars WHERE CAST(date AS DATE) = DATE '2026-08-17'"
                     ).fetchone()[0] == len(SESSION_HOURS)


def test_bars_before_the_cutoff_are_untouched():
    c = _con(WEEK)
    before = c.execute("SELECT count(*) FROM bars WHERE CAST(date AS DATE) < DATE '2026-08-17'"
                       ).fetchone()[0]
    _run_update(c, WEEK, old_max="2026-08-20")
    assert c.execute("SELECT count(*) FROM bars WHERE CAST(date AS DATE) < DATE '2026-08-17'"
                     ).fetchone()[0] == before


def test_every_session_after_the_cutoff_is_restored():
    c = _con(WEEK)
    _run_update(c, WEEK, old_max="2026-08-20")
    assert _sessions(c) == WEEK, _sessions(c)


def test_the_update_is_idempotent():
    """Running the same night twice must not change the key set or the row count."""
    c = _con(WEEK)
    _run_update(c, WEEK, old_max="2026-08-20")
    first = (c.execute("SELECT count(*) FROM bars").fetchone()[0], _sessions(c))
    _run_update(c, WEEK, old_max="2026-08-20")
    assert (c.execute("SELECT count(*) FROM bars").fetchone()[0], _sessions(c)) == first


def test_delete_and_reinsert_cover_the_same_range():
    """The invariant itself: what DELETE removed is exactly what INSERT put back."""
    c = _con(WEEK)
    deleted, reinserted = _run_update(c, WEEK, old_max="2026-08-20")
    assert deleted == reinserted, f"deleted {deleted} rows, restored {reinserted}"


# ── the schedule regression, in the shape of the real failure ────────────────
@pytest.mark.parametrize("run_day,old_max,cutoff_day,weekday", [
    ("Tue", "2026-08-21", "2026-08-18", "Tue"),     # old_max = Fri -> cutoff Tue
    ("Wed", "2026-08-24", "2026-08-21", "Fri"),     # old_max = Mon -> cutoff Fri
    ("Sat", "2026-08-20", "2026-08-17", "Mon"),     # old_max = Thu -> cutoff Mon
])
def test_the_three_weekdays_the_bug_actually_destroyed(run_day, old_max, cutoff_day, weekday):
    """Mon, Tue and Fri are the sessions that landed on the cutoff under the Tue-Sat schedule, and
    they are the three that were destroyed in the real stores. Each must now survive its run."""
    assert overlap_cutoff(old_max, OVERLAP) == pd.Timestamp(cutoff_day)
    assert dt.date.fromisoformat(cutoff_day).strftime("%a") == weekday
    days = sorted({*WEEK, "2026-08-24", "2026-08-25"})
    c = _con(days)
    _run_update(c, days, old_max=old_max)
    assert cutoff_day in _sessions(c), (
        f"the {run_day} run destroyed its {weekday} cutoff session — the exact production failure")
