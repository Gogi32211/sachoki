"""Six regression tests, one per failure mode we have actually seen. No invented seventh.

Every one of these is here because it broke something real on 2026-08-09/10, and every one was
found by disbelieving an output rather than by a test — "zero trades is absurd", "random
selection cannot lose 3.4% at full exposure", "I never computed that interval". This file is
that disbelief written down so it runs by itself.

They are grouped by which class of guarantee failed, because knowing that tells you where to
look:

    DATA CONTRACT        the frame describes a different world
    COMPUTATION          the algorithm computes something other than what it claims
    REPORT               a number reached the reader that no estimator produced

The list is deliberately short. Invariants written for imagined failures are guesses; these
six are certainties, and one of them (the join dtype) already recurred after I had seen it
once — which is the definition of a check that should not depend on my memory.

    python test_invariants.py
"""
from __future__ import annotations

import os
import sys
import traceback

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_contract import ContractError, assert_contract          # noqa: E402

RESULTS: list[tuple[str, str, bool, str]] = []


def check(cls: str, name: str):
    def deco(fn):
        try:
            fn()
            RESULTS.append((cls, name, True, ""))
        except AssertionError as e:
            RESULTS.append((cls, name, False, str(e)[:180]))
        except Exception as e:                       # a crashing test is a failing test
            RESULTS.append((cls, name, False, f"{type(e).__name__}: {str(e)[:150]}"))
        return fn
    return deco


def _frame(n_tickers=3, n_days=40, start="2024-01-01") -> pd.DataFrame:
    """A small, entirely synthetic panel. No database, no network, runs in milliseconds."""
    days = pd.bdate_range(start, periods=n_days)
    rows = []
    for t in range(n_tickers):
        px = 100.0
        for d in days:
            px *= 1.001
            rows.append(dict(ticker=f"T{t}", date=d, open=px, high=px * 1.01,
                             low=px * 0.99, close=px, volume=1e6))
    df = pd.DataFrame(rows)
    df["_dt"] = pd.to_datetime(df["date"])
    df["prev_ok"] = df.groupby("ticker")["_dt"].diff().dt.days.le(4).fillna(False)
    return df


# ── DATA CONTRACT ────────────────────────────────────────────────────────────
@check("DATA", "1 · duplicate grain is fatal")
def _t1():
    """39.6% of the bars table was duplicated by index membership. Sorted by (ticker,date)
    the copies sit adjacent, so shift(-1) returned the SAME bar and manufactured a 73%/26%
    asymmetry out of arithmetic."""
    df = _frame()
    assert_contract(df, name="clean", verbose=False)
    dup = pd.concat([df, df.iloc[:10]], ignore_index=True).sort_values(["ticker", "_dt"])
    try:
        assert_contract(dup, name="duplicated", verbose=False)
        raise AssertionError("duplicated grain passed the contract")
    except ContractError as e:
        assert "duplicate" in str(e).lower(), f"wrong reason: {e}"


@check("DATA", "2 · row adjacency is not calendar adjacency")
def _t2():
    """A liquidity floor deletes bars from the MIDDLE of a history, so the previous ROW can
    be months from the previous BAR. JLHL was labelled T2→T2→T6 when its chart reads
    Z10→T5→T6 — the two intervening bars were under $3M and had been dropped."""
    df = _frame(n_tickers=1, n_days=40)
    holed = df[~df.index.isin(range(10, 25))].reset_index(drop=True)
    holed["prev_ok"] = holed.groupby("ticker")["_dt"].diff().dt.days.le(4).fillna(False)
    assert holed["prev_ok"].sum() < len(holed) - 1, "the hole left prev_ok untouched"
    gap_row = holed.index[~holed["prev_ok"]][-1]
    assert (holed["_dt"].iloc[gap_row] - holed["_dt"].iloc[gap_row - 1]).days > 4
    try:
        assert_contract(holed, name="holed", require_adjacency=0.99, verbose=False)
        raise AssertionError("a frame with a three-week hole passed require_adjacency=0.99")
    except ContractError as e:
        assert "trading day" in str(e).lower() or "adjacen" in str(e).lower()


@check("DATA", "3 · time joins must canonicalise dtype before merging")
def _t3():
    """Twice in two days. DuckDB hands back datetime64[us], the json path builds [ns], and
    merge_asof either refuses or — worse, in the Calmar experiment — a dict keyed by
    Timestamp silently missed every datetime64 lookup and all four arms traded nothing."""
    left = _frame(n_tickers=1, n_days=10)
    ev = pd.DataFrame({"ticker": ["T0"], "_pub": pd.to_datetime(["2024-01-03"])})
    ev["_pub"] = ev["_pub"].astype("datetime64[us]")
    try:
        pd.merge_asof(left.sort_values("_dt"), ev.sort_values("_pub"),
                      left_on="_dt", right_on="_pub", by="ticker", direction="backward")
        raise AssertionError("merge_asof accepted mismatched datetime resolutions")
    except Exception as e:
        assert "dtype" in str(e).lower() or "incompatible" in str(e).lower()
    ev["_pub"] = ev["_pub"].astype("datetime64[ns]")
    out = pd.merge_asof(left.sort_values("_dt"), ev.sort_values("_pub"),
                        left_on="_dt", right_on="_pub", by="ticker", direction="backward")
    assert out["_pub"].notna().sum() > 0, "canonicalised join matched nothing"

    # the silent variant: a dict keyed one way, looked up the other. The unsafe pattern must
    # be shown to fail, and the provided helper must be shown to fix it — a regression test
    # asserts the remedy, not the hazard.
    from data_contract import assert_time_aligned, keys_of      # noqa: PLC0415
    by_day = {d: 1 for d in left["_dt"]}                        # Timestamp keys
    unsafe = np.sort(left["_dt"].unique())                      # datetime64 values
    assert sum(1 for d in unsafe if d in by_day) == 0, (
        "the unsafe pattern no longer reproduces — this test has stopped testing anything")
    safe = keys_of(by_day)
    assert sum(1 for d in safe if d in by_day) == len(safe), "keys_of() did not align"
    try:
        assert_time_aligned(by_day, unsafe, name="probe")
        raise AssertionError("assert_time_aligned accepted keys that resolve to nothing")
    except ContractError:
        pass
    assert_time_aligned(by_day, safe, name="probe")            # the safe list must pass


# ── COMPUTATION ──────────────────────────────────────────────────────────────
@check("COMPUTATION", "4 · rotating a position at the same price cannot move equity")
def _t4():
    """The Calmar experiment differenced a curve mixing realised and unrealised P&L, so a
    position closing at +20% replaced by a fresh one at 0 was booked as a 2pp down day on ten
    slots. It turned a +9.02% CAGR into −3.36% and looked entirely plausible."""
    slots = 10

    def equity(marks, realised):
        return 1.0 + realised + sum(marks) / slots

    marks = [0.20] + [0.0] * 9                                # one winner, nine flat
    before = equity(marks, 0.0)
    realised = marks[0] / slots                               # book it
    after = equity([0.0] * 10, realised)                      # rotated into a fresh position
    assert abs(after - before) < 1e-12, (
        f"rotation moved equity by {after - before:+.4f} — realised P&L is being dropped")


@check("COMPUTATION", "5 · a fixture with known events cannot yield zero")
def _t5():
    """All four arms of the Calmar experiment reported 220 trades and zero equity movement
    because a dict lookup missed every day. Nothing raised; the output was simply empty, and
    an empty result reads exactly like 'no effect'."""
    df = _frame(n_tickers=2, n_days=20)
    df["tok"] = ""
    df.loc[df.index[[5, 6, 7]], "tok"] = ["T5", "T2G", "T6"]
    by_day = {d: g.index.to_numpy() for d, g in df.groupby("_dt", sort=True)}
    days = np.array(sorted(by_day.keys()))                    # FROM the dict, not .unique()
    seen = sum(len(by_day.get(d, [])) for d in days)
    assert seen == len(df), f"scanner saw {seen} of {len(df)} rows"
    assert (df["tok"] == "T6").sum() == 1, "planted event vanished"


# ── REPORT ───────────────────────────────────────────────────────────────────
@check("REPORT", "6 · a statistic without provenance cannot be reported")
def _t6():
    """The risk verdict first failed R6 against an interval I had typed by hand — ±0.3,
    invented at the keyboard. The report layer may display an estimate object; it may not
    create one."""
    from studio_gates import Integrity, risk_verdict            # noqa: PLC0415

    class Estimate(dict):
        REQUIRED = ("estimate", "ci_low", "ci_high", "method", "cluster_unit",
                    "n_raw", "n_eff")

        def __init__(self, **kw):
            missing = [k for k in self.REQUIRED if k not in kw]
            if missing:
                raise ContractError(f"estimate lacks provenance: {missing} — the report "
                                    f"layer must print NOT COMPUTED rather than a number")
            super().__init__(**kw)

    try:
        Estimate(estimate=0.0, ci_low=-0.3, ci_high=0.3)
        raise AssertionError("an estimate without method/cluster_unit/n was accepted")
    except ContractError:
        pass
    e = Estimate(estimate=0.0, ci_low=-0.21, ci_high=0.23, method="clustered-bootstrap",
                 cluster_unit="date", n_raw=17798, n_eff=5210)
    assert e["method"] and e["cluster_unit"], "provenance fields empty"

    # and the verdict itself must refuse without the human check
    v = risk_verdict(label="probe", stats=dict(
        n_exposed=1, n_control=1, rate_exposed=1.0, rate_control=1.0, rr=1.0, rr_lo=0.9,
        rr_hi=1.1, arr=0.0, arr_lo=-0.1, arr_hi=0.1, n_events=1, n_event_dates=1, nnt=1),
        integrity=Integrity(), per_period=[0.0], return_effect=0.0, return_ci=(-0.1, 0.1),
        human_checked=False)
    assert v == "INVALID", f"verdict returned {v} without verify_sample"


if __name__ == "__main__":
    print("=" * 96)
    print("  INVARIANTS — one per failure mode actually observed, none invented")
    print("=" * 96)
    cls = None
    for c, name, ok, why in RESULTS:
        if c != cls:
            print(f"\n  {c}")
            cls = c
        print(f"    {'✅' if ok else '🔴'} {name}")
        if not ok:
            print(f"         {why}")
    n_ok = sum(1 for *_, ok, _ in [(a, b, c, d) for a, b, c, d in RESULTS] if ok)
    print("\n" + "=" * 96)
    print(f"  {n_ok}/{len(RESULTS)} PASS", flush=True)
    print("=" * 96)
    sys.exit(0 if n_ok == len(RESULTS) else 1)
