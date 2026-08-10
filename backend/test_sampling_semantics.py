"""Regression tests for the 2026-08-10 failure: two correct numbers, different experiments.

Every case below is something that actually happened or that the module exists to stop. No
speculative coverage — the defect had a specific shape and these pin that shape.
"""
from __future__ import annotations

import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sampling_target import (SamplingSemanticsError, calibration_metric,  # noqa: E402
                             compatible, descriptive_metric,
                             empirical_cluster_resampling, finite_population_subsample,
                             frozen_forward, fpc_ratio, synthetic_dgp)

HIST = "realized_history_2021_2026"
ok_n = fail_n = 0


def check(name, fn):
    global ok_n, fail_n
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok_n += 1
    except Exception as e:                                        # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail_n += 1


def raises(fn, needle=""):
    try:
        fn()
    except SamplingSemanticsError as e:
        assert needle.lower() in str(e).lower(), f"wrong reason: {e}"
        return
    raise AssertionError("expected SamplingSemanticsError, none raised")


# 1 — the exact comparison that was made and must never be made silently again
def t1():
    raises(lambda: calibration_metric(
        "coverage", 0.997,
        interval=empirical_cluster_resampling("trading_date"),
        replication=finite_population_subsample(0.70, HIST, "trading_date"),
        n_replications=120), "finite-population correction")


# 2 — the refusal must carry the expected size of the discrepancy, not just say "no"
def t2():
    ok, why = compatible(empirical_cluster_resampling(),
                         finite_population_subsample(0.70, HIST))
    assert not ok
    assert "1.83" in why, f"the 1/√(1−f) magnitude is missing from the reason: {why}"


# 3 — the FPC prediction the four-point curve confirmed to 2%
def t3():
    for f, want in ((0.30, 1.195), (0.50, 1.414), (0.70, 1.826), (0.90, 3.162)):
        assert abs(fpc_ratio(f) - want) < 0.002, f"f={f}: {fpc_ratio(f)} vs {want}"


# 4 — the shared-history harness may report its rate, but not under the name `power`
def t4():
    m = descriptive_metric("conditional_detection_rate", 0.958,
                           target=finite_population_subsample(0.70, HIST), n_replications=120)
    assert m.calibration_claim is False
    assert HIST in str(m), "a conditional rate must print its condition"
    raises(lambda: descriptive_metric("power", 0.958,
                                      target=finite_population_subsample(0.70, HIST),
                                      n_replications=120), "conditional_detection_rate")


# 5 — a conditional target cannot be constructed without naming its condition
def t5():
    raises(lambda: finite_population_subsample(0.70, ""), "conditioned on")


# 6 — synthetic-DGP calibration is allowed against ITSELF and nothing else
def t6():
    g = synthetic_dgp("block_bootstrap_v1")
    m = calibration_metric("coverage", 0.951, interval=g, replication=g, n_replications=2000)
    assert m.calibration_claim is True
    raises(lambda: calibration_metric("coverage", 0.951, interval=g,
                                      replication=synthetic_dgp("garch_v2"),
                                      n_replications=2000), "different synthetic")


# 7 — the forward window is not something to calibrate against resampled history
def t7():
    raises(lambda: calibration_metric("coverage", 0.9,
                                      interval=frozen_forward("2026-08-08"),
                                      replication=empirical_cluster_resampling(),
                                      n_replications=50), "frozen-forward")


# 8 — mismatched exchangeable units are refused even when the kind matches
def t8():
    raises(lambda: calibration_metric(
        "coverage", 0.95, interval=empirical_cluster_resampling("trading_date"),
        replication=empirical_cluster_resampling("ticker"), n_replications=100), "unit differs")


# 9 — the legitimate case still works, so the contract is not merely a blocker
def t9():
    e = empirical_cluster_resampling("trading_date")
    m = calibration_metric("coverage", 0.948, interval=e, replication=e, n_replications=500)
    assert 0.94 < m.value < 0.96 and m.calibration_claim


print("=" * 96, flush=True)
print("  SAMPLING SEMANTICS — regression on the 2026-08-10 calibration defect", flush=True)
print("=" * 96, flush=True)
for i, fn in enumerate([t1, t2, t3, t4, t5, t6, t7, t8, t9], 1):
    check(f"{i} · {fn.__doc__ or fn.__name__}", fn)
print("=" * 96, flush=True)
print(f"  {ok_n} passed · {fail_n} failed", flush=True)
sys.exit(1 if fail_n else 0)
