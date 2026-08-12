"""The three fixes that are really statistical decisions, refused before real-y can provoke them.

Every test here is written against a constructed geometry rather than against the data, on
purpose: the point is that the rule holds regardless of what the real returns turn out to look
like. A gate built after seeing the failure mode is a gate shaped by it.

The one that matters most is `t5`. Full support has both arms — eligibility already required
that — and a date-clustered resample can still lose one. The synthetic worlds the instrument was
accepted on never produced it, because `Y = μ_setup + γ_date + ε` populates every stratum in
every resample. Real returns will, and at that moment `if arm missing: skip` is the most natural
line of code in the world and the most damaging.
"""
from __future__ import annotations

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import outcome_integrity as OI                                       # noqa: E402
import real_y_qualification_spec as SPEC                             # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def reps(n, *, bad=0, reason="ARM_LOST_IN_RESAMPLE", spread=True):
    out = []
    for i in range(n):
        if i < bad:
            out.append(OI.ReplicateOutcome(i, False, reason=reason))
        else:
            out.append(OI.ReplicateOutcome(i, True, value=(i % 7) - 3 if spread else 1.5))
    return out


# ── the criteria were registered before the run ─────────────────────────────
def t1_the_spec_is_frozen_and_says_what_it_is_not():
    assert SPEC.spec_hash(), "the spec must hash, or it is not frozen"
    assert SPEC.BOOTSTRAP_VALID_FLOOR == 1.0
    joined = " ".join(SPEC.NOT_CRITERIA).lower()
    for forbidden in ("build", "positive", "excludes zero", "interesting"):
        assert forbidden in joined, forbidden
    for k in ("adaptive_bootstrap_retries", "silent_row_drops",
              "bootstrap_accounting_reconciles", "day_level_untouched"):
        assert k in SPEC.CRITERIA, k


# ── prohibition one: no silent row drops ────────────────────────────────────
def t2_a_nonfinite_outcome_in_a_used_row_is_an_error_not_a_dropna():
    y = np.array([1.0, 2.0, np.nan, 4.0])
    OI.assert_outcomes_admissible(y, np.array([0, 1, 3]), where="cell-A")   # unused NaN is fine
    try:
        OI.assert_outcomes_admissible(y, np.array([0, 2]), where="cell-A")
    except OI.OutcomeIntegrityError as e:
        assert "change the population" in str(e)
        return
    raise AssertionError("a non-finite outcome inside a used stratum was tolerated")


# ── prohibition two: no adaptive retries ────────────────────────────────────
def t3_topping_up_until_enough_replicates_succeed_is_refused():
    """the shape of it: hand the assessor only the good ones"""
    good_only = reps(1997)
    try:
        OI.assess_cell("A", 1.0, good_only, requested=2000)
    except OI.BootstrapPolicyError as e:
        assert "requested once" in str(e)
    else:
        raise AssertionError("a short replicate set was accepted")

    topped_up = reps(2003)
    try:
        OI.assess_cell("A", 1.0, topped_up, requested=2000)
    except OI.BootstrapPolicyError as e:
        assert "more were drawn until enough succeeded" in str(e)
        return
    raise AssertionError("extra replicates were accepted")


def t4_the_accounting_reconciles_or_the_cell_fails():
    c = OI.assess_cell("A", 1.0, reps(2000, bad=3), requested=2000)
    assert c.reconciles(), c
    assert c.reps_valid == 1997 and c.reps_rejected == 3
    assert c.publishable is False, "1997 of 2000 is below the registered floor of 1.0"
    assert c.uncomputable_reason == "BELOW_VALID_FLOOR"
    assert c.rejection_reasons == {"ARM_LOST_IN_RESAMPLE": 3}


# ── prohibition three: no new degeneracy rule ───────────────────────────────
def t5_a_resample_that_loses_an_arm_is_a_labelled_rejection_not_a_skip():
    """the case real-y surfaces first, and the one `if arm missing: skip` would hide

    Full support has both arms — eligibility required it. A date-clustered resample can still
    draw only dates on which one arm has no rows. The synthetic worlds never produced this.
    """
    lost = OI.classify_replicate(7, 1.23, arms_present=False, strata_nonempty=True)
    assert lost.valid is False and lost.reason == "ARM_LOST_IN_RESAMPLE"
    kept = OI.classify_replicate(8, 1.23, arms_present=True, strata_nonempty=True)
    assert kept.valid is True and kept.value == 1.23


def t5b_the_geometry_that_produces_it():
    """constructed, so the rule is proven without waiting for the data to provide the case"""
    # three dates; the comparator arm exists only on date 2. A clustered resample that draws
    # dates {0, 1} has full support in the design and no comparator rows in the sample.
    dates = np.array([0, 0, 1, 1, 2, 2])
    treated = np.array([1, 1, 1, 1, 0, 0], dtype=bool)
    comparator = ~treated
    assert treated.any() and comparator.any(), "the design has both arms"

    rng = np.random.default_rng(11)
    lost = 0
    for i in range(200):
        drawn = rng.choice(np.unique(dates), size=3, replace=True)
        keep = np.isin(dates, drawn)
        arms = bool(treated[keep].any() and comparator[keep].any())
        if not arms:
            lost += 1
            r = OI.classify_replicate(i, np.nan, arms_present=False, strata_nonempty=True)
            assert r.reason == "ARM_LOST_IN_RESAMPLE"
    assert lost > 0, (
        "the fixture failed to produce a single arm-losing resample, so it proves nothing about "
        "the rule it exists to exercise")


def t6_an_unregistered_rejection_reason_cannot_be_used():
    """a new failure mode must be named, not absorbed as an unlabelled skip"""
    try:
        OI.ReplicateOutcome(1, False, reason="weird_edge_case")
    except OI.BootstrapPolicyError as e:
        assert "not a registered reason code" in str(e)
        return
    raise AssertionError("a replicate was rejected for an unregistered reason")


# ── degenerate is not invalid ───────────────────────────────────────────────
def t7_a_zero_width_interval_from_agreeing_resamples_is_an_answer():
    c = OI.assess_cell("A", 1.5, reps(2000, spread=False), requested=2000)
    assert c.publishable is True, "a correct extreme case was rejected for looking unusual"
    assert c.degeneracy == OI.DEGENERATE_BUT_VALID
    assert c.interval == (1.5, 1.5)


def t8_a_nonfinite_theta_is_invalid_however_clean_the_replicates_are():
    c = OI.assess_cell("A", float("nan"), reps(2000), requested=2000)
    assert c.publishable is False and c.degeneracy == OI.NUMERICALLY_INVALID
    assert c.uncomputable_reason == "NONFINITE_STATISTIC"


def t9_every_uncomputable_result_carries_a_frozen_reason():
    for c in (OI.assess_cell("A", float("inf"), reps(2000), requested=2000),
              OI.assess_cell("B", 1.0, reps(2000, bad=1), requested=2000)):
        assert not c.publishable
        assert c.uncomputable_reason in SPEC.REASON_CODES, c.uncomputable_reason


# ── the report ──────────────────────────────────────────────────────────────
def t10_the_report_passes_only_when_every_criterion_holds():
    cells = [OI.assess_cell(f"c{i}", 1.0, reps(2000), requested=2000) for i in range(31)]
    r = OI.build_report(rows_total=289467, rows_used=289467, nonfinite=0,
                        requested_cells=31, cells=cells)
    assert r.failures() == [], r.failures()
    assert r.cells_publishable == 31 and r.cells_uncomputable == 0

    short = OI.build_report(rows_total=289467, rows_used=289467, nonfinite=0,
                            requested_cells=31, cells=cells[:30])
    assert short.failures(), "30 of 31 cells addressed must fail the gate"

    dirty = OI.build_report(rows_total=289467, rows_used=289467, nonfinite=2,
                            requested_cells=31, cells=cells)
    assert any("non-finite" in f for f in dirty.failures()), dirty.failures()


def t11_the_gate_does_not_read_the_answer():
    """all 31 uninteresting, all publishable — and the gate passes, because that is not its job"""
    flat = [OI.assess_cell(f"c{i}", 0.0,
                           [OI.ReplicateOutcome(j, True, value=0.0) for j in range(2000)],
                           requested=2000) for i in range(31)]
    r = OI.build_report(rows_total=1, rows_used=1, nonfinite=0, requested_cells=31, cells=flat)
    assert r.failures() == [], (
        "the gate rejected a run in which every estimate was zero. Qualification asks whether "
        "the computation held, never whether the answer was pleasant.")
    assert r.cells_degenerate_but_valid == 31


print("=" * 104, flush=True)
print("  REAL-Y NUMERICAL INTEGRITY — registered before the first real bootstrap", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([t1_the_spec_is_frozen_and_says_what_it_is_not,
                        t2_a_nonfinite_outcome_in_a_used_row_is_an_error_not_a_dropna,
                        t3_topping_up_until_enough_replicates_succeed_is_refused,
                        t4_the_accounting_reconciles_or_the_cell_fails,
                        t5_a_resample_that_loses_an_arm_is_a_labelled_rejection_not_a_skip,
                        t5b_the_geometry_that_produces_it,
                        t6_an_unregistered_rejection_reason_cannot_be_used,
                        t7_a_zero_width_interval_from_agreeing_resamples_is_an_answer,
                        t8_a_nonfinite_theta_is_invalid_however_clean_the_replicates_are,
                        t9_every_uncomputable_result_carries_a_frozen_reason,
                        t10_the_report_passes_only_when_every_criterion_holds,
                        t11_the_gate_does_not_read_the_answer], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    print(f"\n  {SPEC.SPEC_VERSION} registered · spec_hash {SPEC.spec_hash()} · "
          f"floor {SPEC.BOOTSTRAP_VALID_FLOOR} · {SPEC.BOOTSTRAP_REPS_REQUESTED} reps",
          flush=True)
sys.exit(1 if fail else 0)
