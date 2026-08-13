"""Gate 3C · the tests that decide whether "prospective" means anything.

Five of these guard the boundary between exposed and novel data. The sixth is the one that would
still be true if all five passed and the system were quietly pooling anyway:

    same future rows · different amounts of exposed history attached · identical result

An evaluator that recomputes θ over the whole new snapshot passes every provenance check ever
written and fails this one, because its answer moves with how much old data came along.
"""
from __future__ import annotations

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import evidence_fingerprint as FP                                     # noqa: E402
import forward_evaluation as FE                                       # noqa: E402

ok = fail = 0
CUTOFF = "2026-08-06"


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                            # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def registered_fingerprint() -> FP.EvidenceFingerprint:
    reg = FP.FingerprintRegistry.load()
    assert reg.entries, "no fingerprint registry; run exposed_evidence.py"
    return FP.EvidenceFingerprint.from_dict(reg.entries[0])


def relabel(fp: FP.EvidenceFingerprint, **kw) -> FP.EvidenceFingerprint:
    lin = fp.data_lineage
    fields = {"snapshot_id": lin.snapshot_id, "rows": lin.rows, "dates": lin.dates,
              "content_digest": lin.content_digest, "coverage_start": lin.coverage_start,
              "coverage_end": lin.coverage_end}
    fields.update(kw)
    return FP.EvidenceFingerprint(
        data_lineage=FP.DataLineage(**fields), outcome_definition=fp.outcome_definition,
        population=fp.population, claim_identity=fp.claim_identity, estimand=fp.estimand)


# ── a deterministic world, so the invariant is about selection and not noise ─
def world(n_days: int, start_day: int, seed: int):
    """`start_day` days after 2026-08-01, one block of rows per day."""
    rng = np.random.default_rng(seed)
    dates, member, y = [], [], []
    for k in range(n_days):
        day = f"2026-{8 + (start_day + k) // 30:02d}-{(start_day + k) % 30 + 1:02d}"
        for j in range(12):
            dates.append(day)
            member.append((j + k) % 2 == 0)
            y.append(float(rng.integers(-500, 500)) / 100.0)
    return np.array(dates), np.array(member, bool), np.array(y)


def estimator_factory(y):
    """Difference of medians over the indices it is GIVEN. Pure, so any drift is selection."""
    def est(cell, idx, member=None):
        m = np.asarray(member)[idx]
        return float(np.median(y[idx][m]) - np.median(y[idx][~m]))
    return est


# ── 1 ───────────────────────────────────────────────────────────────────────
def t1_a_new_snapshot_id_over_identical_rows_is_not_novel():
    fp = registered_fingerprint()
    renamed = relabel(fp, snapshot_id="opportunities-parquet-2026-09-30")
    v = FP.FingerprintRegistry.load().classify(renamed)
    assert v["classification"] == FP.REPLAY_OF_EXPOSED_EVIDENCE, v


# ── 2 ───────────────────────────────────────────────────────────────────────
def t2_a_repackaged_subset_of_exposed_rows_is_not_novel():
    """'different' is not 'new'; a filter is a repackaging"""
    fp = registered_fingerprint()
    lin = fp.data_lineage
    subset = relabel(fp, snapshot_id="opportunities-filtered", rows=lin.rows // 2,
                     dates=lin.dates, content_digest="beef" * 8,
                     coverage_start=lin.coverage_start, coverage_end=lin.coverage_end)
    v = FP.FingerprintRegistry.load().classify(subset)
    assert v["classification"] != FP.NOVEL_EVIDENCE, v
    # and a subset cannot be evaluated as prospective, because none of it is after the cutoff
    dates = np.array(["2025-01-02"] * 40)
    try:
        FE.assert_frozen_before(dates)
    except FE.ProspectiveEvidenceContaminationError:
        return
    raise AssertionError("rows from inside the exposed window were accepted as prospective")


# ── 3 ───────────────────────────────────────────────────────────────────────
def t3_an_extending_snapshot_is_novel_only_in_its_tail():
    fp = registered_fingerprint()
    lin = fp.data_lineage
    extended = relabel(fp, snapshot_id="opportunities-parquet-2026-09-30",
                       rows=lin.rows + 5000, dates=lin.dates + 38,
                       content_digest="cafe" * 8, coverage_end="2026-09-30")
    assert FP.compare_lineage(lin, extended.data_lineage) == FP.EXTENDS_EXPOSED
    assert FP.novel_window(lin, extended.data_lineage) == (CUTOFF, "2026-09-30")
    v = FP.FingerprintRegistry.load().classify(extended)
    assert v["classification"] == FP.PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE, v
    # the evaluator agrees on where the tail starts
    dates = np.array(["2026-08-05", "2026-08-06", "2026-08-07", "2026-09-30"])
    idx = FE.forward_index(dates, CUTOFF)
    assert list(dates[idx]) == ["2026-08-07", "2026-09-30"], list(dates[idx])


# ── 4 · the repair that suggests itself ─────────────────────────────────────
def t4_one_exposed_row_in_a_prospective_population_is_refused():
    """the contamination that arrives wearing a helpful face"""
    dates, member, y = world(n_days=40, start_day=6, seed=1)
    idx = FE.forward_index(dates, CUTOFF)
    smuggled = np.concatenate([np.array([0]), idx])          # one row from the exposed window
    dates_with_history = np.concatenate([np.array(["2026-08-01"]), dates[1:]])
    try:
        FE.evaluate_forward(dates=dates_with_history, membership={"c": member},
                            estimator=lambda c, i: 0.0, cutoff=CUTOFF, population=smuggled)
    except FE.ProspectiveEvidenceContaminationError as e:
        assert "never more history" in str(e)
        return
    raise AssertionError("an exposed observation entered a population declared prospective")


# ── 5 ───────────────────────────────────────────────────────────────────────
def t5_a_thin_forward_window_returns_insufficient_support_and_not_a_number():
    """underpowered is the result, and the fix must not be available"""
    dates, member, y = world(n_days=8, start_day=7, seed=2)          # 8 days, 96 rows
    out = FE.evaluate_forward(dates=dates, membership={"c": member},
                              estimator=estimator_factory(y), cutoff=CUTOFF)
    row = out["cells"]["c"]
    assert row["status"] == FE.INSUFFICIENT_FORWARD_SUPPORT, row
    assert "theta" not in row, "a cell below the frozen floors still produced an estimate"
    assert out["computable"] == 0 and out["insufficient"] == 1, out
    assert out["ranking"] == [], "an ineligible cell was ranked"
    assert row["reasons"], "no reason was given for refusing to compute"
    assert out["historical_backfill"] == "FORBIDDEN"


# ── 6 · the invariant the other five cannot see ─────────────────────────────
def t6_the_prospective_result_does_not_move_with_attached_history():
    """same future rows, more old history bolted on, identical answer"""
    fut_dates, fut_member, fut_y = world(n_days=40, start_day=7, seed=3)

    results = []
    for hist_days in (0, 60, 400):
        if hist_days:
            h_dates, h_member, h_y = world(n_days=hist_days, start_day=-hist_days - 40, seed=9)
            # exposed history, dated strictly before the cutoff
            h_dates = np.array([f"2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
                                for i in range(len(h_dates))])
            dates = np.concatenate([h_dates, fut_dates])
            member = np.concatenate([h_member, fut_member])
            y = np.concatenate([h_y, fut_y])
        else:
            dates, member, y = fut_dates, fut_member, fut_y

        est = estimator_factory(y)
        out = FE.evaluate_forward(
            dates=dates, membership={"c": member},
            estimator=lambda c, i, m=member: est(c, i, m), cutoff=CUTOFF)
        results.append(out)

    base = results[0]
    assert base["computable"] == 1, base
    for out in results[1:]:
        assert out["forward_rows"] == base["forward_rows"], (
            f"the forward population changed with attached history: {out['forward_rows']} vs "
            f"{base['forward_rows']}")
        assert out["forward_window"] == base["forward_window"], out["forward_window"]
        assert out["cells"]["c"]["theta"] == base["cells"]["c"]["theta"], (
            f"theta moved with how much exposed history came along "
            f"({out['cells']['c']['theta']} vs {base['cells']['c']['theta']}). This is not "
            f"forward evaluation — the old data is inside the estimate.")
        assert out["ranking"] == base["ranking"]


# ── the spec itself ─────────────────────────────────────────────────────────
def t7_the_spec_is_frozen_and_says_what_it_forbids():
    assert FE.is_frozen(), "no forward evaluation spec is frozen"
    s = FE.record()
    assert s["estimator_input"] == "FORWARD_ONLY", s["estimator_input"]
    assert s["historical_backfill"] == "FORBIDDEN"
    assert s["pooled_old_and_new_estimator"] == "FORBIDDEN_IN_V1"
    assert s["ranking_policy_hash"] == "2aef967dc92786ce", s["ranking_policy_hash"]
    assert s["registered_claim_count"] == 31, s["registered_claim_count"]
    assert len(s["deferred_day_level"]) == 6
    assert s["eligibility"] == {"n_min": 100, "dates_min": 25, "max_single_date_share": 0.20,
                                "applies_to": "both arms of every stratum"}, s["eligibility"]
    assert s["data_cutoff_at_registration"] == CUTOFF, s["data_cutoff_at_registration"]


def t8_the_cutoff_is_server_derived_and_the_spec_cannot_be_edited_after_freezing():
    """the field a caller most benefits from misstating is the one they may not supply"""
    s = FE.record()
    assert s["data_cutoff_at_registration"] == s["source_lineage"]["coverage_end"], (
        "the cutoff did not come from the measured source lineage")
    edited = dict(s)
    edited["data_cutoff_at_registration"] = "2020-01-01"
    edited["spec_hash"] = FE._h({k: v for k, v in edited.items() if k != "spec_hash"})
    try:
        FE.freeze(edited)
    except FE.ForwardSpecError as e:
        assert "not frozen" in str(e)
        return
    raise AssertionError("a frozen forward spec accepted a rewritten cutoff")


def t9_the_current_exposed_rows_are_not_a_prospective_evaluation():
    """3C must not be runnable on today's data, and the refusal is mechanical"""
    exposed = np.array(["2021-05-27", "2024-01-02", CUTOFF])
    try:
        FE.assert_frozen_before(exposed)
    except FE.ProspectiveEvidenceContaminationError as e:
        assert CUTOFF in str(e)
    else:
        raise AssertionError("the exposed window passed as evidence the spec predates")
    assert FE.forward_index(exposed, CUTOFF).size == 0, (
        "rows from the exposed window were selected as forward")


TESTS = [t1_a_new_snapshot_id_over_identical_rows_is_not_novel,
         t2_a_repackaged_subset_of_exposed_rows_is_not_novel,
         t3_an_extending_snapshot_is_novel_only_in_its_tail,
         t4_one_exposed_row_in_a_prospective_population_is_refused,
         t5_a_thin_forward_window_returns_insufficient_support_and_not_a_number,
         t6_the_prospective_result_does_not_move_with_attached_history,
         t7_the_spec_is_frozen_and_says_what_it_forbids,
         t8_the_cutoff_is_server_derived_and_the_spec_cannot_be_edited_after_freezing,
         t9_the_current_exposed_rows_are_not_a_prospective_evaluation]

print("=" * 100, flush=True)
print("  3C · FORWARD EVALUATION — what the estimate is computed ON, not where it came from",
      flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate(TESTS, 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
