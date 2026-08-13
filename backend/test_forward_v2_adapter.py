"""FORWARD ENGINE WIRING · the ten acceptance criteria, plus the poison test.

Everything here runs on synthetic rows. That is the point: the computational path has to be
proven and frozen BEFORE the first novel outcome exists, because afterwards we would know exactly
where support and the bootstrap hurt, and an "engineering" fix to the adapter would be an
outcome-informed statistical choice in overalls.

The poison test is the sharpest of them. "Attached history changed the answer" can hide inside
ordinary variation; historical rows carrying +1000% outcomes cannot hide anywhere. If a single
one of them is inside the estimate, θ or the interval moves visibly.
"""
from __future__ import annotations

import inspect
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import forward_evaluation as FE                                       # noqa: E402
import forward_v2_adapter as AD                                       # noqa: E402
import historical_ranking_policy as RP                                # noqa: E402

ok = fail = 0
CUTOFF = FE.record()["data_cutoff_at_registration"]                    # 2026-08-06
SPEC_HASH = FE.record()["spec_hash"]
ADAPTER_HASH = AD.record()["adapter_hash"]


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                            # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


# ── a synthetic forward world ───────────────────────────────────────────────
def day(offset: int) -> str:
    """`offset` days after 2026-08-06, as a date string that sorts correctly."""
    return str(np.datetime64("2026-08-06") + np.timedelta64(offset, "D"))


def forward_world(n_days=60, per_day=24, seed=7, effect=1.5):
    """Six families, membership independent of family, a planted effect.

    Six and not two: the frozen decision kernel requires `eligible_setups >= 5`, so a fixture with
    two strata pins every verdict on the support gate and δ* could never matter. A test whose
    outcome cannot move is not testing anything.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for k in range(1, n_days + 1):
        for j in range(per_day):
            # family and membership must be INDEPENDENT, or every stratum has an empty arm
            member = (j // 6) % 2 == 0
            rows.append({"date": day(k), "family": f"fam{j % 6}", "in_cell": member,
                         "y": float(rng.normal(effect if member else 0.0, 3.0))})
    df = pd.DataFrame(rows)
    O = df[["family"]].copy()
    masks = {"cellA": df["in_cell"].to_numpy(bool),
             "cellB": (~df["in_cell"].to_numpy(bool)) | (np.arange(len(df)) % 7 == 0)}
    return O, df["date"].to_numpy(), df["y"].to_numpy(float), masks


def poisoned_history(n_days=120, per_day=24, seed=11, magnitude=1000.0):
    """Exposed rows, before the cutoff, carrying absurd outcomes."""
    rng = np.random.default_rng(seed)
    rows = []
    for k in range(n_days, 0, -1):
        for j in range(per_day):
            rows.append({"date": day(-k), "family": f"fam{j % 6}",
                         "in_cell": (j // 6) % 2 == 0,
                         "y": magnitude * float(rng.integers(1, 5))})
    df = pd.DataFrame(rows)
    O = df[["family"]].copy()
    masks = {"cellA": df["in_cell"].to_numpy(bool),
             "cellB": (~df["in_cell"].to_numpy(bool)) | (np.arange(len(df)) % 7 == 0)}
    return O, df["date"].to_numpy(), df["y"].to_numpy(float), masks


def attach(hist, fut):
    """Prepend exposed history to a forward world, as a snapshot would."""
    hO, hd, hy, hm = hist
    fO, fd, fy, fm = fut
    O = pd.concat([hO, fO], ignore_index=True)
    dates = np.concatenate([hd, fd])
    y = np.concatenate([hy, fy])
    masks = {c: np.concatenate([hm[c], fm[c]]) for c in fm}
    return O, dates, y, masks


def run(world, purpose=AD.SYNTHETIC_WIRING_FIXTURE, **kw):
    O, dates, y, masks = world
    return AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF, purpose=purpose,
                       forward_spec_hash=SPEC_HASH, adapter_hash=ADAPTER_HASH, **kw)


# ── 1 ───────────────────────────────────────────────────────────────────────
def t1_the_arbitrary_estimator_callback_is_gone():
    """the statistical choice cannot still be open when the data arrives"""
    assert not hasattr(FE, "evaluate_forward"), (
        "forward_evaluation still exposes an evaluator taking a caller-supplied estimator")
    for fn in (AD.evaluate, AD.run_evaluation):
        params = set(inspect.signature(fn).parameters)
        for forbidden in ("estimator", "estimand", "n_boot", "delta_star", "eligibility",
                          "ranking_policy", "support"):
            assert forbidden not in params, f"{fn.__name__} still takes {forbidden}"


# ── 2 ───────────────────────────────────────────────────────────────────────
def t2_the_adapter_identity_is_hashed_and_immutable():
    assert AD.is_frozen()
    a = AD.assert_bound_to_forward_spec()
    assert a["adapter_hash"] == ADAPTER_HASH
    edited = dict(a)
    edited["bootstrap"] = dict(a["bootstrap"], replicates=500)
    edited["adapter_hash"] = AD._h({k: v for k, v in edited.items() if k != "adapter_hash"})
    try:
        AD.freeze(edited)
    except AD.ForwardAdapterError as e:
        assert "already frozen" in str(e)
        return
    raise AssertionError("the frozen adapter accepted a changed computational path")


# ── 3 ───────────────────────────────────────────────────────────────────────
def t3_no_exposed_row_can_enter_a_prospective_computation():
    w = attach(poisoned_history(n_days=30), forward_world())
    O, dates, y, masks = w
    # a prospective run is not even allowed to be offered a population
    try:
        AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                    purpose=AD.FIRST_PROSPECTIVE_EVALUATION,
                    population=np.arange(len(dates)), forward_spec_hash=SPEC_HASH,
                    adapter_hash=ADAPTER_HASH)
    except AD.ForwardAdapterError as e:
        assert "may not be handed an explicit population" in str(e)
    else:
        raise AssertionError("a prospective run accepted a caller-chosen population")

    art = AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                      purpose=AD.FIRST_PROSPECTIVE_EVALUATION, forward_spec_hash=SPEC_HASH,
                      adapter_hash=ADAPTER_HASH)
    assert art["forward_window"][0] > CUTOFF, art["forward_window"]
    # 30 and not 60 days: a prospective run's population is the frozen LOOK WINDOW, so the
    # forward rows available are not the same thing as the forward rows evaluated. See
    # test_forward_observation_policy.t4 — waiting longer must add nothing.
    import forward_observation_policy as OP                           # noqa: PLC0415
    assert art["forward_rows"] == OP.FIRST_LOOK_TRADING_DAYS * 24, art["forward_rows"]


# ── 4 · the metamorphic core, on the FULL v2 pipeline ───────────────────────
def t4_attached_history_changes_nothing_the_pipeline_produces():
    """theta · bootstrap interval · verdict · rank, all unmoved"""
    fut = forward_world()
    base = run(fut)
    assert base["computable"] >= 1, base

    for n_hist in (30, 120, 400):
        art = run(attach(poisoned_history(n_days=n_hist), fut))
        assert art["forward_rows"] == base["forward_rows"], art["forward_rows"]
        assert art["forward_rows_digest"] == base["forward_rows_digest"]
        assert art["rng_root"] == base["rng_root"], (
            "the RNG root moved with attached history; the intervals would drift while theta "
            "stayed still, and the leak would look like Monte-Carlo noise")
        for cell, row in base["cells"].items():
            got = art["cells"][cell]
            assert got["status"] == row["status"], (cell, got["status"], row["status"])
            if row["status"] != AD.COMPUTABLE:
                continue
            assert got["theta_hex"] == row["theta_hex"], f"theta moved on {cell}"
            assert got["interval_hex"] == row["interval_hex"], f"interval moved on {cell}"
            assert got["verdict"] == row["verdict"], f"verdict moved on {cell}"
        assert art["ranking"] == base["ranking"], "the rank order moved with attached history"
        assert art["artifact_hash"] == base["artifact_hash"], (
            "the whole artifact is not identical; something situational is inside it")


# ── the poison test ─────────────────────────────────────────────────────────
def t5_POISON_absurd_historical_outcomes_do_not_touch_the_forward_result():
    """+1000% rows before the cutoff; a single one inside the estimate would be visible"""
    fut = forward_world()
    base = run(fut)
    poisoned = run(attach(poisoned_history(n_days=200, magnitude=100_000.0), fut))
    assert poisoned["artifact_hash"] == base["artifact_hash"], (
        "historical rows carrying +100,000 moved the forward artifact. 'attached history changed "
        "the count' can hide inside ordinary variation; this cannot hide anywhere.")
    for cell, row in base["cells"].items():
        if row["status"] == AD.COMPUTABLE:
            assert poisoned["cells"][cell]["theta_hex"] == row["theta_hex"]
            assert poisoned["cells"][cell]["interval_hex"] == row["interval_hex"]


# ── 5 ───────────────────────────────────────────────────────────────────────
def t6_insufficient_forward_support_stays_insufficient():
    """no historical rescue, and no estimate produced anyway"""
    thin = forward_world(n_days=8)                         # 8 days < the frozen floor of 25
    art = run(thin)
    assert art["computable"] == 0, art["computable"]
    for cell, row in art["cells"].items():
        assert row["status"] == AD.INSUFFICIENT_FORWARD_SUPPORT, (cell, row["status"])
        assert "theta_hex" not in row, f"{cell} produced an estimate below the frozen floors"
        assert "contamination, not a repair" in row["reason"]
    assert art["ranking"] == []

    # attaching a mountain of history does not rescue it
    rescued = run(attach(poisoned_history(n_days=400, magnitude=1.0), thin))
    assert rescued["computable"] == 0, (
        "history rescued a cell that the forward window cannot support")
    assert rescued["artifact_hash"] == art["artifact_hash"]


# ── 6 and 7 ─────────────────────────────────────────────────────────────────
def t7_the_rng_is_deterministic_and_situationally_blind():
    """same spec · same future rows · same lineage → identical intervals, always"""
    fut = forward_world()
    a, b = run(fut), run(fut)
    assert a["artifact_hash"] == b["artifact_hash"], "two identical runs diverged"

    key = AD.ForwardBootstrapRNGProvider("root").semantic_key("cellA")
    blob = "|".join(key).lower()
    for situational in ("session", "run_id", "runid", "timestamp", "clock", "attempt", "s0001"):
        assert situational not in blob, f"the RNG stream is keyed on {situational}"
    assert set(inspect.signature(AD.ForwardBootstrapRNGProvider.__init__).parameters) == {
        "self", "keyed_root"}

    # a different forward slice must give a different root, or the key is not doing its job
    other = run(forward_world(seed=99))
    assert other["rng_root"] != a["rng_root"]


# ── 8 ───────────────────────────────────────────────────────────────────────
def t8_a_decision_policy_change_does_not_move_the_ranking():
    fut = forward_world()
    base = run(fut)
    original = AD.DELTA_STAR
    try:
        AD.DELTA_STAR = 25.0                     # nothing can be material at this threshold
        # delta_star must stay positive — the decision contract refuses zero, and
        # a test that reached for it would be testing the wrong refusal
        moved = run(fut)
    finally:
        AD.DELTA_STAR = original
    verdicts_before = {c: r.get("verdict") for c, r in base["cells"].items()}
    verdicts_after = {c: r.get("verdict") for c, r in moved["cells"].items()}
    assert verdicts_before != verdicts_after, (
        "the decision policy did not actually change; this test would pass vacuously")
    assert [r["cell_identity"] for r in moved["ranking"]] == \
           [r["cell_identity"] for r in base["ranking"]], (
        "the rank order followed the decision policy, so this is a decision ranking")


# ── 9 ───────────────────────────────────────────────────────────────────────
def t9_the_claim_universe_is_exact_and_day_level_cannot_re_enter():
    f = FE.record()
    assert f["registered_claim_count"] == 31, f["registered_claim_count"]
    assert len(f["deferred_day_level"]) == 6, f["deferred_day_level"]
    fut = forward_world()
    O, dates, y, masks = fut
    smuggled = dict(masks)
    smuggled["sig_macro_vix_up"] = masks["cellA"]
    try:
        AD.evaluate(O=O, dates=dates, y=y, masks=smuggled, cutoff=CUTOFF,
                    purpose=AD.SYNTHETIC_WIRING_FIXTURE,
                    deferred_day_level=f["deferred_day_level"],
                    forward_spec_hash=SPEC_HASH, adapter_hash=ADAPTER_HASH)
    except AD.ForwardAdapterError as e:
        assert "DAY_LEVEL" in str(e)
        return
    raise AssertionError("a deferred DAY_LEVEL claim re-entered the forward search space")


# ── 10 ──────────────────────────────────────────────────────────────────────
def t10_the_adapter_is_frozen_before_any_novel_evidence_exists():
    a = AD.assert_bound_to_forward_spec()
    assert a["binds_to_forward_evaluation_spec"] == SPEC_HASH
    assert a["statistical_freedom"].startswith("NONE")
    # nothing prospective has been produced, and today's rows cannot produce it
    assert FE.forward_index(np.array(["2021-05-27", CUTOFF]), CUTOFF).size == 0
    assert not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                           "FORWARD_EVALUATION_RUN.json")), (
        "a forward evaluation artifact exists; 3C was run before novel evidence existed")


# ── the engineering-only escape hatch, and its price ────────────────────────
def t11_a_regression_run_may_see_exposed_rows_and_can_never_be_evidence():
    """one flag grants the convenience and applies the label; they cannot be separated"""
    w = attach(poisoned_history(n_days=40, magnitude=1.0), forward_world())
    O, dates, y, masks = w
    art = AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                      purpose=AD.FORWARD_ADAPTER_REGRESSION,
                      population=np.arange(len(dates)),          # deliberately the whole snapshot
                      forward_spec_hash=SPEC_HASH, adapter_hash=ADAPTER_HASH)
    assert art["evidence_role"] == "ENGINEERING_FIXTURE", art["evidence_role"]
    assert art["prospective_claim"] is False
    assert art["may_enter_evidence_ledger"] is False
    assert art["forward_rows"] == len(dates), "the regression run did not see the rows it asked for"
    try:
        AD.assert_not_evidence(art)
    except AD.ForwardAdapterError as e:
        assert "cannot be recorded as a forward observation" in str(e)
        return
    raise AssertionError("an engineering fixture was admissible as evidence")


def t12_the_regression_run_reproduces_the_kernel_on_a_given_population():
    """what the escape hatch is FOR: same rows in, same numbers as the proven kernel out"""
    import combolab_v2 as E                                          # noqa: PLC0415
    import v2_kernel as K                                            # noqa: PLC0415
    O, dates, y, masks = forward_world()
    art = AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                      purpose=AD.FORWARD_ADAPTER_REGRESSION,
                      population=np.arange(len(dates)), forward_spec_hash=SPEC_HASH,
                      adapter_hash=ADAPTER_HASH)
    d = np.asarray([str(x) for x in dates])
    sup = E.Support(O, d, {c: np.asarray(m, bool) for c, m in masks.items()}, verbose=False)
    _, gi = np.unique(d, return_inverse=True)
    for cell in sup.cells:
        theta = float(K.Frozen(sup, cell, gi).theta(np.asarray(y, float)))
        assert art["cells"][cell]["theta_hex"] == theta.hex(), (
            f"the adapter and a direct kernel call disagree on {cell}")


TESTS = [t1_the_arbitrary_estimator_callback_is_gone,
         t2_the_adapter_identity_is_hashed_and_immutable,
         t3_no_exposed_row_can_enter_a_prospective_computation,
         t4_attached_history_changes_nothing_the_pipeline_produces,
         t5_POISON_absurd_historical_outcomes_do_not_touch_the_forward_result,
         t6_insufficient_forward_support_stays_insufficient,
         t7_the_rng_is_deterministic_and_situationally_blind,
         t8_a_decision_policy_change_does_not_move_the_ranking,
         t9_the_claim_universe_is_exact_and_day_level_cannot_re_enter,
         t10_the_adapter_is_frozen_before_any_novel_evidence_exists,
         t11_a_regression_run_may_see_exposed_rows_and_can_never_be_evidence,
         t12_the_regression_run_reproduces_the_kernel_on_a_given_population]

print("=" * 100, flush=True)
print("  FORWARD ENGINE WIRING · frozen before the evidence, proven on synthetic rows", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate(TESTS, 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
