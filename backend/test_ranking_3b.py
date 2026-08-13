"""Gate 3B · the adversarial set, written before the ranking was allowed to run.

Gate 2 opened `HISTORICAL_RESEARCH` and, in doing so, created a way to make old evidence look
new: re-execute it and let the fresh timestamp collect the cleaner label. Every test here points
at that seam or at the smaller one beside it — that ordering results a researcher has already
seen is not free just because the claims are old.

The tests are numbered as they were specified, and t3 is the one that matters most: it is the
only one whose failure would be silent, because a laundered run produces a perfectly well-formed
artifact with the right numbers in it.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import evidence_fingerprint as FP                                     # noqa: E402
import exposed_evidence as EE                                         # noqa: E402
import historical_application_gate as GATE                            # noqa: E402
import historical_ranking_policy as RP                                # noqa: E402
import ranking_run as RR                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
# the trial log is append-only and real; the tests get their own so a test run never inflates it
RR.TRIAL_LOG = os.path.join(HERE, ".test_ranking_trials.json")
if os.path.exists(RR.TRIAL_LOG):
    os.remove(RR.TRIAL_LOG)

ok = fail = 0
NOW = "2026-08-13T00:00:00Z"


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
    """Rebuilt from the registry, so no test opens the data or recomputes anything."""
    reg = FP.FingerprintRegistry.load()
    assert reg.entries, "the fingerprint registry is empty; run exposed_evidence.py first"
    return FP.EvidenceFingerprint.from_dict(reg.entries[0])


def exposure_status() -> dict:
    with open(os.path.join(HERE, "EVIDENCE_EXPOSURE_LOG.json")) as f:
        return json.load(f)["exposures"][0]["evidence_status"]


# ── 1 ───────────────────────────────────────────────────────────────────────
def t1_ranking_the_existing_31_moves_the_selection_path_and_not_k_exposed():
    """no claim became available that was not already available"""
    fp = registered_fingerprint()
    art = RR.rank_exposed_evidence(fingerprint=fp, theta=EE.exposed_theta(),
                                   exposure_ids=EE.exposure_ids(),
                                   evidence_status=exposure_status(), recorded_at=NOW)
    assert art.claim_exposure_delta == 0, art.claim_exposure_delta
    assert art.selection_path_effect == "SELECTION_PATH_CHANGE"
    assert art.input_evidence_standing == "ALREADY_EXPOSED"
    assert len(art.rows) == 31, len(art.rows)
    acc = RR.selection_path_accounting(fp.fingerprint())
    assert acc["new_claim_exposure"] == 0
    assert acc["selection_path_operations"] == 1, acc
    # and the roles under it did not move
    assert art.input_evidence_status["result_role"] == "ENGINE_QUALIFICATION_EVIDENCE"
    assert art.input_evidence_status["application_maturity"] == "FIRST_HISTORICAL_APPLICATION"


# ── 2 ───────────────────────────────────────────────────────────────────────
def t2_a_second_ranking_policy_on_the_same_31_is_a_second_trial():
    """the claims are old; the search over orders is new"""
    fp = registered_fingerprint()
    before = RR.selection_path_accounting(fp.fingerprint())
    # the same policy again is the same trial — a rerun must not inflate the count
    RR.rank_exposed_evidence(fingerprint=fp, theta=EE.exposed_theta(),
                             exposure_ids=EE.exposure_ids(),
                             evidence_status=exposure_status(), recorded_at=NOW)
    same = RR.selection_path_accounting(fp.fingerprint())
    assert same["selection_path_operations"] == before["selection_path_operations"], same

    # a DIFFERENT order over the same evidence is not free
    RR.record_trial(ranking_policy_hash="hypothetical_lcb_v1",
                    ranking_metric="bootstrap lower bound descending",
                    evidence_fingerprint=fp.fingerprint(), ranking_hash="deadbeef",
                    recorded_at=NOW, note="adversarial: a second order over the same 31")
    after = RR.selection_path_accounting(fp.fingerprint())
    assert after["selection_path_operations"] == before["selection_path_operations"] + 1, after
    assert after["distinct_ranking_policies_tried"] == 2, after
    assert after["new_claim_exposure"] == 0, "sorting differently exposed a new claim"


# ── 3 · the one that would fail silently ────────────────────────────────────
def t3_a_rerun_of_the_same_snapshot_after_gate_2_cannot_come_back_cleaner():
    """same data, same claims, same estimand, new timestamp — and a cleaner label on offer"""
    assert GATE.is_open(), (
        "this test is vacuous unless the gate is open; the laundering it guards against only "
        "exists because Gate 2 legitimately opened the historical mode")
    assert GATE.maturity_for_new_results() == "HISTORICAL_APPLICATION_QUALIFIED", (
        "the cleaner label must genuinely be on offer, or nothing is being refused")

    fp = registered_fingerprint()
    verdict = FP.FingerprintRegistry.load().classify(fp)
    assert verdict["classification"] == FP.REPLAY_OF_EXPOSED_EVIDENCE, verdict

    try:
        GATE.maturity_for(verdict)
    except FP.EvidenceReplayLaunderingError as e:
        assert "does not reach back" in str(e)
    else:
        raise AssertionError("a replay of evidence exposed before Gate 2 collected the maturity "
                             "the gate raised for results produced after it")

    # and the status it may actually carry is the one it went in with
    admitted = FP.admissible_status(verdict, {"application_maturity":
                                              "HISTORICAL_APPLICATION_QUALIFIED",
                                              "result_role": "EXPLORATORY_HISTORICAL_EVIDENCE"})
    assert admitted["application_maturity"] == "FIRST_HISTORICAL_APPLICATION", admitted
    assert admitted["result_role"] == "ENGINE_QUALIFICATION_EVIDENCE", admitted


# ── 4 ───────────────────────────────────────────────────────────────────────
def t4_the_current_31_cannot_be_ranked_as_a_preregistered_selection():
    fp = registered_fingerprint()
    try:
        RR.rank_exposed_evidence(fingerprint=fp, theta=EE.exposed_theta(),
                                 exposure_ids=EE.exposure_ids(),
                                 evidence_status=exposure_status(), recorded_at=NOW,
                                 declared_use=RR.PROSPECTIVE)
    except RP.RetroactiveRankingRegistrationError as e:
        assert "exploratory" in str(e)
        return
    raise AssertionError("an already-seen ranking universe was labelled preregistered")


# ── 5 ───────────────────────────────────────────────────────────────────────
def t5_a_new_snapshot_id_over_the_same_rows_is_not_prospective():
    """contamination lives in the data, not in the name of the run"""
    fp = registered_fingerprint()
    renamed = FP.EvidenceFingerprint(
        data_lineage=FP.DataLineage(
            snapshot_id="opportunities-parquet-2026-08-14",          # copied, renamed
            rows=fp.data_lineage.rows, dates=fp.data_lineage.dates,
            content_digest=fp.data_lineage.content_digest,
            coverage_start=fp.data_lineage.coverage_start,
            coverage_end=fp.data_lineage.coverage_end),
        outcome_definition=fp.outcome_definition, population=fp.population,
        claim_identity=fp.claim_identity, estimand=fp.estimand)
    assert renamed.fingerprint() == fp.fingerprint(), (
        "renaming the snapshot changed the evidence identity, which makes the rename a "
        "laundering key")
    verdict = FP.FingerprintRegistry.load().classify(renamed)
    assert verdict["classification"] == FP.REPLAY_OF_EXPOSED_EVIDENCE, verdict

    try:
        RR.rank_exposed_evidence(fingerprint=renamed, theta=EE.exposed_theta(),
                                 exposure_ids=EE.exposure_ids(),
                                 evidence_status=exposure_status(), recorded_at=NOW,
                                 declared_use=RR.PROSPECTIVE)
    except (RP.RetroactiveRankingRegistrationError, RR.RankingInputError):
        return
    raise AssertionError("a renamed copy of exposed data was ranked prospectively")


# ── 6 ───────────────────────────────────────────────────────────────────────
def t6_the_order_does_not_move_when_the_decision_policy_does():
    """proven on the real artifact, not only on the policy"""
    fp = registered_fingerprint()
    theta = EE.exposed_theta()
    art = RR.rank_exposed_evidence(fingerprint=fp, theta=theta, exposure_ids=EE.exposure_ids(),
                                   evidence_status=exposure_status(), recorded_at=NOW)
    order = [r["cell_identity"] for r in art.rows]

    cells = sorted(theta)
    for verdicts in ({c: "BUILD" for c in cells},
                     {c: "UNRESOLVED" for c in cells},
                     {c: ("BUILD" if i % 2 else "VETO") for i, c in enumerate(cells)}):
        again = RP.rank({c: {"theta": t, "verdict": verdicts[c],
                             "interval": (t - 1, t + 1), "support": 0.9} for c, t in theta.items()})
        assert [r["cell_identity"] for r in again] == order, (
            "the order moved with the decision policy; this is a decision ranking wearing an "
            "evidence ranking's name")
    assert art.decision_policy_invariant is True


# ── the ones the six implied ────────────────────────────────────────────────
def t7_the_fingerprint_is_blind_to_everything_a_rerun_changes():
    for k in ("run_id", "execution_timestamp", "engine_version", "execution_mode", "gate_state",
              "rng_policy", "bootstrap_policy", "decision_policy"):
        assert k in FP.NOT_PART_OF_IDENTITY, k
    fp = registered_fingerprint()
    assert set(fp.components()) == {"data_content_hash", "outcome_definition", "population",
                                    "claim_identity", "estimand", "identity_version"}
    assert "snapshot_id" not in json.dumps(fp.components()), (
        "the snapshot NAME reached the identity, and names are free")


def t8_a_ranking_cannot_grant_what_the_evidence_under_it_lacks():
    """the ceiling is inherited; ordering results is not a stronger fact about them"""
    fp = registered_fingerprint()
    art = RR.rank_exposed_evidence(fingerprint=fp, theta=EE.exposed_theta(),
                                   exposure_ids=EE.exposure_ids(),
                                   evidence_status=exposure_status(), recorded_at=NOW)
    for allowed in ("inspect", RR.RECORD_EXPLORATORY_RANKING,
                    "nominate_for_forward_validation", "freeze_forward_spec"):
        art.assert_permits(allowed)
    for refused in (RR.LABEL_PREREGISTERED_SELECTION, RR.RETROACTIVE_REGISTER,
                    "promote_as_validated_edge", "book"):
        try:
            art.assert_permits(refused)
        except RR.RankingActionError:
            continue
        raise AssertionError(f"an exploratory ranking permitted {refused}")

    # even declared prospective, the evidence's own ceiling still governs
    prospective = set(RR.ceiling(exposure_status(), RR.PROSPECTIVE))
    assert "book" not in prospective and "promote_as_validated_edge" not in prospective, (
        "a ranking widened the ceiling of the evidence under it")


def t9_a_fresh_execution_cannot_be_fed_to_the_3b_ranking():
    """3B's input is the artifact on record; anything else is a rerun by another route"""
    fp = registered_fingerprint()
    fresh = FP.EvidenceFingerprint(
        data_lineage=FP.DataLineage(snapshot_id="snap-2027", rows=999_999, dates=2000,
                                    content_digest="ff" * 16, coverage_start="2026-09-01",
                                    coverage_end="2027-01-01"),
        outcome_definition=fp.outcome_definition, population=fp.population,
        claim_identity=fp.claim_identity, estimand=fp.estimand)
    assert FP.FingerprintRegistry.load().classify(fresh)["classification"] == FP.NOVEL_EVIDENCE
    try:
        RR.rank_exposed_evidence(fingerprint=fresh, theta=EE.exposed_theta(),
                                 exposure_ids=EE.exposure_ids(),
                                 evidence_status=exposure_status(), recorded_at=NOW)
    except RR.RankingInputError as e:
        assert "Gate 3C" in str(e)
        return
    raise AssertionError("novel evidence was ranked as though it were the exposed snapshot")


def t10_an_extending_window_is_prospective_only_in_its_tail():
    """2021–2026-08 then 2021–2026-09 is not new evidence; the September rows are"""
    fp = registered_fingerprint()
    lin = fp.data_lineage
    extended = FP.DataLineage(snapshot_id="opportunities-parquet-2026-09-01",
                              rows=lin.rows + 4000, dates=lin.dates + 18,
                              content_digest="a1" * 16, coverage_start=lin.coverage_start,
                              coverage_end="2026-09-01")
    assert FP.compare_lineage(lin, extended) == FP.EXTENDS_EXPOSED
    assert FP.novel_window(lin, extended) == (lin.coverage_end, "2026-09-01")
    cand = FP.EvidenceFingerprint(data_lineage=extended, outcome_definition=fp.outcome_definition,
                                  population=fp.population, claim_identity=fp.claim_identity,
                                  estimand=fp.estimand)
    v = FP.FingerprintRegistry.load().classify(cand)
    assert v["classification"] == FP.PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE, v
    try:
        FP.assert_no_replay_laundering(v, {"application_maturity":
                                           "HISTORICAL_APPLICATION_QUALIFIED"})
    except FP.EvidenceReplayLaunderingError:
        return
    raise AssertionError("a window overlapping exposed evidence collected a clean status whole")


def t11_novelty_requires_coverage_and_sameness_does_not():
    """'different' is not 'new' — a filtered subset of exposed rows is different"""
    a = FP.DataLineage("snap-a", rows=100, dates=10, content_digest="aa")
    subset = FP.DataLineage("snap-b", rows=60, dates=10, content_digest="bb")
    assert FP.compare_lineage(a, subset) == FP.UNDETERMINED
    fp = registered_fingerprint()
    cand = FP.EvidenceFingerprint(data_lineage=subset, outcome_definition=fp.outcome_definition,
                                  population=fp.population, claim_identity=fp.claim_identity,
                                  estimand=fp.estimand)
    v = FP.FingerprintRegistry.load().classify(cand)
    assert v["classification"] == FP.UNDETERMINED_NOT_NOVEL, v


def t12_both_exposures_are_one_evidence_item():
    """31 claims exposed twice, not 62 claims exposed once"""
    reg = FP.FingerprintRegistry.load()
    assert len(reg.entries) == 1, [e["fingerprint"] for e in reg.entries]
    assert len(reg.entries[0]["exposure_ids"]) == 2, reg.entries[0]["exposure_ids"]
    assert reg.entries[0]["derived_from"]["lineage_completeness"] == "COMPLETE"


TESTS = [t1_ranking_the_existing_31_moves_the_selection_path_and_not_k_exposed,
         t2_a_second_ranking_policy_on_the_same_31_is_a_second_trial,
         t3_a_rerun_of_the_same_snapshot_after_gate_2_cannot_come_back_cleaner,
         t4_the_current_31_cannot_be_ranked_as_a_preregistered_selection,
         t5_a_new_snapshot_id_over_the_same_rows_is_not_prospective,
         t6_the_order_does_not_move_when_the_decision_policy_does,
         t7_the_fingerprint_is_blind_to_everything_a_rerun_changes,
         t8_a_ranking_cannot_grant_what_the_evidence_under_it_lacks,
         t9_a_fresh_execution_cannot_be_fed_to_the_3b_ranking,
         t10_an_extending_window_is_prospective_only_in_its_tail,
         t11_novelty_requires_coverage_and_sameness_does_not,
         t12_both_exposures_are_one_evidence_item]

print("=" * 100, flush=True)
print("  3B · EVIDENCE REPLAY LAUNDERING, and the cost of an order over things already seen",
      flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate(TESTS, 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if os.path.exists(RR.TRIAL_LOG):
    os.remove(RR.TRIAL_LOG)
sys.exit(1 if fail else 0)
