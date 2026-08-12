"""Instrument qualification is not evidence qualification.

The sharpest test in this file is `t7`, and it is sharp because everything in it really did
pass. The extraction is faithful, the oracle reproduces bit for bit, the engine is qualified —
and the 31 real-y estimates that were made available WHILE checking that do not become
confirmatory because the check succeeded. That is retroactive preregistration in its most
persuasive form, and the persuasive form is the one worth a fatal error.

The other half is the mistake in the opposite direction. Refusing to let a researcher freeze a
specification that history suggested would be a system that forbids hypothesis formation. Seeing
something interesting and then declaring, in advance, how it will be tested on data that does
not exist yet is exactly right. `FREEZE_FORWARD_SPEC` is available to exploratory evidence; what
it produces is a new forward boundary, and the result that suggested it stays exploratory.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import evidence_status as ES                                         # noqa: E402

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


def t1_four_axes_answer_four_different_questions():
    """one enum could not have said all of this without lying about part of it"""
    s = ES.V2_FIRST_HISTORICAL
    assert s.evidence_origin == ES.HISTORICAL_RESEARCH
    assert s.instrument_validation_basis == ES.SYNTHETIC_CAPABILITY_VALIDATED
    assert s.application_maturity == ES.FIRST_HISTORICAL_APPLICATION
    assert s.result_role == ES.EXPLORATORY_HISTORICAL_EVIDENCE


def t2_authorisation_is_an_intersection():
    """every axis can remove a right and none can add one"""
    s = ES.V2_FIRST_HISTORICAL
    assert s.permits(ES.RECORD_HISTORICAL_VERDICT)
    assert not s.permits(ES.PROMOTE_AS_VALIDATED_EDGE), (
        "the origin allows promotion and the maturity does not; the intersection must refuse")
    # relax only the maturity and the right appears — nothing else changed
    qualified = s.with_maturity(ES.HISTORICAL_APPLICATION_QUALIFIED)
    assert not qualified.permits(ES.PROMOTE_AS_VALIDATED_EDGE), (
        "result_role is still exploratory, so the intersection must still refuse")


def t2b_REPRODUCTION_a_single_axis_would_have_granted_it():
    """the guard shown its defect: read one ceiling instead of the intersection"""
    s = ES.V2_FIRST_HISTORICAL
    origin_alone = ES.PROMOTE_AS_VALIDATED_EDGE in ES.ORIGIN_CEILING[s.evidence_origin]
    assert origin_alone is True, (
        "the reproduction failed: HISTORICAL_RESEARCH was supposed to allow promotion on its "
        "own, which is exactly why a single-axis check would have let this through")
    assert not s.permits(ES.PROMOTE_AS_VALIDATED_EDGE)


def t3_a_fixture_can_only_be_read():
    s = ES.FIXTURE
    for a in ES.READ_ONLY:
        assert s.permits(a), a
    for a in ES.CONSEQUENTIAL:
        assert not s.permits(a), a


def t4_freezing_a_forward_spec_is_not_calling_a_past_result_proven():
    """the mistake in the other direction, and it would be a real one"""
    s = ES.V2_FIRST_HISTORICAL
    s.assert_permits(ES.FREEZE_FORWARD_SPEC)
    s.assert_permits(ES.NOMINATE_FOR_FORWARD_VALIDATION)
    try:
        s.assert_permits(ES.PROMOTE_AS_VALIDATED_EDGE)
    except ES.EvidenceStatusError:
        return
    raise AssertionError("an exploratory historical result was declared a validated edge")


def t5_preregistration_is_never_an_action_on_a_result():
    for s in (ES.FIXTURE, ES.V2_FIRST_HISTORICAL, ES.V2_ORACLE_EVIDENCE,
              ES.EvidenceStatus(ES.FROZEN_FORWARD, ES.HISTORICAL_APPLICATION_VALIDATED,
                                ES.HISTORICAL_APPLICATION_QUALIFIED,
                                ES.FROZEN_FORWARD_EVIDENCE)):
        for a in (ES.REGISTER_CONFIRMATORY_STUDY, ES.RETROACTIVE_CONFIRMATORY_REGISTRATION):
            assert a not in s.ceiling(), (s, a)
            try:
                s.assert_permits(a)
            except ES.EvidenceStatusError as e:
                assert "seen nothing" in str(e)
                continue
            raise AssertionError(f"{s.result_role} offered {a}")


def t6_the_oracle_evidence_is_what_it_says_it_is():
    """31 real-y estimates were made available while checking an extraction"""
    s = ES.V2_ORACLE_EVIDENCE
    assert s.result_role == ES.ENGINE_QUALIFICATION_EVIDENCE
    assert s.evidence_origin == ES.HISTORICAL_RESEARCH, (
        "the outcomes were real returns; calling them synthetic would be the comfortable lie")
    assert s.instrument_validation_basis == ES.SYNTHETIC_CAPABILITY_VALIDATED
    assert not s.permits(ES.PROMOTE_AS_VALIDATED_EDGE)
    s.assert_permits(ES.FREEZE_FORWARD_SPEC)


def t7_qualifying_the_engine_does_not_qualify_what_it_already_produced():
    """the fatal invariant, and everything in it passed

    The extraction is faithful, the oracle reproduces bit for bit, the engine is qualified. The
    31 estimates exposed while establishing that are still what they were produced as.
    """
    exposed = ES.V2_ORACLE_EVIDENCE
    try:
        ES.upgrade_result_role(exposed, ES.REGISTERED_VALIDATION_EVIDENCE, was_exposed=True)
    except ES.RetroactiveEvidenceUpgradeError as e:
        assert "Qualifying the instrument qualifies the instrument" in str(e)
        assert ES.FREEZE_FORWARD_SPEC in str(e), "the refusal must name the legitimate path"
    else:
        raise AssertionError("exposed qualification evidence was re-labelled as validation")

    # and the engine's own maturity may advance — for results produced AFTERWARDS
    future = ES.V2_FIRST_HISTORICAL.with_maturity(ES.HISTORICAL_APPLICATION_QUALIFIED)
    assert future.application_maturity == ES.HISTORICAL_APPLICATION_QUALIFIED
    assert exposed.application_maturity == ES.FIRST_HISTORICAL_APPLICATION, \
        "advancing maturity mutated the status of evidence already produced"


def t8_the_legitimate_path_produces_new_evidence_rather_than_relabelling_old():
    """freeze a spec, evaluate on data nobody has seen — and the old result stays exploratory"""
    historical = ES.V2_FIRST_HISTORICAL
    historical.assert_permits(ES.FREEZE_FORWARD_SPEC)
    forward = ES.EvidenceStatus(ES.FROZEN_FORWARD, ES.SYNTHETIC_CAPABILITY_VALIDATED,
                                ES.HISTORICAL_APPLICATION_QUALIFIED, ES.FROZEN_FORWARD_EVIDENCE)
    forward.assert_permits(ES.BOOK)
    assert historical.result_role == ES.EXPLORATORY_HISTORICAL_EVIDENCE, \
        "the result that suggested the forward study was quietly upgraded by it"


def t9_unqualified_use_cannot_book_even_on_forward_data():
    """the axes are independent: forward data under an untested application is still untested"""
    s = ES.EvidenceStatus(ES.FROZEN_FORWARD, ES.SYNTHETIC_CAPABILITY_VALIDATED,
                          ES.FIRST_HISTORICAL_APPLICATION, ES.FROZEN_FORWARD_EVIDENCE)
    assert not s.permits(ES.BOOK), "maturity was ignored because the origin was strong"
    assert s.permits(ES.RECORD_HISTORICAL_VERDICT)


print("=" * 104, flush=True)
print("  EVIDENCE STATUS — instrument qualification is not evidence qualification", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([t1_four_axes_answer_four_different_questions,
                        t2_authorisation_is_an_intersection,
                        t2b_REPRODUCTION_a_single_axis_would_have_granted_it,
                        t3_a_fixture_can_only_be_read,
                        t4_freezing_a_forward_spec_is_not_calling_a_past_result_proven,
                        t5_preregistration_is_never_an_action_on_a_result,
                        t6_the_oracle_evidence_is_what_it_says_it_is,
                        t7_qualifying_the_engine_does_not_qualify_what_it_already_produced,
                        t8_the_legitimate_path_produces_new_evidence_rather_than_relabelling_old,
                        t9_unqualified_use_cannot_book_even_on_forward_data], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
