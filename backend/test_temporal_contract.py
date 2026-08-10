"""Regression for the failure the temporal barrier actually found — all seven cases.

Case 8 pins the 253 real rows, so a future refactor cannot turn their ambiguity back into
silent accessibility. That is the one this suite exists for; the rest keep it honest.
"""
from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from temporal_contract import (DAY, INTRADAY, KNOWN, UNKNOWN,  # noqa: E402
                               TemporalContractError, TemporalSpec, assert_registry,
                               check_trade_table, may_use_as_feature)

ok = fail = 0
D = pd.Timestamp("2024-06-10")


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def t1_exit_before_entry_is_fatal():
    """exit < entry → INVALID, and no timestamp can repair it"""
    df = pd.DataFrame({"date_in": ["2024-06-10"], "date_out": ["2024-06-09"]})
    try:
        check_trade_table(df)
    except TemporalContractError as e:
        assert "strictly before" in str(e)
        return
    raise AssertionError("a corrupt row was accepted")


def t2_same_day_is_ambiguous_not_invalid():
    """exit == entry at day resolution → AMBIGUOUS; the trade survives"""
    df = pd.DataFrame({"date_in": ["2024-06-10", "2024-06-10"],
                       "date_out": ["2024-06-10", "2024-06-14"]})
    r = check_trade_table(df)
    assert r["corrupt"] == 0 and r["ambiguous"] == 1
    assert r["flag"] == "SAME_DAY_ORDER_AMBIGUOUS"
    assert r["mask"].tolist() == [True, False]


def t3_same_date_outcome_record_is_forbidden():
    """a day-resolution record dated on the decision day cannot be a feature"""
    s = TemporalSpec("probe", "pub_date", DAY, DAY, UNKNOWN, "feature")
    okk, why = may_use_as_feature(s, D, D)
    assert not okk and "FORBIDDEN_FOR_FEATURES" in why, why


def t4_next_day_is_invisible():
    """availability = decision + 1d → not available"""
    s = TemporalSpec("probe", "pub_date")
    okk, why = may_use_as_feature(s, D + pd.Timedelta(days=1), D)
    assert not okk and "not yet available" in why


def t5_previous_day_is_visible():
    """availability = decision − 1d → available"""
    s = TemporalSpec("probe", "pub_date")
    okk, why = may_use_as_feature(s, D - pd.Timedelta(days=1), D)
    assert okk and "strictly earlier" in why


def t6_same_date_with_known_phase_is_allowed():
    """same calendar date is fine once a real intraday ordering exists"""
    s = TemporalSpec("intraday", "pub_ts", INTRADAY, INTRADAY, KNOWN, "feature")
    okk, why = may_use_as_feature(s, D, D)
    assert okk and "phase ordering is known" in why


def t7_outcome_derived_field_cannot_anchor_a_feature():
    """the scenario the barrier surfaced: a source keyed on date_out"""
    for field in ("date_out", "ret", "mfe", "stop_hit"):
        try:
            TemporalSpec("greedy", field, role="feature")
        except TemporalContractError as e:
            assert "outcome-derived" in str(e)
            continue
        raise AssertionError(f"{field} was accepted as a feature anchor")
    # label space is exactly where these belong
    TemporalSpec("labels", "date_out", role="label")
    # and lineage is checked too, not only the anchor
    try:
        TemporalSpec("laundered", "pub_date", role="feature", field_lineage=("mae",))
    except TemporalContractError:
        return
    raise AssertionError("an outcome-derived field passed in through lineage")


def t8_the_253_real_rows():
    """the actual rows that failed the barrier stay valid and stay unusable as features"""
    import combo_lab as CL
    O, _, _ = CL.load_base(verbose=False)
    r = check_trade_table(O)
    assert r["corrupt"] == 0, "no exit precedes its entry in the real table"
    assert r["ambiguous"] == 253, f"expected the 253 known same-day rows, got {r['ambiguous']}"
    assert 0.0008 < r["ambiguous_pct"] < 0.0010
    s = TemporalSpec("exit_keyed", "pub_date", DAY, DAY, UNKNOWN, "feature")
    amb = O.loc[r["mask"]].head(20)
    for _, row in amb.iterrows():
        okk, _ = may_use_as_feature(s, row["date_out"], row["date_in"])
        assert not okk, f"{row['ticker']} {row['date_in']} became usable again"


def t9_registry_refuses_impossible_claims():
    """a day-resolution source may not claim to know intraday phase"""
    good = TemporalSpec("sec", "filed", DAY, DAY, UNKNOWN, "feature")
    assert_registry([good])
    try:
        assert_registry([TemporalSpec("liar", "filed", DAY, DAY, KNOWN, "feature")])
    except TemporalContractError as e:
        assert "not something a date can support" in str(e)
        return
    raise AssertionError("an impossible temporal claim was registered")


print("=" * 100, flush=True)
print("  TEMPORAL CONTRACT — regression on the failure the barrier found", flush=True)
print("=" * 100, flush=True)
for fn in (t1_exit_before_entry_is_fatal, t2_same_day_is_ambiguous_not_invalid,
           t3_same_date_outcome_record_is_forbidden, t4_next_day_is_invisible,
           t5_previous_day_is_visible, t6_same_date_with_known_phase_is_allowed,
           t7_outcome_derived_field_cannot_anchor_a_feature, t8_the_253_real_rows,
           t9_registry_refuses_impossible_claims):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
