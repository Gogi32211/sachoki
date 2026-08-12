"""What counts as the same result seen twice.

The exposure counter grew from five to ten across two runs and the number was right, which is
not the same as the rule being right. `k_exposed 10` is correct if ten distinct things were
made available and wrong if the counter is keyed on something that changes for free.

The identity is `(evidence_claim_hash, decision_spec_hash)`. Not `run_id`, not `rank`, not a row
id — all three change between neighbouring specifications while the underlying result is the
same result. Real ranking makes this acute: adjacent top-N windows overlap heavily, and a
counter keyed on the run would charge full price for a list that moved by one.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_session import ClaimIdentity, ResearchSession                # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                                 # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def claim(tag: str, rule: str = "opportunity_level") -> ClaimIdentity:
    return ClaimIdentity("inc", "median_return", "20", "russell", f"cell-{tag}",
                         "rsi_14", "100", rule, "verdict_v2")


def deliver(session: ResearchSession, tags):
    """One run: every authorised row is executed and exposed, exactly as `search()` does."""
    for t in tags:
        c = claim(t)
        session.execute(c)
        session.expose(c)


def t1_overlapping_runs_charge_only_for_what_is_new():
    """A B C D E then C D E F G is seven claims and three revisits, not ten"""
    s = ResearchSession("OVER").start_exploration()
    deliver(s, ["A", "B", "C", "D", "E"])
    a = s.accounting()
    assert a["k_exposed"] == 5 and a["revisits"] == 0, a

    deliver(s, ["C", "D", "E", "F", "G"])
    b = s.accounting()
    assert b["k_exposed"] == 7, f"expected |{{A..G}}| = 7, got {b['k_exposed']}"
    assert b["revisits"] == 3, f"C, D and E were seen again: {b['revisits']}"


def t2_REPRODUCTION_keying_on_the_run_would_say_ten():
    """the guard shown its defect: count deliveries instead of identities"""
    runs = [["A", "B", "C", "D", "E"], ["C", "D", "E", "F", "G"]]
    naive = sum(len(r) for r in runs)
    honest = len({t for r in runs for t in r})
    assert naive == 10 and honest == 7, (
        "the reproduction failed: a run-keyed counter was supposed to overcharge by three")


def t3_the_same_run_twice_costs_nothing_new():
    """the identity rule, at its simplest: re-delivery is a revisit"""
    s = ResearchSession("SAME").start_exploration()
    deliver(s, ["A", "B", "C"])
    deliver(s, ["A", "B", "C"])
    a = s.accounting()
    assert a["k_exposed"] == 3, a
    assert a["revisits"] == 3, a


def t4_a_new_decision_rule_over_the_same_rows_is_new_exposure():
    """same evidence, different rule — the pair is what k charges for"""
    s = ResearchSession("RULE").start_exploration()
    for t in ("A", "B", "C"):
        c = claim(t)
        s.execute(c)
        s.expose(c)
    for t in ("A", "B", "C"):
        c = claim(t, rule="day_level")
        s.execute(c)
        s.expose(c)
    a = s.accounting()
    assert a["k_exposed"] == 6, a
    assert a["distinct_evidence_claims_exposed"] == 3, a
    assert a["distinct_decision_specs_exposed"] == 2, a
    assert a["revisits"] == 0, "a different decision rule is not a revisit"


def t5_rank_and_row_position_are_not_part_of_identity():
    """the same claim at a different rank is the same claim"""
    s = ResearchSession("RANK").start_exploration()
    deliver(s, ["A", "B", "C", "D", "E"])
    deliver(s, ["E", "D", "C", "B", "A"])          # identical set, reversed order
    a = s.accounting()
    assert a["k_exposed"] == 5, f"a reordering was charged as new exposure: {a['k_exposed']}"
    assert a["revisits"] == 5, a


def t6_a_wider_window_charges_only_the_extra_rows():
    """displayed 5 → 10 over one ranking: five new, five revisited"""
    s = ResearchSession("WIDE").start_exploration()
    top10 = [chr(ord("A") + i) for i in range(10)]
    deliver(s, top10[:5])
    deliver(s, top10)
    a = s.accounting()
    assert a["k_exposed"] == 10, a
    assert a["revisits"] == 5, f"the first five were re-delivered: {a['revisits']}"


print("=" * 100, flush=True)
print("  EXPOSURE IDENTITY — (evidence, decision), not the run", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_overlapping_runs_charge_only_for_what_is_new,
                        t2_REPRODUCTION_keying_on_the_run_would_say_ten,
                        t3_the_same_run_twice_costs_nothing_new,
                        t4_a_new_decision_rule_over_the_same_rows_is_new_exposure,
                        t5_rank_and_row_position_are_not_part_of_identity,
                        t6_a_wider_window_charges_only_the_extra_rows], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
