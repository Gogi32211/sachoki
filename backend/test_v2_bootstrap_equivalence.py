"""Two claims about the bootstrap inversion, kept apart because they prove different things.

    A · BOOT_EXTRACTION_FULL_CELL_EQUIVALENCE
        every registered cell, δ=0. Six cells prove the mechanism; thirty-one prove the move was
        not correct only on whichever support geometry happened to be picked first.

    B · OUTCOME_PERTURBATION_EQUIVALENCE
        a different outcome vector. Two separate assertions live here and only the first is
        about old-vs-new:

            OLD@δ>0 == NEW@δ>0              the inversion holds on a perturbed outcome
            geometry(δ=0) == geometry(δ>0)  resampling does not depend on y

WHAT B DOES NOT REQUIRE, and requiring it would be a mistake: that the interval or the verdict
match between δ=0 and δ>0. They are computed on different outcomes and are entitled to differ —
demanding otherwise would be demanding that an injected effect have no effect. What must not
move between them is the outcome-INDEPENDENT machinery: support, weights, stream identity,
sampled-date geometry.

And B is only worth anything if the injection actually moved something. A perturbation that
changed no estimate would make every invariance assertion pass by doing nothing, so that is
checked first.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ok = fail = 0
HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "V2_BOOTSTRAP_EQUIVALENCE.json")
RUNGS = ("semantic_key", "stream_request_count", "sampled_geometry", "bootstrap_values",
         "intervals", "verdicts")


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def report() -> dict:
    with open(REPORT) as f:
        return json.load(f)


def t1_the_run_says_what_it_was_for():
    """a file of exact float bits from the v2 tract must not read as sealed evidence"""
    r = report()
    assert r["execution_purpose"] == "EXTRACTION_REGRESSION", r["execution_purpose"]
    assert r["rng_origin"] == "NON_SEALED_TEST_FIXTURE", r["rng_origin"]
    for part in ("full_cell_equivalence", "outcome_perturbation"):
        assert r[part]["world"] == 4242 and r[part]["rep"] == 0, r[part]


def t2_every_registered_cell_was_compared():
    """thirty-one, not a convenient six"""
    a = report()["full_cell_equivalence"]
    assert a["claim"] == "BOOT_EXTRACTION_FULL_CELL_EQUIVALENCE"
    assert len(a["cells"]) == 31, len(a["cells"])
    assert len(a["ladder"]) == 31
    assert a["delta"] == 0.0
    assert a["n_boot"] == 200


def t3_every_rung_of_the_ladder_is_exact_at_delta_zero():
    a = report()["full_cell_equivalence"]
    for rung in RUNGS:
        assert a["rungs"][rung] is True, rung
    assert a["all_identical"] is True
    for row in a["ladder"]:
        assert row["estimate_hex_old"] == row["estimate_hex_new"], row["cell"]
        assert row["stream_requests_old"] == row["stream_requests_new"] == 200, row["cell"]


def t4_the_support_geometry_really_did_vary_across_the_31():
    """otherwise breadth proved nothing: 31 copies of one shape is one shape

    Distinct sampled-date hashes mean each cell drew over a different date structure, which is
    the whole reason the full sweep is worth 316 seconds.
    """
    a = report()["full_cell_equivalence"]
    geoms = {row["sampled_geometry_hash"] for row in a["ladder"]}
    assert len(geoms) == 31, f"{len(geoms)} distinct geometries across 31 cells"
    ests = {row["estimate_hex_old"] for row in a["ladder"]}
    assert len(ests) > 20, f"only {len(ests)} distinct estimates; the cells barely differ"


def t5_the_inversion_holds_on_a_perturbed_outcome():
    b = report()["outcome_perturbation"]
    assert b["claim"] == "OUTCOME_PERTURBATION_EQUIVALENCE"
    assert b["delta"] == 1.5 and b["delta"] > 0
    for rung in RUNGS:
        assert b["rungs"][rung] is True, rung
    assert b["all_identical"] is True


def t6_the_injection_actually_moved_something():
    """an invariance test over a perturbation that perturbed nothing proves nothing"""
    b = report()["outcome_perturbation"]
    assert b["outcome_really_moved"] is True
    moved = [i for i in b["invariance"] if i["outcome_actually_changed"]]
    assert len(moved) == len(b["invariance"]), (
        f"only {len(moved)} of {len(b['invariance'])} cells saw the outcome change")


def t7_resampling_geometry_is_invariant_to_the_outcome():
    """the diagnostic invariant: if this ever fails, eligibility depends on y

    That would not be "the perturbation is awkward". It would mean the outcome reached the
    sampling path, which is a far more serious defect than anything this extraction could
    introduce.
    """
    b = report()["outcome_perturbation"]
    assert b["geometry_invariant"] is True
    for inv in b["invariance"]:
        assert inv["geometry_delta0"] == inv["geometry_perturbed"], inv["cell"]


def t8_the_perturbation_set_covers_more_than_one_shape():
    """high and low support, most and fewest strata, most and fewest dates"""
    b = report()["outcome_perturbation"]
    reasons = {row.get("selection_reason") for row in b["ladder"]}
    assert {"high_support", "low_support", "most_strata", "fewest_strata"} <= reasons, reasons
    assert len(b["cells"]) >= 6, b["cells"]


def t9_the_two_claims_are_not_merged():
    """breadth over cells and invariance over outcomes are different evidence"""
    r = report()
    assert r["full_cell_equivalence"]["claim"] != r["outcome_perturbation"]["claim"]
    assert r["full_cell_equivalence"]["delta"] != r["outcome_perturbation"]["delta"]


print("=" * 100, flush=True)
print("  3B.2 EQUIVALENCE — two claims, asserted separately", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_run_says_what_it_was_for,
                        t2_every_registered_cell_was_compared,
                        t3_every_rung_of_the_ladder_is_exact_at_delta_zero,
                        t4_the_support_geometry_really_did_vary_across_the_31,
                        t5_the_inversion_holds_on_a_perturbed_outcome,
                        t6_the_injection_actually_moved_something,
                        t7_resampling_geometry_is_invariant_to_the_outcome,
                        t8_the_perturbation_set_covers_more_than_one_shape,
                        t9_the_two_claims_are_not_merged], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
