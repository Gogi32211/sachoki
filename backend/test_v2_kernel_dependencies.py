"""The inventory, frozen — so the extraction cannot lose an invisible dependency or gain one.

Step 3A exists because the danger in this move is the closure, not the formula. A function
lifted out of `main()` still runs when a dependency is supplied from a slightly different place;
it just computes something else, quietly. Freezing what each target reads TODAY means the
extraction commit has to account for every name, and any new one shows up as a diff rather than
as a bug.

The finding this file records: `boot` is not extractable verbatim, and `verdict` is. That was a
suspicion before the parser ran and is a fact after it.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2_kernel_dependency_inventory as INV                         # noqa: E402

ok = fail = 0
HERE = os.path.dirname(os.path.abspath(__file__))


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def frozen() -> dict:
    with open(INV.INVENTORY) as f:
        return json.load(f)


def t1_the_inventory_is_stable():
    """the same source produces the same inventory; a change is a diff, not a surprise"""
    assert INV.build()["inventory_hash"] == frozen()["inventory_hash"], (
        "the closure of an extraction target changed. That is either the extraction beginning "
        "or a dependency appearing; either way it is not silent.")


def t2_boot_reads_the_sealed_experiments_coordinates():
    """the finding: `boot` keys its RNG on (world, delta, rep) — needle coordinates

    A historical run has no planted δ, no synthetic world and no replicate index. Moving `boot`
    unchanged would carry the capability experiment's coordinate system into the historical
    engine, where those arguments have no meaning and would have to be invented.
    """
    d = frozen()["targets"]["boot"]
    assert set(d["capability_coordinates_read"]) == {"world", "delta", "rep"}, d
    assert d["extractable_verbatim"] is False
    # and it is the RNG that carries them, which is why the policy/material split already exists
    assert "S1R" in d["free_names"], d["free_names"]


def t3_verdict_is_outcome_agnostic_and_moves_as_it_is():
    """it reads a materiality threshold and support metadata, and nothing about the needle"""
    d = frozen()["targets"]["verdict"]
    assert d["capability_coordinates_read"] == [], d
    assert d["extractable_verbatim"] is True
    assert set(d["free_names"]) == {"DELTA_STAR", "decide", "meta"}, d["free_names"]
    # DELTA_STAR is the materiality threshold a verdict is read against, not the planted delta.
    # The names are one character apart, which is exactly why this is asserted rather than
    # assumed: `delta` would have been a leak and `DELTA_STAR` is not.
    assert "delta" not in d["free_names"]


def t4_frozen_layout_carries_no_experiment_state():
    d = frozen()["targets"]["Frozen"]
    assert d["capability_coordinates_read"] == [], d
    assert d["extractable_verbatim"] is True


def t5_the_cut_line_is_where_the_inventory_says_it_is():
    """3B may move what reads nothing capability-specific, and must parameterise the rest"""
    t = frozen()["targets"]
    verbatim = sorted(k for k, v in t.items() if v.get("extractable_verbatim"))
    needs_parameterising = sorted(k for k, v in t.items() if not v.get("extractable_verbatim"))
    assert verbatim == ["Frozen", "verdict"], verbatim
    assert needs_parameterising == ["boot"], needs_parameterising


def t6_REPRODUCTION_reading_the_source_would_have_missed_it():
    """the guard shown its defect: `boot`'s leak is one line deep inside a call

    It is not a parameter and not an obvious global. It arrives through
    `S1R.key_rng(world, delta, cell, rep, ...)`, four positional arguments into a helper, and a
    reviewer scanning for "does this function use delta" reads `delta_star` two lines later and
    moves on.
    """
    with open(os.path.join(HERE, "v2_sealed_run.py")) as f:
        src = f.read()
    assert "key_rng(world, delta, cell, rep" in src, (
        "the reproduction failed: the leak was supposed to be inside a call's arguments, which "
        "is what makes a static inventory worth more than a careful read")


print("=" * 100, flush=True)
print("  V2 KERNEL DEPENDENCIES — the inventory, frozen before the move", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_inventory_is_stable,
                        t2_boot_reads_the_sealed_experiments_coordinates,
                        t3_verdict_is_outcome_agnostic_and_moves_as_it_is,
                        t4_frozen_layout_carries_no_experiment_state,
                        t5_the_cut_line_is_where_the_inventory_says_it_is,
                        t6_REPRODUCTION_reading_the_source_would_have_missed_it], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
