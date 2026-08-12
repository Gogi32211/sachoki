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


def t3_the_moved_verdict_reads_only_what_it_was_given():
    """after 3B.1 its closure reads `decide` and nothing else

    Before the move its free names were {DELTA_STAR, decide, meta}. Both of the others are now
    parameters, which is what "passing dependencies that were already read" means: the set
    shrank by exactly those two and gained nothing.
    """
    d = frozen()["targets"]["verdict"]
    assert d["file"] == "v2_kernel.py", d["file"]
    assert set(d["free_names"]) == {"decide"}, d["free_names"]
    assert d["params"] == ["e", "cell", "meta", "delta_star"], d["params"]
    assert d["capability_coordinates_read"] == [], d
    # `delta_star` is the materiality threshold, not the planted δ. The names are close enough
    # that the distinction is asserted rather than left to a reader.
    assert "delta" not in d["free_names"]


def t4_frozen_layout_carries_no_experiment_state():
    d = frozen()["targets"]["Frozen"]
    assert d["file"] == "v2_kernel.py"
    assert set(d["free_names"]) == {"E", "np", "support_hash"}, d["free_names"]
    assert d["capability_coordinates_read"] == [], d


def t5_the_expected_dependency_diff_and_nothing_else():
    """the diff was predicted before the move; anything extra stops the extraction

    Predicted: the kernel loses nothing it should keep and gains no capability coordinate, and
    `boot` is untouched. An unexpected name here means a dependency arrived or vanished during
    the move, and that has to be explained before a green oracle is worth reading.
    """
    t = frozen()["targets"]
    assert set(t) == {"boot", "verdict", "Frozen", "support_hash"}, sorted(t)
    kernel = {k: v for k, v in t.items() if v["file"] == "v2_kernel.py"}
    assert sorted(kernel) == ["Frozen", "support_hash", "verdict"], sorted(kernel)
    for name, d in kernel.items():
        assert d["capability_coordinates_read"] == [], (name, d)
        assert d["extractable_verbatim"] is True, name
    assert t["boot"]["file"] == "v2_sealed_run.py", "boot was moved; 3B.1 must not touch it"
    assert t["boot"]["extractable_verbatim"] is False


def t5b_the_sealed_closure_became_a_thin_client():
    """the call sites are unchanged; only what the wrapper reads changed, and to the predicted set"""
    import ast
    with open(os.path.join(HERE, "v2_sealed_run.py")) as f:
        tree = ast.parse(f.read())
    found = None
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef) and n.name == "verdict":
            found = n
    assert found is not None, "the sealed verdict wrapper disappeared"
    reads = {x.id for x in ast.walk(found) if isinstance(x, ast.Name)} - {"e", "cell"}
    assert reads == {"K", "meta", "DELTA_STAR"}, sorted(reads)


def t5c_the_kernel_and_the_old_module_are_the_same_object():
    """re-export, not a copy — two Frozen classes would be two engines"""
    import v2_kernel as K                                            # noqa: PLC0415
    import v2_decision_run as D                                      # noqa: PLC0415
    assert K.Frozen is D.Frozen
    assert K.support_hash is D.support_hash


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
                        t3_the_moved_verdict_reads_only_what_it_was_given,
                        t4_frozen_layout_carries_no_experiment_state,
                        t5_the_expected_dependency_diff_and_nothing_else,
                        t5b_the_sealed_closure_became_a_thin_client,
                        t5c_the_kernel_and_the_old_module_are_the_same_object,
                        t6_REPRODUCTION_reading_the_source_would_have_missed_it], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
