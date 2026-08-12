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


def t2_the_coordinates_moved_out_of_the_kernel_and_not_out_of_existence():
    """3B.2 · `boot` no longer reads world/delta/rep; the provider does

    Before the inversion the closure keyed its RNG on the capability experiment's coordinates.
    They have not been deleted — a historical run still must not have them, and the sealed run
    still must. They moved to the object whose whole purpose is to hold them.
    """
    t = frozen()["targets"]
    assert t["boot"]["capability_coordinates_read"] == [], t["boot"]
    assert "provider" in t["boot"]["free_names"], t["boot"]["free_names"]
    assert "S1R" not in t["boot"]["free_names"], "the closure still reaches the key function"

    kernel = t["bootstrap_cell"]
    assert kernel["file"] == "v2_kernel.py"
    assert kernel["capability_coordinates_read"] == [], kernel
    assert set(kernel["free_names"]) == {"Estimate", "np"}, kernel["free_names"]
    assert "rng_stream" in kernel["params"], kernel["params"]

    for name in ("open_stream", "semantic_key"):
        d = t[name]
        assert set(d["capability_coordinates_read"]) == {"world", "delta", "rep"}, (name, d)


def t2b_the_detector_counts_attribute_reads():
    """the guard shown its own defect: the provider looked spotless because it holds them as self.*

    The first version of the inventory read free NAMES only, so `self.world` was invisible and
    the provider — the one object that must carry the coordinates — reported none. A guard that
    cannot see the thing it was extended to watch is worse than no guard, because it reports
    clean.
    """
    d = frozen()["targets"]["open_stream"]
    assert "world" in d["attribute_reads"], d["attribute_reads"]
    assert set(d["free_names"]) == {"S1R"}, d["free_names"]


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

    Predicted for 3B.2, written before it was made:

        BOOT KERNEL      - world, - delta, - rep, + rng stream
        SEALED PROVIDER  + world, + delta, + rep, + the exact legacy key_rng call

    An unexpected name on either side means a dependency arrived or vanished during the
    inversion, and that has to be explained before a green ladder is worth reading.
    """
    t = frozen()["targets"]
    assert set(t) == {"boot", "verdict", "Frozen", "support_hash", "bootstrap_cell",
                      "open_stream", "semantic_key"}, sorted(t)
    kernel = {k: v for k, v in t.items() if v["file"] == "v2_kernel.py"}
    assert sorted(kernel) == ["Frozen", "bootstrap_cell", "support_hash", "verdict"], \
        sorted(kernel)
    for name, d in kernel.items():
        assert d["capability_coordinates_read"] == [], (name, d)
    provider = {k: v for k, v in t.items() if v["nested_in"] == "SealedBootstrapRNGProvider"}
    assert sorted(provider) == ["open_stream", "semantic_key"], sorted(provider)
    for name, d in provider.items():
        assert set(d["capability_coordinates_read"]) == {"world", "delta", "rep"}, (name, d)


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


def t6_the_legacy_key_call_survives_verbatim_inside_the_provider():
    """the arguments and their order are the thing being preserved, not the call site"""
    with open(os.path.join(HERE, "v2_sealed_run.py")) as f:
        src = f.read()
    assert 'S1R.key_rng(self.world, self.delta, cell, self.rep, "claim_bootstrap")' in src, (
        "the legacy key call changed shape. Its arguments and their order ARE the sealed RNG "
        "geometry; a tidier signature here would be a different experiment.")
    assert "def open_stream" in src and "def semantic_key" in src


def t7_the_kernel_never_learns_the_planted_delta():
    """`delta` and `delta_star` are close enough that this is asserted, not assumed"""
    with open(os.path.join(HERE, "v2_kernel.py")) as f:
        src = f.read()
    import ast
    tree = ast.parse(src)
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    for forbidden in ("world", "rep", "delta"):
        assert forbidden not in names and forbidden not in attrs, forbidden
    assert "delta_star" in names, "the decision kernel legitimately reads the materiality margin"


print("=" * 100, flush=True)
print("  V2 KERNEL DEPENDENCIES — the inventory, frozen before the move", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_inventory_is_stable,
                        t2_the_coordinates_moved_out_of_the_kernel_and_not_out_of_existence,
                        t2b_the_detector_counts_attribute_reads,
                        t3_the_moved_verdict_reads_only_what_it_was_given,
                        t4_frozen_layout_carries_no_experiment_state,
                        t5_the_expected_dependency_diff_and_nothing_else,
                        t5b_the_sealed_closure_became_a_thin_client,
                        t5c_the_kernel_and_the_old_module_are_the_same_object,
                        t6_the_legacy_key_call_survives_verbatim_inside_the_provider,
                        t7_the_kernel_never_learns_the_planted_delta], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
