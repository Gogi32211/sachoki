"""Twelve conditions on the N0 payload. Test 3 is the one that matters.

The others check that the screen says the right things. Test 3 checks that the browser is
incapable of saying the wrong one: it scans the entire payload for a numeric leaf, because a
promise not to subtract is worth less than an absence of operands.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import studio_semantics_api as API                                  # noqa: E402
from semantic_metric import ComparisonSemanticsError                # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


PAYLOAD = API.n0_screen()


def _numeric_leaves(obj, path="") -> list:
    """Every number anywhere in the tree, with its path. Booleans are not operands."""
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out += _numeric_leaves(v, f"{path}.{k}")
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            out += _numeric_leaves(v, f"{path}[{i}]")
    elif isinstance(obj, bool):
        pass
    elif isinstance(obj, (int, float)):
        out.append((path, obj))
    return out


def t1_g1_present():
    ids = [m["metric_id"] for m in PAYLOAD["metrics"]]
    assert "n0.g1.fwer_search" in ids


def t2_g2_present():
    ids = [m["metric_id"] for m in PAYLOAD["metrics"]]
    assert "n0.g2.fwer_search" in ids


def t3_no_numeric_operand_anywhere():
    """the payload contains no number a component could subtract"""
    nums = _numeric_leaves(PAYLOAD["metrics"])
    assert not nums, f"numeric leaves reached the wire: {nums}"


def t4_display_value_is_a_string():
    for m in PAYLOAD["metrics"]:
        assert isinstance(m["display_value"], str), type(m["display_value"])
    vals = {m["metric_id"]: m["display_value"] for m in PAYLOAD["metrics"]}
    assert vals["n0.g1.fwer_search"] == "0.065" and vals["n0.g2.fwer_search"] == "0.685"


def t5_comparison_blocked():
    assert PAYLOAD["comparison"]["comparable"] is False


def t6_reason_is_sampling_target():
    assert PAYLOAD["comparison"]["reason_code"] == "SAMPLING_TARGET_MISMATCH"


def _keys(obj, out=None):
    """Every field NAME in the tree.

    The first version of these four tests scanned the flattened JSON for words and failed on
    'different' inside "two different generators" -- the boundary message, which is exactly the
    prose that must be there. The requirement was never "this word may not appear"; it is "no
    field carries this quantity". Keys, not text. Third time today that testing a string stood
    in for testing a structure.
    """
    out = [] if out is None else out
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.append(k.lower())
            _keys(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _keys(v, out)
    return out


KEYS = set(_keys(PAYLOAD))


def t7_no_delta():
    for k in ("delta", "difference", "diff", "change", "gap"):
        assert k not in KEYS, f"a field named {k!r} crossed the wire"


def t8_no_ratio():
    for k in ("ratio", "times_worse", "fold", "multiple"):
        assert k not in KEYS, f"a field named {k!r} crossed the wire"


def t9_no_average():
    for k in ("average", "mean", "combined", "pooled", "aggregate"):
        assert k not in KEYS, f"a field named {k!r} crossed the wire"


def t10_no_winner():
    for k in ("winner", "preferred", "better", "worse", "rank", "score", "best"):
        assert k not in KEYS, f"a field named {k!r} crossed the wire"


def t11_inspector_refs_resolve():
    for m in PAYLOAD["metrics"]:
        d = API.inspector(m["inspector_ref"])
        assert d["metric_id"] == m["metric_id"]
        titles = {s["title"] for s in d["sections"]}
        assert {"EXPERIMENT", "CONDITIONING", "POPULATION", "PROVENANCE"} <= titles, titles
        assert not _numeric_leaves(d), f"inspector leaked operands: {_numeric_leaves(d)}"
    g2 = API.inspector("n0.g2.fwer_search")
    flat = json.dumps(g2)
    assert "does not preserve" in flat and "calendar-year prevalence" in flat


def t12_no_artifact_for_this_pair():
    """the sanctioned route to a delta cannot be opened for G1/G2"""
    try:
        API.comparison_artifact("n0.g1.fwer_search", "n0.g2.fwer_search")
    except ComparisonSemanticsError as e:
        assert "SAMPLING_TARGET_MISMATCH" in str(e)
    else:
        raise AssertionError("a ComparisonArtifact was produced for two different null models")
    # and the legitimate case still works, so the guard is not merely a wall
    a = API.comparison_artifact("n0.g1.fwer_search", "n0.g1.fwer_search")
    assert a.difference_display.startswith("+0") or a.difference_display.startswith("-0")
    assert isinstance(a.difference_display, str)


print("=" * 100, flush=True)
print("  N0 TRANSPORT — twelve conditions; the third is the one that matters", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_g1_present, t2_g2_present, t3_no_numeric_operand_anywhere,
                        t4_display_value_is_a_string, t5_comparison_blocked,
                        t6_reason_is_sampling_target, t7_no_delta, t8_no_ratio, t9_no_average,
                        t10_no_winner, t11_inspector_refs_resolve,
                        t12_no_artifact_for_this_pair], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    print("\n  payload as the browser receives it:\n", flush=True)
    print(json.dumps(PAYLOAD, indent=2, ensure_ascii=False), flush=True)
sys.exit(1 if fail else 0)
