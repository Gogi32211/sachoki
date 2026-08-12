"""What the browser receives is what was exposed, and the accounting says so in the same breath.

The transport rule from N0 was "no numeric leaves anywhere". It cannot be reused here as-is: a
results table legitimately carries `rank = 1`, `displayed_count = 5`, `selectable_count = 31`.
Those are deterministic metadata — nobody derives a verdict by subtracting two ranks. The ban
belongs to STATISTICAL values, which is where an operand would let the screen compute a delta
the guard on N0 exists to prevent.

    forbidden    effect.value · ci_low · ci_high · a raw estimate anywhere
    required     effect.display_value · uncertainty.display_value
    allowed      rank · counts · hashes

The other half of this file is the exposure contract. `rows` is the authorised set, not the
ranking: if thirty-one rows ship and the client renders five, thirty-one were exposed, and the
twenty-six nobody counted are one keystroke from being read.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parameter_surface as PS                                       # noqa: E402
import search_run as SR                                              # noqa: E402

ok = fail = 0

STAT_KEYS = ("effect", "uncertainty", "support")
FORBIDDEN_NUMERIC_PATHS = ("value", "estimate", "ci_low", "ci_high", "lower", "upper",
                           "point", "delta", "raw")


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def artifact(selectable=31, displayed=5, sort_key="effect", spec="SPEC-A"):
    return SR.rank_and_authorise(
        run_id="r001", session_id="s0001", family_id="F1", input_state_hash=spec,
        search_space_hash="space-aaa", selectable_count=selectable,
        displayed_count=displayed, evidence_hash="ev1", decision_hash="ds1", sort_key=sort_key)


def view(a, current="SPEC-A"):
    return SR.to_view(a, current, "ev1", "ds1")


# ── exposure ────────────────────────────────────────────────────────────────
def t1_the_payload_carries_the_authorised_set_and_not_the_ranking():
    """31 ranked, 5 authorised — and the wire holds five"""
    a = artifact()
    assert a.ranked_count == 31 and a.displayed_count == 5
    v = view(a)
    assert len(v.rows) == 5, len(v.rows)
    d = v.as_dict()
    assert len(d["rows"]) == 5
    ranked = set(a.ranked_claim_ids)
    shipped = {r["claim_id"] for r in d["rows"]}
    assert shipped < ranked, "the payload is a proper subset or nothing was withheld"
    assert len(ranked - shipped) == 26, "twenty-six claims must stay on the server"


def t2_the_view_cannot_be_built_with_more_rows_than_it_admits():
    """the invariant, asserted at construction rather than trusted"""
    a = artifact()
    v = view(a)
    try:
        SR.SearchRunView(**{**{k: getattr(v, k) for k in v.__dataclass_fields__},
                            "displayed_count": 3})
    except SR.ExposureAuthorisationError as e:
        assert "exposed claim whether or not it is drawn" in str(e)
        return
    raise AssertionError("a view claimed a display count its payload contradicts")


def t3_REPRODUCTION_shipping_the_ranking_would_expose_thirty_one():
    """the guard shown its defect: hand the client everything and let it slice

    This is what `rows.slice(0, 5)` on the client actually costs. The rows exist in the response,
    in memory and in devtools; the accounting believes five.
    """
    a = artifact()
    naive_payload_rows = len(a.ranked_claim_ids)
    honest_payload_rows = len(view(a).rows)
    assert naive_payload_rows == 31 and honest_payload_rows == 5, (
        "the reproduction failed: shipping the full ranking was supposed to put 31 claims on the "
        "wire while the counter said 5")


# ── the three counts ────────────────────────────────────────────────────────
def t4_displayed_exposed_and_selectable_are_three_numbers():
    a5, a10 = artifact(displayed=5), artifact(displayed=10)
    assert a5.selectable_count == a10.selectable_count == 31
    assert len(view(a5).rows) == 5 and len(view(a10).rows) == 10
    wide = artifact(selectable=37, displayed=10)
    assert wide.selectable_count == 37 and len(view(wide, "SPEC-A").rows) == 10


# ── staleness ───────────────────────────────────────────────────────────────
def t5_a_run_is_stale_when_the_specification_moved():
    a = artifact(spec="SPEC-A")
    assert view(a, "SPEC-A").freshness == SR.FRESH
    stale = view(a, "SPEC-B")
    assert stale.freshness == SR.STALE
    assert len(stale.rows) == 5, "a stale run stays readable; it does not lose its rows"


def t6_a_stale_run_may_be_read_and_not_promoted():
    """two different rights, and collapsing them loses the history"""
    SR.assert_promotable(view(artifact(), "SPEC-A"))
    try:
        SR.assert_promotable(view(artifact(), "SPEC-B"))
    except SR.StaleSearchRunError as e:
        assert "can still be read" in str(e)
        return
    raise AssertionError("a verdict was attached to a specification no longer on screen")


# ── the transport rule, narrower than N0 on purpose ─────────────────────────
def _leaves(obj, path=""):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out += _leaves(v, f"{path}.{k}")
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            out += _leaves(v, f"{path}[{i}]")
    elif isinstance(obj, bool):
        pass
    elif isinstance(obj, (int, float)):
        out.append((path, obj))
    return out


def t7_no_statistical_value_crosses_as_a_number():
    """the ban is on operands, not on counters"""
    d = view(artifact()).as_dict()
    offenders = []
    for row in d["rows"]:
        for key in STAT_KEYS:
            cell = row[key]
            nums = _leaves(cell)
            if nums:
                offenders.append((key, nums))
            for bad in FORBIDDEN_NUMERIC_PATHS:
                if bad in cell:
                    offenders.append((key, f"carries {bad!r}"))
    assert not offenders, f"a statistical operand crossed the wire: {offenders}"


def t8_the_counters_are_still_numbers_because_they_are_metadata():
    """reusing N0's blanket rule here would have been cargo-culting it"""
    d = view(artifact()).as_dict()
    assert isinstance(d["selectable_count"], int) and d["selectable_count"] == 31
    assert isinstance(d["displayed_count"], int) and d["displayed_count"] == 5
    assert all(isinstance(r["rank"], int) for r in d["rows"])
    assert [r["rank"] for r in d["rows"]] == [1, 2, 3, 4, 5]


def t9_every_statistical_cell_is_text_with_a_passport():
    d = view(artifact()).as_dict()
    for row in d["rows"]:
        for key in STAT_KEYS:
            cell = row[key]
            assert isinstance(cell["display_value"], str) and cell["display_value"], (key, cell)
            assert cell["inspector_ref"], (key, cell)
            assert cell["semantic_type"] in ("INFERENTIAL", "DESCRIPTIVE", "DETERMINISTIC"), cell
        assert row["evidence_claim_hash"] and row["decision_spec_hash"], row


def t10_the_fixture_says_it_is_a_fixture():
    """a screenshot of this table must not be mistakable for a finding"""
    d = view(artifact()).as_dict()
    assert d["data_provenance"] == SR.SYNTHETIC_FIXTURE, d["data_provenance"]


# ── ranking belongs to the server ───────────────────────────────────────────
def t11_the_sort_key_changes_which_rows_are_authorised():
    """which is exactly why an outcome sort is a selection path and not a view"""
    by_effect = view(artifact(sort_key="effect"))
    by_ticker = view(artifact(sort_key="ticker"))
    a_ids = [r.claim_id for r in by_effect.rows]
    b_ids = [r.claim_id for r in by_ticker.rows]
    assert a_ids != b_ids, "the sort key did not change the authorised set"
    assert set(a_ids) != set(b_ids), (
        "re-ranking changed only the order of the same five; on a 31-deep list a different key "
        "must be able to authorise different claims, or the selection-path rule has no teeth")


def t12_the_display_policy_is_recorded_on_the_run():
    a = artifact(displayed=7, sort_key="pf")
    assert a.display_policy == "top_7_by_pf", a.display_policy
    assert "pf" in a.ranking_policy_hash


print("=" * 100, flush=True)
print("  SEARCH RUN — the payload IS the exposure", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_payload_carries_the_authorised_set_and_not_the_ranking,
                        t2_the_view_cannot_be_built_with_more_rows_than_it_admits,
                        t3_REPRODUCTION_shipping_the_ranking_would_expose_thirty_one,
                        t4_displayed_exposed_and_selectable_are_three_numbers,
                        t5_a_run_is_stale_when_the_specification_moved,
                        t6_a_stale_run_may_be_read_and_not_promoted,
                        t7_no_statistical_value_crosses_as_a_number,
                        t8_the_counters_are_still_numbers_because_they_are_metadata,
                        t9_every_statistical_cell_is_text_with_a_passport,
                        t10_the_fixture_says_it_is_a_fixture,
                        t11_the_sort_key_changes_which_rows_are_authorised,
                        t12_the_display_policy_is_recorded_on_the_run], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
