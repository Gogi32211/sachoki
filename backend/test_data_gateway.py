"""The gateway, tested by trying to get around it.

The previous milestone could say UNKNOWN when it knew it did not know. The case it could not see
is the one this suite is built around, and `E` is the acceptance statement:

    recorded A · recorded B · unrecorded C

A completely missing footprint is loud — `UNKNOWN` catches it. Partial false completeness is
silent: the ledger holds a tidy history of A and B, nothing looks absent, and the verdict comes
back CLEAN while C has already read the validation window. So the tests that matter are not the
ones where the accounting is empty; they are the ones where it looks full.

Two axes, deliberately not folded together:

    contamination   was this window read
    completeness    can that question be answered at all

`ConfirmatoryEligible ⇒ AccessCompleteness = COMPLETE`, so an unobserved read path removes
CLEAN and FORWARD from the table entirely rather than being weighed against them.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import research_store as RS                                          # noqa: E402
import data_gateway as GW                                            # noqa: E402
from data_access import CATALOG, DataAccessSpec, VALIDATION          # noqa: E402
from evidence_boundary import CONTAMINATED, FORWARD, freeze_boundary  # noqa: E402
from research_family import ResearchFamily                           # noqa: E402
from research_session import ResearchSession                         # noqa: E402

ok = fail = 0
TMP = tempfile.mkdtemp(prefix="gateway_")
CUTOFF = "2026-08-11"
CATALOG.register("bars_1d", lambda: ("snap-gw-0001", CUTOFF))
CATALOG.register("opportunities", lambda: ("snap-opp-0001", CUTOFF))

BARS_PATH = os.path.join(TMP, "studio_analytics.duckdb")
OPP_PATH = os.path.join(TMP, "opportunities.parquet")
for _p in (BARS_PATH, OPP_PATH):
    with open(_p, "w") as f:
        f.write("not a real database; the guard defends the PATH, not the format\n")

_READS: list = []


def _reader(path, start, end, columns):
    """Stands in for DuckDB/parquet. What matters is that it is reachable only from inside."""
    _READS.append((os.path.basename(path), start, end))
    return [{"date": start}], 1


GW.REGISTRY.register(GW.SourceRegistration(
    source_id="bars_1d", path=BARS_PATH, reader=_reader, universe="russell"))
GW.REGISTRY.register(GW.SourceRegistration(
    source_id="opportunities", path=OPP_PATH, reader=_reader, universe="russell",
    derived_from=("bars_1d",), contains_outcome_derived_fields=True,
    artifact_version="opp-v3"))
GW.install_guards()


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def session(name: str, start="2024-01-01", end="2025-12-31"):
    L = RS.DurableLedger(os.path.join(TMP, f"{name}.jsonl"))
    s = ResearchSession(name, code_hash="gw-test", store=L, family_id=f"F-{name}")
    s.access_spec = DataAccessSpec("bars_1d", "russell", start, end).as_dict()
    s.start_exploration()
    return s, L


def execute(s, execution_id="x1", sources=("bars_1d",)):
    cap = GW.capability_for(s, execution_id, sources)
    return GW.ExecutionContext(s, cap, code_hash="study@v1")


def expose(s, claim_hash="C1"):
    from research_session import RESULT_EXPOSED
    s._append(RESULT_EXPOSED, claim_hash=claim_hash)


def boundary(start, end):
    return freeze_boundary(
        DataAccessSpec("bars_1d", "russell", "2021-01-01", "2023-12-31"),
        DataAccessSpec("bars_1d", "russell", start, end, purpose=VALIDATION),
        now="2026-08-12T12:00:00", catalog=CATALOG)


def register(L, sid, fam, claim="C1"):
    rows = L.read_session(sid)
    last = rows[-1]
    L.append(sid, fam, "SESSION_FROZEN", event_id=last.event_id + 1,
             prior_state_hash=last.new_state_hash, new_state_hash=f"{sid}-frozen",
             claim_hash=claim, state="REGISTERED",
             payload={"space_id": "combolab_v2", "size": 31, "hash": "3600ae3dd52a25e6"})


# ── A · the legal path ──────────────────────────────────────────────────────
def tA_a_gateway_read_produces_a_footprint_and_a_complete_receipt():
    """the happy path, stated as three separate facts rather than one"""
    s, L = session("A")
    with execute(s) as ex:
        ex.open("bars_1d").read("2024-01-01", "2024-06-30")
    expose(s)

    st, why = GW.access_completeness(L.read_all())
    assert st == GW.COMPLETE, (st, why)
    receipts = [e for e in L.read_all() if e.event_type == "EXECUTION_RECEIPT"]
    assert len(receipts) == 1, receipts
    r = receipts[0].payload["receipt"]
    assert r["complete"] is True and r["read_count"] == 1, r
    assert r["guarantee"] == GW.ENFORCED_IN_PROCESS, \
        "the receipt must say which guarantee it was produced under, not imply the stronger one"
    assert len(r["footprint_hashes"]) == 1, r


# ── B · the zero read, which is not the same as no instrumentation ──────────
def tB_an_execution_that_read_nothing_is_COMPLETE_not_UNKNOWN():
    """read_count=0 with a receipt is a positive statement; without one it is a gap"""
    s, L = session("B")
    with execute(s):
        pass
    st, why = GW.access_completeness(L.read_all())
    assert st == GW.COMPLETE, (st, why)
    r = [e for e in L.read_all() if e.event_type == "EXECUTION_RECEIPT"][0].payload["receipt"]
    assert r["read_count"] == 0 and r["complete"] is True, r


# ── C, D · the direct paths ─────────────────────────────────────────────────
def tC_direct_duckdb_is_refused_and_recorded():
    s, L = session("C")
    try:
        with execute(s):
            import duckdb
            duckdb.connect(BARS_PATH, read_only=True)
    except GW.DirectDataAccessError as e:
        assert "reachable only through the gateway" in str(e)
        st, why = GW.access_completeness(L.read_all())
        assert st == GW.VIOLATED, (st, why)
        return
    raise AssertionError("research execution opened the protected database directly")


def tD_direct_parquet_is_refused_and_recorded():
    s, L = session("D")
    try:
        with execute(s, sources=("opportunities",)):
            import pandas
            pandas.read_parquet(OPP_PATH)
    except GW.DirectDataAccessError:
        assert GW.access_completeness(L.read_all())[0] == GW.VIOLATED
        return
    raise AssertionError("research execution read the protected artifact directly")


# ── E · THE ACCEPTANCE STATEMENT ────────────────────────────────────────────
def tE_a_partial_bypass_invalidates_the_whole_execution():
    """recorded A, recorded B, unrecorded C — and the answer is not CLEAN on A and B

    This is the case UNKNOWN cannot see. Two legitimate reads leave a tidy history with no gaps
    in it, so an accounting built only on 'is anything missing' concludes everything is present.
    """
    s, L = session("E")
    try:
        with execute(s) as ex:
            h = ex.open("bars_1d")
            h.read("2024-01-01", "2024-06-30")          # A, recorded
            h.read("2024-07-01", "2024-12-31")          # B, recorded
            import duckdb
            duckdb.connect(BARS_PATH, read_only=True)   # C, around the back
    except GW.DirectDataAccessError:
        pass
    expose(s)
    register(L, "E", "F-E")

    events = L.read_all()
    assert len([e for e in events if e.event_type == "DATA_ACCESSED"]) >= 1, \
        "A and B really were recorded, so the history genuinely looks full"

    st, _ = GW.access_completeness(events)
    assert st == GW.VIOLATED, f"a partial bypass read as {st}"

    v = ResearchFamily("F-E", events).confirmatory(boundary("2026-09-01", "2026-12-31"))
    assert v["eligible"] is False, f"a study with an unobserved read path was eligible: {v}"
    assert v["status"] == "ACCESS_VIOLATED", v
    assert v["access_completeness"] == GW.VIOLATED, v


def tE2_REPRODUCTION_completeness_ignored_would_certify_the_bypass():
    """the guard shown its defect: drop the completeness gate and E comes back FORWARD"""
    from evidence_boundary import ExposureRegistry, confirmatory_verdict
    s, L = session("E2")
    try:
        with execute(s) as ex:
            ex.open("bars_1d").read("2024-01-01", "2024-06-30")
            import duckdb
            duckdb.connect(BARS_PATH, read_only=True)
    except GW.DirectDataAccessError:
        pass
    expose(s)
    events = L.read_all()

    blind = confirmatory_verdict(registered=True, boundary=boundary("2026-09-01", "2026-12-31"),
                                 registry=ExposureRegistry.from_events(events))
    assert blind["eligible"] is True and blind["status"] == FORWARD, (
        "the reproduction failed to reproduce: without the completeness gate this bypass was "
        "supposed to be certified, and if it is not, tE is not testing what it claims to")

    gated = confirmatory_verdict(
        registered=True, boundary=boundary("2026-09-01", "2026-12-31"),
        registry=ExposureRegistry.from_events(events),
        completeness=GW.access_completeness(events))
    assert gated["eligible"] is False, "the gate did not catch what the blind version certified"


# ── F · sources nobody declared ─────────────────────────────────────────────
def tF_an_unregistered_source_cannot_be_read():
    s, _ = session("F")
    with execute(s, sources=("bars_1d", "mystery")) as ex:
        try:
            ex.open("mystery")
        except GW.UnregisteredSourceAccessError as e:
            assert "no declared lineage" in str(e)
            return
    raise AssertionError("an undeclared source was served")


def tF2_a_capability_only_opens_what_it_authorises():
    s, _ = session("F2")
    with execute(s, sources=("bars_1d",)) as ex:
        try:
            ex.open("opportunities")
        except GW.CapabilityError as e:
            assert "does not authorise" in str(e)
            return
    raise AssertionError("a capability opened a source it did not name")


# ── G · derived artifacts ───────────────────────────────────────────────────
def tG_reading_a_derived_artifact_contaminates_its_sources():
    """opportunities.parquet for 2026 IS bars for 2026, one materialisation later"""
    s, L = session("G", start="2026-01-01", end="2026-06-30")
    with execute(s, execution_id="g1", sources=("opportunities",)) as ex:
        ex.open("opportunities").read("2026-01-01", "2026-06-30")
    expose(s)
    register(L, "G", "F-G")

    events = L.read_all()
    fps = [e.payload["footprint"] for e in events if e.event_type == "DATA_ACCESSED"]
    sources = {f["source_id"] for f in fps}
    assert sources == {"opportunities", "bars_1d"}, \
        f"the artifact's lineage was not propagated: {sources}"

    v = ResearchFamily("F-G", events).confirmatory(boundary("2026-01-01", "2026-06-30"))
    assert v["status"] == CONTAMINATED, \
        f"a derived artifact laundered a raw-source read: {v}"


def tG2_REPRODUCTION_ignoring_lineage_would_leave_bars_clean():
    """the guard shown its defect: record only the artifact and the raw window looks untouched"""
    s, L = session("G2", start="2026-01-01", end="2026-06-30")
    with execute(s, execution_id="g2", sources=("opportunities",)) as ex:
        ex.open("opportunities").read("2026-01-01", "2026-06-30")
    expose(s)
    fps = [e.payload["footprint"] for e in L.read_all() if e.event_type == "DATA_ACCESSED"]
    artifact_only = [f for f in fps if f["source_id"] == "opportunities"]
    assert artifact_only, "no artifact footprint at all, so the reproduction proves nothing"
    assert not any(f["source_id"] == "bars_1d" for f in artifact_only), (
        "the artifact-only footprints must NOT mention bars_1d, or tG would pass even without "
        "lineage propagation")


# ── H, I · persistence and the missing receipt ──────────────────────────────
def tH_a_receipt_survives_a_restart_and_a_retry_does_not_double_it():
    s, L = session("H")
    with execute(s, execution_id="h1") as ex:
        ex.open("bars_1d").read("2024-01-01", "2024-03-31")
    before = [e.payload["receipt"]["receipt_hash"] for e in L.read_all()
              if e.event_type == "EXECUTION_RECEIPT"]

    L2 = RS.DurableLedger(os.path.join(TMP, "H.jsonl"))
    after = [e.payload["receipt"]["receipt_hash"] for e in L2.read_all()
             if e.event_type == "EXECUTION_RECEIPT"]
    assert before == after and len(after) == 1, (before, after)
    assert GW.access_completeness(L2.read_all())[0] == GW.COMPLETE

    # closing twice returns the same receipt rather than issuing a second
    s2 = ResearchSession.restore("H", L2)
    cap = GW.capability_for(s2, "h1", ("bars_1d",))
    ctx = GW.ExecutionContext(s2, cap, code_hash="study@v1")
    r1, r2 = ctx.close(), ctx.close()
    assert r1.receipt_hash == r2.receipt_hash


def tI_footprints_without_a_receipt_are_UNKNOWN():
    """the false confidence this whole milestone is about: 'but we have several footprints'"""
    s, L = session("I")
    ex = execute(s, execution_id="i1")
    ex.__enter__()
    ex.open("bars_1d").read("2024-01-01", "2024-06-30")
    ex.open("bars_1d").read("2024-07-01", "2024-12-31")
    GW._STATE.execution = None          # the process dies before close(); no receipt is written
    expose(s)
    register(L, "I", "F-I")

    events = L.read_all()
    assert not [e for e in events if e.event_type == "EXECUTION_RECEIPT"]
    st, why = GW.access_completeness(events)
    assert st == GW.UNKNOWN, f"an unattested execution read as {st}"
    assert "do not prove that everything read was recorded" in why

    v = ResearchFamily("F-I", events).confirmatory(boundary("2026-09-01", "2026-12-31"))
    assert v["eligible"] is False and v["status"] == "ACCESS_UNKNOWN", v


# ── the honest name of the guarantee ────────────────────────────────────────
def tJ_the_guarantee_is_named_as_in_process_not_isolated():
    """two words that are not synonyms, and the receipt must not blur them"""
    assert GW.GUARANTEE == GW.ENFORCED_IN_PROCESS
    assert GW.ISOLATED != GW.ENFORCED_IN_PROCESS
    s, L = session("J")
    with execute(s) as ex:
        ex.open("bars_1d").read("2024-01-01", "2024-01-31")
    r = [e for e in L.read_all() if e.event_type == "EXECUTION_RECEIPT"][0].payload["receipt"]
    assert r["guarantee"] == GW.ENFORCED_IN_PROCESS, r


def tK_the_guard_is_scoped_to_executions():
    """outside a study the application reads its own databases, and must keep being able to"""
    import duckdb
    assert GW._active() is None
    try:
        duckdb.connect(BARS_PATH, read_only=True)
    except GW.DirectDataAccessError:
        raise AssertionError("the guard fired outside an execution and broke the application")
    except Exception:                                                # noqa: BLE001
        pass          # not a real database; what matters is that the GUARD did not object


print("=" * 104, flush=True)
print("  MANDATORY DATA GATEWAY — tested by trying to get around it", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([tA_a_gateway_read_produces_a_footprint_and_a_complete_receipt,
                        tB_an_execution_that_read_nothing_is_COMPLETE_not_UNKNOWN,
                        tC_direct_duckdb_is_refused_and_recorded,
                        tD_direct_parquet_is_refused_and_recorded,
                        tE_a_partial_bypass_invalidates_the_whole_execution,
                        tE2_REPRODUCTION_completeness_ignored_would_certify_the_bypass,
                        tF_an_unregistered_source_cannot_be_read,
                        tF2_a_capability_only_opens_what_it_authorises,
                        tG_reading_a_derived_artifact_contaminates_its_sources,
                        tG2_REPRODUCTION_ignoring_lineage_would_leave_bars_clean,
                        tH_a_receipt_survives_a_restart_and_a_retry_does_not_double_it,
                        tI_footprints_without_a_receipt_are_UNKNOWN,
                        tJ_the_guarantee_is_named_as_in_process_not_isolated,
                        tK_the_guard_is_scoped_to_executions], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
GW.uninstall_guards()
shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if fail else 0)
