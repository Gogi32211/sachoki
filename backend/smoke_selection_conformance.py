"""MASSIVE_1M_SMOKE_SELECTION_CONFORMANCE_V1 — the smoke stops before it runs.

The builder spec froze a smoke selection so it could not be chosen after seeing results.
That precommitment only means something if a selection which fails to instantiate its own
claimed case is treated as a spec defect rather than quietly swapped for a nearby date.

One case fails.

    2024-04-02   claimed: WHEN_ISSUED fixture (GEV/SOLV spin-offs)
                 actual:  both securities are REGULAR_WAY on that session

Their WHEN_ISSUED intervals are 2024-04-01 — one session earlier. The spec's own rationale
even says "WHEN_ISSUED interval ADJACENT", which is the admission: adjacency is not
instantiation, and a fixture that merely sits next to the state it claims to test would
have produced a WHEN_ISSUED_EXCLUDED assertion that passes without ever exercising a
when-issued row.

THE REMEDY IS NOT TO PICK 2024-04-01 NOW. That is exactly the substitution the gate
forbids: correcting a frozen test set at the moment of use, with the target already in
view, is how precommitment is lost. The remedy is an explicit builder-spec amendment with a
new digest, after which the smoke runs against the corrected frozen selection.

The other four cases were checked against real archived evidence and do instantiate what
they claim. They are recorded here so the amendment only has to move the one that is wrong.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

SPEC = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"


def main():
    spec = json.load(open(SPEC))
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}

    def state_on(tk, day):
        for i in (lin[tk].get("intervals") or []):
            if (i.get("effective_from") or "0000") <= day <= (i.get("effective_to")
                                                              or "9999"):
                return i["lineage_state"], i["massive_ticker_at_time"]
        return None, None

    cases = {
        "2024-05-15": dict(
            claim="normal full session; boundary bars; extended hours; sparse names",
            measured=dict(xnys_lattice=390, session="09:30-16:00",
                          aapl_regular=390, boundary_bars=1, extended_bars=267,
                          sparse_securities=343),
            instantiates=True),
        "2024-11-29": dict(
            claim="early close (210 slots); boundary bar at 13:00 ET",
            measured=dict(xnys_lattice=210, session="09:30-13:00",
                          aapl_regular=210, boundary_bars=1, extended_bars=143,
                          sparse_securities=298),
            instantiates=True),
        "2023-06-07": dict(
            claim="frozen rename-boundary UNOBSERVED case (FISV/FI)",
            measured=dict(xnys_lattice=390, fisv_raw_ingest_state="UNOBSERVED",
                          v6_state=list(state_on("FISV", "2023-06-07"))),
            instantiates=True),
        "2023-01-05": dict(
            claim="NOT_YET_REGULAR_WAY securities present",
            measured=dict(securities_not_yet_regular_way=18,
                          sample=["BG", "BLK", "CRH", "FDXF", "FERG", "GEV", "HONA",
                                  "KVUE"],
                          note="the spec's parenthetical named GEHC, which is actually "
                               "REGULAR_WAY on this date; the fixture CLAIM is that "
                               "NOT_YET securities are present, and 18 are"),
            instantiates=True),
        "2024-04-02": dict(
            claim="WHEN_ISSUED interval adjacent (GEV/SOLV spin-offs)",
            measured=dict(
                GEV=dict(state_on_2024_04_02=list(state_on("GEV", "2024-04-02")),
                         state_on_2024_04_01=list(state_on("GEV", "2024-04-01")),
                         when_issued_interval="GEVw 2024-04-01..2024-04-01"),
                SOLV=dict(state_on_2024_04_02=list(state_on("SOLV", "2024-04-02")),
                          state_on_2024_04_01=list(state_on("SOLV", "2024-04-01")),
                          when_issued_interval="SOLVw 2024-04-01..2024-04-01")),
            instantiates=False,
            failure="both securities are REGULAR_WAY on 2024-04-02; their WHEN_ISSUED "
                    "intervals are 2024-04-01",
            why_it_matters="a WHEN_ISSUED_EXCLUDED assertion would pass on this session "
                           "without ever exercising a when-issued row — the fixture would "
                           "be green and untested",
            spec_wording_admits_it="the frozen rationale says 'WHEN_ISSUED interval "
                                   "ADJACENT'; adjacency is not instantiation"),
    }

    failing = [d for d, c in cases.items() if not c["instantiates"]]
    p = dict(
        report_id="MASSIVE_1M_SMOKE_SELECTION_CONFORMANCE_V1",
        status="SMOKE_SELECTION_CONFORMANCE_HOLD" if failing else "PASS",
        spec=dict(artifact=SPEC, digest=ART.file_digest(SPEC),
                  frozen_selection=list(spec["smoke_plan"]["sessions"])),
        purpose="verify each frozen smoke case actually instantiates the semantic state it "
                "was selected to test, BEFORE the smoke runs",
        cases=cases,
        failing_cases=failing,
        remedy=dict(
            required="an explicit builder-spec amendment with a NEW digest, then re-run "
                     "the smoke against the corrected frozen selection",
            forbidden="substituting 2024-04-01 now, mid-gate, with the target already in "
                      "view — correcting a frozen test set at the moment of use is how "
                      "precommitment is lost",
            candidate_for_the_amendment="2024-04-01 is the session on which GEV and SOLV "
                                        "are actually WHEN_ISSUED; recorded as a "
                                        "CANDIDATE for the amendment to consider, not as "
                                        "an adopted substitution"),
        smoke_executed=False,
        scratch_written=False,
        production_build="HOLD",
        source_mutations=dict(raw_archive=0, lineage_v6=0, identity_amendment=0,
                              derived_semantics=0, boundary_amendment=0, snapshot=0,
                              note="this check is read-only"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_SMOKE_SELECTION_CONFORMANCE_V1.json",
                 required=("report_id", "status", "cases", "remedy"),
                 supersede=os.path.exists(
                     "MASSIVE_1M_SMOKE_SELECTION_CONFORMANCE_V1.json"))
    print(f"MASSIVE_1M_SMOKE_SELECTION_CONFORMANCE_V1 · {d} · {p['status']}")
    for day, c in cases.items():
        print(f"  {'OK  ' if c['instantiates'] else 'FAIL'} {day}  {c['claim'][:56]}")
    if failing:
        print(f"\n  failing: {failing}")
        print(f"  {cases[failing[0]]['failure']}")
    print(f"  smoke executed: {p['smoke_executed']} · scratch written: "
          f"{p['scratch_written']}")
    return 1 if failing else 0


if __name__ == "__main__":
    raise SystemExit(main())
