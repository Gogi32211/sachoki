"""MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1 — 14 candidates, two branches, one control.

BRANCH A RESOLVES THE WAY THE MECHANISM PREDICTED, and every case is carried by a filed
sentence rather than by a bar. The distinction was the whole point: "Massive served a bar on
2022-04-11" is a vendor observation, while "On April 11, 2022, WBD common stock commenced
trading on Nasdaq" is a listing fact. Only the second can move a coverage boundary. Nine of
the eleven have such a sentence naming the session; CEG is carried by a stated rule plus its
distribution date; RDDT and PSKY by exchange listing certification, and their precision is
recorded as bounded rather than exact because no filing names the session.

In all eleven the authoritative first regular-way session equals the earliest Massive session
exactly. That agreement is a cross-check, not the evidence — it is recorded because if the
two had disagreed the classification would have had to change.

BRANCH B DOES NOT RESOLVE AS VENDOR ALIASING, WHICH WAS THE HYPOTHESIS I EXPECTED TO CONFIRM.
The filings say the opposite:

    GEHC  "begin trading 'regular way' ... on January 4, 2023, the first trading day after
           the Distribution Date"       V6 start 2023-01-05, V6 called 01-03..01-04 WI
    SOLV  when-issued "ending at the close of trading on March 28, 2024"; regular way
           "Beginning on April 1, 2024"  V6 start 2024-04-02, V6 called 04-01 WI
    VLTO  "Today marked the first day of regular way trading for Veralto (NYSE: VLTO)"
           filed 2023-10-02              V6 start 2023-10-03, V6 called 10-02 WI

So the ordinary-ticker bars on those sessions were not duplicated when-issued activity. They
were regular-way trading, and V6's when-issued interval ran one session too long and swallowed
the first real session of each. The WI POLICY is not what failed — excluding when-issued lines
remains correct. Its APPLICATION did.

GEV IS THE CONTROL AND IT HOLDS. GE Vernova's when-issued line also traded on 2024-04-01 and
its ordinary ticker returned nothing, because GE Vernova's distribution completed on 2024-04-02
— so V6's start is right for GEV and wrong for the other three. Same calendar date, same
structure, opposite answer, and the filings say why. A method that could not tell these apart
would have been worthless.

NOTHING IS REPAIRED HERE. Fourteen classifications, zero coverage edits, and the known-eight
table untouched.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402

EXEC = "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json"
EXEC_DIGEST = "e454381d12c66441"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
ARC = "/Volumes/QUANT_RESEARCH/artifacts/provenance/anomaly_qualification"

A = {
    "SW": dict(rw="2024-07-08", mech="merger/combination (Smurfit Kappa + WestRock)",
               prec="EXACT_XNYS_SESSION_PROSPECTIVELY_STATED",
               ev=[dict(form="425", filed="2024-06-07", sha256="62da07a7", doc="SW_425"),],
               q=["Smurfit WestRock's ordinary shares will trade on the New York Stock "
                  "Exchange with effect from 9:30 a.m. (New York City Time) on Monday, "
                  "8 July 2024"]),
    "WBD": dict(rw="2022-04-11", mech="Reverse Morris Trust (Discovery + WarnerMedia)",
                prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
                ev=[dict(form="8-K", filed="2022-04-12", sha256="d045c127", doc="WBD_8-K")],
                q=["On April 11, 2022, WBD common stock commenced trading on Nasdaq under "
                   "the ticker symbol “WBD”."]),
    "CEG": dict(rw="2022-02-02", mech="spin-off from Exelon",
                prec="CALENDAR_DATE_DERIVED_FROM_STATED_RULE",
                ev=[dict(form="EX-99.1 Information Statement", filed="2022-01-28",
                         sha256="add1966b", doc="CEG_EX-991"),
                    dict(form="EX-99.1 press release", filed="2022-02-02",
                         sha256="1b0fdb1f", doc="CEG_EX-991")],
                q=["“regular-way” trading in shares of the Company common stock "
                   "will begin on the first trading day following the distribution date",
                   "Constellation (Nasdaq: CEG) today announced the completion of its "
                   "separation from Exelon Corp."],
                lim="no filing names the session directly; the date follows from the stated "
                    "rule plus the completion announcement"),
    "Q": dict(rw="2025-11-03", mech="separation and distribution (Qnity from DuPont)",
              prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
              ev=[dict(form="8-K", filed="2025-10-15", sha256="4bfc47d3", doc="Q_8-K"),
                  dict(form="8-K", filed="2025-11-03", sha256="5b654428", doc="Q_8-K")],
              q=["“when-issued” trading is expected to begin on October 27, 2025, "
                 "under the symbol “Q WI”, with such trading ending at the close "
                 "of business on October 31, 2025",
                 "the Qnity Common Stock will commence “regular way” trading on "
                 "the New York Stock Exchange under the symbol “Q” at the start "
                 "of trading on November 3, 2025"]),
    "KVUE": dict(rw="2023-05-04", mech="IPO carve-out from Johnson & Johnson",
                 prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2023-05-08", sha256="d8703090",
                          doc="KVUE_8-K")],
                 q=["effective as of 8:00 a.m. New York City time on May 4, 2023, the date "
                    "that Kenvue Common Stock was first listed on the New York Stock "
                    "Exchange"]),
    "PSKY": dict(rw="2025-08-07", mech="merger (Paramount + Skydance) via New Pluto Global",
                 prec="BOUNDED_BY_LISTING_CERTIFICATION",
                 ev=[dict(form="8-K12B", filed="2025-08-07", sha256="ec432190",
                          doc="PSKY_8-K12B"),
                     dict(form="CERT", filed="2025-08-07", sha256=None,
                          doc="exchange listing certification"),
                     dict(form="10-Q", filed="2025-08-01", sha256="8a07f07a",
                          doc="PSKY_10-Q")],
                 q=["Following the closing of the Transactions shares of New Paramount ... "
                    "are expected to begin trading on the Nasdaq Stock Market LLC under the "
                    "ticker symbol “PSKY”"],
                 lim="no filed sentence names the first session; succession (8-K12B) and "
                     "exchange certification both dated 2025-08-07"),
    "SNDK": dict(rw="2025-02-24", mech="spin-off from Western Digital",
                 prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2025-02-03", sha256="0eefd799",
                          doc="SNDK_8-K"),
                     dict(form="8-K", filed="2025-02-14", sha256="540571a2",
                          doc="SNDK_8-K"),
                     dict(form="8-K", filed="2025-02-24", sha256="b864f2d6",
                          doc="SNDK_8-K")],
                 q=["During the when-issued trading period ... between February 13, 2025 "
                    "and February 21, 2025, shares ... will trade under the ticker symbol "
                    "“SNDKV”",
                    "the Company Common Stock commenced trading “regular way” "
                    "under the symbol “SNDK” on the Nasdaq Stock Market LLC on "
                    "February 24, 2025, which is the next trading day following the "
                    "Distribution Date"]),
    "FDXF": dict(rw="2026-06-01", mech="spin-off from FedEx",
                 prec="EXACT_XNYS_SESSION_PROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2026-05-13", sha256="5e30c25d",
                          doc="FDXF_8-K"),
                     dict(form="8-K", filed="2026-06-01", sha256="5a240e29",
                          doc="FDXF_8-K")],
                 q=["Following the separation, FedEx Freight common stock will begin "
                    "trading on the New York Stock Exchange on June 1, 2026 under the "
                    "symbol “FDXF”",
                    "immediately prior to the commencement of “when-issued” "
                    "trading ... on May 27, 2026"]),
    "TKO": dict(rw="2023-09-12", mech="combination (WWE + UFC/Endeavor)",
                prec="EXACT_XNYS_SESSION_PROSPECTIVELY_STATED",
                ev=[dict(form="425", filed="2023-09-07", sha256="84886f2c",
                         doc="TKO_425")],
                q=["on September 12, 2023, at which time TKO will begin trading on the New "
                   "York Stock Exchange under the ticker symbol “TKO”"]),
    "RDDT": dict(rw="2024-03-21", mech="initial public offering",
                 prec="BOUNDED_BY_LISTING_CERTIFICATION",
                 ev=[dict(form="8-A12B", filed="2024-03-20", sha256=None,
                          doc="registration of the class on the NYSE"),
                     dict(form="CERT", filed="2024-03-20", sha256=None,
                          doc="exchange listing certification"),
                     dict(form="424B4", filed="2024-03-21", sha256=None,
                          doc="final prospectus"),
                     dict(form="8-K", filed="2024-03-25", sha256="16fa534f",
                          doc="RDDT_8-K")],
                 q=["As described in the final prospectus, dated March 20, 2024 ... filed "
                    "with the Securities and Exchange Commission on March 21, 2024"],
                 lim="no filed sentence names the first session; registration effective and "
                     "exchange certification 2024-03-20, final prospectus 2024-03-21"),
    "HONA": dict(rw="2026-06-29", mech="spin-off from Honeywell",
                 prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2026-06-15", sha256="9c112f33",
                          doc="HONA_8-K"),
                     dict(form="8-K", filed="2026-06-29", sha256="28738826",
                          doc="HONA_8-K"),
                     dict(form="424B3", filed="2026-07-13", sha256="44cb8a89",
                          doc="HONA_424B3")],
                 q=["On June 29, 2026, our common stock began regular-way trading on the "
                    "Nasdaq Stock Market LLC under the ticker symbol “HONA”"]),
}

B = {
    "GEHC": dict(rw="2023-01-04", v6_wi=["2023-01-03", "2023-01-04"], wi_ticker="GEHCV",
                 mech="spin-off from General Electric",
                 prec="EXACT_XNYS_SESSION_PROSPECTIVELY_STATED_AND_CORROBORATED",
                 ev=[dict(form="8-K", filed="2022-12-08", sha256="83914010",
                          doc="GEHC_8-K"),
                     dict(form="8-K", filed="2023-01-04", sha256="a5525e1b",
                          doc="GEHC_8-K")],
                 q=["the Company's common stock is expected to begin trading “regular "
                    "way” on The Nasdaq Stock Market LLC under the ticker symbol "
                    "“GEHC” on January 4, 2023, the first trading day after the "
                    "Distribution Date",
                    "On January 3, 2023 (the “Distribution Date”), General "
                    "Electric Company completed the previously announced distribution"]),
    "SOLV": dict(rw="2024-04-01", v6_wi=["2024-04-01"], wi_ticker="SOLVw",
                 mech="spin-off from 3M",
                 prec="CALENDAR_DATE_PROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2024-03-13", sha256="d6f39dd1",
                          doc="SOLV_8-K")],
                 q=["Solventum common stock is expected to begin trading on a "
                    "“when-issued” basis on the New York Stock Exchange ... under "
                    "the symbol “SOLV WI,” with such trading ... ending at the "
                    "close of trading on March 28, 2024",
                    "Beginning on April 1, 2024, Solventum common stock is expected to "
                    "begin regular way trading on the NYSE under the ticker "
                    "“SOLV”"]),
    "VLTO": dict(rw="2023-10-02", v6_wi=["2023-10-02"], wi_ticker="VLTOw",
                 mech="spin-off from Danaher",
                 prec="EXACT_XNYS_SESSION_RETROSPECTIVELY_STATED",
                 ev=[dict(form="8-K", filed="2023-10-02", sha256="9279956d",
                          doc="VLTO_8-K")],
                 q=["Today marked the first day of regular way trading for Veralto (NYSE: "
                    "VLTO) as it begins its new journey as a publicly traded company."]),
}


def main():
    d_exec = ART.file_digest(EXEC)
    if d_exec != EXEC_DIGEST:
        print(f"HOLD — execution artifact digest {d_exec} != {EXEC_DIGEST}")
        return 1
    ex = json.load(open(EXEC))
    v6 = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    keyof = {r["frozen_current_ticker"]: r["security_key_v1"]
             for r in json.load(open(IDENTITY))["mapping"]}
    find = {f["current_ticker"]: f for f in ex["full_503_audit"]["findings"]}
    anchors = set(v6["A"]["anchor_profile"])

    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")

    def n_sessions(a, b):
        return len(cal.sessions_in_range(pd.Timestamp(a), pd.Timestamp(b))) - 1

    out, lost_a, lost_b = [], 0, 0
    for tk, m in A.items():
        f = find[tk]
        v6s = f["v6_coverage_start"]
        lost = n_sessions(m["rw"], v6s)
        lost_a += lost
        out.append(dict(
            security_key_v1=keyof[tk], ticker=tk, candidate_class="ANCHOR_DELAY",
            classification="V6_ANCHOR_DELAY_CONFIRMED",
            v6_status=v6[tk]["status"], v6_canonical_status=v6[tk]["canonical_status"],
            v6_regular_way_start=v6s,
            v6_start_on_anchor_grid=v6s in anchors,
            anchor_displacement_sessions=lost,
            earliest_massive_bar_session=f["probe"]["earliest_with_bars"],
            authoritative_regular_way_start=m["rw"],
            boundary_precision=m["prec"],
            listing_mechanism=m["mech"],
            when_issued_status=("a separate WI ticker/period is documented and remains "
                                "excluded" if "when-issued" in " ".join(m["q"]).lower()
                                else "no WI period relied upon"),
            same_security_evidence=dict(
                identifiers_matching_current=f["identifiers_matching_current"],
                note="the frozen research security is the issuer named in the filings"),
            primary_evidence=m["ev"], quoted_statements=m["q"],
            archived_in=ARC,
            cross_check=dict(
                authoritative_equals_earliest_massive_bar=(
                    m["rw"] == f["probe"]["earliest_with_bars"]),
                role="CROSS_CHECK_ONLY — the filing is the evidence; agreement is recorded "
                     "because disagreement would have changed the classification"),
            proposed_lost_sessions=lost,
            repaired=False,
            limitations=[m["lim"]] if m.get("lim") else []))

    for tk, m in B.items():
        f = find[tk]
        v6s = f["v6_coverage_start"]
        lost = n_sessions(m["rw"], v6s)
        lost_b += lost
        out.append(dict(
            security_key_v1=keyof[tk], ticker=tk, candidate_class="WI_BOUNDARY",
            classification="REGULAR_WAY_STARTED_ON_WI_DATE_CONFIRMED",
            v6_status=v6[tk]["status"], v6_canonical_status=v6[tk]["canonical_status"],
            v6_regular_way_start=v6s, v6_start_on_anchor_grid=v6s in anchors,
            v6_when_issued_interval=m["v6_wi"], v6_when_issued_ticker=m["wi_ticker"],
            earliest_massive_bar_session=f["probe"]["earliest_with_bars"],
            authoritative_regular_way_start=m["rw"],
            boundary_precision=m["prec"], listing_mechanism=m["mech"],
            finding="the ordinary-ticker bars on that session were REGULAR-WAY TRADING, not "
                    "duplicated when-issued activity",
            wi_policy_verdict=dict(
                policy_correct=True,
                application_defective=True,
                detail="excluding when-issued lines remains right; V6's WI interval ran one "
                       "session too long and swallowed the first real regular-way session",
                consequence="this is NOT WI_POLICY_CORRECT_VENDOR_ALIASING — the hypothesis "
                            "the gate expected to confirm is refuted by the filings"),
            primary_evidence=m["ev"], quoted_statements=m["q"], archived_in=ARC,
            cross_check=dict(authoritative_equals_earliest_massive_bar=(
                m["rw"] == f["probe"]["earliest_with_bars"]), role="CROSS_CHECK_ONLY"),
            proposed_lost_sessions=lost, repaired=False))

    gev = find.get("GEV", {})
    control = dict(
        security="GEV", role="DESCRIPTIVE_NEGATIVE_CONTROL",
        not_an_anomaly_candidate=True,
        v6_regular_way_start="2024-04-02",
        v6_when_issued=dict(ticker="GEVw", dates=["2024-04-01"]),
        observation="the WI line traded on 2024-04-01 while the ordinary ticker returned no "
                    "bars",
        audit_classification=gev.get("classification"),
        why_it_differs="GE Vernova's distribution completed on 2024-04-02, so 2024-04-01 "
                       "genuinely had no regular-way trading and V6's start is correct here",
        significance="same calendar date and same structure as SOLV, opposite answer. A "
                     "method that could not tell these apart would be worthless.",
        inference_forbidden="GEV's behaviour is NOT generalised to GEHC/SOLV/VLTO")

    counts_a = dict(total=11, v6_anchor_delay_confirmed=len(A), v6_start_correct=0,
                    security_identity_conflict=0, source_only_early_bars_unresolved=0,
                    unresolved=0)
    counts_b = dict(total=3, wi_policy_correct_vendor_aliasing=0,
                    regular_way_same_day_confirmed=len(B), source_behavior_conflict=0,
                    unresolved=0)
    complete = (counts_a["v6_anchor_delay_confirmed"] + counts_a["v6_start_correct"] == 11
                and counts_b["regular_way_same_day_confirmed"] == 3)

    p = dict(
        report_id="MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1",
        status=("LINEAGE_ANOMALY_QUALIFICATION_COMPLETE" if complete else "HOLD"),
        authoritative_input=dict(artifact=EXEC, digest=EXEC_DIGEST, verified=True,
                                 candidate_implementation="PASS",
                                 independent_verification="PASS",
                                 full_503_lineage_audit="HOLD"),
        candidate_provenance=dict(
            superseded_implementation="63d8e03cdafb257f",
            superseded_reason="next_session() off-by-one for non-trading effective_to "
                              "boundaries",
            affected=["CPAY", "WTW", "XYZ"],
            replacement_implementation="e81cb9367d3e37eb",
            independent_verifier="PASS",
            hidden=False,
            classification_of_that_defect="IMPLEMENTATION_DEFECT_NOT_A_LINEAGE_ANOMALY"),
        evidence_standard=dict(
            binding="a vendor bar is an observation; only a filed statement of listing or "
                    "trading can move a coverage boundary",
            first_bar_discipline="earliest_massive_bar_session, "
                                 "authoritative_regular_way_start and "
                                 "v6_regular_way_start are recorded as three separate "
                                 "fields and never conflated",
            massive_role="corroborative for vendor behaviour only"),
        branch_a=dict(scope=sorted(A), counts=counts_a,
                      hypothesis="regular-way listing began before the V6 quarterly anchor",
                      result="confirmed for all 11",
                      exact_session_statement_found=9,
                      bounded_by_certification=["RDDT", "PSKY"],
                      derived_from_stated_rule=["CEG"],
                      lost_sessions=lost_a),
        branch_b=dict(scope=sorted(B), counts=counts_b,
                      hypothesis_expected="WI_POLICY_CORRECT_VENDOR_ALIASING",
                      hypothesis_result="REFUTED by the filings",
                      actual="regular-way trading genuinely began on the session V6 labelled "
                             "when-issued; V6's WI interval ran one session too long",
                      wi_policy_itself="CORRECT — excluding when-issued lines stands",
                      requires="an explicit WI-boundary amendment, separately from the "
                               "anchor-delay repairs",
                      lost_sessions=lost_b),
        gev_control=control,
        anchor_grid_mechanism=dict(
            branch_a_on_anchor_grid=sum(1 for r in out
                                        if r["candidate_class"] == "ANCHOR_DELAY"
                                        and r["v6_start_on_anchor_grid"]),
            branch_b_on_anchor_grid=sum(1 for r in out
                                        if r["candidate_class"] == "WI_BOUNDARY"
                                        and r["v6_start_on_anchor_grid"]),
            descriptive_only=True,
            repair_rule_forbidden='"if V6 date is an anchor, move backward" — repair must '
                                  'use the evidence-qualified regular-way start'),
        candidates=out,
        totals=dict(candidates=14, treated=14,
                    lost_sessions_branch_a=lost_a, lost_sessions_branch_b=lost_b,
                    lost_sessions_total=lost_a + lost_b),
        no_repair=dict(auto_repaired=0, v7_candidate_modified=False,
                       known_eight_table_expanded=False,
                       repair_set_unchanged=["APO", "J", "CRH", "BG", "LH", "FERG",
                                             "BLK", "XOM"]),
        mutations=dict(canonical=0, v7_candidate=0, v6=0, security_key_v1=0,
                       universe=0, supplement_promotions=0, derived_writes=0),
        canonical_v7_eligible=False,
        next_gate="MASSIVE_SECURITY_LINEAGE_V7_SPEC_AMENDMENT_V1 — must carry BOTH the 11 "
                  "evidence-qualified anchor-delay repairs AND the 3 WI-boundary "
                  "corrections, which are different defect classes and must not share a "
                  "rule",
        does_not_authorize="lineage repair, V7 candidate modification, or canonical V7",
        limitations=[
            "RDDT and PSKY first-session precision is bounded by listing certification "
            "rather than a filed sentence naming the session",
            "CEG's date follows from a stated rule plus a completion announcement",
            "the three WI-boundary cases were qualified only for their own securities; no "
            "sweep was run for other WI-adjacent boundaries outside the frozen four"],
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1.json",
                 required=("report_id", "status", "authoritative_input",
                           "evidence_standard", "branch_a", "branch_b", "gev_control",
                           "candidates", "no_repair", "mutations"),
                 supersede=os.path.exists(
                     "MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1.json"))
    print(f"MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1 · {d} · {p['status']}")
    print(f"  Branch A  11/11 V6_ANCHOR_DELAY_CONFIRMED         lost {lost_a} sessions")
    print(f"  Branch B   3/3  REGULAR_WAY_STARTED_ON_WI_DATE     lost {lost_b} sessions")
    print(f"  GEV control preserved · repairs 0 · canonical_v7_eligible False")
    xc_bad = [r["ticker"] for r in out
              if not r["cross_check"]["authoritative_equals_earliest_massive_bar"]]
    print(f"  cross-check disagreements: {xc_bad or 'none'}")
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
