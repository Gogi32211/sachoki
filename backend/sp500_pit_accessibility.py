"""SP500_PIT_CANDIDATE_ACCESSIBILITY_V1 — can we get it, not is it any good.

ACCESSIBILITY IS NOT EVIDENTIARY QUALITY, and the classifications here are deliberately
disjoint from qualification. A source we cannot currently reach is NOT_CURRENTLY_ACCESSIBLE
and stays UNASSESSED; it is not rejected, and nothing about its data has been judged. The
reverse holds too: a cheap API being easy to reach says nothing whatsoever about whether
its history is right.

WHAT WAS ACTUALLY CHECKED. Credentials present on this machine (by NAME only — no value was
read, printed or stored), and public logistics from provider pages: delivery mechanism,
authentication, subscription requirement, stated price, licensing. No candidate was queried
for membership data. No constituent row was read. No fixture was executed.

THE FINDING THAT MATTERS MOST. This project holds no index-membership credential of any
kind. Every BEST-tier source in the inventory — CRSP via WRDS, Compustat/S&P Market
Intelligence, an S&P DJI licence, Bloomberg/FactSet/LSEG — requires a subscription that
does not exist here. The only paths reachable today are a public document corpus and a
low-cost commercial aggregator, which changes what the qualification phase is actually
choosing between.

AND ONE METHODOLOGICAL CONFLICT, RAISED RATHER THAN PAPERED OVER. Candidate C4 IS the S&P
press-release corpus, and seven of the nine fixtures were confirmed from S&P press
releases. Testing C4 against them asks whether the S&P corpus contains what S&P published —
which is tautological for the nine documents we happened to choose, and silent on C4's only
real risk, completeness. This is set out in full below; it does not affect the other
candidates, for which the fixtures test exactly the right thing.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

INV = "SP500_PIT_CANDIDATE_INVENTORY_V1.json"
FIXV2 = "SP500_PIT_FIXTURES_V2.json"
Z = dict(content_rows_inspected=0, fixture_queries_executed=0, price_join=0)


def rows():
    return [
        dict(candidate_id="C1", provider="CRSP (Morningstar Indexes) via WRDS",
             product="CRSP Historical Indexes — S&P 500 membership",
             access_status="NOT_CURRENTLY_ACCESSIBLE",
             account_available=False, subscription_required=True,
             subscription_available_to_us="requires a CRSP subscription via WRDS; WRDS "
                                          "serves corporate, academic and government "
                                          "researchers, so an individual route is not "
                                          "established",
             delivery_method="WRDS web query, SAS Studio, or Jupyter/Python against WRDS",
             authentication="WRDS username/password with Duo two-factor",
             claimed_history_start="1958", claimed_history_end="provider-dependent",
             claimed_effective_dates=True, claimed_stable_id="PERMNO / PERMCO",
             claimed_ticker_history=True, claimed_share_class_support=True,
             licensing_constraints="WRDS terms of use; institutional agreement",
             estimated_cost="institutional subscription — not published",
             limits="not published", **Z),

        dict(candidate_id="C2", provider="S&P Global Market Intelligence / Compustat",
             product="index constituent history (idxcst_his)",
             access_status="NOT_CURRENTLY_ACCESSIBLE",
             account_available=False, subscription_required=True,
             subscription_available_to_us="commercial licence; additionally the WRDS route "
                                          "no longer exists, since idxcst_his was withdrawn "
                                          "from WRDS",
             delivery_method="S&P Global Marketplace / Compustat licence",
             authentication="licensed account",
             claimed_history_start="long", claimed_history_end="current",
             claimed_effective_dates=True, claimed_stable_id="GVKEY / IID",
             claimed_ticker_history=True, claimed_share_class_support=True,
             licensing_constraints="commercial licence; redistribution restricted",
             estimated_cost="enterprise — not published", limits="not published", **Z),

        dict(candidate_id="C3", provider="S&P Dow Jones Indices",
             product="official index constituent data licence",
             access_status="NOT_CURRENTLY_ACCESSIBLE",
             account_available=False, subscription_required=True,
             subscription_available_to_us="commercial index data licence; terms not "
                                          "established",
             delivery_method="licensed feed / index services",
             authentication="licensed account",
             claimed_history_start="full index history", claimed_history_end="current",
             claimed_effective_dates=True, claimed_stable_id="unknown without access",
             claimed_ticker_history="unknown", claimed_share_class_support=True,
             licensing_constraints="commercial licence; redistribution restricted",
             estimated_cost="enterprise — not published", limits="not published", **Z),

        dict(candidate_id="C4", provider="S&P Dow Jones Indices press-release corpus",
             product="public change announcements",
             access_status="ACCESSIBLE_WITH_CONSTRAINTS",
             account_available=True, subscription_required=False,
             subscription_available_to_us="not applicable — public",
             delivery_method="public web pages; wire distribution (PR Newswire) for older "
                             "releases, since press.spglobal.com did not surface 2009-2011 "
                             "announcements in searches",
             authentication="none",
             claimed_history_start="press archive depth UNKNOWN — a real constraint, since "
                                   "reconstruction depends on it",
             claimed_history_end="current",
             claimed_effective_dates=True, claimed_stable_id=False,
             claimed_ticker_history="ticker-at-time within each release only",
             claimed_share_class_support=False,
             licensing_constraints="systematic scraping terms NOT yet checked — this is an "
                                   "open legal item, not a solved one",
             estimated_cost="none", limits="unmeasured; polite crawl rate assumed",
             demonstrated="9/9 fixture documents retrieved and byte-verified in "
                          "SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2",
             **Z),

        dict(candidate_id="C5", provider="EODHD",
             product="Indices Historical Constituents API (marketplace add-on)",
             access_status="ACCESSIBLE_WITH_CONSTRAINTS",
             account_available=False, subscription_required=True,
             subscription_available_to_us="yes — standalone marketplace subscription "
                                          "purchasable by an individual",
             delivery_method="REST API, JSON, 2 endpoints",
             authentication="api_token parameter",
             claimed_history_start="2-12 years for 30 major S&P/DJ indices; join dates for "
                                   "long-standing members claimed back to 1957",
             claimed_history_end="current, updated daily",
             claimed_effective_dates=True, claimed_stable_id="unknown — likely ticker-keyed",
             claimed_ticker_history="unknown", claimed_share_class_support="unknown",
             licensing_constraints="provider states data is sourced from S&P Global under "
                                   "contract; warranty disclaimed; redistribution terms "
                                   "not established",
             estimated_cost="USD 29.99/month stated on the product page",
             limits="no rate or export limit published", **Z),

        dict(candidate_id="C6", provider="Financial Modeling Prep",
             product="historical S&P 500 constituents API",
             access_status="ACCESS_UNRESOLVED",
             account_available=False, subscription_required=True,
             subscription_available_to_us="probable but unconfirmed",
             delivery_method="REST API, JSON", authentication="API key",
             claimed_history_start="unspecified", claimed_history_end="current",
             claimed_effective_dates="claimed", claimed_stable_id="unknown",
             claimed_ticker_history="unknown", claimed_share_class_support="unknown",
             licensing_constraints="NOT ESTABLISHED — the pricing/docs page returned "
                                   "HTTP 403 to an automated request",
             estimated_cost="NOT ESTABLISHED",
             limits="NOT ESTABLISHED",
             unresolved_items=["tier that includes historical constituents", "price",
                               "rate limits", "redistribution terms"],
             note="classified ACCESS_UNRESOLVED rather than guessed from the other "
                  "aggregator's terms",
             **Z),

        dict(candidate_id="C7", provider="Bloomberg / FactSet / LSEG (Refinitiv)",
             product="terminal or licensed feed index membership history",
             access_status="NOT_CURRENTLY_ACCESSIBLE",
             account_available=False, subscription_required=True,
             subscription_available_to_us="enterprise licensing; no terminal or feed "
                                          "available to this project",
             delivery_method="terminal or licensed feed", authentication="licensed account",
             claimed_history_start="long", claimed_history_end="current",
             claimed_effective_dates=True, claimed_stable_id="vendor permanent identifiers",
             claimed_ticker_history=True, claimed_share_class_support=True,
             licensing_constraints="enterprise; redistribution restricted",
             estimated_cost="enterprise — not published", limits="not published", **Z),

        dict(candidate_id="C8", provider="State Street (SPDR S&P 500 ETF Trust)",
             product="published daily fund holdings",
             access_status="ACCESSIBLE",
             account_available=True, subscription_required=False,
             subscription_available_to_us="not applicable — public",
             delivery_method="public fund disclosure files", authentication="none",
             claimed_history_start="fund history", claimed_history_end="current",
             claimed_effective_dates="holdings dates, not index effective dates",
             claimed_stable_id="CUSIP typically present",
             claimed_ticker_history="ticker-at-time per file",
             claimed_share_class_support=True,
             licensing_constraints="public disclosure",
             estimated_cost="none", limits="none known",
             role="CROSS-CHECK ONLY — a fund's holdings are not index membership; "
                  "accessibility does not promote it to canonical",
             **Z),

        dict(candidate_id="C9", provider="Wikipedia",
             product="list page and selected-changes table",
             access_status="ACCESSIBLE",
             account_available=True, subscription_required=False,
             subscription_available_to_us="not applicable — public",
             delivery_method="public web, page revision history",
             authentication="none",
             claimed_history_start="selected changes only", claimed_history_end="current",
             claimed_effective_dates="inconsistent", claimed_stable_id=False,
             claimed_ticker_history=False, claimed_share_class_support=False,
             licensing_constraints="CC licensing",
             estimated_cost="none", limits="none known",
             role="CROSS-CHECK ONLY — NOT ACCEPTABLE as canonical evidence per "
                  "requirement 10; accessibility does not change that",
             **Z),
    ]


def payload():
    rs = rows()
    return dict(
        spec_id="SP500_PIT_CANDIDATE_ACCESSIBILITY_V1",
        status="LOGISTICS ONLY — no membership content inspected",
        phase_boundary=dict(
            checked=["credential presence on this machine, by NAME only",
                     "product existence", "delivery mechanism", "authentication method",
                     "subscription requirement", "published price", "published limits",
                     "licensing constraints", "provider-CLAIMED coverage and identifiers"],
            not_checked=["historical constituent rows", "fixture matching",
                         "actual join/deletion coverage", "membership table construction",
                         "any ranking by episode yield", "any price or outcome join"],
            credential_handling="only key NAMES were read; no secret value was read, "
                                "printed, logged or stored"),
        accessibility_is_not_qualification="ACCESSIBLE says a source can be reached. It "
                                           "says nothing about historical correctness. "
                                           "Every candidate's qualification_status remains "
                                           "UNASSESSED.",
        local_credential_finding=dict(
            index_membership_credentials_present=0,
            credentials_present_by_name=["ANTHROPIC_API_KEY", "MASSIVE_API_KEY",
                                         "ALLOW_YFINANCE_FALLBACK"],
            wrds_or_institutional_config_found=False,
            consequence="every BEST-tier candidate (C1, C2, C3, C7) is "
                        "NOT_CURRENTLY_ACCESSIBLE. The realistically reachable canonical "
                        "paths today are C4 (public corpus, free) and C5 (aggregator, "
                        "USD 29.99/month). This is a constraint on what can be evaluated, "
                        "NOT a judgement that either is adequate."),
        candidates=rs,
        summary={k: [r["candidate_id"] for r in rs if r["access_status"] == k]
                 for k in ("ACCESSIBLE", "ACCESSIBLE_WITH_CONSTRAINTS",
                           "NOT_CURRENTLY_ACCESSIBLE", "ACCESS_UNRESOLVED")},
        all_qualification_status_unassessed=True,

        # ── the conflict ───────────────────────────────────────────────────────
        circularity_finding=dict(
            issue="candidate C4 IS the S&P Dow Jones Indices press-release corpus, and "
                  "eight of the nine fixtures (all but F5, which is from Meta's investor "
                  "relations) were confirmed from S&P press releases",
            why_it_matters="testing C4 against F1-F9 asks whether the S&P corpus contains "
                           "what S&P published. For the nine documents we selected that is "
                           "tautological — we retrieved them from that corpus ourselves — "
                           "and it is completely silent on C4's actual risk, which is "
                           "whether the corpus is COMPLETE",
            contract_clause="the qualification contract requires primary evidence "
                            "independent of every candidate source, and states that a "
                            "candidate must not supply its own fixture truth",
            scope="this affects C4 ONLY. For a candidate that RECORDS the index "
                  "administrator's decisions — C1, C5, C6, C7, and in a different sense "
                  "C2/C3 — using S&P's own announcements as ground truth is exactly the "
                  "right test: it measures faithfulness to the authority.",
            consequence="F1-F9 CANNOT qualify C4. A distinct completeness test is required "
                        "for it — for example reconciling the count of S&P 500 changes in "
                        "a bounded period against an independently compiled tally, where "
                        "the failure mode is a MISSING release rather than a wrong one",
            status="RAISED, NOT RESOLVED — no completeness test is specified or frozen "
                   "here, and C4 must not be qualified on the existing fixtures",
            decision_required="either freeze a C4-specific completeness test, or classify "
                              "C4 as unqualifiable by the current fixture set"),

        # ── frozen before the first content read ───────────────────────────────
        frozen_evaluation_protocol=dict(
            frozen_before="any historical membership row has been opened",
            evaluation_order=["C5 (accessible, subscription obtainable)",
                              "C6 (if access resolves)",
                              "C1, C2, C3, C7 (only if access is ever obtained)"],
            c4_excluded_from_this_order="pending resolution of the circularity finding",
            order_rationale="accessibility determines what CAN be evaluated; the order is "
                            "fixed now so it cannot be reshuffled after a candidate "
                            "disappoints",
            ranking_rule=["1. qualification requirements satisfied",
                          "2. fixture performance",
                          "3. identity and effective-date completeness",
                          "4. reconstructability",
                          "5. provenance and versionability",
                          "6. operational accessibility and cost"],
            ranking_never=["episode yield", "signal count", "any future outcome",
                           "coverage of any particular motif, family or ticker"],
            query_protocol=dict(
                rule="content access begins with FIXTURE-DRIVEN queries only",
                forbidden="dumping a full historical table and browsing it",
                sequence=["query only the identity and boundary facts needed for F1-F9",
                          "classify against the frozen fixtures",
                          "ONLY if it survives fixture qualification, inspect broader "
                          "coverage and reconstructability"],
                enforcement="the evaluation runner executes inside "
                            "qualification_blindness.blindness(), whose allowlist admits "
                            "the fixture archive, the candidate's own membership source "
                            "and identity/reference data, and refuses every other data "
                            "read (conformance PASS)")),

        sources=dict(inventory=dict(artifact=INV, digest=ART.file_digest(INV)
                                    if os.path.exists(INV) else None),
                     fixtures=dict(artifact=FIXV2, digest=ART.file_digest(FIXV2)
                                   if os.path.exists(FIXV2) else None)),
        content_rows_inspected=0, fixture_queries_executed=0, price_joins=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "SP500_PIT_CANDIDATE_ACCESSIBILITY_V1.json",
                 required=("spec_id", "status", "candidates", "summary",
                           "frozen_evaluation_protocol", "circularity_finding"),
                 supersede=os.path.exists("SP500_PIT_CANDIDATE_ACCESSIBILITY_V1.json"))
    print(f"SP500_PIT_CANDIDATE_ACCESSIBILITY_V1 · {d} · {p['status']}")
    for r in p["candidates"]:
        print(f"  {r['candidate_id']}  {r['provider'][:34]:34s} {r['access_status']}")
    print(f"\n  index-membership credentials on this machine: "
          f"{p['local_credential_finding']['index_membership_credentials_present']}")
    print(f"  content rows inspected {p['content_rows_inspected']} · "
          f"fixture queries {p['fixture_queries_executed']} · "
          f"price joins {p['price_joins']}")
    print(f"  RAISED: {p['circularity_finding']['decision_required']}")


if __name__ == "__main__":
    main()
