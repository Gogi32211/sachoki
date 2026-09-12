"""MASSIVE_1M_PROBE_SPEC_V1 — nine falsification probes, frozen BEFORE any is executed.

WHY THE SPEC IS FROZEN FIRST. If the vendor's actual responses are inspected before the
acceptance rules are written, the rules become a description of what came back. That
failure is silent: nothing in the output would show that "PASS" was defined after seeing
the data. Freezing the decision rule first is the only thing that makes a PASS mean
anything, and it is why these probes are specified now and executed later.

EVERY PROBE IS A FALSIFICATION, NOT AN INSPECTION. "Fetch a split date and look" is not a
probe. A probe states a claim, names an independently-known fact that the claim implies,
and commits IN ADVANCE to the numeric boundaries at which the claim is accepted, rejected,
or declared unresolved. A response that lands between the boundaries is UNRESOLVED — never
rounded toward the convenient reading.

VENDOR COMPATIBILITY IS NOT EVIDENCE. Massive is documented as Polygon-compatible. That
claim may not be used to resolve any probe. "Polygon behaves this way, so this probably
does" is explicitly an inadmissible resolution; the state in that case is UNRESOLVED.

Each probe carries exactly six elements:

    1 claim                 what is being tested
    2 request               exact symbol / date / session
    3 independent_fact      the externally-known truth the claim implies
    4 retain                raw vendor fields to archive
    5 decision              PASS / FAIL / UNRESOLVED, with numeric boundaries
    6 consequence           what each state does downstream

NOTHING IS EXECUTED BY THIS MODULE. It writes a specification and exits.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

CHARTER = "MASSIVE_1M_STATE_TRANSITION_V1.json"
AMEND = "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json"

RTH_BARS_FULL = 390          # 09:30..15:59 inclusive
RTH_BARS_EARLY = 210         # 09:30..12:59 inclusive, 13:00 ET close


def probes():
    return [
        dict(
            probe="P1", topic="split/dividend adjustment regime",
            claim="the 1-minute aggregate series is returned on ONE adjustment regime, and "
                  "the `adjusted` request parameter actually controls it",
            request=dict(
                primary="AAPL 1m, sessions 2020-08-28 and 2020-08-31, RTH",
                replicate="NVDA 1m, sessions 2024-06-07 and 2024-06-10, RTH",
                parameter_test="each range requested TWICE, adjusted=true and "
                               "adjusted=false, otherwise byte-identical"),
            independent_fact=dict(
                aapl="4-for-1 split effective 2020-08-31",
                nvda="10-for-1 split effective 2024-06-10",
                confirm_before_execution="both split dates and ratios must be confirmed "
                                         "from a NON-Massive source and that confirmation "
                                         "sealed before this probe runs"),
            retain=["t", "o", "h", "l", "c", "v", "vw", "n", "raw response body sha256",
                    "the exact request URL including the adjusted parameter"],
            decision=dict(
                statistic="r = close(last RTH minute, pre-split session) / "
                          "open(first RTH minute, effective session)",
                PASS_UNADJUSTED="AAPL 3.60 <= r <= 4.40 AND NVDA 9.00 <= r <= 11.00",
                PASS_ADJUSTED="AAPL 0.90 <= r <= 1.10 AND NVDA 0.90 <= r <= 1.10",
                FAIL_AMBIGUOUS="r falls outside both bands on either name, or the two "
                               "names disagree on regime",
                FAIL_PARAMETER_INERT="adjusted=true and adjusted=false return identical "
                                     "response digests — the parameter does not control "
                                     "anything and the regime is whatever it is",
                UNRESOLVED="any required session is missing, or the split fact could not "
                           "be independently confirmed",
                note="the bands are wide enough to absorb a normal overnight move and "
                     "narrow enough that 4x and 1x cannot both satisfy one of them"),
            consequence=dict(
                PASS_UNADJUSTED="a split-adjustment layer becomes a REQUIRED programme "
                                "component before any multi-year feature is computed",
                PASS_ADJUSTED="vendor restatement behaviour becomes a vintage concern: "
                              "the same history may change between extracts",
                FAIL_ANY="canonical ingestion is blocked for all multi-year use; only "
                         "single-session work is admissible")),

        dict(
            probe="P2", topic="timestamp unit, labelling and timezone",
            claim="`t` is a fixed-unit epoch timestamp labelling the bar's START, tz-aware "
                  "through DST",
            request=dict(
                winter="AAPL 1m, full session 2024-01-16 (EST)",
                summer="AAPL 1m, full session 2024-07-16 (EDT)"),
            independent_fact=dict(
                session_open="US equities RTH opens 09:30 America/New_York",
                est="2024-01-16 is EST (UTC-5) -> 09:30 ET = 14:30 UTC",
                edt="2024-07-16 is EDT (UTC-4) -> 09:30 ET = 13:30 UTC",
                dst_2024="DST ran 2024-03-10 to 2024-11-03"),
            retain=["t for every bar", "the first and last RTH bar of each session",
                    "response digest"],
            decision=dict(
                unit_MILLISECONDS="1e12 <= t < 1e13",
                unit_NANOSECONDS="t >= 1e18",
                unit_UNRESOLVED="any other magnitude",
                label_BAR_START="first RTH bar converts to exactly 09:30:00 ET on BOTH "
                                "sessions",
                label_BAR_END="first RTH bar converts to exactly 09:31:00 ET on BOTH "
                              "sessions",
                label_UNRESOLVED="any other value, or the two sessions disagree",
                tz_PASS="first RTH bar is 14:30 UTC in winter AND 13:30 UTC in summer "
                        "(under BAR_START, ms)",
                tz_FAIL_FIXED_OFFSET="the winter and summer first-bar UTC times are equal "
                                     "— the vendor is applying a fixed offset, not a "
                                     "timezone",
                UNRESOLVED="either session missing or truncated"),
            consequence=dict(
                any_UNRESOLVED_or_FAIL="all aggregation is blocked: a one-minute labelling "
                                       "error shifts every 15m and session-aligned 1H "
                                       "boundary and would silently misalign every clock "
                                       "feature",
                PASS="the boundary convention in the charter's aggregation contract is "
                     "confirmed executable as written")),

        dict(
            probe="P3", topic="sparse vs dense minute grid",
            claim="minutes with no trades are OMITTED rather than emitted as zero-volume "
                  "bars",
            request=dict(
                sample="20 (symbol, session) pairs: the 10 highest and 10 lowest "
                       "average-volume S&P 500 constituents on a common full trading "
                       "session, RTH only",
                session="one full (non-early-close) session, chosen before execution and "
                        "named in the sealed run record"),
            independent_fact=dict(
                full_session_minutes=RTH_BARS_FULL,
                basis="09:30..15:59 inclusive is 390 one-minute intervals",
                halts="any halt covering the session must be identified from a non-Massive "
                      "source before interpreting a short count"),
            retain=["bar count per (symbol, session)", "every bar with v == 0",
                    "every bar with n == 0", "the full t domain per symbol"],
            decision=dict(
                SPARSE=f"at least one symbol-session has < {RTH_BARS_FULL} bars with no "
                       f"recorded halt",
                DENSE=f"every symbol-session has exactly {RTH_BARS_FULL} bars AND "
                      f"zero-volume bars are present",
                FAIL=f"any symbol-session has > {RTH_BARS_FULL} RTH bars — duplicates or "
                     f"out-of-session contamination",
                UNRESOLVED=f"every symbol-session has exactly {RTH_BARS_FULL} bars and NO "
                           f"zero-volume bar appears — indistinguishable from a sample in "
                           f"which every minute genuinely traded; the probe must be rerun "
                           f"on a wider or less liquid sample"),
            consequence=dict(
                SPARSE="the three-valued availability semantics of "
                       "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 are live, and the "
                       "STRUCTURAL_NO_TRADE promotion path must be implemented",
                DENSE="the vendor has already made the zero/missing decision for us and "
                      "its rule must be established before its zeros are trusted",
                FAIL="ingestion blocked pending a grain investigation")),

        dict(
            probe="P4", topic="auction attribution",
            claim="the closing auction print is attributable to a specific minute bar",
            request=dict(
                sample="AAPL, MSFT, JPM 1m, one full session, RTH plus the 16:00 minute"),
            independent_fact=dict(
                closing_auction="US closing auctions execute at 16:00 ET and are typically "
                                "the largest single print of the session for large caps",
                opening_auction="opening auctions execute at 09:30 ET"),
            retain=["v and n for every minute from 15:30 to 16:05 ET",
                    "v and n for every minute from 09:25 to 10:00 ET",
                    "presence or absence of a bar stamped 16:00"],
            decision=dict(
                baseline="m = median v over RTH minutes 15:30..15:58",
                CLOSING_IN_1600="a bar stamped 16:00 exists with v > 5*m on all three names",
                CLOSING_IN_1559="no 16:00 bar exists AND the 15:59 bar has v > 5*m on all "
                                "three names",
                UNRESOLVED="the names disagree, or no minute in 15:59..16:00 exceeds 5*m",
                opening_status="INDICATIVE ONLY — the 09:30 minute is genuinely busy "
                               "regardless of auction attribution, so this probe CANNOT "
                               "resolve the opening auction. Opening attribution is "
                               "declared UNRESOLVED unless trade-condition data is "
                               "obtained, and no VWAP may be anchored to the open until "
                               "it is."),
            consequence=dict(
                resolved="the session's last bar is defined explicitly and any close-"
                         "anchored VWAP or last-slot RVOL has a fixed denominator",
                UNRESOLVED="no feature may be anchored to the closing or opening auction; "
                           "last-slot and first-slot RVOL are flagged as containing an "
                           "unattributed auction")),

        dict(
            probe="P5", topic="extended-hours inclusion",
            claim="minute aggregates include pre- and post-market bars",
            request=dict(
                sample="AAPL 1m over a full calendar day covering 04:00-20:00 ET, one "
                       "normal session"),
            independent_fact=dict(
                us_extended="US pre-market runs from 04:00 ET and post-market to 20:00 ET; "
                            "large caps trade in both"),
            retain=["every bar outside 09:30..15:59 ET with its t, v, n"],
            decision=dict(
                EXTENDED_INCLUDED="at least one bar exists before 09:30 ET or after 16:00 "
                                  "ET with v > 0",
                RTH_ONLY="no bar exists outside 09:30..16:00 ET",
                UNRESOLVED="bars exist outside RTH but all have v == 0 and n == 0"),
            consequence=dict(
                EXTENDED_INCLUDED="the RTH filter is MANDATORY at ingestion and "
                                  "extended-hours bars are retained but flagged, never "
                                  "mixed into RTH aggregates",
                RTH_ONLY="extended-hours state features are impossible from this source "
                         "and must be removed from the feature dictionary rather than "
                         "approximated")),

        dict(
            probe="P6", topic="pagination limit and interval completeness",
            claim="a long range can be retrieved completely, deterministically, and "
                  "without page-boundary loss",
            request=dict(
                short="AAPL 1m, one full RTH session — must return the complete domain",
                long="AAPL 1m, 2024-01-01 to 2024-12-31, following pagination to "
                     "exhaustion",
                replay="the long request executed TWICE, independently"),
            independent_fact=dict(
                expected_short_domain="09:30..15:59 ET, subject to the P3 sparse/dense "
                                      "finding",
                magnitude="a year of RTH minutes for one symbol is on the order of 98,000 "
                          "bars, which must exceed any single-page limit and therefore "
                          "forces pagination"),
            retain=["max rows returned per page", "every page boundary timestamp",
                    "full t domain", "both replay row sets", "response digests"],
            decision=dict(
                PASS="duplicates == 0 AND t strictly increasing within the symbol AND "
                     "zero page-boundary gaps AND the two replays produce identical row "
                     "sets",
                page_boundary_gap_test="for each page boundary, re-request the 10 minutes "
                                       "spanning it in isolation; any minute present in "
                                       "isolation but absent from the paginated result is "
                                       "a page-boundary gap",
                FAIL="any duplicate, any non-monotone t, any page-boundary gap, or replay "
                     "row sets that differ",
                UNRESOLVED="pagination terminates without an explicit exhaustion signal, "
                           "so completeness cannot be demonstrated"),
            consequence=dict(
                PASS="this result is the completeness evidence that licenses promotion of "
                     "an absent minute to STRUCTURAL_NO_TRADE under the amendment",
                FAIL_or_UNRESOLVED="every absent minute in the affected range stays "
                                   "UNOBSERVED permanently; no promotion is possible and "
                                   "RVOL baselines exclude the range")),

        dict(
            probe="P7", topic="vw and n semantics",
            claim="`vw` is a within-bar volume-weighted average price and `n` is a count "
                  "of transactions",
            request=dict(
                sample="AAPL, MSFT, and one low-volume S&P constituent, 1m, one full "
                       "session, RTH"),
            independent_fact=dict(
                vwap_property="a within-bar VWAP must lie within that bar's [low, high]",
                discriminator="a VWAP is NOT equal to the typical price (h+l+c)/3 nor to "
                              "the midpoint (h+l)/2 other than by coincidence",
                trade_count="n must be >= 1 on any bar with v > 0"),
            retain=["l, h, c, v, vw, n for every bar", "count of vw outside [l, h]",
                    "count of bars where vw == (h+l+c)/3 within 1e-6",
                    "count of bars where vw == (h+l)/2 within 1e-6",
                    "count of bars with v > 0 and n == 0"],
            decision=dict(
                vw_PASS="zero bars have vw outside [l, h] AND fewer than 1% of bars match "
                        "(h+l+c)/3 or (h+l)/2 within 1e-6",
                vw_FAIL_NOT_WITHIN_BAR="any bar has vw outside [l, h]",
                vw_FAIL_IS_DERIVED_PRICE="more than 1% of bars equal a typical price or "
                                         "midpoint within 1e-6 — vw is a derived price, "
                                         "not a VWAP",
                n_PASS="zero bars have v > 0 with n == 0",
                n_FAIL="any bar has v > 0 with n == 0",
                UNRESOLVED="the fields pass internal consistency but no independent "
                           "external quantity is available to confirm what they COUNT or "
                           "WEIGHT. Vendor Polygon-compatibility may NOT be used to "
                           "resolve this; documentation and empirical result must agree "
                           "explicitly, and where they do not, the state is UNRESOLVED."),
            consequence=dict(
                vw_PASS="the charter's volume-weighted aggregation rule is executable",
                vw_FAIL="vw is discarded and every VWAP feature is recomputed from OHLCV "
                        "or removed; no VWAP-family feature survives on the vendor field",
                n_UNRESOLVED="transaction-count features are removed from the dictionary "
                             "rather than used with an assumed meaning")),

        dict(
            probe="P8", topic="volume comparability with the existing store",
            claim="Massive RTH 1m volume summed to a day is comparable with the existing "
                  "DuckDB bars daily volume",
            request=dict(
                sample="20 (symbol, session) pairs present in BOTH sources, spanning at "
                       "least 4 distinct sessions and a range of liquidity"),
            independent_fact=dict(
                basis="both purport to measure consolidated share volume for the same "
                      "symbol and session",
                caveat="venue coverage, odd-lot inclusion and TRF reporting are the "
                       "known reasons two vendors legitimately disagree"),
            retain=["sum of RTH 1m v per pair", "existing store daily volume per pair",
                    "the ratio per pair", "which pairs were excluded and why"],
            decision=dict(
                statistic="ratio = sum(massive RTH 1m v) / bars.volume, per pair",
                COMPARABLE="|ratio - 1| < 0.01 for at least 19 of 20 pairs",
                SYSTEMATIC_OFFSET="stdev(ratio) < 0.02 across pairs but the mean ratio is "
                                  "outside 0.99..1.01 — a stable, measurable difference; "
                                  "the factor is recorded",
                NOT_COMPARABLE="stdev(ratio) >= 0.02",
                UNRESOLVED="fewer than 20 overlapping pairs can be constructed"),
            consequence=dict(
                COMPARABLE="the 1m-derived 1D may be cross-checked against the existing "
                           "store, which becomes a standing ingestion validation",
                SYSTEMATIC_OFFSET="cross-checks apply the recorded factor and the two "
                                  "stores are never pooled",
                NOT_COMPARABLE="the two stores are kept strictly separate and no study "
                               "may mix them; the existing store cannot validate the new "
                               "one")),

        dict(
            probe="P9", topic="exchange calendar",
            claim="a candidate exchange calendar reproduces known 2024 closures and early "
                  "closes exactly, and the vendor agrees with it",
            request=dict(
                calendar_side="evaluate the candidate calendar library or dataset against "
                              "the fixed 2024 date list below",
                vendor_side="request 1m for a liquid S&P constituent on every full-closure "
                            "date and on every early-close date"),
            independent_fact=dict(
                full_closures_2024=["2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
                                    "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
                                    "2024-11-28", "2024-12-25"],
                early_closes_2024_1300ET=["2024-07-03", "2024-11-29", "2024-12-24"],
                early_close_minutes=RTH_BARS_EARLY,
                confirm_before_execution="this date list must be confirmed from a "
                                         "NON-Massive, non-calendar-library source and "
                                         "sealed before execution, since it is the "
                                         "standard both sides are judged against"),
            retain=["calendar output per date", "vendor RTH bar count per date",
                    "the last RTH bar timestamp on each early-close date"],
            decision=dict(
                calendar_PASS="the candidate reproduces all 10 full closures and all 3 "
                              "early closes with the correct 13:00 ET close",
                calendar_FAIL="any mismatch",
                vendor_PASS="zero RTH bars on every full-closure date AND the last RTH bar "
                            "on each early-close date is 12:59 ET",
                vendor_FAIL="any RTH bar on a full-closure date, or trading past 12:59 ET "
                            "on an early-close date",
                UNRESOLVED="the vendor returns no data at all for a date, which is "
                           "indistinguishable from a coverage gap"),
            consequence=dict(
                both_PASS="the calendar is sealed and becomes the session authority; it is "
                          "a required precondition for STRUCTURAL_NO_TRADE promotion",
                calendar_FAIL="that candidate is rejected and another is evaluated under "
                              "this same unchanged specification",
                vendor_FAIL="vendor session semantics are unreliable; every session "
                            "boundary must be enforced by our calendar and never inferred "
                            "from vendor data presence")),
    ]


def payload():
    ps = probes()
    return dict(
        spec_id="MASSIVE_1M_PROBE_SPEC_V1",
        status="FROZEN BEFORE EXECUTION — no probe has been run",
        programme="MASSIVE_1M_STATE_TRANSITION_V1",
        sources=dict(
            charter=dict(artifact=CHARTER, digest=ART.file_digest(CHARTER)
                         if os.path.exists(CHARTER) else None),
            amendment=dict(artifact=AMEND, digest=ART.file_digest(AMEND)
                           if os.path.exists(AMEND) else None)),
        covers="the 9 non-universe items of the charter's must_resolve_before_ingest list; "
               "the 10th, point-in-time S&P 500 membership, is a source-selection gate and "
               "is handled separately",
        global_rules=[
            "every independent_fact must be confirmed from a NON-Massive source, and that "
            "confirmation sealed, BEFORE the probe that relies on it is executed",
            "no acceptance boundary, band or threshold in this artifact may be changed "
            "after any vendor response has been inspected",
            "each probe is executed once; the raw response body and its sha256 are "
            "archived with the request URL",
            "a result between the committed boundaries is UNRESOLVED — never rounded "
            "toward the convenient reading",
            "vendor Polygon-compatibility may not resolve any probe; where documentation "
            "and empirical result disagree, the state is UNRESOLVED",
            "UNRESOLVED on any probe blocks canonical ingestion for the semantics that "
            "probe governs, and blocks it silently for nothing else",
            "probes are independent: no probe's outcome may relax another's rule"],
        execution_order=dict(
            rule="P2 and P6 first — timestamp semantics and interval completeness are "
                 "preconditions for interpreting every other probe",
            note="P3's promotion path depends on P6, and P9 is a precondition for P3's "
                 "halt and session reasoning"),
        n_probes=len(ps),
        probes=ps,
        not_executed="this artifact contains no request, performs no I/O against the "
                     "vendor, and authorises no download",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "MASSIVE_1M_PROBE_SPEC_V1.json",
                 required=("spec_id", "status", "probes", "global_rules", "n_probes"),
                 supersede=os.path.exists("MASSIVE_1M_PROBE_SPEC_V1.json"))
    print(f"MASSIVE_1M_PROBE_SPEC_V1 · {d} · {p['status']}")
    for q in p["probes"]:
        keys = [k for k in ("claim", "request", "independent_fact", "retain", "decision",
                            "consequence") if k in q]
        print(f"  {q['probe']}  {q['topic']:42s} elements {len(keys)}/6")
    print(f"  execution order: {p['execution_order']['rule'][:70]}")
    print(f"  probes executed: 0")


if __name__ == "__main__":
    main()
