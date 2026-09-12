"""MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1 — four holes closed, one that cannot be.

Three of the five pre-flight items resolved cleanly against evidence. The fourth — mintick —
did not, and under the no-fallback rule you froze, that is the whole gate.

TIMESTAMP ALIGNMENT: RESOLVED, AND THE WINTER DATE IS WHY. The legacy 15m store labels bars
13:30..19:45 on 2024-05-15 and 14:30..20:45 on 2024-01-16 — 26 bars either way. A fixed offset
could not produce both; only UTC storage of an ET session open can. So legacy `date` is bar
START in UTC, and it lines up value-for-value with the Massive coarse `bar_start`. The rule is
equality, not tolerance.

PRICE BASIS: SAME BASIS, DIFFERENT PRECISION — shown rather than assumed. On a matched AAPL bar
the legacy close is 187.91 where Massive carries 187.9132; 188.8521 -> 188.85; 189.3713 ->
189.37. Legacy is Massive rounded to two decimals. That is the daily-level storage rounding from
the recovery artifact, now confirmed to hold at 15m, and it means PRICE_BASIS_DIFFERENCE and
SOURCE_FEED_OHLC_DIFFERENCE stay separable. One ticker and one session is indicative, not proof,
so measurement at scale is required in smoke rather than claimed here.

MINTICK: NO AUTHORITY EXISTS, AND I LOOKED PROPERLY. Neither legacy store carries a tick or
increment column — the only matches are the substring inside "ticker". The Massive reference
endpoint returns active, cik, composite_figi, currency_name, last_updated_utc, locale, market,
name, primary_exchange, share_class_figi, ticker, type — and nothing about a trading increment.
There is no effective-dated tick table anywhere in the project.

AND IT IS NOT A HARMLESS GAP, WHICH IS THE PART THAT MATTERS. `prevBodySafe = max(prevBody,
mintick)` only binds when prevBody is smaller than mintick. On the legacy side that was almost
moot: prices are stored to two decimals, so a prior body is either 0 or at least $0.01. On the
MASSIVE side prices are full precision, and measurement over a twelve-session sample of 95,263
qualifying bars finds prevBody below $0.01 on 6.01% of them — 1.51% exactly zero. Those are
precisely the bars where the chosen mintick sets the denominator of bodyRatioOk, which gates
fullyEngulfs, which decides T4 and T6 — first and second in the priority chain, preempting every
state beneath them.

So roughly one bar in seventeen has its T label decided by a number the project cannot source.
Picking $0.01 would not be a small convenience; it would silently author the outcome on 6% of
the population and then let that appear in the report as feed divergence.

Under NO FALLBACK the consequence is unavoidable: mintick is unresolvable, so every observation
where it binds is UNAVAILABLE_MINTICK, and this amendment cannot be frozen. The decision is
yours and it is a real fork, not a formality.
"""
from __future__ import annotations
import json, os, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

BASE, BASE_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1.json", "545d293b4f1e4653"
REC_D, PINE_D = "7d00e3908feb26d5", "696f40d730662a60"
COARSE_D = "6ec73d5f462761f0"


def main():
    if ART.file_digest(BASE) != BASE_D:
        print("HOLD — base spec digest mismatch"); return 1

    mintick = dict(
        authority_inventory=dict(
            searched=[
                dict(source="studio_analytics.duckdb (legacy daily)",
                     result="no tick/increment column; the only matches are the substring "
                            "inside 'ticker'"),
                dict(source="studio_15m.duckdb (legacy 15m, 418 columns)",
                     result="no tick column"),
                dict(source="Massive /v3/reference/tickers archived responses",
                     fields=["active", "cik", "composite_figi", "currency_name",
                             "last_updated_utc", "locale", "market", "name",
                             "primary_exchange", "share_class_figi", "ticker", "type"],
                     result="no trading-increment field"),
                dict(source="frozen project reference tables with effective dates",
                     result="none carrying tick size")],
            authoritative_pit_source_found=False,
            forbidden_inferences_not_used=["number of OHLC decimals",
                                           "observed price differences",
                                           "minimum historical price change", "ticker",
                                           "price level", "the 0.01 convention"]),
        binding_model_required=["security_key_v1", "effective_from", "effective_to",
                                "mintick_value", "mintick_source", "mintick_source_digest",
                                "mintick_authority_status"],
        resolution_key="security identity AND bar timestamp, never ticker string alone",
        missing_policy=dict(
            rule="NO FALLBACK",
            on_unresolved="T availability = UNAVAILABLE_MINTICK",
            forbidden=["0.01", "0.001", "1e-10", "nearest known tick size",
                       "current tick size", "decimal precision",
                       "security-class default"],
            not_false="UNAVAILABLE_MINTICK != FALSE",
            excluded_from="conformance denominators requiring executable T"),
        temporal=dict(
            treated_as="potentially time-varying",
            single_current_value_backfill="FORBIDDEN without continuity evidence",
            window="2021-2026"),
        pine_binding="prevBodySafe = math.max(prevBody, syminfo.mintick), with the resolved "
                     "point-in-time value and no numerical surrogate, epsilon or floor",
        exposure_measurement=dict(
            question="how often does mintick actually bind on Massive data",
            method="prevBody = |prev close - prev open| over COMPLETE 15m bars with an "
                   "available prior COMPLETE bar",
            sample="12 sessions spread across the window",
            qualifying_bars=95263,
            prev_body_under_1_cent=dict(count=5727, share=0.0601),
            prev_body_under_a_tenth_cent=dict(count=1531, share=0.0161),
            prev_body_exactly_zero=dict(count=1442, share=0.0151),
            legacy_contrast="legacy prices are stored to two decimals, so a legacy prior body "
                            "is either 0 or at least $0.01; the exposure is a MASSIVE-side "
                            "problem created by full precision",
            why_it_matters="mintick sets the denominator of bodyRatioOk, which gates "
                           "fullyEngulfs, which decides T4 and T6 — ranks 1 and 2 in the "
                           "priority chain, preempting every state beneath them",
            plain_statement="roughly one bar in seventeen would have its T label decided by a "
                            "number the project cannot source",
            consequence_of_guessing="choosing $0.01 would silently author the outcome on ~6% "
                                    "of the population, and that authorship would then appear "
                                    "in the report as feed divergence"),
        status="UNRESOLVED — no authoritative source exists")

    timestamp = dict(
        status="RESOLVED",
        legacy=dict(store="studio_15m.duckdb", column="date", dtype="TIMESTAMP",
                    bars_per_session=26,
                    evidence=dict(
                        summer="2024-05-15 runs 13:30 .. 19:45",
                        winter="2024-01-16 runs 14:30 .. 20:45",
                        inference="a fixed offset cannot produce both; only UTC storage of an "
                                  "09:30 ET open can. 13:30 UTC = 09:30 EDT, 14:30 UTC = "
                                  "09:30 EST."),
                    convention="bar START, stored in UTC"),
        massive=dict(authority="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1",
                     digest=COARSE_D, fields=["bar_start", "bar_end",
                                              "feature_information_available_at"],
                     convention="left-closed, left-labelled; bar_start is epoch ms UTC"),
        alignment_rule="legacy.date == Massive.bar_start, exact equality",
        verified_on=dict(security="AAPL", session="2024-05-15",
                         legacy=["13:30", "13:45", "14:00"],
                         massive=["13:30", "13:45", "14:00"]),
        forbidden=["nearest timestamp", "±15 minute tolerance", "fuzzy matching",
                   "best-overlap matching", "alignment chosen to maximise agreement"],
        session_boundaries="the 26-bar structure holds on both sampled sessions; early-close "
                           "and open/final-interval behaviour must still be verified across "
                           "all sessions in smoke")

    price_basis = dict(
        status="INDICATIVE_COMPARABLE — measurement required at scale",
        evidence=dict(security="AAPL", session="2024-05-15",
                      pairs=[dict(bar="13:30", legacy_close=187.91,
                                  massive_close=187.9132),
                             dict(bar="14:00", legacy_open=188.85,
                                  massive_open=188.8521),
                             dict(bar="14:00", legacy_close=189.37,
                                  massive_close=189.3713)],
                      reading="legacy is Massive rounded to two decimals — same basis, "
                              "different precision"),
        corroborates="the recovery artifact's daily-level STORAGE_PRECISION_ROUNDING finding, "
                     "now seen at 15m",
        consequence="PRICE_BASIS_DIFFERENCE and SOURCE_FEED_OHLC_DIFFERENCE remain separable",
        limitation="one security, one session, three bars — indicative only; the taxonomy "
                   "PRICE_BASIS_CONFIRMED_COMPARABLE / PRICE_BASIS_DIFFERENCE / "
                   "PRICE_BASIS_UNRESOLVED must be assigned by measurement in smoke",
        no_ad_hoc_normalization=dict(
            forbidden=["rescaling one source to match the other",
                       "reverse-engineering split factors from price ratios",
                       "normalising by agreement maximisation",
                       "rounding Massive OHLC to legacy precision as primary input",
                       "adjusting legacy OHLC to improve label agreement"],
            primary_diagnostic="native-source comparison"))

    z7 = dict(
        classification="T_INTERNAL_SUPPORTING_PREDICATE",
        why_computed="recovered T semantics require prev1IsBear = (close[1] < open[1]) OR "
                     "Z7_raw[1]",
        allowed="exact T dependency evaluation only; an internal diagnostic value may be "
                "retained for verifier inspection",
        forbidden=["research-facing Z output", "bearCode output", "Z-state materialisation",
                   "Z transition", "Z candidate family", "Z conditioning variable",
                   "Z result", "appearance in the research-facing feature registry"],
        does_not_alter="Z_STATUS = HOLD / DISCOVERY_EVIDENCE_ONLY",
        request_for_z_output="FAIL")

    added_negatives = [
        "missing mintick yields UNAVAILABLE_MINTICK",
        "a 0.01 fallback is rejected",
        "a decimal-derived mintick is rejected",
        "current-only tick metadata cannot be backfilled historically without continuity "
        "evidence",
        "a wrong effective-dated mintick changes the boundary predicate and is detected",
        "an ambiguous legacy timestamp convention yields HOLD",
        "nearest-timestamp matching is rejected",
        "a price-basis-unresolved observation cannot be labelled pure "
        "SOURCE_FEED_OHLC_DIFFERENCE",
        "ad-hoc price normalisation is rejected",
        "Z7_raw can support T internally",
        "Z7_raw cannot become research-facing Z output",
    ]

    acceptance = {
        "base_spec_unchanged": True,
        "authoritative_mintick_source_identified": False,
        "no_implicit_or_fallback_mintick": True,
        "historical_mintick_temporality_resolved": False,
        "timestamp_alignment_deterministic": True,
        "price_basis_relationship_classified": True,
        "z7_classified_internal_support": True,
        "t_production_writes_zero": True,
        "z_remains_unregistered": True,
        "y_exposed_zero": True,
    }
    ok = all(acceptance.values())

    p = dict(
        spec_id="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1",
        status=("MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1_FROZEN" if ok else "HOLD"),
        verdict_reason=("all five pre-flight items resolved" if ok else
                        "no authoritative point-in-time tick-size source exists in any "
                        "project authority, and under the frozen NO FALLBACK rule mintick "
                        "therefore cannot be bound"),
        task_class="SPECIFICATION_AMENDMENT_ONLY",
        supplements=dict(base_spec="MASSIVE_T_FEATURE_PORT_SPEC_V1", digest=BASE_D,
                         modified=False,
                         relationship="additional binding authority; the base spec is not "
                                      "edited"),
        binds=dict(recovery=REC_D, pine_source=PINE_D, coarse_production=COARSE_D),
        mintick=mintick,
        timestamp_alignment=timestamp,
        price_basis=price_basis,
        z7_internal=z7,
        added_negative_fixtures=dict(count=len(added_negatives), required=added_negatives),
        resolved_items=["timestamp alignment", "price-basis relationship",
                        "Z7_raw classification"],
        unresolved_items=["Massive-port syminfo.mintick binding",
                          "point-in-time mintick temporality"],
        decision_required=dict(
            summary="mintick has no source in this project, and it decides the T label on "
                    "roughly 6% of Massive bars",
            options=[
                dict(id="A", name="admit an external regulatory authority as the tick source",
                     detail="US equity minimum increments are set by rule rather than by "
                            "vendor metadata; adopting that as the authority would need it "
                            "named, versioned and frozen like any other source",
                     effect="mintick becomes resolvable and the port executes",
                     note="this ADDS an authority the project has not previously used — it is "
                          "a real provenance decision, not a lookup",
                     recommended=None),
                dict(id="B", name="accept UNAVAILABLE_MINTICK where it binds",
                     detail="T is executable only on the ~94% of bars where prevBody is large "
                            "enough that mintick cannot change prevBodySafe",
                     effect="the port runs with an explicit, measured availability hole "
                            "concentrated exactly on small-bodied bars",
                     caveat="that hole is not random — it selects against low-volatility and "
                            "low-priced bars, which is itself a population statement that "
                            "must be reported",
                     recommended=None),
                dict(id="C", name="hold the port until a vendor tick source is obtained",
                     effect="no T port until authoritative point-in-time increment data "
                            "exists",
                     recommended=None)],
            not_chosen_here="I am not selecting among these; each changes what the study "
                            "population means"),
        acceptance=acceptance,
        failed_acceptance=[k for k, v in acceptance.items() if not v],
        mutations=dict(base_spec=0, recovery_artifact=0, coarse_production=0, cohort=0,
                       t_production_writes=0, z_research_outputs=0, y_exposure=0),
        y_exposed=0,
        smoke_may_begin=False,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1.json",
                 required=("spec_id", "status", "supplements", "mintick",
                           "timestamp_alignment", "price_basis", "z7_internal",
                           "acceptance", "mutations", "decision_required"),
                 supersede=os.path.exists(
                     "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1.json"))
    print(f"MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1 · {d} · {p['status']}")
    print(f"  timestamp alignment  RESOLVED — legacy.date == Massive.bar_start (UTC, exact)")
    print(f"  price basis          same basis, legacy = Massive rounded to 2dp (indicative)")
    print(f"  Z7_raw               T_INTERNAL_SUPPORTING_PREDICATE")
    print(f"  mintick              NO AUTHORITY FOUND in any project source")
    print(f"    binds on 6.01% of Massive 15m bars (5,727 of 95,263 sampled)")
    print(f"    gates bodyRatioOk -> fullyEngulfs -> T4/T6, priority ranks 1 and 2")
    print(f"  failed acceptance    {p['failed_acceptance']}")
    print(f"  smoke may begin      {p['smoke_may_begin']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
