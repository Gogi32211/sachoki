"""MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION — a status, not a pardon.

C2.5 reported 0.999171 and "EXACT False". Left there, that reads as a failed conformance test
carried forward on sufferance. It is not one, and the distinction is worth its own artifact:

    EXACT REPRODUCTION FAILED
        the authoritative same-input surface exists and the engine did not match it

    EXACT REPRODUCTION IS NOT TESTABLE
        the authoritative same-input surface was demonstrably not preserved

The daily anchor is the second. The writer and producer are positively resolved by call path;
what is gone is the full-precision frame that path consumed, because the exporter rounded to two
decimals before anything was stored.

SO THE RULE IS SHARPENED RATHER THAN RELAXED. Where an authoritative same-input surface exists,
semantic conformance must be EXACT — that stands, and 15m met it. Where the input surface is
provably not preserved, exactness is not replaced by a tolerance; the reproducibility status
becomes NOT_VERIFIABLE_FROM_PRESERVED_INPUTS. No threshold is created, 0.999171 becomes no one's
acceptance bar, and nothing is corrected or tuned toward it.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

BASE, BASE_D = "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1.json", "7b4a7ac5facbf0a7"
if ART.file_digest(BASE) != BASE_D:
    print("HOLD — base digest mismatch"); raise SystemExit(1)

p = dict(
    amendment_id="MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION",
    status="QUALIFIED_PROVENANCE",
    task_class="STATUS_QUALIFICATION_ONLY",
    qualifies=dict(artifact="MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1",
                   digest=BASE_D, disposition="FROZEN — not edited"),

    classification=dict(
        WRITER_AUTHORITY="RESOLVED",
        CANONICAL_PRODUCER="RESOLVED",
        TRUE_HISTORICAL_INPUT_SURFACE="IDENTIFIED_BY_CALL_PATH_BUT_NOT_PRESERVED",
        SAME_INPUT_DAILY_MATERIALIZATION_CONFORMANCE="NOT_VERIFIABLE_FROM_PRESERVED_INPUTS",
        STORED_2DP_RECONSTRUCTION=dict(value=0.999171, status="DIAGNOSTIC_ONLY"),
        MASSIVE_1D_RECONSTRUCTION=dict(value=0.916174,
                                       status="CROSS_SOURCE_DIAGNOSTIC_ONLY")),

    the_distinction=dict(
        not_this="EXACT REPRODUCTION FAILED — the authoritative same-input surface exists and "
                 "the engine did not match it",
        but_this="EXACT REPRODUCTION IS NOT TESTABLE — the authoritative same-input surface "
                 "was demonstrably not preserved",
        evidence="0 of 780,261 stored sp500 rows carry more than two decimals; the exporter "
                 "rounds before writing"),

    sharpened_rule=dict(
        where_input_surface_exists="semantic conformance must be EXACT — unchanged, and 15m "
                                   "met it on qualified support",
        where_input_surface_not_preserved="exactness is NOT replaced by a tolerance; the "
                                          "status becomes NOT_VERIFIABLE_FROM_PRESERVED_INPUTS",
        explicitly_not="a relaxation of the exact-or-HOLD rule"),

    prohibitions=["NO TOLERANCE ACCEPTED", "NO 0.999171 THRESHOLD CREATED",
                  "NO LABEL CORRECTION", "NO PARAMETER TUNING",
                  "0.999171 may not become an expected Massive agreement",
                  "0.999171 may not become an estimate of true historical semantic error",
                  "0.916174 does not mean the daily engine fails to reproduce itself — it is "
                  "cross-source population sensitivity"],

    consequence_for_c3=dict(
        authorized=True,
        allowed_claim=("The historical daily T1 selector semantics are ported to Massive 1D "
                       "source data using the resolved canonical producer path. Exact "
                       "same-input reproduction of the historical daily materialization "
                       "cannot be tested, because the unrounded historical input frame was "
                       "not preserved."),
        forbidden_claim="Massive daily T1 exactly reproduces the historical daily T1 "
                        "materializer population",
        historical_diagnostic_authority="the PRESERVED positive episode set "
                                        "(t1_episodes.parquet, 220,351)",
        forbidden_diagnostic_authority="engine(stored rounded daily OHLC) used as truth"),

    y_exposed=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION.json",
             required=("amendment_id", "status", "classification", "the_distinction",
                       "sharpened_rule", "prohibitions", "consequence_for_c3"),
             supersede=os.path.exists(
                 "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION.json"))
print(f"MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION · {d} · {p['status']}")
print("  writer / producer            RESOLVED")
print("  true historical input        IDENTIFIED BY CALL PATH BUT NOT PRESERVED")
print("  same-input daily conformance NOT_VERIFIABLE_FROM_PRESERVED_INPUTS  (not FAILED)")
print("  0.999171 / 0.916174          DIAGNOSTIC_ONLY · no threshold created")
print("  C3                           AUTHORIZED · preserved positives are the diagnostic")
print("                               authority, NOT rounded-OHLC reconstruction")
