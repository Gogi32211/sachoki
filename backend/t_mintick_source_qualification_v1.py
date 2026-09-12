"""MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1 — the answer was already frozen in this project.

The gate asked me to obtain a TradingView syminfo.mintick snapshot for 476 symbols. Before
launching anything I inventoried existing authorities, and the search returned a frozen artifact
that settles the question without TradingView:

    T4_ENGULF_CONFIG_V1 (FROZEN, 713f09144a245408)
      use_wick          false
      min_body_ratio    1.0
      mintick_semantics "prevBodySafe = max(prevBody, 1e-10); ratio = currBody/prevBodySafe"
      source_identity   analyzers/tz_wlnbb/config.py:51-55, signal_engine.py:67-69,97-122
      located_not_assumed "both call sites pass the config constants; no caller overrides"

MINTICK WAS NEVER syminfo.mintick. The legacy materialisation was produced by a PYTHON engine,
not by the Pine indicator, and that engine uses a fixed numerical epsilon of 1e-10 as a
divide-by-zero guard. Verified directly in source at analyzers/tz_wlnbb/signal_logic.py:140
(`prev_body_safe = max(prev_body, 1e-10)`) and signal_engine.py:99.

So the whole authority hunt was chasing a Pine construct the materialisation never evaluated.
There is no per-symbol tick metadata to obtain, no venue disambiguation to perform, and no
regulatory MPV question to settle — because no trading increment enters the computation at all.
1e-10 is an epsilon, not a tick size, and the 6.01% exposure I measured earlier was exposure to
a parameter that does not exist in the authoritative implementation.

WHICH ALSO MEANS MY PREVIOUS FRAMING WAS WRONG, and the correction matters more than the
finding. I treated the Pine script as the authority and measured the recovered Pine semantics
against the materialised column, reporting 99.14%. But T4_ENGULF_CONFIG_V1 had already ruled
that exact comparison invalid:

    invalid: "raw_T4(current_store_OHLC) == materialized_canonical_T4 — the two sides do not
              share an input vintage"
    valid:   "identical input bars + identical config + identical implementation => raw == canonical"
    result:  "PASSED — engine_vs_SQL_same_input 78,181/78,181, 0 mismatches"

Under the valid invariant the legacy engine reproduces its own column EXACTLY. My 0.86%
residual was not a semantic gap; it was the invalid comparison a frozen artifact had already
retired.

AND THE INVENTORY FOUND A REAL INCONSISTENCY IN THE REPO, which is reported rather than fixed.
Two Python implementations disagree on isDoji:

    signal_logic.py:117-118   `# Pine 260506: isDoji = close == open (exact equality)`
                              `is_doji = (c == o)`            doji_thresh present but UNUSED
    signal_engine.py:101      `isDoji = (rng>0) & ((bdy/rng) <= doji_thresh)`   thresh = 0.05

They cannot both be the materialiser. The data says signal_logic.py is: exact-equality doji
reproduces the column at 99.14% against threshold-doji's 98.10%. That also corroborates the
frozen artifact's doji_semantics claim — which cites signal_engine.py as its source identity
while describing signal_logic.py's behaviour. The citation appears to point at the wrong file.

isDoji feeds prev1IsBear, which gates T1, T3, T5, T9 and T1G — most of the family. Which
implementation a future port binds is therefore a material choice, and it is not mine to make.
"""
from __future__ import annotations
import json, os, re, sys, time                                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

AM1, AM1_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1.json", "de60bdc75fc61e3c"
BASE_D, REC_D, PINE_D = "545d293b4f1e4653", "7d00e3908feb26d5", "696f40d730662a60"
T4CFG, T4CFG_D = "T4_ENGULF_CONFIG_V1.json", "713f09144a245408"


def main():
    for f, d in ((AM1, AM1_D), (T4CFG, T4CFG_D)):
        if ART.file_digest(f) != d:
            print(f"HOLD — {f} digest mismatch"); return 1
    t4 = json.load(open(T4CFG))
    src_ok = {}
    for path, pat in (("analyzers/tz_wlnbb/signal_logic.py", r"prev_body_safe\s*=\s*max\("),
                      ("signal_engine.py", r"mintick\s*=\s*1e-10")):
        src_ok[path] = bool(os.path.exists(path)
                            and re.search(pat, open(path).read()))

    p = dict(
        spec_id="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1",
        status="MINTICK_SOURCE_QUALIFIED",
        task_class="SOURCE_QUALIFICATION_ONLY",
        unblocks=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1", digest=AM1_D,
                      previous_status="HOLD",
                      resolved_gap="Massive-port mintick binding"),

        headline=dict(
            finding="mintick was never syminfo.mintick",
            detail="the legacy materialisation was produced by a PYTHON engine, not the Pine "
                   "indicator. That engine uses a fixed numerical epsilon of 1e-10 as a "
                   "divide-by-zero guard.",
            consequence="no per-symbol tick metadata exists to obtain, no venue "
                        "disambiguation is needed, and no regulatory MPV question arises — "
                        "no trading increment enters the computation at all",
            tradingview_needed=False,
            app_launch_avoided=True),

        authority=dict(
            artifact="T4_ENGULF_CONFIG_V1", digest=T4CFG_D, status=t4["status"],
            mintick_semantics=t4["mintick_semantics"],
            use_wick=t4["use_wick"], min_body_ratio=t4["min_body_ratio"],
            source_identity=t4["source_identity"],
            located_not_assumed=t4["located_not_assumed"],
            classification="ALREADY_FROZEN_PROJECT_AUTHORITY",
            hierarchy_rank="pre-empts the TradingView search entirely — the value is not a "
                           "tick size and TradingView is not its authority"),

        source_verification=dict(
            checked=src_ok,
            signal_logic_line="analyzers/tz_wlnbb/signal_logic.py:140  "
                              "prev_body_safe = max(prev_body, 1e-10)",
            signal_engine_line="signal_engine.py:99  mintick = 1e-10",
            config_constants="analyzers/tz_wlnbb/config.py: USE_WICK=False, "
                             "MIN_BODY_RATIO=1.0, DOJI_THRESH=0.05",
            epsilon_not_tick="1e-10 is a divide-by-zero guard, not a trading increment"),

        supersedes_amendment_1_requirement=dict(
            amendment_1_demanded="an authoritative point-in-time tick-size source with "
                                 "effective_from / effective_to per security",
            why_that_was_wrong="it presumed the materialisation evaluated syminfo.mintick. It "
                               "did not. The requirement was built on a Pine construct the "
                               "legacy pipeline never used.",
            user_correction_acknowledged="syminfo.mintick is symbol-level static metadata in "
                                         "Pine, not a per-bar PIT series — and here it is not "
                                         "used at all",
            amendment_1_disposition="remains an immutable HOLD artifact; not edited",
            no_fallback_rule="not violated — 1e-10 is not a fallback, it is the frozen "
                             "authoritative value from the implementation that produced the "
                             "column"),

        correction_to_my_own_prior_framing=dict(
            severity="MATERIAL — this corrects the recovery artifact's comparison, not just a "
                     "detail",
            what_i_did="treated the Pine script as the authority and measured recovered Pine "
                       "semantics against the materialised column, reporting 99.14% and "
                       "characterising the residual",
            what_was_already_frozen=t4["invariant_amended"]["invalid"],
            valid_invariant=t4["invariant_amended"]["valid"],
            frozen_result=t4["invariant_amended"]["result"],
            meaning="under the valid invariant the legacy engine reproduces its own column "
                    "EXACTLY (78,181/78,181, 0 mismatches). My 0.86% residual was not a "
                    "semantic gap — it was the invalid comparison a frozen artifact had "
                    "already retired.",
            recovery_artifact_disposition="MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1 "
                                          "(7d00e3908feb26d5) remains immutable; its Pine "
                                          "extraction is correct and independently useful, "
                                          "but its conformance FRAMING should be read "
                                          "alongside this correction",
            not_edited=True),

        repo_inconsistency_found=dict(
            severity="MATERIAL — reported, not fixed",
            two_implementations_disagree_on="isDoji",
            signal_logic=dict(
                path="analyzers/tz_wlnbb/signal_logic.py:117-118",
                code="is_doji = (c == o)",
                comment="# Pine 260506: isDoji = close == open (exact equality)",
                doji_thresh="present as a parameter, UNUSED in is_doji"),
            signal_engine=dict(
                path="signal_engine.py:101",
                code="isDoji = (rng>0) & ((bdy/rng) <= doji_thresh)",
                doji_thresh=0.05, used=True),
            which_is_the_materialiser=dict(
                answer="signal_logic.py",
                evidence="exact-equality doji reproduces the materialised column at 99.14%; "
                         "threshold doji reproduces it at 98.10%",
                measured_on="legacy daily bars, deduplicated, full population"),
            frozen_artifact_citation_issue=dict(
                claim=t4["doji_semantics"],
                cites="signal_engine.py:67-69,97-122",
                problem="the claim describes signal_logic.py's behaviour while citing "
                        "signal_engine.py, which does use the threshold",
                assessment="the CLAIM is supported by the data; the CITATION appears to point "
                           "at the wrong file",
                not_resolved_here=True),
            why_it_matters="isDoji feeds prev1IsBear, which gates T1, T3, T5, T9 and T1G — "
                           "most of the family. Which implementation a future port binds is a "
                           "material choice and is not mine to make."),

        per_security_binding=dict(
            model="a single scalar constant, not a per-security table",
            value=1e-10,
            applies_to="all 476 securities uniformly",
            symbol_mapping_required=False,
            venue_disambiguation_required=False,
            unresolved_securities=0,
            coverage=1.0,
            why="the value is implementation-level, not instrument-level"),

        legacy_runtime_status=dict(
            LEGACY_RUNTIME_MINTICK="KNOWN — 1e-10, from the implementation that produced the "
                                   "column",
            note="this is a stronger statement than Amendment 1 could make, because the "
                 "authority turned out to be the materialising code itself rather than "
                 "external metadata",
            use_wick="False (frozen)", min_body_ratio="1.0 (frozen)",
            still_unknown="nothing required for mintick"),

        regulatory_consistency_audit=dict(
            performed=False,
            reason="not applicable — 1e-10 is an epsilon guard, so comparing it to an "
                   "exchange minimum price variation would be a category error",
            not_collected="SEC Rule 612 / venue MPV values were not adopted or compared"),

        carried_forward=dict(
            timestamp_alignment=dict(status="RESOLVED",
                                     rule="legacy.date == Massive.bar_start, exact, UTC bar "
                                          "start",
                                     not_reopened=True),
            price_basis=dict(status="INDICATIVE / REQUIRES FULL MATCHED CONFIRMATION",
                             not_upgraded=True,
                             note="exact 2-decimal rounding equivalence demonstrated on "
                                  "limited matched evidence only"),
            z7=dict(classification="T_INTERNAL_SUPPORTING_PREDICATE",
                    Z_research_family="HOLD")),

        forbidden_inference_not_used=["OHLC decimal places", "minimum observed price "
                                                             "difference",
                                      "minimum non-zero return", "legacy 2-decimal storage",
                                      "ticker", "price level", "security type convention",
                                      "assumed $0.01", "agreement-maximizing search"],
        y_exposed=0, t_production_writes=0, z_research_outputs=0,
        smoke_started=False, port_spec_amended=False,

        decision_required=dict(
            summary="the mintick gap is closed, but the inventory raised a question that "
                    "must be answered before the port proceeds",
            question="which implementation is the authoritative semantic target for the "
                     "Massive port — signal_logic.py (exact-equality doji, matches the "
                     "materialisation) or the Pine indicator (also exact-equality, but a "
                     "sibling implementation)?",
            secondary="whether the T4_ENGULF_CONFIG_V1 source citation should be corrected by "
                      "its own amendment",
            not_chosen_here=True),
        next_step="a narrow amendment binding mintick=1e-10 and resolving the implementation "
                  "authority question. Smoke remains not started.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1.json",
                 required=("spec_id", "status", "headline", "authority",
                           "source_verification", "correction_to_my_own_prior_framing",
                           "repo_inconsistency_found", "per_security_binding",
                           "carried_forward"),
                 supersede=os.path.exists(
                     "MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1.json"))
    print(f"MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1 · {d} · {p['status']}")
    print(f"  authority   T4_ENGULF_CONFIG_V1 ({T4CFG_D}) FROZEN — already in the project")
    print(f"  mintick     1e-10 — an EPSILON GUARD, not syminfo.mintick, not a tick size")
    print(f"  coverage    476/476, single scalar; no symbol mapping, no venue work needed")
    print(f"  TradingView not required · no app launched")
    print(f"  source ver  {src_ok}")
    print(f"  correction  my 99.14% Pine comparison was already ruled an INVALID invariant;")
    print(f"              the valid one PASSED 78,181/78,181 with 0 mismatches")
    print(f"  found       signal_logic.py and signal_engine.py DISAGREE on isDoji")
    print(f"  Y_EXPOSED {p['y_exposed']} · smoke started {p['smoke_started']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
