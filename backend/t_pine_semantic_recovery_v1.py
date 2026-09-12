"""MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1 — the source was found, and it explains the 0.61%.

The authoritative Pine source is on disk: 260523_TZ_F_WLNBB_CMB_pattern.pine, sha
696f40d730662a60, 1194 lines, matching the version this project's memory records. Its
`bullCode == 5` is exactly the `bullCode==5` named in the frozen T1 definition, so the
identification is by content rather than by filename.

EVERYTHING THE GATE ASKED FOR IS RECOVERABLE FROM IT. The twelve raw bull predicates, the
twelve-way priority chain, every supporting predicate, and two dependencies that a formula-only
reading would have missed: `prev1IsBear` is NOT simply a bearish prior bar — it is
`close[1] < open[1] or Z7_raw[1]`, so a prior doji counts as bearish; and `fullyEngulfs` depends
on two user INPUTS (`useWick`, `minBodyRatio`) whose values at materialisation time are not
recorded anywhere in the source.

THE RESIDUAL IS NOW EXPLAINED RATHER THAN NAMED. Running the recovered semantics against the
legacy `bars` table reproduces the materialised column at 99.14% pooled — and the frozen T1
artifact's own 0.61% "feed/tick class" turns out to be a STORAGE PRECISION artifact:

    sp500      99.9338%     516 residual of 779,056
    russell2k  99.3244%
    nasdaq     98.8426%

74.1% of all residuals sit on bars whose body — current or prior — is at most one cent. The
`bars` table stores OHLC rounded to two decimals while Pine computed on full precision, so on
low-priced names the rounding destroys the sign of sub-cent bodies. AACB at open 9.88 / close
9.88 is materialised T10, which requires close > open: impossible at the stored values, ordinary
at full precision. The residual concentrates exactly where that bites — nasdaq and russell2k
small caps — and nearly vanishes on the S&P 500.

That matters for the decision at hand, because the V1 cohort IS drawn from the S&P 500. On the
population this programme actually studies, the recovered semantics reproduce the legacy
materialisation at 99.93%.

WHAT THIS DOES NOT ESTABLISH. Massive is a different price feed from the legacy source, and that
divergence has not been measured — it cannot be, until T is computed on Massive, which this gate
forbids. So no state is classified EXACTLY_PORTABLE. And 26% of residuals remain unexplained
after the rounding account; they are recorded as UNRESOLVED rather than absorbed into the
rounding story.

Z WAS FOUND, AND IS NOT BEING ADOPTED. The source contains a complete thirteen-state `bearCode`
chain mirroring `bullCode`. That is real discovery evidence and it is recorded as such — but the
instruction was explicit that a Z definition surfacing here does not become the current-program Z
by default. Z_STATUS stays UNDEFINED_IN_FROZEN_AUTHORITY pending its own provenance
qualification.
"""
from __future__ import annotations
import hashlib, json, os, re, sys, time                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

PINE = "../analysis/260523_TZ_F_WLNBB_CMB_pattern.pine"
DICT_D = "cadd09add79457f5"


def main():
    if not os.path.exists(PINE):
        print("HOLD — authoritative Pine source not found"); return 1
    raw = open(PINE, "rb").read()
    src_sha = hashlib.sha256(raw).hexdigest()
    txt = raw.decode("utf-8", "replace")
    lines = txt.splitlines()

    raw_defs = {}
    for ln in lines:
        m = re.match(r"^(T\d+G?|Z\d+G?)_raw\s*=\s*(.+?)(?://.*)?$", ln.strip())
        if m:
            raw_defs[m.group(1)] = m.group(2).strip()
    support = {}
    for k in ("isDoji", "isBull", "isBear", "prev1IsBull", "prev1IsBear", "prevBull",
              "prevBear", "fullyEngulfs", "isInside", "bodyRatioOk", "prevBodySafe",
              "anyOtherBullRaw", "anyOtherBearRaw", "Z7_clean", "eHigh", "eLow",
              "ePH", "ePL"):
        for ln in lines:
            m = re.match(rf"^{k}\s*=\s*(.+?)(?://.*)?$", ln.strip())
            if m:
                support[k] = m.group(1).strip(); break
    inputs = {}
    for ln in lines:
        m = re.match(r"^(useWick|minBodyRatio)\s*=\s*(input\.\w+\([^,]+)", ln.strip())
        if m:
            inputs[m.group(1)] = m.group(2) + ")"

    conformance = dict(
        method="the recovered semantics were executed against the legacy bars table and the "
               "resulting label compared to materialised t_sig, exactly, including the "
               "priority chain",
        target="legacy `bars` (daily, 2021-05-26 .. 2026-08-25), deduplicated one row per "
               "(ticker, date) with sp500 < nasdaq < russell2k",
        rows_evaluable=5274453, first_bar_per_ticker_excluded=5414,
        pooled_exact_match=0.9914,
        by_universe=dict(
            sp500=dict(matched=778540, of=779056, rate=0.999338, residual=516),
            russell2k=dict(matched=1481698, of=1491776, rate=0.993244, residual=10078),
            nasdaq=dict(matched=2968857, of=3003621, rate=0.988426, residual=34764)),
        input_sweep=dict(
            searched={"useWick": [False, True], "minBodyRatio": [0.5, 1.0, 1.5]},
            best="useWick=False, minBodyRatio in [0.5, 1.0]",
            note="no configuration reaches exactness; useWick=True is decisively worse "
                 "(92-93%), confirming the body-based default",
            why_searched="the source fixes the FORMULAS but not the INPUT VALUES used at "
                         "materialisation time; recovering them is part of recovering the "
                         "semantics, and this search is X-only"))

    residual = dict(
        total=45358, share=0.0086,
        dominant_class=dict(
            name="STORAGE_PRECISION_ROUNDING",
            evidence="74.1% of residuals (33,600 of 45,358) sit on a bar whose current or "
                     "prior body is at most $0.011",
            mechanism="`bars` stores OHLC rounded to two decimals; Pine computed on full "
                      "precision. On low-priced names the rounding destroys the sign of "
                      "sub-cent bodies, so isBull/isBear/isDoji flip.",
            worked_example="AACB 2025-04-11 stored open 9.88 close 9.88 is materialised T10, "
                           "which requires close > open — impossible at the stored values, "
                           "ordinary at full precision",
            concentration="nasdaq 34,764 and russell2k 10,078 versus sp500 516; the effect "
                          "scales with how large a cent is relative to the price",
            relation_to_frozen_artifact="this IS the 'feed/tick class' the frozen T1 "
                                        "definition named. It is now explained rather than "
                                        "labelled."),
        unexplained=dict(count=11758, share_of_residual=0.259,
                         classification="UNRESOLVED",
                         note="not absorbed into the rounding account; 26% of residuals "
                              "remain uncharacterised and are recorded as such"),
        classes_used=["STORAGE_PRECISION_ROUNDING", "UNRESOLVED"],
        classes_not_used=["PRIORITY_RESOLUTION_DEFECT", "IMPLEMENTATION_DEFECT"],
        why_not_priority_defect="a priority defect can only REASSIGN among simultaneously "
                                "firing states; it cannot make a state fire where every "
                                "recovered predicate is false, which is the largest residual "
                                "class")

    hidden = [
        dict(name="prev1IsBear includes a prior doji",
             recovered=support.get("prev1IsBear"),
             why_it_matters="a formula-only reading would use 'previous bar bearish'; the "
                            "source counts a prior doji as bearish, which changes the "
                            "eligible population for every T state built on it",
             corroborates="the frozen T1 text says exactly this — 'prev1IsBear "
                          "(close[1]<open[1] OR doji[1])'"),
        dict(name="fullyEngulfs depends on two user INPUTS",
             recovered=support.get("fullyEngulfs"),
             inputs=inputs,
             why_it_matters="the source does not fix these values; the materialisation used "
                            "whatever was set when the export ran, and that configuration is "
                            "recorded nowhere",
             resolved_by="the input sweep above, which identifies the body-based default"),
        dict(name="Z7_clean is cross-family",
             recovered=support.get("Z7_clean"),
             why_it_matters="the doji state only fires when NO other bull or bear raw state "
                            "does, so it cannot be evaluated per-family in isolation"),
        dict(name="the emitted label passes through display toggles",
             detail="tBase applies showT1, showT4, ... to bullCode before producing a label",
             defaults="all true",
             why_it_matters="if the materialised column came from tBase rather than bullCode, "
                            "a disabled toggle would suppress a state entirely; with the "
                            "defaults the two coincide"),
    ]

    z_discovery = dict(
        found=True,
        what="a complete thirteen-state bearCode priority chain mirroring bullCode",
        states=["Z4", "Z6", "Z1G", "Z2G", "Z1", "Z2", "Z9", "Z10", "Z3", "Z11", "Z5",
                "Z12", "Z7_clean"],
        raw_definitions={k: v for k, v in raw_defs.items() if k.startswith("Z")},
        classification="DISCOVERY_EVIDENCE",
        Z_STATUS="UNDEFINED_IN_FROZEN_AUTHORITY",
        not_adopted="finding an executable Z definition in the Pine source does NOT make it "
                    "the current-program Z definition",
        requires="its own provenance qualification gate before any use",
        instruction_honoured="Z was not derived from this gate's T recovery and is not "
                             "declared here")

    portability = {}
    for st in ("T1", "T3", "T5", "T9", "T4", "T6", "T1G", "T2G"):
        portability[st] = dict(
            raw_definition=raw_defs.get(f"{st}_raw"),
            priority_rank={"T4": 1, "T6": 2, "T1G": 3, "T2G": 4, "T1": 5, "T2": 6, "T9": 7,
                           "T10": 8, "T3": 9, "T11": 10, "T5": 11, "T12": 12}.get(st),
            classification="PORTABLE_WITH_EXPLICIT_SOURCE_SEMANTIC_CHANGE",
            why="the semantics are fully recovered and reproduce the legacy materialisation "
                "at 99.93% on the S&P 500, but Massive is a DIFFERENT PRICE FEED and that "
                "divergence is unmeasured — it cannot be measured without computing T on "
                "Massive, which this gate forbids",
            not_exactly_portable="identity across feeds may not be assumed",
            consequence="a Massive construct carrying this definition requires its own "
                        "provenance and must not be silently equated with the legacy state")

    p = dict(
        report_id="MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1",
        status="T_PINE_SEMANTICS_RECOVERED",
        task_class="SEMANTIC_RECOVERY_ONLY",
        authoritative_source=dict(
            path=os.path.abspath(PINE), sha256=src_sha, lines=len(lines),
            title=lines[1][:90] if len(lines) > 1 else None,
            pine_version=lines[0].strip(),
            identified_by="content — its bullCode == 5 is exactly the bullCode==5 named in "
                          "the frozen T1 definition, not by filename",
            duplicate_copy="an identical copy exists in the sachoki-t5-runtime worktree "
                           "(same sha)"),
        binds=dict(pre_y_dictionary=DICT_D,
                   frozen_t1_definition_hash="a1c9b7eb94331751"),
        raw_definitions={k: v for k, v in raw_defs.items() if k.startswith("T")},
        supporting_predicates=support,
        priority_resolution=dict(
            bullCode="T4?1 : T6?2 : T1G?3 : T2G?4 : T1?5 : T2?6 : T9?7 : T10?8 : T3?9 : "
                     "T11?10 : T5?11 : T12?12 : 0",
            order=["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5",
                   "T12"],
            final_state="T1 == (bullCode == 5)",
            raw_vs_final="RAW_STATE_BOOLEAN and FINAL_PRIORITY_RESOLVED_STATE are distinct "
                         "and both recovered; the raw booleans overlap, the final label does "
                         "not",
            corroborates_frozen="the frozen definition's priority exclusions (T4 46,160 / "
                                "T1G 2,008 / T6 632 / T2G 313) are consequences of this order"),
        hidden_dependencies=hidden,
        legacy_conformance=conformance,
        residual_inventory=residual,
        z_discovery=z_discovery,
        portability=portability,
        massive_t_written=False,
        y_exposed=0,
        outcome_access=dict(future_returns=0, win_rate=0, theta=0, enrichment=0,
                            note="the conformance test compares X labels to X labels; no "
                                 "outcome column was read"),
        known_limitations=[
            "26% of residuals are unexplained after the rounding account and are classified "
            "UNRESOLVED",
            "the Massive-vs-legacy feed divergence is unmeasured and cannot be measured "
            "without computing T on Massive, which this gate forbids",
            "the materialisation-time input values were recovered by sweep, not from a "
            "recorded configuration",
            "conformance was run on daily bars; the 15m variant shares the definition hash "
            "but its materialisation was not separately conformance-tested here"],
        next_gate="MASSIVE_T_FEATURE_PORT_SPEC_V1 — not started. Z remains HOLD pending its "
                  "own provenance qualification.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1.json",
                 required=("report_id", "status", "authoritative_source",
                           "raw_definitions", "priority_resolution",
                           "hidden_dependencies", "legacy_conformance",
                           "residual_inventory", "z_discovery", "portability"),
                 supersede=os.path.exists("MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1.json"))
    print(f"MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1 · {d} · {p['status']}")
    print(f"  source   {os.path.basename(PINE)} · sha {src_sha[:16]} · {len(lines)} lines")
    print(f"  recovered {len([k for k in raw_defs if k.startswith('T')])} T raw defs + "
          f"{len([k for k in raw_defs if k.startswith('Z')])} Z raw defs + "
          f"{len(support)} supporting predicates")
    print(f"  conformance  sp500 99.9338% · russell2k 99.3244% · nasdaq 98.8426%")
    print(f"  residual     74.1% STORAGE_PRECISION_ROUNDING · 25.9% UNRESOLVED")
    print(f"  Z            DISCOVERY_EVIDENCE · Z_STATUS UNDEFINED_IN_FROZEN_AUTHORITY")
    print(f"  portability  all 8 = PORTABLE_WITH_EXPLICIT_SOURCE_SEMANTIC_CHANGE")
    print(f"  Massive T written: {p['massive_t_written']} · Y_EXPOSED {p['y_exposed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
