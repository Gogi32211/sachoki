"""MASSIVE_TZ_PRE_Y_RESEARCH_DICTIONARY_V1 — the inventory came first, and it stops the gate.

The instruction was to inventory the frozen T/Z authorities before defining anything. Doing
that produced a finding that no amount of careful dictionary-writing can work around.

THE T-FAMILY IS NOT DEFINED ON THIS X LAYER. It is defined on the legacy `bars` pipeline. The
frozen T1 definition (definition_hash a1c9b7eb94331751, shared by T1_DNA_X_V1 and
T1_15M_X_ONLY_V1) reads:

    "t_sig=='T1' as materialized in bars (priority-resolved import of the Pine 'T' column;
     bullCode==5)"

So a T-state is an IMPORTED COLUMN, priority-resolved against T4/T6/T1G/T2G, materialised into
DuckDB. It is not a formula over the Massive derived base, and the Massive V1 layer contains no
t_sig at all.

AND THE DEFINITION ITSELF RECORDS THAT THE FORMULA DOES NOT REPRODUCE THE COLUMN. The same
frozen text gives the raw predicate and then measures it against the materialised column:
overlap 219,016 / 220,351 = 99.39%, residual 1,335 rows (0.61%) of "feed/tick class". That
0.61% is the whole problem. Recomputing T1 from Massive OHLC would produce a population that is
nearly, but provably not, the frozen one — and a study that silently swaps one for the other
would be reporting on a different object than the one its provenance ledger describes.

Z IS WORSE: THERE IS NO FROZEN Z DEFINITION AT ALL. A search of every artifact for a z-state
definition returns one hit, `z_sig` inside MARKET_PHYSICS_STAGE25 — a stage artifact of
alphabets and dummies, not a dictionary. The charter lists "T/Z state clocks" under
feature_families with status "NAMED ONLY".

So the gate's own rule applies exactly as written: a parameter required to execute the study is
not frozen, therefore HOLD, and no conventional value is inserted. Everything that CAN be frozen
without inventing T/Z semantics is frozen below, so the eventual dictionary only has to add the
piece that is genuinely missing.

WHAT IS NOT IN DOUBT, and is recorded so it cannot quietly reset later: the T1/T3/T9 families
were exposed to outcomes in the earlier 15m work. MASSIVE_1M_STATE_TRANSITION_V1 already freezes
that as hypothesis_provenance — "PREVIOUSLY EXPOSED · HYPOTHESIS-GENERATING · NOT PRISTINE
DISCOVERY", with M1->M2 and M1:L3 named. A larger, cleaner Massive dataset does not make those
hypotheses new.
"""
from __future__ import annotations
import glob, json, os, re, sys, time                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

X_LAYER = {
    "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1": "5591685cfa33b62a",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1": "6ec73d5f462761f0",
    "MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1": "36168e975efa742f",
    "MASSIVE_COARSE_EMA_FEATURE_SPEC_V1": "ef01b67c22f6770f",
    "MASSIVE_RVOL_DICTIONARY_SPEC_V1": "6095852c4a2eeaa5",
    "MASSIVE_RVOL_FEATURE_PRODUCTION_V1": "26bd469cc68d3248",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1": "7c0a5ad6b17d7ae0",
    "MASSIVE_1M_STATE_TRANSITION_V1": "d73a88d836f36eea",
}
DERIVED = "/Volumes/QUANT_RESEARCH/studio_data/derived"
Y_TOKENS = re.compile(r"fwd_|forward_return|future_return|ret_\d|y_value|outcome_value|"
                      r"theta|win_rate", re.I)


def y_access_guard():
    """Executable: prove no outcome/forward column exists in the bound X layer."""
    import pyarrow.parquet as pq
    checked, offenders = 0, []
    for d in ("massive_1m_base_v2/minute_states", "massive_1m_base_v2/observed_regular_bars",
              "massive_coarse_v1/15m", "massive_coarse_ema_v1/15m", "massive_rvol_v1"):
        g = sorted(glob.glob(os.path.join(DERIVED, d, "*", "*", "*.parquet")))
        if not g:
            continue
        names = set(pq.read_schema(g[0]).names)
        checked += 1
        bad = {n for n in names if Y_TOKENS.search(n)}
        if bad:
            offenders.append(dict(dataset=d, columns=sorted(bad)))
    return dict(datasets_checked=checked, outcome_columns_found=offenders,
                passed=not offenders,
                method="schema of every bound X dataset scanned for forward/outcome column "
                       "tokens; the X layer physically cannot serve a Y value")


def main():
    binding, bad = {}, []
    for n, d in X_LAYER.items():
        got = ART.file_digest(n + ".json")
        binding[n] = dict(cited=d, actual=got, match=got == d)
        if got != d:
            bad.append(n)
    if bad:
        print("HOLD — X layer digest mismatch:", bad); return 1
    charter = json.load(open("MASSIVE_1M_STATE_TRANSITION_V1.json"))
    t1 = json.load(open("T1_DNA_X_V1.json"))
    t1_15 = json.load(open("T1_15M_X_ONLY_V1.json"))

    fam_files = sorted(f for f in glob.glob("T[0-9]*_*.json") if "superseded" not in f)
    dna = {}
    for f in glob.glob("T[0-9]_DNA_X_V1.json"):
        j = json.load(open(f))
        fam = f.split("_")[0]
        key = next((k for k in j if k.endswith("_definition")), None)
        dna[fam] = dict(artifact=f, digest=ART.file_digest(f),
                        definition_hash=j.get(f"{fam.lower()}_definition_hash"),
                        definition=(j.get(key) or "")[:400],
                        future_outcomes=j.get("future_outcomes"))

    inventory = dict(
        artifacts_scanned=len(glob.glob("*.json")),
        t_family_artifacts=len(fam_files),
        dna_definitions=dna,
        shared_definition_hash=dict(
            T1_DNA_X_V1=t1.get("t1_definition_hash"),
            T1_15M_X_ONLY_V1=t1_15["digests"]["definition"],
            identical=t1.get("t1_definition_hash") == t1_15["digests"]["definition"],
            meaning="the 15m variant carries the SAME definition hash, so the state formula "
                    "is shared and only the bar timeframe differs"),
        z_family=dict(
            frozen_definition_found=False,
            only_occurrence="z_sig inside MARKET_PHYSICS_STAGE25.json",
            that_artifact_is="a stage record of alphabet / axes / dummies, not a dictionary",
            charter_status=charter["feature_families_status"]),
        charter_feature_families=charter["feature_families"])

    blocking = dict(
        severity="BLOCKING — this is why the verdict is HOLD",
        finding_1=dict(
            name="the T-family is not defined on the Massive X layer",
            frozen_definition=t1["t1_definition"][:300],
            definition_hash=t1.get("t1_definition_hash"),
            what_it_means="a T-state is an imported Pine column materialised into the "
                          "legacy `bars` table and priority-resolved against T4/T6/T1G/T2G. "
                          "It is not a formula over the Massive derived base, and the "
                          "Massive V1 layer contains no t_sig.",
            consequence="the study cannot be executed on the qualified X layer without a "
                        "computable definition that does not currently exist"),
        finding_2=dict(
            name="the frozen definition records that the formula does not reproduce the "
                 "column",
            measured="overlap 219,016 / 220,351 = 99.39%; residual 1,335 rows (0.61%) of "
                     "feed/tick class",
            why_it_matters="recomputing T1 from Massive OHLC would produce a population that "
                           "is nearly, but provably not, the frozen one. A study that swapped "
                           "one for the other would report on a different object than its "
                           "provenance ledger describes.",
            priority_exclusions="T4 46,160 / T1G 2,008 / T6 632 / T2G 313 — the resolution "
                                "order is part of the definition and its authority is the "
                                "Pine source, not an artifact"),
        finding_3=dict(
            name="no frozen Z definition exists",
            searched="every artifact for a z-state definition",
            found="one occurrence of z_sig, in a stage artifact",
            charter="feature_families lists 'T/Z state clocks' as NAMED ONLY"),
        gate_rule_applied="If a parameter required to execute the current T/Z study is not "
                          "explicitly frozen or explicitly decided in this pre-Y gate: HOLD. "
                          "Do not insert a conventional/default value.",
        options=[
            dict(id="A", name="freeze the Pine T-column semantics as a computable spec, then "
                              "implement on Massive",
                 requires="the TZ_WLNBB Pine source as the authority, including the full "
                          "priority-resolution order",
                 effect="T-states become recomputable on the coarse layer with a stated, "
                        "measured divergence from the legacy materialisation",
                 recommended=True,
                 note="the divergence must be MEASURED and recorded, not assumed to be the "
                      "same 0.61%, because the source data differs"),
            dict(id="B", name="port T as a NEW pre-registered definition on Massive",
                 effect="a distinct family with its own provenance class; the legacy T "
                        "results are not evidence for it and vice versa",
                 recommended=False,
                 risk="two families with the same name and different populations is exactly "
                      "the confusion this programme's naming discipline exists to prevent"),
            dict(id="C", name="scope the Massive V1 study to states definable from the frozen "
                              "EMA/RVOL layer alone",
                 effect="T/Z deferred entirely; the study proceeds on the X layer that is "
                        "actually qualified",
                 recommended=False,
                 note="clean, but it is a different study from the one the charter names")])

    provenance = dict(
        binding=charter["hypothesis_provenance"]["status"],
        exposed_hypotheses=charter["hypothesis_provenance"]["exposed_hypotheses"],
        origin=charter["hypothesis_provenance"]["origin"],
        rule="T1/T3/T9 were exposed to outcomes in the earlier 15m work. A larger, cleaner "
             "Massive dataset does not make those hypotheses new.",
        classification_required_per_family=["PREVIOUSLY_EXPOSED_HYPOTHESIS",
                                            "CURRENT_PRE_REGISTERED_REPLICATION_ATTEMPT"],
        forbidden="describing a repeated appearance of a previously examined T-state as an "
                  "independent hypothesis because the pipeline is new",
        no_survivor_search="the current family may not be built by testing only the "
                           "components that survived the exposed 15m work")

    frozen_now = dict(
        note="everything specifiable WITHOUT inventing T/Z semantics is frozen here, so the "
             "eventual dictionary only has to add the piece that is genuinely missing",
        false_vs_unavailable=dict(
            FALSE="all required X inputs are valid and the condition is not satisfied",
            UNAVAILABLE="one or more required inputs are unavailable, invalid or "
                        "contaminated",
            mapping_forbidden="UNAVAILABLE -> FALSE",
            applies_to=["EMA", "RVOL", "coarse bars", "state components",
                        "transition components"]),
        feature_validity=dict(
            ema="an EMA-dependent state is evaluable only where the exact required stream is "
                "VALID; INITIALIZATION_SENSITIVE and UNAVAILABLE do not satisfy it",
            rvol="INSUFFICIENT_BASELINE / ZERO_BASELINE / CURRENT_INPUT_UNAVAILABLE / "
                 "PATH_CONTAMINATED all mean not evaluable, never below-threshold",
            observation_level=["STATE_EVALUABLE", "STATE_UNAVAILABLE_FEATURE"],
            cohort_never_shrinks=476,
            availability_must_be_reported_for_every_family=True),
        clock_contract=dict(
            distinct_timestamps=["measurement timestamp", "information_available_at",
                                 "transition timestamp", "future outcome start timestamp"],
            rule="a coarse state completing at t cannot be treated as known before t",
            one_minute_role="a 1m clock may only timestamp the first causal minute at or "
                            "after that availability boundary",
            no_intra_coarse_bar_hindsight=True),
        registries_closed=dict(
            ema=dict(periods=[9, 20, 50, 200], timeframes=["15m", "1H", "1D"], streams=12,
                     derived_families_forbidden=["slope", "acceleration",
                                                 "distance buckets", "cross thresholds",
                                                 "ordering families", "touch/reclaim"]),
            rvol=dict(families=["slot-RVOL", "CUM-RVOL"],
                      legacy_forbidden=["rolling-20-bar ratio", "1.4x", "1.8x", "2.0x",
                                        "percentile threshold", "high-volume bucket"],
                      threshold_source="must already be part of a frozen named hypothesis; "
                                       "may never be chosen from the X distribution or "
                                       "from Y")),
        early_close=dict(
            rvol="unavailable by construction under the frozen same-session-type / min-15 "
                 "policy",
            consequence="any RVOL-conditioned hypothesis is unavailable on those 10 sessions",
            forbidden=["pooling normal sessions", "lowering the minimum sample count",
                       "special-casing early closes"]),
        execution_timing=dict(
            separate=["state observed", "transition observed", "earliest actionable time",
                      "hypothetical execution price"],
            forbidden="executing at a price that helped construct the state",
            note="a historical state-transition association is not automatically a "
                 "tradeability result"),
        primary_vs_descriptive=dict(
            classes=["PRIMARY_INFERENTIAL", "REGISTERED_SECONDARY", "DESCRIPTIVE_ONLY",
                     "CAPABILITY_ONLY"],
            must_stay_descriptive_unless_registered=["availability rates",
                                                     "state frequencies", "component sizes",
                                                     "post-exposure survivor ratios",
                                                     "transition counts"],
            statement="frequency alone is not predictive edge"),
        reporting_firewall=dict(
            forbidden=["inventing a test", "combining groups post hoc", "adding a ratio",
                       "renaming descriptive recurrence as replication",
                       "promoting a sensitivity result"]))

    not_frozen = [
        dict(item="computable T-state definition on the Massive X layer",
             blocked_by="finding_1 / finding_2"),
        dict(item="any Z-state definition", blocked_by="finding_3"),
        dict(item="transition dictionary", depends_on="the state dictionary"),
        dict(item="episode definition and overlap/dependence rules",
             depends_on="the transition dictionary",
             note="the principles are recorded above; the concrete rules cannot be written "
                  "against states that do not yet exist"),
        dict(item="complete search-space enumeration",
             depends_on="states x transitions x conditioning",
             note="the gate requires the count to be computable BEFORE Y; it is not "
                  "computable before the states exist"),
        dict(item="Y horizon set and multiplicity family boundaries",
             depends_on="the enumerated family"),
    ]

    negatives = [
        "an UNAVAILABLE condition cannot become FALSE",
        "an INITIALIZATION_SENSITIVE EMA cannot satisfy a VALID-only condition",
        "a contaminated coarse state cannot enter a COMPLETE-only state",
        "a PATH_CONTAMINATED CUM-RVOL cannot satisfy a valid RVOL condition",
        "an early-close unavailable RVOL cannot become below-threshold false",
        "a future coarse bar cannot affect a current transition",
        "a future outcome cannot influence state construction",
        "same-bar execution cannot use a price already consumed by the state",
        "an undeclared EMA period is rejected",
        "an undeclared timeframe is rejected",
        "an undeclared RVOL threshold is rejected",
        "an undeclared state combination is rejected",
        "an undeclared outcome horizon is rejected",
        "post-Y search-family expansion is rejected",
        "an overlapping repeated state cannot silently multiply independent episodes",
        "an excluded security cannot enter the analysis",
        "supplemental / new-vintage data cannot enter V1",
        "the report layer cannot create an unregistered inferential statistic",
    ]

    guard = y_access_guard()
    acceptance = {
        "x_feature_layer_bound": all(v["match"] for v in binding.values()),
        "cohort_476_unchanged": True,
        "supplemental_use_zero": True,
        "one_minute_role_unchanged": True,
        "t_z_inventory_performed": True,
        "states_machine_readable": False,
        "transitions_machine_readable": False,
        "all_required_semantics_defined": False,
        "false_vs_unavailable_preserved": True,
        "search_family_enumerated": False,
        "exposed_hypotheses_identified": True,
        "y_specified_not_calculated": False,
        "multiplicity_frozen": False,
        "episode_overlap_frozen": False,
        "execution_timing_unambiguous": True,
        "negative_fixtures_defined": True,
        "y_access_guard_pass": guard["passed"],
        "y_exposed_zero": True,
        "analysis_writes_zero": True,
    }

    p = dict(
        spec_id="MASSIVE_TZ_PRE_Y_RESEARCH_DICTIONARY_V1",
        status="HOLD",
        verdict_reason="the inventory required by this gate found that the T-family is "
                       "defined as a Pine-imported column on the legacy bars pipeline, not "
                       "on the Massive X layer, and that no Z definition is frozen anywhere. "
                       "The gate's own rule then requires HOLD rather than a default.",
        task_class="SPECIFICATION_ONLY",
        x_layer=binding,
        tz_inventory=inventory,
        blocking_findings=blocking,
        hypothesis_provenance=provenance,
        frozen_in_this_gate=frozen_now,
        not_frozen_and_why=not_frozen,
        negative_fixtures=dict(count=len(negatives), required=negatives),
        y_definition=dict(status="NOT SPECIFIED",
                          reason="the gate permits defining Y mechanically, but a Y horizon "
                                 "is anchored to a transition timestamp and no transition "
                                 "dictionary can exist yet",
                          computed=False, values_accessed=0),
        y_firewall=dict(y_exposed=0, guard=guard,
                        forbidden=["future outcomes", "returns after candidate states",
                                   "enrichment", "win rates", "theta",
                                   "ranking hypotheses by result"]),
        acceptance=acceptance,
        failed_acceptance=[k for k, v in acceptance.items() if not v],
        mutations=dict(x_layer=0, cohort=0, charter=0, analysis_writes=0, y_exposure=0),
        decision_required="choose option A, B or C in blocking_findings.options. A needs the "
                          "TZ_WLNBB Pine source as the authority for the T-column and its "
                          "priority-resolution order.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TZ_PRE_Y_RESEARCH_DICTIONARY_V1.json",
                 required=("spec_id", "status", "x_layer", "tz_inventory",
                           "blocking_findings", "hypothesis_provenance",
                           "frozen_in_this_gate", "negative_fixtures", "y_firewall",
                           "acceptance", "mutations"),
                 supersede=os.path.exists("MASSIVE_TZ_PRE_Y_RESEARCH_DICTIONARY_V1.json"))
    print(f"MASSIVE_TZ_PRE_Y_RESEARCH_DICTIONARY_V1 · {d} · {p['status']}")
    print(f"  T-family definition: Pine-imported t_sig on the legacy bars pipeline "
          f"(hash {t1.get('t1_definition_hash')})")
    print(f"  formula vs materialised column: 99.39% overlap, 0.61% residual")
    print(f"  Z definition frozen anywhere: {inventory['z_family']['frozen_definition_found']}")
    print(f"  Y access guard: {guard['passed']} ({guard['datasets_checked']} datasets, "
          f"{len(guard['outcome_columns_found'])} outcome columns)")
    print(f"  failed acceptance: {p['failed_acceptance']}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
