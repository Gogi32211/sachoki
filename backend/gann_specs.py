"""The three GANN inference specs, sealed together and before any outcome is read.

    GANN_ESTIMAND_V1          treated / control, control direction from D-1 only, blocks
    GANN_PHASE_NULL_V1        the registered H0 and the ticker-local phase mechanism
    GANN_CAPABILITY_PROTOCOL_V1   nullization, injection, matrix, acceptance rule

Also supersedes GANN_OUTCOME_SPEC_V1 with the direction-neutral sidecar: a row's future
path must not be materialised through the phi=0 direction, because the same row is LONG in
one phase world and SHORT in another and both must read the SAME future path.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

N_PHASE_WORLDS = 999
DELTAS_PP = [0.5, 1.0, 2.0, 3.0, 4.0, 5.0]
WORLDS_PER_CELL = 20
CLAIMS = ["GANN_ASC_TOUCH_V1", "GANN_DESC_TOUCH_V1", "GANN_CONFLUENCE_TOUCH_V1"]


def outcome_spec():
    body = dict(
        spec_id="GANN_OUTCOME_SPEC_V1", status="FROZEN",
        family_id="GANN_VIBRATION_GRID_V1",
        frozen_before="any GANN outcome value is read",
        x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
        supersedes="the first sealing of this spec, which defined a single DIR_MFE_10D "
                   "materialised through the phi=0 direction — wrong under a phase null, "
                   "because one row is LONG at phi=0 and SHORT at phi=0.37 and both worlds "
                   "must read the SAME future path",
        signal="day D — the contact; the grid was frozen on data through D-1",
        entry="the next regular-session open after D",
        horizon_sessions=10,
        direction_neutral_components=dict(
            MFE_LONG_10D="max_{j=1..10}( High_j / EntryOpen - 1 )",
            MFE_SHORT_10D="max_{j=1..10}( 1 - Low_j / EntryOpen )",
            MFE_ATR_LONG_10D="max_{j=1..10}( High_j - EntryOpen ) / ATR20[D-1]",
            MFE_ATR_SHORT_10D="max_{j=1..10}( EntryOpen - Low_j ) / ATR20[D-1]",
            rule="both components are persisted for every eligible row, independent of any "
                 "phase; the statistic selects one at evaluation time"),
        selection_at_evaluation="direction(phi) == LONG -> LONG component; "
                                "direction(phi) == SHORT -> SHORT component",
        normaliser="ATR20 at D-1 — the scale must come from the state that existed before "
                   "the contact bar",
        post_exposure_descriptive=dict(
            MAE_LONG_10D="max_{j=1..10}( 1 - Low_j / EntryOpen )",
            MAE_SHORT_10D="max_{j=1..10}( High_j / EntryOpen - 1 )",
            RET_LONG_10D="Close_10 / EntryOpen - 1",
            RET_SHORT_10D="1 - Close_10 / EntryOpen",
            rule="same direction-neutral treatment; descriptive only, never selection"),
        availability=dict(
            statuses=["AVAILABLE", "NOT_YET_MATURE", "NO_NEXT_SESSION_OPEN",
                      "TERMINAL_DURING_HORIZON", "DATA_GAP"],
            rule="availability is a STATUS and is PHASE-INDEPENDENT: it depends on the "
                 "entry and the 10-session path, never on direction or on which lattice "
                 "line was contacted"),
        forbidden=["materialising a single directional outcome",
                   "reading any outcome value before the capability protocol and its "
                   "pre-Y gates are sealed and passed",
                   "using MAE or RET for selection",
                   "changing horizon, entry or normaliser after any outcome is seen"],
        outcome_exposure="NOT_EXPOSED — this artifact defines the outcome, it does not "
                         "read it",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    return ART.seal(body, "GANN_OUTCOME_SPEC_V1.json",
                    required=("spec_id", "direction_neutral_components", "entry"),
                    supersede=True)


def estimand():
    body = dict(
        spec_id="GANN_ESTIMAND_V1", status="FROZEN",
        family_id="GANN_VIBRATION_GRID_V1",
        x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
        outcome_spec=ART.file_digest("GANN_OUTCOME_SPEC_V1.json"),
        claims=CLAIMS, k=3,
        multiplicity="k = 3 is FIXED. No claim may be dropped on support after any outcome "
                     "is seen; support is a validity diagnostic. If a claim's engine "
                     "validity breaks in some phase world, the RUN holds — the claim does "
                     "not disappear.",
        treated=dict(
            definition="a registered phase-specific onset event for that claim",
            direction="the frozen touched-line direction, evaluated at D-1"),
        control=dict(
            definition="same phase, same claim, onset == FALSE, grid observable on D and "
                       "outcome-eligible",
            direction_rule="X-ONLY from D-1. A non-touch row has no contacted line, so the "
                           "direction comes from the phase-specific lattice line NEAREST "
                           "to Close[D-1], evaluated at D-1:",
            ASC="nearest ASC line at D-1 -> LONG if Close[D-1] > line, SHORT if below",
            DESC="nearest DESC line at D-1 -> same rule",
            CONFLUENCE="nearest ASC and nearest DESC at D-1; same sign -> valid consensus; "
                       "opposite -> INVALID_CONFLICT; exact equality -> INVALID_EQUALITY",
            forbidden="D's own OHLC may not be used to assign control direction"),
        directional_population="rows with direction in {LONG, SHORT}; for CONFLUENCE that "
                               "additionally means consensus",
        inference_blocks=dict(
            dimensions=["decision_date", "direction", "pre_liquidity LOW/HIGH",
                        "pre_volatility LOW/HIGH"],
            anchors="liquidity and volatility are taken at D-1",
            why_direction_is_a_block="LONG market drift must not be compared with SHORT "
                                     "rows; putting direction in the block makes the "
                                     "comparison within-side by construction"),
        statistic=dict(
            module="gann_rank.py", code_digest=ART.file_digest("gann_rank.py"),
            form="blockwise Mann-Whitney/AUC with analytic tie-corrected null SE",
            U_b="(sum treated midranks - n_t(N_b+1)/2)/(n_t n_c) + 0.5",
            weights="w_b = n_t,b / sum over ELIGIBLE blocks",
            variance="Var0(U_b) = [(N_b+1) - sum(t^3-t)/(N_b(N_b-1))] / (12 n_t n_c)",
            Z="R / sqrt(sum w_b^2 Var0(U_b)),  R = sum w_b (U_b - 0.5)",
            eligibility="a block contributes only with >=1 treated AND >=1 control and "
                        "N_b >= 2; degenerate blocks enter neither the numerator nor the "
                        "weight normaliser",
            degenerate="a claim whose every block is degenerate yields Z = nan and is "
                       "REPORTED, never silently zeroed",
            binding="the code digest above is the definition; prose parity with another "
                    "family is not sufficient, because every phase world recomputes "
                    "memberships"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    return ART.seal(body, "GANN_ESTIMAND_V1.json",
                    required=("spec_id", "treated", "control", "statistic"),
                    supersede=os.path.exists("GANN_ESTIMAND_V1.json"))


def phase_null():
    body = dict(
        spec_id="GANN_PHASE_NULL_V1", status="FROZEN",
        family_id="GANN_VIBRATION_GRID_V1",
        H0="Conditional on the frozen anchors, spacing, slopes and pre-treatment "
           "structure, the anchored integer phase phi = 0 has no special association with "
           "subsequent directional price excursion relative to ticker-local phase shifts.",
        mechanism=dict(
            shift="i -> i + phi",
            draw="phi[w, ticker] ~ Uniform[0, 1)",
            shared="ONE phi per ticker per world, used on EVERY date of that ticker and on "
                   "BOTH the ascending and descending family",
            why_shared="the registered hypothesis has both families sharing one lattice "
                       "phase; shifting them independently would change their relative "
                       "geometry and test a different null",
            invariant=["L", "H", "tL", "tH", "M", "B", "slopes", "spacing", "lookback",
                       "dates", "tickers", "OHLC", "outcome paths"],
            recomputed_per_world=["touch", "5-session onset", "nearest integer level",
                                  "direction", "confluence consensus", "control direction",
                                  "blocks", "Z_rank for all 3 claims"]),
        worlds=N_PHASE_WORLDS,
        family_wise="maxZ_null[w] = max(Z_ASC[w], Z_DESC[w], Z_CONF[w])",
        promotion="STRICT: Z_obs(claim, phi=0) > p95(maxZ_phase_null)",
        p_value_wording="any empirical p must be described as a family-wise empirical "
                        "p-value under the registered ticker-local phase-exchangeability "
                        "null — never as a probability that the Gann construction is false",
        why_this_null="a dense enough lattice touches something constantly, so 'touch vs "
                      "no touch' is not the question. The question is whether the anchored "
                      "integer phase carries information an equally dense shifted lattice "
                      "does not.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    return ART.seal(body, "GANN_PHASE_NULL_V1.json",
                    required=("spec_id", "H0", "mechanism", "promotion"),
                    supersede=os.path.exists("GANN_PHASE_NULL_V1.json"))


def capability():
    body = dict(
        spec_id="GANN_CAPABILITY_PROTOCOL_V1", status="FROZEN",
        family_id="GANN_VIBRATION_GRID_V1",
        frozen_before="any GANN capability code runs and before any observed statistic",
        estimand=ART.file_digest("GANN_ESTIMAND_V1.json"),
        phase_null=ART.file_digest("GANN_PHASE_NULL_V1.json"),
        outcome_spec=ART.file_digest("GANN_OUTCOME_SPEC_V1.json"),
        the_invariant="a capability world may not begin from the observed association. The "
                      "real future paths supply the NOISE; the association is destroyed "
                      "first.",
        nullization=dict(
            unit="the PAIR (MFE_LONG_10D, MFE_SHORT_10D) moves together",
            why_paired="direction depends on phi, so permuting a directionalised outcome "
                       "would destroy the very thing each phase world must re-derive",
            blocks=["decision_date", "pre_liquidity", "pre_volatility"],
            direction_excluded="direction is NOT a nullization block — it is phase-"
                               "dependent; it remains a block of the INFERENCE comparison",
            distinction="capability Y-nullization blocks != inference comparison blocks"),
        injection=dict(
            rule="add delta only to the component matching the frozen phi=0 treated "
                 "direction of the row, for the claim under test",
            deltas_pp=DELTAS_PP),
        matrix=dict(claims=CLAIMS, deltas=len(DELTAS_PP), worlds_per_cell=WORLDS_PER_CELL,
                    total_worlds=len(CLAIMS) * len(DELTAS_PP) * WORLDS_PER_CELL,
                    phase_worlds_per_capability_world=N_PHASE_WORLDS,
                    note="no q10/q50/q90 needles — with k = 3 every claim is tested"),
        detection="Z_injected_claim(phi=0) > p95(maxZ_phase_null) computed inside that "
                  "capability world — the same inference machinery the historical result "
                  "will use",
        acceptance=dict(
            at_1pp="each of the 3 claims >= 16 / 20 detections",
            at_2pp="each of the 3 claims >= 19 / 20 detections",
            at_0_5pp="sensitivity characterization only — NOT a blocking requirement",
            integrity=["360 / 360 capability worlds completed",
                       f"{N_PHASE_WORLDS} phase worlds per capability world",
                       "0 structurally invalid phase worlds",
                       "deterministic RNG and replay PASS"],
            sealed_before="the first GANN outcome read"),
        rng="derived from sealed hashes per T_FAMILY_CAPABILITY_RNG_RULE_V1; no hand-picked "
            "seed and no builtin hash() on the stochastic path",
        wording_allowed="Under the registered homogeneous additive MFE-shift alternative, "
                        "claim C detected +X pp in Y/20 finite capability worlds.",
        wording_forbidden=["true power", "guaranteed detection", "universal sensitivity"],
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    return ART.seal(body, "GANN_CAPABILITY_PROTOCOL_V1.json",
                    required=("spec_id", "the_invariant", "nullization", "acceptance"),
                    supersede=os.path.exists("GANN_CAPABILITY_PROTOCOL_V1.json"))


if __name__ == "__main__":
    print("GANN_OUTCOME_SPEC_V1          ·", outcome_spec())
    print("GANN_ESTIMAND_V1              ·", estimand())
    print("GANN_PHASE_NULL_V1            ·", phase_null())
    print("GANN_CAPABILITY_PROTOCOL_V1   ·", capability())
