"""T1 family governance + 15m pre-spec. Frozen BEFORE any T1 outcome value exists.

Two artifacts:

    T1_FAMILY_GOVERNANCE_V1      what T5 may and may not contribute to T9
    T1_15M_PRESPEC_V1            the 15m localization design, frozen before 1H Y exposure

THE INDEPENDENCE CLAIM, STATED SO IT CAN BE AUDITED

T5 contributes INFRASTRUCTURE (token registry, builders, the qualified V2 rank statistic,
capability protocol shape) and ENGINEERING knowledge (the 3-bar 15m grammar's computational
and multiplicity cost — learned before any T1 outcome was seen). T5 contributes ZERO
hypothesis selection: no winning token, family, Z, theta, cluster, medoid or band may
preferentially direct any T1 search. The full grammar runs; the data speaks.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, combo_tokens_spec as TS                     # noqa: E402
import t1_dna as D9                                                    # noqa: E402


def main():
    ART.smoke_test(verbose=False)

    g = ART.seal(dict(
        spec_id="T1_FAMILY_GOVERNANCE_V1", status="FROZEN",
        family_id="T1_MICROSTRUCTURE_DNA_V1",
        frozen_before="any T1 outcome value exists",
        setup="FINAL_PRIORITY_RESOLVED_T9",
        definition_hash=D9.T1_DEF_HASH,
        relationship_to_t5=dict(
            reused=["token registry (identical, hash recorded)",
                    "episode/microstructure builders (byte-identical code)",
                    "qualified V2 bounded blockwise Mann-Whitney/AUC rank statistic",
                    "capability protocol SHAPE (needles, worlds, delta grid)",
                    "support gates as frozen starting rules",
                    "artifact writer and gate discipline"],
            cited_as="methodological precedent ONLY"),
        forbidden_design_inputs=[
            "T5 BUY→L5", "T5 VOL_W→L46x", "T5 L43→T2", "T5 BUY→Z2", "T5 VA→*",
            "T5 L4/L6→*", "T5 15m medoids", "T5 survivor Z", "T5 theta",
            "T5 historical band", "T5 forward result",
            "ANY T9 winner discovered in the parallel T9 research",
            "T9 Z_rank", "T9 theta", "T9 survivor identity", "T9 overlap clusters",
            "T9 historical band", "T9 15m result",
            "ANY T3 winner discovered in the parallel T3 research",
            "T3 Z_rank", "T3 theta", "T3 survivor identity", "T3 overlap clusters",
            "T3 historical band", "T3 15m result"],
        natural_occurrence_rule="if a forbidden-list pattern occurs naturally in the complete "
                                "frozen T1 grammar it is evaluated normally with every other "
                                "eligible claim — it is never preferentially selected",
        no_narrative_rule="no hypothesis (absorption, reclaim, failed gap, volume expansion, "
                          "breakout, oversold reversal, VA, VOL_W, BUY, L43) is privileged "
                          "because the T1 candle visually suggests it; the full frozen "
                          "grammar decides, and economic interpretation is POST-EXPOSURE "
                          "only",
        forbidden_meaning="none of these may select, seed, weight or preferentially order any "
                          "T1 hypothesis; the full grammar is enumerated and the data speaks",
        historical_cutoff_rule="the T1 max-null band MUST come from T9's own permutation "
                               "distribution; no other family's band (T5, T9 or T3) is ever reused",
        capability_rule="T5 capability does NOT qualify T1 — own population, support "
                        "distribution, block geometry, k and null dependence",
        cross_family_selection="comparing T5 vs T1 and picking the better signal is a "
                               "SEPARATE selection problem, logged separately when it happens",
        support_gates=dict(treated_episodes=300, control_episodes=300,
                           treated_tickers=50, control_tickers=50,
                           treated_dates=50, control_dates=50,
                           max_single_date_treated_share=0.10,
                           revision_rule="never changed because the realized X distribution "
                                         "is inconvenient; an X-only feasibility problem "
                                         "STOPS the run and is documented as a pre-Y "
                                         "feasibility decision, not a preregistered gate"),
        outcome_spec=dict(
            signal="canonical T1 daily close", decision="after T1 session close",
            entry="NEXT regular trading session OPEN",
            primary="MFE_10D %", secondary_registered="MFE_ATR_10D",
            descriptive_only=["MAE_10D %", "terminal R_10D %"],
            not_promotion_paths=["MAE_10D", "R_10D"],
            exit_policy="NONE", y_status="NOT EXPOSED"),
        estimand=dict(
            delta="median(MFE_10D|S=1,b) - median(MFE_10D|S=0,b)",
            theta="sum_b w_sb * Delta_sb, treated-episode weights",
            blocks="exact T1 date x pre-window liquidity LOW/HIGH x pre-window volatility "
                   "LOW/HIGH, all pre-treatment",
            anchor="T1 - 2 sessions",
            liquidity="20-session median dollar volume ending T9-2, full pre-window required",
            volatility="ATR14/close at T9-2",
            split="deterministic value-then-ticker tie-break, rank(method='first')",
            filter_order="sequence-analyzable filtering BEFORE block construction"),
        inference=dict(statistic="Z_rank (bounded blockwise MW/AUC, analytic tie-corrected "
                                 "null SE)", direction="one-sided positive",
                       multiplicity="search-wide max-Z_rank permutation control",
                       theta_role="magnitude only; cannot rescue a Z_rank failure"),
        token_registry_hash=TS.digest()),
        "T1_FAMILY_GOVERNANCE_V1.json",
        required=("spec_id", "forbidden_design_inputs", "support_gates", "outcome_spec"))
    print(f"T1_FAMILY_GOVERNANCE_V1 · {g}")

    p = ART.seal(dict(
        spec_id="T1_15M_PRESPEC_V1", status="FROZEN",
        family_id="T1_MICROSTRUCTURE_DNA_V1",
        frozen_before="T1 historical outcome exposure — and before the 1H phase is sealed",
        primary_scope="OPENING HOUR ONLY",
        position_families=["M1→M2", "M2→M3", "M3→M4"],
        grammar="position-anchored 2-bar claims only",
        excluded="the 3-bar grammar (million-claim scale) is NOT in T1 V1",
        why_excluded="pre-existing computational/multiplicity feasibility knowledge from the "
                     "T5 ENGINEERING record — a lesson about cost, learned before any T1 "
                     "outcome was seen; not any T1 outcome and not any T5 WINNER",
        tokens="same canonical registry", token_registry_hash=TS.digest(),
        support_rules="same frozen floors initially",
        evidence_status="a SEPARATE localization family; NOT independent evidence from 1H — "
                        "same episode universe, same outcome",
        sequencing="no 15m outcome statistic is enumerated until the T1 1H phase is sealed "
                   "per the registered sequence",
        definition_hash=D9.T1_DEF_HASH),
        "T1_15M_PRESPEC_V1.json",
        required=("spec_id", "primary_scope", "position_families", "why_excluded"))
    print(f"T1_15M_PRESPEC_V1 · {p}")


if __name__ == "__main__":
    main()
