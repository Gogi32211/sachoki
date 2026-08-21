"""T9 family governance + 15m pre-spec. Frozen BEFORE any T9 outcome value exists.

Two artifacts:

    T9_FAMILY_GOVERNANCE_V1      what T5 may and may not contribute to T9
    T9_15M_PRESPEC_V1            the 15m localization design, frozen before 1H Y exposure

THE INDEPENDENCE CLAIM, STATED SO IT CAN BE AUDITED

T5 contributes INFRASTRUCTURE (token registry, builders, the qualified V2 rank statistic,
capability protocol shape) and ENGINEERING knowledge (the 3-bar 15m grammar's computational
and multiplicity cost — learned before any T9 outcome was seen). T5 contributes ZERO
hypothesis selection: no winning token, family, Z, theta, cluster, medoid or band may
preferentially direct any T9 search. The full grammar runs; the data speaks.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, combo_tokens_spec as TS                     # noqa: E402
import t9_dna as D9                                                    # noqa: E402


def main():
    ART.smoke_test(verbose=False)

    g = ART.seal(dict(
        spec_id="T9_FAMILY_GOVERNANCE_V1", status="FROZEN",
        family_id="T9_MICROSTRUCTURE_DNA_V1",
        frozen_before="any T9 outcome value exists",
        setup="FINAL_PRIORITY_RESOLVED_T9",
        definition_hash=D9.T9_DEF_HASH,
        relationship_to_t5=dict(
            reused=["token registry (identical, hash recorded)",
                    "episode/microstructure builders (byte-identical code)",
                    "qualified V2 bounded blockwise Mann-Whitney/AUC rank statistic",
                    "capability protocol SHAPE (needles, worlds, delta grid)",
                    "support gates as frozen starting rules",
                    "artifact writer and gate discipline"],
            cited_as="methodological precedent ONLY"),
        forbidden_design_inputs=[
            "T5 BUY→L5", "T5 VOL_W", "T5 L43→T2", "T5 BUY→Z2", "T5 VA→*",
            "T5 15m medoids", "T5 survivor Z", "T5 theta", "T5 historical band"],
        forbidden_meaning="none of these may select, seed, weight or preferentially order any "
                          "T9 hypothesis; the full grammar is enumerated and the data speaks",
        historical_cutoff_rule="the T9 max-null band MUST come from T9's own permutation "
                               "distribution; T5's band is never reused",
        capability_rule="T5 capability does NOT qualify T9 — own population, support "
                        "distribution, block geometry, k and null dependence",
        cross_family_selection="comparing T5 vs T9 and picking the better signal is a "
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
            signal="canonical T9 daily close", decision="after T9 session close",
            entry="NEXT regular trading session OPEN",
            primary="MFE_10D %", secondary_registered="MFE_ATR_10D",
            descriptive_only=["MAE_10D %", "terminal R_10D %"],
            not_promotion_paths=["MAE_10D", "R_10D"],
            exit_policy="NONE", y_status="NOT EXPOSED"),
        estimand=dict(
            delta="median(MFE_10D|S=1,b) - median(MFE_10D|S=0,b)",
            theta="sum_b w_sb * Delta_sb, treated-episode weights",
            blocks="exact T9 date x pre-window liquidity LOW/HIGH x pre-window volatility "
                   "LOW/HIGH, all pre-treatment",
            anchor="T9 - 2 sessions",
            liquidity="20-session median dollar volume ending T9-2, full pre-window required",
            volatility="ATR14/close at T9-2",
            split="deterministic value-then-ticker tie-break, rank(method='first')",
            filter_order="sequence-analyzable filtering BEFORE block construction"),
        inference=dict(statistic="Z_rank (bounded blockwise MW/AUC, analytic tie-corrected "
                                 "null SE)", direction="one-sided positive",
                       multiplicity="search-wide max-Z_rank permutation control",
                       theta_role="magnitude only; cannot rescue a Z_rank failure"),
        token_registry_hash=TS.digest()),
        "T9_FAMILY_GOVERNANCE_V1.json",
        required=("spec_id", "forbidden_design_inputs", "support_gates", "outcome_spec"))
    print(f"T9_FAMILY_GOVERNANCE_V1 · {g}")

    p = ART.seal(dict(
        spec_id="T9_15M_PRESPEC_V1", status="FROZEN",
        family_id="T9_MICROSTRUCTURE_DNA_V1",
        frozen_before="T9 historical outcome exposure — and before the 1H phase is sealed",
        primary_scope="OPENING HOUR ONLY",
        position_families=["M1→M2", "M2→M3", "M3→M4"],
        grammar="position-anchored 2-bar claims only",
        excluded="the 3-bar grammar (million-claim scale) is NOT in T9 V1",
        why_excluded="pre-existing computational/multiplicity feasibility knowledge from the "
                     "T5 ENGINEERING record — a lesson about cost, learned before any T9 "
                     "outcome was seen; not any T9 outcome and not any T5 WINNER",
        tokens="same canonical registry", token_registry_hash=TS.digest(),
        support_rules="same frozen floors initially",
        evidence_status="a SEPARATE localization family; NOT independent evidence from 1H — "
                        "same episode universe, same outcome",
        sequencing="no 15m outcome statistic is enumerated until the T9 1H phase is sealed "
                   "per the registered sequence",
        definition_hash=D9.T9_DEF_HASH),
        "T9_15M_PRESPEC_V1.json",
        required=("spec_id", "primary_scope", "position_families", "why_excluded"))
    print(f"T9_15M_PRESPEC_V1 · {p}")


if __name__ == "__main__":
    main()
