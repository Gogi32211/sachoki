"""MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1 — the rule is found; the population is not.

The recovery chain ran to its end and split cleanly in two, and the two halves must not be
reported as one result.

THE RULE IS RECOVERED FROM ITS GENERATOR, not guessed from a materialized table. t15m_x.py
selects M1..M4 by EXACT ET clock — 09:30 / 09:45 / 10:00 / 10:15, "never by observation order,
so a missing slot means the claim cannot fire and no later bar slides into the vacancy" — and
the episodes it consumes are written by t1_dna.py, whose SQL is derived from t5_dna's by the
t_sig literal alone and asserts that at import. A canonical episode is one (ticker,
session_date) where the DAILY bars.t_sig equals 'T1', deduplicated across universes by
row_number, with a prior session present. That is a definitional source, not an inference.

THE FROZEN POPULATION IS NOT REPRODUCIBLE, AND SAYING SO IS THE POINT. Re-running that exact SQL
against the current store yields 221,229 against the sealed 220,351. Sweeping a date cutoff gets
to 220,352 at 2026-08-20 and never to 220,351, so the store has been REVISED and not merely
extended. The programme's own vintage law predicted this: store signals are
window-vintage-dependent. So the rule is authority; the count is not reproducible today.

AND THE STRUCTURAL FINDING MATTERS MORE THAN EITHER. The registered historical 15m study is
ANCHORED ON 1D t_sig == 'T1' episodes drawn from sp500 + nasdaq + russell2k — 5,252 distinct
tickers — with 15m supplying only opening-hour microstructure at M1..M4. The Massive T port
produces 15m T states over the 476-security V1 cohort and supplies no 1D anchor at all, and 1D
is DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION under the frozen scope. The Massive port
therefore cannot host the registered historical study as written. That is a finding for the
dictionary gate to decide on, not something to work around here.

NO Y. Only t_sig, dates and tickers were read. No outcome, no MFE, no path status.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

BIND = {"T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3",
        "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
        "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2.json": "1582a8d21042c6a8"}


def main():
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1

    p = dict(
        report_id="MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1",
        status="EPISODE_RULE_RECOVERED_FROM_GENERATOR__POPULATION_NOT_REPRODUCIBLE",
        task_class="SEMANTIC_RECOVERY_ONLY",
        bindings=BIND,

        recovery_chain=[
            "frozen prespec T1_15M_PRESPEC_V1 (71d49dcde8921eb3)",
            "exact-hash search located the governing artifacts and the git commits",
            "generator t15m_x.py — the 15m X-only runner",
            "episode generator t1_dna.py -> data/t1_episodes.parquet",
            "episode SQL inherited from t5_dna._EPISODE_SQL",
            "re-execution of that SQL against the current store"],

        rule_recovered=dict(
            status="RECOVERED_FROM_GENERATOR_SOURCE",
            stronger_than="RECOVERED_FROM_MATERIALIZED_POPULATION — the generator itself was "
                          "found, so the rule is read rather than inferred",
            family_id="T1_MICROSTRUCTURE_DNA_V1",
            setup="FINAL_PRIORITY_RESOLVED_T1 (bullCode == 5, t_sig == 'T1')",
            episode_definition="one (ticker, session_date) where the DAILY bars.t_sig == 'T1', "
                               "deduplicated across universes by row_number (rn = 1), with "
                               "prev_session_date NOT NULL",
            episode_universe="universe IN ('sp500','nasdaq','russell2k')",
            m_positions=dict(
                M1="09:30 ET", M2="09:45 ET", M3="10:00 ET", M4="10:15 ET",
                selector="EXACT ET clock time",
                explicitly_not="observation order — a missing slot means the claim cannot "
                               "fire and no later bar slides into the vacancy"),
            position_families=["M1->M2", "M2->M3", "M3->M4"],
            grammar="position-anchored 2-bar claims only; the 3-bar grammar is EXCLUDED from V1",
            position_anchoring="M1->M2 and M2->M3 with the same tokens are DIFFERENT claims",
            arrow_meaning="a position-PAIR claim between two clock-anchored opening-hour "
                          "slots — NOT a state-change transition",
            source_files=["t15m_x.py", "t1_dna.py", "t5_dna.py"]),

        population_not_reproducible=dict(
            sealed_count=220351,
            recomputed_today=221229,
            difference=878,
            cutoff_sweep={"2026-08-17": 219756, "2026-08-18": 219911, "2026-08-19": 220284,
                          "2026-08-20": 220352, "2026-08-21": 220752},
            exact_cutoff_exists=False,
            conclusion="no date cutoff reproduces 220,351 — the closest is +1 at 2026-08-20 — "
                       "so the store has been REVISED, not merely extended",
            predicted_by="the programme's own vintage law: store signals are "
                         "window-vintage-dependent",
            status="POPULATION_NOT_REPRODUCIBLE_FROM_CURRENT_STORE",
            what_this_does_not_mean="it does not mean the rule is unknown; the rule is "
                                    "recovered from its generator"),

        structural_finding=dict(
            headline="the registered historical 15m study is ANCHORED ON 1D t_sig episodes, "
                     "not on 15m T states",
            anchor="daily bars.t_sig == 'T1'",
            anchor_universe="sp500 + nasdaq + russell2k",
            anchor_distinct_tickers=5252,
            fifteen_m_role="supplies opening-hour microstructure tokens at M1..M4 ONLY",
            massive_port_provides="15m T states over the 476-security V1 cohort",
            massive_port_does_not_provide="any 1D T anchor",
            one_d_status_in_current_programme="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
            consequence="the Massive T port CANNOT host the registered historical study as "
                        "written",
            cohort_mismatch=dict(historical=5252, massive_v1=476),
            resolution_is_not_mine="this is a finding for the dictionary gate to decide on, "
                                   "not something to work around here"),

        historical_feed_residual_observed=dict(
            note="the historical artifact already measured its own feed/tick residual at 1D",
            canonical_t_sig_T1=220351, raw_ohlc_reconstruction=268854,
            canonical_and_raw=219016, canonical_not_raw=1335,
            canonical_not_raw_share=0.0061,
            relevance="an independent corroboration that canonical t_sig and a raw "
                      "reconstruction differ on a small residual, measured before any of the "
                      "Massive work"),

        doji_case_carried_forward=dict(
            historical_statement="T1 CAN mechanically follow a previous doji; RAW prev-doji "
                                 "12,619 (4.7%), CANONICAL prev-doji 682 (0.31%)",
            status="AUDIT FIELD ONLY",
            forbidden="treating prev-bear vs prev-doji as separate inferential families — "
                      "that would be a NEW research family and a NEW multiplicity decision"),

        gates=dict(pre_y_analysis_dictionary="HOLD", z="HOLD", y="HOLD"),
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        read_only=True, analysis_writes=0,
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json",
                 required=("report_id", "status", "rule_recovered",
                           "population_not_reproducible", "structural_finding"),
                 supersede=os.path.exists(
                     "MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json"))
    print(f"MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1 · {d} · {p['status']}")
    print("  RULE        RECOVERED FROM GENERATOR (t15m_x.py + t1_dna.py + t5_dna SQL)")
    print("              episode = (ticker, session_date) where DAILY bars.t_sig == 'T1'")
    print("              M1..M4 = 09:30 / 09:45 / 10:00 / 10:15 ET, EXACT clock selector")
    print("              arrow = position-PAIR claim, NOT a state-change transition")
    print(f"  POPULATION  NOT reproducible — sealed 220,351 vs today 221,229 (+878);")
    print(f"              no cutoff hits it (closest +1 at 2026-08-20) => store REVISED")
    print("  STRUCTURAL  the historical study is anchored on 1D t_sig episodes over")
    print("              5,252 tickers; the Massive port gives 15m states over 476 and")
    print("              NO 1D anchor => it cannot host the registered study as written")
    print("  gates       dictionary HOLD · Z HOLD · Y HOLD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
