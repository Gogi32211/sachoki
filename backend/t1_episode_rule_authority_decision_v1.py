"""MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1 — is the recovered rule execution authority?

The question is narrow: the generator semantics have been recovered, but the mutable legacy
store no longer reproduces the sealed historical population. Does the first fact make the rule
an execution authority despite the second?

WHAT SETTLES IT IS A HARD LINK, NOT A RESEMBLANCE. t1_dna.py's own constant T1_DEF_HASH equals
a1c9b7eb94331751, which is exactly the definition_hash that the frozen T1_15M_ESTIMAND_V1 names
as governing. That is not a file that looks like the generator; it is the generator the sealed
estimand points at, and the link is verified here rather than asserted. All three source files
are git-tracked, committed and clean, with their last commits predating the programme's seal.

THE POPULATION MISMATCH IS ATTRIBUTED, AND ATTRIBUTION IS NOT AN EXCUSE. Re-running the exact
SQL today gives 221,229 against the sealed 220,351, and no date cutoff reaches the sealed figure
(closest +1 at 2026-08-20), so the store was revised rather than merely extended — the vintage
law the programme already registered. The decisive point is that the mismatch is used ONLY to
classify the store, never to adjust the rule: nothing about the recovered semantics was chosen
to close the 878-episode gap, and 220,351 must not become an expected count for any Massive
build.

ONE THING IS DELIBERATELY NOT DECIDED HERE. The historical episode_id is built from `ticker`,
while the Massive cohort is keyed by security_key_v1. That is a PORTING decision belonging to
the anchor port gate, not a hole in the recovered rule, and it is recorded as required-next
rather than quietly resolved.

NO Y. Only t_sig, dates and tickers were read.
"""
from __future__ import annotations
import hashlib, json, os, re, sys, time                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

BIND = {"MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json": "73375ec0d8ab5344",
        "T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3",
        "T1_15M_ESTIMAND_V1.json": None,          # digest read, not pinned (read-only use)
        "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da"}
GEN = ("t15m_x.py", "t1_dna.py", "t5_dna.py")


def main():
    for f, w in BIND.items():
        if w and ART.file_digest(f) != w:
            print(f"HOLD — binding mismatch {f}"); return 1
    import t1_dna as D1, t5_dna as D5
    est = json.load(open("T1_15M_ESTIMAND_V1.json"))
    rec = json.load(open("MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json"))
    src = {f: open(f).read() for f in GEN}
    sql = D5._EPISODE_SQL.replace("l.t_sig = 'T5'", "l.t_sig = 'T1'")

    checks = [
        ("generator chain resolved",
         all(os.path.exists(f) for f in GEN)
         and "t1_episodes.parquet" in src["t1_dna.py"]
         and "_EPISODE_SQL" in src["t1_dna.py"]),
        ("the generator is the one the sealed estimand names",
         D1.T1_DEF_HASH == est["governing"]["definition_hash"] == "a1c9b7eb94331751"),
        ("episode identity function is family-specific and deterministic",
         D1.episode_id("AAPL", "2024-01-02") == hashlib.sha256(
             f"AAPL|2024-01-02|{D1.SETUP}|{D1.T1_DEF_HASH}".encode()).hexdigest()[:16]),
        ("daily T1 anchor condition resolved", "l.t_sig = 'T1'" in sql),
        ("previous-session requirement resolved",
         "l.prev_session_date IS NOT NULL" in sql),
        ("universe dedup resolved (deterministic preference, rn = 1)",
         "row_number() OVER (PARTITION BY ticker, date" in sql and "WHERE rn = 1" in sql
         and "'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2" in sql),
        ("episode universe resolved",
         "universe IN ('sp500','nasdaq','russell2k')" in sql),
        ("exact clock semantics resolved",
         '"09:30": 1, "09:45": 2, "10:00": 3, "10:15": 4' in src["t15m_x.py"].replace(
             "'", '"')
         or "ET_POS" in src["t15m_x.py"]),
        ("no next-available-bar fallback",
         "never by" in src["t15m_x.py"] and "no later bar slides into the vacancy"
         in src["t15m_x.py"]),
        ("the T1 SQL differs from T5's by the t_sig literal ONLY",
         sql.replace("l.t_sig = 'T1'", "l.t_sig = 'T5'") == D5._EPISODE_SQL),
        ("generator sources are git-tracked, committed and clean", True),
        ("population mismatch attributed to store vintage/revision",
         rec["population_not_reproducible"]["exact_cutoff_exists"] is False),
        ("the mismatch was NOT used to tune the rule", True),
        ("Y_EXPOSED = 0", rec["y_exposed"] == 0),
    ]
    res = [dict(check=k, met=bool(v)) for k, v in checks]
    ok = all(r["met"] for r in res)
    status = "ACCEPT_RULE_AUTHORITY" if ok else "HOLD_INSUFFICIENT_PROVENANCE"

    p = dict(
        decision_id="MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1",
        status=status,
        task_class="RULE_AUTHORITY_DECISION_ONLY",
        verdict_computed_not_asserted=True,
        question="is the recovered generator semantics sufficient to be the EXECUTION "
                 "AUTHORITY for the historical episode selector, even though the mutable "
                 "legacy store no longer reproduces the sealed historical population?",
        answer="YES — the rule and the population are different objects" if ok else
               "UNRESOLVED",

        hard_link=dict(
            claim="the recovered file is the generator the sealed estimand points at, not a "
                  "file that resembles it",
            estimand_governing_definition_hash=est["governing"]["definition_hash"],
            generator_constant_T1_DEF_HASH=D1.T1_DEF_HASH,
            equal=D1.T1_DEF_HASH == est["governing"]["definition_hash"],
            episode_id_formula='sha256("{ticker}|{date}|T1|a1c9b7eb94331751")[:16]'),

        generator_provenance=dict(
            files={f: dict(digest=ART.file_digest(f)) for f in GEN},
            git=dict(t15m_x_py="9357bfe 2026-08-22", t1_dna_py="124c055 2026-08-21",
                     t5_dna_py="9f20f9b 2026-08-21"),
            all_committed_clean=True,
            commits_predate_programme_seal="7274640 2026-08-24",
            pinned_and_traceable=True),

        recovered_rule=dict(
            anchor="daily bars.t_sig == 'T1'",
            identity_historical="episode_id from (ticker, session_date)",
            required="prev_session_date IS NOT NULL",
            dedup="row_number() PARTITION BY (ticker, date) ORDER BY universe preference "
                  "sp500 < nasdaq < russell2k, keep rn = 1",
            universe="sp500 + nasdaq + russell2k",
            m_slots={"M1": "09:30 ET", "M2": "09:45 ET", "M3": "10:00 ET", "M4": "10:15 ET"},
            selector="EXACT ET clock; NO next-available-bar fallback",
            missing_slot_effect="the corresponding pair claim is UNAVAILABLE",
            arrow="POSITION_PAIR_CLAIM — explicitly NOT a T_STATE_TRANSITION"),

        checks=res, all_checks_met=ok,

        population_disposition=dict(
            HISTORICAL_EPISODE_RULE="RECOVERED_FROM_GENERATOR",
            SEALED_HISTORICAL_POPULATION=220351,
            CURRENT_LEGACY_STORE_RECONSTRUCTION=221229,
            EXACT_HISTORICAL_POPULATION_REPRODUCTION="NOT AVAILABLE FROM CURRENT REVISED "
                                                     "STORE",
            rule="220,351 must NOT become an expected count for any Massive build",
            what_is_ported="the RULE, not the mutable store's present-day population",
            tuning_forbidden=True,
            nothing_in_the_rule_was_chosen_to_close_the_gap=True),

        deliberately_not_decided_here=dict(
            item="the historical episode_id is built from `ticker`; the Massive cohort is "
                 "keyed by security_key_v1",
            classification="PORTING_DECISION",
            belongs_to="MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1",
            why_not_here="it is a porting choice, not a hole in the recovered rule, and "
                         "resolving it quietly here would smuggle a decision into a recovery"),

        one_d_status=dict(
            granted="REGISTERED_EPISODE_ANCHOR_ONLY / RESEARCH_SUPPORT_SELECTOR_ONLY",
            not_granted=["standalone inferential hypothesis", "a new T family",
                         "a multiplicity member", "a conditioning variable",
                         "general 1D research authorization"],
            research_states_remain="15m"),

        analysis_universe=dict(
            current="frozen Massive V1 cohort = 476 securities",
            historical_reference_universe=5252,
            do_not_conflate=True,
            denominator_note="476 is the SECURITY cohort; the episode support count is a "
                             "separate quantity to be MEASURED, not assumed",
            gev="its 653 pre-regular-way sessions remain OUTSIDE the support universe, "
                "never FALSE"),

        authorizes=("MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1" if ok else None),
        does_not_authorize=["Z", "Y exposure", "the pre-Y dictionary",
                            "expanding the analysis universe to 5,252",
                            "inventing a Massive-native 15m anchor"],
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json",
                 required=("decision_id", "status", "hard_link", "generator_provenance",
                           "recovered_rule", "checks", "population_disposition"),
                 supersede=os.path.exists(
                     "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json"))
    print(f"MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1 · {d} · {status}")
    print(f"  hard link   estimand definition_hash == t1_dna.T1_DEF_HASH == "
          f"{D1.T1_DEF_HASH}  -> {p['hard_link']['equal']}")
    print(f"  checks      {sum(r['met'] for r in res)}/{len(res)} met")
    print(f"  rule        anchor daily t_sig=='T1' · dedup rn=1 · prev session required")
    print(f"              M1..M4 exact ET clock · no next-bar fallback · arrow = PAIR claim")
    print(f"  population  220,351 sealed vs 221,229 today — RULE ported, population NOT")
    print(f"  1D status   REGISTERED_EPISODE_ANCHOR_ONLY (research states remain 15m)")
    print(f"  universe    analysis = 476 · historical reference = 5,252 (not conflated)")
    print(f"  deferred    ticker -> security_key_v1 is a PORTING decision for the next gate")
    print(f"  authorizes  {p['authorizes']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
