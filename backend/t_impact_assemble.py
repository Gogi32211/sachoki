"""SESSION_VALIDITY_AND_ADJACENCY_IMPACT_V1 — assembles the per-family impact results
under the name the root cause deserves, with the provenance statement on the top line.

The per-family JSONs are produced by t_adjacency_impact.py; this module only assembles,
labels and seals them, plus the T1 INTG-2022-06-10 lineage.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import glob, json, os, sys, time                                      # noqa: E402
import pandas as pd                                                   # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "SESSION_VALIDITY_AND_ADJACENCY_IMPACT_V1.json"
PART = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/" \
       "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/impact_%s.json"
PROVENANCE = ("All old-vs-corrected family comparisons are performed on the same sealed "
              "family X vintage; the currently mutable 1H store is not used to regenerate "
              "historical family X. Only expected_bars(date) is taken from the store, via "
              "SESSION_REFERENCE_UNIVERSE_V1.")


def intg_lineage():
    """The exact claims the mis-classified 2022-06-10 session touched, if T1 moved."""
    import t1_dna as D1
    M = pd.read_parquet(D1.OUT_MS, columns=["episode_id", "ticker", "relative_day",
                                            "session_date", "ts_et", "session_position",
                                            "bars_in_session", "token_set"])
    s = M[(M.session_date == "2022-06-10")]
    per = (s.groupby(["ticker", "episode_id", "relative_day"])
             .agg(bars=("session_position", "size"),
                  times=("ts_et", lambda x: sorted(str(v)[-5:] for v in x))).reset_index())
    return dict(
        session_date="2022-06-10",
        family_sessions=per.to_dict("records"),
        old_rule="family-local modal over these sessions -> mode({7,2,6}) resolved to the "
                 "SMALLEST value, 2",
        old_effect="the 2-bar session (09:30 slot 0, 12:45 slot 3) was admitted as "
                   "complete and its two bars became a STRICT_ADJACENT pair spanning three "
                   "missing hours; the genuine 7-bar session was rejected as incomplete",
        corrected_rule="market-wide expected_bars(2022-06-10) = 7 "
                       "(SESSION_REFERENCE_UNIVERSE_V1) with the tie policy pinned upward "
                       "(SESSION_MODE_TIE_POLICY_V1)",
        corrected_effect="the 2-bar session is incomplete and drops out; the 7-bar session "
                         "is complete and enters")


def main():
    fams = {}
    for f in ("t5", "t9", "t3", "t1"):
        p = PART % f
        if os.path.exists(p):
            fams[f.upper()] = json.load(open(p))
    branches = {k: v.get("branch", "?") for k, v in fams.items()}
    body = dict(
        spec_id="SESSION_VALIDITY_AND_ADJACENCY_IMPACT_V1", status="X_ONLY",
        provenance=PROVENANCE,
        root_cause="the registered session-validity rule (modal bar count across ALL "
                   "tickers that traded the date) was implemented as a family-local modal; "
                   "the dense-rank adjacency finding is its downstream symptom, which is "
                   "why this report carries the session-validity name",
        specs=dict(
            remediation=ART.file_digest("ADJACENCY_REMEDIATION_V1.json"),
            session_validity_amendment=ART.file_digest(
                "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1.json"),
            reference_universe=ART.file_digest("SESSION_REFERENCE_UNIVERSE_V1.json"),
            tie_policy=ART.file_digest("SESSION_MODE_TIE_POLICY_V1.json"),
            completeness_audit=ART.file_digest("SESSION_REFERENCE_COMPLETENESS_V1.json"),
            data_version_incident=ART.file_digest(
                "ONE_HOUR_DATA_VERSION_DIVERGENCE_V1.json"),
            fifteen_minute_closure=ART.file_digest("T5_15M_GAP_CLOSURE_V1.json"),
            t5_forward_medoid_identity=ART.file_digest(
                "T5_FORWARD_MEDOID_IDENTITY_V1.json"),
            t5_forward_medoid_ontology=ART.file_digest(
                "T5_FORWARD_MEDOID_IDENTITY_ONTOLOGY_AMENDMENT_V1.json")),
        branch_rule="frozen before any impact number was opened, in "
                    "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1",
        branches=branches,
        families=fams,
        intg_2022_06_10_lineage=intg_lineage(),
        outcome_exposure="NOT_EXPOSED — no outcome, Z, theta or survivor computed anywhere "
                         "in this report",
        assembled_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "provenance", "branches", "families"),
                 supersede=os.path.exists(OUT))
    for k, v in branches.items():
        print(f"  {k}: {v}")
    print(f"\nSESSION_VALIDITY_AND_ADJACENCY_IMPACT_V1 · {d}")


if __name__ == "__main__":
    main()
