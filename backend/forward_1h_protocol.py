"""T9 / T3 / T1 · 1H FORWARD PROTOCOLS — sealed now, so the clock can start.

Forward does not wait for 15m: the 1H medoids are already known, so their forward family
can be frozen today. The 15m forward family is a separate seal, later, once its historical
medoids exist.

    T9_FORWARD_1H   k = 4 frozen overlap-cluster medoids
    T3_FORWARD_1H   k = 7
    T1_FORWARD_1H   k = 9   (the J>=0.50 graph left nine singleton components, so every
                             survivor IS a medoid — not a decision to include them all)

NO BACKFILL. Forward evidence begins only from signal episodes generated AFTER this seal.
An episode that already exists today cannot become prospective evidence by being noticed
today. The T5 discovery cutoff is NOT reused: each family gets its own seal instant.

The source gate is independent: FORWARD_1H_SOURCE_V1 is currently HOLD, so the spec is
sealed and the clock starts, but the prospective evidence counter stays frozen.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import pandas as pd                                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import pt_family as PF                                               # noqa: E402


def seal(fam: str):
    F = fam.upper()
    D = PF.dna(F)
    dcol = PF.date_col(F)
    meds = PF.medoids(F)
    cache_art = f"PT{F[1:]}_1H_MEMBERSHIP_CACHE_V1.json"
    cache = json.load(open(cache_art))
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", dcol])
    last_hist = str(E[dcol].max())[:10]
    now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    seal_date = time.strftime("%Y-%m-%d")

    body = dict(
        spec_id=f"{F}_FORWARD_1H_V1", status="SEALED",
        family_id=f"{F}_MICROSTRUCTURE_DNA_V1",
        mode="PROSPECTIVE — outcome-blind accrual",
        k=len(meds),
        inferential_family=[dict(key=k, claim_id=v,
                                 j=cache["medoid_detail"][v]["j"],
                                 sealed_members=cache["medoid_detail"][v]["members"],
                                 membership_hash=cache["medoid_detail"][v]["membership_hash"])
                            for k, v in meds.items()],
        why_medoids_not_survivors="the forward family is the frozen overlap-cluster medoid "
                                  "set, not every historical survivor: within-cluster "
                                  "aliases carry the same membership and would inflate k "
                                  "without adding an independent structure. T9 6->4, "
                                  "T3 8->7, T1 9->9 (nine singleton components).",
        bound=dict(
            definition_hash=PF.definition_hash(F),
            membership_cache=ART.file_digest(cache_art),
            claim_order=ART.file_digest(f"{F}_1H_CLAIM_ORDER_V1.json"),
            historical_exposure=ART.file_digest(f"{F}_HISTORICAL_EXPOSED_V1.json"),
            characterization=ART.file_digest(f"{F}_SURVIVOR_CHARACTERIZATION_V1.json"),
            outcome_spec=ART.file_digest(f"{F}_OUTCOME_SPEC_V1.json"),
            session_semantics=ART.file_digest(
                "SESSION_FILTER_UNIFICATION_AMENDMENT_V1.json"),
            source_gate=ART.file_digest("FORWARD_1H_SOURCE_V1.json")),
        decision_spec=dict(
            signal="a canonical 1D episode of this family whose frozen 1H microstructure "
                   "matches at least one medoid membership",
            evaluation="membership is READ from the sealed cache semantics, never "
                       "re-derived by a second implementation",
            entry="next regular-session open after the episode date",
            horizon_sessions=10,
            primary="MFE_10D", secondary="MFE_ATR_10D",
            maturity="an episode is evidence-eligible only after its full 10-session path "
                     "exists; before that it is FORWARD_X_UNMATURED and its outcome is "
                     "not read"),
        no_backfill=dict(
            rule="forward evidence begins ONLY from signal episodes whose decision date is "
                 "strictly AFTER this seal date",
            seal_date=seal_date, seal_timestamp=now,
            last_historical_episode_in_store=last_hist,
            explicitly_not_evidence=f"every episode dated {last_hist} or earlier, including "
                                    f"today's, is historical — it cannot become prospective "
                                    f"evidence by being noticed after the seal",
            t5_cutoff_not_reused="the T5 discovery cutoff (2026-08-20) is a different "
                                 "family's instant and is not inherited here"),
        source_gate_state=dict(
            verdict=json.load(open("FORWARD_1H_SOURCE_V1.json"))["verdict"],
            consequence="spec SEALED and the clock runs; prospective evidence accrual is "
                        "HELD until the source qualifies. Signals may be shown as an "
                        "explicitly labelled operational preview and counted nowhere."),
        forbidden=["counting any pre-seal episode as prospective evidence",
                   "reading a forward outcome before maturity",
                   "re-deriving membership with a second implementation",
                   "changing the medoid set, entry, horizon or outcome after the seal"],
        outcome_exposure="NOT_EXPOSED",
        sealed_at=now)
    d = ART.seal(body, f"{F}_FORWARD_1H_V1.json",
                 required=("spec_id", "inferential_family", "no_backfill",
                           "decision_spec"),
                 supersede=os.path.exists(f"{F}_FORWARD_1H_V1.json"))
    print(f"{F}_FORWARD_1H_V1 · {d} · k={len(meds)} · seal {seal_date} · "
          f"last historical episode {last_hist} · source {body['source_gate_state']['verdict']}")
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9", "t3", "t1"]):
        seal(f)
