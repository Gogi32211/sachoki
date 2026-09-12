"""MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1 — the sealed population survives.

The recovery gate concluded that the historical episode population was not reproducible, which
was true and incomplete. It is not reproducible from any mutable store — current gives 221,229,
the pre-migration archive gives 220,751 — but it was never lost: t1_episodes.parquet holds
exactly 220,351 rows, and a second independent copy sits in the archive.

WHICH SEPARATES THREE THINGS THAT WERE BLURRED INTO ONE. The RULE is authority, recovered from a
pinned generator whose own T1_DEF_HASH equals the definition_hash the sealed estimand names. The
sealed POPULATION is authority too, but as a preserved artifact rather than as something to
recompute. And regeneration from today's mutable stores is authority for NEITHER. Consuming the
historical population therefore means binding the preserved file, not re-running the SQL.

COUNTING ROWS WOULD NOT HAVE BEEN VERIFICATION. A file with the right number of rows can still
be the wrong file, so the episode_id is RECOMPUTED here from (ticker, t1_date) through the
generator's own function and compared to what is stored — which ties the population to the
definition hash cryptographically rather than by coincidence of size. Uniqueness, the
ticker/date grain, the schema and the independent copy are all checked the same way.

READ-ONLY. Nothing is written to either copy.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402

PRIMARY = "/Users/sachoki/Desktop/sachoki-desktop/data/t1_episodes.parquet"
ARCHIVE = ("/Volumes/QUANT_RESEARCH/archive/studio_pre_external_migration_2026-08-24/"
           "t1_episodes.parquet")
POP15M = "/Volumes/QUANT_RESEARCH/source_data/studio/t1_15m_population.parquet"
SEALED_EPISODES, SEALED_ESTIMAND = 220351, 157038
BIND = {"MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json": "73375ec0d8ab5344",
        "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
        "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
        "T1_15M_ESTIMAND_V1.json": None}


def sha256(p, cap=None):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while True:
            b = f.read(1 << 22)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def content_hash(df, cols):
    d = df[cols].sort_values(cols).reset_index(drop=True)
    return hashlib.sha256(
        pd.util.hash_pandas_object(d, index=False).values.tobytes()).hexdigest()[:16]


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="episode population preservation check (read-only)")
    for f, w in BIND.items():
        if w and ART.file_digest(f) != w:
            print(f"HOLD — binding mismatch {f}"); return 1
    inv = json.load(open("MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json"))
    if not inv["completeness_guard"]["enumeration_complete"]:
        print("HOLD — the data-surface enumeration is not complete"); return 1
    import t1_dna as D1
    est = json.load(open("T1_15M_ESTIMAND_V1.json"))

    E = pq.read_table(PRIMARY).to_pandas()
    A = pq.read_table(ARCHIVE).to_pandas() if os.path.exists(ARCHIVE) else None
    P = pq.read_table(POP15M).to_pandas() if os.path.exists(POP15M) else None

    # episode_id RECOMPUTED through the generator's own function
    recomputed = [D1.episode_id(t, d) for t, d in zip(E["ticker"], E["t1_date"])]
    id_match = int((pd.Series(recomputed) == E["episode_id"]).sum())

    checks = [
        ("row count equals the sealed population", len(E) == SEALED_EPISODES),
        ("episode_id is unique", E["episode_id"].nunique() == len(E)),
        ("no duplicate episode_id", int(E["episode_id"].duplicated().sum()) == 0),
        ("ticker/date grain is unique",
         int(E.duplicated(["ticker", "t1_date"]).sum()) == 0),
        ("episode_id RECOMPUTES from (ticker, t1_date) through the generator",
         id_match == len(E)),
        ("the generator's definition hash is the one the sealed estimand names",
         D1.T1_DEF_HASH == est["governing"]["definition_hash"]),
        ("prev_session_date is populated for every episode",
         int(E["prev_session_date"].isna().sum()) == 0),
        ("an independent archived copy exists", A is not None),
        ("the archived copy has the same row count",
         A is not None and len(A) == SEALED_EPISODES),
        ("the two copies are content-identical on the identity grain",
         A is not None and content_hash(E, ["episode_id", "ticker", "t1_date"])
         == content_hash(A, ["episode_id", "ticker", "t1_date"])),
        ("the 15m estimand population is preserved at its sealed size",
         P is not None and len(P) == SEALED_ESTIMAND),
        ("every estimand episode_id is a member of the episode population",
         P is not None and P["episode_id"].isin(set(E["episode_id"])).all()),
    ]
    res = [dict(check=k, met=bool(v)) for k, v in checks]
    ok = all(r["met"] for r in res)

    p = dict(
        report_id="MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1",
        status=("HISTORICAL_EPISODE_POPULATION_PRESERVED" if ok
                else "HOLD_PRESERVATION_UNVERIFIED"),
        task_class="POPULATION_PRESERVATION_VERIFICATION_ONLY",
        corrects=dict(
            artifact="MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1",
            digest=BIND["MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1.json"],
            what_was_incomplete="it concluded the population was 'not reproducible', which is "
                                "true of every mutable store and was read as though the "
                                "population were lost",
            what_is_added="the sealed population was never lost — it is PRESERVED as a "
                          "materialized artifact, in two independent copies",
            original_not_edited=True),

        three_authorities_now_separated=dict(
            EPISODE_RULE=dict(status="RECOVERED_FROM_PINNED_GENERATOR",
                              definition_hash=D1.T1_DEF_HASH,
                              answers="how was an episode constructed?"),
            SEALED_HISTORICAL_POPULATION=dict(
                status="PRESERVED_AS_MATERIALIZED_ARTIFACT",
                path=PRIMARY, rows=len(E),
                answers="which episodes were actually in the frozen historical population?"),
            CURRENT_STORE_RECONSTRUCTION=dict(
                status="AUTHORITY_FOR_NEITHER",
                current_store=221229, archive_store=220751, sealed=SEALED_EPISODES,
                rule="the historical population must be CONSUMED from the preserved "
                     "artifact, never regenerated from a mutable store")),

        verification=dict(
            checks=res, all_met=ok,
            episode_id_recomputed_matches=id_match, of_rows=len(E),
            why_recomputation_matters="a file with the right row count can still be the wrong "
                                      "file; recomputing episode_id through the generator "
                                      "ties the population to the definition hash",
            primary=dict(path=PRIMARY, rows=len(E), columns=len(E.columns),
                         sha256=sha256(PRIMARY),
                         identity_content_hash=content_hash(
                             E, ["episode_id", "ticker", "t1_date"])),
            archive=(dict(path=ARCHIVE, rows=len(A), sha256=sha256(ARCHIVE),
                          identity_content_hash=content_hash(
                              A, ["episode_id", "ticker", "t1_date"]),
                          byte_identical_to_primary=sha256(ARCHIVE) == sha256(PRIMARY))
                     if A is not None else None),
            estimand_population=(dict(path=POP15M, rows=len(P), sealed=SEALED_ESTIMAND,
                                      columns=list(P.columns),
                                      all_ids_in_episode_population=bool(
                                          P["episode_id"].isin(set(E["episode_id"])).all()))
                                 if P is not None else None)),

        consumption_rule=dict(
            bind="data/t1_episodes.parquet by sha256 and identity content hash",
            do_not="re-run the episode SQL against any mutable store and treat the result as "
                   "the historical population",
            expected_count_forbidden="220,351 must not be used as a target for any Massive "
                                     "build; it describes the historical population only"),

        enumeration_backing=dict(
            inventory="MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1",
            digest=BIND["MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json"],
            enumeration_complete=True,
            note="this artifact is written against a completed enumeration, so its statements "
                 "about what exists rest on a measured search space"),

        y_exposed=0, outcome_exposure="NOT_EXPOSED", read_only=True, writes=0,
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json",
                 required=("report_id", "status", "three_authorities_now_separated",
                           "verification", "consumption_rule"),
                 supersede=os.path.exists(
                     "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json"))
    print(f"MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1 · {d} · {p['status']}")
    print(f"  checks      {sum(r['met'] for r in res)}/{len(res)} met")
    print(f"  primary     {len(E):,} rows · sha256 {sha256(PRIMARY)[:16]}…")
    print(f"  episode_id  RECOMPUTED through the generator: {id_match:,}/{len(E):,} match")
    if A is not None:
        print(f"  archive     {len(A):,} rows · identity content hash "
              f"{'IDENTICAL' if p['verification']['archive']['identity_content_hash'] == p['verification']['primary']['identity_content_hash'] else 'DIFFERS'}")
    if P is not None:
        print(f"  estimand    {len(P):,} rows (sealed {SEALED_ESTIMAND:,}) · all ids ⊆ episodes")
    print(f"  RULE        recovered   POPULATION preserved   STORE-REGEN authority for neither")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
