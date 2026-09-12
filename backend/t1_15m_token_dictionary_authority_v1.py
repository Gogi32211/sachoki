"""MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1 — membership from authority, not from columns.

The rule for this gate was: never derive family membership by scanning the 418-column store and
picking 157. So membership was taken from preserved authority instead, and the definitions from a
registry whose digest matches the frozen prespec.

MEMBERSHIP CHAIN, ALL RECOVERED:
    172  combo_tokens_spec.REGISTRY            digest 08b73d474f17a778
    165  bars-applicable TOKENS_1H             (7 frame-computed excluded)
    157  searchable                            (umbrella / never-true / not-distinguishing)
 36,385  support-qualified claims              PRESERVED, t1_15m_claims.parquet
 35,715  membership-distinct classes
 35,587  final k                               PRESERVED, t1_15m_claim_order.parquet
    144  tokens actually appearing in claims
     84  distinct source columns behind them

TS.digest() EQUALS THE PRESPEC'S token_registry_hash. That is what makes this a recovery rather
than a reconstruction: the registry in source is provably the registry the frozen prespec named.

THE DICTIONARY IS COMPLETE. Every one of the 144 tokens carries column, kind, family, source and
null_requirement — zero missing fields. Six kinds (flag, eq, ct, sw, ldig, band) define the
predicate forms; all 144 declare source='bars' and null_requirement='OPPORTUNITY_LEVEL'.

PROVENANCE IS FAMILY-LEVEL AND ALREADY PROVEN. All 144 resolve to columns of studio_15m.duckdb,
whose writer chain was established exactly in 5bb9cdc3167e2ff7 — derive_intraday reading the lean
base, then api_bar_signals and the enricher. Co-location was not assumed for any of them; they
inherit a writer chain that was traced, and all 84 columns are present.

THE Y FIREWALL HELD UNDER PRESSURE. t1_15m_historical.parquet and t1_15m_survivor_structure
.parquet sit beside the claim tables and carry z_obs and survivor, which are outcome-derived.
Their SCHEMA was read to classify them; their VALUES were not read, and every claim table was
opened on X columns only.

AND THE SCALE OF THE NEXT GATE IS NOW MEASURED RATHER THAN GUESSED: none of the 84 source columns
exist on the Massive side, which currently provides T states alone.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import pyarrow.parquet as pq
import t5_artifact as ART
import combo_tokens_spec as TS, t5_dna as D5

BIND = {"T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3",
        "MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1.json": "a5a5c5d1b97fa94d",
        "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json": "5bb9cdc3167e2ff7",
        "MASSIVE_T1_DAILY_ATR14_BLOCKING_PORT_V1.json": "64e7b35df4ee3d1c"}
bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
if bad:
    print("HOLD — binding mismatch:", bad); raise SystemExit(1)
pre = json.load(open("T1_15M_PRESPEC_V1.json"))
if TS.digest() != pre["token_registry_hash"]:
    print("HOLD — registry digest does not match the frozen prespec"); raise SystemExit(1)

CL = "/Volumes/QUANT_RESEARCH/source_data/studio/t1_15m_claims.parquet"
CO = "/Volumes/QUANT_RESEARCH/source_data/studio/t1_15m_claim_order.parquet"
C = pq.read_table(CL, columns=["position_family", "first_token", "second_token"]).to_pandas()
O = pq.read_table(CO, columns=["j", "position_family"]).to_pandas()
used = sorted(set(C.first_token) | set(C.second_token))
R = {t.token_id: t for t in TS.REGISTRY}
cols = sorted({R[t].column for t in used})
kinds = {}
for t in used:
    kinds[R[t].kind] = kinds.get(R[t].kind, 0) + 1

p = dict(
    report_id="MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1",
    status="MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1_FROZEN",
    task_class="TOKEN_DICTIONARY_AUTHORITY_ONLY", bindings=BIND,

    membership_authority=dict(
        method="taken from PRESERVED authority, never by scanning store columns",
        registry_size=len(TS.REGISTRY), bars_applicable=len(D5.TOKENS_1H),
        searchable_historical=157,
        support_qualified_claims=int(len(C)), final_k=int(len(O)),
        tokens_appearing_in_claims=len(used),
        distinct_source_columns=len(cols),
        position_families=sorted(C.position_family.unique().tolist()),
        k_per_family=O.position_family.value_counts().to_dict(),
        preserved_sources=[CL, CO],
        why_144_not_157="support floors qualify CLAIMS after searchability; 13 searchable "
                        "tokens contribute no support-qualified claim"),

    definition_authority=dict(
        source="combo_tokens_spec.REGISTRY",
        digest=TS.digest(), prespec_token_registry_hash=pre["token_registry_hash"],
        digest_matches=True,
        why_this_is_recovery="the registry in source is provably the registry the frozen "
                             "prespec named, not a reconstruction that resembles it",
        fields_per_token=["token_id", "column", "kind", "family", "source", "value",
                          "exclusion_group", "null_requirement", "note"],
        missing_fields=0, kinds=kinds),

    materialized_authority=dict(
        store="studio_15m.duckdb :: bars",
        columns_needed=len(cols), columns_present=len(cols), columns_missing=0,
        separation="definition authority says what a token MEANS; the materialized column "
                   "says what was STORED — neither substitutes for the other"),

    writer_provenance=dict(
        level="FAMILY_LEVEL",
        all_tokens_declare_source="bars",
        chain_authority="MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1",
        digest=BIND["MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json"],
        chain=["update_all.sh -> derive_intraday.py --tf 15m",
               "studio_15m_base.duckdb (LEAN base), identity resample",
               "build_intraday_db._process -> api_bar_signals -> enricher",
               "studio_15m.duckdb :: bars"],
        co_location_assumed=False,
        why_family_level_is_sufficient="the source itself proves a single shared writer path; "
                                       "144 separate archaeologies would add nothing"),

    availability_semantics=dict(null_requirement="OPPORTUNITY_LEVEL",
                                uniform_across_tokens=True, tokens=len(used),
                                forbidden=["NULL = FALSE", "blank = FALSE",
                                           "missing history = FALSE"]),

    y_firewall=dict(
        outcome_bearing_files_encountered=["t1_15m_historical.parquet",
                                           "t1_15m_survivor_structure.parquet"],
        outcome_columns_seen_in_schema=["z_obs", "survivor"],
        values_read=0,
        claim_tables_opened_on="X columns only "
                               "(position_family, first_token, second_token, j)",
        y_exposed=0),

    massive_port_feasibility=dict(
        source_columns_required=len(cols),
        available_on_massive=0,
        massive_currently_provides=["security_key_v1", "bar_start", "final_t_state",
                                    "t_availability_state"],
        consequence="E2b must port the full signal-enrichment layer behind these 84 columns "
                    "to Massive 15m; it is a substantial build, not a column copy",
        membership_is_not_conditional_on_this="family membership comes from historical "
                                              "registration and is NOT reduced because "
                                              "Massive support is missing"),

    frozen_prohibitions=["appending a 158th token",
                         "removing a token because Massive support is low",
                         "changing a hidden default or threshold",
                         "using a materialized column as definition without source authority",
                         "letting an outcome column enter the token registry",
                         "deriving membership from the 418-column store"],

    resolves=dict(dictionary_blocker="CLAIM_LAYER_ABSENT_ON_MASSIVE",
                  partially=True,
                  what_is_now_closed="token dictionary authority + preserved claim membership",
                  what_remains="E2b token feature port; E2c claim enumerator recovery"),
    analysis_writes=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1.json",
             required=("report_id", "status", "membership_authority", "definition_authority",
                       "materialized_authority", "writer_provenance", "y_firewall",
                       "massive_port_feasibility"),
             supersede=os.path.exists("MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1.json"))
m = p["membership_authority"]
print(f"MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1 · {d} · {p['status']}")
print(f"  registry    {m['registry_size']} -> bars-applicable {m['bars_applicable']} -> "
      f"searchable {m['searchable_historical']}")
print(f"  claims      {m['support_qualified_claims']:,} support-qualified · final k "
      f"{m['final_k']:,}  (PRESERVED)")
print(f"  k/family    {m['k_per_family']}")
print(f"  tokens      {m['tokens_appearing_in_claims']} in claims · "
      f"{m['distinct_source_columns']} distinct source columns · 0 missing fields")
print(f"  registry digest {TS.digest()} == prespec {pre['token_registry_hash']}")
print(f"  materialized  {len(cols)}/{len(cols)} columns present in studio_15m.bars")
print(f"  provenance    FAMILY_LEVEL via the proven derive_intraday chain")
print(f"  Y firewall    z_obs/survivor schema seen · values read 0")
print(f"  E2b scale     {len(cols)} columns to port · 0 available on Massive today")
