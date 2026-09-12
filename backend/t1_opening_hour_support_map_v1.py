"""MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1 — X-side support geometry, nothing else.

For every Massive-native daily T1 anchor, map the exact same-session opening-hour positions the
recovered historical programme requires. Support only: no outcome, no theta, no pair test, no
transition, no edge.

THE POPULATION IS ALL 8,588 MASSIVE-NATIVE ANCHORS. Not the 8,117 that also appear in the
preserved historical positive set. Building on the intersection would quietly turn a
replication-like source and cohort port into a historical-intersection study, and would change
the estimand without anyone registering the change. Historical overlap travels as a per-row
DIAGNOSTIC FLAG and is never an inclusion criterion.

POSITIONS ARE BY CLOCK. M1=09:30, M2=09:45, M3=10:00, M4=10:15 ET, matched on the exact
timestamp. If 09:45 is absent then M2 is unavailable and 10:00 stays M3 — no next-observed bar,
no nearest match, no fill, no shifting. That is the historical selector's own rule and the
reason it exists: a slid slot would manufacture memberships that never occurred.

THREE THINGS ARE KEPT APART PER SLOT, because collapsing them would corrupt every downstream
denominator: whether the ROW exists, whether T was EVALUABLE on it, and what the STATE was. "Row
missing", "row present but T unavailable", "evaluable and NONE" and "evaluable and T*" are four
different facts.

THE ANCHOR IS NOT KNOWN DURING THE OPENING HOUR. anchor_information_available_at falls after D's
daily bar completes, which is after every slot it selects. Carried forward as
POST_EXPOSURE_SESSION_LOCALIZATION so no downstream layer can read this map as "anchor observed
-> trade at M1".

PAIRS ARE POSITION_PAIR_SUPPORT, NOT TRANSITIONS. The arrow in M1->M2 is a position-pair claim
from the recovered historical grammar, not a T-state change.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402

BIND = {
    "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
    "MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json": "819329bf48bd7175",
    "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION.json": "ded5c340dfb9fd6a",
    "MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1.json": "26e7eda6ffd22a95",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
    "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json": "4a19b83f40adc6cc",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0",
}
ANCH = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t1_daily_anchor_v1/anchors.parquet"
T15 = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t_v1/15m"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t1_opening_hour_v1"
PRESERVED = "/Users/sachoki/Desktop/sachoki-desktop/data/t1_episodes.parquet"
ET = "America/New_York"
SLOTS = {"09:30": "M1", "09:45": "M2", "10:00": "M3", "10:15": "M4"}
PAIRS = (("M1_M2", "M1", "M2"), ("M2_M3", "M2", "M3"), ("M3_M4", "M3", "M4"))
EXPECT_ANCHORS = 8588


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1")
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1
    anch_art = json.load(open("MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1.json"))
    if anch_art["status"] != "MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1_PASS":
        print("HOLD — anchor port did not pass"); return 1

    A = pq.read_table(ANCH).to_pandas()
    A = A[A["anchor_availability"] == "ANCHOR_TRUE"].copy()
    n_anchor = len(A)
    print(f"  Massive-native ANCHOR_TRUE rows: {n_anchor:,} "
          f"(cross-check expectation {EXPECT_ANCHORS:,})", flush=True)

    # historical overlap is a FLAG, never a filter
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    E = pq.read_table(PRESERVED, columns=["ticker", "t1_date"]).to_pandas()
    hset = set(zip(E["ticker"].map(key_of), E["t1_date"].astype(str)))
    A["historical_preserved_positive_overlap"] = [
        (k, d) in hset for k, d in zip(A["security_key_v1"], A["session_date"])]

    by_day = {d: set(g["security_key_v1"]) for d, g in A.groupby("session_date")}
    rows = []
    t0 = time.time()
    for i, (day, keys) in enumerate(sorted(by_day.items())):
        p = os.path.join(T15, day[:4], day[5:7], f"{day}.parquet")
        if not os.path.exists(p):
            for k in keys:
                rows.append(dict(security_key_v1=k, session_date=day, _missing_partition=True))
            continue
        d = pq.read_table(p, columns=["security_key_v1", "bar_start",
                                      "t_availability_state", "final_t_state",
                                      "unavailable_reason"]).to_pandas()
        d = d[d["security_key_v1"].isin(keys)]
        if len(d):
            et = (pd.to_datetime(d["bar_start"], unit="ms", utc=True)
                  .dt.tz_convert(ET).dt.strftime("%H:%M"))
            d = d.assign(slot=et.map(SLOTS))
            d = d[d["slot"].notna()]
        got = {k: {} for k in keys}
        for k, sl, av, st in zip(d["security_key_v1"], d["slot"],
                                 d["t_availability_state"], d["final_t_state"]):
            got[k][sl] = (av, st)
        for k in keys:
            r = dict(security_key_v1=k, session_date=day)
            for sl in ("M1", "M2", "M3", "M4"):
                if sl in got[k]:
                    av, st = got[k][sl]
                    r[f"{sl}_row_present"] = True
                    r[f"{sl}_t_availability"] = av
                    r[f"{sl}_final_t_state"] = st if st else ""
                else:
                    r[f"{sl}_row_present"] = False
                    r[f"{sl}_t_availability"] = "ROW_ABSENT"
                    r[f"{sl}_final_t_state"] = ""
            rows.append(r)
        if i % 200 == 0:
            print(f"    {i+1}/{len(by_day)} sessions · {time.time()-t0:.0f}s", flush=True)

    M = pd.DataFrame(rows)
    M = A[["security_key_v1", "session_date", "anchor_information_available_at",
           "historical_preserved_positive_overlap"]].merge(
        M, on=["security_key_v1", "session_date"], how="left")

    def usable(sl):
        return M[f"{sl}_t_availability"].isin(["EVALUABLE_T_STATE", "EVALUABLE_NO_T_STATE"])

    per_slot = {}
    for sl in ("M1", "M2", "M3", "M4"):
        pres = M[f"{sl}_row_present"].fillna(False)
        us = usable(sl)
        per_slot[sl] = dict(
            row_present=int(pres.sum()), row_absent=int((~pres).sum()),
            present_but_t_unavailable=int((pres & ~us).sum()),
            usable=int(us.sum()),
            usable_and_T=int((us & (M[f"{sl}_final_t_state"].fillna("")
                                    .str.startswith("T"))).sum()),
            usable_and_NONE=int((us & (M[f"{sl}_final_t_state"] == "NONE")).sum()))

    per_pair = {}
    for name, a, b in PAIRS:
        ua, ub = usable(a), usable(b)
        pa = M[f"{a}_row_present"].fillna(False); pb = M[f"{b}_row_present"].fillna(False)
        per_pair[name] = dict(
            both_rows_present=int((pa & pb).sum()),
            comparable_both_usable=int((ua & ub).sum()),
            lost_row_absent=int((~(pa & pb)).sum()),
            lost_row_present_but_unavailable=int(((pa & pb) & ~(ua & ub)).sum()),
            classification="POSITION_PAIR_SUPPORT",
            explicitly_not="T_STATE_TRANSITION")

    all4_rows = int((M["M1_row_present"].fillna(False) & M["M2_row_present"].fillna(False)
                     & M["M3_row_present"].fillna(False)
                     & M["M4_row_present"].fillna(False)).sum())
    all4_usable = int((usable("M1") & usable("M2") & usable("M3") & usable("M4")).sum())

    retained = all4_usable
    lost_absent = int((~(M["M1_row_present"].fillna(False)
                         & M["M2_row_present"].fillna(False)
                         & M["M3_row_present"].fillna(False)
                         & M["M4_row_present"].fillna(False))).sum())
    lost_unavail = n_anchor - retained - lost_absent

    os.makedirs(OUT, exist_ok=True)
    op = os.path.join(OUT, "opening_hour_support.parquet")
    M.to_parquet(op, index=False, compression="zstd")
    dg = ART.file_digest(op)

    ov = int(M["historical_preserved_positive_overlap"].sum())
    p = dict(
        report_id="MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1",
        status="MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1_PASS",
        task_class="X_SIDE_SUPPORT_GEOMETRY_ONLY",
        bindings=BIND,

        population=dict(
            C4_INPUT_ANCHORS="all Massive-native ANCHOR_TRUE rows",
            count=n_anchor, cross_check_expectation=EXPECT_ANCHORS,
            matches=n_anchor == EXPECT_ANCHORS,
            explicitly_not="the historical-positive intersection",
            why="building on the intersection would convert a replication-like source and "
                "cohort port into a historical-intersection study and change the estimand "
                "without registration"),

        slot_rule=dict(M1="D 09:30 ET", M2="D 09:45 ET", M3="D 10:00 ET", M4="D 10:15 ET",
                       matching="EXACT timestamp by CLOCK",
                       forbidden=["next observed bar", "nearest bar", "forward fill",
                                  "backfill", "slot shifting"],
                       example="if 09:45 is absent, M2 is unavailable and 10:00 remains M3"),

        three_facts_per_slot=dict(
            slot_row_present="does the 15m row exist at all",
            slot_t_availability="was T evaluable on it",
            slot_final_t_state="what the state was",
            four_distinct=["row missing", "row present but T unavailable",
                           "evaluable and NONE", "evaluable and T*"],
            why="collapsing them corrupts every downstream denominator"),

        per_slot=per_slot,
        per_pair=per_pair,
        all_four=dict(rows_present=all4_rows, all_usable=all4_usable),

        support_decomposition=dict(
            anchors=n_anchor,
            retained_full_opening_hour=retained,
            lost_exact_slot_absent=lost_absent,
            lost_slot_present_but_feature_unavailable=lost_unavail,
            note="shows how much further support the opening-hour layer costs after the "
                 "daily availability loss already measured in C3"),

        temporal_guard=dict(
            classification="POST_EXPOSURE_SESSION_LOCALIZATION",
            anchor_information_available_at="after D's daily bar completes — later than every "
                                            "slot it selects",
            forbidden_downstream="anchor observed -> trade at M1",
            authority=BIND["MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json"]),

        historical_overlap=dict(
            rows_flagged=ov, not_flagged=n_anchor - ov,
            role="DIAGNOSTIC FLAG ONLY",
            explicitly_not="an inclusion criterion",
            not_in_preserved_set_means="MASSIVE_POSITIVE_NOT_IN_PRESERVED_HISTORICAL_"
                                       "POSITIVE_SET, never a historical negative"),

        pairs_are_not_transitions=dict(
            classification="POSITION_PAIR_SUPPORT",
            arrow_meaning="a position-pair claim from the recovered historical grammar",
            explicitly_not="a T-state change"),

        output=dict(path=op, rows=int(len(M)), digest=dg, schema=list(M.columns)),
        computed_here=["support geometry"],
        not_computed_here=["Y", "theta", "M1->M2 / M2->M3 / M3->M4 outcomes", "edge",
                           "transitions"],
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1.json",
                 required=("report_id", "status", "population", "slot_rule",
                           "three_facts_per_slot", "per_slot", "per_pair",
                           "support_decomposition", "temporal_guard"),
                 supersede=os.path.exists("MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1.json"))
    print(f"\nMASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1 · {d} · {p['status']}")
    print(f"  anchors     {n_anchor:,} (expectation {EXPECT_ANCHORS:,} · "
          f"match {n_anchor == EXPECT_ANCHORS})")
    print("  per slot (present / absent / present-but-unavailable / usable):")
    for sl, v in per_slot.items():
        print(f"    {sl}  {v['row_present']:>6,} / {v['row_absent']:>6,} / "
              f"{v['present_but_t_unavailable']:>6,} / {v['usable']:>6,}")
    print("  per pair (both rows / comparable):")
    for k, v in per_pair.items():
        print(f"    {k}  {v['both_rows_present']:>6,} / {v['comparable_both_usable']:>6,}")
    print(f"  all four    rows {all4_rows:,} · usable {all4_usable:,}")
    print(f"  decomposition retained {retained:,} · slot-absent {lost_absent:,} · "
          f"feature-unavailable {lost_unavail:,}")
    print(f"  historical overlap flag: {ov:,} of {n_anchor:,} (diagnostic only)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
