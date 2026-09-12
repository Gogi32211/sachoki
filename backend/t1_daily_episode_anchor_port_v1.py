"""MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1 — the episode selector, ported to Massive 1D.

The historical study selects episodes by a DAILY t_sig == 'T1', then reads that same session's
opening hour. This gate ports the selector — nothing else. It is an EPISODE ANCHOR, not a
hypothesis, not a feature family, and not a second research timeframe.

IDENTITY IS security_key_v1 + session_date. Ticker never enters anchor construction; it is used
only to map the PRESERVED historical positives for the diagnostic, and only through the frozen
mapping authority. Ambiguous or unmapped tickers become UNAVAILABLE_FOR_COMPARISON rather than
being resolved by resemblance.

THE ANCHOR IS THE PRIORITY-RESOLVED STATE, NOT THE RAW PREDICATE. The historical selector read
bars.t_sig, which is the final resolved label. So a bar where raw T1 fires but T4, T6, T1G or
T2G outranks it is ANCHOR_FALSE. Getting this backwards would silently widen the episode
universe.

AVAILABILITY IS FROZEN HERE, NOT INHERITED. AMENDMENT_3's precedence was qualified for 15m; this
gate restates it for 1D explicitly rather than assuming it carries. And structural
non-membership stays its own category: GEV's pre-regular-way sessions are OUTSIDE SUPPORT, never
ANCHOR_FALSE and never UNAVAILABLE.

THE HISTORICAL DIAGNOSTIC USES PRESERVED POSITIVES AS ITS ONLY AUTHORITY. The daily materializer
provenance is qualified NOT_VERIFIABLE_FROM_PRESERVED_INPUTS, so a reconstruction from the stored
rounded OHLC may not stand in for historical truth. And because t1_episodes.parquet preserves
POSITIVES only, a Massive positive absent from it is recorded as
MASSIVE_POSITIVE_NOT_IN_PRESERVED_HISTORICAL_POSITIVE_SET — never as a historical negative. No
confusion matrix is fabricated from a complement that was never preserved.

220,351 IS NOT A TARGET. It describes a 5,252-ticker historical universe at its own vintage. This
cohort is 476 securities on a different source.
"""
from __future__ import annotations
import glob, hashlib, json, os, shutil, sys, time                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
import t_port_v1 as PORT                                                  # noqa: E402
from signal_engine import compute_signals                                 # noqa: E402

BIND = {
    "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
    "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1.json": "7b4a7ac5facbf0a7",
    "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION.json": "ded5c340dfb9fd6a",
    "MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json": "819329bf48bd7175",
    "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json": "4a19b83f40adc6cc",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND.json": "d90a9c3a20f9a838",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json": "6ec73d5f462761f0",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0",
}
COARSE1D = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/1D"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t1_daily_anchor_v1"
PRESERVED = "/Users/sachoki/Desktop/sachoki-desktop/data/t1_episodes.parquet"
T_SLICE = "2901107d9baa6320"
STATES = ["ANCHOR_TRUE", "ANCHOR_FALSE", "UNAVAILABLE_CURRENT_INPUT",
          "UNAVAILABLE_REQUIRED_HISTORY", "STRUCTURALLY_OUTSIDE_SUPPORT"]


def anchor_port_hash():
    h = hashlib.sha256()
    for f in ("t1_daily_episode_anchor_port_v1.py", "signal_engine.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()[:16]


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1")
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1
    reb = json.load(open("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND.json"))
    if reb["status"] != "ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT":
        print("HOLD — port not accepted"); return 1
    temporal = json.load(open("MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json"))

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    tick_of = {v: k for k, v in key_of.items()}
    keys = sorted(key_of[t] for t in el["cohort"]["eligible_tickers"])
    if len(keys) != 476:
        print("HOLD — cohort mismatch"); return 1
    kset = set(keys)
    APH = anchor_port_hash()

    # ---------- load Massive 1D per security -------------------------------
    print("  loading Massive 1D …", flush=True)
    acc = {}
    for f in sorted(glob.glob(os.path.join(COARSE1D, "*", "*", "*.parquet"))):
        day = os.path.basename(f)[:-8]
        d = pq.read_table(f, columns=["security_key_v1", "coverage_state", "bar_end",
                                      "feature_information_available_at",
                                      "open", "high", "low", "close"]).to_pandas()
        d = d[d["security_key_v1"].isin(kset)]
        if len(d):
            d = d.assign(session_date=day)
            for k, g in d.groupby("security_key_v1", sort=False):
                acc.setdefault(k, []).append(g)
    MV = {k: pd.concat(v, ignore_index=True).sort_values("session_date")
          for k, v in acc.items()}
    del acc
    print(f"  securities with 1D rows: {len(MV)}", flush=True)

    shutil.rmtree(OUT, ignore_errors=True); os.makedirs(OUT, exist_ok=True)
    counts = {s: 0 for s in STATES}
    reasons = dict(NO_PRIOR_BAR=0, PRIOR_INPUT_NOT_COMPLETE=0, CURRENT_BAR_NOT_COMPLETE=0)
    per_sec = {}
    frames = []
    t0 = time.time()

    for n_i, k in enumerate(keys):
        if k not in MV:
            continue
        m = MV[k]
        o = m["open"].to_numpy(float); h = m["high"].to_numpy(float)
        l = m["low"].to_numpy(float); c = m["close"].to_numpy(float)
        comp = (m["coverage_state"].to_numpy() == "COMPLETE")
        idx = pd.to_datetime(m["session_date"])
        s = compute_signals(pd.DataFrame({"open": o, "high": h, "low": l, "close": c},
                                         index=idx))
        bc = s["bc"].to_numpy().astype(int)
        final = np.where(bc > 0, s["sig_name"].to_numpy(), "NONE")

        n = len(m)
        prior_exists = np.zeros(n, bool); prior_exists[1:] = True
        prior_ok = np.zeros(n, bool); prior_ok[1:] = comp[:-1]
        # AMENDMENT_3 precedence, restated for 1D — disjoint masks
        s1 = ~prior_exists
        s2 = prior_exists & ~comp
        s3 = prior_exists & comp & ~prior_ok
        ev = prior_exists & comp & prior_ok
        state = np.empty(n, dtype=object); reason = np.full(n, "", dtype=object)
        state[s1] = "UNAVAILABLE_REQUIRED_HISTORY"; reason[s1] = "NO_PRIOR_BAR"
        state[s2] = "UNAVAILABLE_CURRENT_INPUT"; reason[s2] = "CURRENT_BAR_NOT_COMPLETE"
        state[s3] = "UNAVAILABLE_REQUIRED_HISTORY"; reason[s3] = "PRIOR_INPUT_NOT_COMPLETE"
        state[ev] = np.where(final[ev] == "T1", "ANCHOR_TRUE", "ANCHOR_FALSE")

        out = pd.DataFrame({
            "security_key_v1": k, "session_date": m["session_date"].to_numpy(),
            "structural_support_state": "IN_SUPPORT",
            "anchor_availability": state,
            "anchor_unavailable_reason": reason,
            "daily_final_t_state": np.where(ev, final, ""),
            "anchor_true": (state == "ANCHOR_TRUE"),
            "anchor_information_available_at":
                m["feature_information_available_at"].to_numpy(),
            "priority_winner_code": np.where(ev, bc, 0).astype(np.int8),
            "producer_semantics_digest": T_SLICE,
            "daily_anchor_port_hash": APH})
        frames.append(out)
        for st in STATES[:4]:
            counts[st] += int((state == st).sum())
        for rr in reasons:
            reasons[rr] += int((reason == rr).sum())
        per_sec[k] = dict(evaluable=int(ev.sum()),
                          t1=int((state == "ANCHOR_TRUE").sum()), rows=n)
        if n_i % 120 == 0:
            print(f"    {n_i+1}/{len(keys)} · {time.time()-t0:.0f}s", flush=True)

    A = pd.concat(frames, ignore_index=True)
    p_out = os.path.join(OUT, "anchors.parquet")
    A.to_parquet(p_out, index=False, compression="zstd")
    dg = ART.file_digest(p_out)

    # GEV's absent sessions are STRUCTURALLY_OUTSIDE_SUPPORT, not FALSE / UNAVAILABLE
    all_sessions = sorted({os.path.basename(f)[:-8]
                           for f in glob.glob(os.path.join(COARSE1D, "*", "*", "*.parquet"))})
    outside = 0
    for k in keys:
        outside += len(all_sessions) - (len(MV[k]) if k in MV else 0)
    counts["STRUCTURALLY_OUTSIDE_SUPPORT"] = outside

    # ---------- historical diagnostic: PRESERVED positives only -------------
    E = pq.read_table(PRESERVED, columns=["episode_id", "ticker", "t1_date"]).to_pandas()
    mapped = E["ticker"].map(key_of)
    diag = dict(preserved_total=len(E),
                unmapped_or_ambiguous=int(mapped.isna().sum()),
                mapped_into_cohort=int(mapped.notna().sum()))
    H = E.assign(key=mapped).dropna(subset=["key"])
    H = H[H["key"].isin(kset)]
    ak = A.set_index(["security_key_v1", "session_date"])
    hj = H.assign(sd=H["t1_date"].astype(str))
    idx = pd.MultiIndex.from_arrays([hj["key"], hj["sd"]])
    present = idx.isin(ak.index)
    diag["preserved_positives_in_cohort"] = int(len(hj))
    diag["within_massive_session_support"] = int(present.sum())
    sub = ak.reindex(idx[present])
    diag["H_true_M_true"] = int((sub["anchor_availability"] == "ANCHOR_TRUE").sum())
    diag["H_true_M_false"] = int((sub["anchor_availability"] == "ANCHOR_FALSE").sum())
    diag["H_true_M_unavailable"] = int(
        sub["anchor_availability"].isin(["UNAVAILABLE_CURRENT_INPUT",
                                         "UNAVAILABLE_REQUIRED_HISTORY"]).sum())
    diag["H_true_outside_massive_support"] = int((~present).sum())
    diag["retention_on_evaluable"] = (
        round(diag["H_true_M_true"] /
              (diag["H_true_M_true"] + diag["H_true_M_false"]), 6)
        if (diag["H_true_M_true"] + diag["H_true_M_false"]) else None)
    m_true = int((A["anchor_availability"] == "ANCHOR_TRUE").sum())
    hset = set(zip(hj["key"], hj["sd"]))
    mt = A[A["anchor_availability"] == "ANCHOR_TRUE"]
    diag["MASSIVE_POSITIVE_NOT_IN_PRESERVED_HISTORICAL_POSITIVE_SET"] = int(
        sum(1 for a, b in zip(mt["security_key_v1"], mt["session_date"])
            if (a, b) not in hset))
    diag["note"] = ("t1_episodes.parquet preserves POSITIVES only, so a Massive positive "
                    "absent from it is NOT a historical negative and no confusion matrix is "
                    "constructed from a complement that was never preserved")

    ev_secs = sum(1 for v in per_sec.values() if v["evaluable"] > 0)
    t1_secs = sum(1 for v in per_sec.values() if v["t1"] > 0)

    p = dict(
        report_id="MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1",
        status="MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1_PASS",
        task_class="EPISODE_ANCHOR_PORT_ONLY",
        bindings=BIND,
        role="REGISTERED_EPISODE_ANCHOR_ONLY",
        registry=dict(RESEARCH_STATE_TIMEFRAMES=["15m"],
                      EPISODE_SELECTOR_TIMEFRAMES=["1D"], disjoint_roles=True),
        daily_anchor_port_hash=APH,
        hash_is_role_bound=dict(timeframe="1D", role="EPISODE_SELECTOR_ONLY",
                                distinct_from_15m_port="0ca86eeb1f8ee8fe",
                                why="a 15m research port hash must not become a "
                                    "multi-timeframe T engine hash"),
        identity=dict(anchor_key=["security_key_v1", "session_date"], ticker_used=False,
                      ticker_role="historical diagnostic mapping only, via frozen authority"),
        anchor_semantics=dict(basis="priority-resolved FINAL daily T state",
                              anchor_true_iff="final_t_state == 'T1'",
                              raw_predicate_insufficient=True,
                              why="the historical selector read bars.t_sig, the resolved "
                                  "label; a raw T1 outranked by T4/T6/T1G/T2G is ANCHOR_FALSE"),
        availability_precedence=dict(
            frozen_here_for_1D=True, not_inherited_from_15m=True,
            order=["NO_PRIOR_BAR -> UNAVAILABLE_REQUIRED_HISTORY",
                   "prior exists + current not COMPLETE -> UNAVAILABLE_CURRENT_INPUT",
                   "current COMPLETE + prior not COMPLETE -> UNAVAILABLE_REQUIRED_HISTORY "
                   "(PRIOR_INPUT_NOT_COMPLETE)",
                   "otherwise -> EVALUABLE"]),
        temporal_semantics=dict(
            authority="MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1",
            digest=BIND["MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json"],
            anchor_session_date="D", selected_opening_hour_session="D",
            classification=temporal["classification"],
            prohibitions=["NOT a real-time predictor", "NOT known at M1/M2/M3/M4",
                          "NOT tradable at an opening-hour slot on this anchor"]),

        capability_census=dict(
            securities=len(keys), sessions=len(all_sessions),
            counts=counts, unavailable_reasons=reasons,
            securities_with_any_evaluable_day=ev_secs,
            securities_with_any_T1_anchor=t1_secs,
            zero_evaluable_securities=len(keys) - ev_secs,
            zero_T1_securities=len(keys) - t1_secs,
            no_support_threshold_invented=True),

        historical_diagnostic=diag,
        historical_authority=dict(
            used="PRESERVED positive episode set (t1_episodes.parquet)",
            digest=BIND["MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json"],
            forbidden="engine(stored rounded daily OHLC) as truth",
            because="daily same-input conformance is NOT_VERIFIABLE_FROM_PRESERVED_INPUTS"),
        not_a_target=dict(value=220351,
                          why="it describes a 5,252-ticker historical universe at its own "
                              "vintage; this cohort is 476 securities on a different source"),

        output=dict(path=p_out, rows=int(len(A)), digest=dg,
                    schema=list(A.columns)),
        allowed_claim=("the historical daily T1 selector semantics are ported to Massive 1D "
                       "source data using the resolved canonical producer path; exact "
                       "same-input reproduction of the historical daily materialization "
                       "cannot be tested because the unrounded historical input frame was not "
                       "preserved"),
        forbidden_claims=["Massive daily T1 exactly reproduces the historical daily T1 "
                          "materializer population",
                          "1D T is a registered research state",
                          "the anchor is known during the opening hour"],
        pass_means_only="a Massive-native daily T1 episode-selector layer was built on the "
                        "frozen 476 cohort",
        pass_does_not_mean=["historical episode population reproduced", "a 1D T edge",
                            "real-time predictability", "opening-hour tradability",
                            "C4 authorized", "Y exposure"],
        y_exposed=0, z_outputs=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1.json",
                 required=("report_id", "status", "role", "identity", "anchor_semantics",
                           "availability_precedence", "capability_census",
                           "historical_diagnostic", "output"),
                 supersede=os.path.exists("MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1.json"))
    print(f"\nMASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1 · {d} · {p['status']}")
    print(f"  anchor hash {APH}  (role-bound: 1D / EPISODE_SELECTOR_ONLY)")
    print(f"  rows        {len(A):,} · {dg}")
    print(f"  census      {counts}")
    print(f"  reasons     {reasons}")
    print(f"  securities  evaluable {ev_secs}/{len(keys)} · with T1 {t1_secs}/{len(keys)}")
    print(f"  historical  {diag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
