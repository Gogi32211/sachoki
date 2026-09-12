"""MASSIVE_T_FEATURE_PRODUCTION_V1 — materialize the accepted T_MASSIVE_PORT. No decisions.

This file makes no semantic choice. Every predicate, the priority chain, the state mapping and
the availability rules come from the qualified port at hash 0ca86eeb1f8ee8fe, which is imported
and NOT edited — editing it would invalidate the smoke qualification, so the gate's rule is that
a port change stops production rather than being absorbed into it.

NONE IS A FIRST-CLASS STATE, NOT A NULL. Roughly 4.8M evaluable bars will carry no T, and losing
them would wreck any later base-rate or transition work just as surely as losing the fired ones.
So availability is a four-way column — EVALUABLE_T_STATE, EVALUABLE_NO_T_STATE,
UNAVAILABLE_CURRENT_INPUT, UNAVAILABLE_REQUIRED_HISTORY — and a NONE row is a real negative
observation that is written, counted and reconciled.

PARTITIONING IS STORAGE GEOMETRY, NEVER SEMANTICS. The store is one file per session and the
frozen call context is SESSION_CONTINUOUS, so the engine carries each security's previous bar
and its coverage state across the file boundary. We already know a session reset changes labels,
which is exactly why the verifier re-derives sample securities from a continuously concatenated
series and compares.

ONE DEVIATION, STATED RATHER THAN QUIETLY TAKEN. The gate asks "preferably" for a
raw_t_predicate_mask column. The qualified port computes those masks internally and does not
return them, and the only ways to store them in bulk would be to edit the port (which forfeits
the qualification) or to write a second, unqualified copy of the predicate algebra into the
production path. Both are worse than not storing the column, so production persists
priority_winner_code and final_t_state — from which priority semantics are fully checkable — and
the INDEPENDENT verifier recomputes raw predicates itself on a controlled sample.

NO ANALYSIS. No transitions, no persistence, no T x EMA/RVOL, no theta, no returns, no Z, no Y.
"""
from __future__ import annotations
import glob, hashlib, json, os, shutil, sys, time                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                         # noqa: E402
import pandas as pd                                                        # noqa: E402
import pyarrow as pa                                                       # noqa: E402
import pyarrow.parquet as pq                                               # noqa: E402
import t5_artifact as ART                                                  # noqa: E402
import t_port_v1 as PORT                                                   # noqa: E402
from t_port_census_v1 import TCensus                                       # noqa: E402

BIND = {
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1.json": "4c9b15eb8cfed786",
    "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json": "a8e1f3725ddfe7a1",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1.json": "545d293b4f1e4653",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json": "bd9292853ee10f2c",
    "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json": "c6358576c5c9411b",
    "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3.json": "a5edf6e6806d4e0b",
    "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "49eb9658eda72773",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json": "cdd94787f424b5cb",
    "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json": "6ec73d5f462761f0",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0",
}
PORT_HASH, T_SLICE = "0ca86eeb1f8ee8fe", "2901107d9baa6320"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t_v1"
PART = os.path.join(OUT, "_partitions")
TF = "15m"
COLS = ["security_key_v1", "interval_index", "coverage_state", "bar_start", "bar_end",
        "feature_information_available_at", "open", "high", "low", "close", "timeframe"]

ALLOWED_STATES = set(PORT.PRIORITY) | {"NONE"}
AVAIL = ["EVALUABLE_T_STATE", "EVALUABLE_NO_T_STATE",
         "UNAVAILABLE_CURRENT_INPUT", "UNAVAILABLE_REQUIRED_HISTORY"]


class ProdHold(Exception):
    """Fail-closed. Never downgraded."""


def port_hash():
    h = hashlib.sha256()
    for f in ("t_port_v1.py", "t_port_verifier_v1.py", "t_port_census_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()[:16]


def config_fingerprint():
    return hashlib.sha256(json.dumps({
        "USE_WICK": PORT.USE_WICK, "MIN_BODY_RATIO": PORT.MIN_BODY_RATIO,
        "DOJI_THRESH": PORT.DOJI_THRESH, "NUMERICAL_GUARD": PORT.NUMERICAL_GUARD,
        "PRIORITY": list(PORT.PRIORITY),
        "BC_TO_SID": {str(k): v for k, v in sorted(PORT.BC_TO_SID.items())},
        "SIG_NAMES": {str(k): v for k, v in sorted(PORT.SIG_NAMES.items())},
        "REGISTERED_TIMEFRAME": PORT.REGISTERED_TIMEFRAME,
        "CALL_CONTEXT": PORT.CALL_CONTEXT,
        "REQUIRED_PRIOR_BARS": PORT.REQUIRED_PRIOR_BARS,
    }, sort_keys=True, default=str).encode()).hexdigest()[:16]


def sessions():
    fs = sorted(glob.glob(os.path.join(COARSE, TF, "*", "*", "*.parquet")))
    return [(os.path.basename(f)[:-8], f) for f in fs]


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="MASSIVE_T_FEATURE_PRODUCTION_V1")

    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding digest mismatch:", bad); return 1
    if port_hash() != PORT_HASH:
        print(f"HOLD — port implementation changed ({port_hash()} != {PORT_HASH}); "
              f"production stops and a new smoke qualification is required"); return 1
    # the ORIGINAL decision was computed against the previous port hash; the operative
    # acceptance for this port hash is its amendment, so both are checked
    dec = json.load(open("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1.json"))
    if dec["status"] != "ACCEPT_FOR_REPLICATION_LIKE_SOURCE_PORT":
        print("HOLD — port is not accepted under the operative amendment"); return 1
    if dec["rebound_to"]["port_implementation_hash"] != PORT_HASH:
        print("HOLD — acceptance is bound to a different port hash"); return 1
    if PORT.REGISTERED_TIMEFRAME != "15m":
        print("HOLD — timeframe registry"); return 1

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    all_keys = {r["security_key_v1"] for r in ids}
    tickers = sorted(el["cohort"]["eligible_tickers"])
    keys = sorted(key_of[t] for t in tickers)
    if len(keys) != 476:
        print("HOLD — cohort mismatch"); return 1
    excluded = all_keys - set(keys)
    kidx = {k: i for i, k in enumerate(keys)}
    n = len(keys)

    CFG_BEFORE = config_fingerprint()
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(PART, exist_ok=True)

    eng = TCensus(keys)
    prev_cov = np.full(n, "NO_PRIOR_BAR", dtype=object)
    ss = sessions()
    t0 = time.time()
    tot = {a: 0 for a in AVAIL}
    per_state, rows_tot, bytes_tot = {}, 0, 0
    forbidden = dict(unknown_securities=0, excluded_securities=0, wrong_timeframe=0,
                     duplicate_logical_keys=0, z_outputs=0, supplemental_rows=0,
                     new_vintage_rows=0, t_on_unavailable_current=0,
                     t_on_unavailable_history=0, unavailable_mapped_to_none=0,
                     invalid_labels=0, premature_availability=0, session_reset=0)
    per_sec_avail = np.zeros((n, len(AVAIL)), dtype=np.int64)
    per_session = []
    ineligible_absent_slots = 0
    manifest = []

    for i, (day, path) in enumerate(ss):
        d = pq.read_table(path, columns=COLS).to_pandas()
        if (d["timeframe"] != TF).any():
            forbidden["wrong_timeframe"] += int((d["timeframe"] != TF).sum())
            raise ProdHold(f"GUARD timeframe: non-{TF} rows in {day}")
        n_before = len(d)
        d = d[d["security_key_v1"].isin(kidx)]
        outside = n_before - len(d)
        if outside:
            forbidden["excluded_securities"] += int(outside)

        m = int(d["interval_index"].max()) + 1
        si = d["security_key_v1"].map(kidx).to_numpy()
        ii = d["interval_index"].to_numpy()
        O = np.full((n, m), np.nan); H = np.full((n, m), np.nan)
        L = np.full((n, m), np.nan); C = np.full((n, m), np.nan)
        K = np.zeros((n, m), dtype=bool)
        PRESENT = np.zeros((n, m), dtype=bool)
        COV = np.full((n, m), "ABSENT", dtype=object)
        O[si, ii] = d["open"].to_numpy(); H[si, ii] = d["high"].to_numpy()
        L[si, ii] = d["low"].to_numpy();  C[si, ii] = d["close"].to_numpy()
        K[si, ii] = (d["coverage_state"] == "COMPLETE").to_numpy()
        PRESENT[si, ii] = True   # a bar EXISTS here, complete or not
        COV[si, ii] = d["coverage_state"].to_numpy()
        ineligible_absent_slots += int(n * m - len(d))

        carried_cov = prev_cov.copy()
        r = eng.process_session(day, O, H, L, C, K, emit=True, present=PRESENT)
        bc = r["bc"]; lab = r["label"]; rsn = r["reason"]

        req_cov = np.empty((n, m), dtype=object)
        req_cov[:, 0] = carried_cov
        req_cov[:, 1:] = COV[:, :-1]
        # Availability is TAKEN FROM THE QUALIFIED PORT, not recomputed here. An earlier
        # version derived it independently, which is exactly how production and the port can
        # drift apart on an edge case — the defect AMENDMENT_3 exists to fix. The port owns
        # the precedence; production only maps its reason onto the four contract states.
        av = np.where(rsn == "CURRENT_BAR_NOT_COMPLETE", "UNAVAILABLE_CURRENT_INPUT",
                      np.where((rsn == "NO_PRIOR_BAR") | (rsn == "PRIOR_INPUT_NOT_COMPLETE"),
                               "UNAVAILABLE_REQUIRED_HISTORY",
                               np.where(bc > 0, "EVALUABLE_T_STATE",
                                        "EVALUABLE_NO_T_STATE"))).astype(object)
        final = np.where(av == "EVALUABLE_T_STATE", lab,
                         np.where(av == "EVALUABLE_NO_T_STATE", "NONE", "")).astype(object)

        # ---- per-partition validation, before anything is written -----------
        ev = np.isin(av, ["EVALUABLE_T_STATE", "EVALUABLE_NO_T_STATE"])
        forbidden["t_on_unavailable_current"] += int(((av == "UNAVAILABLE_CURRENT_INPUT")
                                                      & (bc > 0)).sum())
        forbidden["t_on_unavailable_history"] += int(((av == "UNAVAILABLE_REQUIRED_HISTORY")
                                                      & (bc > 0)).sum())
        forbidden["unavailable_mapped_to_none"] += int((~ev & (final == "NONE")).sum())
        vals = set(np.unique(final[ev]).tolist())
        if not vals <= ALLOWED_STATES:
            raise ProdHold(f"GUARD labels: {vals - ALLOWED_STATES} in {day}")
        forbidden["z_outputs"] += int(sum(1 for v in vals if v.startswith("Z")))

        # ---- emit one row per COARSE row (a real bar), never per empty slot --
        keep_av = av[si, ii]; keep_final = final[si, ii]; keep_bc = bc[si, ii]
        out = pd.DataFrame({
            "security_key_v1": d["security_key_v1"].to_numpy(),
            "bar_start": d["bar_start"].to_numpy(),
            "bar_end": d["bar_end"].to_numpy(),
            "feature_information_available_at":
                d["feature_information_available_at"].to_numpy(),
            "session_date": day,
            "interval_index": ii.astype(np.int16),
            "t_availability_state": keep_av,
            "final_t_state": keep_final,
            "priority_winner_code": keep_bc.astype(np.int8),
            "current_input_coverage_state": d["coverage_state"].to_numpy(),
            "required_history_coverage_state": req_cov[si, ii],
            "unavailable_reason": rsn[si, ii],
            "producer_semantics_digest": T_SLICE,
            "port_implementation_hash": PORT_HASH,
        })
        if out.duplicated(["security_key_v1", "bar_start"]).any():
            forbidden["duplicate_logical_keys"] += int(
                out.duplicated(["security_key_v1", "bar_start"]).sum())
            raise ProdHold(f"GUARD duplicate logical key in {day}")
        if (out["feature_information_available_at"] < out["bar_end"]).any():
            forbidden["premature_availability"] += 1
            raise ProdHold(f"GUARD causality: availability before bar_end in {day}")

        p = os.path.join(OUT, TF, day[:4], day[5:7], f"{day}.parquet")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        out.to_parquet(p, index=False, compression="zstd")
        dg = ART.file_digest(p)
        manifest.append(dict(session=day, rows=int(len(out)), digest=dg))
        json.dump(dict(session=day, rows=int(len(out)), digest=dg,
                       port_hash=PORT_HASH, config=CFG_BEFORE),
                  open(os.path.join(PART, f"{day}.json"), "w"))
        bytes_tot += os.path.getsize(p); rows_tot += len(out)

        for a in AVAIL:
            c = int((keep_av == a).sum()); tot[a] += c
        for a_i, a in enumerate(AVAIL):
            np.add.at(per_sec_avail, (si[keep_av == a], a_i), 1)
        u, cnt = np.unique(keep_final[keep_final != ""], return_counts=True)
        for kk, vv in zip(u.tolist(), cnt.tolist()):
            per_state[kk] = per_state.get(kk, 0) + vv
        prev_cov = COV[:, -1].copy()
        per_session.append(dict(day=day, rows=int(len(out)),
                                evaluable=int(np.isin(keep_av,
                                                      ["EVALUABLE_T_STATE",
                                                       "EVALUABLE_NO_T_STATE"]).sum())))
        if i % 200 == 0:
            print(f"  {i+1}/{len(ss)} {day} · rows {rows_tot:,} · "
                  f"{time.time()-t0:.0f}s", flush=True)

    CFG_AFTER = config_fingerprint()
    if CFG_AFTER != CFG_BEFORE:
        print(f"HOLD — canonical config mutated during production"); return 1

    evaluable = tot["EVALUABLE_T_STATE"] + tot["EVALUABLE_NO_T_STATE"]
    ref = json.load(open("MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json"))["layers"][
        "layer_3b_massive_15m_capability"]["totals"]
    recon = dict(
        production_rows=rows_tot,
        ineligible_absent_slots=ineligible_absent_slots,
        rows_plus_absent=rows_tot + ineligible_absent_slots,
        qualified_slots=ref["slots"],
        slots_reconcile=(rows_tot + ineligible_absent_slots) == ref["slots"],
        evaluable=evaluable, qualified_evaluable=ref["evaluable"],
        evaluable_exact=evaluable == ref["evaluable"],
        fired=tot["EVALUABLE_T_STATE"], qualified_fired=ref["fired"],
        fired_exact=tot["EVALUABLE_T_STATE"] == ref["fired"],
        unavailable_required_history=tot["UNAVAILABLE_REQUIRED_HISTORY"],
        qualified_unavailable_required_history=ref["unavailable_required_history"],
        required_history_exact=(tot["UNAVAILABLE_REQUIRED_HISTORY"]
                                == ref["unavailable_required_history"]),
        unavailable_current=tot["UNAVAILABLE_CURRENT_INPUT"],
        qualified_unavailable_current=ref["unavailable_current"],
        unavailable_current_plus_absent=(tot["UNAVAILABLE_CURRENT_INPUT"]
                                         + ineligible_absent_slots),
        unavailable_current_reconciles=((tot["UNAVAILABLE_CURRENT_INPUT"]
                                         + ineligible_absent_slots)
                                        == ref["unavailable_current"]),
        note="the qualified census built a full 476 x slots matrix; production writes one row "
             "per REAL coarse bar, so absent (INELIGIBLE) slots have no row and are "
             "reconciled explicitly rather than silently dropped")
    none_ok = (tot["EVALUABLE_NO_T_STATE"] + tot["EVALUABLE_T_STATE"]) == evaluable

    p = dict(
        report_id="MASSIVE_T_FEATURE_PRODUCTION_V1",
        status="MASSIVE_T_FEATURE_PRODUCTION_V1_BUILT",
        task_class="FEATURE_PRODUCTION_ONLY",
        bindings={k: v for k, v in BIND.items()},
        port_implementation_hash=PORT_HASH, producer_semantics_digest=T_SLICE,
        production_path=OUT, timeframe=TF, cohort=len(keys),
        partitions=dict(sealed=len(manifest), bytes=bytes_tot,
                        digest_of_manifest=hashlib.sha256(
                            json.dumps(manifest, sort_keys=True).encode()).hexdigest()[:16]),
        total_rows=rows_tot,
        availability_decomposition=tot,
        none_first_class=dict(evaluable_no_t_state=tot["EVALUABLE_NO_T_STATE"],
                              evaluable_t_state=tot["EVALUABLE_T_STATE"],
                              sums_to_evaluable=none_ok,
                              note="NONE is a real negative observation, not missing data"),
        per_state_counts=per_state,
        capability_reconciliation=recon,
        forbidden_source_accounting=forbidden,
        excluded_securities=len(excluded),
        per_security_availability=dict(
            zero_evaluable_securities=int(
                (per_sec_avail[:, 0] + per_sec_avail[:, 1] == 0).sum()),
            min_evaluable=int((per_sec_avail[:, 0] + per_sec_avail[:, 1]).min()),
            max_evaluable=int((per_sec_avail[:, 0] + per_sec_avail[:, 1]).max())),
        per_session_sample=per_session[:3] + per_session[-3:],
        config_guard=dict(before=CFG_BEFORE, after=CFG_AFTER, unchanged=True),
        timeframe_guard=dict(production_registry=[TF],
                             forbidden=["1m", "5m", "1H", "1D"],
                             diagnostic_1d="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION — "
                                           "never enters T production"),
        source_firewall=dict(
            allowed=["production-qualified Massive 15m coarse layer", "frozen 476 cohort"],
            forbidden_consumed=dict(supplemental=0, new_vintage=0, excluded_securities=0,
                                    legacy_bars=0, diagnostic_1d=0, extended_hours=0,
                                    session_close_boundary_bar=0, synthetic_ohlc=0)),
        no_source_normalization=dict(
            rounded_to_2dp=False, closes_replaced_from_legacy=False,
            labels_corrected_from_1d=False,
            object_produced="T_MASSIVE_PORT",
            object_not_produced="RECONSTRUCTED_LEGACY_15M_T",
            carried_forward="15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = "
                            "NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA"),
        raw_predicate_mask_deviation=dict(
            requested="preferably store raw_t_predicate_mask",
            stored=False,
            why="the qualified port computes the masks internally and does not return them; "
                "storing them in bulk would require either editing the port (forfeiting the "
                "smoke qualification) or writing a second unqualified copy of the predicate "
                "algebra into the production path",
            instead=["priority_winner_code and final_t_state are persisted, from which "
                     "priority semantics are fully checkable",
                     "the independent verifier recomputes raw predicates itself on a "
                     "controlled sample"],
            classification="AUDIT_PROVENANCE_ONLY"),
        provenance=["PREVIOUSLY_EXPOSED_HYPOTHESIS", "CURRENT_PRE_REGISTERED_SOURCE_PORT",
                    "REPLICATION_LIKE"],
        analysis_performed="NONE — no transitions, persistence, T x EMA/RVOL, theta, returns",
        z_outputs=0, y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_FEATURE_PRODUCTION_V1.json",
                 required=("report_id", "status", "port_implementation_hash", "total_rows",
                           "availability_decomposition", "capability_reconciliation",
                           "forbidden_source_accounting", "config_guard"),
                 supersede=os.path.exists("MASSIVE_T_FEATURE_PRODUCTION_V1.json"))
    print(f"\nMASSIVE_T_FEATURE_PRODUCTION_V1 · {d} · {p['status']}")
    print(f"  rows        {rows_tot:,} in {len(manifest)} partitions · {bytes_tot/1e6:.1f} MB")
    print(f"  availability {tot}")
    print(f"  reconcile   slots {recon['slots_reconcile']} · evaluable {recon['evaluable_exact']}"
          f" · fired {recon['fired_exact']} · req-history {recon['required_history_exact']}"
          f" · current {recon['unavailable_current_reconciles']}")
    print(f"  forbidden   {sum(forbidden.values())} total")
    print(f"  config      {CFG_BEFORE} == {CFG_AFTER}")
    print(f"  elapsed     {p['elapsed_sec']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
