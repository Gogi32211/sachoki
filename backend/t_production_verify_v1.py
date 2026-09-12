"""Independent verification of MASSIVE_T_FEATURE_PRODUCTION_V1.

Two passes that answer different questions and are kept apart on purpose.

THE ROW-LEVEL PASS RE-DERIVES T FROM RAW OHLC, not from anything production computed. It picks
sample securities, concatenates their ENTIRE 15m history across all 1,254 partitions into one
continuous series — which is the whole point, since the store is partitioned by session and the
contract is session-continuous — and recomputes availability, priority winner and final state
with the scalar if/elif verifier. Production used the vectorised census; these are different
programs, so agreement means something. Required mismatch: zero.

THE STRUCTURAL PASS reads every written partition and checks the things a sample cannot: logical
key uniqueness across 15.4M rows, cohort membership, timeframe, the availability decomposition
summing to the row count, NONE accounting, label validity, no Z, and that the sealed per-partition
digests still match the files on disk.

THE SESSION-BOUNDARY CHECK IS CALLED OUT SEPARATELY because we already measured that a session
reset changes labels. So the first bar of each session is compared specifically, not just
averaged into the row-level pass — a partition-boundary reset would otherwise hide inside a 99.9%
agreement figure.

This file imports the scalar verifier and the raw stores. It does NOT import t_production_v1.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
import t_port_verifier_v1 as VER                                          # noqa: E402

PROD_ART, PROD_D = "MASSIVE_T_FEATURE_PRODUCTION_V1.json", None
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/15m"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t_v1/15m"
PART = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_t_v1/_partitions"
AVAIL = ["EVALUABLE_T_STATE", "EVALUABLE_NO_T_STATE",
         "UNAVAILABLE_CURRENT_INPUT", "UNAVAILABLE_REQUIRED_HISTORY"]
VALID = {"T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5", "T12", "NONE"}


def parts(root):
    return sorted(glob.glob(os.path.join(root, "*", "*", "*.parquet")))


def row_level(sample_keys):
    """Re-derive T for sample securities from raw OHLC over one continuous series."""
    cf, pf = parts(COARSE), parts(OUT)
    src, got = [], []
    for f in cf:
        d = pq.read_table(f, columns=["security_key_v1", "interval_index", "coverage_state",
                                      "bar_start", "open", "high", "low", "close"]).to_pandas()
        d = d[d["security_key_v1"].isin(sample_keys)]
        if len(d):
            src.append(d)
    for f in pf:
        d = pq.read_table(f, columns=["security_key_v1", "bar_start", "t_availability_state",
                                      "final_t_state", "priority_winner_code",
                                      "unavailable_reason"]).to_pandas()
        d = d[d["security_key_v1"].isin(sample_keys)]
        if len(d):
            got.append(d)
    src = pd.concat(src, ignore_index=True).sort_values(["security_key_v1", "bar_start"])
    got = pd.concat(got, ignore_index=True).sort_values(["security_key_v1", "bar_start"])

    # `reason` is compared explicitly: the 4-way availability state would mask a
    # NO_PRIOR_BAR / PRIOR_INPUT_NOT_COMPLETE mix-up, which is exactly the defect
    # class AMENDMENT_3 was written for.
    mism = dict(availability=0, reason=0, final_state=0, priority=0, compared=0,
                boundary_compared=0, boundary_mismatch=0)
    for k, g in src.groupby("security_key_v1"):
        g = g.sort_values("bar_start")
        rows = list(zip(g["open"], g["high"], g["low"], g["close"]))
        obs = (g["coverage_state"] == "COMPLETE").tolist()
        vr = VER.verify(rows, observed=obs)
        p = got[got["security_key_v1"] == k].sort_values("bar_start")
        if len(p) != len(g):
            mism["availability"] += abs(len(p) - len(g)); continue
        exp_av, exp_st, exp_bc, exp_rs = [], [], [], []
        for j, r in enumerate(vr):
            if r["state"] == "AVAILABLE":
                exp_av.append("EVALUABLE_T_STATE" if r["bc"] > 0 else "EVALUABLE_NO_T_STATE")
                exp_st.append(r["t_label"] if r["bc"] > 0 else "NONE")
                exp_rs.append("")
            else:
                exp_av.append("UNAVAILABLE_CURRENT_INPUT"
                              if r["reason"] == "CURRENT_BAR_NOT_COMPLETE"
                              else "UNAVAILABLE_REQUIRED_HISTORY")
                exp_st.append("")
                exp_rs.append(r["reason"])
            exp_bc.append(r["bc"])
        av = p["t_availability_state"].to_numpy(); st = p["final_t_state"].to_numpy()
        bc = p["priority_winner_code"].to_numpy()
        mism["compared"] += len(g)
        mism["availability"] += int((av != np.array(exp_av)).sum())
        mism["final_state"] += int((st != np.array(exp_st)).sum())
        mism["priority"] += int((bc != np.array(exp_bc)).sum())
        rs = p["unavailable_reason"].to_numpy()
        mism["reason"] += int((rs != np.array(exp_rs)).sum())
        # session-boundary rows specifically
        first = g["interval_index"].to_numpy() == 0
        mism["boundary_compared"] += int(first.sum())
        mism["boundary_mismatch"] += int((st[first] != np.array(exp_st)[first]).sum())
    return mism


def structural(cohort):
    tot = {a: 0 for a in AVAIL}
    rows = 0
    states, bad_label, zout, wrongkey = {}, 0, 0, 0
    dup_probe, digest_bad = 0, 0
    seen = set()
    for f in parts(OUT):
        day = os.path.basename(f)[:-8]
        d = pq.read_table(f).to_pandas()
        rows += len(d)
        if not set(d["security_key_v1"]).issubset(cohort):
            wrongkey += len(set(d["security_key_v1"]) - cohort)
        if d.duplicated(["security_key_v1", "bar_start"]).any():
            dup_probe += int(d.duplicated(["security_key_v1", "bar_start"]).sum())
        for a in AVAIL:
            tot[a] += int((d["t_availability_state"] == a).sum())
        ev = d["final_t_state"] != ""
        u, c = np.unique(d.loc[ev, "final_t_state"], return_counts=True)
        for k, v in zip(u.tolist(), c.tolist()):
            states[k] = states.get(k, 0) + v
            if k not in VALID:
                bad_label += v
            if k.startswith("Z"):
                zout += v
        mp = os.path.join(PART, f"{day}.json")
        if os.path.exists(mp):
            man = json.load(open(mp))
            if man["digest"] != ART.file_digest(f) or man["rows"] != len(d):
                digest_bad += 1
        else:
            digest_bad += 1
        seen.add(day)
    return dict(rows=rows, availability=tot, states=states, invalid_labels=bad_label,
                z_outputs=zout, keys_outside_cohort=wrongkey, duplicate_logical_keys=dup_probe,
                partitions=len(seen), digest_mismatches=digest_bad,
                availability_sums_to_rows=sum(tot.values()) == rows)


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="T production independent verification (read-only)")
    prod = json.load(open(PROD_ART))
    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    sample = set(sorted(cohort)[::40][:12])
    t0 = time.time()
    print(f"  row-level pass over {len(sample)} securities (full continuous history) …",
          flush=True)
    rl = row_level(sample)
    print(f"    {rl}", flush=True)
    print("  structural pass over every partition …", flush=True)
    st = structural(cohort)
    print(f"    rows {st['rows']:,} · partitions {st['partitions']} · "
          f"digest mismatches {st['digest_mismatches']}", flush=True)

    row_ok = (rl["availability"] == 0 and rl["reason"] == 0 and rl["final_state"] == 0
              and rl["priority"] == 0 and rl["boundary_mismatch"] == 0 and rl["compared"] > 0)
    struct_ok = (st["invalid_labels"] == 0 and st["z_outputs"] == 0
                 and st["keys_outside_cohort"] == 0 and st["duplicate_logical_keys"] == 0
                 and st["digest_mismatches"] == 0 and st["availability_sums_to_rows"]
                 and st["rows"] == prod["total_rows"])
    agree = st["availability"] == prod["availability_decomposition"]
    states_agree = st["states"] == {**prod["per_state_counts"],
                                    "NONE": prod["availability_decomposition"][
                                        "EVALUABLE_NO_T_STATE"]}
    recon = prod["capability_reconciliation"]
    recon_ok = all(recon[k] for k in ("slots_reconcile", "evaluable_exact", "fired_exact",
                                      "required_history_exact",
                                      "unavailable_current_reconciles"))
    ok = row_ok and struct_ok and agree and recon_ok

    p = dict(
        report_id="MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1",
        status="MASSIVE_T_FEATURE_PRODUCTION_V1_PASS" if ok else "HOLD",
        verifies=dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_V1",
                      digest=ART.file_digest(PROD_ART)),
        independence=dict(imports_production_module=False,
                          method="scalar if/elif verifier re-deriving T from raw coarse OHLC "
                                 "over one continuously concatenated per-security series",
                          production_used="vectorised census — a different program"),
        row_level=rl, row_level_pass=row_ok,
        session_boundary=dict(compared=rl["boundary_compared"],
                              mismatch=rl["boundary_mismatch"],
                              why_separate="a session reset provably changes labels, so a "
                                           "partition-boundary reset must not be able to hide "
                                           "inside an overall agreement figure"),
        structural=st, structural_pass=struct_ok,
        availability_agrees_with_production=agree,
        state_counts_agree=states_agree,
        capability_reconciliation_pass=recon_ok,
        rows=st["rows"], y_exposed=0, z_outputs=st["z_outputs"],
        outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json",
                 required=("report_id", "status", "row_level", "structural", "independence"),
                 supersede=os.path.exists("MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json"))
    print(f"\nMASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1 · {d} · {p['status']}")
    print(f"  row-level   {rl['compared']:,} bars · availability {rl['availability']} · "
          f"reason {rl['reason']} · state {rl['final_state']} · priority {rl['priority']}")
    print(f"  boundary    {rl['boundary_compared']:,} first-of-session bars · "
          f"{rl['boundary_mismatch']} mismatch")
    print(f"  structural  rows {st['rows']:,} · invalid {st['invalid_labels']} · Z "
          f"{st['z_outputs']} · outside cohort {st['keys_outside_cohort']} · dup "
          f"{st['duplicate_logical_keys']} · digests {st['digest_mismatches']}")
    print(f"  agreement   availability {agree} · states {states_agree} · recon {recon_ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
