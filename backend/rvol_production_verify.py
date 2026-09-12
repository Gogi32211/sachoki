"""Independent RVOL production verifier + MASSIVE_RVOL_FEATURE_PRODUCTION_V1 sealing.

Does not import the RVOL engine. The trailing-20 baseline is rebuilt here with its own ring
buffers so a bug in the engine's window handling cannot be reproduced and then confirmed — the
failure mode a shared implementation guarantees, and the one that already caught the
eligibility defect in the census.

READS ARE COLUMN-SELECTED ON PURPOSE. Production writes 371k rows per session with a 64-char
key column, and reading all of that back 1,254 times is what made the build itself slow. The
structural pass pulls only the columns each check needs, which is the difference between a
verification that finishes and one that costs more than the build.

THE FOUR-WAY RECONCILIATION IS THE STRONGEST CHECK AVAILABLE HERE. The capability census, the
production engine, this verifier's independent recomputation, and the written parquet all
compute the same counts from the same 1m base. The census already caught two real defects by
disagreeing with the derived base by exactly 253,950; agreement across all four now is
meaningful rather than ceremonial.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                    # noqa: E402
import t5_artifact as ART                                             # noqa: E402

OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_rvol_v1"
PART = os.path.join(OUT, "_partitions")
BASE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
DICT_D, CAP_D, BASE_D = "6095852c4a2eeaa5", "3113441b3bfb22bf", "5591685cfa33b62a"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
LOOKBACK, MIN_SAMPLES, NORMAL, EARLY = 20, 15, 390, 210
SAMPLE_SESSIONS = 40


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="RVOL production verification (read-only)")
    import pyarrow.parquet as pq
    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    t0 = time.time()

    for f, w in (("MASSIVE_RVOL_DICTIONARY_SPEC_V1.json", DICT_D),
                 ("MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1.json", CAP_D),
                 ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json", BASE_D), (ELIG, ELIG_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — {f} digest mismatch"); return 1
    cap = json.load(open("MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1.json"))
    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort = sorted(key_of[t] for t in el["cohort"]["eligible_tickers"])
    kidx = {k: i for i, k in enumerate(cohort)}
    n = len(cohort)

    parts = sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(PART, "*.json")))
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(BASE, "_partitions", "*.json")))
    sample_days = set(parts[::max(1, len(parts) // SAMPLE_SESSIONS)][:SAMPLE_SESSIONS])

    # ---------- independent recomputation (own ring buffers) ----------
    hist = {t_: dict(vol=np.zeros((n, m_, LOOKBACK)), ok=np.zeros((n, m_, LOOKBACK), bool),
                     cum=np.zeros((n, m_, LOOKBACK)),
                     cum_ok=np.zeros((n, m_, LOOKBACK), bool),
                     ring=np.zeros(n, int), filled=np.zeros(n, int))
            for t_, m_ in (("normal", NORMAL), ("early", EARLY))}
    ind = {"slot-RVOL": collections.Counter(), "CUM-RVOL": collections.Counter()}
    prod = {"slot-RVOL": collections.Counter(), "CUM-RVOL": collections.Counter()}
    rows_tot = 0
    bad_digest = nonfinite = tax_bad = zero_finite = sample_mismatch = 0
    sample_compared = 0
    problems = []

    for i, day in enumerate(parts, 1):
        sch = cal.schedule.loc[pd.Timestamp(day)]
        o = int(sch["open"].tz_convert("UTC").value // 10 ** 6)
        m = (int(sch["close"].tz_convert("UTC").value // 10 ** 6) - o) // 60000
        st_ = "normal" if m == NORMAL else "early"
        H = hist[st_]

        ms = pq.read_table(os.path.join(BASE, "minute_states", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "minute_ts",
                                    "observation_state"]).to_pandas()
        bars = pq.read_table(os.path.join(BASE, "observed_regular_bars", day[:4], day[5:7],
                                          f"{day}.parquet"),
                             columns=["security_key_v1", "minute_ts", "v"]).to_pandas()
        ss = pq.read_table(os.path.join(BASE, "security_sessions", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "eligibility_state"]).to_pandas()
        states = np.zeros((n, m), bool); vols = np.zeros((n, m))
        si = ms.security_key_v1.map(kidx).to_numpy(); g = ~pd.isna(si)
        states[si[g].astype(int), ((ms.minute_ts.to_numpy()[g] - o) // 60000).astype(int)] \
            = ms.observation_state.to_numpy()[g] == "OBSERVED"
        bi = bars.security_key_v1.map(kidx).to_numpy(); bg = ~pd.isna(bi)
        vols[bi[bg].astype(int), ((bars.minute_ts.to_numpy()[bg] - o) // 60000).astype(int)] \
            = bars.v.to_numpy()[bg]
        elig = np.zeros(n, bool)
        ei = ss.security_key_v1.map(kidx).to_numpy()
        eg = (~pd.isna(ei)) & (ss.eligibility_state.to_numpy() == "REGULAR_WAY_EXPECTED")
        elig[ei[eg].astype(int)] = True
        states &= elig[:, None]; vols[~states] = 0.0

        cnt = H["ok"].sum(axis=2); tot = np.where(H["ok"], H["vol"], 0.0).sum(axis=2)
        ccnt = H["cum_ok"].sum(axis=2)
        ctot = np.where(H["cum_ok"], H["cum"], 0.0).sum(axis=2)
        enough, cenough = cnt >= MIN_SAMPLES, ccnt >= MIN_SAMPLES
        base = np.divide(tot, np.maximum(cnt, 1), where=enough, out=np.zeros((n, m)))
        cbase = np.divide(ctot, np.maximum(ccnt, 1), where=cenough, out=np.zeros((n, m)))
        cur_cum = np.cumsum(np.where(states, vols, 0.0), axis=1)
        path_ok = np.logical_and.accumulate(states, axis=1)

        s_valid = states & enough & (base > 0)
        s_zero = states & enough & (base == 0)
        s_ins = states & ~enough
        s_nonum = ~states
        c_valid = path_ok & cenough & (cbase > 0)
        c_zero = path_ok & cenough & (cbase == 0)
        c_ins = path_ok & ~cenough
        c_path = ~path_ok
        ne = ~elig
        for nm_, masks in (("slot-RVOL", (("VALID", s_valid),
                                          ("UNAVAILABLE_ZERO_BASELINE", s_zero),
                                          ("UNAVAILABLE_INSUFFICIENT_BASELINE", s_ins),
                                          ("UNAVAILABLE_CURRENT_INPUT", s_nonum))),
                           ("CUM-RVOL", (("VALID", c_valid),
                                         ("UNAVAILABLE_ZERO_BASELINE", c_zero),
                                         ("UNAVAILABLE_INSUFFICIENT_BASELINE", c_ins),
                                         ("CUM_PATH_CONTAMINATED", c_path)))):
            for label, mk in masks:
                ind[nm_][label] += int((mk & ~ne[:, None]).sum())
            ind[nm_]["INELIGIBLE"] += int(ne.sum() * m)

        ei2 = np.nonzero(elig)[0]
        if ei2.size:
            r = H["ring"][ei2]
            H["vol"][ei2, :, r] = np.where(states[ei2], vols[ei2], 0.0)
            H["ok"][ei2, :, r] = states[ei2]
            H["cum"][ei2, :, r] = cur_cum[ei2]
            H["cum_ok"][ei2, :, r] = path_ok[ei2]
            H["ring"][ei2] = (r + 1) % LOOKBACK
            H["filled"][ei2] += 1

        # ---------- structural pass over the written partition ----------
        man = json.load(open(os.path.join(PART, f"{day}.json")))
        p = os.path.join(OUT, day[:4], day[5:7], f"{day}.parquet")
        if not os.path.exists(p):
            problems.append(f"{day} missing"); continue
        if sha_file(p) != man["sha256"]:
            bad_digest += 1; problems.append(f"{day} digest mismatch")
        cols = ["rvol_family", "rvol_validity_state", "rvol_value", "baseline_value",
                "sample_count", "slot_index", "boundary_print_attribution"]
        t = pq.read_table(p, columns=cols).to_pandas()
        rows_tot += len(t)
        for fam, sub in t.groupby("rvol_family", observed=True):
            for k, v in sub.rvol_validity_state.value_counts().items():
                prod[fam][k] += int(v)
            vv = sub[sub.rvol_validity_state == "VALID"].rvol_value.to_numpy(float)
            nonfinite += int((~np.isfinite(vv)).sum())
            zb = sub[sub.rvol_validity_state == "UNAVAILABLE_ZERO_BASELINE"]
            zero_finite += int(np.isfinite(zb.rvol_value.to_numpy(float)).sum())
        b = t[t.slot_index.isin([1, m])].boundary_print_attribution
        if len(b) and set(b.unique()) != {"UNRESOLVED"}:
            tax_bad += 1; problems.append(f"{day}: boundary attribution {set(b.unique())}")
        if day in sample_days:
            sv = t[(t.rvol_family == "slot-RVOL")
                   & (t.rvol_validity_state == "VALID")].rvol_value.to_numpy(float)
            exp = (vols[s_valid] / base[s_valid])
            if len(sv) == len(exp):
                sample_compared += len(sv)
                sample_mismatch += int((np.abs(np.sort(sv) - np.sort(exp)) > 1e-9).sum())
            else:
                sample_mismatch += 1
                problems.append(f"{day}: VALID count {len(sv)} vs recomputed {len(exp)}")
        del t, ms, bars, ss, states, vols
        if i % 200 == 0:
            print(f"  verified {i}/{len(parts)} · {time.time()-t0:.0f}s", flush=True)

    recon = {}
    for fam in ("slot-RVOL", "CUM-RVOL"):
        capk = cap["capability_census"]["slot_rvol" if fam == "slot-RVOL" else "cum_rvol"]
        capv = capk.get("VALID")
        recon[fam] = dict(production_VALID=prod[fam].get("VALID", 0),
                          independent_VALID=ind[fam].get("VALID", 0),
                          capability_VALID=capv,
                          exact=(prod[fam].get("VALID", 0) == ind[fam].get("VALID", 0)
                                 == capv))

    checks = dict(
        partitions_complete=len(parts) == len(sessions) == 1254,
        all_digests_valid=bad_digest == 0,
        no_nonfinite_valid=nonfinite == 0,
        zero_baseline_never_finite=zero_finite == 0,
        boundary_attribution_unresolved=tax_bad == 0,
        four_way_reconciliation_exact=all(r["exact"] for r in recon.values()),
        sample_values_exact=sample_mismatch == 0,
        no_problems=not problems)
    ok = all(checks.values())
    imm = {k: ART.file_digest(k + ".json") == d for k, d in (
        ("MASSIVE_RVOL_DICTIONARY_SPEC_V1", DICT_D),
        ("MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1", CAP_D),
        ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1", BASE_D),
        ("MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1", "36168e975efa742f"),
        ("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1", "6ec73d5f462761f0"),
        ("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1", ELIG_D),
        ("MASSIVE_1M_STATE_TRANSITION_V1", "d73a88d836f36eea"))}
    imm_ok = all(imm.values())
    total_bytes = sum(os.path.getsize(x)
                      for x in glob.glob(os.path.join(OUT, "20*", "*", "*.parquet")))

    p = dict(
        report_id="MASSIVE_RVOL_FEATURE_PRODUCTION_V1",
        status=("MASSIVE_RVOL_FEATURE_PRODUCTION_V1_PASS" if (ok and imm_ok) else "HOLD"),
        binding=dict(dictionary=DICT_D, smoke_capability=CAP_D, derived_base=BASE_D,
                     eligibility=ELIG_D, implementation_hash=cap["implementation_hash"],
                     cohort=476, sessions=len(parts),
                     families=["slot-RVOL", "CUM-RVOL"]),
        production_path=OUT, output_bytes=total_bytes, total_rows=rows_tot,
        validity_counts={k: dict(v) for k, v in prod.items()},
        reconciliation=dict(
            method="four paths — capability census, production engine, this verifier's "
                   "independent ring-buffer recomputation, and the written parquet",
            per_family=recon, all_exact=all(r["exact"] for r in recon.values()),
            why_meaningful="the census already caught two real defects by disagreeing with "
                           "the derived base by exactly 253,950; agreement here is not "
                           "ceremonial"),
        finite_value_checks=dict(nonfinite_valid=nonfinite,
                                 zero_baseline_with_finite_value=zero_finite,
                                 epsilon_or_floor_present=False),
        boundary_slots=dict(attribution="UNRESOLVED on first and last regular slots",
                            violations=tax_bad, auction_claim_made=False),
        sample_comparison=dict(sessions=len(sample_days), values=sample_compared,
                               mismatches=sample_mismatch, tolerance="1e-9"),
        checks=checks, failed=[k for k, v in checks.items() if not v],
        problems=problems[:20], problem_count=len(problems),
        immutability=dict(artifacts=imm, all_unchanged=imm_ok, y_exposure=0),
        provenance_level="FILE_LEVEL", y_exposed=0,
        authorises="nothing beyond itself. T/Z and Y exposure remain HOLD.",
        closes="the V1 X feature-construction layer, together with the 1m base, the coarse "
               "layer and EMA production",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_RVOL_FEATURE_PRODUCTION_V1.json",
                 required=("report_id", "status", "binding", "validity_counts",
                           "reconciliation", "finite_value_checks", "checks",
                           "immutability"),
                 supersede=os.path.exists("MASSIVE_RVOL_FEATURE_PRODUCTION_V1.json"))
    print(f"\nMASSIVE_RVOL_FEATURE_PRODUCTION_V1 · {d} · {p['status']}")
    print(f"  rows {rows_tot:,} · {total_bytes/1e9:.2f} GB · {len(parts)} partitions")
    for fam in recon:
        print(f"  {fam:10s} VALID prod {recon[fam]['production_VALID']:,} · "
              f"indep {recon[fam]['independent_VALID']:,} · cap "
              f"{recon[fam]['capability_VALID']:,} · exact {recon[fam]['exact']}")
    print(f"  checks {sum(1 for v in checks.values() if v)}/{len(checks)} · "
          f"immutability {'OK' if imm_ok else 'CHANGED'}")
    if p["failed"]:
        print(f"  FAILED: {p['failed']}")
    return 0 if (ok and imm_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
