"""RVOL production — slot-RVOL and CUM-RVOL for 476 x 1,254, under the qualified implementation.

Separate file from the engine and the smoke runner, which are what the qualified hash covers.
This file verifies that hash and never contributes to it. No semantic decision is made here:
the ten dictionary parameters are already frozen, and this runner only reads them out.

TWO THINGS ARE DERIVED IN THIS FILE RATHER THAN IN THE ENGINE, both deliberately, because
touching the engine would invalidate the smoke qualification that authorises this run:

`excluded_unobserved_count` is window_used minus sample_count, where window_used is the number
of prior ELIGIBLE sessions actually available to that security (capped at 20). The runner
tracks its own per-security eligible-session counter to get this, which is arithmetic on
already-frozen quantities rather than a new rule.

The validity taxonomy is a RENAMING of the engine's states onto the names the gate asked for —
NUMERATOR_UNOBSERVED becomes UNAVAILABLE_CURRENT_INPUT, INSUFFICIENT_SAMPLES becomes
UNAVAILABLE_INSUFFICIENT_BASELINE, and so on. Both the mapped name and the engine's own reason
are written to every row, so the mapping can be audited rather than trusted.

RESUME NEEDS STATE. Like EMA, the trailing-20 baseline is carried across sessions, so a resume
that merely skipped sealed partitions would rebuild every baseline from an empty ring. The
engine's history is snapshotted with each sealed partition and resume is refused unless the
snapshot's session matches the last sealed partition exactly.
"""
from __future__ import annotations
import glob, hashlib, json, os, shutil, sys, time                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                    # noqa: E402
import t5_artifact as ART                                             # noqa: E402
from rvol_engine_v1 import (RvolV1, RvolHold, LOOKBACK, MIN_SAMPLES,  # noqa: E402
                            NORMAL_MINUTES, EARLY_MINUTES)

DICT_D = "6095852c4a2eeaa5"
CAP_F, CAP_D = "MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1.json", "3113441b3bfb22bf"
BASE_D = "5591685cfa33b62a"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
BASE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_rvol_v1"
PART = os.path.join(OUT, "_partitions")
STATE = os.path.join(OUT, "_state", "latest.json")
SAFETY = 20 * 1024 ** 3

TAXONOMY = {"VALID": "VALID",
            "NUMERATOR_UNOBSERVED": "UNAVAILABLE_CURRENT_INPUT",
            "INSUFFICIENT_SAMPLES": "UNAVAILABLE_INSUFFICIENT_BASELINE",
            "ZERO_BASELINE": "UNAVAILABLE_ZERO_BASELINE",
            "PATH_CONTAMINATED": "CUM_PATH_CONTAMINATED",
            "NOT_ELIGIBLE_SESSION": "INELIGIBLE"}


def impl_hash():
    h = hashlib.sha256()
    for f in ("rvol_engine_v1.py", "rvol_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def save_state(E, ec, day):
    snap = dict(session=day, impl=impl_hash()[:16], elig_count=ec.tolist(), hist={})
    for tf, h in E.hist.items():
        snap["hist"][tf] = dict(ring=h["ring"].tolist(), filled=h["filled"].tolist())
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    np.savez_compressed(STATE + ".npz",
                        **{f"{tf}_{k}": E.hist[tf][k] for tf in E.hist
                           for k in ("vol", "ok", "cum", "cum_ok")})
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snap, f, separators=(",", ":"))
    os.replace(tmp, STATE)


def load_state(E, sealed):
    if not os.path.exists(STATE) or not os.path.exists(STATE + ".npz") or not sealed:
        return None, None
    try:
        snap = json.load(open(STATE))
        z = np.load(STATE + ".npz")
    except Exception:
        return None, None
    if snap.get("impl") != impl_hash()[:16] or snap.get("session") != sealed[-1]:
        return None, None
    for tf in E.hist:
        for k in ("vol", "ok", "cum", "cum_ok"):
            E.hist[tf][k] = z[f"{tf}_{k}"]
        E.hist[tf]["ring"] = np.array(snap["hist"][tf]["ring"])
        E.hist[tf]["filled"] = np.array(snap["hist"][tf]["filled"])
    return snap["session"], np.array(snap["elig_count"])


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="RVOL feature production v1")
    import pyarrow.parquet as pq
    import pandas as pd, exchange_calendars as xc
    t0 = time.time()

    pre = {}
    for f, w in (("MASSIVE_RVOL_DICTIONARY_SPEC_V1.json", DICT_D), (CAP_F, CAP_D),
                 ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json", BASE_D), (ELIG, ELIG_D),
                 ("MASSIVE_1M_STATE_TRANSITION_V1.json", "d73a88d836f36eea")):
        got = ART.file_digest(f)
        pre[f.replace(".json", "")] = dict(cited=w, actual=got, match=got == w)
        if got != w:
            print(f"HOLD — {f} digest {got} != {w}"); return 1
    cap = json.load(open(CAP_F))
    if cap["status"] != "MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1_PASS":
        print("HOLD — smoke/capability did not PASS"); return 1
    ih = impl_hash()
    if ih[:16] != cap["implementation_hash"][:16]:
        print(f"HOLD — implementation hash {ih[:16]} != qualified "
              f"{cap['implementation_hash'][:16]}; a code change requires new smoke "
              f"qualification"); return 1

    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort = sorted(key_of[t] for t in el["cohort"]["eligible_tickers"])
    if len(cohort) != 476:
        print("HOLD — cohort mismatch"); return 1
    cohort_arr = np.array(cohort)
    cal = xc.get_calendar("XNYS")
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(BASE, "_partitions", "*.json")))
    if len(sessions) != 1254:
        print(f"HOLD — {len(sessions)} sessions"); return 1

    os.makedirs(OUT, exist_ok=True)
    E = RvolV1(cohort)
    kidx = E.idx
    elig_count = np.zeros(len(cohort), dtype=int)
    sealed = sorted(os.path.basename(p)[:-5]
                    for p in glob.glob(os.path.join(PART, "*.json")))
    resumed, ec = load_state(E, sealed)
    if sealed and resumed is None:
        print("  resume refused: state snapshot does not match the last sealed partition; "
              "rebuilding from the first session (the trailing-20 baseline cannot be "
              "reconstructed by skipping partitions)")
        shutil.rmtree(PART, ignore_errors=True)
        for d in glob.glob(os.path.join(OUT, "20*")):
            shutil.rmtree(d, ignore_errors=True)
        sealed = []
    if resumed:
        elig_count = ec
        print(f"  resuming after {resumed}")
    start = sessions.index(resumed) + 1 if resumed else 0

    def load_session(day):
        sch = cal.schedule.loc[pd.Timestamp(day)]
        o = int(sch["open"].tz_convert("UTC").value // 10 ** 6)
        c = int(sch["close"].tz_convert("UTC").value // 10 ** 6)
        m = (c - o) // 60000
        ms = pq.read_table(os.path.join(BASE, "minute_states", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "minute_ts",
                                    "observation_state"]).to_pandas()
        bars = pq.read_table(os.path.join(BASE, "observed_regular_bars", day[:4],
                                          day[5:7], f"{day}.parquet"),
                             columns=["security_key_v1", "minute_ts", "v"]).to_pandas()
        ss = pq.read_table(os.path.join(BASE, "security_sessions", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "eligibility_state"]).to_pandas()
        states = np.zeros((len(cohort), m), bool); vols = np.zeros((len(cohort), m))
        si = ms.security_key_v1.map(kidx).to_numpy(); g = ~pd.isna(si)
        sl = ((ms.minute_ts.to_numpy() - o) // 60000).astype(int)
        states[si[g].astype(int), sl[g]] = ms.observation_state.to_numpy()[g] == "OBSERVED"
        bi = bars.security_key_v1.map(kidx).to_numpy(); bg = ~pd.isna(bi)
        bl = ((bars.minute_ts.to_numpy() - o) // 60000).astype(int)
        vols[bi[bg].astype(int), bl[bg]] = bars.v.to_numpy()[bg]
        elig = np.zeros(len(cohort), bool)
        ei = ss.security_key_v1.map(kidx).to_numpy()
        eg = (~pd.isna(ei)) & (ss.eligibility_state.to_numpy() == "REGULAR_WAY_EXPECTED")
        elig[ei[eg].astype(int)] = True
        states &= elig[:, None]; vols[~states] = 0.0
        return o, c, m, states, vols, elig

    def rows_for(day, o, m, fam, state, reason, val, base, cnt, num, window_used, elig):
        n = len(cohort)
        secs = np.repeat(cohort_arr, m)
        slots = np.tile(np.arange(1, m + 1), n)
        ts = o + (slots - 1) * 60000
        st = state.ravel(); rs = reason.ravel()
        excl = np.maximum(window_used[:, None] - cnt, 0).ravel()
        attr = np.where((slots == 1) | (slots == m), "UNRESOLVED", "NOT_APPLICABLE")
        return pd.DataFrame(dict(
            security_key_v1=secs, session_date=day, slot_index=slots, minute_ts=ts,
            feature_information_available_at=ts + 60000,
            session_type=("normal" if m == NORMAL_MINUTES else "early"),
            rvol_family=fam, numerator_volume=num.ravel(),
            baseline_value=np.where(cnt.ravel() >= MIN_SAMPLES, base.ravel(), np.nan),
            rvol_value=val.ravel(), sample_count=cnt.ravel(),
            excluded_unobserved_count=excl,
            rvol_validity_state=[TAXONOMY.get(x, x) if x != "UNAVAILABLE"
                                 else TAXONOMY.get(r, r) for x, r in zip(st, rs)],
            unavailable_reason=rs,
            boundary_print_attribution=attr))

    # ---- capacity probe
    o, c, m, states, vols, elig = load_session(sessions[len(sessions) // 2])
    Ep = RvolV1(cohort)
    rp = Ep.process_session("probe", o, m, states, vols, emit=True, eligible=elig)
    tmpd = os.path.join(OUT, "_probe"); os.makedirs(tmpd, exist_ok=True)
    wu = np.zeros(len(cohort), dtype=int)
    df = pd.concat([rows_for("probe", o, m, "slot-RVOL", *rp["slot"][:5],
                             rp["slot"][5], wu, elig),
                    rows_for("probe", o, m, "CUM-RVOL", *rp["cum"][:5],
                             rp["cum"][5], wu, elig)], ignore_index=True)
    pp = os.path.join(tmpd, "p.parquet")
    df.to_parquet(pp, index=False, compression="zstd")
    pb = os.path.getsize(pp); shutil.rmtree(tmpd)
    est, free = pb * len(sessions), shutil.disk_usage(OUT).free
    capi = dict(probe_bytes=pb, estimated_output_bytes=est, available_bytes_before=free,
                sufficient=free > est + SAFETY)
    print(f"  preflight: probe {pb/1e6:.2f}MB -> est {est/1e9:.2f}GB · free "
          f"{free/1e9:.1f}GB · sufficient {capi['sufficient']}", flush=True)
    if not capi["sufficient"]:
        print("HOLD — insufficient capacity"); return 1

    tot = {"slot-RVOL": 0, "CUM-RVOL": 0}
    vst = {"slot-RVOL": {}, "CUM-RVOL": {}}
    built, t1 = 0, time.time()
    for i in range(start, len(sessions)):
        day = sessions[i]
        o, c, m, states, vols, elig = load_session(day)
        window_used = np.minimum(elig_count, LOOKBACK)
        r = E.process_session(day, o, m, states, vols, emit=True, eligible=elig)
        elig_count += elig.astype(int)
        parts = []
        for fam, key in (("slot-RVOL", "slot"), ("CUM-RVOL", "cum")):
            st, rs, val, base, cnt, num, _ = r[key]
            g = rows_for(day, o, m, fam, st, rs, val, base, cnt, num, window_used, elig)
            parts.append(g); tot[fam] += len(g)
            for k, n in g.rvol_validity_state.value_counts().items():
                vst[fam][k] = vst[fam].get(k, 0) + int(n)
        df = pd.concat(parts, ignore_index=True)
        bad = df[(df.rvol_validity_state == "VALID")
                 & (~np.isfinite(df.rvol_value.to_numpy(float)))]
        if len(bad):
            raise RvolHold(f"GUARD nonfinite_valid: {len(bad)} VALID rows are not finite")
        d = os.path.join(OUT, day[:4], day[5:7]); os.makedirs(d, exist_ok=True)
        p = os.path.join(d, f"{day}.parquet"); tmp = p + ".tmp"
        df.to_parquet(tmp, index=False, compression="zstd")
        dg = sha_file(tmp); os.replace(tmp, p)
        man = dict(session=day, sealed=True, implementation_hash=ih[:16],
                   dictionary_digest=DICT_D, capability_digest=CAP_D,
                   derived_base_digest=BASE_D, eligibility_digest=ELIG_D,
                   cohort_digest=el["cohort"]["cohort_security_key_sha256"][:16],
                   rows=len(df), sha256=dg, bytes=os.path.getsize(p),
                   provenance_level="FILE_LEVEL")
        os.makedirs(PART, exist_ok=True)
        tp = os.path.join(PART, f"{day}.json.tmp")
        with open(tp, "w") as f:
            json.dump(man, f, separators=(",", ":"), sort_keys=True)
        os.replace(tp, os.path.join(PART, f"{day}.json"))
        save_state(E, elig_count, day)
        built += 1
        del df, parts, states, vols, r
        if built % 100 == 0 or i == len(sessions) - 1:
            e = time.time() - t1
            print(f"  {i+1:4d}/{len(sessions)} {day} · built {built} · {e:.0f}s · "
                  f"eta {e/max(built,1)*(len(sessions)-i-1)/60:.0f}m", flush=True)

    st8 = dict(sessions=len(sessions), built=built, rows=tot, validity=vst,
               capacity=capi, preflight=pre, implementation_hash=ih,
               elapsed_sec=round(time.time() - t0, 1))
    with open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
              "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/rvol_prod_state.json",
              "w") as f:
        json.dump(st8, f)
    print(f"\n  RVOL PRODUCTION COMPLETE · built {built} · "
          f"{(time.time()-t0)/60:.1f} min · rows {sum(tot.values()):,}")
    for fam in tot:
        print(f"    {fam:10s} {tot[fam]:>13,d} rows · {json.dumps(vst[fam])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
