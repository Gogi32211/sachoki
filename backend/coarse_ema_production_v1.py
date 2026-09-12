"""Coarse EMA production — 12 streams over 476 x 1,254, under implementation from bd57b0e1cb959d7d.

Separate file from the engine and the smoke runner, which are what the qualified hash covers.
This file verifies that hash and never contributes to it.

RESUME NEEDS STATE, NOT JUST PARTITIONS. Every other production gate in this programme could
resume by checking which partitions were sealed, because each session was independent. EMA is
not: an EMA200 on 1D reaches VALID only by carrying age across 200 consecutive sessions, so a
resume that merely skipped sealed partitions would restart every segment from zero and quietly
produce a different — wrong — result that still looked complete.

So the engine's whole state is snapshotted atomically alongside each sealed partition, and
resume is refused unless the snapshot's session matches the last sealed partition exactly. If
they disagree the build starts over from the first session. Restarting is cheap; a silently
mis-seeded 81M-row feature layer is not.
"""
from __future__ import annotations
import glob, hashlib, json, os, shutil, sys, time                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
from coarse_ema_v1 import (CoarseEmaV1, EmaHold, REGISTERED_PERIODS,  # noqa: E402
                           REGISTERED_TFS)

SPEC_D = "ef01b67c22f6770f"
CAP_D = "bd57b0e1cb959d7d"
COARSE_D = "6ec73d5f462761f0"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_ema_v1"
PART = os.path.join(OUT, "_partitions")
STATE = os.path.join(OUT, "_state", "latest.json")
SAFETY = 10 * 1024 ** 3


def impl_hash():
    h = hashlib.sha256()
    for f in ("coarse_ema_v1.py", "coarse_ema_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def read_coarse(tf, day):
    import pyarrow.parquet as pq
    p = os.path.join(COARSE, tf, day[:4], day[5:7], f"{day}.parquet")
    return pq.read_table(p).to_pandas() if os.path.exists(p) else None


def save_state(engines, day):
    snap = dict(session=day, impl=impl_hash()[:16], engines={})
    for tf, e in engines.items():
        snap["engines"][tf] = dict(
            state={f"{k[0]}|{k[2]}": v for k, v in e.state.items()},
            seg={k[0]: v for k, v in e.seg.items()},
            seg_len={k[0]: v for k, v in e.seg_len.items()},
            next_seg=e.next_seg)
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snap, f, separators=(",", ":"))
    os.replace(tmp, STATE)


def load_state(engines, sealed):
    """Resume only if the snapshot matches the last sealed partition exactly."""
    if not os.path.exists(STATE) or not sealed:
        return None
    try:
        snap = json.load(open(STATE))
    except Exception:
        return None
    if snap.get("impl") != impl_hash()[:16] or snap.get("session") != sealed[-1]:
        return None
    for tf, e in engines.items():
        s = snap["engines"].get(tf)
        if not s:
            return None
        e.state = {(k.split("|")[0], tf, int(k.split("|")[1])): v
                   for k, v in s["state"].items()}
        e.seg = {(k, tf): v for k, v in s["seg"].items()}
        e.seg_len = {(k, tf): v for k, v in s["seg_len"].items()}
        e.next_seg = s["next_seg"]
    return snap["session"]


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse EMA feature production v1")
    t0 = time.time()
    pre = {}
    for f, want in (("MASSIVE_COARSE_EMA_FEATURE_SPEC_V1.json", SPEC_D),
                    ("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json", CAP_D),
                    ("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json", COARSE_D),
                    (ELIG, ELIG_D)):
        got = ART.file_digest(f)
        pre[f.replace(".json", "")] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            print(f"HOLD — {f} digest {got} != {want}"); return 1
    cap = json.load(open("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json"))
    if cap["status"] != "MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1_PASS":
        print("HOLD — smoke/capability did not PASS"); return 1
    ih = impl_hash()
    if ih[:16] != cap["implementation_hash"][:16]:
        print(f"HOLD — implementation hash {ih[:16]} != qualified "
              f"{cap['implementation_hash'][:16]}"); return 1

    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    if len(cohort_keys) != 476:
        print("HOLD — cohort mismatch"); return 1

    import pandas as pd
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(COARSE, "_partitions", "*.json")))
    if len(sessions) != 1254:
        print(f"HOLD — {len(sessions)} coarse sessions, expected 1254"); return 1

    os.makedirs(OUT, exist_ok=True)
    engines = {tf: CoarseEmaV1(cohort_keys) for tf in REGISTERED_TFS}
    sealed = sorted(os.path.basename(p)[:-5]
                    for p in glob.glob(os.path.join(PART, "*.json")))
    resumed = load_state(engines, sealed)
    if sealed and resumed is None:
        print(f"  resume refused: state snapshot does not match the last sealed partition; "
              f"rebuilding from the first session (EMA state cannot be reconstructed by "
              f"skipping partitions)")
        shutil.rmtree(PART, ignore_errors=True)
        for tf in REGISTERED_TFS:
            shutil.rmtree(os.path.join(OUT, tf), ignore_errors=True)
        sealed = []
    start_at = sessions.index(resumed) + 1 if resumed else 0
    if resumed:
        print(f"  resuming after {resumed} ({start_at}/{len(sessions)} done)")

    # capacity probe on a real session
    probe = sessions[len(sessions) // 2]
    pb = 0
    tmpd = os.path.join(OUT, "_probe"); os.makedirs(tmpd, exist_ok=True)
    E = {tf: CoarseEmaV1(cohort_keys) for tf in REGISTERED_TFS}
    for tf in REGISTERED_TFS:
        d = read_coarse(tf, probe)
        rows = E[tf].update_session(tf, d, emit=True)
        pp = os.path.join(tmpd, f"{tf}.parquet")
        pd.DataFrame(rows).to_parquet(pp, index=False, compression="zstd")
        pb += os.path.getsize(pp)
    shutil.rmtree(tmpd)
    est, free = pb * len(sessions), shutil.disk_usage(OUT).free
    capinfo = dict(probe_session=probe, probe_bytes=pb, estimated_output_bytes=est,
                   available_bytes_before=free, sufficient=free > est + SAFETY)
    print(f"  preflight: probe {pb/1e6:.2f}MB -> est {est/1e9:.2f}GB · free "
          f"{free/1e9:.1f}GB · sufficient {capinfo['sufficient']}", flush=True)
    if not capinfo["sufficient"]:
        print("HOLD — insufficient capacity"); return 1

    rows_tot = {tf: 0 for tf in REGISTERED_TFS}
    vstate = {(tf, p): {"VALID": 0, "INITIALIZATION_SENSITIVE": 0, "UNAVAILABLE": 0}
              for tf in REGISTERED_TFS for p in REGISTERED_PERIODS}
    anyvalid = {(tf, p): set() for tf in REGISTERED_TFS for p in REGISTERED_PERIODS}
    t1, built = time.time(), 0
    for i in range(start_at, len(sessions)):
        day = sessions[i]
        files = {}
        for tf in REGISTERED_TFS:
            d = read_coarse(tf, day)
            if d is None:
                raise EmaHold(f"GUARD missing_coarse: {tf} {day}")
            out = engines[tf].update_session(tf, d, emit=True)
            g = pd.DataFrame(out)
            dd = os.path.join(OUT, tf, day[:4], day[5:7]); os.makedirs(dd, exist_ok=True)
            p = os.path.join(dd, f"{day}.parquet"); tmp = p + ".tmp"
            g.to_parquet(tmp, index=False, compression="zstd")
            dg = sha_file(tmp); os.replace(tmp, p)
            files[tf] = dict(rows=len(g), sha256=dg, bytes=os.path.getsize(p))
            rows_tot[tf] += len(g)
            for per in REGISTERED_PERIODS:
                sub = g[g.ema_period == per]
                vc = sub.ema_validity_state.value_counts().to_dict()
                for k in vstate[(tf, per)]:
                    vstate[(tf, per)][k] += int(vc.get(k, 0))
                anyvalid[(tf, per)].update(
                    sub[sub.ema_validity_state == "VALID"].security_key_v1.unique())
        man = dict(session=day, sealed=True, implementation_hash=ih[:16],
                   spec_digest=SPEC_D, capability_digest=CAP_D,
                   coarse_digest=COARSE_D, eligibility_digest=ELIG_D,
                   cohort_digest=el["cohort"]["cohort_security_key_sha256"][:16],
                   periods=list(REGISTERED_PERIODS), timeframes=list(REGISTERED_TFS),
                   provenance_level="FILE_LEVEL", files=files)
        os.makedirs(PART, exist_ok=True)
        tmp = os.path.join(PART, f"{day}.json.tmp")
        with open(tmp, "w") as f:
            json.dump(man, f, separators=(",", ":"), sort_keys=True)
        os.replace(tmp, os.path.join(PART, f"{day}.json"))
        save_state(engines, day)
        built += 1
        if built % 100 == 0 or i == len(sessions) - 1:
            e = time.time() - t1
            print(f"  {i+1:4d}/{len(sessions)} {day} · built {built} · {e:.0f}s · "
                  f"eta {e/max(built,1)*(len(sessions)-i-1)/60:.0f}m", flush=True)

    state = dict(sessions=len(sessions), built=built, resumed_after=resumed,
                 rows=rows_tot, validity={f"EMA{p}@{tf}": vstate[(tf, p)]
                                          for tf in REGISTERED_TFS
                                          for p in REGISTERED_PERIODS},
                 securities_with_any_valid={f"EMA{p}@{tf}": len(anyvalid[(tf, p)])
                                            for tf in REGISTERED_TFS
                                            for p in REGISTERED_PERIODS},
                 capacity=capinfo, preflight=pre, implementation_hash=ih,
                 elapsed_sec=round(time.time() - t0, 1))
    with open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
              "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/ema_prod_state.json",
              "w") as f:
        json.dump(state, f)
    print(f"\n  EMA PRODUCTION COMPLETE · built {built} · "
          f"{(time.time()-t0)/60:.1f} min · rows {sum(rows_tot.values()):,}")
    for tf in REGISTERED_TFS:
        for per in REGISTERED_PERIODS:
            v = vstate[(tf, per)]
            print(f"    EMA{per:<3d}@{tf:<4s} VALID {v['VALID']:>11,d} · securities "
                  f"{len(anyvalid[(tf, per)]):>3d}/476")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
