"""Coarse production build — 15m / 1H / 1D for 476 x 1,254, under implementation ec63aeb5a40de812.

Separate file from the aggregator and the smoke runner on purpose: those two are what the
qualified implementation hash covers, so an orchestrator that joined that hash could not exist
without invalidating the qualification it depends on. This file verifies the hash and never
contributes to it.

STRICT SEMANTICS ARE INHERITED, NOT RE-EXPRESSED. There is no tolerance parameter here because
there is none in the aggregator either. One UNOBSERVED constituent minute makes an interval
UNOBSERVED_CONTAMINATED and a contaminated interval carries no OHLCV. The capability census
showed 1D COMPLETE at 36.2% under exactly this rule; production must reproduce that number, not
improve on it.

THE CAPABILITY CENSUS IS A CROSS-CHECK, NEVER A TARGET. Production counts are recomputed from
the derived input and compared against da30cf75b791a6cb afterwards. Both runs measure the same
definition over the same population, so agreement should be exact — and if it is not, the
correct response is to investigate, never to adjust output until it matches a prior number.
"""
from __future__ import annotations
import glob, hashlib, json, os, shutil, sys, time                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
from coarse_aggregator_v1 import (CoarseAggregatorV1, AggHold,        # noqa: E402
                                  REGISTERED_TF, DERIVED_ROOT)

SPEC_D = "51540a1cefe2ec2b"
CAP_D = "da30cf75b791a6cb"
IMPL_D = "ec63aeb5a40de812"
PROD_1M_D = "5591685cfa33b62a"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
PART = os.path.join(OUT, "_partitions")
TFS = ["15m", "1H", "1D"]
SAFETY = 10 * 1024 ** 3
COLS = ["security_key_v1", "timeframe", "session_date", "interval_index", "bar_start",
        "bar_end", "interval_minutes", "is_stub", "feature_information_available_at",
        "expected", "observed", "unobserved", "coverage_state",
        "open", "high", "low", "close", "volume"]


def impl_hash():
    h = hashlib.sha256()
    for f in ("coarse_aggregator_v1.py", "coarse_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def ppath(tf, day):
    return os.path.join(OUT, tf, day[:4], day[5:7], f"{day}.parquet")


def complete(day):
    mp = os.path.join(PART, f"{day}.json")
    if not os.path.exists(mp):
        return False
    try:
        m = json.load(open(mp))
    except Exception:
        return False
    if m.get("implementation_hash") != IMPL_D or not m.get("sealed"):
        return False
    for tf, info in m.get("files", {}).items():
        p = ppath(tf, day)
        if not os.path.exists(p) or sha_file(p) != info["sha256"]:
            return False
    return True


def load(day):
    import pyarrow.parquet as pq
    ms = pq.read_table(os.path.join(DERIVED_ROOT, "minute_states", day[:4], day[5:7],
                                    f"{day}.parquet"),
                       columns=["security_key_v1", "session_date", "minute_ts",
                                "observation_state"]).to_pandas()
    bp = os.path.join(DERIVED_ROOT, "observed_regular_bars", day[:4], day[5:7],
                      f"{day}.parquet")
    b = pq.read_table(bp, columns=["security_key_v1", "minute_ts", "o", "h", "l", "c",
                                   "v"]).to_pandas() if os.path.exists(bp) else None
    return ms, b


def validate_partition(frames, cohort_keys):
    import pandas as pd
    g = pd.concat(frames, ignore_index=True)
    bad = dict(
        invalid_tf=int((~g.timeframe.isin(TFS)).sum()),
        unknown_security=int((~g.security_key_v1.isin(cohort_keys)).sum()),
        cross_session=int((g.session_date != g.session_date.iloc[0]).sum()),
        invalid_geometry=int(((g.bar_start >= g.bar_end)
                              | (g.interval_minutes <= 0)).sum()),
        duplicate_coarse_key=int(g.duplicated(
            subset=["security_key_v1", "timeframe", "bar_start"]).sum()),
        contaminated_marked_complete=int(((g.coverage_state == "COMPLETE")
                                          & (g.unobserved > 0)).sum()),
        structural_no_trade=int((g.coverage_state == "STRUCTURAL_NO_TRADE").sum()),
        contaminated_with_ohlcv=int(((g.coverage_state == "UNOBSERVED_CONTAMINATED")
                                     & g["close"].notna()).sum()),
        premature_availability=int(
            (g.feature_information_available_at != g.bar_end).sum()),
        invalid_stub=int(((g.is_stub) & (g.timeframe == "15m")).sum()))
    fail = {k: v for k, v in bad.items() if v}
    if fail:
        raise AggHold(f"GUARD partition_invariant: {fail}")
    return bad


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse aggregation production v1")
    t0 = time.time()
    pre = {}
    for f, want in (("MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1.json", SPEC_D),
                    ("MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json", CAP_D),
                    ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json", PROD_1M_D),
                    (ELIG, ELIG_D)):
        got = ART.file_digest(f)
        pre[f.replace(".json", "")] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            print(f"HOLD — {f} digest {got} != {want}"); return 1
    ih = impl_hash()
    if ih[:16] != IMPL_D:
        print(f"HOLD — implementation hash {ih[:16]} != qualified {IMPL_D}; a code change "
              f"requires new smoke/capability qualification"); return 1
    cap = json.load(open("MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json"))
    if cap["status"] != "MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1_PASS":
        print("HOLD — capability gate did not PASS"); return 1

    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    if len(cohort_keys) != 476 or el["cohort"]["cohort_security_key_sha256"][:16] != \
            "1f86d09a76d3c6e9":
        print("HOLD — cohort binding mismatch"); return 1

    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    A = CoarseAggregatorV1(cal, cohort_keys)
    sessions = sorted(os.path.basename(p)[:-5] for p in
                      glob.glob(os.path.join(DERIVED_ROOT, "_partitions", "*.json")))
    if len(sessions) != 1254:
        print(f"HOLD — {len(sessions)} sessions, expected 1254"); return 1

    os.makedirs(OUT, exist_ok=True)
    probe = sessions[len(sessions) // 2]
    ms, b = load(probe)
    pb = 0
    tmpd = os.path.join(OUT, "_probe"); os.makedirs(tmpd, exist_ok=True)
    for tf in TFS:
        g = A.aggregate(probe, ms, b, tf); A.validate(g)
        g = g.rename(columns={"_iv": "interval_index"})[COLS]
        pp = os.path.join(tmpd, f"{tf}.parquet")
        g.to_parquet(pp, index=False, compression="zstd"); pb += os.path.getsize(pp)
    shutil.rmtree(tmpd)
    est, free = pb * len(sessions), shutil.disk_usage(OUT).free
    capinfo = dict(probe_session=probe, probe_bytes=pb, estimated_output_bytes=est,
                   available_bytes_before=free, required_safety_margin=SAFETY,
                   sufficient=free > est + SAFETY)
    print(f"  preflight: probe {pb/1e6:.2f}MB -> est {est/1e9:.2f}GB · free "
          f"{free/1e9:.1f}GB · sufficient {capinfo['sufficient']}", flush=True)
    if not capinfo["sufficient"]:
        print("HOLD — insufficient capacity"); return 1

    rows = {tf: 0 for tf in TFS}
    cov = {tf: dict(COMPLETE=0, UNOBSERVED_CONTAMINATED=0) for tf in TFS}
    stub = {tf: dict(complete=0, contaminated=0) for tf in TFS}
    sealed = skipped = 0
    t1 = time.time()
    for i, day in enumerate(sessions, 1):
        if complete(day):
            skipped += 1
            m = json.load(open(os.path.join(PART, f"{day}.json")))
            for tf, info in m["files"].items():
                rows[tf] += info["rows"]
                for k, v in info["coverage"].items():
                    cov[tf][k] += v
                stub[tf]["complete"] += info["stub"]["complete"]
                stub[tf]["contaminated"] += info["stub"]["contaminated"]
            continue
        ms, b = load(day)
        frames, files = [], {}
        for tf in TFS:
            g = A.aggregate(day, ms, b, tf)
            A.validate(g)
            g = g.rename(columns={"_iv": "interval_index"})[COLS]
            frames.append(g)
        validate_partition(frames, cohort_keys)
        for tf, g in zip(TFS, frames):
            d = os.path.join(OUT, tf, day[:4], day[5:7]); os.makedirs(d, exist_ok=True)
            p = os.path.join(d, f"{day}.parquet"); tmp = p + ".tmp"
            g.to_parquet(tmp, index=False, compression="zstd")
            dg = sha_file(tmp); os.replace(tmp, p)
            vc = g.coverage_state.value_counts().to_dict()
            sc = g[g.is_stub].coverage_state.value_counts().to_dict()
            files[tf] = dict(rows=len(g), sha256=dg, bytes=os.path.getsize(p),
                             coverage={k: int(vc.get(k, 0))
                                       for k in ("COMPLETE",
                                                 "UNOBSERVED_CONTAMINATED")},
                             stub=dict(complete=int(sc.get("COMPLETE", 0)),
                                       contaminated=int(
                                           sc.get("UNOBSERVED_CONTAMINATED", 0))))
            rows[tf] += len(g)
            for k in cov[tf]:
                cov[tf][k] += int(vc.get(k, 0))
            stub[tf]["complete"] += files[tf]["stub"]["complete"]
            stub[tf]["contaminated"] += files[tf]["stub"]["contaminated"]
        man = dict(session=day, sealed=True, implementation_hash=IMPL_D,
                   spec_digest=SPEC_D, capability_digest=CAP_D,
                   derived_base_digest=PROD_1M_D, eligibility_digest=ELIG_D,
                   cohort_digest="1f86d09a76d3c6e9", timeframes=TFS,
                   provenance_level="FILE_LEVEL", files=files)
        os.makedirs(PART, exist_ok=True)
        tmp = os.path.join(PART, f"{day}.json.tmp")
        with open(tmp, "w") as f:
            json.dump(man, f, separators=(",", ":"), sort_keys=True)
        os.replace(tmp, os.path.join(PART, f"{day}.json"))
        sealed += 1
        del ms, b, frames
        if i % 100 == 0 or i == len(sessions):
            e = time.time() - t1
            print(f"  {i:4d}/{len(sessions)} {day} · sealed {sealed} skipped {skipped} · "
                  f"{e:.0f}s · eta {e/max(sealed,1)*(len(sessions)-i)/60:.0f}m", flush=True)

    state = dict(sessions=len(sessions), sealed=sealed, skipped=skipped, rows=rows,
                 coverage=cov, stub=stub, capacity=capinfo, preflight=pre,
                 implementation_hash=ih, elapsed_sec=round(time.time() - t0, 1))
    with open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
              "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/coarse_prod_state.json",
              "w") as f:
        json.dump(state, f)
    print(f"\n  COARSE BUILD COMPLETE · sealed {sealed} skipped {skipped} · "
          f"{(time.time()-t0)/60:.1f} min")
    for tf in TFS:
        c = cov[tf]; t = c["COMPLETE"] + c["UNOBSERVED_CONTAMINATED"]
        print(f"    {tf:4s} {rows[tf]:>12,d} rows · COMPLETE {c['COMPLETE']:>11,d} "
              f"({c['COMPLETE']/t:.1%}) · CONTAMINATED "
              f"{c['UNOBSERVED_CONTAMINATED']:>11,d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
