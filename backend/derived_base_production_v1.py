"""MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1 — 1,254 sessions x 476 securities, under one frozen hash.

Executes the smoke-authorised builder unchanged. The production runner is a SEPARATE file on
purpose: the authorised implementation hash covers derived_base_builder_v2.py and
derived_base_smoke_v2.py, so an orchestrator that participated in that hash could not be
written without invalidating the authorisation it depends on. This file verifies the hash and
never contributes to it.

RESUME IS EARNED, NOT ASSUMED. A partition counts as complete only if its manifest exists AND
every file it names is present AND every digest still matches. Anything else is rebuilt from
scratch. A half-written parquet from a killed process must never be mistaken for finished work,
so files are written to .tmp and moved into place with os.replace after the digest is taken.

EXPECT A LARGE UNOBSERVED COUNT AND DO NOT TREAT IT AS A DEFECT. 330 of 476 securities lack
complete emitted minutes on an ordinary session; ERIE carried 42 of 390. Production is
therefore expected to emit tens of millions of UNOBSERVED minute states. That number is the
measurement working correctly — it is the count of minutes the vendor did not emit and that we
declined to invent. The PASS condition is that every missing minute is classified honestly,
never that the count is small.
"""
from __future__ import annotations
import hashlib, json, os, shutil, sys, time                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402
from derived_base_builder_v2 import (DerivedBaseBuilderV2, BuildHold,  # noqa: E402
                                     CANON_ROOT)

SPEC, SPEC_D = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.json", "3af8cc42370fda86"
AM1, AM1_D = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1.json", "317759f7fc843ccd"
SMOKE, SMOKE_D = "MASSIVE_1M_DERIVED_BASE_SMOKE_V2.json", "bce44a9000ca3a5f"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
COHORT_D, EXCL_D = "1f86d09a76d3c6e9", "3604f7c1dbf902a1"
IMPL_D = "3a8236bf56cbe9c0"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
CM = os.path.join(CANON_ROOT, "_manifest")
PROD = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
PART = os.path.join(PROD, "_partitions")
DATASETS = ("security_sessions", "minute_states", "observed_regular_bars",
            "session_close_boundary_bars", "extended_hours_bars")
SAFETY_MARGIN = 20 * 1024 ** 3          # 20 GiB


def impl_hash():
    h = hashlib.sha256()
    for f in ("derived_base_builder_v2.py", "derived_base_smoke_v2.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def partition_complete(day):
    mp = os.path.join(PART, f"{day}.json")
    if not os.path.exists(mp):
        return False
    try:
        m = json.load(open(mp))
    except Exception:
        return False
    for ds, info in m.get("files", {}).items():
        p = os.path.join(PROD, ds, day[:4], day[5:7], f"{day}.parquet")
        if info["rows"] == 0:
            if os.path.exists(p):
                return False
            continue
        if not os.path.exists(p) or sha_file(p) != info["sha256"]:
            return False
    return m.get("sealed") is True and m.get("implementation_hash") == IMPL_D


def write_partition(day, out, src_sha, prov):
    import pandas as pd
    files = {}
    for ds in DATASETS:
        rows = out[ds]
        d = os.path.join(PROD, ds, day[:4], day[5:7])
        p = os.path.join(d, f"{day}.parquet")
        if not rows:
            files[ds] = dict(rows=0, sha256=None, bytes=0)
            if os.path.exists(p):
                os.remove(p)
            continue
        os.makedirs(d, exist_ok=True)
        tmp = p + ".tmp"
        pd.DataFrame(rows).to_parquet(tmp, index=False, compression="zstd")
        dg = sha_file(tmp)
        os.replace(tmp, p)
        files[ds] = dict(rows=len(rows), sha256=dg, bytes=os.path.getsize(p))
    man = dict(session=day, sealed=True, implementation_hash=IMPL_D,
               builder_spec_digest=SPEC_D, amendment_digest=AM1_D,
               smoke_digest=SMOKE_D, eligibility_digest=ELIG_D,
               cohort_digest=COHORT_D,
               canonical_source_gz_sha256=src_sha,
               calendar_authority="US_EQUITY_SESSION_CALENDAR_V1",
               scoped_lineage_authority="MASSIVE_TICKER_LINEAGE_V6",
               provenance_level="FILE_LEVEL",
               row_level_raw_byte_provenance_claimed=False,
               files=files, **prov)
    os.makedirs(PART, exist_ok=True)
    tmp = os.path.join(PART, f"{day}.json.tmp")
    with open(tmp, "w") as f:
        json.dump(man, f, separators=(",", ":"), sort_keys=True)
    os.replace(tmp, os.path.join(PART, f"{day}.json"))
    return man


def validate_partition(out, cohort_keys):
    """Per-partition invariants. Any failure means the partition is not sealed."""
    ok = dict(excluded_securities=0, supplemental_provenance=0, duplicate_regular_keys=0,
              duplicate_extended_keys=0, duplicate_close_boundaries=0,
              structural_no_trade=0, synthetic_rows=0, scope_collisions=0,
              missing_identity=0, unknown_identity=0)
    reg = set()
    for r in out["observed_regular_bars"]:
        k = (r["security_key_v1"], r["minute_ts"])
        if k in reg:
            ok["duplicate_regular_keys"] += 1
        reg.add(k)
        if not r.get("security_key_v1"):
            ok["missing_identity"] += 1
        if r["security_key_v1"] not in cohort_keys:
            ok["excluded_securities"] += 1
    ext = set()
    for r in out["extended_hours_bars"]:
        k = (r["security_key_v1"], r["minute_ts"])
        if k in ext:
            ok["duplicate_extended_keys"] += 1
        ext.add(k)
        if k in reg:
            ok["scope_collisions"] += 1
    cb = {}
    for r in out["session_close_boundary_bars"]:
        k = (r["security_key_v1"], r["session_date"])
        cb[k] = cb.get(k, 0) + 1
        if cb[k] > 1:
            ok["duplicate_close_boundaries"] += 1
        if (r["security_key_v1"], r["minute_ts"]) in reg:
            ok["scope_collisions"] += 1
    for m in out["minute_states"]:
        if m["observation_state"] == "STRUCTURAL_NO_TRADE":
            ok["structural_no_trade"] += 1
        if {"o", "h", "l", "c", "v"} & set(m):
            ok["synthetic_rows"] += 1
        if m["security_key_v1"] not in cohort_keys:
            ok["unknown_identity"] += 1
    bad = {k: v for k, v in ok.items() if v}
    if bad:
        raise BuildHold(f"GUARD partition_invariant: {bad}")
    return ok


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="derived base production v1")
    t0 = time.time()

    # ---------------- pre-flight ----------------
    pre = {}
    for f, want in ((SPEC, SPEC_D), (AM1, AM1_D), (SMOKE, SMOKE_D), (ELIG, ELIG_D)):
        got = ART.file_digest(f)
        pre[f.replace(".json", "")] = dict(cited=want, actual=got, match=got == want)
        if got != want:
            print(f"HOLD — {f} digest {got} != {want}"); return 1
    ih = impl_hash()
    if ih[:16] != IMPL_D:
        print(f"HOLD — implementation hash {ih[:16]} != authorised {IMPL_D}. "
              f"A code change requires a new full smoke first."); return 1
    el = json.load(open(ELIG))
    if (el["cohort"]["eligible_count"] != 476
            or el["cohort"]["cohort_security_key_sha256"][:16] != COHORT_D
            or el["cohort"]["exclusion_list_sha256"][:16] != EXCL_D
            or len(el["cohort"]["exclusion_list"]) != 27):
        print("HOLD — cohort binding mismatch"); return 1
    import glob
    sess_files = sorted(glob.glob(os.path.join(CM, "*.json")))
    if len(sess_files) != 1254:
        print(f"HOLD — {len(sess_files)} canonical session manifests, expected 1254")
        return 1
    sessions = [os.path.basename(p)[:-5] for p in sess_files]

    ids = json.load(open(IDENTITY))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    cohort_t = sorted(el["cohort"]["eligible_tickers"])
    cohort_keys = {key_of[t] for t in cohort_t}

    import exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    B = DerivedBaseBuilderV2(cohort_keys, key_of, lin, SPEC_D, ELIG_D, COHORT_D, cal)

    # capacity: measure one real session, then extrapolate
    os.makedirs(PROD, exist_ok=True)
    probe_day = sessions[len(sessions) // 2]
    out = B.build_session(probe_day, cohort_t)
    B.validate(out); validate_partition(out, cohort_keys)
    import pandas as pd
    probe_bytes = 0
    tmpd = os.path.join(PROD, "_probe")
    os.makedirs(tmpd, exist_ok=True)
    for ds in DATASETS:
        if out[ds]:
            p = os.path.join(tmpd, f"{ds}.parquet")
            pd.DataFrame(out[ds]).to_parquet(p, index=False, compression="zstd")
            probe_bytes += os.path.getsize(p)
    shutil.rmtree(tmpd)
    est = probe_bytes * len(sessions)
    free = shutil.disk_usage(PROD).free
    cap = dict(probe_session=probe_day, probe_bytes=probe_bytes,
               estimated_output_bytes=est, available_bytes_before=free,
               required_safety_margin=SAFETY_MARGIN,
               sufficient=free > est + SAFETY_MARGIN)
    print(f"  preflight: probe {probe_day} {probe_bytes/1e6:.1f}MB -> est "
          f"{est/1e9:.1f}GB · free {free/1e9:.1f}GB · sufficient {cap['sufficient']}",
          flush=True)
    if not cap["sufficient"]:
        print("HOLD — insufficient capacity before any production write"); return 1

    # ---------------- build ----------------
    tot = {ds: 0 for ds in DATASETS}
    cov = dict(REGULAR_WAY_EXPECTED=0, NOT_YET_REGULAR_WAY=0, WHEN_ISSUED_EXCLUDED=0,
               expected_regular_minutes=0, observed_regular_minutes=0,
               unobserved_regular_minutes=0, normal_sessions=0, early_close_sessions=0)
    sec_obs, sec_exp, sec_unobs, sess_unobs = {}, {}, {}, 0
    sealed, skipped, t1 = 0, 0, time.time()
    for i, day in enumerate(sessions, 1):
        if partition_complete(day):
            skipped += 1
            continue
        # No payload= here: the builder must load it itself so every row carries the real
        # decompressed-payload digest rather than a fixture marker.
        out = B.build_session(day, cohort_t)
        src_sha = sha_file(os.path.join(CANON_ROOT, day[:4], day[5:7], f"{day}.json.gz"))
        B.validate(out)
        inv = validate_partition(out, cohort_keys)
        _, _, exp = B.lattice(day)
        cov["normal_sessions" if exp == 390 else "early_close_sessions"] += 1
        u_here = 0
        for r in out["security_sessions"]:
            cov[r["eligibility_state"]] += 1
            cov["expected_regular_minutes"] += r["expected_regular_minutes"]
            cov["observed_regular_minutes"] += r["observed_regular_minutes"]
            cov["unobserved_regular_minutes"] += r["unobserved_regular_minutes"]
            k = r["security_key_v1"]
            sec_exp[k] = sec_exp.get(k, 0) + r["expected_regular_minutes"]
            sec_obs[k] = sec_obs.get(k, 0) + r["observed_regular_minutes"]
            if r["unobserved_regular_minutes"]:
                sec_unobs[k] = sec_unobs.get(k, 0) + r["unobserved_regular_minutes"]
                u_here += r["unobserved_regular_minutes"]
        if u_here:
            sess_unobs += 1
        write_partition(day, out, src_sha, dict(
            expected_regular_minutes=exp,
            securities=len(out["security_sessions"]),
            partition_invariants=inv,
            cohort_securities=len(cohort_t)))
        for ds in DATASETS:
            tot[ds] += len(out[ds])
        sealed += 1
        del out
        if i % 50 == 0 or i == len(sessions):
            el_s = time.time() - t1
            print(f"  {i:4d}/{len(sessions)} {day} · sealed {sealed} skipped {skipped} · "
                  f"{el_s:.0f}s · eta {el_s/max(sealed,1)*(len(sessions)-i)/60:.0f}m",
                  flush=True)

    state = dict(sessions=len(sessions), sealed=sealed, skipped_already_complete=skipped,
                 dataset_rows=tot, coverage=cov,
                 securities_with_any_unobserved=len(sec_unobs),
                 sessions_with_any_unobserved=sess_unobs,
                 per_security_ratio={k: round(sec_obs[k] / sec_exp[k], 4)
                                     for k in sec_exp if sec_exp[k]},
                 capacity=cap, preflight=pre, implementation_hash=ih,
                 elapsed_sec=round(time.time() - t0, 1))
    with open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
              "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/prod_state.json", "w") as f:
        json.dump(state, f)
    print(f"\n  BUILD COMPLETE · sealed {sealed} · skipped {skipped} · "
          f"{(time.time()-t0)/60:.1f} min")
    for ds in DATASETS:
        print(f"    {ds:32s} {tot[ds]:>14,d} rows")
    print(f"  expected {cov['expected_regular_minutes']:,} · observed "
          f"{cov['observed_regular_minutes']:,} · unobserved "
          f"{cov['unobserved_regular_minutes']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
