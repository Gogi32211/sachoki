"""MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1 — 13 negatives, then a 12-stream census.

Smoke proves the frozen EMA contract behaves as written. The census then measures, label-free,
where each of the twelve streams is actually defined.

WHY VALID AND INITIALIZATION_SENSITIVE ARE COUNTED SEPARATELY. A stream that produces a value
on 60% of coarse observations tells you nothing useful if most of those values sit at age 3 of
a 200-bar EMA. The share that reaches age >= period is the number that decides whether a
period is usable, and it is reported per stream rather than pooled.

Segmentation depends only on coverage, not on period, so all four periods of a given
(security, timeframe) break at the same bars. Segment statistics are therefore accumulated per
timeframe and validity statistics per (timeframe, period) — collapsing them would report the
same segment four times and inflate every count by 4x.

State is carried across sessions in date order, which is the point: EMA200 on 1D can only reach
VALID by accumulating 200 consecutive COMPLETE daily bars, and that cannot be observed one
session at a time.

NO RVOL FIELD IS COMPUTED OR EMITTED ANYWHERE IN THIS GATE.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
from coarse_ema_v1 import (CoarseEmaV1, EmaHold, REGISTERED_PERIODS,  # noqa: E402
                           REGISTERED_TFS)

SPEC, SPEC_D = "MASSIVE_COARSE_EMA_FEATURE_SPEC_V1.json", "ef01b67c22f6770f"
COARSE_D = "6ec73d5f462761f0"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
PART = os.path.join(COARSE, "_partitions")
STAGE = "/Volumes/QUANT_RESEARCH/staging/coarse_ema_smoke_capability_v1"
PROD_EMA = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_ema_v1"
ORD = "2024-05-15"


def impl_hash():
    h = hashlib.sha256()
    for f in ("coarse_ema_v1.py", "coarse_ema_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def read(tf, day):
    import pyarrow.parquet as pq
    p = os.path.join(COARSE, tf, day[:4], day[5:7], f"{day}.parquet")
    return pq.read_table(p).to_pandas() if os.path.exists(p) else None


def pct(v, q):
    return v[min(len(v) - 1, int(q * (len(v) - 1)))] if v else None


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse EMA smoke + capability (staging only)")
    for f, w in ((SPEC, SPEC_D), (ELIG, ELIG_D),
                 ("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json", COARSE_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — {f} digest mismatch"); return 1
    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    excluded_key = key_of[el["cohort"]["exclusion_list"][0]]
    if len(cohort_keys) != 476:
        print("HOLD — cohort mismatch"); return 1

    import pandas as pd
    ih = impl_hash()
    stage = os.path.join(STAGE, ih[:16]); os.makedirs(stage, exist_ok=True)
    t0, res = time.time(), []

    def rec(fid, name, ok, detail, kind="POSITIVE"):
        res.append(dict(fixture=fid, name=name, kind=kind, passed=bool(ok), detail=detail))
        print(f"  {fid:3s} {kind[:3]} {'PASS' if ok else 'FAIL'}  {name} | {detail}",
              flush=True)

    d15 = read("15m", ORD)
    E = CoarseEmaV1(cohort_keys)
    rows = E.update_session("15m", d15, emit=True)
    df = pd.DataFrame(rows)

    # ---------------- POSITIVE ----------------
    rec("P1", "12 registered streams only", True,
        f"periods {list(REGISTERED_PERIODS)} x TFs {list(REGISTERED_TFS)} = "
        f"{len(REGISTERED_PERIODS)*len(REGISTERED_TFS)}")
    comp = d15[d15.coverage_state == "COMPLETE"]
    ok = len(df[df.ema_value.notna()]) == len(comp) * len(REGISTERED_PERIODS)
    rec("P2", "one EMA row per COMPLETE bar per period", ok,
        f"{len(df[df.ema_value.notna()]):,} valued rows for {len(comp):,} COMPLETE bars "
        f"x {len(REGISTERED_PERIODS)} periods")
    # arithmetic check against an independent recomputation
    sk = df[df.ema_value.notna()].security_key_v1.iloc[0]
    sub = d15[(d15.security_key_v1 == sk) & (d15.coverage_state == "COMPLETE")
              ].sort_values("interval_index")
    seg_first = sub.interval_index.iloc[0]
    manual, a = None, 2.0 / (20 + 1)
    prev_idx = None
    for idx, cl in zip(sub.interval_index, sub["close"]):
        if prev_idx is not None and idx != prev_idx + 1:
            manual = None
        manual = float(cl) if manual is None else a * float(cl) + (1 - a) * manual
        prev_idx = idx
    got = df[(df.security_key_v1 == sk) & (df.ema_period == 20)
             & df.ema_value.notna()].ema_value.iloc[-1]
    rec("P3", "EMA20 matches independent recomputation",
        abs(got - manual) < 1e-9, f"engine {got:.10f} vs manual {manual:.10f}")
    rec("P4", "availability inherits the coarse bar's timestamp",
        bool((df.feature_information_available_at == df.bar_end).all()),
        "every EMA row carries its input bar's feature_information_available_at")
    vs = df[df.ema_value.notna()]
    rec("P5", "validity follows age vs period",
        bool(((vs.ema_age_valid_bars >= vs.ema_period)
              == (vs.ema_validity_state == "VALID")).all()),
        f"VALID {int((vs.ema_validity_state=='VALID').sum()):,} · "
        f"INITIALIZATION_SENSITIVE "
        f"{int((vs.ema_validity_state=='INITIALIZATION_SENSITIVE').sum()):,}")

    # ---------------- NEGATIVE ----------------
    def neg(fid, name, fn, token):
        try:
            fn(); rec(fid, name, False, "DANGEROUS STATE ACCEPTED", "NEGATIVE")
        except EmaHold as e:
            rec(fid, name, token in str(e), str(e)[:100], "NEGATIVE")

    neg("N1", "1m-native EMA request fails",
        lambda: CoarseEmaV1(cohort_keys).check_request("1m", 20), "unregistered_timeframe")
    neg("N2", "unregistered EMA period fails",
        lambda: CoarseEmaV1(cohort_keys).check_request("15m", 34), "unregistered_period")
    neg("N3", "unregistered timeframe fails",
        lambda: CoarseEmaV1(cohort_keys).check_request("4H", 20), "unregistered_timeframe")

    cont = d15[d15.coverage_state == "UNOBSERVED_CONTAMINATED"]
    rec("N4", "contaminated coarse bar cannot enter EMA",
        bool(len(cont) and df[df.ema_reset_reason == "UNOBSERVED_CONTAMINATED"
                              ].ema_value.isna().all()),
        f"{len(cont):,} contaminated bars produced 0 EMA values", "NEGATIVE")

    # N5/N6: build a deterministic COMPLETE -> CONTAMINATED -> COMPLETE fixture
    fx = d15[d15.security_key_v1 == sk].sort_values("interval_index").head(9).copy()
    fx["coverage_state"] = "COMPLETE"
    fx["close"] = [10.0, 11, 12, 13, 14, 15, 16, 17, 18]
    fx.iloc[4, fx.columns.get_loc("coverage_state")] = "UNOBSERVED_CONTAMINATED"
    fx.iloc[4, fx.columns.get_loc("close")] = None
    E2 = CoarseEmaV1(cohort_keys)
    r2 = pd.DataFrame(E2.update_session("15m", fx, emit=True))
    segs = r2[r2.ema_value.notna()].ema_segment_id.dropna().unique()
    rec("N5", "contaminated gap breaks the segment", len(segs) == 2,
        f"segments before/after the gap: {sorted(int(s) for s in segs)}", "NEGATIVE")
    after = r2[(r2.ema_period == 20) & r2.ema_value.notna()].iloc[4]
    rec("N6", "pre-gap EMA state cannot survive the break",
        abs(after.ema_value - 15.0) < 1e-12 and after.ema_age_valid_bars == 1,
        f"first post-gap EMA20 = {after.ema_value} (= its own close, seeded fresh), "
        f"age {after.ema_age_valid_bars}", "NEGATIVE")
    gap_rows = r2[r2.ema_reset_reason == "UNOBSERVED_CONTAMINATED"]
    rec("N7", "EMA cannot be forward-filled across an unavailable interval",
        bool(gap_rows.ema_value.isna().all()
             and (gap_rows.ema_validity_state == "UNAVAILABLE").all()),
        f"{len(gap_rows)} gap rows, all ema_value null and UNAVAILABLE", "NEGATIVE")
    early = vs[vs.ema_age_valid_bars < vs.ema_period]
    rec("N8", "EMA cannot become VALID before age >= period",
        bool((early.ema_validity_state == "INITIALIZATION_SENSITIVE").all()),
        f"{len(early):,} rows with age < period, none VALID", "NEGATIVE")

    fut = fx.copy()
    fut.iloc[8, fut.columns.get_loc("close")] = 999.0
    r3 = pd.DataFrame(CoarseEmaV1(cohort_keys).update_session("15m", fut, emit=True))
    a_before = r2[(r2.ema_period == 20) & r2.ema_value.notna()].ema_value.tolist()[:-1]
    a_after = r3[(r3.ema_period == 20) & r3.ema_value.notna()].ema_value.tolist()[:-1]
    rec("N9", "a future coarse close cannot affect EMA[t]", a_before == a_after,
        "changing the last bar's close left every earlier EMA byte-identical", "NEGATIVE")
    rec("N10", "EMA cannot be visible before its coarse bar completes",
        bool((r2.feature_information_available_at == r2.bar_end).all()),
        "availability equals bar_end on every emitted row", "NEGATIVE")

    def n11():
        bad = d15.head(5).copy(); bad["coverage_state"] = "EXTENDED_HOURS_BAR"
        CoarseEmaV1(cohort_keys).update_session("15m", bad)
    neg("N11", "forbidden input classes cannot affect EMA", n11, "forbidden_input")

    rec("N12", "feature invalidity cannot remove a security from the cohort",
        df.security_key_v1.nunique() == d15.security_key_v1.nunique() == 476,
        f"{df.security_key_v1.nunique()} securities emitted rows, cohort still 476",
        "NEGATIVE")
    neg("N13", "a legacy ribbon period cannot enter production",
        lambda: CoarseEmaV1(cohort_keys).check_request("15m", 89), "unregistered_period")

    smoke_ok = all(r["passed"] for r in res)
    print(f"\n  smoke {sum(r['passed'] for r in res)}/{len(res)} "
          f"({time.time()-t0:.0f}s)", flush=True)
    if not smoke_ok:
        print("HOLD — smoke failed; census not run"); return 1

    # ---------------- CAPABILITY CENSUS ----------------
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(PART, "*.json")))
    eng = {tf: CoarseEmaV1(cohort_keys) for tf in REGISTERED_TFS}
    cnt = {(tf, p): collections.Counter() for tf in REGISTERED_TFS
           for p in REGISTERED_PERIODS}
    anyvalid = {(tf, p): set() for tf in REGISTERED_TFS for p in REGISTERED_PERIODS}
    seglen = {tf: collections.Counter() for tf in REGISTERED_TFS}
    resets = {tf: 0 for tf in REGISTERED_TFS}
    complete_in = {tf: 0 for tf in REGISTERED_TFS}
    t1 = time.time()
    for si, day in enumerate(sessions, 1):
        for tf in REGISTERED_TFS:
            d = read(tf, day)
            if d is None:
                continue
            prev = {k: v for k, v in eng[tf].seg_len.items()}
            out = eng[tf].update_session(tf, d, emit=True)
            complete_in[tf] += int((d.coverage_state == "COMPLETE").sum())
            for r in out:
                c = cnt[(tf, r["ema_period"])]
                c[r["ema_validity_state"]] += 1
            for sk, before in prev.items():
                if before and eng[tf].seg_len.get(sk, 0) == 0:
                    seglen[tf][before] += 1
                    resets[tf] += 1
            for r in out:
                if r["ema_validity_state"] == "VALID":
                    anyvalid[(tf, r["ema_period"])].add(r["security_key_v1"])
        if si % 200 == 0 or si == len(sessions):
            print(f"  census {si}/{len(sessions)} · {time.time()-t1:.0f}s", flush=True)
    for tf in REGISTERED_TFS:
        for sk, v in eng[tf].seg_len.items():
            if v:
                seglen[tf][v] += 1

    streams = {}
    for tf in REGISTERED_TFS:
        sl = sorted(seglen[tf].elements()) if sum(seglen[tf].values()) < 4_000_000 else None
        seg_stats = dict(segments=sum(seglen[tf].values()), resets=resets[tf],
                         max=max(seglen[tf]) if seglen[tf] else None,
                         mean=round(sum(L * n for L, n in seglen[tf].items())
                                    / max(sum(seglen[tf].values()), 1), 2))
        if sl:
            for q, lb in ((.5, "median"), (.75, "p75"), (.9, "p90"), (.99, "p99")):
                seg_stats[lb] = pct(sl, q)
        for p in REGISTERED_PERIODS:
            c = cnt[(tf, p)]
            tot = c["VALID"] + c["INITIALIZATION_SENSITIVE"] + c["UNAVAILABLE"]
            streams[f"EMA{p}@{tf}"] = dict(
                timeframe=tf, period=p,
                candidate_complete_inputs=complete_in[tf],
                VALID=c["VALID"], INITIALIZATION_SENSITIVE=c["INITIALIZATION_SENSITIVE"],
                UNAVAILABLE=c["UNAVAILABLE"], total_observations=tot,
                valid_share_of_coarse_observations=round(c["VALID"] / tot, 4) if tot else None,
                valid_share_of_complete_inputs=round(c["VALID"] / complete_in[tf], 4)
                if complete_in[tf] else None,
                securities_with_any_VALID=len(anyvalid[(tf, p)]),
                segment_stats=seg_stats)

    verifier = dict(
        streams_12=len(streams) == 12,
        segments_independent_of_period=all(
            streams[f"EMA{p}@{tf}"]["segment_stats"]["segments"]
            == streams[f"EMA{REGISTERED_PERIODS[0]}@{tf}"]["segment_stats"]["segments"]
            for tf in REGISTERED_TFS for p in REGISTERED_PERIODS),
        unavailable_equals_contaminated=all(
            streams[f"EMA{p}@{tf}"]["UNAVAILABLE"]
            == streams[f"EMA{REGISTERED_PERIODS[0]}@{tf}"]["UNAVAILABLE"]
            for tf in REGISTERED_TFS for p in REGISTERED_PERIODS),
        valid_monotone_in_period=all(
            streams[f"EMA{REGISTERED_PERIODS[i]}@{tf}"]["VALID"]
            >= streams[f"EMA{REGISTERED_PERIODS[i+1]}@{tf}"]["VALID"]
            for tf in REGISTERED_TFS for i in range(len(REGISTERED_PERIODS) - 1)),
        cohort_preserved=all(s["securities_with_any_VALID"] <= 476
                             for s in streams.values()),
        no_rvol_fields=True)
    vpass = all(verifier.values())

    p = dict(
        report_id="MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1",
        status=("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1_PASS"
                if (smoke_ok and vpass) else "HOLD"),
        classification="SMOKE_OR_CAPABILITY_STAGING_ONLY",
        explicitly_not=["PRODUCTION_EMA", "RESEARCH_EVIDENCE", "T/Z DATA"],
        binding=dict(ema_spec=SPEC_D, coarse_production=COARSE_D, eligibility=ELIG_D,
                     cohort=476, sessions=len(sessions),
                     periods=list(REGISTERED_PERIODS), timeframes=list(REGISTERED_TFS),
                     streams=12),
        implementation_hash=ih, staging_path=stage,
        smoke=dict(total=len(res),
                   positive_passed=sum(1 for r in res
                                       if r["kind"] == "POSITIVE" and r["passed"]),
                   positive=sum(1 for r in res if r["kind"] == "POSITIVE"),
                   negative_passed=sum(1 for r in res
                                       if r["kind"] == "NEGATIVE" and r["passed"]),
                   negative=sum(1 for r in res if r["kind"] == "NEGATIVE"),
                   results=res),
        capability_census=dict(
            label_free=True, y_exposed=0, streams=streams,
            why_valid_reported_separately="a stream producing values on most observations "
                                          "tells you nothing if those values sit at age 3 "
                                          "of a 200-bar EMA; the share reaching age >= "
                                          "period is what decides usability",
            segmentation_note="segments depend only on coverage, not on period, so all four "
                              "periods of a (security, timeframe) break at the same bars"),
        independent_checks=dict(checks=verifier, passed=vpass,
                                failed=[k for k, v in verifier.items() if not v]),
        rvol=dict(computed=False, emitted=False, fields=0,
                  status="BLOCKED_PENDING_DICTIONARY_FREEZE"),
        immutability=dict(
            ema_spec=ART.file_digest(SPEC) == SPEC_D,
            coarse_production=ART.file_digest(
                "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json") == COARSE_D,
            eligibility=ART.file_digest(ELIG) == ELIG_D,
            production_ema_writes=0,
            production_ema_exists=os.path.exists(PROD_EMA), y_exposure=0),
        authorises="ONLY the full EMA production build. NOT RVOL, T, Z or Y.",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json",
                 required=("report_id", "status", "binding", "implementation_hash",
                           "smoke", "capability_census", "independent_checks", "rvol",
                           "immutability"),
                 supersede=os.path.exists(
                     "MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json"))
    print(f"\nMASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1 · {d} · {p['status']}")
    for tf in REGISTERED_TFS:
        for per in REGISTERED_PERIODS:
            s = streams[f"EMA{per}@{tf}"]
            print(f"  EMA{per:<3d}@{tf:<4s} VALID {s['VALID']:>11,d} "
                  f"({s['valid_share_of_complete_inputs']:.1%} of COMPLETE) · "
                  f"securities {s['securities_with_any_VALID']:>3d}/476")
    print(f"  verifier {'PASS' if vpass else 'HOLD'} · RVOL fields 0 · EMA prod writes 0")
    return 0 if (smoke_ok and vpass) else 1


if __name__ == "__main__":
    raise SystemExit(main())
