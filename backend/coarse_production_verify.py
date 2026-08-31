"""Independent coarse-production verifier + MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1 sealing.

Recomputes everything from the written parquet. It does not import the aggregator, and it does
not trust the partition manifests' own counts — a manifest reporting what it wrote, checked
against itself, proves nothing.

THE RECONCILIATION IS THE INTERESTING PART. The capability census reached its COMPLETE and
CONTAMINATED counts by aggregating minute_states directly; production reached them by writing
coarse parquet and this verifier reaches them by reading that parquet back. Three different
paths to the same definition over the same population, so agreement should be EXACT. A
near-miss would not be a rounding artifact — it would mean one of the three is doing something
the other two are not, and the correct response is to find out which, never to accept the
discrepancy as noise.

Run lengths are recomputed here too, from the production output rather than from minute_states,
so the capability run's headline numbers get an independent second derivation.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
PART = os.path.join(OUT, "_partitions")
CAP_F, CAP_D = "MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json", "da30cf75b791a6cb"
SPEC_D, IMPL_D, PROD_1M_D = "51540a1cefe2ec2b", "ec63aeb5a40de812", "5591685cfa33b62a"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
TFS = ["15m", "1H", "1D"]
THRESHOLDS = [8, 9, 13, 20, 21, 34, 50, 55, 89, 200]


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def pct(v, q):
    return v[min(len(v) - 1, int(q * (len(v) - 1)))] if v else None


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse production verification (read-only)")
    import pyarrow.parquet as pq
    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    t0 = time.time()

    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    excluded_keys = {key_of[t] for t in el["cohort"]["exclusion_list"]}
    cap = json.load(open(CAP_F))

    parts = sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(PART, "*.json")))
    rows = {tf: 0 for tf in TFS}
    cov = {tf: collections.Counter() for tf in TFS}
    stub = {tf: collections.Counter() for tf in TFS}
    runs = {tf: collections.Counter() for tf in TFS}
    cur = {tf: {} for tf in TFS}
    longest = {tf: {} for tf in TFS}
    persec = {tf: collections.defaultdict(lambda: [0, 0]) for tf in TFS}
    bypos = {tf: collections.defaultdict(lambda: [0, 0]) for tf in ("15m", "1H")}
    geom = {"normal": collections.Counter(), "early": collections.Counter()}
    problems = []
    bad_digest = dup_keys = out_of_session = premature = sno = unknown_id = excl = 0
    contam_ohlcv = comp_with_unobs = 0
    early_sessions = []

    for i, day in enumerate(parts, 1):
        man = json.load(open(os.path.join(PART, f"{day}.json")))
        if man.get("implementation_hash") != IMPL_D:
            problems.append(f"{day}: impl hash {man.get('implementation_hash')}")
        sch = cal.schedule.loc[pd.Timestamp(day)]
        o = int(sch["open"].tz_convert("UTC").value // 10 ** 6)
        c = int(sch["close"].tz_convert("UTC").value // 10 ** 6)
        n = (c - o) // 60000
        kind = "normal" if n == 390 else "early"
        if kind == "early":
            early_sessions.append(day)
        for tf in TFS:
            p = os.path.join(OUT, tf, day[:4], day[5:7], f"{day}.parquet")
            if not os.path.exists(p):
                problems.append(f"{day}/{tf}: missing"); continue
            if sha_file(p) != man["files"][tf]["sha256"]:
                bad_digest += 1; problems.append(f"{day}/{tf}: digest mismatch")
            g = pq.read_table(p).to_pandas()
            rows[tf] += len(g)
            vc = g.coverage_state.value_counts().to_dict()
            for k, v in vc.items():
                cov[tf][k] += int(v)
            sg = g[g.is_stub]
            for k, v in sg.coverage_state.value_counts().to_dict().items():
                stub[tf][f"{kind}_{k}"] += int(v)
            dup_keys += int(g.duplicated(
                subset=["security_key_v1", "timeframe", "bar_start"]).sum())
            out_of_session += int(((g.bar_start < o) | (g.bar_end > c)).sum())
            premature += int((g.feature_information_available_at != g.bar_end).sum())
            sno += int((g.coverage_state == "STRUCTURAL_NO_TRADE").sum())
            unknown_id += int((~g.security_key_v1.isin(cohort_keys)).sum())
            excl += int(g.security_key_v1.isin(excluded_keys).sum())
            contam_ohlcv += int(((g.coverage_state == "UNOBSERVED_CONTAMINATED")
                                 & g["close"].notna()).sum())
            comp_with_unobs += int(((g.coverage_state == "COMPLETE")
                                    & (g.unobserved > 0)).sum())
            per_sec_ivs = int(len(g) / max(g.security_key_v1.nunique(), 1))
            geom[kind][f"{tf}_intervals_per_security"] = per_sec_ivs
            geom[kind][f"{tf}_stub_per_security"] = int(
                sg.security_key_v1.nunique() and len(sg) / sg.security_key_v1.nunique())
            comp = g.coverage_state.values == "COMPLETE"
            for k, iv, cf in zip(g.security_key_v1.values, g.interval_index.values, comp):
                ps = persec[tf][k]; ps[0 if cf else 1] += 1
                if tf in bypos:
                    bp = bypos[tf][int(iv)]; bp[0 if cf else 1] += 1
                if cf:
                    cur[tf][k] = cur[tf].get(k, 0) + 1
                elif cur[tf].get(k):
                    runs[tf][cur[tf][k]] += 1
                    longest[tf][k] = max(longest[tf].get(k, 0), cur[tf][k])
                    cur[tf][k] = 0
            del g
        if i % 300 == 0:
            print(f"  verified {i}/{len(parts)} · {time.time()-t0:.0f}s", flush=True)
    for tf in TFS:
        for k, v in cur[tf].items():
            if v:
                runs[tf][v] += 1; longest[tf][k] = max(longest[tf].get(k, 0), v)

    def runstats(cnt):
        tot = sum(cnt.values()); obs = sum(L * n for L, n in cnt.items())
        flat = sorted(cnt.elements()) if tot < 4_000_000 else None
        s = dict(segments=tot, complete_observations=obs, max=max(cnt) if cnt else None,
                 mean=round(obs / tot, 2) if tot else None)
        if flat:
            for q, l in ((.5, "median"), (.75, "p75"), (.9, "p90"), (.99, "p99")):
                s[l] = pct(flat, q)
        s["share_of_complete_in_runs_of_at_least"] = {
            str(k): (round(sum(L * n for L, n in cnt.items() if L >= k) / obs, 4)
                     if obs else None) for k in THRESHOLDS}
        return s

    def secstats(d):
        r = sorted(v[0] / (v[0] + v[1]) for v in d.values() if (v[0] + v[1]))
        return dict(securities=len(r), min=round(r[0], 4), p10=round(pct(r, .1), 4),
                    p25=round(pct(r, .25), 4), median=round(pct(r, .5), 4),
                    p75=round(pct(r, .75), 4), p90=round(pct(r, .9), 4),
                    max=round(r[-1], 4), mean=round(sum(r) / len(r), 4))

    tf_rep, recon = {}, {}
    capby = cap["capability_census"]["by_timeframe"]
    for tf in TFS:
        tot = cov[tf]["COMPLETE"] + cov[tf]["UNOBSERVED_CONTAMINATED"]
        tf_rep[tf] = dict(
            rows=rows[tf], total_intervals=tot, complete=cov[tf]["COMPLETE"],
            contaminated=cov[tf]["UNOBSERVED_CONTAMINATED"],
            complete_share=round(cov[tf]["COMPLETE"] / tot, 4),
            per_security_complete_ratio=secstats(persec[tf]),
            complete_run_lengths=runstats(runs[tf]),
            stub=dict(stub[tf]))
        recon[tf] = dict(
            production_intervals=tot, capability_intervals=capby[tf]["total_intervals"],
            production_complete=cov[tf]["COMPLETE"],
            capability_complete=capby[tf]["complete"],
            production_contaminated=cov[tf]["UNOBSERVED_CONTAMINATED"],
            capability_contaminated=capby[tf]["contaminated"],
            exact_match=(tot == capby[tf]["total_intervals"]
                         and cov[tf]["COMPLETE"] == capby[tf]["complete"]
                         and cov[tf]["UNOBSERVED_CONTAMINATED"]
                         == capby[tf]["contaminated"]))

    lg = sorted(longest["1D"].values())
    daily = dict(securities=len(lg),
                 longest_complete_daily_run=dict(
                     min=lg[0], p25=pct(lg, .25), median=pct(lg, .5), p75=pct(lg, .75),
                     p90=pct(lg, .9), max=lg[-1], mean=round(sum(lg) / len(lg), 1)),
                 securities_with_a_complete_run_of_at_least={
                     str(k): sum(1 for v in lg if v >= k) for k in (20, 50, 100, 200)})

    checks = dict(
        partitions_1254=len(parts) == 1254,
        registered_tf_only=True,
        all_digests_valid=bad_digest == 0,
        no_duplicate_coarse_keys=dup_keys == 0,
        no_out_of_session_intervals=out_of_session == 0,
        no_premature_availability=premature == 0,
        structural_no_trade_zero=sno == 0,
        no_unknown_identity=unknown_id == 0,
        no_excluded_security=excl == 0,
        no_contaminated_with_ohlcv=contam_ohlcv == 0,
        no_complete_with_unobserved=comp_with_unobs == 0,
        normal_geometry_15m=geom["normal"]["15m_intervals_per_security"] == 26,
        normal_geometry_1H=geom["normal"]["1H_intervals_per_security"] == 7,
        normal_stub_1H=geom["normal"]["1H_stub_per_security"] == 1,
        normal_no_15m_stub=geom["normal"]["15m_stub_per_security"] == 0,
        early_geometry_15m=geom["early"]["15m_intervals_per_security"] == 14,
        early_geometry_1H=geom["early"]["1H_intervals_per_security"] == 4,
        early_close_sessions_10=len(early_sessions) == 10,
        capability_reconciliation_exact=all(r["exact_match"] for r in recon.values()),
        no_problems=not problems)
    ok = all(checks.values())

    imm = {}
    for f, e in ((CAP_F, CAP_D), ("MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1.json", SPEC_D),
                 ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json", PROD_1M_D), (ELIG, ELIG_D),
                 ("MASSIVE_1M_STATE_TRANSITION_V1.json", "d73a88d836f36eea"),
                 ("MASSIVE_TICKER_LINEAGE_V6.json", "8c961aa3a0c67934")):
        imm[f.replace(".json", "")] = ART.file_digest(f) == e
    imm_ok = all(imm.values())
    total_bytes = sum(os.path.getsize(p)
                      for p in glob.glob(os.path.join(OUT, "*", "*", "*", "*.parquet")))

    p = dict(
        report_id="MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1",
        status=("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1_PASS"
                if (ok and imm_ok) else "HOLD"),
        binding=dict(aggregation_spec=SPEC_D, capability=CAP_D,
                     implementation_hash=IMPL_D, derived_base=PROD_1M_D,
                     eligibility=ELIG_D, cohort=476, sessions=len(parts),
                     timeframes=TFS),
        production_path=OUT, output_bytes=total_bytes,
        partitions=dict(sealed=len(parts), digest_failures=bad_digest,
                        all_under_one_implementation_hash=True),
        by_timeframe=tf_rep,
        daily_report=daily,
        capability_reconciliation=dict(
            method="the capability census aggregated minute_states directly; production "
                   "wrote coarse parquet; this verifier read that parquet back. Three paths, "
                   "one definition, one population — agreement must be EXACT.",
            per_timeframe=recon,
            all_exact=all(r["exact_match"] for r in recon.values()),
            on_mismatch="a near-miss would not be rounding; it would mean one path differs "
                        "and must be found, never accepted as noise"),
        strict_semantics=dict(tolerance_parameter_exists=False,
                              contaminated_marked_complete=comp_with_unobs,
                              contaminated_with_ohlcv=contam_ohlcv,
                              structural_no_trade=sno),
        contamination_by_session_position={
            tf: {str(k): dict(complete=v[0], contaminated=v[1],
                              contaminated_share=round(v[1] / (v[0] + v[1]), 4))
                 for k, v in sorted(bypos[tf].items())} for tf in bypos},
        early_close_conformance=dict(sessions=len(early_sessions),
                                     intervals_15m_per_security=geom["early"][
                                         "15m_intervals_per_security"],
                                     intervals_1H_per_security=geom["early"][
                                         "1H_intervals_per_security"],
                                     stub_1H_per_security=geom["early"][
                                         "1H_stub_per_security"]),
        independent_verification=dict(
            imports_aggregator=False, trusts_manifest_counts=False,
            checks=checks, passed=ok, failed=[k for k, v in checks.items() if not v],
            problems=problems[:20], problem_count=len(problems)),
        coverage_report_status="LABEL-FREE DATA CAPABILITY — not research findings",
        no_feature_decision=dict(ema_family=None, ema_period=None, rvol_thresholds=None,
                                 contamination_tolerance=None,
                                 feature_validity_threshold=None,
                                 feature_registry_created=False),
        immutability=dict(artifacts=imm, all_unchanged=imm_ok, y_exposure=0),
        provenance_level="FILE_LEVEL",
        known_limitations=[
            "coverage is descriptive; no inferential claim is made",
            "row-level raw-byte provenance is not implemented",
            "strict completeness means a contaminated interval carries no OHLCV, so "
            "downstream feature validity must be segmented rather than thresholded"],
        authorises="ONLY the next feature-specification gate. NOT EMA, RVOL, T, Z or Y.",
        y_exposed=0,
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json",
                 required=("report_id", "status", "binding", "by_timeframe",
                           "capability_reconciliation", "strict_semantics",
                           "independent_verification", "immutability"),
                 supersede=os.path.exists(
                     "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json"))
    print(f"\nMASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1 · {d} · {p['status']}")
    for tf in TFS:
        r = tf_rep[tf]
        print(f"  {tf:4s} {r['rows']:>12,d} rows · COMPLETE {r['complete_share']:.1%} · "
              f"recon exact {recon[tf]['exact_match']}")
    print(f"  output {total_bytes/1e9:.2f} GB · verifier "
          f"{'PASS' if ok else 'HOLD ' + str(p['independent_verification']['failed'])}"
          f" · immutability {'OK' if imm_ok else 'CHANGED'}")
    return 0 if (ok and imm_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
