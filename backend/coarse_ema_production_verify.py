"""Independent EMA production verifier + MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1 sealing.

Does not import the EMA engine. The recursion is re-implemented here from the frozen formula
so that a bug in the engine's segmentation or seeding cannot be reproduced and then confirmed
as correct — the failure mode a shared implementation guarantees.

THREE LAYERS, DELIBERATELY DIFFERENT IN COST AND STRENGTH.

Structural checks run over every written file: digests, key uniqueness, availability equals
bar_end, VALID exactly where age >= period, contaminated bars carrying no value. These are
cheap and cover all 80.8M rows.

Independent recomputation of the COUNTS runs over the whole dataset with this file's own
recursion. If the engine mis-seeded after a gap or carried state across one, the VALID totals
would drift, so an exact match across all twelve streams is a strong statement.

Row-level value comparison runs on a sampled set of securities across all 1,254 sessions.
Comparing 80.8M float pairs would cost more than it proves; comparing a sample exactly — every
value, every session, to 1e-12 — catches an arithmetic error just as reliably.

The capability artifact's numbers are the fourth check, and they were produced by a third code
path on a different day. Exact agreement there is not a formality.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_ema_v1"
PART = os.path.join(OUT, "_partitions")
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
SPEC_D, CAP_D, COARSE_D = "ef01b67c22f6770f", "bd57b0e1cb959d7d", "6ec73d5f462761f0"
RVOL_D = "6095852c4a2eeaa5"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
PERIODS, TFS = (9, 20, 50, 200), ("15m", "1H", "1D")
SAMPLE_SECURITIES = 20
RVOL_COLS = {"rvol", "rvol_value", "baseline_value", "slot_index", "numerator_volume"}


def sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="EMA production verification (read-only)")
    import pyarrow.parquet as pq
    import pandas as pd
    t0 = time.time()

    for f, w in (("MASSIVE_COARSE_EMA_FEATURE_SPEC_V1.json", SPEC_D),
                 ("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json", CAP_D),
                 ("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json", COARSE_D),
                 (ELIG, ELIG_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — {f} digest mismatch"); return 1
    cap = json.load(open("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1.json"))
    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    sample = sorted(cohort_keys)[::max(1, len(cohort_keys) // SAMPLE_SECURITIES)][
        :SAMPLE_SECURITIES]
    sample_set = set(sample)

    parts = sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(PART, "*.json")))
    rows = {tf: 0 for tf in TFS}
    prod_v = {(tf, p): collections.Counter() for tf in TFS for p in PERIODS}
    prod_any = {(tf, p): set() for tf in TFS for p in PERIODS}
    # independent recomputation state
    ind_state, ind_v, ind_any = {}, {(tf, p): collections.Counter() for tf in TFS
                                     for p in PERIODS}, {(tf, p): set() for tf in TFS
                                                          for p in PERIODS}
    bad_digest = dup = avail_bad = valid_bad = contam_val = seed_bad = 0
    rvol_cols_found = sample_mismatch = sample_compared = 0
    problems = []

    for i, day in enumerate(parts, 1):
        man = json.load(open(os.path.join(PART, f"{day}.json")))
        for tf in TFS:
            p = os.path.join(OUT, tf, day[:4], day[5:7], f"{day}.parquet")
            if not os.path.exists(p):
                problems.append(f"{day}/{tf} missing"); continue
            if sha_file(p) != man["files"][tf]["sha256"]:
                bad_digest += 1; problems.append(f"{day}/{tf} digest mismatch")
            schema = set(pq.read_schema(p).names)
            rvol_cols_found += len(RVOL_COLS & schema)
            g = pq.read_table(p).to_pandas()
            rows[tf] += len(g)
            dup += int(g.duplicated(subset=["security_key_v1", "timeframe", "bar_start",
                                            "ema_period"]).sum())
            avail_bad += int((g.feature_information_available_at != g.bar_end).sum())
            v = g[g.ema_value.notna()]
            valid_bad += int(((v.ema_age_valid_bars >= v.ema_period)
                              != (v.ema_validity_state == "VALID")).sum())
            contam_val += int(g[g.ema_reset_reason == "UNOBSERVED_CONTAMINATED"
                                ].ema_value.notna().sum())
            for per in PERIODS:
                s = g[g.ema_period == per]
                for k, n in s.ema_validity_state.value_counts().items():
                    prod_v[(tf, per)][k] += int(n)
                prod_any[(tf, per)].update(
                    s[s.ema_validity_state == "VALID"].security_key_v1.unique())

            # ---- independent recomputation from the coarse source
            cp = os.path.join(COARSE, tf, day[:4], day[5:7], f"{day}.parquet")
            c = pq.read_table(cp, columns=["security_key_v1", "interval_index",
                                           "coverage_state", "close",
                                           "bar_start"]).to_pandas()
            c = c.sort_values(["security_key_v1", "interval_index"])
            seed_check = {}
            for k, cs, cl, bs in zip(c.security_key_v1, c.coverage_state, c["close"],
                                     c.bar_start):
                for per in PERIODS:
                    kk = (k, tf, per)
                    if cs != "COMPLETE":
                        ind_state.pop(kk, None)
                        ind_v[(tf, per)]["UNAVAILABLE"] += 1
                        continue
                    st = ind_state.get(kk)
                    if st is None:
                        ema, age = float(cl), 1
                        if per == PERIODS[0]:
                            seed_check[(k, int(bs))] = float(cl)
                    else:
                        a = 2.0 / (per + 1)
                        ema, age = a * float(cl) + (1 - a) * st[0], st[1] + 1
                    ind_state[kk] = [ema, age]
                    ind_v[(tf, per)]["VALID" if age >= per
                                     else "INITIALIZATION_SENSITIVE"] += 1
                    if age >= per:
                        ind_any[(tf, per)].add(k)
                    if k in sample_set:
                        pr = g[(g.security_key_v1 == k) & (g.ema_period == per)
                               & (g.bar_start == bs)]
                        if len(pr) == 1:
                            sample_compared += 1
                            if abs(float(pr.ema_value.iloc[0]) - ema) > 1e-12:
                                sample_mismatch += 1
            # fresh post-gap seeding: an age==1 row must equal its own coarse close
            s1 = g[(g.ema_age_valid_bars == 1) & (g.ema_period == PERIODS[0])
                   & g.ema_value.notna()]
            for k, bs, ev in zip(s1.security_key_v1, s1.bar_start, s1.ema_value):
                exp = seed_check.get((k, int(bs)))
                if exp is None or abs(float(ev) - exp) > 1e-12:
                    seed_bad += 1
            del g, c
        if i % 200 == 0:
            print(f"  verified {i}/{len(parts)} · {time.time()-t0:.0f}s", flush=True)

    recon, counts_match = {}, True
    for tf in TFS:
        for per in PERIODS:
            nm = f"EMA{per}@{tf}"
            capv = cap["capability_census"]["streams"][nm]
            r = dict(production_VALID=prod_v[(tf, per)]["VALID"],
                     independent_VALID=ind_v[(tf, per)]["VALID"],
                     capability_VALID=capv["VALID"],
                     production_securities=len(prod_any[(tf, per)]),
                     independent_securities=len(ind_any[(tf, per)]),
                     capability_securities=capv["securities_with_any_VALID"])
            r["all_three_exact"] = (r["production_VALID"] == r["independent_VALID"]
                                    == r["capability_VALID"]
                                    and r["production_securities"]
                                    == r["independent_securities"]
                                    == r["capability_securities"])
            counts_match &= r["all_three_exact"]
            recon[nm] = r

    checks = dict(
        partitions_1254=len(parts) == 1254,
        twelve_streams_only=len(recon) == 12,
        cohort_476_preserved=all(len(prod_any[k]) <= 476 for k in prod_any),
        all_file_digests_valid=bad_digest == 0,
        no_duplicate_keys=dup == 0,
        causal_availability=avail_bad == 0,
        valid_iff_age_ge_period=valid_bad == 0,
        contaminated_carry_no_value=contam_val == 0,
        fresh_post_gap_seeding=seed_bad == 0,
        no_rvol_fields=rvol_cols_found == 0,
        independent_recomputation_exact=counts_match,
        sample_values_exact=sample_mismatch == 0,
        no_problems=not problems)
    ok = all(checks.values())

    imm = {n: ART.file_digest(n + ".json") == d for n, d in (
        ("MASSIVE_COARSE_EMA_FEATURE_SPEC_V1", SPEC_D),
        ("MASSIVE_COARSE_EMA_FEATURE_SMOKE_CAPABILITY_V1", CAP_D),
        ("MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1", COARSE_D),
        ("MASSIVE_RVOL_DICTIONARY_SPEC_V1", RVOL_D),
        ("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1", ELIG_D),
        ("MASSIVE_1M_STATE_TRANSITION_V1", "d73a88d836f36eea"))}
    imm_ok = all(imm.values())
    total_bytes = sum(os.path.getsize(p)
                      for p in glob.glob(os.path.join(OUT, "*", "*", "*", "*.parquet")))

    p = dict(
        report_id="MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1",
        status=("MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1_PASS"
                if (ok and imm_ok) else "HOLD"),
        binding=dict(ema_spec=SPEC_D, smoke_capability=CAP_D, coarse_production=COARSE_D,
                     eligibility=ELIG_D, implementation_hash=cap["implementation_hash"],
                     cohort=476, sessions=len(parts), periods=list(PERIODS),
                     timeframes=list(TFS), streams=12),
        production_path=OUT, output_bytes=total_bytes,
        dataset_rows=rows, total_rows=sum(rows.values()),
        stream_reconciliation=dict(
            method="three independent code paths — the production engine, this verifier's "
                   "own recursion, and the earlier capability census — computed the same "
                   "quantities from the same coarse source",
            all_exact=counts_match, per_stream=recon,
            why_it_is_strong="a mis-seeded segment or state carried across a gap would drift "
                             "the VALID totals; exact agreement across all twelve streams "
                             "is not a formality"),
        verification_layers=dict(
            structural="every written file: digests, key uniqueness, availability = bar_end, "
                       "VALID iff age >= period, contaminated rows carry no value",
            independent_recomputation="this file's own recursion over the whole dataset, "
                                      "compared on counts",
            row_level_sample=dict(securities=len(sample), values_compared=sample_compared,
                                  mismatches=sample_mismatch, tolerance="1e-12",
                                  why_sampled="comparing 80.8M float pairs would cost more "
                                              "than it proves; an exact sample catches an "
                                              "arithmetic error just as reliably")),
        checks=checks, failed=[k for k, v in checks.items() if not v],
        problems=problems[:20], problem_count=len(problems),
        segmentation=dict(fresh_post_gap_seeding_violations=seed_bad,
                          contaminated_rows_with_value=contam_val,
                          rule="an age==1 row must equal its own coarse close; state is "
                               "deleted at a break rather than flagged"),
        rvol=dict(fields_in_output=rvol_cols_found, computed=False,
                  dictionary_frozen_separately=RVOL_D,
                  production_status="HOLD"),
        immutability=dict(artifacts=imm, all_unchanged=imm_ok, y_exposure=0),
        provenance_level="FILE_LEVEL",
        authorises="nothing beyond itself. T/Z and Y exposure remain HOLD.",
        y_exposed=0,
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1.json",
                 required=("report_id", "status", "binding", "dataset_rows",
                           "stream_reconciliation", "verification_layers", "checks",
                           "rvol", "immutability"),
                 supersede=os.path.exists("MASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1.json"))
    print(f"\nMASSIVE_COARSE_EMA_FEATURE_PRODUCTION_V1 · {d} · {p['status']}")
    print(f"  rows {sum(rows.values()):,} · {total_bytes/1e9:.2f} GB · "
          f"{len(parts)} partitions")
    print(f"  three-path reconciliation exact: {counts_match}")
    print(f"  sample rows compared {sample_compared:,} · mismatches {sample_mismatch}")
    print(f"  checks {sum(1 for v in checks.values() if v)}/{len(checks)} · "
          f"RVOL fields {rvol_cols_found} · immutability {'OK' if imm_ok else 'CHANGED'}")
    if p["failed"]:
        print(f"  FAILED: {p['failed']}")
    return 0 if (ok and imm_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
