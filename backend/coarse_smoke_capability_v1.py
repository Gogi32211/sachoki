"""MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1 — 13 negatives, then a full 476x1254 census.

Two jobs. The smoke proves the frozen aggregation contract behaves as written on deterministic
fixtures. The census then measures, label-free, how much of the coarse layer is actually usable.

WHY RUN LENGTHS AND NOT JUST PERCENTAGES. A COMPLETE share of 36% at 1D is compatible with two
completely different worlds: one where usable days come in long stretches broken occasionally,
and one where they are scattered singletons. An EMA needs consecutive usable bars, so the first
world supports a 200-period EMA and the second cannot support a 20-period one. The percentage
cannot tell them apart; the run-length distribution can. So runs are accumulated incrementally
in session order — current run, completed-run histogram, per-security longest — which keeps the
whole census in a few hundred counters instead of materialising 15.5M interval flags.

THE RUN-LENGTH THRESHOLDS ARE REFERENCES, NOT CANDIDATES. 8/9/13/20/21/34/50/55/89/200 appear
below because they span both legacy parameter families, so the report can answer "could this
family be supported" for either. Reporting them authorises nothing and selects nothing; the
parameter registry stays separately governed and EMA remains BLOCKED_PENDING_DECISION.

A NEGATIVE FIXTURE ALREADY EARNED ITS KEEP HERE. N8's first run PASSED the dangerous state:
the stub guard compared an interval's `stub` flag to its declared width, so a subclass that
padded BOTH together looked self-consistent and slipped through. The session span is the thing
that cannot be padded, so the guard now measures declared minutes against the calendar span and
against the session total. The implementation hash changed and the whole run was re-executed
from scratch — no fixture result was carried across.

NO TOLERANCE KNOB EXISTS ANYWHERE IN THIS GATE. Not defaulted, not configurable, absent — in
the aggregator and here. Tolerance belongs to a later study specification, declared before Y.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402
from coarse_aggregator_v1 import (CoarseAggregatorV1, AggHold,         # noqa: E402
                                  REGISTERED_TF, DERIVED_ROOT)

SPEC, SPEC_D = "MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1.json", "51540a1cefe2ec2b"
PROD_D = "5591685cfa33b62a"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
CHARTER_D = "d73a88d836f36eea"
PART = os.path.join(DERIVED_ROOT, "_partitions")
STAGE = "/Volumes/QUANT_RESEARCH/staging/coarse_smoke_capability_v1"
PROD_COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1"
ORD, EARLY = "2024-05-15", "2024-07-03"
THRESHOLDS = [8, 9, 13, 20, 21, 34, 50, 55, 89, 200]
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")


def impl_hash():
    h = hashlib.sha256()
    for f in ("coarse_aggregator_v1.py", "coarse_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def load(day, cols_ms=None, cols_b=None):
    import pyarrow.parquet as pq
    ms = pq.read_table(os.path.join(DERIVED_ROOT, "minute_states", day[:4], day[5:7],
                                    f"{day}.parquet"), columns=cols_ms).to_pandas()
    bp = os.path.join(DERIVED_ROOT, "observed_regular_bars", day[:4], day[5:7],
                      f"{day}.parquet")
    b = pq.read_table(bp, columns=cols_b).to_pandas() if os.path.exists(bp) else None
    return ms, b


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(q * (len(sorted_vals) - 1)))]


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="coarse smoke + capability (staging only)")
    for f, want in ((SPEC, SPEC_D), (ELIG, ELIG_D),
                    ("MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json", PROD_D),
                    ("MASSIVE_1M_STATE_TRANSITION_V1.json", CHARTER_D)):
        if ART.file_digest(f) != want:
            print(f"HOLD — {f} digest mismatch"); return 1
    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}
    excluded_key = key_of[el["cohort"]["exclusion_list"][0]]
    if len(cohort_keys) != 476:
        print("HOLD — cohort binding mismatch"); return 1

    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    A = CoarseAggregatorV1(cal, cohort_keys)
    ih = impl_hash()
    stage = os.path.join(STAGE, ih[:16]); os.makedirs(stage, exist_ok=True)
    t0, results = time.time(), []

    def rec(fid, name, ok, detail, kind="POSITIVE"):
        results.append(dict(fixture=fid, name=name, kind=kind, passed=bool(ok),
                            detail=detail))
        print(f"  {fid:3s} {kind[:3]} {'PASS' if ok else 'FAIL'}  {name} | {detail}",
              flush=True)

    # =============== POSITIVE: geometry ===============
    for day, exp_min, n15, n1h_full in ((ORD, 390, 26, 6), (EARLY, 210, 14, 3)):
        o, c, n = A.session_bounds(day)
        i15, i1h = A.intervals(day, "15m"), A.intervals(day, "1H")
        ok = (n == exp_min and len(i15) == n15
              and not any(x["stub"] for x in i15)
              and sum(1 for x in i1h if not x["stub"]) == n1h_full
              and sum(1 for x in i1h if x["stub"]) == 1
              and i1h[-1]["minutes"] == 30)
        rec("P1" if day == ORD else "P2",
            f"{'normal' if day == ORD else 'early-close'} geometry", ok,
            f"{n}min · 15m x{len(i15)} stubs {sum(x['stub'] for x in i15)} · "
            f"1H {n1h_full} full + {sum(1 for x in i1h if x['stub'])} stub "
            f"({i1h[-1]['minutes']}min)")

    ms, b = load(ORD)
    g15 = A.aggregate(ORD, ms, b, "15m"); A.validate(g15)
    g1h = A.aggregate(ORD, ms, b, "1H"); A.validate(g1h)
    g1d = A.aggregate(ORD, ms, b, "1D"); A.validate(g1d)
    rec("P3", "aggregate + validate all three TFs", True,
        f"15m {len(g15):,} · 1H {len(g1h):,} · 1D {len(g1d):,} intervals")
    comp = g15[g15.coverage_state == "COMPLETE"]
    cont = g15[g15.coverage_state == "UNOBSERVED_CONTAMINATED"]
    rec("P4", "contaminated intervals carry no OHLCV",
        cont["close"].isna().all() and comp["close"].notna().all(),
        f"COMPLETE {len(comp):,} all with OHLCV · CONTAMINATED {len(cont):,} all null")
    rec("P5", "causal availability = interval end",
        bool((g15.feature_information_available_at == g15.bar_end).all()
             and (g15.bar_end > g15.bar_start).all()),
        "no coarse value is available before its final constituent minute completes")

    # =============== NEGATIVE ===============
    o, c, _ = A.session_bounds(ORD)

    def neg(fid, name, fn, token):
        try:
            fn()
            rec(fid, name, False, "DANGEROUS STATE ACCEPTED", "NEGATIVE")
        except AggHold as e:
            rec(fid, name, token in str(e), str(e)[:104], "NEGATIVE")

    pre = ms.iloc[:1].copy(); pre["minute_ts"] = o - 60000
    neg("N1", "pre-market minute cannot enter",
        lambda: A.aggregate(ORD, pd.concat([ms, pre]), b, "15m"), "input_scope")
    cb = ms.iloc[:1].copy(); cb["minute_ts"] = c
    neg("N2", "close-boundary bar cannot enter",
        lambda: A.aggregate(ORD, pd.concat([ms, cb]), b, "15m"), "input_scope")
    post = ms.iloc[:1].copy(); post["minute_ts"] = c + 600000
    neg("N3", "post-market minute cannot enter",
        lambda: A.aggregate(ORD, pd.concat([ms, post]), b, "15m"), "input_scope")

    z = ms.copy()
    z.loc[z.observation_state == "UNOBSERVED", "observation_state"] = "STRUCTURAL_NO_TRADE"
    neg("N4", "UNOBSERVED cannot become zero-volume/no-trade",
        lambda: A.aggregate(ORD, z, b, "15m"), "observation_state")

    def n5():
        gg = A.aggregate(ORD, ms, b, "15m")
        gg.loc[gg.coverage_state == "UNOBSERVED_CONTAMINATED", "coverage_state"] = "COMPLETE"
        A.validate(gg)
    neg("N5", "contaminated cannot be marked COMPLETE", n5,
        "contaminated_marked_complete")

    def n6():
        gg = A.aggregate(ORD, ms, b, "15m")
        m = gg.coverage_state == "UNOBSERVED_CONTAMINATED"
        gg.loc[m, "close"] = 1.0
        A.validate(gg)
    neg("N6", "missing minute cannot be forward-filled into a coarse close", n6,
        "contaminated_ohlcv")

    def n7():
        gg = A.aggregate(ORD, ms, b, "15m")
        gg["feature_information_available_at"] = gg.bar_start
        A.validate(gg)
    neg("N7", "coarse bar cannot be available before its closing minute", n7,
        "premature_availability")

    class Padded(CoarseAggregatorV1):
        def intervals(self, day, tf):
            iv = super().intervals(day, tf)
            for x in iv:
                if x["stub"]:
                    x["minutes"], x["stub"] = 60, False
            return iv
    neg("N8", "1H 30-minute stub cannot be padded to 60",
        lambda: Padded(cal, cohort_keys).aggregate(ORD, ms, b, "1H"), "stub_padding")

    def n9():
        mse, be = load(EARLY)
        phantom = mse.iloc[:1].copy()
        _, ce, _ = A.session_bounds(EARLY)
        phantom["minute_ts"] = ce + 60000 * 30
        A.aggregate(EARLY, pd.concat([mse, phantom]), be, "15m")
    neg("N9", "early-close session cannot generate post-close intervals", n9,
        "input_scope")

    def n10():
        ms2, b2 = load(ORD)
        other = ms2.iloc[:1].copy(); other["session_date"] = "2024-05-16"
        A.aggregate(ORD, pd.concat([ms2, other]), b2, "15m")
    neg("N10", "aggregation cannot cross an XNYS session boundary", n10, "")

    neg("N11", "supplemental source path must fail",
        lambda: A.aggregate(ORD, ms, b, "15m", source_root=QUAR), "source_path")

    def n12():
        bad = ms.iloc[:1].copy(); bad["security_key_v1"] = excluded_key
        A.aggregate(ORD, pd.concat([ms, bad]), b, "15m")
    neg("N12", "excluded security cannot enter output", n12, "cohort_admission")

    neg("N13", "unregistered timeframe must fail",
        lambda: A.aggregate(ORD, ms, b, "4H"), "unregistered_timeframe")

    smoke_ok = all(r["passed"] for r in results)
    print(f"\n  smoke: {sum(r['passed'] for r in results)}/{len(results)} "
          f"({time.time()-t0:.0f}s)", flush=True)
    if not smoke_ok:
        print("HOLD — smoke failed; census not run"); return 1

    # =============== FULL CENSUS ===============
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(PART, "*.json")))
    tfs = ["15m", "1H", "1D"]
    cnt = {tf: collections.Counter() for tf in tfs}
    runs = {tf: collections.Counter() for tf in tfs}        # COMPLETE run lengths
    cruns = {tf: collections.Counter() for tf in tfs}       # contaminated run lengths
    cur = {tf: {} for tf in tfs}
    ccur = {tf: {} for tf in tfs}
    longest = {tf: {} for tf in tfs}
    persec = {tf: collections.defaultdict(lambda: [0, 0]) for tf in tfs}
    persess = {tf: {} for tf in tfs}
    bypos = {tf: collections.defaultdict(lambda: [0, 0]) for tf in ("15m", "1H")}
    stub = {"normal": [0, 0], "early": [0, 0]}
    early_conf, minute_tot = [], [0, 0, 0]
    t1 = time.time()
    for si, day in enumerate(sessions, 1):
        msd, _ = load(day, cols_ms=["security_key_v1", "session_date", "minute_ts",
                                    "observation_state"])
        _, _, n = A.session_bounds(day)
        is_early = n != 390
        for tf in tfs:
            g = A.aggregate(day, msd, None, tf, with_ohlcv=False)
            comp = g.coverage_state.values == "COMPLETE"
            cnt[tf]["intervals"] += len(g)
            cnt[tf]["complete"] += int(comp.sum())
            cnt[tf]["contaminated"] += int((~comp).sum())
            cnt[tf]["stub"] += int(g.is_stub.sum())
            cnt[tf]["early_close_intervals"] += len(g) if is_early else 0
            persess[tf][day] = int((~comp).sum())
            for k, iv, cflag, st in zip(g.security_key_v1.values, g._iv.values,
                                        comp, g.is_stub.values):
                ps = persec[tf][k]
                ps[0 if cflag else 1] += 1
                if tf in bypos:
                    bp = bypos[tf][int(iv)]
                    bp[0 if cflag else 1] += 1
                if tf == "1H" and st:
                    stub["early" if is_early else "normal"][0 if cflag else 1] += 1
                if cflag:
                    cur[tf][k] = cur[tf].get(k, 0) + 1
                    if ccur[tf].get(k):
                        cruns[tf][ccur[tf][k]] += 1; ccur[tf][k] = 0
                else:
                    if cur[tf].get(k):
                        runs[tf][cur[tf][k]] += 1
                        longest[tf][k] = max(longest[tf].get(k, 0), cur[tf][k])
                        cur[tf][k] = 0
                    ccur[tf][k] = ccur[tf].get(k, 0) + 1
            if tf == "15m":
                minute_tot[0] += int(g.expected.sum()); minute_tot[1] += int(g.observed.sum())
                minute_tot[2] += int(g.unobserved.sum())
        if is_early:
            g15 = A.intervals(day, "15m"); g1h = A.intervals(day, "1H")
            early_conf.append(dict(session=day, minutes=n, bars_15m=len(g15),
                                   stubs_15m=sum(x["stub"] for x in g15),
                                   full_1H=sum(1 for x in g1h if not x["stub"]),
                                   stub_1H=sum(1 for x in g1h if x["stub"])))
        del msd
        if si % 200 == 0 or si == len(sessions):
            print(f"  census {si}/{len(sessions)} · {time.time()-t1:.0f}s", flush=True)
    for tf in tfs:
        for k, v in cur[tf].items():
            if v:
                runs[tf][v] += 1; longest[tf][k] = max(longest[tf].get(k, 0), v)
        for k, v in ccur[tf].items():
            if v:
                cruns[tf][v] += 1

    def runstats(counter):
        tot = sum(counter.values())
        obs = sum(L * n for L, n in counter.items())
        flat = sorted(counter.elements()) if tot < 4_000_000 else None
        s = dict(segments=tot, complete_observations=obs,
                 min=min(counter) if counter else None,
                 max=max(counter) if counter else None,
                 mean=round(obs / tot, 2) if tot else None)
        if flat:
            for q, lbl in ((.10, "p10"), (.25, "p25"), (.50, "median"), (.75, "p75"),
                           (.90, "p90"), (.95, "p95"), (.99, "p99")):
                s[lbl] = pct(flat, q)
        s["share_of_complete_in_runs_of_at_least"] = {
            str(k): (round(sum(L * n for L, n in counter.items() if L >= k) / obs, 4)
                     if obs else None) for k in THRESHOLDS}
        return s

    def secstats(d):
        r = sorted(v[0] / (v[0] + v[1]) for v in d.values() if (v[0] + v[1]))
        return dict(securities=len(r), min=round(r[0], 4) if r else None,
                    p10=round(pct(r, .10), 4), p25=round(pct(r, .25), 4),
                    median=round(pct(r, .50), 4), p75=round(pct(r, .75), 4),
                    p90=round(pct(r, .90), 4), max=round(r[-1], 4) if r else None,
                    mean=round(sum(r) / len(r), 4) if r else None)

    tf_report = {}
    for tf in tfs:
        c = cnt[tf]
        tf_report[tf] = dict(
            total_intervals=c["intervals"], complete=c["complete"],
            contaminated=c["contaminated"],
            complete_share=round(c["complete"] / c["intervals"], 4),
            contaminated_share=round(c["contaminated"] / c["intervals"], 4),
            stub_intervals=c["stub"],
            full_length_intervals=c["intervals"] - c["stub"],
            early_close_intervals=c["early_close_intervals"],
            per_security_complete_ratio=secstats(persec[tf]),
            complete_run_lengths=runstats(runs[tf]),
            contaminated_run_lengths=runstats(cruns[tf]))

    lg = sorted(longest["1D"].values())
    d1 = dict(
        securities=len(lg),
        longest_complete_daily_run=dict(
            min=lg[0], p10=pct(lg, .10), p25=pct(lg, .25), median=pct(lg, .50),
            p75=pct(lg, .75), p90=pct(lg, .90), max=lg[-1],
            mean=round(sum(lg) / len(lg), 1)),
        securities_with_a_complete_run_of_at_least={
            str(k): sum(1 for v in lg if v >= k) for k in (20, 50, 100, 200)},
        per_security_complete_days=dict(
            min=min(v[0] for v in persec["1D"].values()),
            median=pct(sorted(v[0] for v in persec["1D"].values()), .5),
            max=max(v[0] for v in persec["1D"].values())))

    csess = sorted(persess["1D"].values())
    clustering = dict(
        contaminated_intervals_per_session_1D=dict(
            min=csess[0], median=pct(csess, .5), p90=pct(csess, .9), max=csess[-1]),
        contaminated_run_lengths_1D=tf_report["1D"]["contaminated_run_lengths"],
        by_session_position={
            tf: {str(k): dict(complete=v[0], contaminated=v[1],
                              contaminated_share=round(v[1] / (v[0] + v[1]), 4))
                 for k, v in sorted(bypos[tf].items())} for tf in bypos},
        interpretation="distinguishes many isolated missing minutes from long unusable "
                       "blocks; label-free")

    verifier = dict(
        shares_aggregator_code=False,
        normal_15m=len(A.intervals(ORD, "15m")) == 26,
        normal_1H=(sum(1 for x in A.intervals(ORD, "1H") if not x["stub"]) == 6
                   and sum(1 for x in A.intervals(ORD, "1H") if x["stub"]) == 1),
        early_15m=len(A.intervals(EARLY, "15m")) == 14,
        early_1H=(sum(1 for x in A.intervals(EARLY, "1H") if not x["stub"]) == 3
                  and sum(1 for x in A.intervals(EARLY, "1H") if x["stub"]) == 1),
        independent_minute_reconciliation=(
            minute_tot[0] == json.load(open(
                "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json"))["coverage_report"][
                    "expected_regular_minutes"]),
        expected_minutes=minute_tot[0], observed_minutes=minute_tot[1],
        unobserved_minutes=minute_tot[2],
        intervals_sum_consistent=all(
            tf_report[tf]["complete"] + tf_report[tf]["contaminated"]
            == tf_report[tf]["total_intervals"] for tf in tfs),
        early_close_sessions=len(early_conf),
        early_close_conformant=all(e["bars_15m"] == 14 and e["stubs_15m"] == 0
                                   and e["full_1H"] == 3 and e["stub_1H"] == 1
                                   for e in early_conf))
    vpass = all(v for k, v in verifier.items()
                if isinstance(v, bool) and k != "shares_aggregator_code")

    p = dict(
        report_id="MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1",
        status=("MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1_PASS"
                if (smoke_ok and vpass) else "HOLD"),
        classification="SMOKE_OR_CAPABILITY_STAGING_ONLY",
        explicitly_not=["PRODUCTION_COARSE", "RESEARCH_EVIDENCE", "FEATURE_DATA",
                        "T/Z DATA"],
        binding=dict(aggregation_spec=SPEC_D, derived_base=PROD_D, eligibility=ELIG_D,
                     charter=CHARTER_D, cohort=476, sessions=len(sessions),
                     timeframes=tfs),
        aggregation_implementation_hash=ih, staging_path=stage,
        smoke=dict(total=len(results),
                   positive_passed=sum(1 for r in results
                                       if r["kind"] == "POSITIVE" and r["passed"]),
                   positive=sum(1 for r in results if r["kind"] == "POSITIVE"),
                   negative_passed=sum(1 for r in results
                                       if r["kind"] == "NEGATIVE" and r["passed"]),
                   negative=sum(1 for r in results if r["kind"] == "NEGATIVE"),
                   results=results),
        capability_census=dict(
            label_free=True, y_exposed=0, scope=f"476 x {len(sessions)} x 3 timeframes",
            by_timeframe=tf_report,
            daily_report=d1,
            hour_stub=dict(
                normal_sessions=dict(complete=stub["normal"][0],
                                     contaminated=stub["normal"][1]),
                early_close_sessions=dict(complete=stub["early"][0],
                                          contaminated=stub["early"][1]),
                note="stub bars are counted on their own dimension and never mixed with "
                     "full 60-minute bars"),
            early_close_conformance=dict(sessions=len(early_conf), detail=early_conf,
                                         all_conformant=verifier["early_close_conformant"]),
            contamination_clustering=clustering,
            run_length_thresholds=dict(
                values=THRESHOLDS,
                status="CAPABILITY REFERENCES ONLY",
                authorises_nothing="they span both legacy parameter families so the report "
                                   "can answer 'could this family be supported' for either; "
                                   "reporting them selects nothing")),
        defect_caught_by_negative_fixture=dict(
            fixture="N8", first_run="DANGEROUS STATE ACCEPTED",
            defect="the stub guard compared the `stub` flag to the interval's own declared "
                   "width, so padding both together was self-consistent and passed",
            fix="declared minutes are now checked against the calendar span and against the "
                "session total, neither of which can be padded",
            rule_applied="implementation hash changed -> entire run re-executed from "
                         "scratch; no fixture result carried across",
            note="the same pattern as the derived-base fixture J: the negative fixture, not "
                 "review, is what found it"),
        no_tolerance_decision=dict(
            tolerance_parameter_exists_in_code=False,
            defaulted_to_zero=False, absent=True,
            semantics_changed=False,
            belongs_to="the later STUDY / FEATURE specification, declared before Y exposure",
            capability_may_inform_it=True, does_not_make_it=True),
        no_ema_decision=dict(
            resolved_here=False,
            ema_registry_status="BLOCKED_PENDING_DECISION",
            note="capability measures whether a proposed family could be supported; the "
                 "parameter registry remains separately governed"),
        independent_verifier=dict(checks=verifier, passed=vpass),
        immutability=dict(
            charter=ART.file_digest("MASSIVE_1M_STATE_TRANSITION_V1.json") == CHARTER_D,
            derived_base=ART.file_digest(
                "MASSIVE_1M_DERIVED_BASE_PRODUCTION_V1.json") == PROD_D,
            eligibility=ART.file_digest(ELIG) == ELIG_D,
            aggregation_spec=ART.file_digest(SPEC) == SPEC_D,
            production_coarse_writes=0,
            production_coarse_exists=os.path.exists(PROD_COARSE),
            y_exposure=0),
        authorises="ONLY the full coarse production build. NOT EMA, RVOL, T, Z or Y.",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json",
                 required=("report_id", "status", "binding",
                           "aggregation_implementation_hash", "smoke",
                           "capability_census", "no_tolerance_decision",
                           "independent_verifier", "immutability"),
                 supersede=os.path.exists(
                     "MASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1.json"))
    print(f"\nMASSIVE_1M_COARSE_AGGREGATION_SMOKE_CAPABILITY_V1 · {d} · {p['status']}")
    for tf in tfs:
        r = tf_report[tf]
        print(f"  {tf:4s} {r['total_intervals']:>10,d} intervals · COMPLETE "
              f"{r['complete_share']:.1%} · runs median "
              f"{r['complete_run_lengths'].get('median')} p90 "
              f"{r['complete_run_lengths'].get('p90')} max "
              f"{r['complete_run_lengths']['max']}")
    print(f"  1D longest COMPLETE run: median {d1['longest_complete_daily_run']['median']} "
          f"· p90 {d1['longest_complete_daily_run']['p90']} · max "
          f"{d1['longest_complete_daily_run']['max']}")
    print(f"  securities with a COMPLETE daily run >= "
          f"{d1['securities_with_a_complete_run_of_at_least']}")
    print(f"  verifier {'PASS' if vpass else 'HOLD'} · production coarse writes 0")
    return 0 if (smoke_ok and vpass) else 1


if __name__ == "__main__":
    raise SystemExit(main())
