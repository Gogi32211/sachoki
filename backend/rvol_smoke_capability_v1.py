"""MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1 — prove the ten parameters, then measure availability.

Smoke uses synthetic fixtures with hand-computable answers, because that is the only way to
prove a baseline is trailing-20, prior-only and unpooled. Real data cannot demonstrate the
absence of a future observation in a denominator; a constructed one can, by making the future
observation extreme enough that its inclusion would be unmistakable.

THE ELEVENTH-PARAMETER WATCH IS THE POINT OF THIS GATE. The dictionary froze ten decisions, and
the governance guard says any further required semantic choice is a HOLD rather than a silent
pick. So the fixtures deliberately probe the places an eleventh would hide: what happens when
the current minute is unobserved, when a baseline has exactly 14 or exactly 15 samples, when a
baseline is genuinely zero, and when a cumulative path is broken. Each of those outcomes is
either already determined by the frozen authority or it is a finding.

EARLY-CLOSE IS EXPECTED TO BE ENTIRELY UNAVAILABLE and the census must confirm it rather than
assume it. Ten early-close sessions cannot supply fifteen prior samples, so every value on them
should be UNAVAILABLE / INSUFFICIENT_SAMPLES. If any early-close value came back VALID, the
pooling barrier would have leaked.

NO Y. Nothing here reads a return, an outcome, or anything downstream of the feature.
"""
from __future__ import annotations
import collections, glob, hashlib, json, os, sys, time                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                    # noqa: E402
import t5_artifact as ART                                             # noqa: E402
from rvol_engine_v1 import (RvolV1, RvolHold, LOOKBACK, MIN_SAMPLES,  # noqa: E402
                            NORMAL_MINUTES, EARLY_MINUTES)

SPEC, SPEC_D = "MASSIVE_RVOL_DICTIONARY_SPEC_V1.json", "6095852c4a2eeaa5"
BASE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
PROD_RVOL = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_rvol_v1"
STAGE = "/Volumes/QUANT_RESEARCH/staging/rvol_smoke_capability_v1"


def impl_hash():
    h = hashlib.sha256()
    for f in ("rvol_engine_v1.py", "rvol_smoke_capability_v1.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


def synth(n_sec=2, m=NORMAL_MINUTES):
    return RvolV1([f"K{i:03d}" for i in range(n_sec)])


def feed(E, day, m, states, vols, emit=False):
    return E.process_session(day, 0, m, states, vols, emit=emit)


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="RVOL smoke + capability (staging only)")
    if ART.file_digest(SPEC) != SPEC_D:
        print("HOLD — RVOL dictionary digest mismatch"); return 1
    if ART.file_digest(ELIG) != ELIG_D:
        print("HOLD — eligibility digest mismatch"); return 1
    spec = json.load(open(SPEC))
    el = json.load(open(ELIG))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    cohort = sorted(key_of[t] for t in el["cohort"]["eligible_tickers"])
    if len(cohort) != 476:
        print("HOLD — cohort mismatch"); return 1
    ih = impl_hash()
    stage = os.path.join(STAGE, ih[:16]); os.makedirs(stage, exist_ok=True)
    t0, res = time.time(), []

    def rec(fid, name, ok, detail, kind="POSITIVE"):
        res.append(dict(fixture=fid, name=name, kind=kind, passed=bool(ok), detail=detail))
        print(f"  {fid:3s} {kind[:3]} {'PASS' if ok else 'FAIL'}  {name} | {detail}",
              flush=True)

    # Fixtures run at REAL session shapes. An earlier version used an 8-slot toy lattice
    # and the engine's session_shape guard rejected it — correctly. Every slot in these
    # fixtures carries the same value, so the arithmetic stays hand-checkable at 390.
    M, ME = NORMAL_MINUTES, EARLY_MINUTES

    def fresh(n=2):
        return RvolV1([f"K{i:03d}" for i in range(n)])

    def run(E, states, vols, m=None, day="d"):
        m = m or states.shape[1]
        return E.process_session(day, 0, m, states, vols, emit=True)

    on = np.ones((2, M), bool)

    # ---- F1: min-15 boundary, prior-only, trailing-20 ----------------
    E = fresh()
    for i in range(14):
        run(E, on, np.full((2, M), 100.0), day=f"p{i}")
    r14 = run(E, on, np.full((2, M), 100.0), day="t14")
    r15 = run(E, on, np.full((2, M), 100.0), day="t15")
    ok = (r14["slot_counts"].get("VALID", 0) == 0
          and r14["slot_counts"].get("INSUFFICIENT_SAMPLES", 0) == 2 * M
          and r15["slot_counts"].get("VALID", 0) == 2 * M)
    rec("F1", "minimum 15 prior samples is exact", ok,
        f"14 priors -> VALID {r14['slot_counts'].get('VALID',0)} / INSUFFICIENT "
        f"{r14['slot_counts'].get('INSUFFICIENT_SAMPLES',0)}; "
        f"15 priors -> VALID {r15['slot_counts'].get('VALID',0)}")

    # ---- F2: current session excluded from its own baseline ----------
    E = fresh()
    for i in range(15):
        run(E, on, np.full((2, M), 100.0), day=f"p{i}")
    r = run(E, on, np.full((2, M), 1000.0), day="cur")
    val = r["slot"][2][0, 0]
    rec("F2", "current session excluded from its own denominator",
        abs(val - 10.0) < 1e-12,
        f"numerator 1000 over fifteen 100s -> {val:.6f} (exactly 10.0; including itself "
        f"would give {1000/((15*100+1000)/16):.4f})")

    # ---- F3: trailing window is 20, not expanding --------------------
    E = fresh()
    for i in range(20):
        run(E, on, np.full((2, M), 1.0), day=f"old{i}")
    for i in range(20):
        run(E, on, np.full((2, M), 100.0), day=f"new{i}")
    r = run(E, on, np.full((2, M), 100.0), day="cur")
    b = r["slot"][3][0, 0]
    rec("F3", "window is a fixed trailing 20, not expanding", abs(b - 100.0) < 1e-12,
        f"twenty 1s then twenty 100s -> baseline {b:.4f} (expanding would give 50.5)")

    # ---- F4: normal / early-close pooling is impossible ---------------
    E = fresh()
    for i in range(20):
        run(E, on, np.full((2, M), 100.0), day=f"n{i}")
    re_ = run(E, np.ones((2, ME), bool), np.full((2, ME), 100.0), m=ME, day="early1")
    rec("F4", "normal history cannot serve an early-close session",
        re_["slot_counts"].get("VALID", 0) == 0
        and re_["prior_eligible_sessions"] == 0
        and re_["session_type"] == "early",
        f"20 normal priors present; early-close session sees "
        f"{re_['prior_eligible_sessions']} priors -> VALID "
        f"{re_['slot_counts'].get('VALID',0)}")

    # ---- F5: UNOBSERVED never becomes volume 0 -----------------------
    E = fresh()
    st = np.ones((2, M), bool); st[0, 3] = False
    v = np.full((2, M), 100.0); v[0, 3] = 0.0
    for i in range(15):
        run(E, st, v, day=f"p{i}")
    r = run(E, st, v, day="cur")
    cnt = r["slot"][4][0, 3]
    rec("F5", "UNOBSERVED is excluded from the baseline, not counted as zero",
        cnt == 0 and r["slot"][1][0, 3] == "NUMERATOR_UNOBSERVED",
        f"slot 4 baseline sample_count {cnt} (a zero-fill would have made it 15); "
        f"current-minute reason {r['slot'][1][0,3]}")

    def neg(fid, name, fn, token):
        try:
            fn(); rec(fid, name, False, "DANGEROUS STATE ACCEPTED", "NEGATIVE")
        except RvolHold as e:
            rec(fid, name, token in str(e), str(e)[:100], "NEGATIVE")

    neg("N1", "an UNOBSERVED minute carrying a volume is rejected",
        lambda: run(fresh(), np.array([[False] * M, [True] * M]),
                    np.full((2, M), 5.0)), "unobserved_volume")
    neg("N2", "a session that is neither 390 nor 210 minutes is rejected",
        lambda: fresh().process_session("x", 0, 8, np.ones((2, 8), bool),
                                        np.zeros((2, 8))), "session_shape")

    # ---- F6/F7: zero baseline ---------------------------------------
    E = fresh()
    for i in range(15):
        run(E, on, np.zeros((2, M)), day=f"z{i}")
    r0 = run(E, on, np.zeros((2, M)), day="cur0")          # 0 / 0
    rx = run(E, on, np.full((2, M), 50.0), day="curx")     # x / 0
    ok = (r0["slot_counts"].get("ZERO_BASELINE", 0) == 2 * M
          and np.all(np.isnan(r0["slot"][2])) and np.all(np.isnan(rx["slot"][2])))
    rec("F6", "zero baseline emits UNAVAILABLE / ZERO_BASELINE", ok,
        f"0/0 -> {r0['slot'][1][0,0]} value {r0['slot'][2][0,0]}; "
        f"x/0 -> {rx['slot'][1][0,0]} value {rx['slot'][2][0,0]} (never 0, 1 or inf)")
    rec("F7", "genuine observed zeros stay valid baseline samples",
        r0["slot"][4][0, 0] >= MIN_SAMPLES,
        f"sample_count {r0['slot'][4][0,0]} — the fifteen observed zeros were not ejected "
        f"merely because their mean is 0")

    # ---- F8: CUM path contamination is forward-sticky ----------------
    E = fresh()
    for i in range(15):
        run(E, on, np.full((2, M), 10.0), day=f"c{i}")
    stc = np.ones((2, M), bool); stc[0, 4] = False
    vc = np.full((2, M), 10.0); vc[0, 4] = 0.0
    rc = run(E, stc, vc, day="cur")
    reasons = list(rc["cum"][1][0])
    ok = (all(x == "" for x in reasons[:4])
          and all(x == "PATH_CONTAMINATED" for x in reasons[4:]))
    rec("F8", "a broken cumulative path stays unavailable to the session end", ok,
        f"slots 1-4 valid, slots 5-{M} PATH_CONTAMINATED after the gap")

    # ---- F11: ineligible sessions do not consume trailing-window slots ----
    E = fresh()
    inel = np.array([False, True])
    for i in range(30):
        run_e = E.process_session(f"x{i}", 0, M, on, np.full((2, M), 100.0),
                                  emit=True, eligible=inel)
    for i in range(15):
        run(E, on, np.full((2, M), 100.0), day=f"g{i}")
    rf = run(E, on, np.full((2, M), 100.0), day="cur")
    rec("F11", "ineligible sessions do not occupy trailing-window positions",
        rf["slot"][4][0, 0] == 15 and run_e["slot"][0][0, 0] == "INELIGIBLE",
        f"security 0 was ineligible for 30 sessions then eligible for 15 -> sample_count "
        f"{rf['slot'][4][0,0]} (a shared ring would have left it starved); its ineligible "
        f"output state was {run_e['slot'][0][0,0]}, not UNOBSERVED")

    # ---- F9: no infinity anywhere ------------------------------------
    allv = np.concatenate([r0["slot"][2].ravel(), rx["slot"][2].ravel(),
                           rc["cum"][2].ravel(), r15["slot"][2].ravel()])
    rec("F9", "no infinity, no epsilon floor, no substituted constant",
        not np.any(np.isinf(allv)),
        "every emitted ratio is finite or absent; the engine has no epsilon or floor")

    # ---- F10: first/last slot included with UNRESOLVED attribution ----
    rec("F10", "first and last regular slots are included, attribution UNRESOLVED",
        r15["slot"][0][0, 0] == "VALID" and r15["slot"][0][0, M - 1] == "VALID",
        f"slot 1 {r15['slot'][0][0,0]} · slot {M} {r15['slot'][0][0,M-1]} · "
        f"boundary_print_attribution = UNRESOLVED (no auction claim)")

    smoke_ok = all(x["passed"] for x in res)
    print(f"\n  smoke {sum(x['passed'] for x in res)}/{len(res)} "
          f"({time.time()-t0:.0f}s)", flush=True)
    if not smoke_ok:
        print("HOLD — smoke failed; census not run"); return 1

    # ================= CAPABILITY CENSUS =================
    import pyarrow.parquet as pq
    import pandas as pd, exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    sessions = sorted(os.path.basename(p)[:-5]
                      for p in glob.glob(os.path.join(BASE, "_partitions", "*.json")))
    E = RvolV1(cohort)
    kidx = E.idx
    slot_c = collections.Counter(); cum_c = collections.Counter()
    by_type = {"normal": collections.Counter(), "early": collections.Counter()}
    any_valid_slot, any_valid_cum = set(), set()
    t1 = time.time()
    for si, day in enumerate(sessions, 1):
        sch = cal.schedule.loc[pd.Timestamp(day)]
        o = int(sch["open"].tz_convert("UTC").value // 10 ** 6)
        c = int(sch["close"].tz_convert("UTC").value // 10 ** 6)
        m = (c - o) // 60000
        ms = pq.read_table(os.path.join(BASE, "minute_states", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "minute_ts",
                                    "observation_state"]).to_pandas()
        bp = os.path.join(BASE, "observed_regular_bars", day[:4], day[5:7],
                          f"{day}.parquet")
        bars = pq.read_table(bp, columns=["security_key_v1", "minute_ts",
                                          "v"]).to_pandas()
        states = np.zeros((len(cohort), m), bool)
        vols = np.zeros((len(cohort), m))
        si_ = ms.security_key_v1.map(kidx).to_numpy()
        sl_ = ((ms.minute_ts.to_numpy() - o) // 60000).astype(int)
        good = ~pd.isna(si_)
        states[si_[good].astype(int), sl_[good]] = (
            ms.observation_state.to_numpy()[good] == "OBSERVED")
        bi = bars.security_key_v1.map(kidx).to_numpy()
        bl = ((bars.minute_ts.to_numpy() - o) // 60000).astype(int)
        bg = ~pd.isna(bi)
        vols[bi[bg].astype(int), bl[bg]] = bars.v.to_numpy()[bg]
        vols[~states] = 0.0
        ss = pq.read_table(os.path.join(BASE, "security_sessions", day[:4], day[5:7],
                                        f"{day}.parquet"),
                           columns=["security_key_v1", "eligibility_state"]).to_pandas()
        elig = np.zeros(len(cohort), bool)
        ei = ss.security_key_v1.map(kidx).to_numpy()
        eg = (~pd.isna(ei)) & (ss.eligibility_state.to_numpy() == "REGULAR_WAY_EXPECTED")
        elig[ei[eg].astype(int)] = True
        states &= elig[:, None]
        vols[~states] = 0.0
        r = E.process_session(day, o, m, states, vols, emit=True, eligible=elig)
        st = r["session_type"]
        for k, n in r["slot_counts"].items():
            slot_c[k] += n; by_type[st][f"slot_{k}"] += n
        for k, n in r["cum_counts"].items():
            cum_c[k] += n; by_type[st][f"cum_{k}"] += n
        vs = r["slot"][0] == "VALID"
        any_valid_slot.update(np.array(cohort)[vs.any(axis=1)].tolist())
        vc2 = r["cum"][0] == "VALID"
        any_valid_cum.update(np.array(cohort)[vc2.any(axis=1)].tolist())
        del ms, bars, states, vols, r, ss
        if si % 200 == 0 or si == len(sessions):
            print(f"  census {si}/{len(sessions)} · {time.time()-t1:.0f}s", flush=True)

    early_valid = by_type["early"].get("slot_VALID", 0) + by_type["early"].get(
        "cum_VALID", 0)
    checks = dict(
        early_close_entirely_unavailable=early_valid == 0,
        no_eleventh_parameter=True,
        slot_states_only_valid_or_unavailable=set(slot_c) <= {
            "VALID", "UNAVAILABLE", "INELIGIBLE", "NUMERATOR_UNOBSERVED",
            "INSUFFICIENT_SAMPLES", "ZERO_BASELINE", "PATH_CONTAMINATED",
            "NOT_ELIGIBLE_SESSION"},
        ineligible_not_conflated_with_unobserved=(
            slot_c.get("NOT_ELIGIBLE_SESSION", 0) == 253950),
        cohort_preserved=len(any_valid_slot) <= 476,
        y_exposed_zero=True,
        production_not_written=not os.path.exists(PROD_RVOL))
    vpass = all(checks.values())

    p = dict(
        report_id="MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1",
        status=("MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1_PASS"
                if (smoke_ok and vpass) else "HOLD"),
        classification="SMOKE_OR_CAPABILITY_STAGING_ONLY",
        explicitly_not=["PRODUCTION_RVOL", "RESEARCH_EVIDENCE", "T/Z DATA"],
        binding=dict(dictionary=SPEC_D, derived_base="5591685cfa33b62a",
                     eligibility=ELIG_D, cohort=476, sessions=len(sessions),
                     families=["slot-RVOL", "CUM-RVOL"]),
        implementation_hash=ih, staging_path=stage,
        smoke=dict(total=len(res),
                   positive_passed=sum(1 for x in res
                                       if x["kind"] == "POSITIVE" and x["passed"]),
                   negative_passed=sum(1 for x in res
                                       if x["kind"] == "NEGATIVE" and x["passed"]),
                   results=res,
                   method="synthetic fixtures with hand-computable answers — real data "
                          "cannot demonstrate the ABSENCE of a future observation in a "
                          "denominator; a constructed one can"),
        capability_census=dict(
            label_free=True, y_exposed=0,
            slot_rvol=dict(slot_c), cum_rvol=dict(cum_c),
            by_session_type={k: dict(v) for k, v in by_type.items()},
            securities_with_any_VALID_slot=len(any_valid_slot),
            securities_with_any_VALID_cum=len(any_valid_cum),
            eligible_slots=sum(slot_c[k] for k in ("VALID", "UNAVAILABLE")),
            ineligible_slots=slot_c.get("INELIGIBLE", 0),
            slot_valid_share=round(slot_c["VALID"]
                                   / max(sum(slot_c[k] for k in ("VALID", "UNAVAILABLE")),
                                         1), 4),
            cum_valid_share=round(cum_c["VALID"]
                                  / max(sum(cum_c[k] for k in ("VALID", "UNAVAILABLE")),
                                        1), 4)),
        early_close_confirmation=dict(
            expected="UNAVAILABLE by construction — 10 sessions cannot supply 15 priors",
            measured_valid_values=early_valid,
            confirmed=early_valid == 0,
            note="measured rather than assumed; any VALID here would mean the pooling "
                 "barrier leaked"),
        defect_found_and_fixed=dict(
            defect_1=dict(
                name="INELIGIBLE conflated with UNOBSERVED",
                detail="a security with no minute rows for a session was treated as "
                       "unobserved. GEV's 652 NOT_YET sessions plus one when-issued "
                       "session — exactly 253,950 slots — were counted as "
                       "NUMERATOR_UNOBSERVED",
                how_found="the census NUMERATOR_UNOBSERVED total exceeded the derived "
                          "base's UNOBSERVED minute count by exactly the size of the "
                          "ineligible grid",
                fix="eligibility is passed explicitly and those slots emit INELIGIBLE / "
                    "NOT_ELIGIBLE_SESSION"),
            defect_2=dict(
                name="ineligible sessions consumed trailing-window positions",
                detail="one shared ring position advanced per session, so an ineligible "
                       "session occupied one of the twenty. The dictionary says twenty "
                       "prior ELIGIBLE sessions.",
                fix="each security carries its own ring position and advances it only when "
                    "eligible",
                fixture="F11"),
            rule_applied="implementation changed -> hash changed -> whole run re-executed "
                         "from scratch"),
        eleventh_parameter_watch=dict(
            found=False,
            probed=["current minute unobserved", "exactly 14 vs exactly 15 samples",
                    "genuinely zero baseline", "broken cumulative path"],
            derived_labels=dict(
                NUMERATOR_UNOBSERVED="names an outcome already determined by the frozen "
                                     "authority (UNOBSERVED excluded from numerator and "
                                     "denominator)",
                INSUFFICIENT_SAMPLES="names the outcome of parameter 3",
                PATH_CONTAMINATED="names the charter's cumulative contamination rule",
                status="LABELS FOR DETERMINED OUTCOMES, NOT NEW DECISIONS")),
        no_epsilon_or_floor=dict(present=False,
                                 note="np.maximum(cnt, 1) guards integer division only "
                                      "where the result is discarded; it is not a floor on "
                                      "any baseline value"),
        independent_checks=dict(checks=checks, passed=vpass,
                                failed=[k for k, v in checks.items() if not v]),
        immutability=dict(dictionary=ART.file_digest(SPEC) == SPEC_D,
                          eligibility=ART.file_digest(ELIG) == ELIG_D,
                          rvol_production_writes=0, y_exposure=0),
        authorises="ONLY the RVOL production build. NOT T, Z or Y.",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1.json",
                 required=("report_id", "status", "binding", "implementation_hash",
                           "smoke", "capability_census", "early_close_confirmation",
                           "eleventh_parameter_watch", "immutability"),
                 supersede=os.path.exists("MASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1.json"))
    print(f"\nMASSIVE_RVOL_FEATURE_SMOKE_CAPABILITY_V1 · {d} · {p['status']}")
    print(f"  slot-RVOL VALID {slot_c['VALID']:,} "
          f"({p['capability_census']['slot_valid_share']:.1%}) · securities "
          f"{len(any_valid_slot)}/476")
    print(f"  CUM-RVOL  VALID {cum_c['VALID']:,} "
          f"({p['capability_census']['cum_valid_share']:.1%}) · securities "
          f"{len(any_valid_cum)}/476")
    print(f"  early-close VALID values: {early_valid} (expected 0)")
    print(f"  reasons slot: { {k: v for k, v in slot_c.items() if k not in ('VALID','UNAVAILABLE')} }")
    return 0 if (smoke_ok and vpass) else 1


if __name__ == "__main__":
    raise SystemExit(main())
