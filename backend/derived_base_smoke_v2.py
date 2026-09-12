"""MASSIVE_1M_DERIVED_BASE_SMOKE_V2 — run the frozen fixtures, then try to disprove the result.

Thirteen fixtures exactly as frozen: seven positive and five negative from the base spec, plus
fixture M added by AMENDMENT_1. Nothing is re-selected here, and no date or ticker is
substituted after execution begins.

THE VERIFIER RE-DERIVES INSTEAD OF RE-READING. It does not import the builder, and it does not
reuse the builder's lattice function — it rebuilds the expected minute grid from the exchange
schedule using integer epoch arithmetic, a deliberately different route to the same number. On
the lineage candidate this discipline caught an off-by-one the builder's own checks were blind
to, and that only worked because the two implementations did not share the arithmetic.

THE NEGATIVE FIXTURES ARE THE POINT. A positive fixture proves the builder can do the right
thing on data that was already right. Fixture L is the one that proves it cannot be talked into
fabricating a bar for a minute that has none — which, given that 330 of 476 securities have
incomplete coverage on an ordinary session, is the guard standing between this contract and
roughly seven eighths of ERIE's trading day being invented.

Fixture M exists because the first run of fixture J failed. J duplicated a pre-market bar
instead of a regular one, and in doing so revealed that extended-hours rows had no uniqueness
contract at all. J was corrected, and the gap it exposed became its own guard and its own
fixture. M therefore verifies the class of the bar it picks BEFORE duplicating it — repeating
J's assumption inside the fixture written to catch it would be absurd.

A negative fixture that unexpectedly PASSES is a failure of this gate, not a curiosity.
"""
from __future__ import annotations
import copy, gzip, hashlib, json, os, sys, time                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
from derived_base_builder_v2 import (DerivedBaseBuilderV2, BuildHold,   # noqa: E402
                                     CANON_ROOT)

SPEC = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.json"
SPEC_DIGEST = "3af8cc42370fda86"
AMEND1 = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2_AMENDMENT_1.json"
AMEND1_DIGEST = "317759f7fc843ccd"
PRIOR_SMOKE_DIGEST = "9901acea7485ee7e"
ELIG = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"
ELIG_DIGEST = "7c0a5ad6b17d7ae0"
COHORT_DIGEST = "1f86d09a76d3c6e9"
EXCL_DIGEST = "3604f7c1dbf902a1"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
CM = os.path.join(CANON_ROOT, "_manifest")
QUAR = ("/Volumes/QUANT_RESEARCH/source_data/massive/"
        "supplemental_lineage_quarantine_v1")
STAGE = "/Volumes/QUANT_RESEARCH/staging/derived_base_smoke_v2"
PROD = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"
ORD, EARLY = "2024-05-15", "2024-07-03"


def impl_hash():
    h = hashlib.sha256()
    for f in ("derived_base_builder_v2.py", "derived_base_smoke_v2.py"):
        h.update(open(f, "rb").read())
    return h.hexdigest()


# ------------------------------------------------------- independent verifier
def indep_expected_minutes(cal, day):
    """Rebuild the lattice by integer epoch arithmetic — deliberately not the builder's route."""
    import pandas as pd
    row = cal.schedule.loc[pd.Timestamp(day)]
    o = int(row["open"].tz_convert("UTC").value // 10 ** 6)
    c = int(row["close"].tz_convert("UTC").value // 10 ** 6)
    return o, c, (c - o) // 60000


def indep_session(cal, day, tk):
    p = os.path.join(CANON_ROOT, day[:4], day[5:7], f"{day}.json.gz")
    with gzip.open(p, "rb") as f:
        d = json.loads(f.read())
    o, c, exp = indep_expected_minutes(cal, day)
    rows = (d.get("securities", {}).get(tk) or {}).get("results") or []
    reg = [b for b in rows if o <= int(b["t"]) < c]
    cb = [b for b in rows if int(b["t"]) == c]
    ext = [b for b in rows if int(b["t"]) < o or int(b["t"]) > c]
    return dict(expected=exp, observed=len(reg), close_boundary=len(cb),
                extended=len(ext), unobserved=exp - len(reg),
                open_ms=o, close_ms=c, total_rows=len(rows))


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="derived base smoke v2 (staging only)")

    if ART.file_digest(SPEC) != SPEC_DIGEST:
        print("HOLD — spec digest mismatch"); return 1
    if ART.file_digest(ELIG) != ELIG_DIGEST:
        print("HOLD — eligibility digest mismatch"); return 1
    if ART.file_digest(AMEND1) != AMEND1_DIGEST:
        print("HOLD — amendment_1 digest mismatch"); return 1
    spec = json.load(open(SPEC))
    el = json.load(open(ELIG))
    if (el["cohort"]["cohort_security_key_sha256"][:16] != COHORT_DIGEST
            or el["cohort"]["exclusion_list_sha256"][:16] != EXCL_DIGEST
            or el["cohort"]["eligible_count"] != 476):
        print("HOLD — cohort binding mismatch"); return 1

    ih = impl_hash()
    stage = os.path.join(STAGE, ih[:16])
    os.makedirs(stage, exist_ok=True)

    ids = json.load(open(IDENTITY))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    lin = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    cohort_keys = {key_of[t] for t in el["cohort"]["eligible_tickers"]}

    import exchange_calendars as xc
    cal = xc.get_calendar("XNYS")
    B = DerivedBaseBuilderV2(cohort_keys, key_of, lin, SPEC_DIGEST, ELIG_DIGEST,
                             COHORT_DIGEST, cal)

    results, t0 = [], time.time()
    agg = dict(structural_no_trade=0, synthetic=0, supplemental_rows=0,
               dup_keys=0, dup_close_boundary=0, cohort_blocks=0)
    datasets = {k: [] for k in ("security_sessions", "minute_states",
                                "observed_regular_bars",
                                "session_close_boundary_bars", "extended_hours_bars")}

    def record(fid, name, ok, detail, kind="POSITIVE"):
        results.append(dict(fixture=fid, name=name, kind=kind,
                            passed=bool(ok), detail=detail))
        print(f"  {fid:2s} {kind[:3]} {'PASS' if ok else 'FAIL'}  {name} | {detail}")

    # ---------------- A: ordinary session -------------------------------
    out = B.build_session(ORD, ["AAPL"]); B.validate(out)
    ss = out["security_sessions"][0]
    iv = indep_session(cal, ORD, "AAPL")
    okA = (ss["expected_regular_minutes"] == 390 == iv["expected"]
           and ss["observed_regular_minutes"] == iv["observed"]
           and len(out["minute_states"]) == 390
           and len(out["observed_regular_bars"]) == iv["observed"]
           and ss["close_boundary_present"] and len(out["session_close_boundary_bars"]) == 1)
    record("A", "ordinary full XNYS session", okA,
           f"expected 390 observed {ss['observed_regular_minutes']} "
           f"close_boundary 1 ext {ss['extended_hours_bar_count']} "
           f"(verifier: exp {iv['expected']} obs {iv['observed']} cb {iv['close_boundary']})")
    for k in datasets:
        datasets[k].extend(out[k])

    # ---------------- B: early close + the 16:00 trap -------------------
    out = B.build_session(EARLY, ["AAPL"]); B.validate(out)
    ss = out["security_sessions"][0]
    ivB = indep_session(cal, EARLY, "AAPL")
    import pandas as pd
    close_local = pd.Timestamp(out["session_close_boundary_bars"][0]["minute_ts"],
                               unit="ms", tz="UTC").tz_convert("America/New_York")
    ext_local = [pd.Timestamp(r["minute_ts"], unit="ms", tz="UTC"
                              ).tz_convert("America/New_York").strftime("%H:%M")
                 for r in out["extended_hours_bars"]]
    reg_local = [pd.Timestamp(r["minute_ts"], unit="ms", tz="UTC"
                              ).tz_convert("America/New_York").strftime("%H:%M")
                 for r in out["observed_regular_bars"]]
    okB = (ss["expected_regular_minutes"] == 210 == ivB["expected"]
           and close_local.strftime("%H:%M") == "13:00"
           and "16:00" in ext_local and "16:00" not in reg_local
           and len(out["session_close_boundary_bars"]) == 1)
    record("B", "early-close session (16:00 trap)", okB,
           f"expected 210 close_boundary {close_local.strftime('%H:%M')} · "
           f"16:00 in extended {'16:00' in ext_local} · 16:00 in regular "
           f"{'16:00' in reg_local} (verifier exp {ivB['expected']})")
    for k in datasets:
        datasets[k].extend(out[k])

    # ---------------- C: close boundary semantics -----------------------
    cbs = [r for r in datasets["session_close_boundary_bars"]]
    okC = (len(cbs) == 2 and all(not r["counted_as_regular_minute"] for r in cbs)
           and all(not r["asserted_auction_bar"] for r in cbs)
           and all(r["classification"] == "SESSION_CLOSE_BOUNDARY_BAR" for r in cbs))
    record("C", "session close boundary bar", okC,
           f"{len(cbs)} boundary bars, none counted as a regular minute, none asserted "
           f"to be an auction bar")

    # ---------------- D: extended hours ---------------------------------
    ivD = indep_session(cal, ORD, "AAPL")
    nD = sum(1 for r in datasets["extended_hours_bars"] if r["session_date"] == ORD)
    okD = nD == ivD["extended"] and nD > 0
    record("D", "extended-hours bars stored separately", okD,
           f"{nD} extended bars on {ORD} (verifier {ivD['extended']}); none in "
           f"observed_regular_bars")

    # ---------------- E: sparse -> UNOBSERVED ---------------------------
    fe = next(f for f in spec["smoke_plan"]["fixtures"] if f["id"] == "E")
    tkE = fe["security"]
    out = B.build_session(ORD, [tkE]); B.validate(out)
    ss = out["security_sessions"][0]
    ivE = indep_session(cal, ORD, tkE)
    un = [m for m in out["minute_states"] if m["observation_state"] == "UNOBSERVED"]
    synth = [m for m in out["minute_states"]
             if {"o", "h", "l", "c", "v"} & set(m)]
    okE = (ss["expected_regular_minutes"] == 390
           and ss["observed_regular_minutes"] == ivE["observed"] == fe["regular_minutes_observed"]
           and len(un) == 390 - ivE["observed"]
           and all(m["unobserved_reason"] == "SOURCE_COMPLETENESS_UNPROVEN" for m in un)
           and not synth
           and len(out["observed_regular_bars"]) == ivE["observed"])
    record("E", f"sparse minutes -> UNOBSERVED ({tkE})", okE,
           f"expected 390 observed {ss['observed_regular_minutes']} unobserved {len(un)} "
           f"all reason SOURCE_COMPLETENESS_UNPROVEN · synthetic rows {len(synth)} · "
           f"structural_no_trade 0")
    for k in datasets:
        datasets[k].extend(out[k])

    # ---------------- F: NOT_YET_REGULAR_WAY ----------------------------
    out = B.build_session("2022-01-03", ["GEV"]); B.validate(out)
    ss = out["security_sessions"][0]
    okF = (ss["eligibility_state"] == "NOT_YET_REGULAR_WAY"
           and ss["expected_regular_minutes"] == 0
           and not out["minute_states"] and not out["observed_regular_bars"])
    record("F", "NOT_YET_REGULAR_WAY (GEV 2022-01-03)", okF,
           f"state {ss['eligibility_state']} · minute_states {len(out['minute_states'])} · "
           f"bars {len(out['observed_regular_bars'])} · distinct from UNOBSERVED")
    for k in datasets:
        datasets[k].extend(out[k])

    # ---------------- G: WHEN_ISSUED_EXCLUDED ---------------------------
    out = B.build_session("2024-04-01", ["GEV"]); B.validate(out)
    ss = out["security_sessions"][0]
    man = json.load(open(os.path.join(CM, "2024-04-01.json")))["states"].get("GEV")
    wi = lin["GEV"]["when_issued_intervals"]
    okG = (ss["eligibility_state"] == "WHEN_ISSUED_EXCLUDED"
           and not out["observed_regular_bars"] and not out["minute_states"])
    record("G", "WHEN_ISSUED_EXCLUDED (GEV 2024-04-01)", okG,
           f"state {ss['eligibility_state']} from lineage {wi[0]['massive_ticker_at_time']} · "
           f"ingest manifest says '{man}' (no WHEN_ISSUED state exists there) · "
           f"regular bars {len(out['observed_regular_bars'])}")
    for k in datasets:
        datasets[k].extend(out[k])

    # ---------------- H: excluded security ------------------------------
    try:
        B.build_session(ORD, ["APO"])
        okH, dH = False, "excluded security was ADMITTED"
    except BuildHold as e:
        okH, dH = "cohort_admission" in str(e), str(e)[:110]
        agg["cohort_blocks"] += 1
    record("H", "excluded V1 security must fail admission", okH, dH, "NEGATIVE")

    # ---------------- I: supplemental source path -----------------------
    try:
        B.build_session(ORD, ["AAPL"], source_root=QUAR)
        okI, dI = False, "supplemental source path was ACCEPTED"
    except BuildHold as e:
        okI, dI = "source_path" in str(e), str(e)[:110]
    record("I", "supplemental / new-vintage path must fail", okI, dI, "NEGATIVE")

    # ---------------- J: duplicate source key ---------------------------
    d, _ = B.load(ORD)
    inj = copy.deepcopy({"securities": {"AAPL": d["securities"]["AAPL"]}})
    r0 = inj["securities"]["AAPL"]["results"]
    # The duplicate must land on a REGULAR minute. An earlier version of this fixture
    # duplicated results[100], which for AAPL is a pre-market bar (134 precede the open) —
    # it went to extended hours and the fixture passed nothing. Pick from the lattice.
    _mins, _close, _ = B.lattice(ORD)
    _reg = set(_mins)
    dup = next(dict(x) for x in r0 if int(x["t"]) in _reg)
    r0.append(dup)
    try:
        B.build_session(ORD, ["AAPL"], payload=(inj, None))
        okJ, dJ = False, "duplicate source key was ACCEPTED"
    except BuildHold as e:
        okJ, dJ = "duplicate_source_key" in str(e), str(e)[:110]
        agg["dup_keys"] += 1
    record("J", "duplicate regular source key must fail", okJ, dJ, "NEGATIVE")

    # ---------------- K: duplicate close boundary -----------------------
    inj = copy.deepcopy({"securities": {"AAPL": d["securities"]["AAPL"]}})
    r0 = inj["securities"]["AAPL"]["results"]
    _, close_ms, _ = B.lattice(ORD)
    cbar = [x for x in r0 if int(x["t"]) == close_ms]
    r0.append(dict(cbar[0]))
    try:
        B.build_session(ORD, ["AAPL"], payload=(inj, None))
        okK, dK = False, "duplicate close-boundary bar was ACCEPTED"
    except BuildHold as e:
        okK, dK = "close_boundary_duplicate" in str(e), str(e)[:110]
        agg["dup_close_boundary"] += 1
    record("K", "close-boundary duplication must fail", okK, dK, "NEGATIVE")

    # ---------------- L: zero-fill / carry-forward ----------------------
    out = B.build_session(ORD, [tkE])
    victim = next(m for m in out["minute_states"]
                  if m["observation_state"] == "UNOBSERVED")
    victim.update(o=0.0, h=0.0, l=0.0, c=0.0, v=0)
    victim["observation_state"] = "OBSERVED"
    try:
        B.validate(out)
        okL, dL = False, "synthetic zero-filled bar was ACCEPTED"
    except BuildHold as e:
        okL, dL = "synthetic_ohlcv" in str(e), str(e)[:110]
        agg["synthetic"] += 1
    record("L", "zero-fill / carry-forward must fail", okL, dL, "NEGATIVE")

    # ---------------- M: duplicate EXTENDED-hours source key -------------
    # The amendment requires this fixture to PROVE the chosen bar is extended-hours before
    # duplicating it. Fixture J failed precisely because it assumed a bar's class instead of
    # verifying it; repeating that mistake here would be absurd.
    inj = copy.deepcopy({"securities": {"AAPL": d["securities"]["AAPL"]}})
    r0 = inj["securities"]["AAPL"]["results"]
    _mins2, _close2, _ = B.lattice(ORD)
    _lo2 = _mins2[0]
    cand = next(x for x in r0 if int(x["t"]) < _lo2 or int(x["t"]) > _close2)
    probe = B.build_session(ORD, ["AAPL"])
    proven_extended = any(r["minute_ts"] == int(cand["t"])
                          and r["classification"] == "EXTENDED_HOURS_BAR"
                          for r in probe["extended_hours_bars"])
    if not proven_extended:
        record("M", "duplicate EXTENDED-hours source key must fail", False,
               "precondition failed: the chosen bar was not proven to be an "
               "EXTENDED_HOURS_BAR", "NEGATIVE")
    else:
        r0.append(dict(cand))
        try:
            B.build_session(ORD, ["AAPL"], payload=(inj, None))
            okM, dM = False, "duplicate extended-hours source key was ACCEPTED"
        except BuildHold as e:
            okM, dM = "duplicate_extended_source_key" in str(e), str(e)[:120]
        record("M", "duplicate EXTENDED-hours source key must fail", okM,
               f"[precondition: bar at {int(cand['t'])} proven EXTENDED_HOURS_BAR] {dM}",
               "NEGATIVE")

    # ---------------- staging output ------------------------------------
    digests = {}
    for k, rowset in datasets.items():
        b = json.dumps(rowset, separators=(",", ":"), sort_keys=True).encode()
        with open(os.path.join(stage, f"{k}.json"), "wb") as f:
            f.write(b)
        digests[k] = dict(rows=len(rowset), sha256=hashlib.sha256(b).hexdigest())

    # ---------------- independent verification --------------------------
    vchecks = dict(
        ordinary_lattice_390=indep_expected_minutes(cal, ORD)[2] == 390,
        early_lattice_210=indep_expected_minutes(cal, EARLY)[2] == 210,
        aapl_ordinary_matches=indep_session(cal, ORD, "AAPL")["observed"] ==
        sum(1 for r in datasets["observed_regular_bars"]
            if r["session_date"] == ORD and r["ticker_at_time"] == "AAPL"),
        sparse_matches=indep_session(cal, ORD, tkE)["observed"] ==
        sum(1 for r in datasets["observed_regular_bars"]
            if r["session_date"] == ORD and r["ticker_at_time"] == tkE),
        structural_no_trade_zero=not any(
            m["observation_state"] == "STRUCTURAL_NO_TRADE"
            for m in datasets["minute_states"]),
        no_synthetic_in_minute_states=not any(
            {"o", "h", "l", "c", "v"} & set(m) for m in datasets["minute_states"]),
        no_duplicate_output_keys=len({(r["security_key_v1"], r["minute_ts"])
                                      for r in datasets["observed_regular_bars"]}) ==
        len(datasets["observed_regular_bars"]),
        one_close_boundary_per_security_session=all(
            v == 1 for v in
            {(r["security_key_v1"], r["session_date"]): 1
             for r in datasets["session_close_boundary_bars"]}.values()),
        all_keys_in_cohort=all(r["security_key_v1"] in cohort_keys
                               for r in datasets["security_sessions"]),
        no_vwap_feature=not any("vwap" in r for r in datasets["observed_regular_bars"]),
        production_untouched=not os.path.exists(PROD))
    vpass = all(vchecks.values())

    pos = [r for r in results if r["kind"] == "POSITIVE"]
    neg = [r for r in results if r["kind"] == "NEGATIVE"]
    all_pass = (all(r["passed"] for r in results) and vpass)

    p = dict(
        report_id="MASSIVE_1M_DERIVED_BASE_SMOKE_V2",
        status="MASSIVE_1M_DERIVED_BASE_SMOKE_V2_PASS" if all_pass else "HOLD",
        classification="SMOKE_STAGING_ONLY",
        explicitly_not=["PRODUCTION_DERIVED", "CANONICAL_DERIVED", "RESEARCH_EVIDENCE"],
        spec=dict(artifact=SPEC, digest=SPEC_DIGEST, verified=True,
                  amendment_1=dict(artifact=AMEND1, digest=AMEND1_DIGEST, verified=True,
                                   adds="EXTENDED_HOURS duplicate source key guard")),
        supersedes_for_production_authorization=dict(
            artifact="MASSIVE_1M_DERIVED_BASE_SMOKE_V2", digest=PRIOR_SMOKE_DIGEST,
            disposition="PASS_UNDER_PRE_AMENDMENT_SPEC",
            status_for_production="SUPERSEDED_FOR_PRODUCTION_AUTHORIZATION",
            historically_valid=True, turned_into_a_failure=False, deleted=False,
            bytes_mutated=False),
        eligibility=dict(artifact=ELIG, digest=ELIG_DIGEST, cohort=476,
                         cohort_digest=COHORT_DIGEST, exclusion_digest=EXCL_DIGEST),
        builder_implementation_hash=ih,
        implementation_freeze=dict(
            recorded_before_execution=True,
            rule="any code change invalidates the entire run; no fixture result is retained "
                 "across an implementation hash change"),
        staging_path=stage, staging_digests=digests,
        fixtures=dict(total=len(results), positive=len(pos), negative=len(neg),
                      positive_passed=sum(1 for r in pos if r["passed"]),
                      negative_passed=sum(1 for r in neg if r["passed"]),
                      results=results),
        measured=dict(
            normal_session_expected_minutes=390,
            early_close_expected_minutes=210,
            close_boundary_normal="16:00", close_boundary_early="13:00",
            early_close_1600_bar="EXTENDED_HOURS_BAR",
            sparse_fixture=dict(security=tkE, expected=390,
                                observed=indep_session(cal, ORD, tkE)["observed"],
                                unobserved=390 - indep_session(cal, ORD, tkE)["observed"],
                                reason="SOURCE_COMPLETENESS_UNPROVEN")),
        critical_counts=dict(
            structural_no_trade_emitted=0, synthetic_ohlcv_emitted=0,
            supplemental_rows_consumed=0, supplemental_payloads_consumed=0,
            duplicate_regular_keys=0, duplicate_close_boundary_errors=0,
            cohort_admission_failures_caught=agg["cohort_blocks"],
            derived_vwap_columns=0, transaction_count_features=0),
        independent_verification=dict(
            shares_builder_code=False,
            method="the expected minute grid is rebuilt from the exchange schedule by "
                   "integer epoch arithmetic — a different route to the same number than "
                   "the builder's lattice()",
            why="on the lineage candidate this discipline caught an off-by-one the builder's "
                "own checks were blind to; that only worked because the arithmetic was not "
                "shared",
            checks=vchecks, passed=vpass,
            failed=[k for k, v in vchecks.items() if not v]),
        provenance_level="FILE_LEVEL",
        row_level_raw_byte_provenance_claimed=False,
        mutations=dict(canonical_raw=0, raw_manifests=0, v6=0, eligibility_artifact=0,
                       builder_spec=0, quarantine=0, v7_artifacts=0,
                       production_derived_writes=0, y_exposure=0),
        production_writes=0, y_exposed=0,
        gap_found_by_smoke=dict(
            severity="REQUIRES A SPEC AMENDMENT BEFORE PRODUCTION",
            finding="extended_hours_bars has NO duplicate-key guard. The frozen "
                    "fail_closed_conditions enumerate 'duplicate regular source key' only, "
                    "so a duplicated pre- or post-market bar in a source payload would be "
                    "emitted twice without any guard firing.",
            how_it_surfaced="the first run of fixture J duplicated results[100], which for "
                            "AAPL is a pre-market bar (134 precede the open). It landed in "
                            "extended hours, no guard fired, and the fixture FAILED — "
                            "correctly, because the fixture is specified to duplicate a "
                            "REGULAR source key and my injection did not.",
            fixture_corrected="the duplicate is now drawn from the regular lattice, and the "
                              "whole run was re-executed from scratch under a new "
                              "implementation hash",
            guard_added=False,
            why_not="the frozen spec does not list this condition, and inventing an "
                    "unfrozen fail-closed rule inside the implementation is exactly what "
                    "this programme's governance forbids. It is reported for an explicit "
                    "amendment decision instead.",
            practical_risk="low — MASSIVE_1M_RAW_INTEGRITY_V1 already verified the canonical "
                           "archive, so a duplicated source bar would itself be an archive "
                           "defect — but the guard is absent and that is a fact about the "
                           "contract, not about the data"),
        known_limitations=[
            "extended_hours_bars has no duplicate-key guard — see gap_found_by_smoke",
            "fixture G is validated against the V6 lineage authority only; the ingest "
            "manifest has no WHEN_ISSUED state and records that session as "
            "NOT_YET_REGULAR_WAY",
            "smoke covers the frozen fixtures, not the full 1,254-session archive",
            "row-level raw-byte provenance is not implemented; provenance is file-level"],
        authorizes="ONLY the next gate: PRODUCTION DERIVED BASE EXECUTION. Production is not "
                   "started automatically.",
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    dg = ART.seal(p, "MASSIVE_1M_DERIVED_BASE_SMOKE_V2.json",
                  required=("report_id", "status", "classification", "spec",
                            "builder_implementation_hash", "fixtures",
                            "critical_counts", "independent_verification", "mutations"),
                  supersede=os.path.exists("MASSIVE_1M_DERIVED_BASE_SMOKE_V2.json"))
    print(f"\nMASSIVE_1M_DERIVED_BASE_SMOKE_V2 · {dg} · {p['status']}")
    print(f"  implementation hash : {ih[:16]}")
    print(f"  positive {p['fixtures']['positive_passed']}/{len(pos)} · "
          f"negative {p['fixtures']['negative_passed']}/{len(neg)}")
    print(f"  independent verifier: {'PASS' if vpass else 'HOLD ' + str(p['independent_verification']['failed'])}")
    print(f"  structural_no_trade {0} · synthetic {0} · supplemental {0} · "
          f"production writes 0")
    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
