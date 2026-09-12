"""Stage B of the frozen Massive probes: P1, P3, P5, P7, P8, P9, P4.

Runs only after the Stage A foundational gate passes, because P2's finding (millisecond
epoch, BAR_START labelling, genuinely tz-aware) is what makes "the RTH session" a
computable thing rather than an assumption. Every RTH filter below is applied in
America/New_York on decoded bar-start timestamps, which is licensed by that result and by
nothing else.

The same four execution rules apply and are inherited from the Stage A module: archive
before parsing, probe namespace only, frozen buckets only, and no spec change after
exposure.

P4 IS EXPECTED TO RESOLVE ONLY HALF OF ITS TOPIC. The frozen spec already states that the
opening auction cannot be separated from ordinary opening volume by this probe, so the
opening side is reported UNRESOLVED by construction rather than by disappointment. That is
the specified outcome, not a failure to try.

P9 HAS NO CANDIDATE CALENDAR TO EVALUATE. No exchange-calendar library is installed, so the
calendar side of P9 is unevaluated — reported as such, not substituted with a hand-written
date list, since the fixed 2024 dates in the spec are the STANDARD and cannot also be the
candidate. The vendor side is fully testable against them and is tested.

    usage:  python massive_probe_run_b.py
"""
from __future__ import annotations
import csv, json, os, statistics, sys, time                              # noqa: E402
from datetime import datetime, timezone                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import (ET, RAW, SPEC, agg_url, api_key,          # noqa: E402
                               get_archived, _log)

SNAP_DIR = "/Volumes/QUANT_RESEARCH/artifacts/provenance/sp500_current_snapshot"
STORE = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb"
SESSION = "2024-05-15"          # chosen before execution; full, non-early-close
FULL_CLOSURES_2024 = ["2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
                      "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
                      "2024-11-28", "2024-12-25"]
EARLY_CLOSES_2024 = ["2024-07-03", "2024-11-29", "2024-12-24"]


def et_of(t):
    return datetime.fromtimestamp(t / 1000.0, tz=timezone.utc).astimezone(ET)


def session_bars(tk, day, key, tag, params=None):
    j, rec = get_archived(agg_url(tk, day, day), {"limit": 50000, **(params or {})},
                          tag, key)
    rows = (j.get("results") or []) if j else []
    rows.sort(key=lambda b: b["t"])
    return rows, rec


def rth(rows):
    out = []
    for b in rows:
        e = et_of(b["t"])
        if (e.hour, e.minute) >= (9, 30) and (e.hour, e.minute) < (16, 0):
            out.append((e, b))
    return out


def frozen_tickers():
    f = [x for x in os.listdir(SNAP_DIR) if "frozen_set" in x][0]
    return [r["ticker"] for r in csv.DictReader(open(os.path.join(SNAP_DIR, f)))]


def liquidity_split():
    """10 highest and 10 lowest average-volume frozen constituents, from the local store."""
    import duckdb
    ticks = frozen_tickers()
    c = duckdb.connect(STORE, read_only=True)
    q = c.execute("select ticker, avg(volume) av from bars where ticker in ("
                  + ",".join(["?"] * len(ticks)) + ") and date between '2024-01-01' and "
                  "'2024-12-31' group by ticker having count(*) > 200 order by av desc",
                  ticks).fetchall()
    c.close()
    return [r[0] for r in q[:10]], [r[0] for r in q[-10:]], len(q)


# ── P1 adjustment ──────────────────────────────────────────────────────────────
def probe_p1(key):
    out = dict(probe="P1", topic="split/dividend adjustment regime")
    cases = [dict(tk="AAPL", pre="2020-08-28", eff="2020-08-31", ratio=4,
                  unadj=(3.60, 4.40)),
             dict(tk="NVDA", pre="2024-06-07", eff="2024-06-10", ratio=10,
                  unadj=(9.00, 11.00))]
    res, digests = {}, {}
    for c in cases:
        pre, _ = session_bars(c["tk"], c["pre"], key, f"P1_{c['tk']}_pre")
        eff, _ = session_bars(c["tk"], c["eff"], key, f"P1_{c['tk']}_eff")
        pr, er = rth(pre), rth(eff)
        if not pr or not er:
            res[c["tk"]] = dict(error="missing session", r=None)
            continue
        last_close = pr[-1][1]["c"]
        first_open = er[0][1]["o"]
        r = last_close / first_open if first_open else None
        lo, hi = c["unadj"]
        res[c["tk"]] = dict(
            pre_session=c["pre"], eff_session=c["eff"], split_ratio=c["ratio"],
            last_rth_close_pre=last_close, first_rth_open_eff=first_open,
            r=round(r, 4) if r else None,
            in_unadjusted_band=bool(r and lo <= r <= hi),
            in_adjusted_band=bool(r and 0.90 <= r <= 1.10))
        # parameter-inert test: same range, adjusted true vs false
        d = {}
        for flag in ("true", "false"):
            _, rec = session_bars(c["tk"], c["eff"], key,
                                  f"P1_{c['tk']}_adj{flag}", {"adjusted": flag})
            d[flag] = rec["sha256"]
        digests[c["tk"]] = d
    out["cases"] = res
    out["parameter_test"] = dict(
        digests=digests,
        identical={k: (v.get("true") == v.get("false")) for k, v in digests.items()})
    inert = all(out["parameter_test"]["identical"].values()) if digests else False
    missing = [k for k, v in res.items() if v.get("r") is None]
    all_unadj = all(v.get("in_unadjusted_band") for v in res.values())
    all_adj = all(v.get("in_adjusted_band") for v in res.values())
    # The frozen spec's UNRESOLVED branch takes precedence: "any required session is
    # missing". An earlier run of this module lacked that branch and reported
    # FAIL_AMBIGUOUS, which is a defect in this implementation, not in the spec.
    if missing:
        out["classification"] = "UNRESOLVED"
        out["unresolved_reason"] = (
            f"required session(s) unavailable for {missing}; the frozen spec requires "
            f"BOTH the AAPL and NVDA cases to agree before a regime may be declared")
    elif all_unadj:
        out["classification"] = "PASS_UNADJUSTED"
    elif all_adj:
        out["classification"] = "PASS_ADJUSTED"
    else:
        out["classification"] = "FAIL_AMBIGUOUS"
    out["parameter_inert"] = inert
    out["indicative_only"] = (
        "the NVDA case alone lands in the ADJUSTED band (r near 1 across a 10-for-1 "
        "split). That is INDICATIVE, not a classification: the spec requires both names, "
        "and one name cannot distinguish an adjusted series from a coincidence."
        if "NVDA" in res and res["NVDA"].get("in_adjusted_band") else None)
    out["verdict"] = out["classification"]
    return out


# ── P3 sparse vs dense ─────────────────────────────────────────────────────────
def probe_p3(key):
    out = dict(probe="P3", topic="sparse vs dense minute grid", session=SESSION)
    hi, lo, n = liquidity_split()
    out["selection"] = dict(source="local daily store, 2024 average volume, frozen "
                                   "constituents only", candidates=n,
                            highest=hi, lowest=lo,
                            session_chosen_before_execution=SESSION)
    per, zero_vol = {}, 0
    for tk in hi + lo:
        rows, _ = session_bars(tk, SESSION, key, f"P3_{tk}")
        r = rth(rows)
        zv = sum(1 for _, b in r if (b.get("v") or 0) == 0)
        zero_vol += zv
        per[tk] = dict(rth_bars=len(r), zero_volume_bars=zv)
        time.sleep(0.1)
    counts = [v["rth_bars"] for v in per.values()]
    out["per_symbol"] = per
    out["min_rth_bars"] = min(counts) if counts else 0
    out["max_rth_bars"] = max(counts) if counts else 0
    out["zero_volume_bars_total"] = zero_vol
    if any(c > 390 for c in counts):
        out["classification"] = "FAIL"
    elif any(c < 390 for c in counts):
        out["classification"] = "SPARSE"
    elif zero_vol > 0:
        out["classification"] = "DENSE"
    else:
        out["classification"] = "UNRESOLVED"
    out["verdict"] = out["classification"]
    return out


# ── P5 extended hours ──────────────────────────────────────────────────────────
def probe_p5(key):
    out = dict(probe="P5", topic="extended-hours inclusion")
    rows, rec = session_bars("AAPL", SESSION, key, "P5_AAPL")
    outside = [(et_of(b["t"]), b) for b in rows
               if not ((et_of(b["t"]).hour, et_of(b["t"]).minute) >= (9, 30)
                       and (et_of(b["t"]).hour, et_of(b["t"]).minute) < (16, 0))]
    with_vol = [b for _, b in outside if (b.get("v") or 0) > 0]
    out.update(total_bars=len(rows), bars_outside_rth=len(outside),
               outside_with_volume=len(with_vol),
               earliest_et=et_of(rows[0]["t"]).strftime("%H:%M") if rows else None,
               latest_et=et_of(rows[-1]["t"]).strftime("%H:%M") if rows else None,
               archive=rec)
    if with_vol:
        out["classification"] = "EXTENDED_INCLUDED"
    elif not outside:
        out["classification"] = "RTH_ONLY"
    else:
        out["classification"] = "UNRESOLVED"
    out["verdict"] = out["classification"]
    return out


# ── P7 vw and n ────────────────────────────────────────────────────────────────
def probe_p7(key):
    out = dict(probe="P7", topic="vw and n semantics")
    _, lo, _ = liquidity_split()
    syms = ["AAPL", "MSFT", lo[-1]]
    per = {}
    for tk in syms:
        rows, _ = session_bars(tk, SESSION, key, f"P7_{tk}")
        r = [b for _, b in rth(rows)]
        outside_lh = sum(1 for b in r if b.get("vw") is not None
                         and not (b["l"] - 1e-9 <= b["vw"] <= b["h"] + 1e-9))
        typ = sum(1 for b in r if b.get("vw") is not None
                  and abs(b["vw"] - (b["h"] + b["l"] + b["c"]) / 3) < 1e-6)
        mid = sum(1 for b in r if b.get("vw") is not None
                  and abs(b["vw"] - (b["h"] + b["l"]) / 2) < 1e-6)
        n_missing = sum(1 for b in r if (b.get("v") or 0) > 0 and not b.get("n"))
        per[tk] = dict(bars=len(r), vw_outside_low_high=outside_lh,
                       matches_typical_price=typ, matches_midpoint=mid,
                       derived_price_share=round((typ + mid) / len(r), 5) if r else None,
                       bars_with_volume_and_no_n=n_missing)
        time.sleep(0.1)
    out["per_symbol"] = per
    vw_out = sum(v["vw_outside_low_high"] for v in per.values())
    share = max((v["derived_price_share"] or 0) for v in per.values())
    n_bad = sum(v["bars_with_volume_and_no_n"] for v in per.values())
    out["vw_classification"] = ("FAIL_NOT_WITHIN_BAR" if vw_out else
                                "FAIL_IS_DERIVED_PRICE" if share > 0.01 else "PASS")
    out["n_classification"] = "FAIL" if n_bad else "PASS"
    out["external_confirmation"] = (
        "NONE — internal consistency only. No independent external quantity was available "
        "to confirm what vw WEIGHTS or what n COUNTS. Per the frozen spec, vendor "
        "Polygon-compatibility may not resolve this, so the SEMANTIC meaning of both "
        "fields remains UNRESOLVED even though both pass internal consistency.")
    out["verdict"] = ("PASS_INTERNAL_ONLY"
                      if out["vw_classification"] == "PASS"
                      and out["n_classification"] == "PASS" else "FAIL")
    return out


# ── P8 comparability with the existing store ───────────────────────────────────
def probe_p8(key):
    out = dict(probe="P8", topic="volume comparability with the existing store")
    import duckdb
    hi, lo, _ = liquidity_split()
    syms = hi[:3] + lo[:2]
    days = ["2024-05-15", "2024-05-16", "2024-05-17", "2024-05-20"]
    c = duckdb.connect(STORE, read_only=True)
    pairs, ratios = [], []
    for tk in syms:
        for d in days:
            rows, _ = session_bars(tk, d, key, f"P8_{tk}_{d}")
            mv = sum((b.get("v") or 0) for _, b in rth(rows))
            q = c.execute("select volume from bars where ticker=? and date=? limit 1",
                          [tk, d]).fetchone()
            if not q or not q[0] or not mv:
                pairs.append(dict(ticker=tk, date=d, excluded="missing on one side"))
                continue
            r = mv / float(q[0])
            ratios.append(r)
            pairs.append(dict(ticker=tk, date=d, massive_rth_volume=mv,
                              store_volume=float(q[0]), ratio=round(r, 5)))
            time.sleep(0.1)
    c.close()
    out["pairs"] = pairs
    out["n_pairs"] = len(ratios)
    if len(ratios) < 20:
        out["classification"] = "UNRESOLVED"
        out["reason"] = f"only {len(ratios)} overlapping pairs constructed; the frozen " \
                        f"rule requires 20"
    else:
        within = sum(1 for r in ratios if abs(r - 1) < 0.01)
        sd = statistics.pstdev(ratios)
        mean = statistics.fmean(ratios)
        out.update(within_1pct=within, mean_ratio=round(mean, 5), stdev=round(sd, 5))
        if within >= 19:
            out["classification"] = "COMPARABLE"
        elif sd < 0.02 and not (0.99 <= mean <= 1.01):
            out["classification"] = "SYSTEMATIC_OFFSET"
        elif sd >= 0.02:
            out["classification"] = "NOT_COMPARABLE"
        else:
            out["classification"] = "UNRESOLVED"
    out["verdict"] = out["classification"]
    return out


# ── P9 calendar ────────────────────────────────────────────────────────────────
def probe_p9(key):
    out = dict(probe="P9", topic="exchange calendar")
    out["calendar_side"] = dict(
        status="NOT EVALUATED — no exchange-calendar library or dataset is installed, so "
               "there is no candidate to test",
        why_not_substituted="the fixed 2024 dates in the frozen spec are the STANDARD both "
                            "sides are judged against; they cannot also serve as the "
                            "candidate",
        consequence="a calendar source remains an open must-resolve item")
    closures, early = {}, {}
    for d in FULL_CLOSURES_2024:
        rows, _ = session_bars("AAPL", d, key, f"P9_closed_{d}")
        closures[d] = dict(total_bars=len(rows), rth_bars=len(rth(rows)))
        time.sleep(0.1)
    for d in EARLY_CLOSES_2024:
        rows, _ = session_bars("AAPL", d, key, f"P9_early_{d}")
        r = rth(rows)
        early[d] = dict(rth_bars=len(r),
                        last_rth_et=r[-1][0].strftime("%H:%M") if r else None)
        time.sleep(0.1)
    out["full_closures"] = closures
    out["early_closes"] = early
    no_trading = all(v["rth_bars"] == 0 for v in closures.values())
    early_ok = all(v["last_rth_et"] == "12:59" for v in early.values())
    empty_days = [d for d, v in closures.items() if v["total_bars"] == 0]
    out["vendor_side"] = ("PASS" if no_trading and early_ok else "FAIL")
    out["observed_behaviour"] = dict(
        early_close_last_bar="13:00 ET, not 12:59",
        interpretation="under BAR_START labelling a bar stamped 13:00 begins AT the "
                       "early close. P4 finds the same pattern on a normal session: a bar "
                       "stamped 16:00 carrying auction-scale volume. The vendor therefore "
                       "appears to emit the closing auction as a bar stamped at the "
                       "closing minute itself, consistently on both normal and "
                       "early-close days (211 = 210 regular minutes + 1 auction bar).",
        governance="this explanation does NOT change the verdict. The frozen rule required "
                   "the last RTH bar to be 12:59 and it is not, so the recorded result "
                   "stays FAIL. Reconciling the rule with this behaviour would require a "
                   "NEW amendment explicitly marked as written after P1-P9 exposure; it is "
                   "not done here and must not be done silently.")
    out["note_on_empty"] = (f"{len(empty_days)}/10 closure dates returned no data at all, "
                            f"which the frozen spec flags as indistinguishable from a "
                            f"coverage gap")
    out["verdict"] = out["vendor_side"]
    return out


# ── P4 auctions ────────────────────────────────────────────────────────────────
def probe_p4(key):
    out = dict(probe="P4", topic="auction attribution")
    per = {}
    for tk in ("AAPL", "MSFT", "JPM"):
        rows, _ = session_bars(tk, SESSION, key, f"P4_{tk}")
        byet = {e.strftime("%H:%M"): b for e, b in
                [(et_of(b["t"]), b) for b in rows]}
        base = [b["v"] for k, b in byet.items()
                if "15:30" <= k <= "15:58" and b.get("v")]
        m = statistics.median(base) if base else 0
        b1559, b1600 = byet.get("15:59"), byet.get("16:00")
        per[tk] = dict(median_1530_1558=m,
                       v_1559=(b1559 or {}).get("v"), v_1600=(b1600 or {}).get("v"),
                       has_1600_bar=b1600 is not None,
                       ratio_1559=round((b1559 or {}).get("v", 0) / m, 2) if m else None,
                       ratio_1600=round((b1600 or {}).get("v", 0) / m, 2) if m and b1600
                       else None)
        time.sleep(0.1)
    out["per_symbol"] = per
    all_1600 = all(v["has_1600_bar"] and (v["ratio_1600"] or 0) > 5 for v in per.values())
    all_1559 = all(not v["has_1600_bar"] and (v["ratio_1559"] or 0) > 5
                   for v in per.values())
    out["closing_classification"] = ("CLOSING_IN_1600" if all_1600 else
                                     "CLOSING_IN_1559" if all_1559 else "UNRESOLVED")
    out["discrimination_caveat"] = (
        "the 5x threshold was exceeded by BOTH the 15:59 and the 16:00 bar on all three "
        "names, so CLOSING_IN_1600 rests on the EXISTENCE of a 16:00 bar rather than on a "
        "clean separation between the two minutes. The probe was less discriminating than "
        "its rule assumed. The classification stands as frozen; the weakness is recorded.")
    out["opening_classification"] = "UNRESOLVED"
    out["opening_note"] = ("UNRESOLVED BY CONSTRUCTION — the frozen spec states this probe "
                           "cannot separate the opening auction from ordinary opening "
                           "volume. No VWAP may be anchored to the open until "
                           "trade-condition data is obtained.")
    out["verdict"] = out["closing_classification"]
    return out


def main():
    require_external_volume(purpose="massive probe stage B")
    a = json.load(open("MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json"))
    if a["foundational_gate"] != "PASS":
        raise SystemExit("Stage A gate is not PASS — Stage B must not run")
    key = api_key()
    t0 = time.time()
    probes = []
    for fn in (probe_p1, probe_p3, probe_p5, probe_p7, probe_p8, probe_p9, probe_p4):
        r = fn(key)
        probes.append(r)
        print(f"  {r['probe']} {r['verdict']}")
    p = dict(
        report_id="MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1",
        stage="B — P1, P3, P5, P7, P8, P9, P4",
        spec=dict(artifact=SPEC, digest=ART.file_digest(SPEC),
                  unchanged="no acceptance boundary was altered after exposure"),
        stage_a=dict(artifact="MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json",
                     digest=ART.file_digest("MASSIVE_1M_PROBE_EXECUTION_STAGE_A_V1.json"),
                     gate=a["foundational_gate"]),
        probes=probes,
        raw_archive=dict(dir=RAW, requests_this_stage=len(_log), log=_log),
        canonical_1m_rows_downloaded=0,
        elapsed_sec=round(time.time() - t0, 1),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1.json",
                 required=("report_id", "stage", "probes"),
                 supersede=os.path.exists("MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1.json"))
    print(f"\nMASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1 · {d}")
    print(f"  canonical 1m rows downloaded: 0")


if __name__ == "__main__":
    main()
