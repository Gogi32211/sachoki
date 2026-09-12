"""MASSIVE_1M_P1B_AMENDMENT_V1 — a replacement adjustment probe, written AFTER P1-P9
exposure and saying so.

WHY A REPLACEMENT IS ADMISSIBLE HERE. P1 did not fail because the answer was inconvenient.
It failed because the AAPL 2020 sessions it required return HTTP 403: they lie outside the
plan's five-year entitlement window and no amount of retrying will produce them. That is a
SOURCE CAPABILITY limit discovered by the probe, not an outcome someone wanted to escape.
Replacing an impossible fixture with a possible one is not the same move as replacing a
fixture that gave the wrong answer, and the difference is the whole justification for this
artifact existing.

WHAT IS NOT TOUCHED. P1 stays UNRESOLVED in MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1 and in
the consolidated packet. This amendment does not rewrite a verdict; it opens a new question
with new fixtures.

NVDA IS DELIBERATELY EXCLUDED. Its r = 1.0041 has already been seen. A fixture whose answer
is known cannot test anything, and reusing it would let an already-observed result stand in
for independent confirmation.

TWO FIXTURES, BOTH CONFIRMED FROM SEC FILINGS BEFORE ANY MASSIVE QUERY. The ratios differ by
almost an order of magnitude (3 and 20), so the adjusted and unadjusted bands are far apart
on both and cannot overlap by accident.

    usage:  python massive_p1b.py spec     # archive fixtures, freeze the rule
            python massive_p1b.py run      # execute against Massive, seal the result
"""
from __future__ import annotations
import hashlib, html, json, os, re, sys, time                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key, get_archived, agg_url, _log       # noqa: E402
from massive_probe_run_b import rth, session_bars                        # noqa: E402

ARCHIVE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/massive_p1b_fixtures"
SEC_UA = {"User-Agent": "Sachoki Quant Research research@sachoki.local",
          "Accept-Encoding": "gzip, deflate"}
SPEC_ART = "MASSIVE_1M_P1B_AMENDMENT_V1.json"

FIXTURES = [
    dict(fixture="P1B-1", ticker="TSLA", split_factor=3,
         pre_session="2022-08-24", effective_session="2022-08-25",
         source_url="https://www.sec.gov/Archives/edgar/data/1318605/000156459022028207/"
                    "tsla-ex991_6.htm",
         source_authority="Tesla, Inc. Form 8-K exhibit 99.1, filed with the SEC",
         must_contain=["three-for-one", "August 25, 2022", "split-adjusted basis"],
         expected_facts=dict(ratio="three-for-one", record_date="2022-08-17",
                             distribution="2022-08-24 after close of trading",
                             split_adjusted_trading_begins="2022-08-25")),
    dict(fixture="P1B-2", ticker="AMZN", split_factor=20,
         pre_session="2022-06-03", effective_session="2022-06-06",
         source_url="https://www.sec.gov/Archives/edgar/data/1018724/000101872422000009/"
                    "amzn-20220309.htm",
         source_authority="Amazon.com, Inc. Form 8-K, filed with the SEC",
         must_contain=["20-for-1"],
         expected_facts=dict(ratio="20-for-1", record_date="2022-05-27",
                             split_adjusted_trading_begins="2022-06-06")),
]


def normalise(raw: bytes) -> str:
    t = raw.decode("utf-8", errors="replace")
    t = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = html.unescape(t).replace(" ", " ").replace("‑", "-")
    return re.sub(r"\s+", " ", t).strip().lower()


def do_spec():
    require_external_volume(purpose="p1b fixture archive")
    os.makedirs(ARCHIVE, exist_ok=True)
    recs = []
    for f in FIXTURES:
        r = requests.get(f["source_url"], headers=SEC_UA, timeout=45)
        raw = r.content
        sha = hashlib.sha256(raw).hexdigest()
        fn = f"{f['ticker']}_8k_{sha[:8]}.htm"
        if r.status_code == 200:
            with open(os.path.join(ARCHIVE, fn), "wb") as fh:
                fh.write(raw)
        txt = normalise(raw)
        found = {q: (q.lower() in txt) for q in f["must_contain"]}
        recs.append(dict(**{k: v for k, v in f.items() if k != "must_contain"},
                         http_status=r.status_code, raw_filename=fn, raw_sha256=sha,
                         raw_size_bytes=len(raw),
                         retrieved_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                        time.gmtime()),
                         quote_checks=found,
                         snapshot_supports_claim=(r.status_code == 200
                                                  and all(found.values()))))
    ok = all(x["snapshot_supports_claim"] for x in recs)

    p = dict(
        spec_id="MASSIVE_1M_P1B_AMENDMENT_V1",
        status="FROZEN BEFORE QUERY" if ok else "HOLD — a fixture snapshot does not "
                                                "support its claim",
        written_after_exposure=dict(
            declaration="THIS AMENDMENT WAS WRITTEN AFTER P1-P9 EXPOSURE",
            why_admissible="P1's AAPL 2020 sessions return HTTP 403 and lie outside the "
                           "plan's five-year entitlement window. The fixture is "
                           "impossible, not inconvenient — a source-capability limit "
                           "discovered by the probe, not an outcome being escaped.",
            what_is_not_changed="P1 remains UNRESOLVED in "
                                "MASSIVE_1M_PROBE_EXECUTION_STAGE_B_V1 and in the "
                                "consolidated packet; no verdict is rewritten",
            nvda_excluded="NVDA is deliberately not reused: its r = 1.0041 has already "
                          "been seen, and a fixture whose answer is known tests nothing"),
        frozen_rule=dict(
            statistic="r = close(last RTH minute of pre_session) / "
                      "open(first RTH minute of effective_session)",
            ADJUSTED="0.90 <= r <= 1.10",
            UNADJUSTED="0.90*S <= r <= 1.10*S, where S is the split factor",
            FAIL_AMBIGUOUS="r outside both bands on either fixture, or the two fixtures "
                           "disagree on regime",
            FAIL_PARAMETER_INERT="adjusted=true and adjusted=false return identical "
                                 "response digests",
            UNRESOLVED="any required session is missing",
            both_must_agree="a regime is declared only if BOTH fixtures land in the same "
                            "band",
            frozen_before="any Massive query for these sessions"),
        entitlement_check=dict(
            window_start="2021-08-25",
            all_fixtures_inside_window=all(f["pre_session"] >= "2021-08-25"
                                           for f in FIXTURES)),
        fixtures=recs,
        fixture_archive=ARCHIVE,
        massive_queried="NO — not one aggregate request has been made for these sessions",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, SPEC_ART, required=("spec_id", "status", "frozen_rule", "fixtures",
                                        "written_after_exposure"),
                 supersede=os.path.exists(SPEC_ART))
    print(f"MASSIVE_1M_P1B_AMENDMENT_V1 · {d} · {p['status']}")
    for x in recs:
        print(f"  {x['fixture']} {x['ticker']:5s} S={x['split_factor']:2d} "
              f"{x['pre_session']} -> {x['effective_session']}  http {x['http_status']} "
              f"{x['raw_size_bytes']:>7}B  supports={x['snapshot_supports_claim']}")
        print(f"      quotes: {x['quote_checks']}")
    print(f"  massive queried: NO")
    return 0 if ok else 1


def do_run():
    require_external_volume(purpose="p1b execution")
    if not os.path.exists(SPEC_ART):
        raise SystemExit("the P1B amendment must be sealed before it is executed")
    spec = json.load(open(SPEC_ART))
    if spec["status"] != "FROZEN BEFORE QUERY":
        raise SystemExit(f"P1B spec status is {spec['status']} — refusing to run")
    key = api_key()
    res, digests = {}, {}
    for f in FIXTURES:
        tk, S = f["ticker"], f["split_factor"]
        pre, _ = session_bars(tk, f["pre_session"], key, f"P1B_{tk}_pre")
        eff, _ = session_bars(tk, f["effective_session"], key, f"P1B_{tk}_eff")
        pr, er = rth(pre), rth(eff)
        if not pr or not er:
            res[tk] = dict(error="missing session", r=None)
            continue
        r = pr[-1][1]["c"] / er[0][1]["o"] if er[0][1]["o"] else None
        res[tk] = dict(split_factor=S, pre_session=f["pre_session"],
                       effective_session=f["effective_session"],
                       last_rth_close_pre=pr[-1][1]["c"],
                       first_rth_open_eff=er[0][1]["o"], r=round(r, 4) if r else None,
                       in_adjusted_band=bool(r and 0.90 <= r <= 1.10),
                       in_unadjusted_band=bool(r and 0.90 * S <= r <= 1.10 * S))
        d = {}
        for flag in ("true", "false"):
            _, rec = session_bars(tk, f["effective_session"], key,
                                  f"P1B_{tk}_adj{flag}", {"adjusted": flag})
            d[flag] = rec["sha256"]
        digests[tk] = d
        time.sleep(0.2)

    missing = [k for k, v in res.items() if v.get("r") is None]
    inert = {k: v["true"] == v["false"] for k, v in digests.items()}
    all_adj = res and all(v.get("in_adjusted_band") for v in res.values())
    all_un = res and all(v.get("in_unadjusted_band") for v in res.values())
    if missing:
        cls = "UNRESOLVED"
    elif all(inert.values()) and inert:
        cls = "FAIL_PARAMETER_INERT"
    elif all_adj:
        cls = "PASS_ADJUSTED"
    elif all_un:
        cls = "PASS_UNADJUSTED"
    else:
        cls = "FAIL_AMBIGUOUS"

    p = dict(
        report_id="MASSIVE_1M_P1B_EXECUTION_V1",
        spec=dict(artifact=SPEC_ART, digest=ART.file_digest(SPEC_ART),
                  rule_unchanged="the bands were frozen before any of these sessions was "
                                 "requested"),
        classification=cls, cases=res,
        parameter_test=dict(digests=digests, identical=inert),
        consequence=dict(
            PASS_ADJUSTED="the series is split-adjusted; vendor RESTATEMENT behaviour "
                          "becomes a vintage concern, since the same history may change "
                          "between extracts",
            PASS_UNADJUSTED="a split-adjustment layer is REQUIRED before any multi-year "
                            "feature",
            other="canonical ingestion stays blocked for multi-year use")[
            cls if cls in ("PASS_ADJUSTED", "PASS_UNADJUSTED") else "other"],
        supersedes_nothing="P1 remains UNRESOLVED in its original artifacts",
        raw_archive=dict(requests=len(_log)),
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_P1B_EXECUTION_V1.json",
                 required=("report_id", "classification", "cases", "spec"),
                 supersede=os.path.exists("MASSIVE_1M_P1B_EXECUTION_V1.json"))
    print(f"MASSIVE_1M_P1B_EXECUTION_V1 · {d} · {cls}")
    for k, v in res.items():
        print(f"  {k:5s} S={v.get('split_factor')} r={v.get('r')} "
              f"adj_band={v.get('in_adjusted_band')} unadj_band={v.get('in_unadjusted_band')}")
    print(f"  parameter inert: {inert}")
    print(f"  consequence: {p['consequence']}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "spec"
    raise SystemExit(do_spec() if cmd == "spec" else (do_run() or 0))
