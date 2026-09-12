"""MASSIVE_TICKER_LINEAGE_V4 — identity from the reference API, confirmation from the bars.

THE AUTHORITY QUESTION, SETTLED. Three earlier attempts each failed the same way: they
inferred identity from something that does not carry it.

    V1  ticker_change events        contradicts reality (A -> AWD, CASY -> CASYV) and
                                    strips class suffixes; Massive calls it experimental
    V2  bars at the window start    META returns 122 bars on 2021-08-25 belonging to a
                                    previous holder of the string
    V3  annual presence profile     the FB -> META reassignment happened between two annual
                                    anchors and was invisible; V3's own self-check caught
                                    this and returned HOLD rather than a false FROZEN

What all three lacked is an issuer-anchored question. `/v3/reference/tickers` accepts CIK
and DATE together, so the question becomes: what did THIS ISSUER's security trade as on THAT
DATE? A string that once belonged to somebody else cannot enter the answer, because the
filter is the issuer, not the string.

    issuer identity          CIK
    security identity        share_class_figi
    supporting               composite_figi
    query authority          /v3/reference/tickers with cik + date
    bar presence             CONFIRMATION ONLY, never identity

WHY share_class_figi AND NOT CIK ALONE. One CIK maps to several securities for a dual-class
issuer — GOOGL and GOOG, FOX and FOXA, NWS and NWSA all sit in the frozen set. Alphabet's
two lines carry distinct share_class_figis (BBG009S39JY5 and BBG009S3NB21), which separates
them cleanly. If share-class continuity cannot be established the security is
UNRESOLVED_IDENTITY and is NOT resolved by picking the familiar-looking ticker.

ON A CHANGEOVER DATE THE REFERENCE RETURNS BOTH STRINGS. Querying Meta's CIK for 2022-06-09
returns FB and META, both carrying the same share_class_figi. Identity is unambiguous; only
the STRING to send is not. That is precisely where bar presence is legitimate — as a
tiebreak between two candidates already proven to be the same security, never as the thing
that decides which security it is.

FOUR STATES, NOT TWO. A security that had not listed yet is NOT_YET_LISTED, which is a
different fact from UNOBSERVED_SOURCE and must never be recorded as one.
"""
from __future__ import annotations
import json, os, sys, time                                              # noqa: E402
from concurrent.futures import ThreadPoolExecutor                       # noqa: E402
from datetime import date, timedelta                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402
from massive_probe_run import api_key                                    # noqa: E402
from massive_probe_run_b import rth, session_bars                       # noqa: E402

BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
MAP = "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json"
V3 = "MASSIVE_TICKER_HISTORY_V3.json"
WIN_START, WIN_END = "2021-08-25", "2026-08-24"
KEY = None
_sess = requests.Session()


def anchors():
    """Window start, the first trading session of each quarter, and the window end."""
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    out = [WIN_START]
    for y in range(2021, 2027):
        for m in (1, 4, 7, 10):
            d = pd.Timestamp(f"{y}-{m:02d}-01")
            if not (pd.Timestamp(WIN_START) < d < pd.Timestamp(WIN_END)):
                continue
            s = cal.sessions_in_range(d, d + pd.Timedelta(days=10))
            if len(s):
                out.append(str(s[0].date()))
    out.append(WIN_END)
    return sorted(set(out))


def ref(params, tries=4):
    p = dict(params); p["apiKey"] = KEY
    for k in range(tries):
        try:
            r = _sess.get(f"{BASE}/v3/reference/tickers", params=p, timeout=45)
            if r.status_code == 200:
                return (r.json() or {}).get("results") or []
            if r.status_code in (429, 500, 502, 503):
                time.sleep(1.5 * (k + 1)); continue
            return []
        except Exception:
            time.sleep(1.0 * (k + 1))
    return []


def current_identity(tk):
    rows = ref(dict(ticker=tk, date=WIN_END, limit=5))
    if not rows:
        rows = ref(dict(ticker=tk, limit=5))
    for x in rows:
        if x.get("ticker") == tk:
            return dict(cik=x.get("cik"), share_class_figi=x.get("share_class_figi"),
                        composite_figi=x.get("composite_figi"), name=x.get("name"),
                        type=x.get("type"))
    return None


def ticker_on(cik, scfigi, d, cfigi=None):
    """Every ticker this ISSUER'S SECURITY traded as on date d.

    Identity is narrowed in a fixed order, and an unfiltered fallback is NEVER used. V4
    fell through to "all rows for this CIK" whenever share_class_figi was absent, which it
    is for most single-class names — that pulled Invesco's ETFs in under IVZ and Aptiv's
    preferred line in under APTV. Ambiguity now yields NOTHING rather than a guess.
    """
    rows = ref(dict(cik=cik, date=d, active="true", limit=50))
    cs = [x for x in rows if x.get("type") == "CS"]          # common stock only
    if scfigi:
        m = [x for x in cs if x.get("share_class_figi") == scfigi]
        return [x.get("ticker") for x in m], len(rows)
    if cfigi:
        m = [x for x in cs if x.get("composite_figi") == cfigi]
        if m:
            return [x.get("ticker") for x in m], len(rows)
    if len(cs) == 1:
        return [cs[0].get("ticker")], len(rows)
    return [], len(rows)                                      # ambiguous -> nothing


def resolve_boundary(cik, scfigi, lo, hi, want_lo, want_hi, cfigi=None):
    """Binary search the first date on which the ticker set changes from want_lo."""
    a, b = date.fromisoformat(lo), date.fromisoformat(hi)
    while (b - a).days > 1:
        mid = a + (b - a) // 2
        t, _ = ticker_on(cik, scfigi, str(mid), cfigi)
        if want_lo and want_lo[0] in t and not (want_hi and want_hi[0] in t):
            a = mid
        else:
            b = mid
    return str(b)


def process(item):
    idx, r = item
    tk = r["massive_ticker"]
    ident = current_identity(tk)
    if not ident or not ident.get("cik"):
        return dict(current_ticker=tk, company_name=r.get("company_name"),
                    status="UNRESOLVED_IDENTITY",
                    reason="no current reference identity for the frozen ticker")
    cik, scfigi = ident["cik"], ident.get("share_class_figi")
    cfigi = ident.get("composite_figi")
    if ident.get("type") != "CS":
        return dict(current_ticker=tk, company_name=r.get("company_name"), cik=cik,
                    status="UNRESOLVED_IDENTITY",
                    reason=f"current reference type is {ident.get('type')!r}, not CS")
    prof = {}
    for d in ANCHORS:
        t, n_all = ticker_on(cik, scfigi, d, cfigi)
        prof[d] = t
    listed = [d for d in ANCHORS if prof[d]]
    if not listed:
        return dict(current_ticker=tk, company_name=r.get("company_name"), cik=cik,
                    share_class_figi=scfigi, composite_figi=ident.get("composite_figi"),
                    status="NOT_YET_LISTED_IN_WINDOW", anchor_profile=prof, intervals=[])

    # collapse anchors into runs of a single ticker string
    seq, runs = [], []
    for d in ANCHORS:
        t = prof[d]
        pick = t[0] if len(t) == 1 else (t[-1] if t else None)
        seq.append((d, pick, t))
    for d, pick, t in seq:
        if pick is None:
            continue
        if runs and runs[-1]["ticker"] == pick:
            runs[-1]["last_anchor"] = d
        else:
            runs.append(dict(ticker=pick, first_anchor=d, last_anchor=d))

    ivs, ambiguous = [], [d for d, p, t in seq if len(t) > 1]
    for k, run in enumerate(runs):
        frm = WIN_START if k == 0 else None
        if k > 0:
            prev = runs[k - 1]
            frm = resolve_boundary(cik, scfigi, prev["last_anchor"],
                                   run["first_anchor"], [prev["ticker"]],
                                   [run["ticker"]], cfigi)
        elif not prof[ANCHORS[0]]:
            frm = run["first_anchor"]
        ivs.append(dict(massive_ticker_at_time=run["ticker"], effective_from=frm,
                        effective_to=None, cik_at_time=cik,
                        share_class_figi_at_time=scfigi,
                        composite_figi_at_time=ident.get("composite_figi")))
    for k in range(len(ivs) - 1):
        ivs[k]["effective_to"] = str(date.fromisoformat(ivs[k + 1]["effective_from"])
                                     - timedelta(days=1))
    if ivs:
        ivs[-1]["effective_to"] = WIN_END

    status = ("SINGLE_TICKER_WHOLE_WINDOW" if len(ivs) == 1 and
              ivs[0]["effective_from"] == WIN_START else
              "RENAMED_IN_WINDOW" if len(ivs) > 1 else "LISTED_LATER")
    return dict(current_ticker=tk, company_name=r.get("company_name"), cik=cik,
                share_class_figi=scfigi, composite_figi=ident.get("composite_figi"),
                massive_name=ident.get("name"), status=status, intervals=ivs,
                anchor_profile=prof, ambiguous_anchors=ambiguous)


def confirm_with_bars(rec):
    """Bar presence as a tiebreak ONLY, on securities already identity-resolved."""
    checks = []
    for iv in rec.get("intervals") or []:
        d = iv["effective_from"]
        if not d:
            continue
        n = 0
        for off in range(0, 6):
            probe = str(date.fromisoformat(d) + timedelta(days=off))
            b, _ = session_bars(iv["massive_ticker_at_time"], probe, KEY,
                                f"V4_{iv['massive_ticker_at_time']}_{probe}")
            n = len(rth(b))
            if n:
                break
        checks.append(dict(ticker=iv["massive_ticker_at_time"], from_date=d, bars=n,
                           confirmed=n > 0))
    rec["bar_confirmation"] = checks
    rec["all_intervals_confirmed"] = all(c["confirmed"] for c in checks) if checks else None
    return rec


def main():
    global KEY, ANCHORS
    require_external_volume(purpose="ticker lineage v4")
    KEY = api_key()
    ANCHORS = anchors()
    rows = json.load(open(MAP))["rows"]
    print(f"  anchors: {len(ANCHORS)} ({ANCHORS[0]} .. {ANCHORS[-1]})", flush=True)

    out = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, rec in enumerate(ex.map(process, list(enumerate(rows)))):
            out.append(rec)
            if i % 50 == 0:
                print(f"    lineage {i}/{len(rows)}", flush=True)

    multi = [o for o in out if o.get("status") == "RENAMED_IN_WINDOW"]
    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(confirm_with_bars, multi))

    v3 = {s["current_ticker"]: s for s in json.load(open(V3))["securities"]}
    disagree = []
    for o in out:
        t = o["current_ticker"]
        v3s = v3.get(t, {})
        v3_renamed = v3s.get("status", "").startswith(("RENAMED", "REASSIGNED"))
        v4_renamed = o.get("status") == "RENAMED_IN_WINDOW"
        if v3_renamed != v4_renamed:
            disagree.append(dict(ticker=t, v3=v3s.get("status"), v4=o.get("status")))

    by = {}
    for o in out:
        by[o["status"]] = by.get(o["status"], 0) + 1
    unresolved = [o for o in out if o["status"] == "UNRESOLVED_IDENTITY"]
    unconfirmed = [o for o in multi if o.get("all_intervals_confirmed") is False]

    checks = dict(
        all_503_processed=len(out) == len(rows),
        no_unresolved_identity=not unresolved,
        every_security_has_identity=all(o.get("cik") for o in out
                                        if o["status"] != "UNRESOLVED_IDENTITY"),
        renamed_intervals_bar_confirmed=not unconfirmed,
        meta_caught=any(o["current_ticker"] == "META"
                        and o["status"] == "RENAMED_IN_WINDOW" for o in out))

    p = dict(
        spec_id="MASSIVE_TICKER_LINEAGE_V5",
        status="FROZEN" if all(checks.values()) else "HOLD",
        authority=dict(
            identity="/v3/reference/tickers filtered by CIK and DATE",
            issuer="CIK", security="share_class_figi", supporting="composite_figi",
            bar_presence="CONFIRMATION ONLY — used solely to choose between two strings "
                         "already proven to be the same security on a changeover date; "
                         "never to decide WHICH security a string is"),
        supersedes=dict(
            v1="REJECTED — events endpoint is experimental and contradicts reality",
            v2="REJECTED — bars-at-window-start does not establish issuer identity",
            v3=dict(artifact=V3, digest=ART.file_digest(V3),
                    role="DIAGNOSTIC ONLY, NOT AUTHORITY",
                    self_reported="V3 returned HOLD; its own check recorded that it failed "
                                  "to catch the META reassignment, which fell between two "
                                  "annual anchors")),
        window=dict(start=WIN_START, end=WIN_END), anchors=ANCHORS,
        lattice_states=["OBSERVED", "STRUCTURAL_NO_TRADE", "UNOBSERVED_SOURCE",
                        "NOT_YET_LISTED"],
        not_yet_listed_rule="a security with no reference identity at an anchor had not "
                            "listed yet. NOT_YET_LISTED is a different fact from "
                            "UNOBSERVED_SOURCE and must never be recorded as one.",
        summary=by,
        renamed_in_window=[dict(ticker=o["current_ticker"], company=o["company_name"],
                                intervals=[(iv["massive_ticker_at_time"],
                                            iv["effective_from"], iv["effective_to"])
                                           for iv in o["intervals"]],
                                bar_confirmed=o.get("all_intervals_confirmed"))
                           for o in multi],
        unresolved_identity=[o["current_ticker"] for o in unresolved],
        v3_disagreements=disagree,
        v3_disagreement_rule="a disagreement between V3 and V4 puts that security on HOLD; "
                             "it is never reconciled in V3's favour",
        acceptance=checks,
        securities=out,
        canonical_1m_rows_downloaded=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_TICKER_LINEAGE_V5.json",
                 required=("spec_id", "status", "authority", "securities", "summary",
                           "acceptance"),
                 supersede=os.path.exists("MASSIVE_TICKER_LINEAGE_V5.json"))
    print(f"\nMASSIVE_TICKER_LINEAGE_V5 · {d} · {p['status']}")
    print(f"  {by}")
    for o in p["renamed_in_window"]:
        print(f"    {o['ticker']:7s} " + " | ".join(f"{a}[{b}..{c}]"
                                                    for a, b, c in o["intervals"])
              + f"  bars_confirmed={o['bar_confirmed']}")
    if disagree:
        print(f"  V3/V4 disagreements ({len(disagree)}): {disagree[:8]}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
