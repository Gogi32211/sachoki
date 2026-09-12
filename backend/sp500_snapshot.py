"""SP500_CURRENT_SNAPSHOT_V1 — today's S&P 500 constituents, frozen once and never again.

THE ONE THING THAT MUST BE DONE PROPERLY. The universe is now a fixed list, so the list IS
the universe definition. If it drifts mid-research — because someone re-ran a fetch a month
later and picked up an index change — then two studies in the same programme silently ran
on different universes, and nothing in either result would show it. So the set is captured
once, hashed, and every downstream step cites that digest.

PARSED FROM THE ARCHIVE, NOT FROM A SECOND FETCH. The page is retrieved once, written to
disk, hashed, and the constituent list is then extracted FROM THOSE BYTES. Fetching twice —
once to archive and once to parse — would leave the archived digest describing a page that
is merely similar to the one actually parsed. Here the digest describes exactly the bytes
the list came from.

SOURCE HONESTY. The list is taken from a community-maintained encyclopedia page. For a
POINT-IN-TIME history that source was correctly classed as unacceptable, because nobody
maintains its historical deletions. For a snapshot of what is in the index TODAY it is a
different proposition: the current list is heavily watched, and the claim it supports is
narrow. It is still not an index-administrator feed, and that limitation is recorded rather
than dressed up. Independent structural checks are applied — constituent count, multi-class
share lines, and duplicate detection — because a silently truncated table would otherwise
become the universe.

    survivorship bias   KNOWN · ACCEPTED · DECLARED in SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1
    allowed claim       historical behaviour of CURRENT S&P 500 constituents
"""
from __future__ import annotations
import hashlib, html, io, os, re, sys, time                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402

ARCHIVE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/sp500_current_snapshot"
URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
SNAPSHOT_DATE = "2026-08-25"


def parse_constituents(raw: bytes):
    """Extract (ticker, security, gics_sector, cik) from the archived bytes.

    Deliberately a direct parse of the first wikitable rather than pandas.read_html, so the
    extraction has no optional HTML-parser dependency and its behaviour is inspectable.
    """
    text = raw.decode("utf-8", errors="replace")
    m = re.search(r'<table[^>]*id="constituents".*?</table>', text, re.S)
    if not m:
        m = re.search(r'<table[^>]*class="[^"]*wikitable.*?</table>', text, re.S)
    if not m:
        raise RuntimeError("constituent table not found in archived bytes")
    table = m.group(0)

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S)
    out = []
    for r in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r, re.S)
        if len(cells) < 4:
            continue

        def clean(c):
            c = re.sub(r"(?s)<sup.*?</sup>", "", c)
            c = re.sub(r"(?s)<[^>]+>", "", c)
            return html.unescape(c).replace(" ", " ").strip()

        vals = [clean(c) for c in cells]
        tick = vals[0]
        # a ticker cell: uppercase letters with an optional . or - class suffix
        if not re.fullmatch(r"[A-Z]{1,6}([.\-][A-Z])?", tick):
            continue
        cik = next((v for v in vals if re.fullmatch(r"\d{6,10}", v)), None)
        out.append(dict(ticker=tick, security=vals[1] if len(vals) > 1 else None,
                        gics_sector=vals[2] if len(vals) > 2 else None, cik=cik))
    return out


def main():
    require_external_volume(purpose="sp500 current snapshot")
    r = requests.get(URL, headers={"User-Agent": UA}, timeout=45, allow_redirects=True)
    r.raise_for_status()
    raw = r.content
    sha = hashlib.sha256(raw).hexdigest()

    os.makedirs(ARCHIVE, exist_ok=True)
    fname = f"{SNAPSHOT_DATE}_sp500_constituents_source_{sha[:8]}.html"
    with open(os.path.join(ARCHIVE, fname), "wb") as f:
        f.write(raw)

    cons = parse_constituents(raw)                     # parsed FROM the archived bytes
    tickers = [c["ticker"] for c in cons]
    dupes = sorted({t for t in tickers if tickers.count(t) > 1})
    multi = sorted(t for t in tickers if re.search(r"[.\-]", t))
    cons_sorted = sorted(cons, key=lambda c: c["ticker"])

    # the frozen set's own identity, independent of the page it came from
    set_digest = hashlib.sha256(
        "\n".join(sorted(tickers)).encode()).hexdigest()

    checks = dict(
        count_in_expected_range=480 <= len(cons) <= 520,
        no_duplicate_tickers=not dupes,
        multi_class_lines_present=len(multi) >= 2,
        every_row_has_security=all(c["security"] for c in cons),
        cik_present_for_most=sum(1 for c in cons if c["cik"]) >= 0.9 * len(cons))

    csv_path = os.path.join(ARCHIVE, f"{SNAPSHOT_DATE}_sp500_frozen_set_{set_digest[:8]}.csv")
    with open(csv_path, "w") as f:
        f.write("ticker,security,gics_sector,cik\n")
        for c in cons_sorted:
            sec = (c["security"] or "").replace(",", " ")
            f.write(f"{c['ticker']},{sec},{c['gics_sector'] or ''},{c['cik'] or ''}\n")

    p = dict(
        spec_id="SP500_CURRENT_SNAPSHOT_V1",
        status="FROZEN" if all(checks.values()) else "HOLD — structural checks failed",
        snapshot_date=SNAPSHOT_DATE,
        definition="all securities that are constituents of the S&P 500 on the snapshot "
                   "date",
        historical_treatment="the frozen current constituent set is carried BACKWARD "
                             "through the historical period",
        membership_changes_during_history="IGNORED BY DESIGN",
        survivorship_bias="KNOWN · ACCEPTED · DECLARED",
        allowed_claim="historical behaviour of CURRENT S&P 500 constituents",
        forbidden_claims=["historical behaviour of the S&P 500 constituent universe",
                          "point-in-time S&P 500 evidence"],
        immutability=dict(
            rule="this set does NOT change mid-research, even if S&P adds or removes a "
                 "constituent tomorrow",
            enforcement="downstream steps cite frozen_set_digest; a different digest is a "
                        "different universe and a different study",
            reseal="a new snapshot is a NEW artifact with a new date and digest, never an "
                   "edit of this one"),
        n_securities=len(cons),
        frozen_set_digest=set_digest,
        multi_class_lines=multi,
        source=dict(
            url=URL, final_url=r.url, http_status=r.status_code,
            retrieved_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            raw_filename=fname, raw_sha256=sha, raw_size_bytes=len(raw),
            authority="community-maintained encyclopedia page",
            limitation="NOT an index-administrator feed. Acceptable for a CURRENT snapshot, "
                       "where the claim is narrow and the list is heavily watched; it was "
                       "correctly rejected for point-in-time history, where nobody "
                       "maintains historical deletions.",
            parsed_from="the archived bytes above, not a second fetch"),
        structural_checks=checks,
        artifacts=dict(raw_snapshot=fname, frozen_set_csv=os.path.basename(csv_path),
                       archive_dir=ARCHIVE),
        ticker_alias_note=dict(
            issue="multi-class lines use a '.' or '-' separator that vendors write "
                  "differently (BRK.B / BRK-B / BRK/B)",
            status="NOT RESOLVED HERE — alias resolution requires querying the vendor and "
                   "is a separate step",
            rule="alias resolution may change the STRING used to request a security; it "
                 "may never add or remove a security from this frozen set"),
        related=dict(
            branch_closure="SP500_PIT_UNIVERSE_BRANCH_CLOSURE_V1",
            charter="MASSIVE_1M_STATE_TRANSITION_V1 (universe_contract superseded)"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "SP500_CURRENT_SNAPSHOT_V1.json",
                 required=("spec_id", "status", "snapshot_date", "n_securities",
                           "frozen_set_digest", "source", "structural_checks"),
                 supersede=os.path.exists("SP500_CURRENT_SNAPSHOT_V1.json"))
    print(f"SP500_CURRENT_SNAPSHOT_V1 · {d} · {p['status']}")
    print(f"  securities        {len(cons)}")
    print(f"  frozen_set_digest {set_digest}")
    print(f"  raw sha256        {sha}")
    print(f"  multi-class lines {multi}")
    if dupes:
        print(f"  DUPLICATES        {dupes}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  archive           {ARCHIVE}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
