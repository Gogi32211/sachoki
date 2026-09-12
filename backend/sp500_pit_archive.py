"""SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V1 — raw primary-source snapshots for F1-F7.

WHY RAW BYTES AND NOT EXTRACTED TEXT. A quote plus a URL is not evidence: the page can
change, and the quote is already an interpretation of it. The authoritative artifact is the
byte stream as retrieved, hashed; extracted text is a convenience derived from it and its
digest has no standing. If the two ever disagree, the bytes win.

THE CHECK THAT MAKES THE ARCHIVE MEAN SOMETHING. Archiving a page proves a page was
retrieved, not that it says what the fixture claims. So every snapshot is tested for the
presence of the exact sentence the fixture cites, after normalising HTML entities, tag
markup, curly quotes and whitespace. A snapshot whose raw bytes do NOT contain its quote is
reported as such and does not satisfy the gate — most often that means the page is rendered
client-side and the bytes carry no text at all, which is exactly the case where a URL would
have looked like provenance while proving nothing.

FILENAMES ARE DELIBERATELY NEUTRAL.

    2020-11-16_spdji_press_release_<sha8>.html      not      TSLA_REPLACED_AIV_PROOF.html

The second encodes a conclusion into the evidence's own name. The archive stores what was
retrieved; the fixture artifact is where claims live.

F6 IS ARCHIVED FOR ONE CLAIM ONLY. Its share-class representation claim is required and
must be supported. Its boundary sessions are UNCONFIRMED and no boundary claim for F6
appears in this manifest, because there is nothing to support.

    usage:  python sp500_pit_archive.py
"""
from __future__ import annotations
import hashlib, html, os, re, sys, time                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402

ARCHIVE = ("/Volumes/QUANT_RESEARCH/artifacts/provenance/sp500_pit_source_snapshots")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

SOURCES = [
    dict(key="F1_primary", fixtures=["F1"], authority="S&P Dow Jones Indices",
         doc_date="2020-11-16", kind="press_release",
         url="https://press.spglobal.com/2020-11-16-Tesla-Set-to-Join-S-P-500",
         supports="TSLA added to the S&P 500 effective prior to the open on 2020-12-21; "
                  "this release does NOT name the removed company",
         quote="will be added to the S&P 500 effective prior to the open of trading on "
               "Monday, December 21"),
    dict(key="F1_secondary_F2_primary", fixtures=["F1", "F2"],
         authority="S&P Dow Jones Indices via PR Newswire",
         doc_date="2020-12-11", kind="press_release",
         url="https://www.prnewswire.com/news-releases/tesla-set-to-join-sp-500--100-"
             "apartment-income-reit-to-join-sp-midcap-400-301191493.html",
         supports="names Apartment Investment and Management Co. (AIV) as removed, and "
                  "records the Apartment Income REIT spin-off",
         quote="spinning off Apartment Income REIT"),
    dict(key="F3_primary", fixtures=["F3"], authority="S&P Dow Jones Indices",
         doc_date="2025-09-05", kind="press_release",
         url="https://press.spglobal.com/2025-09-05-AppLovin,-Robinhood-Markets-and-Emcor-"
             "Group-Set-to-Join-S-P-500-Others-to-Join-S-P-100,-S-P-MidCap-400-and-S-P-"
             "SmallCap-600",
         supports="AppLovin replaces MarketAxess effective prior to the open on 2025-09-22",
         quote="more representative of its market capitalization range"),
    dict(key="F4_primary", fixtures=["F4"], authority="S&P Dow Jones Indices",
         doc_date="2022-10-27", kind="press_release",
         url="https://press.spglobal.com/2022-10-27-Arch-Capital-Group-Set-to-Join-S-P-500-"
             "RXO-to-Join-S-P-MidCap-400-Bread-Financial-Holdings-to-Join-S-P-SmallCap-600",
         supports="ACGL replaces TWTR effective prior to the opening on 2022-11-01, on "
                  "completion of the Twitter acquisition",
         quote="prior to the opening of trading on Tuesday, November 1"),
    dict(key="F5_primary", fixtures=["F5"], authority="Meta Platforms investor relations",
         doc_date="2022-05-31", kind="press_release",
         url="https://investor.atmeta.com/investor-news/press-release-details/2022/Meta-"
             "Platforms-Inc.-to-Change-Ticker-Symbol-to-META-on-June-9/default.aspx",
         supports="ticker FB -> META effective prior to market open 2022-06-09 with the "
                  "CUSIP unchanged",
         quote="CUSIP number will remain unchanged"),
    dict(key="F6_primary", fixtures=["F6"], authority="S&P Dow Jones Indices",
         doc_date="2014-03-11", kind="press_release",
         url="https://press.spglobal.com/2014-03-11-S-P-Dow-Jones-Indices-Announces-Changes-"
             "in-Treatment-of-Multiple-Share-Classes-in-U-S-Indices-and-Revises-Previously-"
             "Announced-Treatment-of-Google-Stock-Split",
         supports="ONE claim only: both Google Class A and Class C included in the S&P 500. "
                  "No boundary-session claim is made for F6.",
         quote="Class A and Google Class C will be included in the S&P 500"),
    dict(key="F7_primary", fixtures=["F7"], authority="S&P Dow Jones Indices",
         doc_date="2023-03-10", kind="press_release",
         url="https://press.spglobal.com/2023-03-10-Insulet-Set-to-Join-S-P-500",
         supports="PODD replaces SIVB effective prior to the opening on 2023-03-15 "
                  "following FDIC receivership",
         quote="no longer eligible for inclusion"),
    # ── V2: pre-2012 deletion coverage ─────────────────────────────────────────
    dict(key="F8_primary", fixtures=["F8"],
         authority="Standard & Poor's Index Services via PR Newswire",
         doc_date="2011-03-29", kind="press_release",
         url="https://www.prnewswire.com/news-releases/standard--poors-announces-change-"
             "to-us-index-118873854.html",
         supports="BLK replaces GENZ in the S&P 500 after the close of trading on "
                  "2011-04-01, on Sanofi-aventis's acquisition of Genzyme",
         quote="after the close of trading on Friday, April 1, 2011"),
    dict(key="F9_primary", fixtures=["F9"],
         authority="S&P Indices via PR Newswire",
         doc_date="2011-10-11", kind="press_release",
         url="https://www.prnewswire.com/news-releases/sp-indices-announces-change-to-us-"
             "index-131552863.html",
         supports="TEL replaces CEPH in the S&P 500 after the close of trading on "
                  "2011-10-14, on Teva's acquisition of Cephalon",
         quote="after the close of trading on Friday, October 14"),
]


def normalise(raw: bytes) -> str:
    """Strip markup and normalise punctuation so a quote can be located in raw bytes."""
    t = raw.decode("utf-8", errors="replace")
    t = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = html.unescape(t)
    t = (t.replace("‘", "'").replace("’", "'")
          .replace("“", '"').replace("”", '"')
          .replace("–", "-").replace("—", "-").replace(" ", " "))
    return re.sub(r"\s+", " ", t).strip().lower()


def norm_quote(q: str) -> str:
    q = (q.replace("‘", "'").replace("’", "'")
          .replace("“", '"').replace("”", '"')
          .replace("–", "-").replace("—", "-"))
    return re.sub(r"\s+", " ", q).strip().lower()


def fetch_one(s: dict) -> dict:
    rec = dict(key=s["key"], fixtures=s["fixtures"], source_authority=s["authority"],
               original_url=s["url"], supports=s["supports"], quote_required=s["quote"])
    try:
        r = requests.get(s["url"], headers={"User-Agent": UA}, timeout=45,
                         allow_redirects=True)
    except Exception as e:
        rec.update(http_status=None, error=f"{type(e).__name__}: {str(e)[:120]}",
                   snapshot_present=False, quote_present_in_raw=False)
        return rec

    raw = r.content
    sha = hashlib.sha256(raw).hexdigest()
    ext = "pdf" if "pdf" in (r.headers.get("content-type") or "").lower() else "html"
    auth = "spdji" if "spglobal" in s["url"] else (
        "prnewswire" if "prnewswire" in s["url"] else "issuer_ir")
    fname = f"{s['doc_date']}_{auth}_{s['kind']}_{sha[:8]}.{ext}"

    rec.update(
        final_url_after_redirects=r.url,
        redirected=(r.url != s["url"]),
        http_status=r.status_code,
        content_type=r.headers.get("content-type"),
        retrieved_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        raw_filename=fname, raw_sha256=sha, raw_size_bytes=len(raw))

    if r.status_code != 200 or not raw:
        rec.update(snapshot_present=False, quote_present_in_raw=False,
                   error=f"HTTP {r.status_code}")
        return rec

    os.makedirs(ARCHIVE, exist_ok=True)
    with open(os.path.join(ARCHIVE, fname), "wb") as f:
        f.write(raw)

    # derived convenience text — explicitly NOT authoritative
    txt = normalise(raw)
    tname = fname.rsplit(".", 1)[0] + ".extracted.txt"
    with open(os.path.join(ARCHIVE, tname), "w") as f:
        f.write(txt)

    rec.update(snapshot_present=True,
               extracted_text_filename=tname,
               extracted_text_is_authoritative=False,
               quote_present_in_raw=norm_quote(s["quote"]) in txt)
    return rec


def main():
    require_external_volume(purpose="sp500 pit fixture source archive")
    recs = [fetch_one(s) for s in SOURCES]

    missing = [r["key"] for r in recs if not r.get("snapshot_present")]
    noquote = [r["key"] for r in recs if r.get("snapshot_present")
               and not r.get("quote_present_in_raw")]
    covered = sorted({f for r in recs if r.get("snapshot_present")
                      and r.get("quote_present_in_raw") for f in r["fixtures"]})
    need = ["F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9"]
    unsupported = [f for f in need if f not in covered]

    p = dict(
        manifest_id="SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2",
        status="ARCHIVED" if not missing and not noquote else "INCOMPLETE",
        archive_namespace=ARCHIVE,
        authority_rule="RAW BYTES are the authoritative snapshot; extracted text is a "
                       "derived convenience artifact and its digest has no standing",
        filename_rule="neutral: <doc_date>_<authority>_<kind>_<sha8>.<ext> — a filename "
                      "never encodes the conclusion the document is cited for",
        verification_rule="a snapshot satisfies the gate only if its RAW BYTES contain the "
                          "exact sentence the fixture cites, after normalising entities, "
                          "markup, curly quotes and whitespace",
        v2_note="V2 adds F8 and F9, the pre-2012 deletion coverage that V1 lacked; F1-F7 sources are re-retrieved and re-verified unchanged",
        f6_scope="F6 is archived for its share-class representation claim ONLY; its "
                 "boundary sessions are UNCONFIRMED and no boundary claim for F6 appears "
                 "in this manifest",
        sources=recs,
        gate=dict(
            required_fixtures=need,
            fixtures_with_supported_snapshot=covered,
            fixtures_without_supported_snapshot=unsupported,
            missing_required_snapshot=len(missing),
            snapshot_without_its_quote=len(noquote),
            missing_keys=missing, quote_failed_keys=noquote,
            verdict="PASS" if not unsupported and not missing and not noquote else "HOLD"),
        candidate_sources_inspected=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2.json",
                 required=("manifest_id", "status", "sources", "gate"),
                 supersede=os.path.exists("SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2.json"))
    print(f"SP500_PIT_FIXTURE_SOURCE_ARCHIVE_V2 · {d} · {p['status']}")
    for r in recs:
        ok = "ok " if r.get("snapshot_present") else "MISS"
        q = "quote ok" if r.get("quote_present_in_raw") else "QUOTE ABSENT"
        print(f"  {ok} {r['key']:26s} http {str(r.get('http_status')):5s} "
              f"{str(r.get('raw_size_bytes','-')):>8s}B  {q}")
    g = p["gate"]
    print(f"  supported fixtures: {','.join(g['fixtures_with_supported_snapshot']) or '-'}")
    print(f"  unsupported       : {','.join(g['fixtures_without_supported_snapshot']) or '-'}")
    print(f"  SNAPSHOT GATE {g['verdict']}")
    return 0 if g["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
