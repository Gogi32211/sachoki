"""SP500_CURRENT_MASSIVE_TICKER_MAP_V1 — the 503 frozen securities in Massive's own
nomenclature.

THE RULE THIS IMPLEMENTS. Massive is the canonical bar source, so Massive decides how a
security is named in every request, table and file downstream. We do not invent a
convention and hope it matches. The snapshot ticker becomes provenance only.

    snapshot_ticker   what the frozen source snapshot said        provenance only
    massive_ticker    what Massive calls this security            CANONICAL OPERATIONAL

WHY MATCHING ON CIK RATHER THAN GUESSING PUNCTUATION. The obvious approach to BRK.B is to
try BRK.B, then BRK-B, then BRK/B and keep whichever returns something. That is guessing,
and it fails silently in the one case that matters: if some OTHER live security happens to
own the string we guessed, we would map a frozen constituent onto a different company and
nothing downstream would ever notice. CIK is a regulatory identifier for the issuer, it is
present for essentially every row of the snapshot, and it is completely indifferent to how
a vendor punctuates share classes. So an exact ticker match is tried first, CIK second, and
punctuation variants are accepted ONLY when they agree with the CIK.

ONE REQUEST SET, NOT 503. The full active US stock reference list is paginated once and
matched locally. Beyond being kinder to the vendor, it makes CIK matching possible at all —
a per-ticker lookup can only answer about a string we already guessed correctly.

INVARIANTS. Alias resolution may change the string used to request a security. It may never
add one, remove one, substitute a different company, or silently drop a security that fails
to resolve. A security that does not resolve is UNRESOLVED — which is not NOT_AVAILABLE and
is not a licence to drop it — and ingestion for it stays on HOLD.

No bar data is requested. This touches the reference endpoint only.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                          # noqa: E402
import t5_artifact as ART                                                # noqa: E402
from studio.mount_guard import require_external_volume                   # noqa: E402

ARCHIVE = "/Volumes/QUANT_RESEARCH/artifacts/provenance/sp500_massive_reference"
SNAP = "SP500_CURRENT_SNAPSHOT_V1.json"
BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")


def api_key() -> str:
    """Read the key from .env. Never printed, never stored in any artifact."""
    for line in open(".env"):
        line = line.strip()
        if line.startswith("MASSIVE_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("MASSIVE_API_KEY not found in backend/.env")


def frozen_set():
    d = json.load(open(SNAP))
    import csv
    p = os.path.join("/Volumes/QUANT_RESEARCH/artifacts/provenance/sp500_current_snapshot",
                     d["artifacts"]["frozen_set_csv"])
    with open(p) as f:
        rows = list(csv.DictReader(f))
    return d, rows


def fetch_reference(key):
    """Page the full active US stock reference list; archive every raw page."""
    os.makedirs(ARCHIVE, exist_ok=True)
    out, pages = [], []
    url = f"{BASE}/v3/reference/tickers"
    params = {"market": "stocks", "active": "true", "limit": 1000, "apiKey": key}
    n = 0
    while url and n < 40:
        r = requests.get(url, params=params if n == 0 else {"apiKey": key}, timeout=45)
        if r.status_code != 200:
            raise RuntimeError(f"reference page {n} HTTP {r.status_code}: {r.text[:200]}")
        raw = r.content
        sha = hashlib.sha256(raw).hexdigest()
        fn = f"reference_tickers_page{n:02d}_{sha[:8]}.json"
        with open(os.path.join(ARCHIVE, fn), "wb") as f:
            f.write(raw)
        pages.append(dict(page=n, raw_filename=fn, sha256=sha, bytes=len(raw)))
        j = r.json()
        out.extend(j.get("results") or [])
        url = j.get("next_url")
        n += 1
        time.sleep(0.15)
    return out, pages


def norm_cik(v):
    try:
        return int(str(v).strip().lstrip("0") or 0) or None
    except Exception:
        return None


def main():
    require_external_volume(purpose="sp500 massive ticker map")
    snap, rows = frozen_set()
    key = api_key()
    ref, pages = fetch_reference(key)

    by_ticker, by_cik = {}, {}
    for t in ref:
        sym = (t.get("ticker") or "").strip()
        if sym:
            by_ticker.setdefault(sym, t)
        c = norm_cik(t.get("cik"))
        if c:
            by_cik.setdefault(c, []).append(t)

    mapped, unresolved = [], []
    for r in rows:
        snap_t = r["ticker"]
        cik = norm_cik(r.get("cik"))
        rec = dict(company_name=r.get("security"), snapshot_ticker=snap_t,
                   snapshot_cik=r.get("cik") or None)

        hit, method = None, None
        # 1 exact ticker, corroborated by CIK when both sides have one
        if snap_t in by_ticker:
            cand = by_ticker[snap_t]
            ccik = norm_cik(cand.get("cik"))
            if cik and ccik and cik != ccik:
                method = None                      # same string, different issuer
            else:
                hit, method = cand, ("EXACT_TICKER_CIK_CONFIRMED"
                                     if cik and ccik else "EXACT_TICKER")
        # 2 CIK — indifferent to how the vendor punctuates share classes
        if hit is None and cik and cik in by_cik:
            cands = by_cik[cik]
            if len(cands) == 1:
                hit, method = cands[0], "CIK"
            else:
                base = snap_t.replace(".", "").replace("-", "").upper()
                exact = [c for c in cands
                         if (c.get("ticker") or "").replace(".", "").replace("-", "")
                         .upper() == base]
                if len(exact) == 1:
                    hit, method = exact[0], "CIK_PLUS_SHARE_CLASS"
        # 3 punctuation variant, accepted ONLY if the CIK agrees
        if hit is None:
            for v in (snap_t.replace(".", "-"), snap_t.replace(".", "/"),
                      snap_t.replace("-", "."), snap_t.replace(".", "")):
                cand = by_ticker.get(v)
                if not cand:
                    continue
                ccik = norm_cik(cand.get("cik"))
                if cik and ccik and cik == ccik:
                    hit, method = cand, "PUNCTUATION_VARIANT_CIK_CONFIRMED"
                    break

        if hit is None:
            rec.update(massive_ticker=None, massive_reference_id=None,
                       mapping_status="UNRESOLVED", mapping_method=None,
                       note="not NOT_AVAILABLE and not a licence to drop; ingestion for "
                            "this security is on HOLD until the mapping is established")
            unresolved.append(rec)
        else:
            rec.update(massive_ticker=hit.get("ticker"),
                       massive_reference_id=(hit.get("composite_figi")
                                             or hit.get("share_class_figi")
                                             or hit.get("cik")),
                       massive_name=hit.get("name"), massive_cik=hit.get("cik"),
                       primary_exchange=hit.get("primary_exchange"),
                       mapping_status="RESOLVED", mapping_method=method,
                       ticker_changed=(hit.get("ticker") != snap_t))
        rec["verified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        mapped.append(rec)

    res = [m for m in mapped if m["mapping_status"] == "RESOLVED"]
    mt = [m["massive_ticker"] for m in res]
    dup_massive = sorted({t for t in mt if mt.count(t) > 1})
    changed = [m for m in res if m.get("ticker_changed")]
    snap_in = {r["ticker"] for r in rows}
    snap_out = {m["snapshot_ticker"] for m in mapped}

    checks = dict(
        expected_503=len(rows) == 503,
        every_frozen_security_has_a_row=len(mapped) == len(rows),
        no_additions=not (snap_out - snap_in),
        no_removals=not (snap_in - snap_out),
        no_duplicate_massive_ticker=not dup_massive,
        no_many_to_one=not dup_massive,
        all_resolved=not unresolved)

    os.makedirs(ARCHIVE, exist_ok=True)
    csv_path = os.path.join(ARCHIVE, "sp500_massive_ticker_map.csv")
    with open(csv_path, "w") as f:
        f.write("snapshot_ticker,massive_ticker,mapping_status,mapping_method,company\n")
        for m in sorted(mapped, key=lambda x: x["snapshot_ticker"]):
            f.write(f"{m['snapshot_ticker']},{m.get('massive_ticker') or ''},"
                    f"{m['mapping_status']},{m.get('mapping_method') or ''},"
                    f"{(m.get('company_name') or '').replace(',', ' ')}\n")
    map_digest = hashlib.sha256(
        "\n".join(f"{m['snapshot_ticker']}={m.get('massive_ticker')}"
                  for m in sorted(mapped, key=lambda x: x["snapshot_ticker"])
                  ).encode()).hexdigest()

    p = dict(
        spec_id="SP500_CURRENT_MASSIVE_TICKER_MAP_V1",
        status="FROZEN" if all(checks.values()) else "HOLD — see unresolved / failed checks",
        universe=dict(artifact=SNAP, digest=ART.file_digest(SNAP),
                      frozen_set_digest=snap["frozen_set_digest"],
                      n_securities=snap["n_securities"],
                      unchanged="the frozen set is NOT modified by alias resolution"),
        rule="massive_ticker is the CANONICAL OPERATIONAL TICKER. Everywhere the programme "
             "says 'ticker' without qualification, it means massive_ticker. "
             "snapshot_ticker is provenance only.",
        invariants=dict(
            may=["change the query ticker string",
                 "record the Massive reference identifier",
                 "retain alias metadata"],
            may_not=["add a security", "remove a security", "substitute a different "
                     "company", "silently drop a security that fails to resolve"],
            unresolved_semantics="UNRESOLVED is not NOT_AVAILABLE and is not DROP; "
                                 "ingestion for that security is HOLD until resolved"),
        matching=dict(
            order=["exact ticker (corroborated by CIK where both sides carry one)",
                   "CIK", "CIK plus share-class disambiguation",
                   "punctuation variant ONLY when the CIK agrees"],
            why_cik="a punctuation guess can silently land on a different live issuer that "
                    "happens to own the string; CIK is issuer identity and is indifferent "
                    "to vendor punctuation",
            rejected_by_design="accepting a punctuation variant on the strength of it "
                               "returning data"),
        counts=dict(expected=len(rows), resolved=len(res), unresolved=len(unresolved),
                    ticker_string_changed=len(changed),
                    duplicate_massive_tickers=len(dup_massive)),
        acceptance=checks,
        ticker_changes=[dict(snapshot=m["snapshot_ticker"], massive=m["massive_ticker"],
                             method=m["mapping_method"]) for m in changed],
        unresolved=unresolved,
        methods_used={k: sum(1 for m in res if m["mapping_method"] == k)
                      for k in sorted({m["mapping_method"] for m in res})},
        map_digest=map_digest,
        source=dict(endpoint=f"{BASE}/v3/reference/tickers",
                    query="market=stocks, active=true, paginated",
                    reference_records=len(ref), pages=pages,
                    archive_dir=ARCHIVE, csv=os.path.basename(csv_path),
                    credential="MASSIVE_API_KEY read from backend/.env; never printed, "
                               "logged, or stored in this artifact",
                    bar_data_requested="NONE — reference endpoint only"),
        rows=mapped,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json",
                 required=("spec_id", "status", "universe", "rule", "invariants",
                           "counts", "acceptance", "rows"),
                 supersede=os.path.exists("SP500_CURRENT_MASSIVE_TICKER_MAP_V1.json"))
    print(f"SP500_CURRENT_MASSIVE_TICKER_MAP_V1 · {d} · {p['status']}")
    print(f"  reference records pulled : {len(ref)} over {len(pages)} pages")
    print(f"  resolved {len(res)}/{len(rows)} · unresolved {len(unresolved)} · "
          f"string changed {len(changed)}")
    print(f"  methods: {p['methods_used']}")
    for c in p["ticker_changes"]:
        print(f"    {c['snapshot']:8s} -> {c['massive']:8s}  {c['method']}")
    for u in unresolved[:10]:
        print(f"    UNRESOLVED {u['snapshot_ticker']:8s} {u['company_name']}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
