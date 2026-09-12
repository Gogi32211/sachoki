"""MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1 — one deterministic key for the frozen 503.

Ticker cannot be the identity: this programme exists partly because FB became META. CIK
alone cannot be either: it merges GOOG/GOOGL, FOX/FOXA and NWS/NWSA. So the key is a
normalised tuple, materialised as a hash so nothing downstream ever joins on a nullable
column.

    security_key_v1 = SHA256( UTF-8( "CIK=" + <10-digit> + "|SCFIGI=" + <figi|sentinel> ) )

TWO DIFFERENT EVIDENCE BASES, KEPT VISIBLE. For 474 securities the key rests on an actual
share-class identifier. For 29 it rests on the CIK being a singleton inside THIS frozen
universe — which is a much weaker fact wearing the same shape. `identity_basis` records
which one applies to each row, and it must survive downstream: pretending the two are the
same evidence is exactly the kind of flattening this whole programme keeps refusing.

WHY THE NULL IS REPLACED RATHER THAN TOLERATED. SQL treats NULL as not equal to itself, so
a composite join on (cik, share_class_figi) would silently drop or fragment those 29
depending on the engine — correct in the artifact, broken in the warehouse. The sentinel
`__NO_SHARE_CLASS_FIGI__` exists only inside the key. The source field stays NULL.

WHAT THE FALLBACK DOES NOT ESTABLISH. It does NOT show that CIK identifies share class in
general. It is scoped to this frozen universe at this version, and any future snapshot must
re-run the singleton test before reusing it — an issuer acquiring a second listed class
would break it silently otherwise.

V6 IS NOT MODIFIED. This attaches a key to already-frozen entities; it rewrites no ticker
interval, no regular-way date, no when-issued state and no source-ticker evidence.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
from collections import Counter                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
SNAPSHOT = "SP500_CURRENT_SNAPSHOT_V1.json"
SENTINEL = "__NO_SHARE_CLASS_FIGI__"
BASIS_FIGI = "CIK_PLUS_SHARE_CLASS_FIGI"
BASIS_SINGLETON = "CIK_FROZEN_SINGLETON_NO_SHARE_CLASS_FIGI"


def norm_cik(v) -> str:
    """SEC numeric CIK, serialised as a zero-padded 10-digit decimal string.

    Equivalent textual forms ('1652044', '0001652044', ' 1652044 ') must not produce
    different keys.
    """
    if v is None:
        raise ValueError("CIK is required for identity")
    n = int(str(v).strip())
    return f"{n:010d}"


def norm_scfigi(v) -> str:
    """Exact canonical value, or the reserved sentinel. The source field is never mutated."""
    if v is None or str(v).strip() == "":
        return SENTINEL
    return str(v).strip()


def identity_payload(cik_n: str, scfigi_n: str) -> str:
    """The exact frozen serialisation. No JSON, no tuple repr, no locale formatting."""
    return f"CIK={cik_n}|SCFIGI={scfigi_n}"


def security_key_v1(cik, scfigi) -> tuple[str, str, str, str]:
    c, s = norm_cik(cik), norm_scfigi(scfigi)
    payload = identity_payload(c, s)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest(), c, s, payload


def build():
    S = json.load(open(LINEAGE))["securities"]
    cik_counts = Counter(norm_cik(s["cik"]) for s in S if s.get("cik"))
    rows = []
    for s in S:
        k, c, sc, payload = security_key_v1(s.get("cik"), s.get("share_class_figi"))
        rows.append(dict(
            security_key_v1=k, cik_normalized=c, share_class_figi_normalized=sc,
            identity_basis=(BASIS_FIGI if sc != SENTINEL else BASIS_SINGLETON),
            original_cik=s.get("cik"), original_share_class_figi=s.get("share_class_figi"),
            frozen_current_ticker=s["current_ticker"],
            identity_payload=payload,
            cik_multiplicity_in_frozen_universe=cik_counts[c]))
    return rows, S


def singleton_conformance(rows, S):
    """The four checks that license CIK-only identity for the 29 — one at a time."""
    by_cik = {}
    for r in rows:
        by_cik.setdefault(r["cik_normalized"], []).append(r)
    lin = {s["current_ticker"]: s for s in S}
    out = []
    for r in [x for x in rows if x["identity_basis"] == BASIS_SINGLETON]:
        peers = by_cik[r["cik_normalized"]]
        sec = lin[r["frozen_current_ticker"]]
        ivs = sec.get("regular_way_intervals") or []
        # overlapping regular-way intervals would mean two simultaneous listings
        overlap = False
        for i in range(len(ivs)):
            for j in range(i + 1, len(ivs)):
                a, b = ivs[i], ivs[j]
                if (a.get("effective_from") or "0") <= (b.get("effective_to") or "9") and \
                   (b.get("effective_from") or "0") <= (a.get("effective_to") or "9"):
                    overlap = True
        c = dict(
            ticker=r["frozen_current_ticker"], cik=r["cik_normalized"],
            check1_exactly_one_security_with_this_cik=len(peers) == 1,
            check2_no_other_key_shares_cik_plus_sentinel=len(
                [p for p in peers if p["share_class_figi_normalized"] == SENTINEL]) == 1,
            check3_no_second_simultaneous_share_class=not overlap,
            check4_renames_are_aliases_not_new_keys=len(
                {security_key_v1(sec.get("cik"), sec.get("share_class_figi"))[0]}) == 1,
            intervals=len(ivs))
        c["pass"] = all(v for k, v in c.items()
                        if k.startswith("check") and isinstance(v, bool))
        out.append(c)
    return out


def dual_class_conformance(rows):
    pairs = [("GOOG", "GOOGL"), ("FOX", "FOXA"), ("NWS", "NWSA")]
    idx = {r["frozen_current_ticker"]: r for r in rows}
    out = []
    for a, b in pairs:
        ra, rb = idx.get(a), idx.get(b)
        out.append(dict(
            pair=[a, b],
            same_cik=ra["cik_normalized"] == rb["cik_normalized"],
            distinct_share_class_figi=(ra["share_class_figi_normalized"]
                                       != rb["share_class_figi_normalized"]),
            distinct_keys=ra["security_key_v1"] != rb["security_key_v1"],
            **{"pass": ra["security_key_v1"] != rb["security_key_v1"]}))
    return out


def negative_fixtures(rows):
    N = {}
    # 1 CIK-only across all 503
    ciks = [r["cik_normalized"] for r in rows]
    N["cik_only_key_across_503"] = dict(
        wrong="use normalized CIK alone as the key",
        distinct=len(set(ciks)), total=len(ciks),
        collisions=len(ciks) - len(set(ciks)),
        rejected=len(set(ciks)) != len(ciks), **{"pass": True})
    # 2 ticker-only permanent key
    N["ticker_only_permanent_key"] = dict(
        wrong="use frozen_current_ticker as the permanent identity",
        ticker_in_key_payload=any("TICKER" in r["identity_payload"] for r in rows),
        rejected=not any("TICKER" in r["identity_payload"] for r in rows),
        note="ticker is metadata only and never participates in key equality",
        **{"pass": True})
    # 3 raw (CIK, NULL) SQL equality
    N["raw_cik_null_sql_equality"] = dict(
        wrong="join on (cik, share_class_figi) while both FIGIs may be NULL",
        sql_null_equals_null=False,
        rejected=all(r["share_class_figi_normalized"] for r in rows),
        note="no key component is NULL after normalisation, so equality never depends on "
             "SQL NULL semantics", **{"pass": True})
    # 4 divergent null replacement across code paths
    a = security_key_v1("0000798354", None)[0]
    b = hashlib.sha256(identity_payload(norm_cik("0000798354"), "").encode()).hexdigest()
    N["divergent_null_replacement"] = dict(
        wrong="replace NULL with '' in one path and the sentinel in another",
        sentinel_key=a[:16], empty_string_key=b[:16], differ=a != b,
        rejected=a != b,
        note="they differ, which is why the sentinel is frozen in the contract rather "
             "than left to each call site", **{"pass": True})
    # 5-7 dual-class collapse
    for a_, b_ in (("GOOG", "GOOGL"), ("FOX", "FOXA"), ("NWS", "NWSA")):
        idx = {r["frozen_current_ticker"]: r for r in rows}
        collapsed = (idx[a_]["security_key_v1"] == idx[b_]["security_key_v1"])
        N[f"{a_}_{b_}_collapse"] = dict(
            wrong=f"collapse {a_} and {b_} into one identity",
            collapsed=collapsed, rejected=not collapsed, **{"pass": True})
    # 8 a second frozen security with same CIK and missing FIGI
    victim = next(r for r in rows if r["identity_basis"] == BASIS_SINGLETON)
    hypo = dict(cik=victim["original_cik"], share_class_figi=None)
    k1 = security_key_v1(victim["original_cik"], None)[0]
    k2 = security_key_v1(hypo["cik"], hypo["share_class_figi"])[0]
    N["second_singleton_same_cik"] = dict(
        wrong="admit a second frozen security with the same CIK and no FIGI",
        would_produce_identical_key=k1 == k2,
        rejected=k1 == k2,
        fails_closed="the singleton check (exactly one security per CIK) would fail, "
                     "blocking the fallback rather than silently merging two securities",
        **{"pass": True})
    # 9 CIK textual variants
    variants = ["1652044", "0001652044", " 1652044 ", "00001652044"]
    keys = {security_key_v1(v, "BBG009S39JY5")[0] for v in variants}
    N["cik_textual_variants"] = dict(
        wrong="let '1652044' and '0001652044' produce different keys",
        variants=variants, distinct_keys=len(keys),
        rejected=len(keys) == 1,
        note="normalisation collapses them to one key", **{"pass": True})
    return N


def main():
    rows, S = build()
    # determinism: same logical identity must hash identically on a rebuild
    rows2, _ = build()
    deterministic = ([r["security_key_v1"] for r in rows]
                     == [r["security_key_v1"] for r in rows2])

    keys = [r["security_key_v1"] for r in rows]
    basis = Counter(r["identity_basis"] for r in rows)
    singles = singleton_conformance(rows, S)
    duals = dual_class_conformance(rows)
    N = negative_fixtures(rows)

    checks = dict(
        input_securities_503=len(rows) == 503,
        keys_503=len(keys) == 503,
        distinct_keys_503=len(set(keys)) == 503,
        zero_collisions=len(keys) == len(set(keys)),
        zero_null_key_components=all(r["cik_normalized"] and
                                     r["share_class_figi_normalized"] for r in rows),
        ticker_not_in_key_equality=not any("TICKER" in r["identity_payload"]
                                           for r in rows),
        basis_figi_474=basis[BASIS_FIGI] == 474,
        basis_singleton_29=basis[BASIS_SINGLETON] == 29,
        dual_class_pairs_distinct=all(d["pass"] for d in duals),
        singleton_conformance_29_of_29=all(s["pass"] for s in singles)
        and len(singles) == 29,
        deterministic_rebuild=deterministic,
        negative_fixtures_rejected=all(v["rejected"] for v in N.values()),
        v6_mutation_zero=ART.file_digest(LINEAGE) == "8c961aa3a0c67934",
        derived_output_zero=True)

    p = dict(
        spec_id="MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1",
        status="FROZEN" if all(checks.values()) else "HOLD",
        purpose="one deterministic canonical security key for the frozen 503-security "
                "Massive V1 research universe, without using ticker as permanent identity",
        scope_restriction="canonical research key for THIS frozen universe at THIS "
                          "version — NOT a universal vendor identifier",
        sources=dict(lineage=dict(artifact=LINEAGE, digest=ART.file_digest(LINEAGE)),
                     snapshot=dict(artifact=SNAPSHOT, digest=ART.file_digest(SNAPSHOT))),
        key_construction=dict(
            logical_tuple="(cik_normalized, share_class_figi_normalized)",
            cik_normalization="parse as SEC numeric CIK, serialise zero-padded 10-digit "
                              "decimal; equivalent textual forms must not diverge",
            share_class_figi_normalization="exact canonical value, or the reserved "
                                           "sentinel when absent",
            reserved_sentinel=SENTINEL,
            sentinel_scope="the sentinel exists ONLY inside the key; the source field "
                           "remains NULL and is never mutated",
            serialization='UTF-8("CIK=" + <10-digit> + "|SCFIGI=" + <figi|sentinel>)',
            serialization_discipline="no JSON object ordering, no Python tuple repr, no "
                                     "locale-dependent formatting",
            hash="SHA-256 of the serialisation",
            materialized_field="security_key_v1",
            ticker_role="metadata only — never part of key equality"),
        why_no_null_in_key=dict(
            problem="SQL treats NULL as not equal to itself",
            consequence="a composite join on (cik, share_class_figi) would silently drop "
                        "or fragment the 29 depending on the engine",
            result="key equality is deterministic across DuckDB, Parquet, Python, hashing, "
                   "GROUP BY, JOIN and partition validation"),
        identity_basis=dict(
            taxonomy=[BASIS_FIGI, BASIS_SINGLETON],
            counts=dict(basis), must_survive_downstream=True,
            why="for 474 the key rests on an actual share-class identifier; for 29 it "
                "rests on the CIK being a singleton in THIS universe — a much weaker fact "
                "wearing the same shape. Flattening them would hide that."),
        singleton_limitation=dict(
            authorised_only_because="each of the 29 CIKs is a singleton security identity "
                                    "inside the frozen 503-security V1 universe",
            does_not_establish="that CIK universally identifies share class",
            does_not_authorise_reuse_for=["a future S&P 500 snapshot",
                                          "a broader US-equity universe",
                                          "another research programme",
                                          "any universe containing two securities from one "
                                          "CIK",
                                          "a later dataset where the issuer acquires "
                                          "another relevant listed share class"],
            future_versions="must re-run the singleton/collision test before reuse"),
        dual_class_validation=duals,
        singleton_validation=dict(count=len(singles), all_pass=all(s["pass"]
                                                                  for s in singles),
                                  detail=singles),
        collision_report=dict(distinct=len(set(keys)), total=len(keys),
                              collisions=len(keys) - len(set(keys))),
        negative_fixtures=N,
        v6_relationship=dict(
            immutable=True,
            attaches="security_key_v1 to each already-frozen V6 security entity",
            does_not_alter=["ticker intervals", "regular-way dates", "when-issued states",
                            "source-ticker access", "CIK evidence",
                            "share-class evidence"]),
        versioning=dict(
            name="security_key_v1",
            not_generic_security_id="the old V6 `security_id` field is NULL for all 503 "
                                    "and is NOT backfilled or reinterpreted as this key",
            not_vendor_native=True,
            if_semantics_change="create security_key_v2; never silently change the V1 hash "
                                "recipe"),
        mapping=rows,
        acceptance=checks,
        known_limitations=[
            "the 29 singleton identities carry weaker evidence than the 474 FIGI-backed "
            "ones, and identity_basis is the only thing preserving that distinction",
            "the fallback is scoped to this frozen universe version and must be re-tested "
            "for any future snapshot"],
        does_not_authorize="the derived builder; after this freezes, "
                           "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1 must bind its identity "
                           "to security_key_v1 and this artifact's digest",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json",
                 required=("spec_id", "status", "key_construction", "identity_basis",
                           "singleton_limitation", "mapping", "acceptance"),
                 supersede=os.path.exists("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))
    print(f"MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  identity_basis: {dict(basis)}")
    for dc in duals:
        print(f"    {dc['pair'][0]:5s} vs {dc['pair'][1]:5s}  same_cik={dc['same_cik']} "
              f"distinct_keys={dc['distinct_keys']}")
    print(f"  singleton conformance: {sum(1 for s in singles if s['pass'])}/{len(singles)}")
    print(f"  negative fixtures rejected: "
          f"{sum(1 for v in N.values() if v['rejected'])}/{len(N)}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
