"""MASSIVE_1M_DERIVED_BASE_IDENTITY_HOLD_V1 — the builder stops before inventing a key.

The builder spec requires reusing "the exact frozen security-identity tuple/key used by
V6", and stopping rather than constructing one if none exists. Inspecting V6 shows none is
declared, so this is that stop.

WHAT V6 ACTUALLY CARRIES

    security_id          0 / 503     the field exists and is null everywhere
    composite_figi     474 / 503     distinct 474
    share_class_figi   474 / 503     distinct 474
    cik                503 / 503     distinct 500  <- 3 dual-class pairs collapse
    current_ticker     503 / 503     distinct 503  <- explicitly not a permanent key

No single field identifies all 503. composite_figi misses 29. cik alone merges GOOG/GOOGL,
FOX/FOXA and NWS/NWSA. Ticker is excluded by the spec, and rightly: this whole programme
exists partly because FB became META.

THE 29 ARE NOT RANDOM. ACN · AON · CB · ETN · JCI · LIN · MDT · TT · TEL · STE · PNR ·
ALLE · APTV · IVZ · WTW · CRH · EG · ACGL · BG · FLEX · GRMN · LYB · NXPI · STX · SW ·
AMCR · CCL · NCLH · RCL — a coherent class of non-US-incorporated issuers, where the
vendor's FIGI fields are simply absent. That is a property of the source, not a data error,
and it will not resolve by re-fetching.

THE CANDIDATE, OFFERED RATHER THAN ADOPTED

    (cik, share_class_figi)   503 / 503 distinct, zero collisions

It works because the three dual-class pairs differ in share_class_figi while the 29
null-figi securities each hold a unique CIK. But it is a tuple this analysis assembled from
two V6 fields, not a key V6 declares — and adopting it unilaterally is precisely the move
the spec forbids.

ONE HAZARD WORTH NAMING BEFORE ANYONE APPROVES IT. The key would carry a NULL component for
29 of 503 securities. SQL treats NULL as not equal to itself, so a naive join or GROUP BY on
that tuple silently drops or fragments those 29 depending on the engine. If this key is
adopted, the null must be normalised to a sentinel at construction time and that
normalisation frozen with it — otherwise the key is correct in the artifact and broken in
the warehouse.

No production or smoke build was started. No output namespace was created.
"""
from __future__ import annotations
import json, os, sys, time                                               # noqa: E402
from collections import Counter                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"


def main():
    S = json.load(open(LINEAGE))["securities"]
    fields = {}
    for f in ("security_id", "composite_figi", "share_class_figi", "cik",
              "current_ticker"):
        v = [s.get(f) for s in S]
        fields[f] = dict(present=sum(1 for x in v if x), total=len(S),
                         distinct=len({x for x in v if x}),
                         identifies_all=sum(1 for x in v if x) == len(S)
                         and len({x for x in v if x}) == len(S))

    cikc = Counter(s.get("cik") for s in S if s.get("cik"))
    dual = {k for k, n in cikc.items() if n > 1}
    dual_detail = [dict(cik=k, securities=[s["current_ticker"] for s in S
                                           if s.get("cik") == k]) for k in sorted(dual)]
    nofigi = sorted(s["current_ticker"] for s in S if not s.get("composite_figi"))
    keys = [(s.get("cik"), s.get("share_class_figi")) for s in S]
    collisions = [k for k, n in Counter(keys).items() if n > 1]

    p = dict(
        spec_id="MASSIVE_1M_DERIVED_BASE_IDENTITY_HOLD_V1",
        status="HOLD",
        reason="V6 declares no security-identity key, and the spec forbids inventing one",
        spec_clause="Reuse the exact frozen security-identity tuple/key used by V6 … If no "
                    "explicit stable V6 key exists: STOP before inventing one. Return HOLD "
                    "with the identity fields available. Do not silently construct a "
                    "ticker-based primary key.",
        lineage=dict(artifact=LINEAGE, digest=ART.file_digest(LINEAGE), securities=len(S)),
        identity_fields_available=fields,
        findings=dict(
            security_id="the field exists in V6 and is NULL for all 503",
            composite_figi_gap=dict(missing=len(nofigi), securities=nofigi),
            cik_collapses_dual_class=dual_detail,
            ticker="503 distinct but excluded by the spec as a permanent key — this "
                   "programme exists partly because FB became META",
            no_single_field_identifies_all_503=True),
        missing_figi_class=dict(
            observation="the 29 are a coherent class of non-US-incorporated issuers "
                        "(Irish, Bermudan, Swiss, UK domiciles), where the vendor's FIGI "
                        "fields are absent",
            implication="a property of the source, not a data error; re-fetching will not "
                        "resolve it"),
        candidate_key=dict(
            tuple="(cik, share_class_figi)",
            distinct=len(set(keys)), total=len(keys), collisions=len(collisions),
            works_because="the three dual-class pairs differ in share_class_figi, and each "
                          "of the 29 null-figi securities holds a unique CIK",
            status="OFFERED, NOT ADOPTED",
            why_not_adopted="it is a tuple assembled here from two V6 fields, not a key V6 "
                            "declares; adopting it unilaterally is the move the spec "
                            "forbids"),
        hazard_if_adopted=dict(
            issue="the key carries a NULL component for 29 of 503 securities",
            consequence="SQL treats NULL as not equal to itself, so a naive join or GROUP "
                        "BY on this tuple silently drops or fragments those 29 depending "
                        "on the engine",
            required_if_approved="normalise the null to an explicit sentinel at key "
                                 "construction and freeze that normalisation with the key, "
                                 "otherwise the key is correct in the artifact and broken "
                                 "in the warehouse"),
        options_for_the_decision=[
            "approve (cik, share_class_figi) with a frozen null-sentinel normalisation",
            "obtain a vendor identifier that covers all 503 and re-seal identity first",
            "declare an explicit V6 key amendment before any build"],
        not_started=dict(builder_spec_sealed=False, smoke_build=False,
                         production_build=False, output_namespace_created=False,
                         raw_archive_touched=False, frozen_artifacts_touched=False),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_1M_DERIVED_BASE_IDENTITY_HOLD_V1.json",
                 required=("spec_id", "status", "reason", "identity_fields_available",
                           "candidate_key", "hazard_if_adopted"),
                 supersede=os.path.exists("MASSIVE_1M_DERIVED_BASE_IDENTITY_HOLD_V1.json"))
    print(f"MASSIVE_1M_DERIVED_BASE_IDENTITY_HOLD_V1 · {d} · {p['status']}")
    for f, v in fields.items():
        print(f"    {f:18s} {v['present']}/{v['total']} present · {v['distinct']} distinct "
              f"· identifies_all={v['identifies_all']}")
    print(f"  candidate (cik, share_class_figi): {len(set(keys))}/{len(keys)} distinct · "
          f"collisions {len(collisions)}")
    print(f"  composite_figi missing for {len(nofigi)} securities")
    print(f"  builder spec sealed: False · smoke build: False · production build: False")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
