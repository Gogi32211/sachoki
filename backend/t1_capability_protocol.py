"""T1_CAPABILITY_PROTOCOL_V1 — frozen BEFORE any T1 capability code runs and before any
observed statistic exists. Same shape as T9's frozen protocol, with T1's own k and its
own derived RNG root (T_FAMILY_CAPABILITY_RNG_RULE_V1, which was frozen for exactly this).

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "T1_CAPABILITY_PROTOCOL_V1.json"


def rng_root(claim_order_hash, population_hash, block_hash, needle_artifact_hash,
             protocol_hash):
    """T_FAMILY_CAPABILITY_RNG_RULE_V1, verbatim."""
    m = hashlib.sha256()
    for part in ("T1_CAPABILITY_EVIDENCE_V1", claim_order_hash, population_hash,
                 block_hash, needle_artifact_hash, protocol_hash):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def main():
    CO = json.load(open("T1_1H_CLAIM_ORDER_V1.json"))
    assert CO["k_final"] == 911
    body = dict(
        spec_id="T1_CAPABILITY_PROTOCOL_V1", status="FROZEN",
        family_id="T1_MICROSTRUCTURE_DNA_V1",
        frozen_before="any T1 capability code exists and before any observed statistic",
        the_invariant="a capability world may NOT begin from the observed T1 sequence-Y "
                      "association. Real historical MFE may supply the NOISE; the observed "
                      "association must be DESTROYED first.",
        pipeline=[
            "sealed T1 X / membership + historical raw MFE",
            "DESTROY sequence<->Y association under the registered block-preserving null "
            "mechanism (permute Y within blocks BEFORE anything else)",
            "inject homogeneous additive delta ONLY into the frozen needle membership",
            "rebuild raw-MFE ranks",
            "rebuild tie-corrected analytic SD",
            "999 inner max-Z permutations",
            "detection yes/no"],
        hard_gates=["NO observed historical T1 Z_rank is computed, stored or printed",
                    "NO survivor ranking", "NO winner inspection"],
        distinction="Y values used for capability noise != historical T1 result exposed. "
                    "The engine may open Y to build empirical-noise worlds; the "
                    "human-facing record sees detections/20 and band geometry only.",
        needles=dict(rule="q10/q50/q90 from the SEALED final claim order, sort by support, "
                          "tie-break sealed j, nearest-rank ceil(q*k)-1",
                     k_final=911,
                     indices={q: CO["needles"][q]["index_zero_based"]
                              for q in ("q10", "q50", "q90")},
                     identities={q: {kk: CO["needles"][q][kk]
                                     for kk in ("j", "claim_id", "membership_hash",
                                                "representative", "family", "length",
                                                "support")}
                                 for q in ("q10", "q50", "q90")},
                     needle_artifact="T1_1H_CLAIM_ORDER_V1 (the needles are sealed inside "
                                     "the claim order itself; no separate extraction)"),
        grid_pp=[0.5, 1.0, 2.0, 3.0, 4.0, 5.0],
        worlds_per_needle=20, inner_permutations=999,
        rng=dict(rule="T_FAMILY_CAPABILITY_RNG_RULE_V1: root = SHA256("
                      "'T1_CAPABILITY_EVIDENCE_V1' | claim_order_hash | population_hash | "
                      "block_assignment_hash | needle_artifact_hash | "
                      "capability_protocol_hash) — derived, never hand-chosen",
                 rule_digest=ART.file_digest("T_FAMILY_CAPABILITY_RNG_RULE_V1.json"),
                 world_namespace="T1_CAPABILITY_WORLD_V1",
                 perm_namespace="T1_CAPABILITY_PERM_V1",
                 builtin_hash="FORBIDDEN on the stochastic path (process-salted, "
                              "non-replayable) — swept by gate 3",
                 note="the root closes over this very artifact's digest, so it is "
                      "computed by the runner after this protocol is sealed"),
        wording_allowed="Under the registered homogeneous additive MFE-shift alternative, "
                        "the qXX needle detected +Xpp in Y/20 finite capability worlds.",
        wording_forbidden=["true power", "guaranteed detection", "universal sensitivity"],
        session_semantics=dict(
            unification_amendment=ART.file_digest(
                "SESSION_FILTER_UNIFICATION_AMENDMENT_V1.json"),
            unified_path_closure=ART.file_digest("SESSION_UNIFIED_PATH_CLOSURE_V1.json"),
            mapping_digest=__import__("session_calendar").mapping_digest(),
            status="T1 final inference universe RETAINED at k=911; the upstream session "
                   "implementations are SUPERSEDED"),
        upstream=dict(claim_order_digest=ART.file_digest("T1_1H_CLAIM_ORDER_V1.json"),
                      claim_order_hash=CO["claim_order_hash"],
                      population_hash=CO["population_hash"],
                      block_assignment_hash=CO["block_assignment_hash"],
                      stop_closure=ART.file_digest("T1_STOP_CLOSURE_V1.json"),
                      stop_extras=ART.file_digest("T1_STOP_EXTRAS_V1.json"),
                      corrected_x_reconciliation=ART.file_digest(
                          "T1_CORRECTED_X_RECONCILIATION_V1.json"),
                      class_lineage=ART.file_digest("T1_CLASS_LINEAGE_V1.json"),
                      impact_report=ART.file_digest(
                          "ADJACENCY_IMPACT_REPORT_V1.json")),
        outcome_exposure="NOT_EXPOSED — no outcome value read")
    digest = ART.seal(body, OUT, required=("spec_id", "the_invariant", "pipeline",
                                           "needles", "rng"))
    print(f"T1_CAPABILITY_PROTOCOL_V1 · {digest} · k=911 · needles "
          + " · ".join(f"{q}=j{body['needles']['identities'][q]['j']}"
                       for q in ("q10", "q50", "q90")))


if __name__ == "__main__":
    main()
