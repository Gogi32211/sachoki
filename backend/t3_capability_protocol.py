"""T3_CAPABILITY_PROTOCOL_V1 — frozen BEFORE any T3 capability code runs and before any
observed statistic exists. Same shape as T9's frozen protocol, with T3's own k and its
own derived RNG root (T_FAMILY_CAPABILITY_RNG_RULE_V1, which was frozen for exactly this).

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "T3_CAPABILITY_PROTOCOL_V1.json"


def rng_root(claim_order_hash, population_hash, block_hash, needle_artifact_hash,
             protocol_hash):
    """T_FAMILY_CAPABILITY_RNG_RULE_V1, verbatim."""
    m = hashlib.sha256()
    for part in ("T3_CAPABILITY_EVIDENCE_V1", claim_order_hash, population_hash,
                 block_hash, needle_artifact_hash, protocol_hash):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def main():
    CO = json.load(open("T3_1H_CLAIM_ORDER_V1.json"))
    assert CO["k_final"] == 853
    body = dict(
        spec_id="T3_CAPABILITY_PROTOCOL_V1", status="FROZEN",
        family_id="T3_MICROSTRUCTURE_DNA_V1",
        frozen_before="any T3 capability code exists and before any observed statistic",
        the_invariant="a capability world may NOT begin from the observed T3 sequence-Y "
                      "association. Real historical MFE may supply the NOISE; the observed "
                      "association must be DESTROYED first.",
        pipeline=[
            "sealed T3 X / membership + historical raw MFE",
            "DESTROY sequence<->Y association under the registered block-preserving null "
            "mechanism (permute Y within blocks BEFORE anything else)",
            "inject homogeneous additive delta ONLY into the frozen needle membership",
            "rebuild raw-MFE ranks",
            "rebuild tie-corrected analytic SD",
            "999 inner max-Z permutations",
            "detection yes/no"],
        hard_gates=["NO observed historical T3 Z_rank is computed, stored or printed",
                    "NO survivor ranking", "NO winner inspection"],
        distinction="Y values used for capability noise != historical T3 result exposed. "
                    "The engine may open Y to build empirical-noise worlds; the "
                    "human-facing record sees detections/20 and band geometry only.",
        needles=dict(rule="q10/q50/q90 from the SEALED final claim order, sort by support, "
                          "tie-break sealed j, nearest-rank ceil(q*k)-1",
                     k_final=853,
                     indices={q: CO["needles"][q]["index_zero_based"]
                              for q in ("q10", "q50", "q90")},
                     identities={q: {kk: CO["needles"][q][kk]
                                     for kk in ("j", "claim_id", "membership_hash",
                                                "representative", "family", "length",
                                                "support")}
                                 for q in ("q10", "q50", "q90")},
                     needle_artifact="T3_1H_CLAIM_ORDER_V1 (the needles are sealed inside "
                                     "the claim order itself; no separate extraction)"),
        grid_pp=[0.5, 1.0, 2.0, 3.0, 4.0, 5.0],
        worlds_per_needle=20, inner_permutations=999,
        rng=dict(rule="T_FAMILY_CAPABILITY_RNG_RULE_V1: root = SHA256("
                      "'T3_CAPABILITY_EVIDENCE_V1' | claim_order_hash | population_hash | "
                      "block_assignment_hash | needle_artifact_hash | "
                      "capability_protocol_hash) — derived, never hand-chosen",
                 rule_digest=ART.file_digest("T_FAMILY_CAPABILITY_RNG_RULE_V1.json"),
                 world_namespace="T3_CAPABILITY_WORLD_V1",
                 perm_namespace="T3_CAPABILITY_PERM_V1",
                 builtin_hash="FORBIDDEN on the stochastic path (process-salted, "
                              "non-replayable) — swept by gate 3",
                 note="the root closes over this very artifact's digest, so it is "
                      "computed by the runner after this protocol is sealed"),
        wording_allowed="Under the registered homogeneous additive MFE-shift alternative, "
                        "the qXX needle detected +Xpp in Y/20 finite capability worlds.",
        wording_forbidden=["true power", "guaranteed detection", "universal sensitivity"],
        upstream=dict(claim_order_digest=ART.file_digest("T3_1H_CLAIM_ORDER_V1.json"),
                      claim_order_hash=CO["claim_order_hash"],
                      population_hash=CO["population_hash"],
                      block_assignment_hash=CO["block_assignment_hash"],
                      semantic_sweep=ART.file_digest("T3_SEMANTIC_SWEEP_V1.json"),
                      stop_closure_1=ART.file_digest("T3_HOLD_CLOSURE_V1.json"),
                      stop_closure_2=ART.file_digest("T3_STOP_CLOSURE_2_V1.json"),
                      label_amendment=ART.file_digest("T3_LABEL_AMENDMENT_V1.json")),
        outcome_exposure="NOT_EXPOSED — no outcome value read")
    digest = ART.seal(body, OUT, required=("spec_id", "the_invariant", "pipeline",
                                           "needles", "rng"))
    print(f"T3_CAPABILITY_PROTOCOL_V1 · {digest} · k=853 · needles "
          + " · ".join(f"{q}=j{body['needles']['identities'][q]['j']}"
                       for q in ("q10", "q50", "q90")))


if __name__ == "__main__":
    main()
