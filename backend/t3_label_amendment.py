"""T3_LABEL_AMENDMENT_V1 — corrects five T9→T3 typos in DESCRIPTIVE artifact text.

Found by T3_SEMANTIC_SWEEP_V1, which also proved each of them inert: no executable reads
the governance artifact, `setup_1d` is written and never read, episode identity hashes
SETUP == 'T3', the episode SQL selects t_sig='T3', and the X freeze already carries the
correct setup label. So none of these five changed a single selected episode, claim,
hash or number — they are wrong WORDS in artifacts a human auditor reads.

The originals stay sealed. This amendment is the correction of record; it never rewrites
history and it changes no computational field. The producing sources are also left
byte-frozen, so re-running them reproduces the sealed originals with this amendment
applied on top — the artifacts and the code that made them stay in agreement.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "T3_LABEL_AMENDMENT_V1.json"
CORRECTIONS = [
    dict(artifact="T3_FAMILY_GOVERNANCE_V1.json", path="setup",
         was="FINAL_PRIORITY_RESOLVED_T9", now="FINAL_PRIORITY_RESOLVED_T3",
         why="family label; the X freeze T3_DNA_X_V1.setup already reads "
             "FINAL_PRIORITY_RESOLVED_T3, so the two now agree"),
    dict(artifact="T3_FAMILY_GOVERNANCE_V1.json", path="historical_cutoff_rule",
         was="the T3 max-null band MUST come from T9's own permutation distribution; "
             "neither T5's nor T9's band is ever reused",
         now="the T3 max-null band MUST come from T3's own permutation distribution; "
             "neither T5's nor T9's band is ever reused",
         why="the first clause named the wrong family; the rule always meant T3's OWN "
             "distribution, and the second clause (never reuse T5's or T9's) was already "
             "correct and is unchanged"),
    dict(artifact="T3_FAMILY_GOVERNANCE_V1.json", path="estimand.liquidity",
         was="20-session median dollar volume ending T9-2, full pre-window required",
         now="20-session median dollar volume ending T3-2, full pre-window required",
         why="anchor description; the computation is E5.anchors on the T3 frame and was "
             "always T3-2"),
    dict(artifact="T3_FAMILY_GOVERNANCE_V1.json", path="estimand.volatility",
         was="ATR14/close at T9-2", now="ATR14/close at T3-2",
         why="same anchor description"),
    dict(artifact="T3_HOLD_CLOSURE_V1.json", path="item1.cross_vintage.canonical_population",
         was="materialized t_sig='T9'", now="materialized t_sig='T3'",
         why="already flagged in T3_STOP_CLOSURE_2_V1.item3.supersedes; recorded here as "
             "the single amendment of record"),
]


def get(doc, path):
    node = doc
    for part in path.split("."):
        node = node[part]
    return node


def main():
    t0 = time.time()
    checks = {}
    for c in CORRECTIONS:
        doc = json.load(open(c["artifact"]))
        cur = get(doc, c["path"])
        checks[f"{c['artifact']}:{c['path']} still reads the sealed original"] = cur == c["was"]
        checks[f"{c['artifact']}:{c['path']} correction is T9-free"] = "T9" not in c["now"] \
            or c["path"] == "historical_cutoff_rule"   # that one keeps a deliberate mention
    # nothing computational is touched: the corrected values are pure text
    checks["all corrections are strings"] = all(isinstance(c["now"], str)
                                                for c in CORRECTIONS)
    checks["no correction touches a hash, count or index"] = all(
        not any(t in c["path"] for t in ("hash", "digest", "k_final", "index", "needle",
                                         "support", "count", "n_"))
        for c in CORRECTIONS)
    ok = all(checks.values())
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")

    digest = ART.seal(dict(
        spec_id="T3_LABEL_AMENDMENT_V1", result="PASS" if ok else "FAIL",
        status="AMENDMENT — descriptive text only",
        scope="five T9->T3 typos in artifact text; originals remain sealed and are not "
              "rewritten; producing sources left byte-frozen",
        corrections=CORRECTIONS,
        inertness="established by T3_SEMANTIC_SWEEP_V1 (" +
                  ART.file_digest("T3_SEMANTIC_SWEEP_V1.json") + "): no executable reads "
                  "the governance artifact, setup_1d is never read, episode identity uses "
                  "SETUP=='T3', episode SQL selects t_sig='T3'",
        computational_effect="NONE — no episode, claim, membership, block, hash, count or "
                             "index changes; k_final stays 853 and the sealed claim order "
                             "is untouched",
        checks={k: bool(v) for k, v in checks.items()},
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1)),
        OUT, required=("spec_id", "result", "corrections"),
        supersede=os.path.exists(OUT))
    print(f"\nT3_LABEL_AMENDMENT_V1 · {digest} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
