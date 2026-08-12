"""Step 1 of the sealed protocol: collect every hash that must not move, and stop.

This writes nothing that decides anything. It records what the sealed run will be checked
against, so that afterwards the question "was this the registered experiment" has a mechanical
answer instead of a recollection.

Two hashes per artifact, because one is not enough and this project learned that twice in a day:

    semantic digest    covers the payload the digest was told to cover
    source blob        covers the bytes

Constants added outside a digest payload leave the digest unchanged while the file moves, and a
CSV round-trip once shifted a float by one ULP while every printed figure matched. Identical
digest does not imply identical artifact.

The freeze commit hash cannot be recorded here — it does not exist until this file is committed.
That is deliberate and it is what makes the seeds unknowable in advance: they are derived from
it afterwards.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combolab_v2_spec as V2                                       # noqa: E402
import n0_spec as N0                                                # noqa: E402
import s1_spec as S1                                                # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))

# every file whose bytes participate in producing a sealed number
SOURCES = ("combolab_v2.py", "combolab_v2_spec.py", "combo_lab.py", "studio_verdict.py",
           "s1_run.py", "s1_spec.py", "s1_reveal_contract.py", "n0_spec.py", "n0_run.py",
           "sources.py", "sampling_target.py", "v2_decision_run.py", "v2_sealed_run.py")


def blob(path: str) -> str:
    return subprocess.check_output(["git", "hash-object", path], cwd=HERE).decode().strip()


def sha(path: str) -> str:
    with open(os.path.join(HERE, path), "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def build() -> dict:
    import combo_lab as CL
    import combolab_v2 as E
    O, _, d = CL.load_base(verbose=False)
    ma = CL.build_masks(O)
    classes = E.equivalence_classes(ma, verbose=False)
    masks = {c["representative"]: ma[c["representative"]] for c in classes}
    g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
    fam = E.assert_one_null_family(list(g1))

    m = {
        "protocol": "SEALED v2 FINAL",
        "claim_manifest": {
            "n_search_eligible": len(g1),
            "null_family": fam,
            "manifest_hash": hashlib.sha256("|".join(sorted(g1)).encode()).hexdigest(),
            "membership_hash": hashlib.sha256(
                b"".join(masks[c].tobytes() for c in sorted(g1))).hexdigest(),
            "deferred_day_level": sorted(c for c in masks if c not in g1),
        },
        "support_policy": {
            "values": V2.ELIGIBILITY,
            "hash": hashlib.sha256(
                json.dumps(V2.ELIGIBILITY, sort_keys=True).encode()).hexdigest(),
        },
        "semantic_digests": {
            "combolab_v2_spec": V2.digest(),
            "s1_spec": S1.digest(),
            "n0_spec": N0.digest(),
        },
        "source_blobs": {f: blob(f) for f in SOURCES if os.path.exists(os.path.join(HERE, f))},
        "source_sha256": {f: sha(f) for f in SOURCES if os.path.exists(os.path.join(HERE, f))},
        "delta_grid": list(S1.DELTA_GRID),
        "primary_endpoints": {
            "retention": "P(FinalSearchAccept = 1 | KnownLocationAccept = 1)",
            "paired_tax": "P(Known = 1, Final = 0)",
            "funnel": "KnownLocation -> Rank -> SearchScreen -> Final",
        },
        "structural_implications": [
            "FinalSearchAccept => SearchScreenPass",
            "SearchScreenPass => Rank <= 5",
            "FinalSearchAccept => KnownLocationAccept",
        ],
        "n0_g1_provenance": {
            "generator": "within_stratum_outcome_v1",
            "fwer_band": 0.065, "fwer_search": 0.065, "fwer_final": 0.065,
            "worlds": 200, "k": 31,
            "transfer": "computation identity proven old-vs-new module, 31/31 theta and "
                        "648/648 weights bitwise, plus 5/5 artifact replay",
        },
        "sampling_targets": {
            "sealed_run": "synthetic_dgp(incremental_composition_generator_v1)",
            "n0_g1": "structured_permutation_null(within_stratum_outcome_v1)",
            "note": "different targets; sampling_target refuses to compare them",
        },
        "seed_derivation": "sha256('combolab-v2-sealed:' + freeze_commit) -> default_rng -> "
                           "integers; the freeze commit does not exist yet, which is the point",
        "outcome_semantics": {
            "integrity_fail": "SEALED = INVALID, CAPABILITY = NOT INTERPRETABLE, even if the "
                              "results file exists and the numbers look good",
            "capability_fail": "VALID EXPERIMENT / FAILED ACCEPTANCE — the generation closes "
                               "with that verdict; it is not rerun under the same name",
        },
    }
    return m


if __name__ == "__main__":
    m = build()
    out = os.path.join(HERE, "V2_SEALED_MANIFEST.json")
    with open(out, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True)
    print(f"claims          {m['claim_manifest']['n_search_eligible']} "
          f"{m['claim_manifest']['null_family']}")
    print(f"manifest hash   {m['claim_manifest']['manifest_hash'][:16]}")
    print(f"membership hash {m['claim_manifest']['membership_hash'][:16]}")
    print(f"support policy  {m['support_policy']['hash'][:16]}")
    for k, v in m["semantic_digests"].items():
        print(f"  {k:<20s} {v[:16]}")
    print(f"source files    {len(m['source_blobs'])}")
    print(f"deferred        {len(m['claim_manifest']['deferred_day_level'])} DAY_LEVEL")
    print(f"\nwritten {out}")
