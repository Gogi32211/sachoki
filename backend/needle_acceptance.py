"""The sealed acceptance run. Opened once, and it checks that it is the first time.

`combolab_spec.py` was frozen before the search layer existed; `combo_lab.py` was developed
against SMOKE and DEVELOPMENT seeds only. The 120 acceptance seeds did not exist until the
freeze commit did, because they are derived from its hash:

    acceptance_seeds = sha256("combolab-acceptance:" + freeze_commit) → default_rng → 120 ints

That is the part a promise cannot do. A plaintext seed list commits to WHICH CODE saw the set,
but leaves `freeze A → look → change the engine → freeze B` available; the ledger would record
it, and the test would already be spent. A hash that does not exist until the code is final
cannot be looked up early, by me or anyone.

This runner therefore refuses to start unless:

    IMPLEMENTATION_FROZEN.json exists
    every file it lists still hashes to what it recorded
    the spec digest still matches

and it writes the freeze commit into its own results, so a second run against edited code is
visibly a different experiment rather than a quiet correction of the first.

No new metrics. Whatever else the run makes visible is an exploratory diagnostic and is reported
as one; acceptance answers the nine preregistered outcomes and stops.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_spec as SPEC                                        # noqa: E402
from needle_test import one_replication, summarise                  # noqa: E402

MARKER = pathlib.Path(os.path.dirname(os.path.abspath(__file__))) / "IMPLEMENTATION_FROZEN.json"


class SealBroken(RuntimeError):
    """The code that is about to see the sealed set is not the code that was frozen."""


def verify_seal() -> dict:
    if not MARKER.exists():
        raise SealBroken(
            "IMPLEMENTATION_FROZEN.json is missing. The acceptance set is not runnable before "
            "the implementation is frozen — that is what makes it an acceptance set rather than "
            "another development run.")
    m = json.loads(MARKER.read_text())
    here = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    bad = []
    for f, want in m["file_sha256"].items():
        got = hashlib.sha256((here / f).read_bytes()).hexdigest()
        if got != want:
            bad.append(f"{f}\n      frozen {want[:16]}…\n      now    {got[:16]}…")
    if bad:
        raise SealBroken(
            "the engine changed after the freeze:\n    " + "\n    ".join(bad) +
            "\n\n  If this is a deliberate second attempt, it is a NEW experiment and needs a "
            "new freeze\n  commit and a new marker — its seeds will differ, and it must be "
            "reported as a second\n  look, not as the first.")
    if m["spec_digest"] != SPEC.spec_digest():
        raise SealBroken(f"spec digest moved: frozen {m['spec_digest'][:16]}… "
                         f"now {SPEC.spec_digest()[:16]}…")
    return m


def main():
    m = verify_seal()
    seeds = SPEC.acceptance_seeds(m["freeze_commit"])
    deltas = list(SPEC.DELTA_GRID)
    bar = "=" * 122
    print(bar, flush=True)
    print(f"  NEEDLE TEST · SEALED ACCEPTANCE · {len(seeds)} seeds × {len(deltas)} δ", flush=True)
    print(bar, flush=True)
    print(f"  freeze commit  {m['freeze_commit']}", flush=True)
    print(f"  spec digest    {m['spec_digest'][:32]}…", flush=True)
    print(f"  seal verified  {len(m['file_sha256'])} files unchanged since freeze", flush=True)
    print(f"  k = {SPEC.DECLARED_K} claims · top-{SPEC.TOP_K} · τ = {SPEC.EXPOSURE_THRESHOLD}",
          flush=True)
    print(bar, flush=True)

    O, v0, d = CL.load_base()
    masks = CL.build_masks(O)
    lab = CL.ComboLab(O, d, masks)
    exposure = SPEC.exposure_matrix(masks)

    rows, t0 = [], time.time()
    for delta in deltas:
        for i, s in enumerate(seeds):
            rows.append(one_replication(lab, v0, exposure, int(s), delta,
                                        n_perm=CL.N_PERM, n_boot=CL.N_BOOT, workers=8))
            if (i + 1) % 20 == 0:
                print(f"    δ={delta:<5.2f} {i+1:>3d}/{len(seeds)}  ({time.time()-t0:.0f}s)",
                      flush=True)
    D = pd.DataFrame(rows)
    D["freeze_commit"] = m["freeze_commit"]
    D.to_csv("needle_acceptance.csv", index=False)

    S = summarise(D)
    S["freeze_commit"] = m["freeze_commit"][:12]
    print("\n" + bar, flush=True)
    print("  SEALED ACCEPTANCE — the nine preregistered outcomes", flush=True)
    print(bar, flush=True)
    print(S.to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)
    print(f"\n  δ = 0 carries semantic_status CONTAMINATED_BY_REAL_STRUCTURE and is not a "
          f"false-positive\n  rate, nor a bound on one. The structured-null placebo is the "
          f"next validation stage.", flush=True)
    print(f"\n  All rates are conditional on the realised history 2021-2026. Not power.",
          flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
