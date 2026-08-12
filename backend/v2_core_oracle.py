"""Step 0 of the extraction: freeze what the tract computes TODAY, before a line moves.

The danger in extracting a working statistical tract is not that the refactor breaks Python. It
is that a second, slightly different ComboLab appears during the move and the UI treats it as
the old one. The only defence is a bit-exact record made before the first edit.

WHY THIS IS NOT "RE-RUN THE SEALED ACCEPTANCE AND DIFF". `v2_sealed_run` is opened once by its
own contract — one look, one outcome — so re-running it to obtain a baseline would spend the
seal to check a refactor. This oracle therefore covers the DETERMINISTIC CORE that the sealed
run and any future historical run both stand on, and touches nothing sealed:

    the eligible strata E_c and the frozen weights w_cs
    the per-cell support metadata the verdict reads
    θ evaluated on fixed outcome vectors

`Frozen.theta(y)` takes the outcome as an argument, so the estimator is outcome-agnostic and can
be pinned on any y. Two are used: a deterministic synthetic world, and the REAL return column.

FLOATS ARE COMPARED AS BITS. `float.hex()` and nothing rounder. Two different computations agree
to four decimal places all the time; that is exactly the failure this file exists to catch, and
this project has already been bitten once by a 1-ULP difference that every printed figure hid.

WHAT THE ORACLE DOES NOT CLAIM. It is a regression baseline for one frozen input on one commit.
Reproducing it proves the extracted core computes what the current core computes, on that
fixture. It is not a proof that the engine is correct, and not a proof about any other input.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as E                                             # noqa: E402
from v2_decision_run import Frozen                                  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ORACLE = os.path.join(HERE, "V2_CORE_ORACLE.json")

# One deterministic synthetic world, so the oracle covers the path the sealed run actually took,
# and the real outcome, so it covers the path a historical run would take. Neither seed is a
# sealed seed; see `SealedRNGReuseError` in the engine for why that matters.
ORACLE_WORLD_SEED = 20260812
ORACLE_RNG_NAMESPACE = "combolab_v2_core_oracle_v1"


def _hex(x) -> str:
    """Bit-exact text for a float. Not a rounded string, on purpose."""
    return float(x).hex()


def _commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=HERE,
                                       text=True).strip()
    except Exception:                                                # noqa: BLE001
        return "unknown"


def build() -> dict:
    t0 = time.time()
    O, v_real, dates = CL.load_base(verbose=True)
    masks_all = CL.build_masks(O)
    classes = E.equivalence_classes(masks_all, verbose=False)
    masks = {c["representative"]: masks_all[c["representative"]] for c in classes}
    g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
    deferred = sorted(c for c in masks if c not in g1)

    sup = E.Support(O, dates, g1)
    _, gi = np.unique(dates, return_inverse=True)
    froz = {c: Frozen(sup, c, gi) for c in sup.cells}

    y_synth = E.composition_world(O, dates, seed=ORACLE_WORLD_SEED, noise=True)

    cells = sorted(sup.cells)
    meta = sup.meta.set_index("cell")
    per_cell = {}
    for c in cells:
        f = froz[c]
        row = meta.loc[c]
        per_cell[c] = {
            "support_hash": f.hash,
            "n_strata": int(len(f.sizes) // 2),
            "sizes": [int(x) for x in f.sizes],
            "weights_hex": [_hex(w) for w in np.atleast_1d(f.w)],
            "eligible_setups": int(row["eligible_setups"]),
            "eligible_cell_opportunities": int(row["eligible_cell_opportunities"]),
            "eligible_dates": int(row["eligible_dates"]),
            "support_fraction_hex": _hex(row["support_fraction"]),
            "theta_synthetic_hex": _hex(f.theta(y_synth)),
            "theta_real_hex": _hex(f.theta(v_real)),
        }

    payload = {
        "oracle_version": "v2_core_oracle_v1",
        "captured_at_commit": _commit(),
        "rng_namespace": ORACLE_RNG_NAMESPACE,
        "world_seed": ORACLE_WORLD_SEED,
        "population": {"rows": int(len(O)), "dates": int(len(np.unique(dates)))},
        "search_space": {
            "opportunity_level_cells": len(cells),
            "deferred_day_level": deferred,
            "deferred_reason": "INCOMPATIBLE_NULL_FAMILY / ComboLab v2.1",
        },
        "cells": cells,
        "per_cell": per_cell,
        "seconds": round(time.time() - t0, 1),
    }
    blob = json.dumps({k: payload[k] for k in ("cells", "per_cell", "search_space")},
                      sort_keys=True, separators=(",", ":"))
    payload["oracle_hash"] = hashlib.sha256(blob.encode()).hexdigest()[:16]
    return payload


def load() -> dict:
    with open(ORACLE) as f:
        return json.load(f)


def compare(fresh: dict, frozen: dict) -> list:
    """Every difference, field by field. Empty is the only acceptable answer."""
    diffs = []
    if fresh["cells"] != frozen["cells"]:
        diffs.append(("cells", set(frozen["cells"]) ^ set(fresh["cells"])))
        return diffs
    if fresh["search_space"] != frozen["search_space"]:
        diffs.append(("search_space", fresh["search_space"], frozen["search_space"]))
    for c in frozen["cells"]:
        a, b = fresh["per_cell"][c], frozen["per_cell"][c]
        for k in sorted(b):
            if a.get(k) != b[k]:
                diffs.append((c, k, a.get(k), b[k]))
    return diffs


if __name__ == "__main__":
    print("=" * 100, flush=True)
    print("  COMBOLAB v2 CORE ORACLE — the pre-refactor baseline", flush=True)
    print("=" * 100, flush=True)
    fresh = build()
    if os.path.exists(ORACLE) and "--force" not in sys.argv:
        frozen = load()
        diffs = compare(fresh, frozen)
        print(f"  frozen at   {frozen['captured_at_commit'][:12]}  hash {frozen['oracle_hash']}",
              flush=True)
        print(f"  recomputed  {fresh['captured_at_commit'][:12]}  hash {fresh['oracle_hash']}",
              flush=True)
        if diffs:
            print(f"\n  {len(diffs)} DIFFERENCE(S) — the core moved:", flush=True)
            for d in diffs[:20]:
                print(f"    {d}", flush=True)
            sys.exit(1)
        print("\n  IDENTICAL — bit for bit, on the frozen fixture", flush=True)
        sys.exit(0)
    with open(ORACLE, "w") as f:
        json.dump(fresh, f, indent=1, sort_keys=True)
    print(f"  captured {len(fresh['cells'])} OPPORTUNITY_LEVEL cells · "
          f"{len(fresh['search_space']['deferred_day_level'])} deferred DAY_LEVEL", flush=True)
    print(f"  population {fresh['population']['rows']:,} rows · "
          f"{fresh['population']['dates']:,} dates · {fresh['seconds']}s", flush=True)
    print(f"  oracle hash {fresh['oracle_hash']} at commit "
          f"{fresh['captured_at_commit'][:12]}", flush=True)
    print(f"  written to {os.path.basename(ORACLE)}", flush=True)
