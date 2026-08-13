"""4C · does the new orchestration reproduce the old sealed computation, on the whole unit.

Two independent proofs, and the second is not a repeat of the first:

    4C.1  old path vs run_v2() inside one process, all 31 cells, every rung
    4C.2  the same comparison across two FRESH processes — no warmed imports, no module
          state, no fixture monkeypatching, no runtime cache

This project has had enough defects living exactly at the runtime/artifact boundary — a spec
edited while its run was in flight, a value patched onto an already-written event, an oracle that
would have agreed with itself because the same process computed both sides — that "it matched in
one process" is not the same claim as "it matches".

THE LEGACY PROJECTION IS EVIDENCE, SO IT NEEDS ITS OWN NEGATIVE TEST. It is now part of the
proof, which makes it a place where the assertion can quietly check less than the claim. A
projection that dropped verdict stages, or compared rounded text, would pass while hiding a
divergence. So the comparison is run against deliberately corrupted artifacts and must fail on
each.

TWO IDENTITIES, DELIBERATELY DIFFERENT SHAPES:

    OutcomeIdentity  = f(world, δ)              two outer reps share one outcome
    RNGIdentity      = f(world, δ, cell, rep)

Binding `rep` into the outcome would be a false provenance link — it really is the same vector —
so the two are checked independently rather than folded into one hash that looks tidier.
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
import combolab_v2_spec as V2                                       # noqa: E402
import s1_run as S1R                                                # noqa: E402
import s1_spec as S1                                                # noqa: E402
import v2_engine as EN                                              # noqa: E402
import v2_engine_contract as C                                      # noqa: E402
import v2_kernel as K                                               # noqa: E402
import v2_sealed_run as SR                                          # noqa: E402
from studio_verdict import Estimate                                 # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "V2_EXTRACTION_EQUIVALENCE.json")

WORLD, DELTA, REP = 4242, 0.0, 0
SNAPSHOT = "v2-core-oracle-fixture"
ALIGNMENT = "opportunities-frozen-membership"
DELTA_STAR = 0.50
SPACE_HASH = "3600ae3dd52a25e6"


def _hex(x) -> str:
    return float(x).hex()


def _sha(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()[:16]


def _old_path(cell, froz, y, n_dates, meta):
    """Transcribed from the closure as it stood before the extraction."""
    f = froz[cell]
    rng = S1R.key_rng(WORLD, DELTA, cell, REP, "claim_bootstrap")
    p = np.full(n_dates, 1 / n_dates)
    d = np.array([f.theta(y, rng.multinomial(n_dates, p).astype(float))
                  for _ in range(S1.N_BOOT)])
    good = d[np.isfinite(d)]
    lo, hi = np.percentile(good, [2.5, 97.5])
    est = f.theta(y)
    e = Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo), ci_high=float(hi),
                 level=0.95, estimand=V2.ESTIMAND, method="clustered bootstrap",
                 cluster_unit="trading_date",
                 n_raw=int(meta.loc[cell, "eligible_cell_opportunities"]),
                 n_eff=int(meta.loc[cell, "eligible_dates"]))
    return {"theta_hex": _hex(f.theta(y)), "ci_low_hex": _hex(e.ci_low),
            "ci_high_hex": _hex(e.ci_high), "verdict": K.verdict(e, cell, meta, DELTA_STAR),
            "support_hash": f.hash}


def _fixture():
    O, _v, dates = CL.load_base(verbose=False)
    masks_all = CL.build_masks(O)
    classes = E.equivalence_classes(masks_all, verbose=False)
    masks = {c["representative"]: masks_all[c["representative"]] for c in classes}
    g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
    deferred = sorted(c for c in masks if c not in g1)
    sup = E.Support(O, dates, g1)
    _, gi = np.unique(dates, return_inverse=True)
    froz = {c: K.Frozen(sup, c, gi) for c in sup.cells}
    meta = sup.meta.set_index("cell")
    n_dates = int(gi.max() + 1)
    y = E.composition_world(O, dates, seed=WORLD, noise=True)
    return O, dates, sup, froz, meta, n_dates, y, deferred


def _spec() -> C.V2RunSpec:
    return C.V2RunSpec(estimand_version=V2.ESTIMAND, search_space_manifest_hash=SPACE_HASH,
                       support_policy_hash="6f825ca4763fea76", null_family="OPPORTUNITY_LEVEL",
                       decision_policy_hash="verdict_v2", bootstrap_policy_hash="boot_v1",
                       outcome_definition="composition world, sealed acceptance")


def _context() -> C.ExecutionContext:
    return C.ExecutionContext(
        C.SEALED_ACCEPTANCE, SNAPSHOT, "code-4c", C.SEALED_RNG,
        C.RNGMaterial(namespace="sealed_v2", provenance=f"freeze_commit:{SPACE_HASH}",
                      frozen_seeds=(WORLD,)), "exec-4c")


def canonical(which: str) -> dict:
    """One side of the comparison, computed from nothing but the fixture."""
    t0 = time.time()
    _O, _dates, sup, froz, meta, n_dates, y, deferred = _fixture()
    order = list(sup.cells)                        # the order support produced, never re-sorted

    if which == "old":
        results = {c: _old_path(c, froz, y, n_dates, meta) for c in order}
        art_meta = {}
    else:
        outcome = SR.sealed_outcome(y, WORLD, DELTA, SNAPSHOT, ALIGNMENT)
        SR.assert_outcome_matches_coordinates(outcome, WORLD, DELTA)
        provider = SR.SealedBootstrapRNGProvider(WORLD, DELTA, REP)
        art = EN.run_v2(_spec(), _context(), outcome, support=sup, frozen_cells=froz, meta=meta,
                        n_dates=n_dates, cell_order=order, rng_provider=provider,
                        registered_space_hash=SPACE_HASH, executed_space_hash=SPACE_HASH,
                        expected_alignment=ALIGNMENT, n_rows=len(y), n_boot=S1.N_BOOT,
                        delta_star=DELTA_STAR, deferred_claim_families=EN.default_deferred())
        results = art.legacy_projection()
        art_meta = {"artifact_hash": art.artifact_hash,
                    "computation_hash": art.computation_hash,
                    "executed_cell_order_hash": art.executed_cell_order_hash,
                    "deferred": art.deferred_claim_families}

    return {"which": which, "cells": order, "n_deferred": len(deferred),
            "results": results, "artifact": art_meta,
            "code": {"v2_kernel": _sha(os.path.join(HERE, "v2_kernel.py")),
                     "v2_engine": _sha(os.path.join(HERE, "v2_engine.py"))},
            "spec_hash": _spec().spec_hash, "execution_mode": C.SEALED_ACCEPTANCE,
            "seconds": round(time.time() - t0, 1)}


def compare(a: dict, b: dict) -> list:
    """Every difference, structure first, then computation. Empty is the only pass."""
    diffs = []
    if a["cells"] != b["cells"]:
        diffs.append(("cell_order", "differs"))
        return diffs
    if a["n_deferred"] != b["n_deferred"] != 6:
        diffs.append(("deferred", a["n_deferred"], b["n_deferred"]))
    for cell in a["cells"]:
        ra, rb = a["results"][cell], b["results"][cell]
        for field in ("support_hash", "theta_hex", "ci_low_hex", "ci_high_hex", "verdict"):
            if ra[field] != rb[field]:
                diffs.append((cell, field, ra[field], rb[field]))
    return diffs


def corrupt(result: dict, how: str) -> dict:
    """Deliberate damage, to prove the comparison can see it."""
    out = json.loads(json.dumps(result))
    cells = out["cells"]
    if how == "swap_cells":
        a, b = cells[3], cells[17]
        out["results"][a], out["results"][b] = out["results"][b], out["results"][a]
    elif how == "flip_theta_bit":
        r = out["results"][cells[0]]
        r["theta_hex"] = float(float.fromhex(r["theta_hex"]) * (1 + 2**-52)).hex()
    elif how == "change_verdict":
        r = out["results"][cells[0]]
        r["verdict"] = "BUILD" if r["verdict"] != "BUILD" else "NULL"
    return out


if __name__ == "__main__":
    if "--emit" in sys.argv:
        side = sys.argv[sys.argv.index("--emit") + 1]
        out = canonical(side)
        with open(os.path.join(HERE, f".v2_equiv_{side}.json"), "w") as f:
            json.dump(out, f, sort_keys=True)
        print(f"  {side}: {len(out['cells'])} cells · {out['seconds']}s", flush=True)
        sys.exit(0)

    print("=" * 100, flush=True)
    print("  4C · EXTRACTION EQUIVALENCE — one process, then two fresh ones", flush=True)
    print("=" * 100, flush=True)

    print("\n  4C.1 · old path vs run_v2 in one process", flush=True)
    old = canonical("old")
    new = canonical("new")
    d1 = compare(old, new)
    print(f"    {len(old['cells'])} cells · {old['seconds']}s + {new['seconds']}s · "
          f"{'EXACT' if not d1 else f'{len(d1)} DIFFERENCES'}", flush=True)
    for x in d1[:5]:
        print(f"      {x}", flush=True)

    print("\n  negative control · the comparison must see damage", flush=True)
    negatives = {}
    for how in ("swap_cells", "flip_theta_bit", "change_verdict"):
        seen = bool(compare(old, corrupt(new, how)))
        negatives[how] = seen
        print(f"    {how:<18s} {'CAUGHT' if seen else 'MISSED — the projection is too weak'}",
              flush=True)

    print("\n  4C.2 · two fresh processes", flush=True)
    env = dict(os.environ)
    for side in ("old", "new"):
        subprocess.run([sys.executable, os.path.abspath(__file__), "--emit", side],
                       cwd=HERE, env=env, check=True)
    with open(os.path.join(HERE, ".v2_equiv_old.json")) as f:
        fold = json.load(f)
    with open(os.path.join(HERE, ".v2_equiv_new.json")) as f:
        fnew = json.load(f)
    d2 = compare(fold, fnew)
    print(f"    {'EXACT' if not d2 else f'{len(d2)} DIFFERENCES'}", flush=True)

    ok = (not d1) and (not d2) and all(negatives.values())
    payload = {"claim": "V2_EXTRACTION_EQUIVALENCE",
               "execution_mode": C.SEALED_ACCEPTANCE,
               "one_process_exact": not d1, "fresh_process_exact": not d2,
               "negative_controls": negatives,
               "cells": len(old["cells"]), "deferred_day_level": old["n_deferred"],
               "spec_hash": old["spec_hash"], "code": old["code"],
               "reference_code_hash": old["code"]["v2_kernel"],
               "candidate_code_hash": new["code"]["v2_engine"],
               "oracle_hash": "7c421ae062742d06",
               "artifact": new["artifact"],
               "world": WORLD, "delta": DELTA, "rep": REP,
               "historical_execution": "BLOCKED"}
    with open(REPORT, "w") as f:
        json.dump(payload, f, indent=1, sort_keys=True)
    for side in ("old", "new"):
        os.remove(os.path.join(HERE, f".v2_equiv_{side}.json"))
    print(f"\n  {'PASS' if ok else 'FAIL'} · written to {os.path.basename(REPORT)}", flush=True)
    sys.exit(0 if ok else 1)
