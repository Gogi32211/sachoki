"""Gate 1 · the registered real-y numerical qualification. Criteria frozen at 601f359bf5f47184.

WHY THIS DOES NOT GO THROUGH `run_v2(HISTORICAL_RESEARCH)`. That path is blocked BY this gate.
Routing the qualification through it would mean the gate opened itself, so the harness calls the
proven kernel primitive directly under an explicit qualification context. The normal engine
route stays shut throughout, and a test asserts it.

WHY THE REPLICATE LOOP IS HERE RATHER THAN `bootstrap_cell`. The kernel does
`good = d[np.isfinite(d)]` and takes percentiles over the survivors. Under the frozen v2 design
that is intended — a replication that cannot be evaluated is dropped — but the count never
surfaces, and the registered policy requires `valid + rejected == requested` per cell with a
floor of 1.0. So this harness runs the same estimator call, `Frozen.theta(y, w_date)`, and
classifies every replicate instead of filtering silently. Where nothing is rejected the two agree
exactly; where something is, the difference is the finding rather than a hidden one.

WHAT THIS GATE ASKS, and it is narrow on purpose: does the frozen computation contract survive
the real outcome vector without acquiring new rules. It does not ask whether the numbers are
interesting, and the spec lists the criteria it explicitly is NOT — no "at least N BUILD", no
"θ positive", no "interval excludes zero".

THE OUTCOME OF A FAILED RUN IS KEPT. If real-y surfaces a numerical defect, fixing the code does
not unmake the fact that a first application happened; the failed run stays in the qualification
history with its reason, and the θ it exposed keep the role they were produced under.
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
import outcome_integrity as OI                                      # noqa: E402
import real_y_qualification_spec as SPEC                            # noqa: E402
import v2_engine_contract as C                                      # noqa: E402
import v2_kernel as K                                               # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "V2_REAL_Y_QUALIFICATION.json")

SNAPSHOT = "opportunities-parquet-2026-08-12"
ALIGNMENT = "opportunities-frozen-membership"
DELTA_STAR = 0.50


class HistoricalBootstrapRNGProvider:
    """`historical_research_rng_v1` — deterministic from the question and the data, nothing else.

    Not the session, not the run id, not the rerun number, not the clock. A technical re-run must
    return the identical interval, or `revisit` — which the ledger charges nothing for — becomes
    a way to shop for Monte-Carlo noise.

    The material carries no sealed lineage, and `ExecutionContext` refuses it if it ever does.
    """

    POLICY_ID = "historical_research_rng_v1"

    def __init__(self, keyed_root: str):
        self.keyed_root = keyed_root

    def semantic_key(self, cell) -> tuple:
        return (self.POLICY_ID, self.keyed_root, cell, "claim_bootstrap")

    def open_stream(self, cell):
        h = hashlib.sha256("|".join(self.semantic_key(cell)).encode()).digest()
        return np.random.default_rng(int.from_bytes(h[:8], "big"))


def _spec() -> C.V2RunSpec:
    return C.V2RunSpec(estimand_version=V2.ESTIMAND,
                       search_space_manifest_hash="3600ae3dd52a25e6",
                       support_policy_hash="6f825ca4763fea76", null_family="OPPORTUNITY_LEVEL",
                       decision_policy_hash="verdict_v2",
                       bootstrap_policy_hash=SPEC.spec_hash(),
                       outcome_definition="O['ret'] * 100 within frozen strata")


def qualify() -> dict:
    t0 = time.time()
    O, v_real, dates = CL.load_base(verbose=False)
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
    order = list(sup.cells)

    # The outcome, declared for what it is. HISTORICAL_OBSERVED, and the modes refuse each
    # other's data — a sealed run could not be handed this vector.
    outcome = C.OutcomeVector(
        values=v_real, outcome_id="O.ret*100", outcome_semantics="realised return, per cent",
        source_kind=C.HISTORICAL_OBSERVED, source_snapshot_id=SNAPSHOT, units="pp",
        row_alignment_hash=ALIGNMENT,
        construction_hash=hashlib.sha256(
            f"ret*100|{len(v_real)}|{SNAPSHOT}".encode()).hexdigest()[:16])

    spec = _spec()
    material = C.research_rng_material(spec, SNAPSHOT)
    provider = HistoricalBootstrapRNGProvider(material.keyed_root)

    # admissibility, once, before anything is estimated
    used = np.unique(np.concatenate([froz[c].flat for c in order]))
    OI.assert_outcomes_admissible(v_real, used, where="real-y qualification")
    nonfinite = int((~np.isfinite(v_real[used])).sum())

    p = np.full(n_dates, 1 / n_dates)
    cells_report = []
    for cell in order:
        f = froz[cell]
        rng = provider.open_stream(cell)
        reps = []
        for i in range(SPEC.BOOTSTRAP_REPS_REQUESTED):
            w = rng.multinomial(n_dates, p).astype(float)
            val = f.theta(v_real, w)                       # the same estimator call as the kernel
            # `theta` returns NaN exactly when a stratum's arm is unrepresented in the resample,
            # which is the frozen uncomputable-stratum contract. It is classified, not filtered.
            reps.append(OI.classify_replicate(
                i, val, arms_present=bool(np.isfinite(val)), strata_nonempty=True))
        cells_report.append(OI.assess_cell(cell, f.theta(v_real), reps,
                                           requested=SPEC.BOOTSTRAP_REPS_REQUESTED))

    rep = OI.build_report(rows_total=int(len(v_real)), rows_used=int(used.size),
                          nonfinite=nonfinite, requested_cells=len(order), cells=cells_report)
    d = rep.as_dict()
    d.update({
        "claim": "REAL_Y_NUMERICAL_QUALIFICATION",
        "execution_mode": "QUALIFICATION_ONLY — run_v2(HISTORICAL_RESEARCH) remains blocked",
        "rng_policy_id": HistoricalBootstrapRNGProvider.POLICY_ID,
        "rng_provenance": material.provenance,
        "rng_is_sealed_lineage": material.is_sealed_lineage,
        "spec_hash_registered": SPEC.spec_hash(),
        "reps_requested": SPEC.BOOTSTRAP_REPS_REQUESTED,
        "valid_floor": SPEC.BOOTSTRAP_VALID_FLOOR,
        "deferred_day_level": deferred,
        "evidence_status": {"evidence_origin": "HISTORICAL_RESEARCH",
                            "instrument_validation_basis": "SYNTHETIC_CAPABILITY_VALIDATED",
                            "application_maturity": "FIRST_HISTORICAL_APPLICATION",
                            "result_role": "ENGINE_QUALIFICATION_EVIDENCE"},
        "seconds": round(time.time() - t0, 1),
    })
    return d


if __name__ == "__main__":
    if "--emit" in sys.argv:
        out = qualify()
        tag = sys.argv[sys.argv.index("--emit") + 1]
        with open(os.path.join(HERE, f".realy_{tag}.json"), "w") as f:
            json.dump({c["cell"]: c for c in out["cells"]}, f, sort_keys=True)
        print(f"  pass {tag}: {out['cells_publishable']}/{out['cells_requested']} publishable · "
              f"{out['seconds']}s", flush=True)
        sys.exit(0)

    print("=" * 100, flush=True)
    print(f"  REAL-Y NUMERICAL QUALIFICATION · spec {SPEC.spec_hash()}", flush=True)
    print("=" * 100, flush=True)
    print("  criteria were registered before this ran; none of them reads the answer", flush=True)

    r = qualify()
    print(f"\n  cells addressed        {len(r['cells'])} / {r['cells_requested']}", flush=True)
    print(f"  non-finite outcomes    {r['nonfinite_y']}", flush=True)
    print(f"  theta finite           {r['cells_theta_finite']}", flush=True)
    print(f"  publishable            {r['cells_publishable']}", flush=True)
    print(f"  degenerate but valid   {r['cells_degenerate_but_valid']}", flush=True)
    print(f"  numerically invalid    {r['cells_numerically_invalid']}", flush=True)
    print(f"  uncomputable           {r['cells_uncomputable']}", flush=True)
    print(f"  deferred DAY_LEVEL     {len(r['deferred_day_level'])}", flush=True)
    print(f"  seconds                {r['seconds']}", flush=True)

    rejected = {}
    for c in r["cells"]:
        for k, n in (c["rejection_reasons"] or {}).items():
            rejected[k] = rejected.get(k, 0) + n
    print(f"\n  replicate rejections   {rejected or 'none'}", flush=True)
    for c in r["cells"]:
        if not c["publishable"]:
            print(f"    UNCOMPUTABLE {c['cell']:<26s} {c['uncomputable_reason']} "
                  f"({c['reps_valid']}/{c['reps_requested']} valid)", flush=True)

    fails = r["failures"]
    print(f"\n  registered criteria    {'ALL HELD' if not fails else f'{len(fails)} FAILED'}",
          flush=True)
    for f_ in fails[:10]:
        print(f"    {f_}", flush=True)

    # determinism: the same spec and snapshot must reproduce bit for bit in a fresh process
    print("\n  determinism · two fresh processes", flush=True)
    for tag in ("a", "b"):
        subprocess.run([sys.executable, os.path.abspath(__file__), "--emit", tag],
                       cwd=HERE, check=True)
    with open(os.path.join(HERE, ".realy_a.json")) as f:
        a = json.load(f)
    with open(os.path.join(HERE, ".realy_b.json")) as f:
        b = json.load(f)
    same = a == b
    print(f"    {'BIT-IDENTICAL' if same else 'DIVERGED'}", flush=True)
    for tag in ("a", "b"):
        os.remove(os.path.join(HERE, f".realy_{tag}.json"))

    r["deterministic_rerun"] = same
    r["status"] = "PASSED" if (not fails and same) else "FAILED"
    with open(REPORT, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)
    print(f"\n  QUALIFICATION {r['status']} · written to {os.path.basename(REPORT)}", flush=True)
    print("  run_v2(HISTORICAL_RESEARCH) remains blocked either way; opening it is a separate "
          "decision", flush=True)
    sys.exit(0)
