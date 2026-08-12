"""3B.2 · the proof, and it is not "the source is identical" — because it is not.

`boot` changed shape. `Frozen` and `verdict` could be moved line for line and checked by reading
the diff; this one had a dependency inverted, so the claim rests on a trace instead. The ladder
is ordered so that the FIRST level that diverges names the class of defect:

    semantic key tuple      wrong parameterisation of the legacy keyed RNG
    stream provenance       a different generator for the same key
    sampled-date geometry   RNG consumption or order changed
    arm computability       support or resampling logic changed
    bootstrap values        the estimator path changed
    interval, verdict       aggregation or decision changed

THE TRACE CONSUMES NOTHING. A recorder that called `rng.random()` to fingerprint the stream
would advance the state it is inspecting and change the computation it exists to check. So the
recorder wraps `multinomial` and observes the draws the bootstrap makes anyway.

ONE STREAM, IN ORDER. The old closure built a single generator per (world, δ, cell, rep) and
drew from it `n_boot` times. The ordered hash of the draws is compared, not a multiset: the same
samples in a different order would leave every summary statistic intact and still be a different
RNG geometry.

THE OLD PATH IS TRANSCRIBED HERE, verbatim from the closure as it stood at 67a03cf, so that both
sides can run in one process on identical inputs. Comparing against a description of the old
code would prove only that the description matches.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as E                                             # noqa: E402
import combolab_v2_spec as V2                                       # noqa: E402
import s1_run as S1R                                                # noqa: E402
import s1_spec as S1                                                # noqa: E402
import v2_kernel as K                                               # noqa: E402
from studio_verdict import Estimate                                 # noqa: E402
from v2_sealed_run import SealedBootstrapRNGProvider                # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "V2_BOOTSTRAP_EQUIVALENCE.json")

# Not sealed seeds. This comparison is between two implementations on identical inputs; using
# the acceptance's own seeds would spend sealed material to check a refactor. The artifact says
# so in fields nobody has to interpret, because in six months a file full of exact float bits
# from the v2 tract is easy to mistake for part of the sealed capability evidence.
EXECUTION_PURPOSE = "EXTRACTION_REGRESSION"
RNG_ORIGIN = "NON_SEALED_TEST_FIXTURE"
TRACE_WORLD, TRACE_DELTA, TRACE_REP = 4242, 0.0, 0

# One value from the registered grid, for the perturbation check. Which δ is immaterial here —
# the claim is about invariance and about old-vs-new, not about detection.
PERTURB_DELTA = 1.50


class _Recorder:
    """Passes the stream through and remembers what the bootstrap drew. Draws nothing itself."""

    def __init__(self, rng):
        self._rng = rng
        self.draws: list = []

    def multinomial(self, n, p, *a, **kw):
        out = self._rng.multinomial(n, p, *a, **kw)
        self.draws.append(np.flatnonzero(out).tobytes() + out[np.flatnonzero(out)].tobytes())
        return out

    @property
    def ordered_hash(self) -> str:
        h = hashlib.sha256()
        for d in self.draws:                     # order matters, so it is hashed in order
            h.update(d)
        return h.hexdigest()[:16]


def _old_boot(cell, froz, y, n_dates, meta, world, delta, rep, rng=None):
    """Transcribed verbatim from the closure at 67a03cf, with the stream injectable for tracing."""
    f = froz[cell]
    rng = rng if rng is not None else S1R.key_rng(world, delta, cell, rep, "claim_bootstrap")
    p = np.full(n_dates, 1 / n_dates)
    d = np.array([f.theta(y, rng.multinomial(n_dates, p).astype(float))
                  for _ in range(S1.N_BOOT)])
    good = d[np.isfinite(d)]
    lo, hi = np.percentile(good, [2.5, 97.5])
    est = f.theta(y)
    return Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo),
                    ci_high=float(hi), level=0.95, estimand=V2.ESTIMAND,
                    method="clustered bootstrap", cluster_unit="trading_date",
                    n_raw=int(meta.loc[cell, "eligible_cell_opportunities"]),
                    n_eff=int(meta.loc[cell, "eligible_dates"]))


def _hex(x) -> str:
    return float(x).hex()


def _fixture():
    """Population, support and frozen layouts. Built once and shared by both runs."""
    O, v_real, dates = CL.load_base(verbose=True)
    masks_all = CL.build_masks(O)
    classes = E.equivalence_classes(masks_all, verbose=False)
    masks = {c["representative"]: masks_all[c["representative"]] for c in classes}
    g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
    sup = E.Support(O, dates, g1)
    _, gi = np.unique(dates, return_inverse=True)
    froz = {c: K.Frozen(sup, c, gi) for c in sup.cells}
    meta = sup.meta.set_index("cell")
    n_dates = int(gi.max() + 1)
    y0 = E.composition_world(O, dates, seed=TRACE_WORLD, noise=True)
    return O, dates, sup, froz, meta, n_dates, y0


def _compare_cell(cell, froz, y, n_dates, meta, delta):
    """One rung-by-rung comparison for one cell on one outcome vector."""
    provider = SealedBootstrapRNGProvider(TRACE_WORLD, delta, TRACE_REP)
    legacy_key = (TRACE_WORLD, delta, cell, TRACE_REP, "claim_bootstrap")
    provider_key = provider.semantic_key(cell)

    old_rec = _Recorder(S1R.key_rng(*legacy_key))
    new_rec = _Recorder(provider.open_stream(cell))
    old = _old_boot(cell, froz, y, n_dates, meta, TRACE_WORLD, delta, TRACE_REP, rng=old_rec)
    new = K.bootstrap_cell(y, froz[cell], cell, new_rec, n_dates, S1.N_BOOT, meta, V2.ESTIMAND)

    row = {
        "cell": cell, "delta": delta,
        "semantic_key_equal": legacy_key == provider_key,
        "stream_requests_old": len(old_rec.draws), "stream_requests_new": len(new_rec.draws),
        "sampled_geometry_equal": old_rec.ordered_hash == new_rec.ordered_hash,
        "sampled_geometry_hash": old_rec.ordered_hash,
        "estimate_hex_old": _hex(old.estimate), "estimate_hex_new": _hex(new.estimate),
        "ci_low_hex_old": _hex(old.ci_low), "ci_low_hex_new": _hex(new.ci_low),
        "ci_high_hex_old": _hex(old.ci_high), "ci_high_hex_new": _hex(new.ci_high),
        "verdict_old": K.verdict(old, cell, meta, 0.50),
        "verdict_new": K.verdict(new, cell, meta, 0.50),
    }
    row["identical"] = (row["semantic_key_equal"] and row["sampled_geometry_equal"]
                        and row["stream_requests_old"] == row["stream_requests_new"]
                        and row["estimate_hex_old"] == row["estimate_hex_new"]
                        and row["ci_low_hex_old"] == row["ci_low_hex_new"]
                        and row["ci_high_hex_old"] == row["ci_high_hex_new"]
                        and row["verdict_old"] == row["verdict_new"])
    return row


def run_all_cells() -> dict:
    """A · every registered cell at δ=0.

    Six cells prove the mechanism. Thirty-one prove the move was not correct only on the support
    geometry that happened to be picked: E_c, weights, date spread and stratum counts differ
    sharply between classes, and a bootstrap inversion could survive one shape and not another.
    """
    t0 = time.time()
    _O, _dates, sup, froz, meta, n_dates, y0 = _fixture()
    cells = sorted(sup.cells)
    ladder = [_compare_cell(c, froz, y0, n_dates, meta, TRACE_DELTA) for c in cells]
    return _summarise("BOOT_EXTRACTION_FULL_CELL_EQUIVALENCE", cells, ladder, t0,
                      {"delta": TRACE_DELTA})


def run_outcome_perturbation() -> dict:
    """B · a different outcome vector, and the geometry that must not notice.

    Two separate claims live here, and only the first is about old-vs-new:

        OLD@δ>0 == NEW@δ>0            the inversion still holds on a perturbed outcome
        geometry(δ=0) == geometry(δ>0)  resampling does not depend on y

    What is NOT required is that the verdict or the interval match between δ=0 and δ>0. They
    are computed on different outcomes and are entitled to differ; requiring otherwise would be
    demanding that an injected effect have no effect.
    """
    t0 = time.time()
    O, dates, sup, froz, meta, n_dates, y0 = _fixture()
    m = meta.copy()
    m["n_strata"] = [len(sup.strata[c]) for c in m.index]
    chosen = []
    for label, cell in (("high_support", m["support_fraction"].idxmax()),
                        ("low_support", m["support_fraction"].idxmin()),
                        ("most_strata", m["n_strata"].idxmax()),
                        ("fewest_strata", m["n_strata"].idxmin()),
                        ("most_dates", m["eligible_dates"].idxmax()),
                        ("fewest_dates", m["eligible_dates"].idxmin())):
        if cell not in [c for _, c in chosen]:
            chosen.append((label, cell))
    planted = chosen[0][1]
    y1 = E.inject(y0, sup, planted, PERTURB_DELTA)

    ladder, invariance = [], []
    for label, cell in chosen:
        at0 = _compare_cell(cell, froz, y0, n_dates, meta, TRACE_DELTA)
        at1 = _compare_cell(cell, froz, y1, n_dates, meta, TRACE_DELTA)
        at1["selection_reason"] = label
        ladder.append(at1)
        invariance.append({
            "cell": cell, "reason": label,
            "geometry_delta0": at0["sampled_geometry_hash"],
            "geometry_perturbed": at1["sampled_geometry_hash"],
            "geometry_invariant": at0["sampled_geometry_hash"] == at1["sampled_geometry_hash"],
            "outcome_actually_changed": at0["estimate_hex_old"] != at1["estimate_hex_old"],
        })
    out = _summarise("OUTCOME_PERTURBATION_EQUIVALENCE", [c for _, c in chosen], ladder, t0,
                     {"delta": PERTURB_DELTA, "planted_cell": planted})
    out["invariance"] = invariance
    out["geometry_invariant"] = all(i["geometry_invariant"] for i in invariance)
    out["outcome_really_moved"] = any(i["outcome_actually_changed"] for i in invariance)
    return out


def _summarise(name, cells, ladder, t0, extra) -> dict:
    rungs = {
        "semantic_key": all(r["semantic_key_equal"] for r in ladder),
        "stream_request_count": all(r["stream_requests_old"] == r["stream_requests_new"]
                                    for r in ladder),
        "sampled_geometry": all(r["sampled_geometry_equal"] for r in ladder),
        "bootstrap_values": all(r["estimate_hex_old"] == r["estimate_hex_new"] for r in ladder),
        "intervals": all(r["ci_low_hex_old"] == r["ci_low_hex_new"]
                         and r["ci_high_hex_old"] == r["ci_high_hex_new"] for r in ladder),
        "verdicts": all(r["verdict_old"] == r["verdict_new"] for r in ladder),
    }
    return {"claim": name, "execution_purpose": EXECUTION_PURPOSE, "rng_origin": RNG_ORIGIN,
            "world": TRACE_WORLD, "rep": TRACE_REP, "n_boot": S1.N_BOOT,
            "cells": cells, "ladder": ladder, "rungs": rungs,
            "all_identical": all(r["identical"] for r in ladder),
            "seconds": round(time.time() - t0, 1), **extra}


if __name__ == "__main__":
    RUNGS = ("semantic_key", "stream_request_count", "sampled_geometry", "bootstrap_values",
             "intervals", "verdicts")

    def report(r):
        print(f"\n  {r['claim']}", flush=True)
        for name in RUNGS:
            print(f"    {name:<22s} {'EXACT' if r['rungs'][name] else 'DIVERGED'}", flush=True)
        print(f"    {len(r['cells'])} cells · {r['n_boot']} replicates · δ={r['delta']} · "
              f"{r['seconds']}s", flush=True)

    print("=" * 100, flush=True)
    print("  3B.2 · BOOTSTRAP DEPENDENCY INVERSION — two claims, not one", flush=True)
    print("=" * 100, flush=True)

    a = run_all_cells()
    report(a)

    b = run_outcome_perturbation()
    report(b)
    print(f"    planted {b['planted_cell']}", flush=True)
    for inv in b["invariance"]:
        mark = "INVARIANT" if inv["geometry_invariant"] else "MOVED"
        print(f"      {inv['reason']:<15s} {inv['cell']:<26s} geometry {mark} · "
              f"outcome changed {inv['outcome_actually_changed']}", flush=True)

    out = {"execution_purpose": EXECUTION_PURPOSE, "rng_origin": RNG_ORIGIN,
           "full_cell_equivalence": a, "outcome_perturbation": b}
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)

    ok_a = a["all_identical"]
    ok_b = b["all_identical"] and b["geometry_invariant"] and b["outcome_really_moved"]
    print(f"\n  A · full cell equivalence      {'PASS' if ok_a else 'FAIL'}", flush=True)
    print(f"  B · outcome perturbation       {'PASS' if ok_b else 'FAIL'}", flush=True)
    if not b["outcome_really_moved"]:
        print("      (the injection changed nothing, so B proved nothing)", flush=True)
    print(f"  written to {os.path.basename(REPORT)}", flush=True)
    sys.exit(0 if (ok_a and ok_b) else 1)
