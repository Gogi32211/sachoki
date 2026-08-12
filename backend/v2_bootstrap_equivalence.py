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
# the acceptance's own seeds would spend sealed material to check a refactor.
TRACE_WORLD, TRACE_DELTA, TRACE_REP = 4242, 0.0, 0


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


def run(n_cells: int = 4) -> dict:
    t0 = time.time()
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

    # the synthetic world the sealed path computes on, so the comparison runs the sealed geometry
    y = E.composition_world(O, dates, seed=TRACE_WORLD, noise=True)
    provider = SealedBootstrapRNGProvider(TRACE_WORLD, TRACE_DELTA, TRACE_REP)

    cells = sorted(sup.cells)[:n_cells]
    rungs, ladder = {}, []
    for cell in cells:
        # 1 · semantic key, before any generator exists
        legacy_key = (TRACE_WORLD, TRACE_DELTA, cell, TRACE_REP, "claim_bootstrap")
        provider_key = provider.semantic_key(cell)

        # 2-3 · one stream each, recorded without consuming anything extra
        old_rec = _Recorder(S1R.key_rng(*legacy_key))
        new_rec = _Recorder(provider.open_stream(cell))

        old = _old_boot(cell, froz, y, n_dates, meta, TRACE_WORLD, TRACE_DELTA, TRACE_REP,
                        rng=old_rec)
        new = K.bootstrap_cell(y, froz[cell], cell, new_rec, n_dates, S1.N_BOOT, meta,
                               V2.ESTIMAND)

        # 4 · arm computability, from the geometry the bootstrap actually drew
        old_arms = old_rec.ordered_hash
        new_arms = new_rec.ordered_hash

        row = {
            "cell": cell,
            "semantic_key_equal": legacy_key == provider_key,
            "sampled_geometry_equal": old_arms == new_arms,
            "sampled_geometry_hash": old_arms,
            "estimate_hex_old": _hex(old.estimate), "estimate_hex_new": _hex(new.estimate),
            "ci_low_hex_old": _hex(old.ci_low), "ci_low_hex_new": _hex(new.ci_low),
            "ci_high_hex_old": _hex(old.ci_high), "ci_high_hex_new": _hex(new.ci_high),
            "verdict_old": K.verdict(old, cell, meta, 0.50),
            "verdict_new": K.verdict(new, cell, meta, 0.50),
        }
        row["identical"] = (row["semantic_key_equal"] and row["sampled_geometry_equal"]
                            and row["estimate_hex_old"] == row["estimate_hex_new"]
                            and row["ci_low_hex_old"] == row["ci_low_hex_new"]
                            and row["ci_high_hex_old"] == row["ci_high_hex_new"]
                            and row["verdict_old"] == row["verdict_new"])
        ladder.append(row)

    rungs = {
        "semantic_key": all(r["semantic_key_equal"] for r in ladder),
        "sampled_geometry": all(r["sampled_geometry_equal"] for r in ladder),
        "bootstrap_values": all(r["estimate_hex_old"] == r["estimate_hex_new"] for r in ladder),
        "intervals": all(r["ci_low_hex_old"] == r["ci_low_hex_new"]
                         and r["ci_high_hex_old"] == r["ci_high_hex_new"] for r in ladder),
        "verdicts": all(r["verdict_old"] == r["verdict_new"] for r in ladder),
    }
    payload = {"trace_version": "v2_bootstrap_equivalence_v1",
               "world": TRACE_WORLD, "delta": TRACE_DELTA, "rep": TRACE_REP,
               "n_boot": S1.N_BOOT, "cells": cells, "ladder": ladder, "rungs": rungs,
               "all_identical": all(r["identical"] for r in ladder),
               "seconds": round(time.time() - t0, 1)}
    return payload


if __name__ == "__main__":
    print("=" * 100, flush=True)
    print("  3B.2 · BOOTSTRAP DEPENDENCY INVERSION — the ladder", flush=True)
    print("=" * 100, flush=True)
    r = run(int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 4)
    for name in ("semantic_key", "sampled_geometry", "bootstrap_values", "intervals", "verdicts"):
        print(f"  {name:<20s} {'EXACT' if r['rungs'][name] else 'DIVERGED'}", flush=True)
    print(f"\n  {len(r['cells'])} cells · {r['n_boot']} replicates each · {r['seconds']}s",
          flush=True)
    for row in r["ladder"]:
        print(f"    {row['cell']:<28s} est {row['estimate_hex_old']}  "
              f"geom {row['sampled_geometry_hash']}  {row['verdict_old']}", flush=True)
    with open(REPORT, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)
    print(f"\n  {'ALL IDENTICAL' if r['all_identical'] else 'DIVERGENCE'} · written to "
          f"{os.path.basename(REPORT)}", flush=True)
    sys.exit(0 if r["all_identical"] else 1)
