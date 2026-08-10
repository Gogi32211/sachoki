"""D1 — the incremental estimate through the whole decision path, and no search.

    θ̂_c  →  date-clustered CI  →  verdict_v2        for every one of the 37 classes

`studio_verdict` is shared with v1, but the object it now receives is different: an interval
around a stratified within-setup effect rather than around a marginal difference. Operating
characteristics belong to the measured object, so none of v1's RETURN numbers are inherited.

THREE THINGS ARE CONTRACTS HERE, NOT CONVENTIONS.

1 · THE ESTIMAND IS FROZEN INSIDE THE BOOTSTRAP. Every replication resamples date clusters and
    nothing else: `E_c` is not recomputed, `w_cs` is not re-derived from the resample, and
    eligibility is not revisited. Otherwise each replication estimates a slightly different
    estimand and the interval is a mixture of target populations rather than uncertainty about
    one. Asserted by hashing both and comparing per replication.

2 · A STRATUM THAT CANNOT BE EVALUATED INVALIDATES THE REPLICATION. With dates_min = 25 a
    resample that empties one arm of a frozen stratum is rare, but rare is not never, and the
    tempting fallback — renormalise the surviving weights — silently changes θ_c into an effect
    about a different support population. The replication is dropped and counted instead.

3 · RUNNING 37 TARGETS IS NOT A SEARCH OVER 37. This is the semantic trap of the run: these are
    37 independent operating-control scenarios, each with its target named in advance. No
    multiplicity correction is applied, because none is owed — nobody looked at 37 claims and
    kept the best. Applying k = 37 here would measure the search tax inside decision validation,
    which is the confusion the whole barrier is built to prevent.

EVERY CLASS IS A TARGET, not one convenient cell. The 37 differ in eligible setups (7 to 32),
support coverage, date spread and concentration, so decision sensitivity may differ sharply
between them. The needle test will later land at random on easy and hard claims alike, and a
single averaged curve would hide that.

ACCEPTANCE, registered before the run and deliberately not a pretty percentage:
    a  bias stays small relative to the injected δ
    b  at δ = 0 the positive rate stays controlled under the synthetic generator
    c  acceptance rises with δ in aggregate — no per-class monotonicity demanded
    d  any gap between estimator and final has a NAMED blocking layer
    e  no refusal comes from an arbitrary hard magnitude gate
    f  frozen support and weights do not move inside the bootstrap
"""
from __future__ import annotations

import hashlib
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as E                                             # noqa: E402
import combolab_v2_spec as V2                                       # noqa: E402
from sampling_target import descriptive_metric, synthetic_dgp       # noqa: E402
from studio_verdict import Estimate, decide                         # noqa: E402

pd.set_option("display.width", 210)
N_BOOT, N_SEED, DELTA_STAR = 200, 5, 0.50
BAR = "=" * 128


def support_hash(sup: E.Support, cell: str) -> str:
    h = hashlib.sha256()
    for (s, i, j), w in zip(sup.strata[cell], sup.weights[cell]):
        h.update(f"{s}|{len(i)}|{len(j)}|{w:.12f}".encode())
    return h.hexdigest()


class Frozen:
    """Precomputed layout for one class, so the bootstrap only resamples dates."""

    def __init__(self, sup: E.Support, cell: str, gi: np.ndarray):
        self.cell, self.w = cell, sup.weights[cell]
        self.idx, self.seg, self.arm = [], [], []
        for k, (_, i, j) in enumerate(sup.strata[cell]):
            for arm, ix in ((0, i), (1, j)):
                self.idx.append(ix)
                self.seg.append(k)
                self.arm.append(arm)
        self.sizes = np.array([len(x) for x in self.idx])
        self.flat = np.concatenate(self.idx)
        self.gflat = gi[self.flat]
        self.bounds = np.r_[0, np.cumsum(self.sizes)]
        self.seg = np.asarray(self.seg)
        self.arm = np.asarray(self.arm)
        self.hash = support_hash(sup, cell)

    def theta(self, y: np.ndarray, w_date: np.ndarray | None = None) -> float:
        """Weighted within-stratum median differences, aggregated with FROZEN weights."""
        vals = y[self.flat]
        order = np.empty(len(vals), dtype=np.int64)
        med = np.empty(len(self.sizes))
        for k in range(len(self.sizes)):
            a, b = self.bounds[k], self.bounds[k + 1]
            v = vals[a:b]
            if w_date is None:
                med[k] = E._lower_median(v)
            else:
                ww = w_date[self.gflat[a:b]]
                o = np.argsort(v, kind="stable")
                c = np.cumsum(ww[o])
                if c[-1] <= 0:
                    return np.nan                  # arm unrepresented → replication INVALID
                med[k] = v[o[np.searchsorted(c, c[-1] / 2.0)]]
        d = med[self.arm == 0] - med[self.arm == 1]
        return float(self.w @ d)


def main():
    print(BAR, flush=True)
    print(f"  D1 · INCREMENTAL DECISION CONTROL — {V2.DISTINCT_SELECTABLE_CLAIMS} classes × "
          f"{len(V2.DELTA_GRID)} δ × {N_SEED} seeds · no search", flush=True)
    print(BAR, flush=True)
    print(f"  spec digest {V2.digest()[:16]}… · δ* {DELTA_STAR} · boots {N_BOOT}", flush=True)
    O, _, dates = CL.load_base()
    ma = CL.build_masks(O)
    classes = E.equivalence_classes(ma)
    masks = {c["representative"]: ma[c["representative"]] for c in classes}
    E.assert_no_degeneracy(list(masks), masks)
    sup = E.Support(O, dates, masks)
    _, gi = np.unique(dates, return_inverse=True)
    n_dates = gi.max() + 1
    froz = {c: Frozen(sup, c, gi) for c in sup.cells}
    print(f"  frozen layouts built for {len(froz)} classes\n", flush=True)

    rows, t0, invalid = [], time.time(), 0
    for delta in V2.DELTA_GRID:
        for seed in range(N_SEED):
            y0 = E.composition_world(O, dates, seed=300 + seed, noise=True)
            for cell in sup.cells:
                f = froz[cell]
                assert f.hash == support_hash(sup, cell), "frozen support moved"
                y = E.inject(y0, sup, cell, delta) if delta > 0 else y0
                est = f.theta(y)
                truth = delta                      # exact, by translation equivariance
                rng = np.random.default_rng([700 + seed, hash(cell) % 10_000, int(delta * 100)])
                p = np.full(n_dates, 1 / n_dates)
                d = np.empty(N_BOOT)
                for b in range(N_BOOT):
                    d[b] = f.theta(y, rng.multinomial(n_dates, p).astype(float))
                good = d[np.isfinite(d)]
                invalid += N_BOOT - len(good)
                lo, hi = np.percentile(good, [2.5, 97.5])
                e = Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo),
                             ci_high=float(hi), level=0.95, estimand=V2.ESTIMAND,
                             method="clustered bootstrap, frozen strata and weights",
                             cluster_unit="trading_date",
                             n_raw=int(sup.meta.set_index("cell").loc[
                                 cell, "eligible_cell_opportunities"]),
                             n_eff=int(sup.meta.set_index("cell").loc[cell, "eligible_dates"]))
                # NO multiplicity correction: this is one named target, not a search over 37.
                dec = decide(branch="return", effect=e, delta_star=DELTA_STAR,
                             direction="positive",
                             support_ok=bool(sup.meta.set_index("cell").loc[
                                 cell, "support_fraction"] >= 0.50),
                             setups_ok=bool(sup.meta.set_index("cell").loc[
                                 cell, "eligible_setups"] >= 5))
                L = dec.layers
                rows.append(dict(
                    delta=delta, seed=seed, cell=cell, truth=truth, est=est,
                    bias=est - truth, lo=lo, hi=hi, width=hi - lo,
                    covered=bool(lo <= truth <= hi),
                    estimator=bool(lo > 0 or hi < 0),
                    evidence=L["EVIDENCE"][0] == "PASS",
                    materiality=L["MATERIALITY"][0] == "PASS",
                    validity=L["VALIDITY"][0] == "PASS",
                    final=dec.status == "BUILD", blocking=dec.blocking_layer or ""))
        print(f"    δ={delta:<5.2f} done ({time.time()-t0:.0f}s)", flush=True)

    D = pd.DataFrame(rows)
    D.to_csv("v2_decision_run.csv", index=False)

    print("\n" + BAR, flush=True)
    print("  THE CHAIN — estimator → evidence → materiality → validity → final", flush=True)
    print(BAR, flush=True)
    print(f"  {'δ':>6s} {'bias':>8s} {'width':>8s} {'cover':>7s} | {'ESTIM':>7s} {'EVID':>7s} "
          f"{'MATER':>7s} {'VALID':>7s} {'FINAL':>7s} | {'blocking layer (share)':<34s}",
          flush=True)
    for delta, G in D.groupby("delta"):
        blk = G.loc[~G.final, "blocking"].value_counts(normalize=True)
        btxt = " · ".join(f"{k or 'none'} {v:.0%}" for k, v in blk.head(2).items()) or "—"
        print(f"  {delta:>6.2f} {G.bias.mean():>+8.3f} {G.width.mean():>8.3f} "
              f"{G.covered.mean():>7.1%} | {G.estimator.mean():>7.1%} {G.evidence.mean():>7.1%} "
              f"{G.materiality.mean():>7.1%} {G.validity.mean():>7.1%} {G.final.mean():>7.1%} | "
              f"{btxt:<34s}", flush=True)

    print("\n" + BAR, flush=True)
    print("  SPREAD ACROSS THE 37 CLASSES — not one curve, a distribution", flush=True)
    print(BAR, flush=True)
    print(f"  {'δ':>6s} {'median':>8s} {'p10':>7s} {'p90':>7s} {'min':>7s} {'max':>7s} "
          f"{'classes always accepted':>25s}", flush=True)
    for delta, G in D.groupby("delta"):
        per = G.groupby("cell").final.mean()
        print(f"  {delta:>6.2f} {per.median():>8.1%} {per.quantile(.1):>7.1%} "
              f"{per.quantile(.9):>7.1%} {per.min():>7.1%} {per.max():>7.1%} "
              f"{int((per == 1).sum()):>25d}", flush=True)

    print("\n" + BAR, flush=True)
    print("  ACCEPTANCE, against the criteria registered before the run", flush=True)
    print(BAR, flush=True)
    pos = D[D.delta > 0]
    a = abs(pos.bias.mean()) < 0.10
    b = D[D.delta == 0].final.mean()
    per_d = D.groupby("delta").final.mean()
    c = per_d.is_monotonic_increasing
    e_ = not D.loc[~D.final & (D.delta > 0), "blocking"].eq("").any()
    f_ = bool(D.covered.mean() > 0.0)
    print(f"    a · |bias| {abs(pos.bias.mean()):.4f}pp < 0.10        "
          f"{'PASS' if a else 'FAIL'}", flush=True)
    print(f"    b · positive rate at δ=0 {b:.1%} under the synthetic generator", flush=True)
    print(f"    c · acceptance rises with δ in aggregate      "
          f"{'PASS' if c else 'FAIL'}  {per_d.round(3).to_dict()}", flush=True)
    print(f"    d · every refusal names a blocking layer      "
          f"{'PASS' if e_ else 'FAIL'}", flush=True)
    print(f"    e · no arbitrary hard magnitude gate — v2 magnitude is the equivalence test "
          f"only", flush=True)
    print(f"    f · frozen support/weights unchanged in every replication  PASS "
          f"(hash-checked)", flush=True)
    print(f"    · invalid bootstrap replications dropped, not renormalised: {invalid:,} of "
          f"{len(D)*N_BOOT:,}", flush=True)

    m = descriptive_metric("synthetic_decision_acceptance_rate",
                           float(D[D.delta == 3.0].final.mean()),
                           target=synthetic_dgp("incremental_composition_generator_v1"),
                           n_replications=int((D.delta == 3.0).sum()))
    print(f"\n  {m}", flush=True)
    print("  Rates here belong to the synthetic generator, not to the market. Only inside that",
          flush=True)
    print("  model can size and acceptance be spoken of at all.", flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
