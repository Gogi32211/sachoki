"""N0 — the incremental structured null, two generators, never pooled.

G1 permutes outcomes with the predictor fixed, so the estimand is literally unchanged and
E_c / w_cs stay frozen. G2 permutes the PREDICTOR, so membership moves and everything downstream
of membership moves with it — eligibility, strata, weights. Freezing the weights there would
give null-world membership carrying real-world weights, an object with no meaning. What never
moves in either is the POLICY: n ≥ 100, dates ≥ 25, concentration ≤ 0.20 in every world.

The asymmetry is the point, and it is carried to the end rather than stopped at membership.

Nothing here reports a single "incremental FWER". Two null models produce two numbers about two
different probabilistic experiments, and `sampling_target` refuses the comparison.
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
import n0_spec as N0                                                # noqa: E402
from sampling_target import descriptive_metric, structured_permutation_null  # noqa: E402
from studio_verdict import Estimate, decide                         # noqa: E402
from v2_decision_run import Frozen                                  # noqa: E402

pd.set_option("display.width", 200)
BAR = "=" * 126
DELTA_STAR, N_PERM, N_BOOT, TOP_K = 0.50, 120, 200, 5
_P: dict = {}


def krng(*parts):
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "big"))


def _perm(p: int) -> float:
    y, froz, cells, segs, seed = _P["y"], _P["froz"], _P["cells"], _P["segs"], _P["seed"]
    rng = np.random.default_rng([seed, p])
    yy = y.copy()
    for a, b in segs:
        if b - a > 1:
            yy[a:b] = rng.permutation(yy[a:b])
    back = yy[_P["inv"]]
    return float(max(froz[c].theta(back) for c in cells))


def run_pipeline(y, froz, cells, meta, gi, n_dates, seed, band_segs=None, inv=None, order=None):
    """Rank → band → bootstrap the top-K → verdict. Returns the three FWER components."""
    th = pd.Series({c: froz[c].theta(y) for c in cells})
    global _P
    _P = dict(y=y[order], froz=froz, cells=cells, segs=band_segs, seed=seed, inv=inv)
    import multiprocessing as mp
    with mp.get_context("fork").Pool(8) as pool:
        best = np.asarray(pool.map(_perm, range(N_PERM), chunksize=4))
    band = float(np.percentile(best, 95))
    clears = int((th > band).sum())
    top = list(th.sort_values(ascending=False).index[:TOP_K])
    prom = fin = 0
    p = np.full(n_dates, 1 / n_dates)
    for c in top:
        if th[c] <= band:
            continue
        rng = krng(seed, c, "boot")
        d = np.array([froz[c].theta(y, rng.multinomial(n_dates, p).astype(float))
                      for _ in range(N_BOOT)])
        good = d[np.isfinite(d)]
        if not len(good):
            continue
        lo, hi = np.percentile(good, [2.5, 97.5])
        if lo <= 0:
            continue
        prom += 1
        e = Estimate(estimate=float(min(max(froz[c].theta(y), lo), hi)), ci_low=float(lo),
                     ci_high=float(hi), level=0.95, estimand=V2.ESTIMAND,
                     method="clustered bootstrap", cluster_unit="trading_date",
                     n_raw=int(meta.loc[c, "eligible_cell_opportunities"]),
                     n_eff=int(meta.loc[c, "eligible_dates"]))
        if decide(branch="return", effect=e, delta_star=DELTA_STAR, direction="positive",
                  support_ok=bool(meta.loc[c, "support_fraction"] >= 0.50),
                  setups_ok=bool(meta.loc[c, "eligible_setups"] >= 5)).status == "BUILD":
            fin += 1
    return dict(band=band, n_clears=clears, n_promoted=prom, n_final=fin)


def main():
    print(BAR, flush=True)
    print(f"  N0 · INCREMENTAL STRUCTURED NULL · {N0.N_REPLICATIONS} worlds × 2 generators",
          flush=True)
    print(BAR, flush=True)
    print(f"  n0 digest {N0.digest()[:16]}… · v2 digest {V2.digest()[:16]}…", flush=True)
    O, y_real, dates = CL.load_base()
    ma = CL.build_masks(O)
    classes = E.equivalence_classes(ma)
    masks = {c["representative"]: ma[c["representative"]] for c in classes}
    fam = O["family"].astype(str).to_numpy().astype("U")
    _, gi = np.unique(dates, return_inverse=True)
    n_dates = int(gi.max() + 1)

    daily = N0.DATE_LEVEL_FEATURES[0]
    g2_cells = [c for c in masks if daily.replace("sig_", "") in c or daily in c]
    g1_cells = [c for c in masks if c not in g2_cells]
    print(f"  routing · G1 {len(g1_cells)} classes · G2 {len(g2_cells)} classes "
          f"(declared {N0.N_CLASSES_G1} / {N0.N_CLASSES_G2})", flush=True)
    assert len(g1_cells) + len(g2_cells) == len(masks)

    key = np.char.add(np.char.add(dates.astype("U"), "|"), fam)
    order = np.argsort(key, kind="stable")
    ks = key[order]
    st = np.r_[0, np.flatnonzero(ks[1:] != ks[:-1]) + 1, len(ks)]
    segs = list(zip(st[:-1], st[1:]))
    inv = np.empty_like(order)
    inv[order] = np.arange(len(order))

    # ── G1 ───────────────────────────────────────────────────────────────────
    sup1 = E.Support(O, dates, {c: masks[c] for c in g1_cells}, verbose=False)
    froz1 = {c: Frozen(sup1, c, gi) for c in sup1.cells}
    meta1 = sup1.meta.set_index("cell")
    print(f"\n  G1 · within_stratum_outcome_v1 — X fixed, Y permuted, design FROZEN", flush=True)
    rows1, t0 = [], time.time()
    for w in range(N0.N_REPLICATIONS):
        rng = krng("G1", w, "outer_null_world")
        yy = y_real[order].copy()
        for a, b in segs:
            if b - a > 1:
                yy[a:b] = rng.permutation(yy[a:b])
        y = yy[inv]
        r = run_pipeline(y, froz1, sup1.cells, meta1, gi, n_dates,
                         int(krng("G1", w, "inner_chance_band").integers(1, 2**31 - 1)),
                         band_segs=segs, inv=inv, order=order)
        rows1.append(r)
        if (w + 1) % 40 == 0:
            print(f"      {w+1:>3d}/{N0.N_REPLICATIONS}  ({time.time()-t0:.0f}s)", flush=True)
    G1 = pd.DataFrame(rows1)

    # ── G2 ───────────────────────────────────────────────────────────────────
    print(f"\n  G2 · date_level_label_circular_v1 — predictor shifted, design RECOMPUTED",
          flush=True)
    for k, v in N0.CIRCULAR_PRESERVES.items():
        print(f"      {k:<26s} {'YES' if v else 'NO'}", flush=True)
    uq_d = np.unique(dates)
    lab_real = pd.DataFrame({"d": dates, "v": O[daily].to_numpy()}).groupby("d")["v"].first()
    lab_real = lab_real.reindex(uq_d).to_numpy()
    dpos = np.searchsorted(uq_d, dates)
    rows2, t0 = [], time.time()
    for w in range(N0.N_REPLICATIONS):
        shift = int(krng("G2", w, "outer_null_world").integers(1, len(uq_d)))
        lab = np.roll(lab_real, shift)
        Ow = O.copy()
        Ow[daily] = lab[dpos]                              # predictor permuted
        maw = CL.build_masks(Ow)                           # membership recomputed
        mw = {c: maw[c] for c in g2_cells}
        supw = E.Support(Ow, dates, mw, verbose=False)      # strata + weights recomputed
        dh = hashlib.sha256(str([(c, len(supw.strata[c]), float(supw.weights[c].sum()))
                                 for c in supw.cells]).encode()).hexdigest()[:12]
        evaluated = len(supw.cells)
        ineligible = len(g2_cells) - evaluated
        if evaluated == 0:
            rows2.append(dict(band=np.nan, n_clears=0, n_promoted=0, n_final=0,
                              evaluated=0, ineligible=ineligible, uncomputable=0,
                              coverage=np.nan, design_hash=dh, shift=shift))
            continue
        frw = {c: Frozen(supw, c, gi) for c in supw.cells}
        r = run_pipeline(y_real, frw, supw.cells, supw.meta.set_index("cell"), gi, n_dates,
                         int(krng("G2", w, "inner_chance_band").integers(1, 2**31 - 1)),
                         band_segs=segs, inv=inv, order=order)
        r.update(evaluated=evaluated, ineligible=ineligible, uncomputable=0,
                 coverage=float(supw.meta.support_fraction.min()), design_hash=dh, shift=shift)
        rows2.append(r)
        if (w + 1) % 40 == 0:
            print(f"      {w+1:>3d}/{N0.N_REPLICATIONS}  ({time.time()-t0:.0f}s)", flush=True)
    G2 = pd.DataFrame(rows2)

    pd.concat([G1.assign(gen="G1"), G2.assign(gen="G2")], ignore_index=True) \
        .to_csv("n0_run.csv", index=False)

    print("\n" + BAR, flush=True)
    print("  RESULTS — reported separately, never pooled", flush=True)
    print(BAR, flush=True)
    for nm, D, gid, k in (("G1 within_stratum_outcome_v1", G1, "within_stratum_outcome_v1",
                           len(g1_cells)),
                          ("G2 date_level_label_circular_v1", G2,
                           "date_level_label_circular_v1", len(g2_cells))):
        fb = (D.n_clears > 0).mean()
        fs = (D.n_promoted > 0).mean()
        ff = (D.n_final > 0).mean()
        lo, hi = N0.TOLERANCE_BAND
        print(f"\n  {nm}   k = {k}", flush=True)
        print(f"    FWER_band  {fb:.3f}   FWER_search {fs:.3f}   FWER_final {ff:.3f}", flush=True)
        print(f"    tolerance [{lo:.2f},{hi:.2f}] → band {'INSIDE' if lo <= fb <= hi else 'OUTSIDE'}",
              flush=True)
        ineq = ff <= fs <= fb
        print(f"    {N0.STRUCTURAL_INEQUALITY}: {'HOLDS' if ineq else 'VIOLATED'}", flush=True)
        print(f"    E[n promoted] {D.n_promoted.mean():.3f} · P(0) {(D.n_promoted==0).mean():.3f}"
              f" · P(>=2) {(D.n_promoted>=2).mean():.3f} · max {int(D.n_promoted.max())}",
              flush=True)

    print("\n" + BAR, flush=True)
    print("  G2 DIAGNOSTICS — part of the result, not an appendix", flush=True)
    print(BAR, flush=True)
    ident = ((G2.evaluated + G2.ineligible + G2.uncomputable) == N0.N_CLASSES_G2).all()
    print(f"    accounting identity  evaluated + ineligible + uncomputable == "
          f"{N0.N_CLASSES_G2} : {'HOLDS' if ident else 'VIOLATED'}", flush=True)
    print(f"    selectable classes per world: min {int(G2.evaluated.min())} · "
          f"median {G2.evaluated.median():.0f} · max {int(G2.evaluated.max())}", flush=True)
    print(f"    support coverage (min per world): p10 {G2.coverage.quantile(.1):.1%} · "
          f"median {G2.coverage.median():.1%}", flush=True)
    print(f"    distinct design hashes: {G2.design_hash.nunique()} of {len(G2)} worlds "
          f"— a reused real-world design would collide", flush=True)

    print("\n" + BAR, flush=True)
    for gid, D in (("within_stratum_outcome_v1", G1), ("date_level_label_circular_v1", G2)):
        m = descriptive_metric("conditional_false_promotion_rate",
                               float((D.n_promoted > 0).mean()),
                               target=structured_permutation_null(gid), n_replications=len(D))
        print(f"  {m}", flush=True)
    print("\n  Two null models, two numbers. No pooled figure is computed.", flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
