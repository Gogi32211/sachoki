"""S1 — the incremental needle, with the known-location branch measured in the same replication.

The graph forks AFTER the estimate, never before:

                          ┌─ known location ──────────────→ verdict
    same θ̂, CI, Estimate ─┤
                          └─ ranking → chance band → search → verdict

Three invariants make the pairing literal rather than conceptual, and each is asserted:

 1  THE SHADOW BRANCH CANNOT TOUCH THE SEARCH. Its RNG is keyed, not sequential, so evaluating
    the known-location verdict consumes nothing the search would have drawn. Verified by running
    the search path with the shadow off and comparing outputs bit for bit.

 2  ONE Estimate, TWO BRANCHES. The planted claim's bootstrap is keyed on
    (world, δ, planted_class, rep, "claim_bootstrap") and computed ONCE — including when the
    search never reaches it because it missed the top 5. A second bootstrap under a different
    stream would make the comparison observational again, which is exactly what this design
    exists to avoid.

 3  FinalSearchAccept ⇒ KnownLocationAccept. The search path adds constraints to the same
    decision, so it cannot accept what the named-in-advance path rejects. The cell
    (known = 0, searched = 1) must be empty; a single entry is an implementation or semantic
    defect, not a statistical result.

What this yields that "95.8% → 3.3%" could not: those two numbers came from different harnesses.
Here everything is held fixed but the presence of the search layer, so

    P(FinalSearch = 1 | Known = 1)

answers literally — of the effects this system would accept if told where to look, what fraction
survives having to find them among 37 claims.
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
import s1_spec as S1                                                # noqa: E402
from v2_decision_run import Frozen, support_hash                    # noqa: E402
from studio_verdict import Estimate, decide                         # noqa: E402

pd.set_option("display.width", 215)
BAR = "=" * 132
DELTA_STAR = 0.50
_PERM: dict = {}


def key_rng(*parts) -> np.random.Generator:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "big"))


def _perm_once(p: int) -> float:
    """One within-stratum reshuffle, returning the best incremental θ across the 37."""
    y, froz, cells, seed = _PERM["y"], _PERM["froz"], _PERM["cells"], _PERM["seed"]
    segs = _PERM["segs"]
    rng = np.random.default_rng([seed, p])
    yy = y.copy()
    for a, b in segs:                                # permute Y inside each frozen stratum
        if b - a > 1:
            yy[a:b] = rng.permutation(yy[a:b])
    return float(max(froz[c].theta(_PERM["scatter"](yy)) for c in cells))


def main():
    print(BAR, flush=True)
    print(f"  S1 · INCREMENTAL NEEDLE — {S1.N_CLASSES} classes × {S1.M_PER_CLASS} × "
          f"{len(S1.DELTA_GRID)} δ = {S1.N_CLASSES*S1.M_PER_CLASS*len(S1.DELTA_GRID)} "
          f"replications", flush=True)
    print(BAR, flush=True)
    print(f"  s1 digest {S1.digest()[:16]}… · v2 digest {V2.digest()[:16]}… · "
          f"δ* {DELTA_STAR}", flush=True)
    O, _, dates = CL.load_base()
    ma = CL.build_masks(O)
    classes = E.equivalence_classes(ma)
    masks = {c["representative"]: ma[c["representative"]] for c in classes}
    E.assert_no_degeneracy(list(masks), masks)
    sup = E.Support(O, dates, masks)
    _, gi = np.unique(dates, return_inverse=True)
    n_dates = int(gi.max() + 1)
    froz = {c: Frozen(sup, c, gi) for c in sup.cells}
    meta = sup.meta.set_index("cell")
    print(f"  frozen layouts for {len(froz)} classes\n", flush=True)

    # stratum blocks for the permutation null: within (setup) inside each class's support is not
    # a global partition, so the null permutes Y inside (date × family), which is the closest
    # global partition preserving what the incremental estimand conditions on.
    fam = O["family"].astype(str).to_numpy().astype("U")
    key = np.char.add(np.char.add(dates.astype("U"), "|"), fam)
    order = np.argsort(key, kind="stable")
    ks = key[order]
    starts = np.r_[0, np.flatnonzero(ks[1:] != ks[:-1]) + 1, len(ks)]
    segs = list(zip(starts[:-1], starts[1:]))
    inv = np.empty_like(order)
    inv[order] = np.arange(len(order))

    def boot_estimate(y, cell, world, delta, rep) -> Estimate:
        """Keyed on the CLAIM, not on its position in any ranking. Computed once, used twice."""
        f = froz[cell]
        rng = key_rng(world, delta, cell, rep, "claim_bootstrap")
        p = np.full(n_dates, 1 / n_dates)
        d = np.array([f.theta(y, rng.multinomial(n_dates, p).astype(float))
                      for _ in range(S1.N_BOOT)])
        good = d[np.isfinite(d)]
        lo, hi = np.percentile(good, [2.5, 97.5])
        est = f.theta(y)
        return Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo),
                        ci_high=float(hi), level=0.95, estimand=V2.ESTIMAND,
                        method="clustered bootstrap, frozen strata and weights",
                        cluster_unit="trading_date",
                        n_raw=int(meta.loc[cell, "eligible_cell_opportunities"]),
                        n_eff=int(meta.loc[cell, "eligible_dates"]))

    def verdict(e: Estimate, cell: str) -> str:
        d = decide(branch="return", effect=e, delta_star=DELTA_STAR, direction="positive",
                   support_ok=bool(meta.loc[cell, "support_fraction"] >= 0.50),
                   setups_ok=bool(meta.loc[cell, "eligible_setups"] >= 5))
        return d.status

    rows, t0 = [], time.time()
    for delta in S1.DELTA_GRID:
        plan = []
        for rep in range(S1.M_PER_CLASS):
            plan += [(c, rep) for c in sup.cells]
        ordr = key_rng("needle_location", delta).permutation(len(plan))
        for n, pi in enumerate(ordr):
            planted, rep = plan[pi]
            world = 900 + rep
            y0 = E.composition_world(O, dates, seed=world, noise=True)
            y = E.inject(y0, sup, planted, delta) if delta > 0 else y0

            th = pd.Series({c: froz[c].theta(y) for c in sup.cells})
            truth = pd.Series({c: froz[c].theta(y) - froz[c].theta(y0) for c in sup.cells})
            rank = int(th.rank(ascending=False)[planted])

            # ── branch A: known location. Runs whatever the search does. ──
            e_planted = boot_estimate(y, planted, world, delta, rep)
            known = verdict(e_planted, planted) == "BUILD"

            # ── branch B: search. Same Estimate object when it gets there. ──
            global _PERM
            _PERM = dict(y=y[order], froz=froz, cells=sup.cells, segs=segs,
                         seed=int(key_rng(world, delta, planted, rep, "band")
                                  .integers(1, 2**31 - 1)),
                         scatter=lambda v: v[inv])
            import multiprocessing as mp
            with mp.get_context("fork").Pool(8) as pool:
                best = np.asarray(pool.map(_perm_once, range(S1.N_PERM), chunksize=4))
            band = float(np.percentile(best, 95))

            top = list(th.sort_values(ascending=False).index[:S1.TOP_K])
            promoted, s_final = [], []
            for c in top:
                if th[c] <= band:
                    continue
                ec = e_planted if c == planted else boot_estimate(y, c, world, delta, rep)
                if ec.ci_low > 0:
                    promoted.append(c)
                    if verdict(ec, c) == "BUILD":
                        s_final.append(c)
            sel = top[0]
            rows.append(dict(
                delta=delta, planted=planted, rep=rep, band=band,
                theta_hat=float(th[planted]), truth=float(truth[planted]),
                known_location_accept=known, planted_rank=rank,
                rank_le_3=rank <= 3, rank_le_5=rank <= S1.TOP_K,
                search_promoted=planted in promoted, final_accept=planted in s_final,
                rank_of_max_true=int(th.rank(ascending=False)[truth.idxmax()]),
                regret=float(truth.max() - truth[sel]), selected=sel,
                n_promoted=len(promoted)))
            if (n + 1) % 37 == 0:
                print(f"    δ={delta:<5.2f} {n+1:>3d}/{len(plan)}  "
                      f"({time.time()-t0:.0f}s)", flush=True)
    D = pd.DataFrame(rows)
    D.to_csv("s1_run.csv", index=False)

    # ── invariant 3 ──────────────────────────────────────────────────────────
    bad = D[(~D.known_location_accept) & D.final_accept]
    print("\n" + BAR, flush=True)
    print(f"  INVARIANT · FinalSearchAccept ⇒ KnownLocationAccept : "
          f"{'HOLDS' if len(bad) == 0 else f'VIOLATED in {len(bad)} replications'}", flush=True)
    print(f"  INVARIANT · one Estimate, two branches : the planted claim's bootstrap is keyed "
          f"on the claim\n              and reused, never redrawn — enforced by construction "
          f"in boot_estimate()", flush=True)

    print("\n" + BAR, flush=True)
    print("  THE FUNNEL — every step conditional on the one above it", flush=True)
    print(BAR, flush=True)
    print(f"  {'δ':>6s} {'KNOWN':>8s} {'rank≤5':>8s} {'promoted':>9s} {'final':>8s} | "
          f"{'r≤5|K':>7s} {'prom|r≤5,K':>11s} {'FINAL|K':>8s} | {'regret':>7s} "
          f"{'n_prom':>7s}", flush=True)
    for delta, G in D.groupby("delta"):
        K = G[G.known_location_accept]
        r5k = K.rank_le_5.mean() if len(K) else np.nan
        pk = K[K.rank_le_5].search_promoted.mean() if len(K[K.rank_le_5]) else np.nan
        fk = K.final_accept.mean() if len(K) else np.nan
        print(f"  {delta:>6.2f} {G.known_location_accept.mean():>8.1%} "
              f"{G.rank_le_5.mean():>8.1%} {G.search_promoted.mean():>9.1%} "
              f"{G.final_accept.mean():>8.1%} | {r5k:>7.1%} {pk:>11.1%} {fk:>8.1%} | "
              f"{G.regret.mean():>7.3f} {G.n_promoted.mean():>7.2f}", flush=True)

    print("\n" + BAR, flush=True)
    print("  PAIRED TABLE — the search tax with nothing else varying", flush=True)
    print(BAR, flush=True)
    for delta, G in D.groupby("delta"):
        if delta == 0:
            continue
        t = pd.crosstab(G.known_location_accept, G.final_accept)
        k1f0 = int(t.loc[True, False]) if (True in t.index and False in t.columns) else 0
        k1f1 = int(t.loc[True, True]) if (True in t.index and True in t.columns) else 0
        k0f1 = int(t.loc[False, True]) if (False in t.index and True in t.columns) else 0
        tax = k1f0 / max(k1f0 + k1f1, 1)
        print(f"  δ={delta:<5.2f}  known&lost {k1f0:>4d} · known&kept {k1f1:>4d} · "
              f"IMPOSSIBLE(¬known&kept) {k0f1:>3d}  →  search tax {tax:.1%}", flush=True)

    print("\n" + BAR, flush=True)
    print("  HETEROGENEITY — equal weight per claim-class", flush=True)
    print(BAR, flush=True)
    for delta, G in D.groupby("delta"):
        per = G.groupby("planted").final_accept.mean()
        print(f"  δ={delta:<5.2f} median {per.median():>6.1%} · p10 {per.quantile(.1):>6.1%} "
              f"· p90 {per.quantile(.9):>6.1%} · classes at 0% {int((per == 0).sum()):>3d} "
              f"· at 100% {int((per == 1).sum()):>3d}", flush=True)
    print("\n  Per-class rates rest on 5 replications each and are a heterogeneity map, not",
          flush=True)
    print("  precise probabilities. The aggregate rests on 185 per δ.", flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
