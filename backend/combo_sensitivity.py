"""POST-M4 · empirical-noise sensitivity characterization.

    DetectionRate(delta, support | empirical historical noise)

At the real scale, tails and block structure of REALIZED_RETURN_TRAIL12_TIMER60_V1, how often
does a planted incremental effect of size delta cross the search-wide band over all 4,636
claims?

THIS IS POST-EXPOSURE AND CARRIES NO CONFIRMATORY STANDING. Historical Y was opened by M4, so
nothing measured here can be evidence about any combination. It characterises the INSTRUMENT.
M4 is immutable: `0 / 4,636 survivors · max theta_hat +4.411 · band +4.830` does not move
whatever this run produces, and no result here may be used to revisit it.

WHY THE NOISE IS THE REAL OUTCOME, PERMUTED, AND NOT A SIMULATOR WITH sd = 21.26

Fitting a parametric world to the historical standard deviation would reproduce one moment and
guess every other. The empirical distribution has p1 −47.4 and p99 +64.0 against the
composition world's −16.0 / +17.9, and it was precisely the TAILS — not the variance — that
moved the band 4.67× while the search tax stayed identical. So each outer world is the real
outcome vector put through the registered G1 permutation: the values, their multiset, their
quantiles and the block heteroskedasticity all survive exactly, and only the association
between tokens and outcome is destroyed. That is checked, not assumed — `sorted(Y_outer)` must
equal `sorted(Y_historical)` bit for bit.

WHAT IS FROZEN BEFORE THE FIRST WORLD

    three needles       q10 / q50 / q90 of the eligible-support distribution, chosen X-only,
                        ONE claim each, reused across every delta and every world. Picking a
                        fresh needle per cell would move the experiment and the effect size
                        at the same time and neither could be read.
    the grid            delta {1.5, 3.0, 4.5, 6.0} x support {q10, q50, q90} x 20 worlds.
    the seeds           outer worlds are shared across cells — a paired design, declared
                        rather than discovered — and the inner permutation stream is
                        independent of the outer one.

SPILLOVER IS GEOMETRY, NOT CONTAMINATION

Planting an effect in `A+B` necessarily lifts `A`, `B` and anything overlapping them. That is
what the search space actually looks like, which is exactly why detection is tested against
max over all 4,636 claims rather than against the needle in isolation. The winning claim and
its overlap with the needle are recorded as diagnostics and never enter the decision.

DETECTION IS ONE CONDITION

    detected  <=>  theta_needle > band_p95 of that world

No rank cut, no CI, no secondary filter.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_capability as CAP                                       # noqa: E402
import combo_m4 as M4                                                # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SPEC = os.path.join(HERE, "COMBO_SENSITIVITY_SPEC.json")
OUT = os.path.join(HERE, "COMBO_SENSITIVITY_RESULT.json")

OUTER_ROOT = 20260817
N_WORLDS = 20
N_PERM = 120
DELTAS = (1.5, 3.0, 4.5, 6.0)
QUANTILES = (("q10", 0.10), ("q50", 0.50), ("q90", 0.90))


def pick_at_quantile(stats: pd.DataFrame, q: float) -> dict:
    """The claim at a support quantile. X-only, deterministic, chosen once."""
    target = stats["support_total"].quantile(q, interpolation="nearest")
    cand = stats[stats["support_total"] == target].sort_values("hash")
    if not len(cand):
        d = (stats["support_total"] - target).abs()
        cand = stats.loc[d == d.min()].sort_values("hash")
    r = cand.iloc[0]
    return dict(claim_id=str(r.claim_id), membership_hash=str(r.hash),
                support_total=int(r.support_total), eligible_setups=int(r.eligible_setups),
                treated_n=int(r.treated_n), treated_dates=int(r.treated_dates),
                top_date_share=round(float(r.top_date_share), 4),
                support_quantile=q, n_tied=int(len(cand)),
                tie_break="lowest membership hash")


def _blocks(fam, dates):
    order = np.lexsort((dates, fam))
    kv = (pd.Series(fam[order]).astype(str) + "|" + pd.Series(dates[order]).astype(str)
          ).to_numpy()
    cuts = np.r_[0, np.flatnonzero(kv[1:] != kv[:-1]) + 1, len(kv)]
    return order, np.argsort(order), list(zip(cuts[:-1].tolist(), cuts[1:].tolist()))


def outer_world(y_hist, order, inv, blocks, seed) -> np.ndarray:
    """One empirical-noise null world: the real outcome, G1-permuted."""
    ys = y_hist[order].copy()
    rng = np.random.default_rng([seed, 0xA1])          # outer stream, tag A1
    for a, b in blocks:
        if b - a > 1:
            ys[a:b] = rng.permutation(ys[a:b])
    return ys[inv]


def main(locations=("q50",), workers: int = 8):
    t0 = time.time()
    m4 = json.load(open(M4.OUT))
    P, M, fam, dates, ids, ph = CAP.load(verbose=False)
    if ph != m4["population_hash"]:
        raise RuntimeError("population moved")
    S = CU.Strata(M, fam, dates, verbose=False)
    uni = json.load(open(CU.OUT_JSON.replace("COMBO_UNIVERSE.json",
                                             "COMBO_UNIVERSE_MATURE.json")))
    classes, stats = CAP.canonical_claims(M, ids, uni, S)
    k = len(classes)
    if k != m4["k_selectable"]:
        raise RuntimeError(f"universe moved: {k} vs {m4['k_selectable']}")
    print(f"  universe {k:,} · population {len(P):,} · M4 {m4['result_digest']} IMMUTABLE",
          flush=True)

    needles = {name: pick_at_quantile(stats, q) for name, q in QUANTILES}
    seeds = [OUTER_ROOT * 1000 + i for i in range(N_WORLDS)]
    spec = dict(
        spec_id="POST_M4_EMPIRICAL_NOISE_SENSITIVITY_V1",
        status="POST_EXPOSURE_CHARACTERIZATION",
        confirmatory_standing="NONE — historical Y was opened by M4",
        historical_m4_digest=m4["result_digest"],
        m4_verdict="IMMUTABLE · 0/4,636 survivors · max theta_hat +4.411 · band +4.8302",
        search_universe=k, population_hash=ph,
        historical_outcome=m4["outcome"],
        noise_generator="the real historical outcome vector, G1-permuted (outcomes inside "
                        "(date x family)) — real tails, scale and block heteroskedasticity "
                        "kept; token-to-outcome association destroyed",
        deltas_pp=list(DELTAS), support_locations=[q[0] for q in QUANTILES],
        worlds_per_cell=N_WORLDS, n_inner_perm=N_PERM,
        needles=needles,
        outer_root_seed=OUTER_ROOT,
        outer_seed_rule="outer_seed_i = OUTER_ROOT * 1000 + i",
        outer_seeds=seeds,
        outer_rng="np.random.default_rng([outer_seed, 0xA1])",
        inner_rng="np.random.default_rng([outer_seed, delta_key, perm]) — independent of "
                  "the outer stream",
        paired_design="the same 20 outer worlds are reused across every delta and every "
                      "support location, so a cell-to-cell difference is the planted effect "
                      "and not a different noise draw. Declared, not discovered.",
        detection="theta_needle > band_p95 of that world. No rank cut, no CI, no secondary "
                  "filter.",
        may_not=["change the M4 verdict", "re-run or re-open M4",
                 "become a PASS/FAIL gate applied backwards",
                 "be reported as a single threshold"],
    )
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    if os.path.exists(SPEC):
        old = json.load(open(SPEC))
        if old["spec_digest"] != spec["spec_digest"]:
            raise RuntimeError("a sensitivity spec exists and differs — it is immutable")
        print(f"  spec unchanged · {spec['spec_digest']}", flush=True)
    else:
        with open(SPEC, "w") as f:
            json.dump(spec, f, indent=2, default=str)
        print(f"  FROZE {SPEC} · {spec['spec_digest']}", flush=True)
    for n, v in needles.items():
        print(f"    {n}  {v['claim_id']:<26} support {v['support_total']:>7,} · "
              f"{v['eligible_setups']:>2} strata · {v['treated_dates']:>4} dates", flush=True)

    print(f"\n  building Support (X-only, once)…", flush=True)
    masks = {c["claim_id"]: c["mask"] for c in classes.values()}
    O = P[["family"]].copy()
    sup = V2I.Support(O, dates, masks, verbose=False)
    del masks
    for c in classes.values():
        c.pop("mask", None)
    y_hist = M4.load_outcome(P)
    order, inv, blocks = _blocks(fam, dates)
    print(f"  Support ready · historical Y loaded (sd {y_hist.std():.2f} pp) · "
          f"{time.time()-t0:.0f}s", flush=True)

    # ── invariants, checked once, before any cell ───────────────────────────
    y0 = outer_world(y_hist, order, inv, blocks, seeds[0])
    same = np.array_equal(np.sort(y0), np.sort(y_hist))
    th0 = sup.theta(y0)
    nid = needles[locations[0]]["claim_id"]
    th1 = sup.theta(V2I.inject(y0, sup, nid, DELTAS[0]))
    shift = float(th1[nid] - th0[nid])
    print(f"\n  INVARIANTS", flush=True)
    print(f"    outer permutation preserves the value multiset : "
          f"{'YES' if same else 'NO'}", flush=True)
    print(f"    injection shifts theta_needle by delta         : "
          f"{shift:+.4f} vs {DELTAS[0]:+.4f}", flush=True)
    if not same:
        raise RuntimeError("the outer permutation changed the outcome values — it must only "
                           "reorder them")
    if abs(shift - DELTAS[0]) > 1e-6:
        raise RuntimeError(f"injection moved theta by {shift:.6f}, not {DELTAS[0]}. v2 makes "
                           f"this exact by construction, so a mismatch means membership or "
                           f"support moved — find out why before running any world.")

    results = []
    if os.path.exists(OUT):
        results = json.load(open(OUT)).get("cells", [])
    done = {(c["support"], c["delta_pp"]) for c in results}

    for loc in locations:
        nd = needles[loc]
        nid = nd["claim_id"]
        needle_rows = set(np.flatnonzero(
            np.isin(np.arange(len(P)), np.concatenate(
                [i for _, i, _ in sup.strata[nid]]))).tolist())
        for di, delta in enumerate(DELTAS):
            if (loc, delta) in done:
                print(f"  skip {loc} δ={delta} (already recorded)", flush=True)
                continue
            tc = time.time()
            worlds = []
            for wi, seed in enumerate(seeds):
                yo = outer_world(y_hist, order, inv, blocks, seed)
                yj = V2I.inject(yo, sup, nid, delta)
                th = sup.theta(yj)
                t_needle = float(th[nid])
                rank = int((th > t_needle).sum()) + 1
                winner = str(th.idxmax())
                CAP._W = dict(y=yj[order], inv=inv, blocks=blocks, sup=sup,
                              seed=int(seed * 1000 + di))
                with mp.get_context("fork").Pool(workers) as pool:
                    band = np.asarray(pool.map(CAP._perm_once, range(N_PERM), chunksize=2))
                thr = float(np.percentile(band, 95))
                det = bool(t_needle > thr)
                ov = 0.0
                if winner != nid and winner in sup.strata:
                    wr = set(np.concatenate([i for _, i, _ in sup.strata[winner]]).tolist())
                    ov = len(needle_rows & wr) / max(len(needle_rows | wr), 1)
                worlds.append(dict(world=wi, outer_seed=seed,
                                   needle_theta=round(t_needle, 4),
                                   band_p95=round(thr, 4),
                                   margin=round(t_needle - thr, 4),
                                   detected=det, needle_rank=rank,
                                   winner=winner, overlap_needle_winner=round(ov, 4)))
                nd_ = sum(w["detected"] for w in worlds)
                print(f"    {loc} δ={delta:>4.1f} world {wi:>2} · θ {t_needle:+7.3f} · "
                      f"band {thr:+7.3f} · {'DET' if det else '   '} · rank {rank:>5} · "
                      f"{nd_}/{wi+1}", flush=True)
            n_det = sum(w["detected"] for w in worlds)
            mg = [w["margin"] for w in worlds]
            cell = dict(support=loc, delta_pp=delta, needle=nid,
                        detected=n_det, worlds=N_WORLDS,
                        detection_rate=round(n_det / N_WORLDS, 3),
                        median_needle_theta=round(float(np.median(
                            [w["needle_theta"] for w in worlds])), 4),
                        median_band=round(float(np.median(
                            [w["band_p95"] for w in worlds])), 4),
                        median_margin=round(float(np.median(mg)), 4),
                        min_margin=round(float(np.min(mg)), 4),
                        max_margin=round(float(np.max(mg)), 4),
                        median_rank=int(np.median([w["needle_rank"] for w in worlds])),
                        seconds=round(time.time() - tc, 1), worlds_detail=worlds)
            results.append(cell)
            with open(OUT, "w") as f:
                json.dump(dict(spec_digest=spec["spec_digest"],
                               status="POST_EXPOSURE_CHARACTERIZATION",
                               m4_digest=m4["result_digest"], m4_verdict="IMMUTABLE",
                               cells=results), f, indent=2, default=str)
            print(f"  → {loc} δ={delta}: {n_det}/{N_WORLDS} detected "
                  f"({time.time()-tc:.0f}s)\n", flush=True)

    print(f"\n{'='*72}", flush=True)
    print(f"  DetectionRate(delta, support | empirical historical noise)", flush=True)
    locs = sorted({c["support"] for c in results})
    print(f"     {'delta':>7}  " + "  ".join(f"{l:>7}" for l in locs), flush=True)
    for d in DELTAS:
        cells = {c["support"]: c for c in results if c["delta_pp"] == d}
        row = "  ".join(f"{cells[l]['detected']:>3}/{N_WORLDS:<3}" if l in cells
                        else f"{'-':>7}" for l in locs)
        print(f"     {d:>+6.1f}  {row}", flush=True)
    print(f"\n  a sensitivity CURVE, not a threshold · {(time.time()-t0)/60:.0f} min",
          flush=True)
    print(f"  WROTE {OUT}", flush=True)
    print("=" * 72, flush=True)


if __name__ == "__main__":
    locs = sys.argv[1].split(",") if len(sys.argv) > 1 else ["q50"]
    main(locations=tuple(locs), workers=int(sys.argv[2]) if len(sys.argv) > 2 else 8)
