"""M3 · PRIMARY capability qualification for the frozen COMBO_MINER_V1_SEARCH_SPEC.

Can this instrument still find a planted effect after paying for 4,636 selectable claims?
That is the only question here, and it is asked entirely on synthetic outcomes. No historical
return value is loaded at any point.

WHAT IS FROZEN BEFORE THE FIRST NUMBER EXISTS

    the needle          ONE claim, chosen X-only at the median of the eligible-support
                        distribution, used in all 20 worlds. Not re-chosen per world — a
                        needle picked afresh each time would let the instrument shop for a
                        location where it happens to succeed.
    the 20 seeds        written down, not generated as we go. "15/20, let us add a few more
                        seeds" is the failure this exists to make impossible. A crashed
                        world may be rerun with the SAME seed; it may never be replaced.
    the criterion       PASS ≥18/20 · PARTIAL 16-17 · FAIL ≤15, recorded before any world.

`COMBO_CAPABILITY_SPEC.json` is written first and then treated as immutable: a second run
refuses to overwrite it and instead verifies that the spec it would have written is identical.

THE NULL IS THE REGISTERED ONE, NOT A BETTER ONE

n0_spec.py names it: `within_stratum_outcome_v1`, "outcomes permuted inside (date × family)",
applies_to classes with no date-level feature, FWER 0.065. That is the generator the 31-class
qualification was measured on, so it is the generator used here — unchanged, including its
block structure. Those blocks are small (57 families × 1,244 dates over 275,307 rows), which
makes the band conservative; block-size statistics are recorded as a DIAGNOSTIC. Widening the
block to make the test easier would be changing the methodology in the middle of qualifying
it, which is the one thing this run may not do.

DETECTION IS CROSSING THE BAND, AND NOTHING ELSE

    detected  ⇔  θ_needle > band_p95 over ALL 4,636 claims

Not a positive θ. Not a top-K rank. Not the best claim in its world. Rank is persisted
because it is informative and is never consulted by the decision.

SUPPORT IS BUILT ONCE. It depends on X and membership only, so rebuilding it per world would
be both wasteful and wrong — an eligibility recomputed from a synthetic outcome would let Y
choose its own support set, which is the mistake the frozen-support design exists to prevent.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import resource
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens_spec as TS                                       # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402
import combolab_v2_spec as V2                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TOKENS = os.path.join(ROOT, "data", "combo_tokens.parquet")
STATUS = os.path.join(ROOT, "data", "combo_label_status.parquet")
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
SEARCH_SPEC = os.path.join(HERE, "COMBO_SEARCH_SPEC.json")
UNI = os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")
CAP_SPEC = os.path.join(HERE, "COMBO_CAPABILITY_SPEC.json")
CAP_RESULT = os.path.join(HERE, "COMBO_CAPABILITY_PRIMARY.json")

ROOT_SEED = 20260816
N_WORLDS = 20
N_PERM = 120
DELTA = 1.5                      # percentage points
PASS_AT, PARTIAL_AT = 18, 16
NULL_GENERATOR = "within_stratum_outcome_v1"   # n0_spec.py — outcomes inside (date × family)


def world_seeds() -> list:
    """Deterministic and written down. world_seed_i = ROOT_SEED * 1000 + i."""
    return [ROOT_SEED * 1000 + i for i in range(N_WORLDS)]


def _rss_gb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e9 if sys.platform == "darwin" else r / 1e6


# ── population and the canonical claim set ───────────────────────────────────
def load(verbose=True):
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", "sig_close", "family",
                                       "dup_group"])
    O = O[O["sig_close"].notna()].drop_duplicates("dup_group")
    O = O[(O["sig_close"] >= 21) & (O["sig_close"] <= 89)].reset_index(drop=True)
    O["sig_date"] = O["sig_date"].astype(str).str[:10]
    st = pd.read_parquet(STATUS)
    keep = st[st["label_status"].str.startswith("ORDINARY")][["ticker", "sig_date"]]
    O = O.merge(keep, on=["ticker", "sig_date"], how="inner", validate="m:1")
    K = pd.read_parquet(TOKENS)
    ids = [t.token_id for t in TS.REGISTRY]
    P = O.merge(K, on=["ticker", "sig_date"], how="inner", validate="m:1")
    M = np.ascontiguousarray(P[ids].to_numpy(dtype=np.uint8))
    fam = P["family"].astype(str).to_numpy()
    dates = P["sig_date"].to_numpy()
    ph = CU.population_hash(P)
    if verbose:
        print(f"  population {len(P):,} · hash {ph}", flush=True)
    return P, M, fam, dates, ids, ph


def canonical_claims(M, ids, uni, S):
    """The 4,636 distinct OPPORTUNITY_LEVEL claims, with X-only support statistics."""
    col = {t: i for i, t in enumerate(ids)}
    nullreq = {t.token_id: t.null_requirement for t in TS.REGISTRY}
    dropped = set(uni["universe"]["depth1"]["dropped_ids"]["degenerate_prevalence"]) | \
        set(uni["universe"]["depth1"]["dropped_ids"]["support"])
    alive1 = [t.token_id for t in TS.REGISTRY if t.token_id not in dropped]
    sig1: dict = {}
    for t in alive1:
        sig1.setdefault(M[:, col[t]].tobytes(), []).append(t)
    alive1 = [v[0] for v in sig1.values()]
    cands = [(t,) for t in alive1] + [tuple(p) for p in uni["surviving_depth2"]]
    opp = [c for c in cands
           if TS.combine_null([nullreq[t] for t in c]) == TS.OPPORTUNITY_LEVEL]

    classes: dict = {}
    rows = []
    for c in opp:
        m = M[:, col[c[0]]].astype(bool)
        for t in c[1:]:
            m &= M[:, col[t]].astype(bool)
        idx = np.flatnonzero(m)
        ne, _cb, ok = S.eligible_mask(idx)
        h = S.claim_hash(idx, ok)
        if h in classes:
            classes[h]["aliases"].append("+".join(c))
            continue
        keep = ok[S.fi[idx]]
        d_treat = S.di[idx[keep]]
        bc = np.bincount(d_treat) if len(d_treat) else np.array([0])
        classes[h] = dict(claim_id="+".join(c), aliases=["+".join(c)], mask=m,
                          hash=h.hex()[:16])
        rows.append(dict(claim_id="+".join(c), hash=h.hex()[:16],
                         eligible_setups=int(ne),
                         support_total=int(keep.sum()),
                         treated_n=int(m.sum()),
                         control_n=int((~m).sum()),
                         treated_dates=int((bc > 0).sum()),
                         top_date_share=float(bc.max() / max(keep.sum(), 1))))
    return classes, pd.DataFrame(rows)


def pick_needle(stats: pd.DataFrame) -> dict:
    """The q50 eligible-support claim. X-only, deterministic, chosen ONCE.

    Ties are broken on the claim's own membership hash — a rule fixed in advance and
    independent of anything the search could later prefer.
    """
    target = stats["support_total"].quantile(0.5, interpolation="nearest")
    cand = stats[stats["support_total"] == target].sort_values("hash")
    if not len(cand):
        d = (stats["support_total"] - target).abs()
        cand = stats.loc[d == d.min()].sort_values("hash")
    r = cand.iloc[0]
    return dict(needle_claim_id=str(r.claim_id), membership_hash=str(r.hash),
                support_total=int(r.support_total), eligible_setups=int(r.eligible_setups),
                treated_n=int(r.treated_n), control_n=int(r.control_n),
                treated_dates=int(r.treated_dates),
                top_date_share=round(float(r.top_date_share), 4),
                support_quantile=0.5, null_family=TS.OPPORTUNITY_LEVEL,
                tie_break="lowest membership hash among exact-q50 claims",
                n_tied=int(len(cand)))


# ── the registered null: outcomes inside (date × family) ─────────────────────
_W: dict = {}


def _perm_once(p: int) -> float:
    y = _W["y"].copy()
    rng = np.random.default_rng([_W["seed"], p])
    for a, b in _W["blocks"]:
        if b - a > 1:
            y[a:b] = rng.permutation(y[a:b])
    th = _W["sup"].theta(y[_W["inv"]])
    return float(np.nanmax(th.to_numpy()))


def main(workers: int = 0):
    t0 = time.time()
    spec = json.load(open(SEARCH_SPEC))
    uni = json.load(open(UNI))
    print(f"\n  SearchSpec {spec['spec_digest']} · k {spec['search']['k_selectable_opportunity']:,}",
          flush=True)

    P, M, fam, dates, ids, ph = load()
    if ph != spec["population"]["population_hash"]:
        raise RuntimeError(f"population moved: {ph}")
    S = CU.Strata(M, fam, dates, verbose=False)

    print(f"  building canonical claim set…", flush=True)
    classes, stats = canonical_claims(M, ids, uni, S)
    k = len(classes)
    if k != spec["search"]["k_selectable_opportunity"]:
        raise RuntimeError(f"universe moved: {k:,} vs frozen "
                           f"{spec['search']['k_selectable_opportunity']:,}")
    print(f"  {k:,} canonical claims · matches the frozen k", flush=True)

    needle = pick_needle(stats)
    seeds = world_seeds()

    # (date × family) blocks — the registered null's unit, with its own diagnostics
    order = np.lexsort((dates, fam))
    inv = np.argsort(order)
    key = pd.Series(fam[order]).astype(str) + "|" + pd.Series(dates[order]).astype(str)
    kv = key.to_numpy()
    cuts = np.r_[0, np.flatnonzero(kv[1:] != kv[:-1]) + 1, len(kv)]
    blocks = list(zip(cuts[:-1].tolist(), cuts[1:].tolist()))
    bsz = np.diff(cuts)

    cap = dict(
        capability_spec_id="COMBO_MINER_V1_PRIMARY_CAPABILITY",
        search_spec_digest=spec["spec_digest"],
        population_hash=ph, search_universe=k,
        primary_delta_pp=DELTA, primary_worlds=N_WORLDS, n_perm=N_PERM,
        PASS=f"detected >= {PASS_AT} / {N_WORLDS}",
        PARTIAL=f"detected {PARTIAL_AT}-{PASS_AT-1} / {N_WORLDS}",
        FAIL=f"detected <= {PARTIAL_AT-1} / {N_WORLDS}",
        detection="planted claim's theta crosses the search-wide max band over ALL "
                  f"{k:,} claims",
        not_detection=["positive theta", "top-K rank", "nominal significance",
                       "best claim in world"],
        needle=needle,
        root_seed=ROOT_SEED,
        world_seed_rule="world_seed_i = ROOT_SEED * 1000 + i",
        world_seeds=seeds,
        permutation_rng="np.random.default_rng([world_seed, permutation_index])",
        null_generator=NULL_GENERATOR,
        null_unit="outcomes permuted inside (date × family)",
        null_provenance="n0_spec.py — the generator the 31-class FWER 0.065 was measured on",
        null_block_diagnostic=dict(n_blocks=int(len(blocks)),
                                   median_size=int(np.median(bsz)),
                                   p90_size=int(np.percentile(bsz, 90)),
                                   max_size=int(bsz.max()),
                                   singleton_share=float((bsz == 1).mean()),
                                   note="small blocks make the band conservative; recorded "
                                        "as a diagnostic, NOT adjusted"),
        world="composition_only_negative + planted delta (combolab_v2)",
        historical_exposure="NONE",
    )
    cap["capability_digest"] = hashlib.sha256(
        json.dumps(cap, sort_keys=True, default=str).encode()).hexdigest()[:16]

    if os.path.exists(CAP_SPEC):
        old = json.load(open(CAP_SPEC))
        if old.get("capability_digest") != cap["capability_digest"]:
            raise RuntimeError("a CapabilitySpec already exists and differs. It is "
                               "immutable once written; a changed needle, seed list or "
                               "criterion is a NEW qualification, not a rerun.")
        print(f"  CapabilitySpec unchanged · {cap['capability_digest']}", flush=True)
    else:
        with open(CAP_SPEC, "w") as f:
            json.dump(cap, f, indent=2, default=str)
        print(f"  FROZE {CAP_SPEC} · {cap['capability_digest']}", flush=True)

    print(f"\n  needle {needle['needle_claim_id']} · support {needle['support_total']:,} "
          f"· {needle['eligible_setups']} strata · {needle['treated_dates']} dates "
          f"(tied {needle['n_tied']})", flush=True)
    print(f"  null blocks {len(blocks):,} · median {int(np.median(bsz))} · "
          f"max {int(bsz.max())} · singletons {(bsz==1).mean():.1%}", flush=True)

    # ── Support, ONCE ────────────────────────────────────────────────────────
    print(f"\n  building Support over {k:,} claims (once, X-only)…", flush=True)
    ts = time.time()
    masks = {c["claim_id"]: c["mask"] for c in classes.values()}
    O = P[["family"]].copy()
    sup = V2I.Support(O, dates, masks, verbose=False)
    del masks
    for c in classes.values():
        c.pop("mask", None)
    print(f"  Support ready · {len(sup.cells):,} selectable · {time.time()-ts:.0f}s · "
          f"RSS {_rss_gb():.1f} GB", flush=True)
    nid = needle["needle_claim_id"]
    if nid not in sup.strata:
        raise RuntimeError(f"needle {nid} is not selectable in Support")

    if not workers:
        free = 17.0 - _rss_gb()
        workers = max(1, min(6, int(free // 1.2)))
    print(f"  permutation workers: {workers}", flush=True)

    # ── the 20 registered worlds ─────────────────────────────────────────────
    global _W
    results = []
    for wi, seed in enumerate(seeds):
        tw = time.time()
        y0 = V2I.composition_world(O, dates, seed=seed, noise=True)
        y = V2I.inject(y0, sup, nid, DELTA)
        th = sup.theta(y)
        needle_theta = float(th[nid])
        rank = int((th > needle_theta).sum()) + 1

        _W = dict(y=y[order], inv=inv, blocks=blocks, sup=sup, seed=seed)
        if workers > 1:
            with mp.get_context("fork").Pool(workers) as pool:
                band = np.asarray(pool.map(_perm_once, range(N_PERM), chunksize=2))
        else:
            band = np.asarray([_perm_once(p) for p in range(N_PERM)])
        thr = float(np.percentile(band, 95))
        det = bool(needle_theta > thr)
        results.append(dict(world=wi, world_seed=seed,
                            needle_theta=round(needle_theta, 4),
                            needle_rank=rank, band_p95=round(thr, 4),
                            band_median=round(float(np.median(band)), 4),
                            band_max=round(float(band.max()), 4),
                            distance_to_band=round(needle_theta - thr, 4),
                            detected=det,
                            observed_max=round(float(th.max()), 4),
                            observed_argmax=str(th.idxmax()),
                            band_draws=[round(float(b), 5) for b in band]))
        n_det = sum(r["detected"] for r in results)
        print(f"    world {wi:>2} seed {seed} · θ_needle {needle_theta:+.3f} · "
              f"band_p95 {thr:+.3f} · {'DETECTED' if det else 'no':<8} · "
              f"rank {rank:>4} · {time.time()-tw:.0f}s · running {n_det}/{wi+1}",
              flush=True)

    n_det = sum(r["detected"] for r in results)
    verdict = ("PASS" if n_det >= PASS_AT else
               "PARTIAL" if n_det >= PARTIAL_AT else "FAIL")
    out = dict(capability_digest=cap["capability_digest"],
               search_spec_digest=spec["spec_digest"], population_hash=ph,
               search_universe=k, delta_pp=DELTA, worlds=N_WORLDS,
               detected=n_det, measured_detection_rate=round(n_det / N_WORLDS, 3),
               verdict=verdict,
               criterion=f"PASS >= {PASS_AT}/{N_WORLDS} · PARTIAL {PARTIAL_AT}-"
                         f"{PASS_AT-1} · FAIL <= {PARTIAL_AT-1}",
               caveat="measured detection rate in a finite capability suite — NOT the true "
                      "statistical power",
               needle=needle, null_generator=NULL_GENERATOR,
               historical_exposure="NONE",
               worlds_detail=results, seconds=round(time.time() - t0, 1))
    with open(CAP_RESULT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n{'='*82}", flush=True)
    print(f"  PRIMARY CAPABILITY   {verdict}", flush=True)
    print(f"    detected {n_det}/{N_WORLDS} · criterion {out['criterion']}", flush=True)
    print(f"    δ = {DELTA} pp · needle {nid} · universe {k:,}", flush=True)
    print(f"    historical exposure NONE · {out['seconds']}s", flush=True)
    print(f"  WROTE {CAP_RESULT}", flush=True)
    print("=" * 82, flush=True)


if __name__ == "__main__":
    main(workers=int(sys.argv[1]) if len(sys.argv) > 1 else 0)
