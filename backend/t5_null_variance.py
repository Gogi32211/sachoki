"""NULL_VARIANCE_DECOMPOSITION_V1 — what makes 54 claims own the raw max.

DIAGNOSTIC ONLY. No new permutation search, no new test, no historical theta.

WHY THIS IS AN IDENTITY AND NOT A REGRESSION

Under within-block permutation the blocks are independent, so the null variance of the
estimand decomposes exactly:

    theta_s = sum_b w_sb * Delta_sb        =>   Var(theta_s) = sum_b w_sb^2 * Var(Delta_sb)

w_sb is X-only (treated counts). Var(Delta_sb) depends on the block's own Y multiset and on
the arm split (n_t, n_c) only — nothing claim-specific beyond those two integers. So the whole
per-claim null scale is reconstructible from block geometry, and the reconstruction can be
CHECKED against the 200-permutation null_sd already measured. A regression would only report
association; this reports whether the mechanism is fully accounted for.

WHY NOT A MEDIAN-DENSITY PROXY

The asymptotic Var(median of m draws) ~ 1/(4 m f(median)^2) assumes m is large. The measured
footprint is ~1.17 treated episodes per block: n_t = 1 is the modal case, where the "treated
median" is a SINGLE draw whose variance is the block's raw dispersion, heavy tail included.
The density approximation understates exactly the case that dominates. So Var(Delta_b) is
computed directly: exactly by leave-one-out when n_t = 1, by permutation Monte Carlo
otherwise. The density proxy is still reported, as a descriptive column, to show where it
would have misled.

WHY BLOCK-LEVEL Y DISPERSION IS NOT AN EXPOSURE

Within-block permutation preserves each block's Y multiset exactly, so block dispersion is
identical in the null world and in the real data — it carries no information about which
sequences pay. What is read here is the spread of Y inside a block, never the association
between membership and Y. Historical theta remains uncomputed.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, resource, sys, tempfile, time                            # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R                                          # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
GEO = os.path.join(DATA, "t5_null_geometry_per_claim.parquet")
PER = os.path.join(DATA, "t5_null_variance_per_claim.parquet")
BLK = os.path.join(DATA, "t5_null_variance_per_block.parquet")
OUT = os.path.join(HERE, "T5_NULL_VARIANCE_DECOMPOSITION_V1.json")

MC = 400                     # permutation draws per (block, n_t>=2) cell
LOCAL_F = 0.125              # central mass fraction for the median-density proxy
SEED = 20260824


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def atomic_parquet(df, path):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    os.close(fd)
    df.to_parquet(tmp, index=False)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def loo_var_nt1(ys):
    """EXACT permutation variance of Delta_b when n_t = 1.

    Delta = y_i - median(all others). Every element is equally likely to be the treated one,
    so the variance is over the n_b leave-one-out values. This is the modal case and it is the
    one the asymptotic median formula gets wrong."""
    n = len(ys)
    s = np.sort(ys)
    # median of the n-1 remaining elements, for each removed index, in sorted order
    m = n - 1
    if m % 2:                                   # odd -> single middle element
        h = m // 2
        med = np.where(np.arange(n) <= h, s[h + 1], s[h])
    else:                                       # even -> mean of two middles
        h = m // 2
        lo = np.where(np.arange(n) <= h - 1, s[h], s[h - 1])
        hi = np.where(np.arange(n) <= h, s[h + 1], s[h])
        med = 0.5 * (lo + hi)
    d = s - med
    return float(d.var(ddof=0))


def mc_var(ys, nt, rng):
    """Permutation variance of median(treated) - median(control) for a given arm split."""
    n = len(ys)
    out = np.empty(MC)
    for i in range(MC):
        p = rng.permutation(n)
        out[i] = np.median(ys[p[:nt]]) - np.median(ys[p[nt:]])
    return float(out.var(ddof=0))


def main():
    t0 = time.time()
    print("NULL_VARIANCE_DECOMPOSITION_V1 · no new permutation search", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    print(f"  state ready · N={len(y):,} blocks={nb:,} · RSS {rss():.2f} GB", flush=True)

    # ── flatten claim x block geometry, then release the heavy structure ─────
    cj, cb, cnt, cnc, cw = [], [], [], [], []
    for j, (ob, rt, rc, w) in enumerate(claims):
        cj.append(np.full(len(ob), j)); cb.append(np.asarray(ob))
        cnt.append(np.array([len(rt[bb]) for bb in ob]))
        cnc.append(np.array([len(rc[bb]) for bb in ob]))
        cw.append(np.asarray(w, float))
    cj = np.concatenate(cj); cb = np.concatenate(cb)
    cnt = np.concatenate(cnt); cnc = np.concatenate(cnc); cw = np.concatenate(cw)
    del claims
    import gc; gc.collect()
    print(f"  claim x block pairs {len(cj):,} · RSS {rss():.2f} GB", flush=True)

    # ── per block: Y multiset properties (identical under within-block permutation) ──
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    ysort = [np.sort(y[bo[a:c]]) for a, c in zip(st[:-1], st[1:])]
    bn = np.array([len(v) for v in ysort])
    biqr = np.array([np.percentile(v, 75) - np.percentile(v, 25) if len(v) > 1 else 0.0
                     for v in ysort])
    bsd = np.array([v.std(ddof=0) if len(v) > 1 else 0.0 for v in ysort])
    # median-density proxy, reported to show where it would have misled
    bspace = np.array([(np.percentile(v, 50 + LOCAL_F * 100)
                        - np.percentile(v, 50 - LOCAL_F * 100)) if len(v) > 2 else 0.0
                       for v in ysort])

    # ── Var(Delta_b) per (block, n_t) actually occurring ─────────────────────
    key = cb.astype(np.int64) * 1000 + np.minimum(cnt, 999)
    uk, inv = np.unique(key, return_inverse=True)
    ukb = (uk // 1000).astype(int); ukt = (uk % 1000).astype(int)
    n1 = int((ukt == 1).sum())
    print(f"  distinct (block, n_t) cells {len(uk):,} · n_t=1 exact {n1:,} "
          f"({n1/len(uk):.1%}) · MC {len(uk)-n1:,} x {MC}", flush=True)
    rng = np.random.default_rng(SEED)
    V = np.empty(len(uk))
    tm = time.time()
    for i in range(len(uk)):
        v = ysort[ukb[i]]; nt = ukt[i]
        if nt >= len(v):                       # no control left: cannot occur in an overlap
            V[i] = np.nan
        elif nt == 1:
            V[i] = loo_var_nt1(v)
        else:
            V[i] = mc_var(v, nt, rng)
        if (i + 1) % 5000 == 0:
            print(f"    cell {i+1:,}/{len(uk):,} · {(time.time()-tm)/60:.1f}m", flush=True)
    vb = V[inv]                                # Var(Delta_b) for every claim x block pair

    # ── per-claim aggregation ────────────────────────────────────────────────
    k = 840
    def agg(vals, fn):
        return np.array([fn(vals[cj == j]) for j in range(k)])
    hhi = np.bincount(cj, weights=cw ** 2, minlength=k)
    predvar = np.bincount(cj, weights=(cw ** 2) * np.nan_to_num(vb), minlength=k)
    wbsd = np.bincount(cj, weights=cw * bsd[cb], minlength=k)
    wbiqr = np.bincount(cj, weights=cw * biqr[cb], minlength=k)
    wbspace = np.bincount(cj, weights=cw * bspace[cb], minlength=k)

    G = pd.read_parquet(GEO).sort_values("j").reset_index(drop=True)
    P = pd.DataFrame(dict(
        j=np.arange(k), claim_id=G.claim_id.to_numpy(), support=G.support.to_numpy(),
        family=G.family.to_numpy(), null_sd=G.null_sd.to_numpy(),
        argmax_count=G.argmax_count.to_numpy(), argmax_share=G.argmax_share.to_numpy(),
        n_overlap_blocks=G.n_overlap_blocks.to_numpy(),
        max_block_weight=agg(cw, np.max), weight_hhi=hhi, effective_blocks=1.0 / hhi,
        median_treated_per_block=agg(cnt, np.median),
        min_treated_per_block=agg(cnt, np.min),
        median_control_per_block=agg(cnc, np.median),
        share_blocks_nt1=agg(cnt, lambda v: float((v == 1).mean())),
        weighted_block_MFE_scale=wbsd, weighted_block_IQR=wbiqr,
        weighted_median_spacing=wbspace,
        predicted_sd=np.sqrt(predvar)))
    atomic_parquet(P, PER)
    atomic_parquet(pd.DataFrame(dict(block=np.arange(nb), n=bn, sd=bsd, iqr=biqr,
                                     local_spacing=bspace)), BLK)

    # ── does the identity reconstruct the measured null_sd? ──────────────────
    ok = np.isfinite(P.predicted_sd) & (P.null_sd > 0)
    ratio = (P.predicted_sd / P.null_sd)[ok]
    lp, lm = np.log(P.predicted_sd[ok]), np.log(P.null_sd[ok])
    recon = dict(spearman=round(float(pd.Series(lp.values).corr(pd.Series(lm.values),
                                                                method="spearman")), 4),
                 pearson_log=round(float(np.corrcoef(lp, lm)[0, 1]), 4),
                 loglog_slope=round(float(np.polyfit(lm, lp, 1)[0]), 3),
                 ratio_p10=round(float(ratio.quantile(.10)), 3),
                 ratio_median=round(float(ratio.median()), 3),
                 ratio_p90=round(float(ratio.quantile(.90)), 3),
                 note="predicted_sd is an analytic identity under within-block permutation; "
                      "residual spread is Monte-Carlo error in Var(Delta_b) plus the 200-perm "
                      "sampling error of the MEASURED null_sd, which is itself ~5%.")

    # ── variance attribution: which factor separates the dominators ──────────
    top = P.sort_values("argmax_count", ascending=False).head(20)
    rest = P.drop(top.index)
    cols = ["support", "n_overlap_blocks", "effective_blocks", "weight_hhi",
            "max_block_weight", "median_treated_per_block", "share_blocks_nt1",
            "median_control_per_block", "weighted_block_MFE_scale", "weighted_block_IQR",
            "weighted_median_spacing", "null_sd", "predicted_sd"]
    comp = {c: dict(top20=round(float(top[c].median()), 4),
                    rest820=round(float(rest[c].median()), 4),
                    ratio=round(float(top[c].median() / rest[c].median()), 2)
                    if rest[c].median() else None) for c in cols}

    # sequential attribution of log(null_sd): each term's marginal R^2 gain
    yv = np.log(P.null_sd[ok].to_numpy())
    steps, R2 = [], []
    terms = [("log(support)", np.log(P.support[ok])),
             ("log(effective_blocks)", np.log(P.effective_blocks[ok])),
             ("log(weighted_block_MFE_scale)", np.log(P.weighted_block_MFE_scale[ok] + 1e-9)),
             ("max_block_weight", P.max_block_weight[ok]),
             ("share_blocks_nt1", P.share_blocks_nt1[ok])]
    X = np.ones((ok.sum(), 1))
    prev = 0.0
    for name, col in terms:
        X = np.column_stack([X, np.asarray(col, float)])
        bhat, *_ = np.linalg.lstsq(X, yv, rcond=None)
        r2 = 1 - ((yv - X @ bhat) ** 2).sum() / ((yv - yv.mean()) ** 2).sum()
        steps.append(dict(term=name, cumulative_R2=round(float(r2), 4),
                          gain=round(float(r2 - prev), 4)))
        R2.append(r2); prev = r2
    Xp = np.column_stack([np.ones(ok.sum()), np.log(P.predicted_sd[ok])])
    bh, *_ = np.linalg.lstsq(Xp, yv, rcond=None)
    r2p = 1 - ((yv - Xp @ bh) ** 2).sum() / ((yv - yv.mean()) ** 2).sum()

    rep = dict(spec_id="NULL_VARIANCE_DECOMPOSITION_V1", status="DIAGNOSTIC_ONLY",
               estimand_digest=SPEC["estimand_digest"], k=840,
               identity="Var(theta_s) = sum_b w_sb^2 Var(Delta_sb) — exact under within-block "
                        "permutation, blocks independent",
               var_delta_method=dict(n_t_1="exact leave-one-out", n_t_ge2=f"MC {MC} draws",
                                     cells=int(len(uk)), exact_share=round(n1 / len(uk), 4)),
               reconstruction=recon,
               reconstruction_R2_log=round(float(r2p), 4),
               sequential_attribution=steps,
               top20_vs_rest=comp,
               density_proxy_caveat="weighted_median_spacing is reported for comparison only. "
                                    "With ~1 treated episode per block the asymptotic median-"
                                    "density variance does not apply; the exact leave-one-out "
                                    "variance is what enters predicted_sd.",
               historical_theta="NOT COMPUTED, NOT EXPOSED",
               standardized_hurdle="Z=7.61 remains a diagnostic indication only — "
                                   "self-standardized on the same 200 permutations, not a "
                                   "calibrated critical value.",
               minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)

    print(f"\n  RECONSTRUCTION  predicted_sd vs measured null_sd")
    print(f"    spearman {recon['spearman']:.4f} · log-log slope {recon['loglog_slope']:+.3f} "
          f"· R2(log) {r2p:.4f}")
    print(f"    ratio  p10 {recon['ratio_p10']}  median {recon['ratio_median']}  "
          f"p90 {recon['ratio_p90']}")
    print(f"\n  SEQUENTIAL ATTRIBUTION of log(null_sd)")
    for s in steps:
        print(f"    {s['term']:<32} cum R2 {s['cumulative_R2']:.4f}  (+{s['gain']:.4f})")
    print(f"\n  TOP20 MAX-WINNERS vs REMAINING 820")
    print(f"    {'':<30} {'top20':>10} {'rest820':>10} {'ratio':>7}")
    for c in cols:
        v = comp[c]
        print(f"    {c:<30} {v['top20']:>10.3f} {v['rest820']:>10.3f} "
              f"{(v['ratio'] if v['ratio'] is not None else float('nan')):>7.2f}")
    print(f"\n  WROTE {OUT}\n        {PER}\n        {BLK}", flush=True)


if __name__ == "__main__":
    main()
