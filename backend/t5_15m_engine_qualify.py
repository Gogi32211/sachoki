"""15M_RANK_ENGINE_QUALIFICATION_V1 — what does the 31,583-claim rank statistic cost?

NO Y ACCESS. The statistic is a linear operator on a within-block rank vector; it does not
care whether those ranks came from MFE or from a synthetic fixture. Every mapping used here
is synthetic and deterministic.

    R_s = (1/N_T,s) * sum_b RankSum_T,sb / n_c,sb  +  const_s
    const_s = -sum_b n_t,sb(n_t,sb+1) / (2 N_T,s n_c,sb)  -  1/2
    Z_s = R_s / sqrt(V_s),   V_s = sum_b w_sb^2 Var_0(U_sb)

The algebra above is exactly the blockwise Mann-Whitney form with the treated-episode weights
folded in. The equivalence gate exists because "obviously equivalent" derivations are where
off-by-one errors in n_c, in the tie handling or in the constant term hide, and one of those
would change the whole search without changing anything that looks wrong.

THREE CANDIDATES, ONE GATE

    A  CSR with a coefficient per nnz          + 1,340 MB of coefficients
    B  NumPy two-level segmented               + ~670 MB transient gather per pass
    C  Numba fused segmented                   no nnz-sized temporary at all

SPEED IS NOT THE ONLY AXIS FOR B. If a pass materialises r[episode_idx] in full, that is a
~670 MB int32 (or 1.34 GB float64) temporary every permutation. A good wall-clock number under
memory pressure is not a viable engine, so the transient is measured and reported next to the
timing rather than left implicit.

SEGMENTS/SEC MATTERS AS MUCH AS NNZ/SEC. nnz/n_seg = 3.48: the kernel spends its life on very
short segments, so per-segment overhead, not per-element work, may well dominate.

VARIANCE USES THE NO-TIES FORM, AND THAT IS A DELIBERATE LIMIT

Var_0(U) = (N+1)/(12 n_t n_c) needs only block and arm sizes — pure X. The tie correction needs
the block's Y multiset, so it is NOT computed here. The engine is identical either way; only
the divisor values differ. This qualification therefore certifies the ENGINE, not the
production denominator.

BATCHING IS NOT TESTED HERE. Mixing it in now would confound "which single-permutation engine
is fastest" with "does amortising the index gather help". The winner gets its own batch
qualification afterwards.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, resource, sys, time                                      # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
from numba import njit                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402

ORD = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
DATA = os.path.join(D.ROOT, "data")
OUT = os.path.join(HERE, "T5_15M_RANK_ENGINE_QUALIFICATION_V1.json")

N_BENCH, N_WARM = 12, 3
EQ_CLAIMS, EQ_MAPS = 500, 50
UMBRELLA = {"ANY_T", "ANY_Z", "ANY_L", "L5_ANY", "AD_ANY", "ANY_P", "ANY_D"}


def rss_peak():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def rss_now():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1e9
    except Exception:
        return float("nan")


@njit(cache=True, fastmath=False, nogil=True)
def _fused(rank2, eidx, seg_ptr, seg_nc, claim_seg_ptr, N_T, const, sd, R, Z):
    for c in range(claim_seg_ptr.shape[0] - 1):
        acc = 0.0
        for s in range(claim_seg_ptr[c], claim_seg_ptr[c + 1]):
            tot = 0
            for i in range(seg_ptr[s], seg_ptr[s + 1]):
                tot += rank2[eidx[i]]
            acc += (tot * 0.5) / seg_nc[s]
        r = acc / N_T[c] + const[c]
        R[c] = r
        Z[c] = r / sd[c]


class State:
    """Immutable index structure in the sealed claim order."""

    def __init__(self):
        t0 = time.time()
        P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
        O = pd.read_parquet(os.path.join(DATA, "t5_15m_order.parquet")).sort_values("j")
        M = pd.read_parquet(os.path.join(DATA, "t5_15m_opening_hour.parquet"),
                            columns=["episode_id", "pos", "token_set"])
        # episodes ordered BY BLOCK, so a claim's treated indices come out already grouped
        P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
        self.blk = P.block.to_numpy(np.int64)
        eix = {e: i for i, e in enumerate(P.episode_id.to_numpy())}
        self.nE = len(P)
        self.blk_n = np.bincount(self.blk)
        self.blk_start = np.r_[0, np.cumsum(self.blk_n)[:-1]]

        M = M[M.episode_id.isin(eix)]
        cnt = pd.Series([t for row in M.token_set.str.split() for t in row]).value_counts()
        keep = [t.token_id for t in D.TOKENS_1H
                if t.token_id not in UMBRELLA and cnt.get(t.token_id, 0) > 0
                and cnt.get(t.token_id, 0) / len(M) <= 0.99]
        tix = {t: i for i, t in enumerate(keep)}
        B = np.zeros((4, self.nE, len(keep)), dtype=bool)
        for e, p, ts in zip(M.episode_id.to_numpy(), M.pos.to_numpy(),
                            M.token_set.to_numpy()):
            r = eix[e]
            for t in ts.split():
                jx = tix.get(t)
                if jx is not None:
                    B[p - 1, r, jx] = True
        print(f"  tokens {len(keep)} · membership matrix {B.nbytes/1e6:.0f} MB · "
              f"{time.time()-t0:.0f}s", flush=True)

        k = len(O); nnz = ORD["nnz_treated_memberships"]; nseg = ORD["n_segments"]
        self.k, self.nnz, self.nseg = k, nnz, nseg
        self.eidx = np.empty(nnz, np.int32)
        self.seg_ptr = np.empty(nseg + 1, np.int64)
        self.seg_nc = np.empty(nseg, np.int32)
        self.claim_seg_ptr = np.empty(k + 1, np.int64)
        self.N_T = np.empty(k, np.int32)
        self.const = np.empty(k, np.float64)
        self.sd = np.empty(k, np.float64)
        self.coef = np.empty(nnz, np.float64)          # candidate A only

        exp_nnz = O.treated_overlap.to_numpy()
        exp_seg = O.n_overlap_blocks.to_numpy()
        pe = ps = 0
        self.seg_ptr[0] = 0; self.claim_seg_ptr[0] = 0
        for c, rep in enumerate(O.representative.to_numpy()):
            fam, _, seq = rep.split("|")
            a, b = fam.split("→")
            ta_, tb_ = seq.split("→")
            v = B[int(a[1]) - 1][:, tix[ta_]] & B[int(b[1]) - 1][:, tix[tb_]]
            idx = np.flatnonzero(v)                     # already block-sorted
            bl = self.blk[idx]
            cut = np.r_[0, np.flatnonzero(bl[1:] != bl[:-1]) + 1, len(bl)]
            nt = np.diff(cut)
            nb_ = self.blk_n[bl[cut[:-1]]]
            nc = nb_ - nt
            # OVERLAP RESTRICTION. A block where every episode is treated has no control arm:
            # it contributes no contrast, n_c = 0 makes the coefficient infinite, and the
            # sealed treated_overlap / n_overlap_blocks already exclude it. Building membership
            # over the whole population instead of the overlap blocks is what produced
            # n_seg 2,889 where the seal says 911.
            keep = nc > 0
            if not keep.all():
                idx = idx[np.repeat(keep, nt)]
                nt = nt[keep]; nc = nc[keep]; nb_ = nb_[keep]
                cut = np.r_[0, np.cumsum(nt)]
            ns = len(nt); NT = int(nt.sum())
            if NT != exp_nnz[c] or ns != exp_seg[c]:
                raise RuntimeError(
                    f"claim {c} ({rep}): built n_t {NT} / n_seg {ns} but the sealed order "
                    f"says {exp_nnz[c]} / {exp_seg[c]}")
            self.eidx[pe:pe + len(idx)] = idx
            self.seg_ptr[ps + 1:ps + 1 + ns] = pe + cut[1:]
            self.seg_nc[ps:ps + ns] = nc
            self.N_T[c] = NT
            self.const[c] = -float(np.sum(nt * (nt + 1.0) / (2.0 * NT * nc))) - 0.5
            w = nt / NT
            varU = (nb_ + 1.0) / (12.0 * nt * nc)       # NO-TIES form, X-only
            self.sd[c] = float(np.sqrt(np.sum(w * w * varU)))
            self.coef[pe:pe + len(idx)] = np.repeat(1.0 / (NT * nc), nt)
            pe += len(idx); ps += ns
            self.claim_seg_ptr[c + 1] = ps
        if pe != nnz or ps != nseg:
            raise RuntimeError(f"state build mismatch: nnz {pe} vs {nnz}, nseg {ps} vs {nseg}")
        del B
        import gc; gc.collect()
        self.nt_seg = np.diff(self.seg_ptr)
        self.seg_claim = np.repeat(np.arange(k), np.diff(self.claim_seg_ptr))
        print(f"  state built · nnz {nnz:,} · n_seg {nseg:,} · {time.time()-t0:.0f}s · "
              f"peak RSS {rss_peak():.2f} GB", flush=True)

    def mapping(self, seed):
        """A synthetic within-block rank vector: rank2 = 2 x midrank, no ties."""
        g = np.random.default_rng(seed)
        r2 = np.empty(self.nE, np.int32)
        for b in range(len(self.blk_n)):
            n = self.blk_n[b]; s = self.blk_start[b]
            r2[s:s + n] = g.permutation(np.arange(1, n + 1, dtype=np.int32)) * 2
        return r2

    # ── candidates ──────────────────────────────────────────────────────────
    def A(self, r2):
        g = self.coef * r2[self.eidx]
        R = np.add.reduceat(g, self.seg_ptr[:-1])
        R = np.add.reduceat(R, self.claim_seg_ptr[:-1]) * 0.5 + self.const
        return R, R / self.sd

    def B_(self, r2):
        g = r2[self.eidx]
        s = np.add.reduceat(g, self.seg_ptr[:-1]).astype(np.float64)
        acc = np.add.reduceat(s * 0.5 / self.seg_nc, self.claim_seg_ptr[:-1])
        R = acc / self.N_T + self.const
        return R, R / self.sd

    def C(self, r2, Rb=None, Zb=None):
        R = np.empty(self.k) if Rb is None else Rb
        Z = np.empty(self.k) if Zb is None else Zb
        _fused(r2, self.eidx, self.seg_ptr, self.seg_nc, self.claim_seg_ptr,
               self.N_T, self.const, self.sd, R, Z)
        return R, Z

    # ── references ──────────────────────────────────────────────────────────
    def ref_python(self, claims, r2):
        """First principles, per claim: U from the rank-sum definition, w from n_t/N_T."""
        out = np.empty(len(claims))
        for i, c in enumerate(claims):
            a, b = self.claim_seg_ptr[c], self.claim_seg_ptr[c + 1]
            NT = float(self.N_T[c]); R = 0.0
            for s in range(a, b):
                lo, hi = self.seg_ptr[s], self.seg_ptr[s + 1]
                nt = float(hi - lo); nc = float(self.seg_nc[s])
                rs = float(r2[self.eidx[lo:hi]].sum()) / 2.0
                U = (rs - nt * (nt + 1.0) / 2.0) / (nt * nc)
                R += (nt / NT) * (U - 0.5)
            out[i] = R
        return out

    def ref_numpy_explicit(self, r2):
        """Full k, U and w written out — the constant term is NOT used, so the algebraic
        folding that A/B/C rely on is what gets tested."""
        rs = np.add.reduceat(r2[self.eidx], self.seg_ptr[:-1]).astype(np.float64) / 2.0
        nt = self.nt_seg.astype(np.float64); nc = self.seg_nc.astype(np.float64)
        U = (rs - nt * (nt + 1.0) / 2.0) / (nt * nc)
        w = nt / self.N_T[self.seg_claim].astype(np.float64)
        R = np.add.reduceat(w * (U - 0.5), self.claim_seg_ptr[:-1])
        return R, R / self.sd


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    print(f"15M_RANK_ENGINE_QUALIFICATION_V1 · claim order {ORD['claim_order_hash']} · "
          f"k {ORD['k']:,} · NO Y ACCESS", flush=True)
    S = State()
    base_rss = rss_now()

    # ── equivalence ─────────────────────────────────────────────────────────
    print("\n  EQUIVALENCE  rtol=0 atol=1e-10", flush=True)
    g = np.random.default_rng(7)
    sub = np.sort(g.choice(S.k, EQ_CLAIMS, replace=False))
    cands = {"A": S.A, "B": S.B_, "C": S.C}
    verdict, worst = {}, {}
    S.C(S.mapping(0))                                   # warm-up compile, discarded
    # the pure-Python reference is the slow part; compute it ONCE per mapping and reuse it
    # across candidates rather than three times over
    tr = time.time()
    eq_maps = [S.mapping(1000 + m) for m in range(EQ_MAPS)]
    eq_ref = [S.ref_python(sub, r2) for r2 in eq_maps]
    print(f"    reference ({EQ_CLAIMS} claims x {EQ_MAPS} mappings, first principles) "
          f"{time.time()-tr:.0f}s", flush=True)
    for nm, fn in cands.items():
        w1 = 0.0
        for r2, ref in zip(eq_maps, eq_ref):
            got, _ = fn(r2)
            w1 = max(w1, float(np.max(np.abs(got[sub] - ref))))
        r2 = S.mapping(99)
        gR, gZ = fn(r2)
        rR, rZ = S.ref_numpy_explicit(r2)
        w2 = max(float(np.max(np.abs(gR - rR))), float(np.max(np.abs(gZ - rZ))))
        ok = w1 <= 1e-10 and w2 <= 1e-10
        verdict[nm] = ok
        worst[nm] = dict(sub_500x50=w1, full_k=w2)
        print(f"    {'✓' if ok else '✗'} {nm}  500x{EQ_MAPS} max|Δ| {w1:.2e} · "
              f"full-k max|Δ| {w2:.2e}", flush=True)

    passed = [n for n, v in verdict.items() if v]
    if not passed:
        raise RuntimeError("no candidate passed equivalence")

    # ── benchmark, PASS candidates only ─────────────────────────────────────
    print(f"\n  BENCHMARK  {N_WARM} warm-up discarded + {N_BENCH} timed mappings", flush=True)
    maps = [S.mapping(5000 + i) for i in range(N_WARM + N_BENCH)]
    Rb, Zb = np.empty(S.k), np.empty(S.k)
    bench = {}
    for nm in passed:
        fn = cands[nm]
        for i in range(N_WARM):
            fn(maps[i]) if nm != "C" else S.C(maps[i], Rb, Zb)
        before = rss_now(); peak_before = rss_peak()
        ts = []
        for i in range(N_WARM, N_WARM + N_BENCH):
            t = time.time()
            fn(maps[i]) if nm != "C" else S.C(maps[i], Rb, Zb)
            ts.append((time.time() - t) * 1000)
        after_peak = rss_peak()
        ts = np.array(ts)
        med = float(np.median(ts)); p95 = float(np.percentile(ts, 95))
        bench[nm] = dict(median_ms=round(med, 1), p95_ms=round(p95, 1),
                         min_ms=round(float(ts.min()), 1), max_ms=round(float(ts.max()), 1),
                         steady_rss_gb=round(before, 2),
                         peak_rss_gb=round(after_peak, 2),
                         transient_gb=round(max(0.0, after_peak - peak_before), 3),
                         nnz_per_sec=round(S.nnz / (med / 1000) / 1e6, 1),
                         seg_per_sec=round(S.nseg / (med / 1000) / 1e6, 1))
        b = bench[nm]
        print(f"    {nm}  median {b['median_ms']:>8.1f} ms · p95 {b['p95_ms']:>8.1f} ms · "
              f"peak RSS {b['peak_rss_gb']:.2f} GB · transient {b['transient_gb']:.2f} GB",
              flush=True)
        print(f"       throughput {b['nnz_per_sec']:.1f} M nnz/s · "
              f"{b['seg_per_sec']:.1f} M seg/s", flush=True)

    win = min(bench, key=lambda n: bench[n]["median_ms"])
    t_perm6 = bench[win]["median_ms"] / 1000 * 6                # six deltas
    workload_h = 60 * 999 * t_perm6 / 3600
    print(f"\n  WINNER {win} · {bench[win]['median_ms']:.1f} ms per single-delta pass")
    print(f"  registered capability workload  60 worlds x 999 perms x 6 δ = "
          f"{workload_h:.1f} h")
    print(f"    (measured throughput, not extrapolated from 1H)")

    payload = dict(
        spec_id="15M_RANK_ENGINE_QUALIFICATION_V1", status="FIXTURE — NO Y ACCESS",
        claim_order_hash=ORD["claim_order_hash"], population_hash=ORD["population_hash"],
        block_assignment_hash=ORD["block_assignment_hash"],
        k=S.k, nnz=S.nnz, n_seg=S.nseg, nnz_per_seg=round(S.nnz / S.nseg, 2),
        variance_form="NO-TIES (N+1)/(12 n_t n_c) — X-only. The tie correction needs the "
                      "block Y multiset and is NOT certified by this run.",
        equivalence=dict(rtol=0, atol=1e-10, sub_claims=EQ_CLAIMS, sub_mappings=EQ_MAPS,
                         verdict=verdict, max_abs_diff=worst),
        benchmark=bench, winner=win,
        single_delta_median_ms=bench[win]["median_ms"],
        six_delta_perm_seconds=round(t_perm6, 3),
        registered_capability_workload_hours=round(workload_h, 2),
        workload_note="60 = 3 needles x 20 worlds; excludes per-(needle,delta,world) rank "
                      "rebuilds and outer-world overhead, which are small but not zero",
        batching="NOT TESTED — a separate batch qualification runs on the winner only",
        y_status="NO Y ACCESS; all rank mappings synthetic and deterministic",
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "k", "winner", "benchmark"))
    print(f"\n  WROTE {OUT} · digest {dig}")


if __name__ == "__main__":
    main()
