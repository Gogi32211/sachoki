"""RankEngine — the V2 selection statistic, with its denominator registered once.

    U_sb = [ #(Y_T > Y_C) + 1/2 #(Y_T = Y_C) ] / (n_t n_c)
    R_s  = sum_b w_sb (U_sb - 1/2)
    Z_s  = R_s / sqrt(V_s),      V_s = sum_b w_sb^2 Var_0(U_sb)

THE DENOMINATOR IS REGISTERED, NEVER RE-ESTIMATED PER DELTA

Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c) is an exact combinatorial fact
about the block's own Y multiset under the permutation null. It is computed ONCE from the
frozen population and reused at every delta and in every world. A denominator re-estimated
inside an injected world would move with the injection and the six deltas would no longer
share one calibrated ruler.

The tie term is taken from the registered population. Injection can break or create ties, but
the registered SE must not follow it: only 7.7% of blocks carry ties at all and the correction
is third-order, while a delta-dependent scale would be a semantic defect regardless of size.

MIDRANKS ARE RE-INDEXED, NOT RECOMPUTED, INSIDE THE PERMUTATION LOOP

Within-block permutation preserves each block's Y-delta multiset, so the midranks of Y_delta
are computed once per (world, delta) and then permuted along with the episodes. Recomputing
them per permutation would be correct and ~30x slower.

WHY THE EQUIVALENCE GATE COVERS THE INJECTED PATH

The rank-sum identity U = (sum of treated midranks - n_t(n_t+1)/2)/(n_t n_c) is checked
against a direct pairwise count on the INJECTED, PERMUTED vector — the exact path the
capability run takes. Three engine rewrites in this project looked obviously right and were
wrong; the gate is not a formality.
"""
from __future__ import annotations
import numpy as np


def block_midranks(Y, b, nb, counts, starts):
    """Average ranks within block, and sum(t^3 - t) per block, in one global sort."""
    o = np.lexsort((Y, b))
    bs, ys = b[o], Y[o]
    pos = np.arange(len(o), dtype=np.float64) - starts[bs]
    new = np.empty(len(o), bool)
    new[0] = True
    np.not_equal(bs[1:], bs[:-1], out=new[1:])
    new[1:] |= ys[1:] != ys[:-1]
    gid = np.cumsum(new) - 1
    gsum = np.bincount(gid, weights=pos + 1.0)
    gcnt = np.bincount(gid).astype(np.float64)
    mr = np.empty(len(o))
    mr[o] = (gsum / gcnt)[gid]
    gblk = bs[new]                                    # block of each tie group
    tie = np.bincount(gblk, weights=gcnt ** 3 - gcnt, minlength=nb)
    return mr, tie


class RankEngine:
    def __init__(self, y, b, nb, flat, cb):
        self.flat = flat
        self.b = b
        self.nb = nb
        self.counts = np.bincount(b, minlength=nb)
        self.starts = np.r_[0, np.cumsum(self.counts)[:-1]].astype(np.float64)
        self.nt = np.diff(flat.segT).astype(np.float64)
        self.nc = np.diff(flat.segC).astype(np.float64)
        if len(cb) != len(self.nt):
            raise RuntimeError("segment order mismatch between block ids and FlatState")
        # REGISTERED null variance, from the frozen population's own tie structure
        _, tie = block_midranks(y, b, nb, self.counts, self.starts)
        self.registered_tie = tie
        N = self.nt + self.nc
        varU = ((N + 1.0) - tie[cb] / (N * (N - 1.0))) / (12.0 * self.nt * self.nc)
        self.varR = np.add.reduceat(flat.w ** 2 * varU, flat.coff[:-1])
        self.sdR = np.sqrt(self.varR)
        self.half = self.nt * (self.nt + 1.0) / 2.0
        self.denom = self.nt * self.nc
        self.blocks_with_ties = int((tie > 0).sum())

    def midranks(self, Y):
        mr, _ = block_midranks(Y, self.b, self.nb, self.counts, self.starts)
        return mr

    def R(self, mrp):
        f = self.flat
        s = np.add.reduceat(mrp[f.T], f.segT[:-1])
        U = (s - self.half) / self.denom
        return np.add.reduceat(f.w * (U - 0.5), f.coff[:-1])

    def Z(self, mrp):
        return self.R(mrp) / self.sdR

    def gate(self, Yinj, pm, n_seg=400, seed=99, atol=1e-10):
        """Rank-sum identity vs direct pairwise count, on the injected+permuted vector."""
        f = self.flat
        yv = Yinj[pm]
        mrp = self.midranks(Yinj)[pm]
        g = np.random.default_rng(seed)
        worst = 0.0
        for s_ in g.choice(len(self.nt), n_seg, replace=False):
            T = f.T[f.segT[s_]:f.segT[s_ + 1]]
            C = f.C[f.segC[s_]:f.segC[s_ + 1]]
            a, c = yv[T][:, None], yv[C][None, :]
            direct = ((a > c).sum() + 0.5 * (a == c).sum()) / (len(T) * len(C))
            fast = (mrp[T].sum() - self.half[s_]) / self.denom[s_]
            worst = max(worst, abs(direct - fast))
        if worst > atol:
            raise RuntimeError(f"rank-sum identity != pairwise count, max |diff| {worst:.3e}. "
                               "REJECTED.")
        return worst
