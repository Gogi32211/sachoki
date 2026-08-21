"""JitTheta6 — the whole segmented estimand in one compiled loop.

WHY THIS AND NOT ANOTHER NUMPY REWRITE

Three numpy-level attempts failed (padded matrix 27.6s, global lexsort 33.6s, fused-delta
55.0s against a 53.8s reference) because none of them touched the actual cost. Profiling it
properly:

    segments                          848,233
    x 6 deltas x 2 arms            10,189,000 median calls per permutation
    53.8s / 10.19M                       5.3 us per call

That 5.3 us is Python/NumPy call overhead on small arrays, not arithmetic. No arrangement of
numpy calls removes it — only removing the calls does. So the segment loop, both arms, all six
deltas and the weighted aggregation move into one njit function and the Python<->NumPy
boundary disappears from the inner loop entirely.

    fastmath=False    reference semantics first; speed must not come from looser floats
    parallel=False    one compiled core measured before any threading is considered

Buffers are allocated ONCE at the largest segment size and sliced, because allocating inside
an 848k-iteration loop would reintroduce the overhead this exists to remove.

The candidate is worthless until it reproduces the reference to atol=1e-10 on all 840 claims
at all six deltas. Two of the three failed attempts looked obviously right too.
"""
from __future__ import annotations
import numpy as np
from numba import njit


@njit(cache=True, fastmath=False, nogil=True)
def _theta6(y, z, T, C, sT, sC, coff, w, deltas, out, bt, bc):
    nd = deltas.shape[0]
    nseg = sT.shape[0] - 1
    d = np.empty((nd, nseg))
    for s in range(nseg):
        a0 = sT[s]; a1 = sT[s + 1]; b0 = sC[s]; b1 = sC[s + 1]
        nt = a1 - a0; nc = b1 - b0
        for k in range(nd):
            dl = deltas[k]
            for i in range(nt):
                ix = T[a0 + i]
                bt[i] = y[ix] + dl * z[ix]
            for i in range(nc):
                ix = C[b0 + i]
                bc[i] = y[ix] + dl * z[ix]
            d[k, s] = np.median(bt[:nt]) - np.median(bc[:nc])
    nclaim = coff.shape[0] - 1
    for j in range(nclaim):
        a = coff[j]; b = coff[j + 1]
        for k in range(nd):
            acc = 0.0
            for t in range(a, b):
                acc += w[t] * d[k, t]
            out[k, j] = acc
    return out


class JitTheta6:
    def __init__(self, flat, n_delta):
        self.T = np.ascontiguousarray(flat.T, np.int64)
        self.C = np.ascontiguousarray(flat.C, np.int64)
        self.sT = np.ascontiguousarray(flat.segT, np.int64)
        self.sC = np.ascontiguousarray(flat.segC, np.int64)
        self.co = np.ascontiguousarray(flat.coff, np.int64)
        self.w = np.ascontiguousarray(flat.w, np.float64)
        mt = int(np.max(np.diff(self.sT))); mc = int(np.max(np.diff(self.sC)))
        self.bt = np.empty(mt); self.bc = np.empty(mc)
        self.out = np.empty((n_delta, self.co.shape[0] - 1))
        self.max_seg = (mt, mc)

    def theta6(self, y, z, deltas):
        return _theta6(np.ascontiguousarray(y, np.float64),
                       np.ascontiguousarray(z, np.float64),
                       self.T, self.C, self.sT, self.sC, self.co, self.w,
                       np.ascontiguousarray(deltas, np.float64),
                       self.out, self.bt, self.bc)


def gate(flat, reference_theta, y, z, deltas, atol=1e-10):
    J = JitTheta6(flat, len(deltas))
    J.theta6(y[:1] * 0 + y, z, deltas)            # warm-up compile, result discarded
    got = J.theta6(y, z, deltas).copy()
    for k, dd in enumerate(deltas):
        ref = reference_theta(y + dd * z)
        if not np.allclose(got[k], ref, atol=atol, rtol=0):
            bad = int(np.argmax(np.abs(got[k] - ref)))
            raise RuntimeError(f"JitTheta6 != reference at delta={dd}: claim {bad}, "
                               f"|diff|={abs(got[k][bad] - ref[bad]):.3e}. REJECTED.")
    return J
