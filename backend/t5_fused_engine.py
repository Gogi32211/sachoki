"""FusedTheta6 — one segment traversal, six deltas. Equivalence-gated before any use.

The reference engine computes theta(Y + d*Z) six times, so the 34.2M-element index traversal
and gather happen six times per permutation. But the six vectors differ only on the needle's
membership:

    Y_d = Y_perm + d * Z_perm        Z in {0,1}

so a segment can be gathered ONCE and its six medians computed on the small local array. This
is not the shared-band approximation the design forbids: there are still six distinct
theta[840], six maxima and six p95 bands. Only their computation is fused.

Nothing here is trusted until it reproduces the reference to 1e-10 on all 840 claims at all
six deltas. Two earlier "optimisations" were slower than the code they replaced, and one of
those was slower because padding is wasted on segments whose median size is small — so the
benchmark decides, not the idea.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS","MKL_NUM_THREADS","OPENBLAS_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import numpy as np


class FusedTheta6:
    def __init__(self, flat):
        self.T, self.C = flat.T, flat.C
        self.sT, self.sC = flat.segT, flat.segC
        self.co, self.w = flat.coff, flat.w
        self.n_seg = len(self.sT) - 1
        self.n_claim = len(self.co) - 1

    def theta6(self, y, z, deltas):
        """(6, n_claim). One pass over segments; six medians per segment."""
        T, C, sT, sC = self.T, self.C, self.sT, self.sC
        nd = len(deltas)
        d = np.empty((nd, self.n_seg))
        dl = np.asarray(deltas, float)
        for s in range(self.n_seg):
            it = T[sT[s]:sT[s+1]]; ic = C[sC[s]:sC[s+1]]
            yt, zt = y[it], z[it]
            yc, zc = y[ic], z[ic]
            for k in range(nd):
                d[k, s] = np.median(yt + dl[k]*zt) - np.median(yc + dl[k]*zc)
        out = np.empty((nd, self.n_claim))
        for j in range(self.n_claim):
            a, b = self.co[j], self.co[j+1]
            out[:, j] = d[:, a:b] @ self.w[a:b]
        return out


def gate(flat, reference_theta, y, z, deltas, atol=1e-10):
    """Equivalence gate: fused must reproduce the reference at every delta, every claim."""
    F = FusedTheta6(flat)
    got = F.theta6(y, z, deltas)
    for k, dd in enumerate(deltas):
        ref = reference_theta(y + dd*z)
        if not np.allclose(got[k], ref, atol=atol, rtol=0):
            bad = int(np.argmax(np.abs(got[k]-ref)))
            raise RuntimeError(f"FusedTheta6 != reference at delta={dd}: worst claim {bad}, "
                               f"|diff|={abs(got[k][bad]-ref[bad]):.3e}. Candidate rejected.")
    return F
