"""The exact rank statistic for GANN — written out, not inherited by prose.

Every phase world recomputes memberships, so "same V2 as the T-family" is not a binding
statement here. This module IS the definition, and its file hash is what the artifacts
bind.

    block eligibility   a block contributes only when it holds at least one treated and
                        at least one control row with a usable outcome
    U_b                 (sum of treated midranks - n_t(N_b+1)/2) / (n_t n_c) + 0.5
    weights             w_b = n_t,b / sum_b n_t,b   over ELIGIBLE blocks only
    tie correction      Var0(U_b) = [(N_b+1) - sum(t^3-t)/(N_b(N_b-1))] / (12 n_t n_c)
    statistic           R = sum_b w_b (U_b - 0.5),  Z = R / sqrt(sum_b w_b^2 Var0(U_b))
    degenerate blocks   n_t == 0, n_c == 0 or N_b < 2 contribute nothing, to neither the
                        numerator nor the weight normaliser; a claim whose every block is
                        degenerate yields Z = nan and is reported, never silently zeroed

Midranks are computed WITHIN each block, so no cross-block comparison is ever made.

No Y value is read at import time; the caller passes the outcome vector.
"""
from __future__ import annotations
import numpy as np                                                    # noqa: E402


def midranks_and_ties(y: np.ndarray, block: np.ndarray, nb: int):
    """Within-block midranks and the tie sum sum(t^3 - t) per block."""
    r = np.empty(len(y), dtype=float)
    tie = np.zeros(nb, dtype=float)
    order = np.lexsort((y, block))
    b_sorted = block[order]
    y_sorted = y[order]
    start = 0
    n = len(y)
    while start < n:
        end = start
        while end + 1 < n and b_sorted[end + 1] == b_sorted[start]:
            end += 1
        idx = order[start:end + 1]
        vals = y_sorted[start:end + 1]
        m = len(vals)
        i = 0
        while i < m:
            j = i
            while j + 1 < m and vals[j + 1] == vals[i]:
                j += 1
            rank = (i + j) / 2.0 + 1.0
            r[idx[i:j + 1]] = rank
            t = j - i + 1
            if t > 1:
                tie[b_sorted[start]] += t ** 3 - t
            i = j + 1
        start = end + 1
    return r, tie


def z_rank(y: np.ndarray, treated: np.ndarray, block: np.ndarray, nb: int):
    """Blockwise Mann-Whitney/AUC statistic, analytic tie-corrected null SE.

    Returns (Z, diagnostics). Z is nan when no block is eligible.
    """
    r, tie = midranks_and_ties(y, block, nb)
    Nb = np.bincount(block, minlength=nb).astype(float)
    nt = np.bincount(block, weights=treated.astype(float), minlength=nb)
    nc = Nb - nt
    eligible = (nt > 0) & (nc > 0) & (Nb >= 2)
    if not eligible.any():
        return float("nan"), dict(eligible_blocks=0, treated_used=0, control_used=0,
                                  reason="no block holds both arms")
    sum_rt = np.bincount(block, weights=r * treated.astype(float), minlength=nb)
    with np.errstate(divide="ignore", invalid="ignore"):
        U = (sum_rt - nt * (Nb + 1) / 2.0) / (nt * nc) + 0.5
        var = ((Nb + 1) - tie / np.where(Nb > 1, Nb * (Nb - 1), 1.0)) / (12.0 * nt * nc)
    U = np.where(eligible, U, 0.0)
    var = np.where(eligible, var, 0.0)
    w = np.where(eligible, nt, 0.0)
    tot = w.sum()
    if tot <= 0:
        return float("nan"), dict(eligible_blocks=int(eligible.sum()), treated_used=0,
                                  control_used=0, reason="zero treated weight")
    w = w / tot
    R = float((w * (U - 0.5)).sum())
    sd = float(np.sqrt((w ** 2 * var).sum()))
    Z = R / sd if sd > 0 else float("nan")
    return Z, dict(eligible_blocks=int(eligible.sum()),
                   treated_used=int(nt[eligible].sum()),
                   control_used=int(nc[eligible].sum()),
                   R=R, sd=sd)
