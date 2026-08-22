"""Vectorised twin of gann_rank — an OPTIMIZATION, never a statistical shortcut.

gann_rank.midranks_and_ties walks every row in pure Python. On 2.87M rows that is seconds
per call, and the registered capability needs 360 x 999 x 3 calls. This module computes the
SAME (r, tie) with array operations and then performs the downstream arithmetic with the
sealed body copied verbatim.

The claim is bit-identity, not approximation, and it is PROVEN rather than argued:
gann_capability_run's rank-rebuild fixture asserts exact equality of Z, of every diagnostic
and of the whole max-Z vector against gann_rank on real data before a single evidentiary
world runs. gann_rank.py remains the reference and is not modified.

WHY THE TWO AGREE EXACTLY, term by term

    order      the same np.lexsort((y, block)); every tie group gets one midrank, so the
               order WITHIN a group cannot matter
    rank       (first + last)/2 + 1 over 0-based WITHIN-BLOCK positions — the reference's
               (i + j)/2 + 1. i + j is an integer, so the half is exact in float64
    tie        t**3 - t summed per block. Exact integers well below 2**53, so the sum is
               independent of the order the terms are added in
    skipping   ineligible blocks are not walked at all; downstream they are zeroed by
               np.where(eligible, ...) in the reference too, so the values never enter
    downstream identical code on identical inputs

PRECONDITION: y must be finite. This is not a limitation of the twin, it is a property of
the reference that the twin made visible. Under NaN the reference's own answer depends on
how its stable lexsort happens to order the NaN rows among themselves — each NaN is its own
tie group, so consecutive ranks are handed out to particular NaN rows, and which treated row
receives which rank changes sum_rt. numba's sort is not stable, so the twin disagreed. The
resolution is a loud precondition, not a silent one: the GANN outcome pair is 100% path
complete with zero NaN (GANN_OUTCOME_VALUES_V1), the runner asserts it, and gann_rank.py is
left exactly as sealed.

No Y value is read at import time; the caller passes the outcome vector.
"""
from __future__ import annotations
import numpy as np                                                     # noqa: E402
from numba import njit                                                 # noqa: E402


# ── prepared form ─────────────────────────────────────────────────────────────
# Nb, n_t, n_c, eligibility and the block weights depend only on `treated` and `block`,
# both of which are X and therefore FIXED across every permutation. Only sum_rt and tie
# move. Hoisting the fixed half out of the loop is what makes 360 x 999 x 3 affordable;
# it changes no value, and the rank-rebuild fixture proves that on real data.

@njit(cache=True)
def _sumrt_tie(yo, tro, bstart, bend, nb, elig):
    """Per block: the treated midrank sum, and sum(t^3 - t) over tie groups.

    Sums are exact: every midrank is a multiple of 0.5 and every tie term is an integer,
    all far below 2**53, so accumulating in block order gives the identical float64 the
    reference's row-order bincount gives.
    """
    sum_rt = np.zeros(nb)
    tie = np.zeros(nb)
    for b in range(nb):
        if not elig[b]:
            continue          # zeroed downstream by np.where(eligible, ...) either way
        s = bstart[b]
        e = bend[b]
        m = e - s
        if m <= 0:
            continue
        vals = yo[s:e]
        srt = np.argsort(vals)
        acc = 0.0
        tacc = 0.0
        i = 0
        while i < m:
            j = i
            vi = vals[srt[i]]
            while j + 1 < m and vals[srt[j + 1]] == vi:   # NaN != NaN -> its own group
                j += 1
            rank = (i + j) / 2.0 + 1.0
            for q in range(i, j + 1):
                if tro[s + srt[q]]:
                    acc += rank
            t = j - i + 1
            if t > 1:
                tacc += float(t) ** 3 - float(t)
            i = j + 1
        sum_rt[b] = acc
        tie[b] = tacc
    return sum_rt, tie


def prepare(treated: np.ndarray, block: np.ndarray, nb: int) -> dict:
    """Everything about a claim that a permutation cannot change."""
    order = np.argsort(block, kind="stable")
    bs = block[order]
    arange = np.arange(nb)
    Nb = np.bincount(block, minlength=nb).astype(float)
    nt = np.bincount(block, weights=treated.astype(float), minlength=nb)
    return dict(order=order.astype(np.int32), tro=np.ascontiguousarray(treated[order]),
                bstart=np.searchsorted(bs, arange).astype(np.int64),
                bend=np.searchsorted(bs, arange, side="right").astype(np.int64),
                nb=nb, Nb=Nb, nt=nt, nc=Nb - nt,
                eligible=(nt > 0) & (Nb - nt > 0) & (Nb >= 2))


def nullize_prepare(nullblock: np.ndarray, n_nullblock: int) -> dict:
    """The segment layout of gann_capability.nullize_pair — it depends only on nullblock."""
    order = np.argsort(nullblock, kind="stable")
    ns = nullblock[order]
    arange = np.arange(n_nullblock)
    return dict(order=order.astype(np.int32), starts=np.searchsorted(ns, arange),
                ends=np.searchsorted(ns, arange, side="right"),
                n=len(nullblock), n_nullblock=n_nullblock)


def nullize_prepared(long_y, short_y, NP: dict, seed: int):
    """gann_capability.nullize_pair with the fixed segment layout hoisted out.

    The loop, its block order and every rng call are untouched, so the random draws are
    consumed in exactly the same sequence and the result is bit-identical; only the
    per-call re-derivation of `order`, `starts` and `ends` is removed.
    """
    rng = np.random.default_rng(seed)
    out = np.arange(NP["n"])
    order, starts, ends = NP["order"], NP["starts"], NP["ends"]
    for b in range(NP["n_nullblock"]):
        seg = order[starts[b]:ends[b]]
        if len(seg) > 1:
            out[seg] = seg[rng.permutation(len(seg))]
    return long_y[out], short_y[out], out


def assert_finite(*arrays) -> None:
    """The precondition, checked once per world rather than once per permutation."""
    for a in arrays:
        if not np.isfinite(a).all():
            raise ValueError("gann_fast requires a finite outcome: under NaN the reference "
                             "statistic itself depends on how its stable sort orders the "
                             "NaN rows, so no twin can be bit-identical to it")


def z_prepared(y: np.ndarray, P: dict):
    """Same arithmetic as gann_rank.z_rank, with the fixed half hoisted.

    Precondition: y is finite — see assert_finite and the module docstring.
    """
    Nb, nt, nc, eligible = P["Nb"], P["nt"], P["nc"], P["eligible"]
    if not eligible.any():
        return float("nan"), dict(eligible_blocks=0, treated_used=0, control_used=0,
                                  reason="no block holds both arms")
    sum_rt, tie = _sumrt_tie(np.ascontiguousarray(y[P["order"]]), P["tro"],
                             P["bstart"], P["bend"], P["nb"], eligible)
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
                   control_used=int(nc[eligible].sum()), R=R, sd=sd)


def midranks_and_ties(y: np.ndarray, block: np.ndarray, nb: int):
    """Within-block midranks and sum(t^3 - t) per block — vectorised."""
    n = len(y)
    if n == 0:
        return np.empty(0, dtype=float), np.zeros(nb, dtype=float)
    order = np.lexsort((y, block))
    b = block[order]
    v = y[order]

    new_block = np.empty(n, dtype=bool)
    new_block[0] = True
    np.not_equal(b[1:], b[:-1], out=new_block[1:])
    bstart = np.flatnonzero(new_block)
    counts = np.diff(np.append(bstart, n))
    pos = np.arange(n) - np.repeat(bstart, counts)          # 0-based within block

    new_grp = new_block.copy()
    new_grp[1:] |= (v[1:] != v[:-1])                        # NaN != NaN -> its own group
    gfirst = np.flatnonzero(new_grp)
    glast = np.append(gfirst[1:], n) - 1
    rank_g = (pos[gfirst] + pos[glast]) / 2.0 + 1.0

    r = np.empty(n, dtype=float)
    r[order] = rank_g[np.cumsum(new_grp) - 1]

    t = (glast - gfirst + 1).astype(np.float64)
    tie = np.bincount(b[gfirst], weights=np.where(t > 1, t ** 3 - t, 0.0), minlength=nb)
    return r, tie


def z_rank(y: np.ndarray, treated: np.ndarray, block: np.ndarray, nb: int):
    """Body copied verbatim from gann_rank.z_rank; only the midrank call is swapped."""
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
