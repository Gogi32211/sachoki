"""Block-weighted median difference — the inherited T5-15m theta, made explicit.

    theta = SUM_b w_b * median(y | treated in b)  -  SUM_b w_b * median(y | control in b)
    w_b   = n_t,b / N_T          over OVERLAP blocks only

Nothing is designed here. The formula and its scope come from t5_15m_historical.theta_for,
which this family's estimand explicitly declares it mirrors. What this module adds is that
the definition now lives in the family's own frozen text and in a conformance-tested
implementation, instead of being implicit in another family's code.

ONE PLACE WHERE T3 TIGHTENS RATHER THAN INHERITS. T5 selected its top slice with
`np.argsort(-Z)[:500]`, and numpy's default sort is NOT stable — claims tied on Z would be
ordered arbitrarily, and on a rerun possibly differently. T3 breaks ties on the sealed claim
order instead, so the slice is reproducible. That is a strict tightening of an underspecified
step, not a change of estimand.

THETA IS NOT A TEST. Its scope (survivors + top slice) is itself selected by Z, so theta
describes magnitude WITHIN an already-selected set. It confirms nothing independently, it
cannot alter survivor status, and no arrow runs back from theta to selection.
"""
from __future__ import annotations
import numpy as np                                                     # noqa: E402

THETA_TOP = 500


def theta_for(claims, eidx, seg_ptr, csp, y, blk, blk_start, blk_n):
    """Weighted median difference for a handful of claims.

    Controls are derived from the block-sorted layout rather than materialised — the full
    control index set does not fit in memory, which is why the scope is a slice.
    """
    out_t = np.empty(len(claims)); out_c = np.empty(len(claims)); out = np.empty(len(claims))
    for k, c in enumerate(claims):
        a, b = csp[c], csp[c + 1]
        NT = sum(seg_ptr[s + 1] - seg_ptr[s] for s in range(a, b))
        wt = wc = 0.0
        for s in range(a, b):
            T = eidx[seg_ptr[s]:seg_ptr[s + 1]]
            bi = blk[T[0]]
            lo = blk_start[bi]; n = blk_n[bi]
            m = np.zeros(n, bool); m[T - lo] = True
            vals = y[lo:lo + n]
            w = len(T) / NT
            wt += w * np.median(vals[m])
            wc += w * np.median(vals[~m])
        out_t[k] = wt; out_c[k] = wc; out[k] = wt - wc
    return out, out_t, out_c


def top_slice(z_obs, j_order, n=THETA_TOP):
    """Top n by Z, ties broken by the SEALED claim order — deterministic, unlike argsort."""
    order = np.lexsort((j_order, -z_obs))
    return order[:n]


def _reference_theta(blocks):
    """Independent, deliberately naive reference. Plain loops over explicit lists."""
    NT = sum(len(t) for t, _ in blocks)
    wt = wc = 0.0
    for treated_vals, control_vals in blocks:
        w = len(treated_vals) / NT
        wt += w * float(np.median(np.array(treated_vals, dtype=float)))
        wc += w * float(np.median(np.array(control_vals, dtype=float)))
    return wt - wc, wt, wc


def conformance():
    """Hand-computable fixture: 3 small blocks, known medians, known weights, known theta."""
    # block 0: treated [1,3,5] med 3      control [2,4] med 3
    # block 1: treated [10,20] med 15     control [4,6,8] med 6
    # block 2: treated [7] med 7          control [1,2,3,100] med 2.5
    spec = [([1., 3., 5.], [2., 4.]), ([10., 20.], [4., 6., 8.]), ([7.], [1., 2., 3., 100.])]
    NT = 3 + 2 + 1
    hand_t = (3 / NT) * 3 + (2 / NT) * 15 + (1 / NT) * 7
    hand_c = (3 / NT) * 3 + (2 / NT) * 6 + (1 / NT) * 2.5
    hand = hand_t - hand_c
    ref, ref_t, ref_c = _reference_theta(spec)

    # lay the fixture out the way the real pipeline stores it: block-sorted, treated first
    y, blk, blk_start, blk_n, eidx, seg_ptr = [], [], [], [], [], [0]
    pos = 0
    for bi, (tv, cv) in enumerate(spec):
        blk_start.append(pos); blk_n.append(len(tv) + len(cv))
        eidx.extend(range(pos, pos + len(tv)))
        seg_ptr.append(len(eidx))
        y.extend(tv + cv); blk.extend([bi] * (len(tv) + len(cv)))
        pos += len(tv) + len(cv)
    got, got_t, got_c = theta_for(
        [0], np.array(eidx), np.array(seg_ptr), np.array([0, 3]), np.array(y, float),
        np.array(blk), np.array(blk_start), np.array(blk_n))

    checks = dict(
        hand_vs_reference=bool(abs(hand - ref) < 1e-12),
        reference_vs_implementation=bool(abs(ref - got[0]) < 1e-12),
        treated_leg=bool(abs(hand_t - got_t[0]) < 1e-12),
        control_leg=bool(abs(hand_c - got_c[0]) < 1e-12))
    # tie determinism: equal Z must order by sealed j, and repeatedly
    z = np.array([5.0, 5.0, 5.0, 1.0]); j = np.array([30, 10, 20, 0])
    a, b = top_slice(z, j, 3), top_slice(z, j, 3)
    checks["tie_break_is_sealed_j"] = list(a) == [1, 2, 0]
    checks["tie_break_is_repeatable"] = list(a) == list(b)
    return dict(hand_theta=round(hand, 12), reference_theta=round(ref, 12),
                implementation_theta=round(float(got[0]), 12),
                treated_leg=round(hand_t, 12), control_leg=round(hand_c, 12),
                fixture="3 blocks · treated [1,3,5]/[10,20]/[7] · "
                        "control [2,4]/[4,6,8]/[1,2,3,100]",
                checks=checks, passed=all(checks.values()))


if __name__ == "__main__":
    r = conformance()
    for k, v in r["checks"].items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  hand {r['hand_theta']} · reference {r['reference_theta']} · "
          f"implementation {r['implementation_theta']}")
    print(f"  CONFORMANCE {'PASS' if r['passed'] else 'FAIL'}")
    raise SystemExit(0 if r["passed"] else 1)
