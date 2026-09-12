"""CI_WEIGHT_ONLY_EXACT_KERNEL_CANDIDATE
Exploits: Y is INVARIANT across bootstrap draws; only the WEIGHTS change.
Qualification authority is ALWAYS WEIGHTED_MEDIAN_V2. Semantics never change.

Frozen pool sort:  primary Y ascending, secondary canonical row_id ascending.
NaN Y must never enter a pool (excluded by the frozen outcome rules)."""
import numpy as np
EMPTY='EMPTY_EFFECTIVE_GROUP'

def freeze_pool_order(pool_idx, poolY):
    """ONE-TIME per pool. pool_idx must already be ascending row_id."""
    if np.isnan(poolY).any(): raise ValueError("NaN Y in pool — frozen outcome rules violated")
    o=np.argsort(poolY, kind='stable')          # stable => row_id ascending within Y ties
    return o, poolY[o]

def _cpos_prefix(P_ranks, P_cumw, r):
    """cumulative POSITIVE weight through sorted rank r (inclusive)."""
    k=int(np.searchsorted(P_ranks, r, side='right'))
    return 0 if k==0 else int(P_cumw[k-1])

def complement_weighted_median(sortedY, C_pool, P_ranks, P_cumw):
    """EXACT weighted median of the NON-POSITIVE group, without materialising it.
    C_comp(r) = C_pool(r) - C_pos(r), monotone non-decreasing."""
    n=sortedY.size
    W=int(C_pool[n-1]) - (int(P_cumw[-1]) if P_cumw.size else 0)
    if W<=0: return np.nan, EMPTY
    def C(r): return int(C_pool[r]) - _cpos_prefix(P_ranks,P_cumw,r)
    def first_ge(target):
        lo,hi=0,n-1
        while lo<hi:
            mid=(lo+hi)//2
            if C(mid)>=target: hi=mid
            else: lo=mid+1
        return lo
    half=W//2
    if W%2==0:
        r=first_ge(half)
        if C(r)==half:                       # a NON-POSITIVE, positive-weight row ends exactly at W/2
            r2=first_ge(half+1)              # next row IN THE SAME GROUP with weight > 0
            return float((sortedY[r]+sortedY[r2])/2), 'OK'
    r=first_ge(half+1)
    return float(sortedY[r]), 'OK'
