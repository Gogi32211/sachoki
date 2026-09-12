"""WEIGHTED_MEDIAN_V2 — canonical bootstrap implementation of the FROZEN theta functional.

SEMANTIC AUTHORITY (oracle, never used in production):
    construct the expanded multiset in which v_i appears exactly w_i times,
    and return the ORDINARY sample median of it, np.median convention:
        odd  effective N -> central observation
        even effective N -> arithmetic mean of the two central observations
"""
import numpy as np
EMPTY = 'EMPTY_EFFECTIVE_GROUP'

def weighted_median_v2(v, w):
    """Returns (value, status). Never raises on empty input."""
    v = np.asarray(v, dtype=np.float64); w = np.asarray(w)
    if v.size == 0:
        return np.nan, EMPTY
    W = int(w.sum())
    if W <= 0:
        return np.nan, EMPTY
    o = np.argsort(v, kind='stable')
    vs = v[o]; ws = w[o].astype(np.int64)
    cw = np.cumsum(ws)
    half = W // 2
    if W % 2 == 0:
        hit = np.flatnonzero((cw == half) & (ws > 0))
        if hit.size:
            i = int(hit[0])
            nxt = np.flatnonzero((np.arange(len(ws)) > i) & (ws > 0))
            j = int(nxt[0])                      # always exists: cw[-1] = W > half
            return (vs[i] + vs[j]) / 2.0, 'OK'
    j = int(np.searchsorted(cw, W / 2.0, side='right'))
    while j < len(ws) and ws[j] == 0: j += 1
    return float(vs[j]), 'OK'

def _oracle(v, w):
    v = np.asarray(v, dtype=np.float64); w = np.asarray(w).astype(int)
    if v.size == 0 or w.sum() <= 0: return np.nan, EMPTY
    return float(np.median(np.repeat(v, w))), 'OK'
