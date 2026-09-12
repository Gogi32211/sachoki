"""EXACT FAST KERNEL — pool/rank path for the per-draw median contrast.
Semantics are IMMUTABLE and defined by the reference (np.partition path).
This file changes the EXECUTION PLAN ONLY."""
import numpy as np

def _median_from_sorted(sorted_Y, sel):
    """sel: callable(j) -> sorted index of the j-th smallest of the target subset.
    Reproduces np.median EXACTLY: odd -> central; even -> (a+b)/2 in float64."""
    pass

def complement_select(R_sorted, j):
    """Sorted index of the j-th smallest element (0-indexed) of the complement of
    the excluded rank set R_sorted, within a sorted array.
    s = j + c, c = #{i : R[i] - i <= j}."""
    if R_sorted.size == 0: return j
    c = int(np.searchsorted(R_sorted - np.arange(R_sorted.size), j, side='right'))
    return j + c

def median_of_complement(sorted_Y, R_sorted):
    """EXACT median of sorted_Y with the positions in R_sorted removed."""
    n_c = sorted_Y.size - R_sorted.size
    if n_c <= 0: return np.nan
    if n_c & 1:
        return float(sorted_Y[complement_select(R_sorted, n_c // 2)])
    a = sorted_Y[complement_select(R_sorted, n_c // 2 - 1)]
    b = sorted_Y[complement_select(R_sorted, n_c // 2)]
    return float((a + b) / 2)                      # SAME float64 op as np.median

def median_of_subset(sorted_Y, R_sorted):
    """EXACT median of the elements AT the positions in R_sorted."""
    m = R_sorted.size
    if m == 0: return np.nan
    if m & 1:
        return float(sorted_Y[R_sorted[m // 2]])
    a = sorted_Y[R_sorted[m // 2 - 1]]; b = sorted_Y[R_sorted[m // 2]]
    return float((a + b) / 2)
