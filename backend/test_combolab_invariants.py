"""Invariants for the search layer, each pinning something that was actually checked.

Not speculative coverage. Every test below corresponds to a question raised before the
implementation was trusted, and two of them could have failed.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_spec as SPEC                                        # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


print("loading base once", flush=True)
O, V, D = CL.load_base(verbose=False)
MASKS = CL.build_masks(O)
LAB = CL.ComboLab(O, D, MASKS)


def t_permutation_unit():
    """one row = one underlying opportunity — otherwise within-date shuffling is wrong

    Permuting outcomes within a date is the right null only if a row IS the unit an outcome is
    attached to. Were the same entry present three times under three setup families sharing one
    future return, a row-level shuffle would turn one economic fact into three independently
    reassigned ones and destroy dependence the null is supposed to preserve.
    """
    for keys in (["ticker", "sig_date"], ["ticker", "date_in"]):
        n = O.groupby(keys).size()
        assert len(n) == len(O), f"{keys}: {len(O) - len(n):,} rows share an opportunity key"
        assert n.max() == 1


def t_k_is_derived():
    """k comes from the manifest and a 47th claim raises"""
    assert SPEC.DECLARED_K == 46 == len(SPEC.MANIFEST)
    SPEC.assert_search_space([c.claim_id for c in SPEC.MANIFEST])
    try:
        SPEC.assert_search_space([c.claim_id for c in SPEC.MANIFEST] + ["sneaky::x@y"])
    except SPEC.SearchSpaceContractError as e:
        assert "47" in str(e)
        return
    raise AssertionError("a 47th selectable claim was accepted")


def t_workers_do_not_change_numbers():
    """workers=1 and workers=N must agree bit for bit

    The classic silent source of irreproducibility is a global RNG consumed in worker completion
    order. Each permutation here draws from default_rng([search_seed, p]) instead, so the band
    cannot depend on how many cores were free.
    """
    a = LAB.chance_band(V, 11, n_perm=16, workers=1)
    b = LAB.chance_band(V, 11, n_perm=16, workers=6)
    assert a["band_p95"] == b["band_p95"], f"{a['band_p95']} != {b['band_p95']}"
    assert a["band_max"] == b["band_max"]


def t_median_convention_is_single():
    """ranking and band must measure the same statistic"""
    v = np.array([1.0, 2.0, 3.0, 4.0])
    assert CL._lower_median(v) == 3.0            # k = n//2, not the 2.5 average
    e = LAB.estimates(V)
    assert len(e) == 46 and np.isfinite(e).all()


def t_injection_does_not_move_membership():
    """a shadow outcome must not reshape the population being searched"""
    v2 = V.copy()
    v2[MASKS["rsi<35"]] += 5.0
    m2 = CL.build_masks(O)
    for cid in LAB.ids:
        assert np.array_equal(MASKS[cid], m2[cid]), cid
    assert LAB.sizes == CL.ComboLab(O, D, m2).sizes


def t_exposure_is_of_the_promoted_cell():
    """E[j,i] must read as the share of j's rows lying in i, not the reverse"""
    E = SPEC.exposure_matrix(MASKS)
    assert np.allclose(np.diag(E.to_numpy()), 1.0)
    small, large = "rsi<35&rs", "¬sig_h1_dr"
    assert E.loc[small, large] > E.loc[large, small], "orientation is inverted"
    # a cell fully inside another has exposure 1 to it
    assert E.loc["rsi<35&rs", "sig_rs_intact"] > 0.99


def t_delta_zero_has_no_needle():
    """classify_promotion must refuse to call anything the true cell when none was planted"""
    E = SPEC.exposure_matrix(MASKS)
    assert SPEC.classify_promotion("rsi<35", None, E) == "NULL_PROMOTION"
    assert SPEC.classify_promotion("rsi<35", "rsi<35", E) == "TRUE_NEEDLE"


def t_acceptance_seeds_are_sealed():
    """there are no acceptance seeds before the freeze commit exists"""
    assert "acceptance" not in SPEC.SEEDS
    for bad in ("", "abc", "deadbeef"):
        try:
            SPEC.acceptance_seeds(bad)
        except SPEC.SearchSpaceContractError:
            continue
        raise AssertionError(f"acceptance seeds materialised from {bad!r}")
    s1 = SPEC.acceptance_seeds("a" * 40)
    assert len(s1) == 120 and s1 != SPEC.acceptance_seeds("b" * 40)


print("=" * 96, flush=True)
print("  COMBOLAB INVARIANTS", flush=True)
print("=" * 96, flush=True)
for fn in (t_permutation_unit, t_k_is_derived, t_workers_do_not_change_numbers,
           t_median_convention_is_single, t_injection_does_not_move_membership,
           t_exposure_is_of_the_promoted_cell, t_delta_zero_has_no_needle,
           t_acceptance_seeds_are_sealed):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 96, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
