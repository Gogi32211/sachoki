"""T5_15M_CAPABILITY_RANK_V2 — pick the needles, gate the tie correction, freeze the spec.

NO Y ACCESS. Needle selection reads support, which is a property of X. The tie gate runs on
synthetic multisets. The first read of 15m MFE values happens only after this file's artifact
exists.

NEEDLE SELECTION IS THE 1H RULE, UNCHANGED

    source        the sealed 15m claim order, hash 3a0c637ae5828243, k = 31,583
    variable      treated_overlap on the final estimand population
    order         support ascending, tie broken by sealed j
    nearest-rank  idx(q) = ceil(q * k) - 1

The chosen identities are written into the spec — sealed_j, membership hash, representative,
family, support — so a needle is never again dependent on re-running the selection. 1H
survivors play no part in it.

THE TIE CORRECTION IS THE ONE PIECE THE ENGINE QUALIFICATION DID NOT CERTIFY

That run used Var_0(U) = (N+1)/(12 n_t n_c), the no-ties form, because it needs only block and
arm sizes and is therefore pure X. Production needs the tie-corrected exact randomization
variance, which reads the block's Y multiset. So it gets its own gate here, against EXHAUSTIVE
ENUMERATION of every split rather than against another formula:

    Var_0(U) = [ (N+1) - sum(t^3 - t) / (N(N-1)) ] / (12 n_t n_c)

checked over a grid of N, n_t and tie patterns including the degenerate all-identical block,
where the variance must come out exactly zero.

DELTA CHANGES THE MULTISET, SO THE DENOMINATOR IS RECOMPUTED PER (needle, world, delta)

Injecting delta on the raw outcome can break ties apart or create new ones. The conditional
randomization variance must belong to the multiset it is dividing, so:

    delta shifts Y -> midranks may change -> tie correction may change
      -> analytic null SE recomputed exactly -> the 999 inner permutations use THAT fixed SE

This is not empirical studentization. The denominator is still a combinatorial null variance
computed from the multiset; it is never estimated from the permutation numerator.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import itertools, json, math, sys, time                               # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402

ORD = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
SCOPE = json.load(open("T5_15M_SEARCH_SCOPE_V2.json"))
AMEND = json.load(open("T5_15M_INFERENCE_AMENDMENT_V2.json"))
BATCH = json.load(open("T5_15M_BATCH_QUALIFICATION_V1.json"))
PAR = json.load(open("T5_15M_PARALLEL_QUALIFICATION_V1.json"))
OUT = os.path.join(HERE, "T5_15M_CAPABILITY_RANK_V2.json")
DELTAS = [0.5, 1.0, 2.0, 3.0, 4.0, 5.0]
WORLDS, N_PERM = 20, 999


def midranks_ties(v):
    n = len(v); o = np.argsort(v, kind="stable"); s = v[o]
    mr = np.empty(n); tie = 0.0; i = 0
    while i < n:
        j = i
        while j + 1 < n and s[j + 1] == s[i]:
            j += 1
        m = j - i + 1
        mr[o[i:j + 1]] = (i + j) / 2.0 + 1.0
        if m > 1:
            tie += m ** 3 - m
        i = j + 1
    return mr, tie


def var_formula(v, nt):
    n = len(v); nc = n - nt
    _, tie = midranks_ties(np.asarray(v, float))
    return ((n + 1.0) - tie / (n * (n - 1.0))) / (12.0 * nt * nc)


def var_exact(v, nt):
    """Every C(N, n_t) split, enumerated. No formula on either side of the comparison."""
    v = np.asarray(v, float); n = len(v); us = []
    for T in itertools.combinations(range(n), nt):
        Ts = set(T)
        a = v[list(T)][:, None]; b = v[[i for i in range(n) if i not in Ts]][None, :]
        us.append(((a > b).sum() + 0.5 * (a == b).sum()) / (nt * (n - nt)))
    u = np.array(us)
    return float(u.mean()), float(u.var())


def tie_gate():
    """Exhaustive check of the tie-corrected variance, including the degenerate block."""
    cases = []
    patterns = {
        "no ties":      lambda n: list(range(1, n + 1)),
        "one pair":     lambda n: [1, 1] + list(range(2, n)),
        "one triple":   lambda n: [1, 1, 1] + list(range(2, n - 1)),
        "two pairs":    lambda n: [1, 1, 2, 2] + list(range(3, n - 1)),
        "all identical": lambda n: [7] * n,
    }
    worst_m = worst_v = 0.0
    for n in range(4, 10):
        for pname, mk in patterns.items():
            v = mk(n)
            if len(v) != n:
                continue
            for nt in range(1, n):
                em, ev = var_exact(v, nt)
                fv = var_formula(v, nt)
                dm = abs(em - 0.5); dv = abs(ev - fv)
                worst_m = max(worst_m, dm); worst_v = max(worst_v, dv)
                cases.append(dict(n=n, n_t=nt, pattern=pname,
                                  exact_var=round(ev, 12), formula_var=round(fv, 12),
                                  d_mean=dm, d_var=dv))
    ok = worst_m <= 1e-12 and worst_v <= 1e-12
    print(f"  TIE GATE · {len(cases)} cases (N 4..9 x n_t x 5 patterns) vs exhaustive "
          f"enumeration")
    print(f"    max |E[U] - 0.5|   {worst_m:.2e}")
    print(f"    max |Var - formula| {worst_v:.2e}   {'PASS' if ok else 'FAIL'}")
    degen = [c for c in cases if c["pattern"] == "all identical"]
    print(f"    degenerate all-identical block: variance {max(c['formula_var'] for c in degen):.2e} "
          f"(must be exactly 0)")
    if not ok:
        bad = max(cases, key=lambda c: c["d_var"])
        raise RuntimeError(f"tie correction does not reproduce exhaustive enumeration: {bad}")
    return dict(cases=len(cases), max_abs_mean_dev=worst_m, max_abs_var_dev=worst_v,
                passed=True,
                method="exhaustive enumeration of all C(N,n_t) splits; no formula on the "
                       "reference side",
                grid="N 4..9 x every n_t x {no ties, one pair, one triple, two pairs, "
                     "all identical}")


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    print(f"T5_15M_CAPABILITY_RANK_V2 · claim order {ORD['claim_order_hash']} · "
          f"k {ORD['k']:,} · NO Y ACCESS", flush=True)
    tg = tie_gate()

    O = pd.read_parquet(os.path.join(D.ROOT, "data", "t5_15m_order.parquet"))
    if len(O) != ORD["k"]:
        raise RuntimeError("order parquet does not match the sealed k")
    S = O.sort_values(["treated_overlap", "j"], kind="stable").reset_index(drop=True)
    k = len(S)
    needles = {}
    print(f"\n  NEEDLES · support ascending, tie-break sealed j, "
          f"idx(q) = ceil(q*k) - 1, k = {k:,}")
    for q, nm in ((0.10, "q10"), (0.50, "q50"), (0.90, "q90")):
        i = math.ceil(q * k) - 1
        r = S.iloc[i]
        needles[nm] = dict(quantile=q, nearest_rank_index=int(i),
                           sealed_j=int(r.j),
                           membership_hash=r.estimand_membership_hash,
                           representative=r.representative,
                           position_family=r.position_family,
                           treated_overlap=int(r.treated_overlap),
                           control_overlap=int(r.control_overlap),
                           n_overlap_blocks=int(r.n_overlap_blocks),
                           n_names=int(r.n_names))
        print(f"    {nm}  idx {i:>6,} · j {int(r.j):>6,} · n {int(r.treated_overlap):>6,} · "
              f"{r.representative}")

    t6 = BATCH["measured_t6_ms"] / 1000.0
    body = dict(
        spec_id="T5_15M_CAPABILITY_RANK_V2", status="FROZEN_BEFORE_ANY_15M_Y_ACCESS",
        governing=dict(base_spec="T5_15M_OPENING_HOUR_V1",
                       inference_amendment=AMEND["amendment_digest"],
                       search_scope=SCOPE["scope_digest"],
                       claim_order=ORD["claim_order_hash"],
                       population_hash=ORD["population_hash"],
                       block_assignment_hash=ORD["block_assignment_hash"]),

        search_universe=dict(k=ORD["k"], families=SCOPE["primary_search"]["families"],
                             deferred=SCOPE["deferred"]["families"]),
        selection_statistic="bounded blockwise Mann-Whitney / AUC, analytically studentized",
        multiplicity="search-wide max Z_rank over all k claims, one permutation mapping "
                     "applied to every claim",
        direction="one-sided positive",

        needles=needles,
        needle_selection_rule=dict(
            source="sealed 15m claim order", variable="treated_overlap on the final estimand "
                                                      "population",
            order="support ascending, tie-break sealed j",
            nearest_rank="idx(q) = ceil(q * k) - 1",
            independence="1H survivors play no part in needle selection"),

        delta_grid_pp=DELTAS, worlds=WORLDS, n_perm_inner=N_PERM,
        logical_cells=3 * WORLDS * len(DELTAS),

        outer_noise="real MFE_10D with the sequence<->Y association destroyed first by "
                    "permutation within the frozen blocks",
        injection="additive shift of the raw MFE_10D on the needle's treated episodes; never "
                  "on U or R",
        detection="Z_rank(needle, delta) > p95( max_s Z_rank ) in the same inner-null world",

        denominator=dict(
            form="exact combinatorial null variance with the rank-sum tie correction",
            formula="Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c)",
            recomputation="RECOMPUTED PER (needle, world, delta) on that injected multiset. "
                          "Injection can break or create ties, and the conditional "
                          "randomization variance must belong to the multiset it divides.",
            fixed_within="the 999 inner permutations of a (needle, world, delta) cell share "
                         "that one SE",
            not_empirical="never estimated from the permutation numerator; this is not "
                          "studentization by resampling",
            gate=tg),

        engine=dict(kernel="Numba fused segmented, prange over claims", threads=8, batch_B=8,
                    measured_t6_ms=BATCH["measured_t6_ms"],
                    wasted_lanes=2,
                    wasted_lanes_note="B=8 computes eight lanes for a six-delta grid. It is "
                                      "still the measured winner on t6: six lanes cost MORE "
                                      "(590.6 ms) than eight (531.0 ms), consistent with SIMD "
                                      "width. Measured, not chosen on theoretical efficiency.",
                    serial_ms=PAR["serial_median_ms"],
                    parallel_ms=PAR["best_median_ms"],
                    qualifications=["15M_RANK_ENGINE_QUALIFICATION_V1",
                                    "15M_PARALLEL_QUALIFICATION_V1",
                                    "15M_BATCH_QUALIFICATION_V1"]),

        expected_inner_workload_hours=round(60 * N_PERM * t6 / 3600, 2),
        workload_excludes="per-(needle, world, delta) rank rebuild and SE recomputation, "
                          "outer-world setup, checkpoint I/O",

        frozen_and_may_not_change=["support floors", "2-bar scope", "tokens",
                                   "position families", "k = 31,583",
                                   "worlds and permutations once the capability begins",
                                   "needle identities now that they are sealed",
                                   "delta grid"],

        remaining_gate_before_exposure=dict(
            when="after SEALED Y ACCESS, before any outcome exposure",
            what="the production analytic_sd built on the real block multisets must pass "
                 "internal integrity checks",
            forbidden="no claim's Z, theta or ranking may be printed by that gate"),

        forbidden_claims=["the 15m result confirms or validates any 1H claim",
                          "15m is an independent replication (same T5 episode universe, same "
                          "10-day outcome)",
                          "a lower band is more power"],
        y_status="NO 15m Y ACCESS AT THE TIME OF THIS FREEZE",
        minutes=round((time.time() - t0) / 60, 1))

    dig = ART.seal(body, OUT, required=("spec_id", "needles", "delta_grid_pp", "worlds"))
    print(f"\n  engine  Numba fused · 8 threads · B=8 · measured t6 {BATCH['measured_t6_ms']} ms")
    print(f"  workload  3 needles x {WORLDS} worlds x {N_PERM} perms x {len(DELTAS)} δ "
          f"= {3*WORLDS*len(DELTAS)} cells · {60*N_PERM*t6/3600:.1f} h inner")
    print(f"  FROZEN · digest {dig}")
    print(f"  WROTE {OUT}")


if __name__ == "__main__":
    main()
