"""RANK_CAPABILITY_ENGINE_QUALIFICATION — equivalence and cost. Nothing else.

This produces NO capability statistic. Every Z it computes is discarded. Its only outputs are
"the engine agrees with the definition" and "one world costs this much", because the previous
feasibility guess in this project cost the machine twice and three engine rewrites.

FOUR GATES

    A  registered SE reproduces RANK_NULL_GEOMETRY_DIAGNOSTIC_V1's analytic_sd_R exactly.
       RankEngine is a refactor of that script's inline code; a refactor that silently changed
       the denominator would invalidate every comparison made against the earlier diagnostic.

    B  rank-sum identity == direct pairwise count, on the INJECTED and PERMUTED vector, for
       every needle at the largest delta — the exact path the capability run takes.

    C  the injected path actually re-ranks. If midranks were reused across deltas by mistake,
       Z would be delta-invariant and every capability surface would read 0/N for a reason
       that has nothing to do with sensitivity. Checked by requiring the needle's own Z to
       move monotonically with delta on one fixture world, WITHOUT recording its values.

    D  cost, measured rather than extrapolated from the raw engine: delta injection forces a
       re-rank per (world, delta), which the null diagnostic never paid for.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, resource, sys, time                                      # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_rank_engine as RE   # noqa: E402

V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))
SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = os.path.join(HERE, "RANK_CAPABILITY_ENGINE_QUALIFICATION.json")

DL = np.array(V2["delta_grid_pp"], float)
N_PROBE = 60
WORLDS, NEEDLES, N_PERM = 20, 3, 999


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def main():
    t0 = time.time()
    print("RANK_CAPABILITY_ENGINE_QUALIFICATION · equivalence + cost · no statistic exposed",
          flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    cb = np.concatenate([np.asarray(ob) for ob, rt, rc, w in claims])
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    E = RE.RankEngine(y, b, nb, flat, cb)
    print(f"  state ready · segments {len(E.nt):,} · RSS {rss():.2f} GB · "
          f"blocks with ties {E.blocks_with_ties:,}", flush=True)

    g = {}
    g["estimand_seal"] = V2["inherited_unchanged"]["estimand_seal"] == SEAL["seal_digest"]
    g["k_840"] = len(order) == 840 == len(E.sdR)

    # ── A · registered SE reproduces the earlier diagnostic ─────────────────
    prev = pd.read_parquet(os.path.join(DATA, "t5_rank_null_per_claim.parquet")) \
             .sort_values("j").analytic_sd_R.to_numpy()
    dA = float(np.max(np.abs(E.sdR - prev)))
    g["A_registered_SE_reproduced"] = dA < 1e-12
    print(f"    {'✓' if g['A_registered_SE_reproduced'] else '✗'} A registered SE  "
          f"max |diff| {dA:.2e}", flush=True)

    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    idxf = np.arange(len(y), dtype=float)
    base = R.perm_fast(y, bo, st, np.random.default_rng([20260901, 0, 0]))   # FIXTURE world
    pm0 = R.perm_fast(idxf, bo, st, np.random.default_rng([20260902, 0, 0, 0])).astype(np.int64)

    # ── B · identity on the injected, permuted vector · every needle ────────
    worst, moved = 0.0, {}
    for name, nd in V2["needles"].items():
        col = int(np.flatnonzero(order.j.to_numpy() == nd["sealed_j"])[0])
        memb = S[col].astype(float)
        worst = max(worst, E.gate(base + DL[-1] * memb, pm0, n_seg=250, seed=7))
        # ── C · the injected path must actually re-rank ─────────────────────
        zs = [float(E.Z(E.midranks(base + d * memb))[col]) for d in np.r_[0.0, DL]]
        moved[name] = bool(all(zs[i + 1] > zs[i] for i in range(len(zs) - 1)))
    g["B_identity_on_injected_path"] = worst < 1e-10
    g["C_injection_reranks"] = all(moved.values())
    print(f"    {'✓' if g['B_identity_on_injected_path'] else '✗'} B pairwise identity  "
          f"max |diff| {worst:.2e}  (3 needles, δ={DL[-1]})", flush=True)
    print(f"    {'✓' if g['C_injection_reranks'] else '✗'} C injection re-ranks  "
          f"needle Z strictly increasing in δ: {moved}  [values discarded]", flush=True)

    # ── D · cost, measured ──────────────────────────────────────────────────
    col = int(np.flatnonzero(order.j.to_numpy() == V2["needles"]["q50"]["sealed_j"])[0])
    memb = S[col].astype(float)
    t = time.time()
    MR = [E.midranks(base + d * memb) for d in DL]
    rerank_s = (time.time() - t) / len(DL)
    t = time.time()
    for p in range(N_PROBE):
        pm = R.perm_fast(idxf, bo, st,
                         np.random.default_rng([20260902, 0, 0, p])).astype(np.int64)
        for mr in MR:
            _ = E.Z(mr[pm]).max()                       # DISCARDED
    per_perm = (time.time() - t) / N_PROBE               # one perm, all 6 deltas
    world_min = (per_perm * N_PERM + rerank_s * len(DL)) / 60.0
    total_h = world_min * WORLDS * NEEDLES / 60.0
    v1_h = 8.3
    print(f"    ✓ D cost · re-rank {rerank_s*1000:.0f} ms/δ · perm {per_perm*1000:.0f} ms "
          f"(all 6 δ) · world {world_min:.1f} min", flush=True)

    g["D_cost_measured"] = True
    passed = all(g.values())
    rep = dict(spec_id="RANK_CAPABILITY_ENGINE_QUALIFICATION",
               status="FIXTURE — every Z computed here is discarded; this is not capability "
                      "evidence and produces no detection surface",
               inference_spec=V2["spec_id"], inference_digest=V2["spec_digest"],
               gates=g,
               A_max_abs_sd_diff=dA, B_max_abs_U_diff=float(worst),
               C_needle_Z_monotone_in_delta=moved,
               cost=dict(probe_perms=N_PROBE,
                         rerank_ms_per_delta=round(rerank_s * 1000, 1),
                         ms_per_perm_all_deltas=round(per_perm * 1000, 1),
                         minutes_per_world=round(world_min, 2),
                         worlds=WORLDS, needles=NEEDLES, n_perm_inner=N_PERM,
                         deltas=len(DL),
                         projected_total_hours=round(total_h, 2),
                         v1_q50_actual_hours=v1_h,
                         speedup_vs_v1_per_needle=round(v1_h / (total_h / NEEDLES), 1)),
               peak_rss_gb=round(rss(), 2),
               discarded="all Z values, all maxima, all detection outcomes",
               y_status="HISTORICAL RANKING NOT COMPUTED, NOT EXPOSED",
               passed=bool(passed), minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)

    print(f"\n  PROJECTED  {WORLDS} worlds × {NEEDLES} needles × {N_PERM} perms × "
          f"{len(DL)} δ = {total_h:.1f} h")
    print(f"             V1 spent {v1_h} h on ONE needle · V2 covers three at "
          f"{total_h/NEEDLES:.1f} h each")
    print(f"  peak RSS {rss():.2f} GB · {'PASS' if passed else 'FAIL'}")
    print(f"  WROTE {OUT}", flush=True)
    if not passed:
        raise RuntimeError(f"qualification failed: {[k for k, v in g.items() if not v]}")


if __name__ == "__main__":
    main()
