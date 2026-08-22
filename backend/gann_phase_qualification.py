"""GANN_PHASE_X_QUALIFICATION_V1 — all 999 registered phase worlds, X-only.

Engine qualification, not a research gate and not evidence. It answers: does the phase
machinery actually produce 999 well-formed worlds, is phi shared per ticker across both
families, does phi = 0 reproduce the sealed X freeze exactly, and where does the observed
phase sit inside the phase-world geometry?

That last one matters and is NOT a verdict: if phi = 0 sat in the extreme tail of the
event-count distribution, the inference would not become invalid, but we would want to
know it before capability rather than after.

No outcome is loaded. No Z is computed.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_grid as G, gann_phase as P                               # noqa: E402

OUT = "GANN_PHASE_X_QUALIFICATION_V1.json"
CSV = os.path.join(G.ROOT, "data", "gann_phase_world_census.parquet")
N_WORLDS = 999
CLAIMS = ("ASC", "DESC", "CONF")


def rng_root():
    """Derived from the sealed artifacts, never hand-chosen."""
    m = hashlib.sha256()
    for part in ("GANN_PHASE_NULL_V1",
                 ART.file_digest("GANN_X_FREEZE_V1.json"),
                 ART.file_digest("GANN_PHASE_NULL_V1.json"),
                 ART.file_digest("GANN_ESTIMAND_V1.json")):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def run(st, root, n_worlds, verbose=True):
    rows, seeds = [], {}
    t0 = time.time()
    for w in range(n_worlds):
        phi = P.world_phi(root, w, st["tickers"])
        seeds[w] = hashlib.sha256(phi.tobytes()).hexdigest()[:16]
        ws = P.world_state(st, phi)
        c = P.census(st, ws)
        for k in CLAIMS:
            rows.append(dict(world=w, claim=k, **c[k]))
        if verbose and (w + 1) % 100 == 0:
            print(f"  {w+1}/{n_worlds} worlds · {time.time()-t0:.0f}s", flush=True)
    return pd.DataFrame(rows), seeds


def main():
    t0 = time.time()
    Gr = pd.read_parquet(G.OUT_GRID)
    st = P.prepare(Gr)
    root = rng_root()

    # ── phi = 0 must reproduce the sealed freeze exactly ──────────────────
    obs = P.census(st, P.world_state(st, np.zeros(len(st["tickers"]))))
    sealed = json.load(open("GANN_X_FREEZE_V1.json"))
    ev = sealed["claims"]["events"]
    key = dict(ASC="GANN_ASC_TOUCH_V1", DESC="GANN_DESC_TOUCH_V1",
               CONF="GANN_CONFLUENCE_TOUCH_V1")
    freeze_ok = all(obs[k]["events"] == ev[key[k]] for k in CLAIMS) \
        and obs["CONF"]["directional_valid"] == sealed["confluence_directional_valid"] \
        and obs["CONF"]["invalid_conflict"] == sealed["confluence_census"]["INVALID_CONFLICT"]

    # ── shared phi: the SAME draw must drive both families ────────────────
    phi_t = P.world_phi(root, 0, st["tickers"])
    ws0 = P.world_state(st, phi_t)
    ws0b = P.world_state(st, phi_t)
    shared_ok = all(np.array_equal(ws0[k]["onset"], ws0b[k]["onset"]) for k in CLAIMS)
    # a per-family independent draw MUST change the confluence structure
    alt = P.world_state(st, phi_t)
    indep_conf = int((P._family(st, phi_t[st["tick_code"]], +1.0)[0] &
                      P._family(st, np.roll(phi_t, 1)[st["tick_code"]], -1.0)[0]).sum())
    shared_conf = int((ws0["ASC"]["touch"] & ws0["DESC"]["touch"]).sum())
    sharing_matters = indep_conf != shared_conf

    print(f"running {N_WORLDS} phase worlds …", flush=True)
    C, seeds = run(st, root, N_WORLDS)
    C.to_parquet(CSV, index=False)

    # two-run replay on a subset
    C2, seeds2 = run(st, root, 25, verbose=False)
    replay_ok = C2.equals(C[C.world < 25].reset_index(drop=True)) and \
        all(seeds[w] == seeds2[w] for w in range(25))

    invalid = C[(C.events == 0) | (C.directional_valid == 0)]
    manifest = hashlib.sha256(json.dumps(seeds, sort_keys=True).encode()).hexdigest()[:16]

    geo = {}
    for k in CLAIMS:
        s = C[C.claim == k]
        o = obs[k]
        for field in ("events", "directional_valid", "tickers", "dates"):
            v = s[field].to_numpy()
            pct = float((v < o[field]).mean())
            geo[f"{k}.{field}"] = dict(
                observed=o[field], null_min=int(v.min()), null_q05=int(np.quantile(v, .05)),
                null_median=int(np.median(v)), null_q95=int(np.quantile(v, .95)),
                null_max=int(v.max()), observed_percentile=round(pct, 4),
                in_null_range=bool(v.min() <= o[field] <= v.max()))

    checks = {
        "phi = 0 reproduces the sealed X freeze exactly": freeze_ok,
        "world state is deterministic for a given phi": shared_ok,
        "sharing phi across families is load-bearing": sharing_matters,
        f"all {N_WORLDS} worlds generated": int(C.world.nunique()) == N_WORLDS,
        "no structurally invalid world": len(invalid) == 0,
        "two-run replay identical (25 worlds)": replay_ok,
    }
    ok = all(checks.values())
    body = dict(
        spec_id="GANN_PHASE_X_QUALIFICATION_V1", result="PASS" if ok else "FAIL",
        scope="engine qualification of the phase null, X-only; no outcome is loaded and no "
              "Z is computed",
        phase_null=ART.file_digest("GANN_PHASE_NULL_V1.json"),
        x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
        phase_code=ART.file_digest("gann_phase.py"),
        rng=dict(root=root.hex()[:16], namespace=P.PHASE_NS,
                 rule="root = SHA256('GANN_PHASE_NULL_V1' | x_freeze | phase_null | "
                      "estimand); phi[w] = default_rng(SHA256(ns|root|w)).random(n_tickers)",
                 seed_manifest_digest=manifest, worlds=N_WORLDS,
                 tickers=int(len(st["tickers"]))),
        observed_phi0=obs,
        phase_geometry=geo,
        geometry_reading="where the observed phase sits inside the phase-world "
                         "distribution. This is NOT evidence and NOT a verdict — an "
                         "extreme percentile would not invalidate the inference, but it "
                         "must be known before capability rather than discovered after.",
        checks={k: bool(v) for k, v in checks.items()},
        census_table=dict(path=os.path.basename(CSV), rows=int(len(C)),
                          digest=ART.file_digest(CSV)),
        outcome_exposure="NOT_EXPOSED",
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(body, OUT, required=("spec_id", "result", "checks", "phase_geometry"),
                 supersede=os.path.exists(OUT))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print("\n  phase geometry (observed vs 999 null worlds):")
    for k in CLAIMS:
        g = geo[f"{k}.events"]
        print(f"    {k:<5} events observed {g['observed']:>7,} · null "
              f"[{g['null_min']:,} .. {g['null_max']:,}] median {g['null_median']:,} · "
              f"pctile {g['observed_percentile']:.3f}")
        g = geo[f"{k}.directional_valid"]
        print(f"          valid  observed {g['observed']:>7,} · null "
              f"[{g['null_min']:,} .. {g['null_max']:,}] median {g['null_median']:,} · "
              f"pctile {g['observed_percentile']:.3f}")
    print(f"\nGANN_PHASE_X_QUALIFICATION_V1 · {d} · {body['result']} · "
          f"{(time.time()-t0)/60:.1f} min")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
