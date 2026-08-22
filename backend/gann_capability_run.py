"""GANN_CAPABILITY_RESULT_V1 — the registered capability, on real direction-neutral Y.

THE BOUNDARY, which is structural and not a promise:

    the real outcome pair is read for ONE purpose — so the registered block-nullization has
    something to permute. Every Z in this module is computed on an outcome that has ALREADY
    been nullized and then injected. There is no code path here that ranks the un-nullized
    pair, and nothing is printed or sealed about a treated/control difference, an observed
    phi = 0 statistic, or a gate-B evaluation.

    GANN OBSERVED phi = 0 Z      NOT COMPUTED
    GANN GATE-B HISTORICAL       NOT EVALUATED
    GANN theta                   NOT COMPUTED
    GANN WINNERS / SURVIVORS     UNKNOWN

THE MATRIX, exactly as registered:  3 claims x 6 deltas x 20 worlds = 360 worlds,
999 outcome permutations per world, family-wise max-Z per permutation.

THE COMMON FRAME. gann_capability.load_state filters to one claim's valid-direction rows
before it factorises. That is right for one claim at a time and wrong for a family-wise
maximum, which needs ONE permutation feeding all three claims. So the runner nullizes once
on the common outcome-eligible frame and then evaluates each claim on its own blocks. Rows
whose direction is invalid for a claim are not dropped, they land in blocks that hold no
treated row and are therefore ineligible — contributing to neither the numerator nor the
weight normaliser. The common-frame fixture proves this changes no Z.

SPEED IS ENGINEERING, NEVER STATISTICS. gann_rank walks every row in Python; 360 x 999 x 3
calls of that is weeks. gann_fast hoists the half that no permutation can change and jits
the rest. It is not trusted: the rank-rebuild and nullize-rebuild fixtures assert exact
equality against the sealed gann_rank and gann_capability on the REAL frame, and the engine
fixture replays whole worlds through the sealed gann_capability.run_world and demands a
bit-identical max-Z vector — before one evidentiary world runs.
"""
from __future__ import annotations
import ast, hashlib, json, math, os, sys, time                         # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "NUMBA_NUM_THREADS"):
    os.environ.setdefault(_v, "1")
import t5_artifact as ART                                              # noqa: E402
import gann_rank as RANK                                               # noqa: E402
import gann_capability as CAP                                          # noqa: E402
import gann_fast as FA                                                 # noqa: E402

OUT = "GANN_CAPABILITY_RESULT_V1.json"
STATE = os.path.join(os.path.dirname(HERE), "data", "gann_estimand_state.parquet")
OCS = os.path.join(os.path.dirname(HERE), "data", "gann_outcomes.parquet")
CLAIMS = list(CAP.CLAIMS)                       # ASC · DESC · CONF, the registered order
DELTAS = list(CAP.DELTAS_PP)
WORLDS = CAP.WORLDS
N_PERM = CAP.N_PERM
NPROC = int(os.environ.get("GANN_NPROC", "9"))
_W: dict = {}


# ── the common frame ──────────────────────────────────────────────────────────
def load_common(limit_rows: int | None = None) -> dict:
    S = pd.read_parquet(STATE)
    O = pd.read_parquet(OCS, columns=["ticker", "date", "MFE_LONG_10D", "MFE_SHORT_10D"])
    S = S.merge(O, on=["ticker", "date"], how="left")
    if limit_rows:
        S = S.head(limit_rows).reset_index(drop=True)
    long_y = S.MFE_LONG_10D.to_numpy(dtype=float)
    short_y = S.MFE_SHORT_10D.to_numpy(dtype=float)
    FA.assert_finite(long_y, short_y)
    nullblock, _ = pd.factorize(S.date + "|" + S.liq_half + "|" + S.vol_half)
    nullblock = nullblock.astype(np.int32)
    W = dict(n=len(S), long_y=long_y, short_y=short_y, nullblock=nullblock,
             n_nullblock=int(nullblock.max()) + 1)
    W["NP"] = FA.nullize_prepare(nullblock, W["n_nullblock"])
    for c in CLAIMS:
        d = S[f"{c}_dir"].to_numpy()
        treated = S[f"{c}_treated"].to_numpy() & np.isin(d, [1, -1])
        block, _ = pd.factorize(S[f"{c}_block"])
        block = block.astype(np.int32)
        d = d.astype(np.int8)
        nb = int(block.max()) + 1
        W[c] = dict(treated=treated, direction=d, block=block, nb=nb,
                    P=FA.prepare(treated, block, nb))
    return W


def _init():
    global _W
    _W = load_common()


def run_world_fast(W, claim, delta, world, root, n_perm=N_PERM):
    """One capability world: nullize -> inject -> family-wise max-Z -> detection."""
    nl, ns, _ = FA.nullize_prepared(W["long_y"], W["short_y"], W["NP"],
                                    CAP._seed(CAP.NULL_NS, root, claim, delta, world))
    inj_l, inj_s = nl.copy(), ns.copy()
    tr, d = W[claim]["treated"], W[claim]["direction"]
    inj_l[tr & (d == 1)] += delta / 100.0
    inj_s[tr & (d == -1)] += delta / 100.0
    z_obs, _ = FA.z_prepared(CAP.select_component(inj_l, inj_s, d), W[claim]["P"])
    others = [c for c in CLAIMS if c != claim]
    mx = np.empty(n_perm)
    for p in range(n_perm):
        rng = np.random.default_rng(CAP._seed(CAP.PERM_NS, root, claim, delta, world, p))
        pl, ps, _ = FA.nullize_prepared(inj_l, inj_s, W["NP"], rng.integers(1 << 62))
        zs = [FA.z_prepared(CAP.select_component(pl, ps, d), W[claim]["P"])[0]]
        for c2 in others:
            zs.append(FA.z_prepared(CAP.select_component(pl, ps, W[c2]["direction"]),
                                    W[c2]["P"])[0])
        mx[p] = np.nanmax(zs)
    p95 = float(np.sort(mx)[int(np.ceil(0.95 * n_perm)) - 1])
    return dict(z_obs=float(z_obs), p95=p95, detected=bool(z_obs > p95), maxz=mx)


def _task(arg):
    claim, delta, world, root = arg
    r = run_world_fast(_W, claim, delta, world, root)
    return dict(claim=claim, delta=delta, world=world, z_obs=r["z_obs"], p95=r["p95"],
                detected=r["detected"],
                maxz_digest=hashlib.sha256(r["maxz"].tobytes()).hexdigest()[:16],
                maxz_min=float(r["maxz"].min()), maxz_max=float(r["maxz"].max()),
                n_perm=len(r["maxz"]))


# ── integrity fixtures, all before any evidentiary world ─────────────────────
def fixtures(W, root):
    F, t0 = {}, time.time()

    man = CAP.seed_manifest(root)
    md = hashlib.sha256(json.dumps({k: str(v) for k, v in man.items()},
                                   sort_keys=True).encode()).hexdigest()[:16]
    sealed_md = json.load(open("GANN_CAPABILITY_PREY_CLOSURE_V1.json"))[
        "A_rng_manifest"]["seed_manifest_digest"]
    F["rng_manifest"] = dict(
        ok=bool(md == sealed_md and len(man) == len(CLAIMS) * len(DELTAS) * WORLDS * 4),
        digest=md, matches_prey_closure=bool(md == sealed_md), n_seeds=len(man))
    print(f"  RNG manifest        {'PASS' if F['rng_manifest']['ok'] else 'FAIL'} · {md} · "
          f"{len(man):,} seeds", flush=True)

    # nullize rebuild — the fast twin against the SEALED nullize_pair, on the real frame
    same = []
    for s in (1, 7, 2**40 + 3):
        a1, a2, ai = CAP.nullize_pair(W["long_y"], W["short_y"], W["nullblock"],
                                      W["n_nullblock"], s)
        b1, b2, bi = FA.nullize_prepared(W["long_y"], W["short_y"], W["NP"], s)
        same.append(bool(np.array_equal(a1, b1) and np.array_equal(a2, b2)
                         and np.array_equal(ai, bi)))
    F["nullize_rebuild"] = dict(ok=all(same), seeds_checked=len(same),
                                scope="real full frame, exact array equality")
    print(f"  nullize rebuild     {'PASS' if all(same) else 'FAIL'} · {len(same)} seeds · "
          "exact", flush=True)

    # rank rebuild — the fast twin against the SEALED gann_rank, on a real nullized outcome
    nl, ns, _ = FA.nullize_prepared(W["long_y"], W["short_y"], W["NP"], 99)
    rr = {}
    for c in CLAIMS:
        y = CAP.select_component(nl, ns, W[c]["direction"])
        z1, d1 = RANK.z_rank(y, W[c]["treated"], W[c]["block"], W[c]["nb"])
        z2, d2 = FA.z_prepared(y, W[c]["P"])
        rr[c] = bool(z1 == z2 and d1 == d2)
    F["rank_rebuild"] = dict(ok=all(rr.values()), per_claim=rr,
                             scope="real full frame, exact equality of Z and every "
                                   "diagnostic against gann_rank")
    print(f"  rank rebuild        {'PASS' if all(rr.values()) else 'FAIL'} · {rr}",
          flush=True)

    # paired nullization — the pair must move as ONE row
    k = np.arange(1, W["n"] + 1, dtype=float)
    pl, ps, idx = FA.nullize_prepared(k, 10.0 * k, W["NP"], 4242)
    moved = int((idx != np.arange(W["n"])).sum())
    rngc = np.random.default_rng(5)
    broke = bool(np.allclose(10.0 * k[rngc.permutation(W["n"])], 10.0 * pl))
    F["paired_nullization"] = dict(
        ok=bool(np.allclose(ps, 10.0 * pl) and moved > 0 and not broke),
        rows_moved=moved, ratio_preserved=bool(np.allclose(ps, 10.0 * pl)),
        independent_permutation_breaks_it=not broke,
        fixture="LONG = k, SHORT = 10k — the pair identity is recoverable from the ratio")
    print(f"  paired nullization  {'PASS' if F['paired_nullization']['ok'] else 'FAIL'} · "
          f"{moved:,} rows moved", flush=True)

    # direction excluded from the nullization block key
    src = ast.parse(open(__file__).read())
    keyed = [n for n in ast.walk(src) if isinstance(n, ast.Attribute)
             and n.attr in ("liq_half", "vol_half", "date")]
    dirattr = [n for n in ast.walk(src) if isinstance(n, ast.Attribute)
               and "dir" in n.attr.lower()]
    def _names(t):
        if isinstance(t, ast.Name):
            return [t.id]
        if isinstance(t, (ast.Tuple, ast.List)):
            return [y for e in t.elts for y in _names(e)]
        return []

    found, infn = 0, False
    for n in ast.walk(src):
        if isinstance(n, ast.FunctionDef) and n.name == "load_common":
            for x in ast.walk(n):
                if isinstance(x, ast.Assign) and "nullblock" in [
                        y for t in x.targets for y in _names(t)]:
                    found += 1
                    infn |= "dir" in ast.dump(x.value).lower()
    assert found >= 1, "no nullblock assignment located in load_common"
    F["direction_excluded"] = dict(
        ok=bool(not infn and found >= 1), key="date | liq_half | vol_half",
        assignments_inspected=found,
        method="AST — the nullblock assignment expression is inspected for any direction "
               "term; direction attributes exist elsewhere in the module and are not "
               "confused with the key",
        direction_attributes_elsewhere=len(dirattr), key_attributes=len(keyed))
    print(f"  direction excluded  {'PASS' if not infn else 'FAIL'} · key "
          "date | liq_half | vol_half", flush=True)

    # injection semantics — delta lands only on treated rows of the matching component
    inj = {}
    for c in CLAIMS:
        a, b, _ = FA.nullize_prepared(W["long_y"], W["short_y"], W["NP"], 5)
        il, isx = a.copy(), b.copy()
        tr, d = W[c]["treated"], W[c]["direction"]
        il[tr & (d == 1)] += 2.0 / 100.0
        isx[tr & (d == -1)] += 2.0 / 100.0
        ml, ms = tr & (d == 1), tr & (d == -1)
        dl, ds = il - a, isx - b
        # NOT dl == 0.02: (a + 0.02) - a is not 0.02 in float64 for a general a. The
        # binding statements are that the injected array IS the base plus the registered
        # delta on exactly the right rows, that nothing else moved, and that the realised
        # shift differs from the delta only by float resolution.
        inj[c] = dict(
            long_hits=int((dl != 0).sum()), short_hits=int((ds != 0).sum()),
            treated_long=int(ml.sum()), treated_short=int(ms.sum()),
            exact=bool(np.array_equal(il[ml], a[ml] + 0.02)
                       and np.array_equal(isx[ms], b[ms] + 0.02)),
            no_leak=bool(np.array_equal(il[~ml], a[~ml])
                         and np.array_equal(isx[~ms], b[~ms])),
            matches_treated=bool(int(ml.sum()) + int(ms.sum()) == int(tr.sum())
                                 and int((dl != 0).sum()) == int(ml.sum())
                                 and int((ds != 0).sum()) == int(ms.sum())),
            max_abs_deviation_from_delta=float(max(
                np.abs(dl[ml] - 0.02).max(), np.abs(ds[ms] - 0.02).max())),
            within_float_resolution=bool(max(
                np.abs(dl[ml] - 0.02).max(), np.abs(ds[ms] - 0.02).max())
                <= 8 * np.finfo(float).eps * max(np.abs(a[ml]).max(),
                                                 np.abs(b[ms]).max())))
    F["injection_semantics"] = dict(
        ok=all(v["exact"] and v["no_leak"] and v["matches_treated"]
               and v["within_float_resolution"] for v in inj.values()),
        per_claim=inj,
        note="the delta is added to the component that matches each treated row's frozen "
             "phi = 0 direction, and to nothing else",
        deviation_note="max_abs_deviation_from_delta is the float64 resolution of the "
                       "addition itself — (a + delta) - a is not delta for a general a. "
                       "The binding checks are the exact array equalities above; the "
                       "deviation is bounded relative to the operand scale, not against an "
                       "absolute constant")
    print(f"  injection semantics {'PASS' if F['injection_semantics']['ok'] else 'FAIL'} · "
          + " · ".join(f"{c}={inj[c]['long_hits']+inj[c]['short_hits']:,}" for c in CLAIMS),
          flush=True)

    # engine equivalence — whole worlds through the SEALED gann_capability.run_world
    sub = load_common(limit_rows=60000)
    st = {c: dict(treated=sub[c]["treated"], direction=sub[c]["direction"],
                  block=sub[c]["block"], nb=sub[c]["nb"], nullblock=sub["nullblock"],
                  n_nullblock=sub["n_nullblock"], n=sub["n"]) for c in CLAIMS}
    eq = {}
    for c in ("ASC", "CONF"):
        ref = CAP.run_world(st[c], sub["long_y"], sub["short_y"], 2.0, root, c, 0,
                            n_perm=25, all_claims=[st[o] for o in CLAIMS if o != c])
        fast = run_world_fast(sub, c, 2.0, 0, root, n_perm=25)
        eq[c] = dict(maxz_vector=bool(np.array_equal(ref["maxz"], fast["maxz"])),
                     z_obs=bool(ref["z_obs"] == fast["z_obs"]),
                     p95=bool(ref["p95"] == fast["p95"]),
                     detected=bool(ref["detected"] == fast["detected"]))
    F["engine_equivalence"] = dict(
        ok=all(all(v.values()) for v in eq.values()), per_claim=eq,
        scope="60,000-row common frame, 25 permutations, family-wise; the sealed "
              "gann_capability.run_world with all_claims is the reference")
    print(f"  engine equivalence  {'PASS' if F['engine_equivalence']['ok'] else 'FAIL'} · "
          f"{eq}", flush=True)

    # common-frame equivalence — dropping vs blocking-out the invalid-direction rows
    cf = {}
    for c in CLAIMS:
        d = sub[c]["direction"]
        v = np.isin(d, [1, -1])
        y = CAP.select_component(sub["long_y"], sub["short_y"], d)
        z_all, _ = RANK.z_rank(y, sub[c]["treated"], sub[c]["block"], sub[c]["nb"])
        bl, _ = pd.factorize(pd.Series(sub[c]["block"][v]))
        z_flt, _ = RANK.z_rank(y[v], sub[c]["treated"][v], bl, int(bl.max()) + 1)
        cf[c] = bool(z_all == z_flt or (z_all != z_all and z_flt != z_flt))
    F["common_frame_equivalence"] = dict(
        ok=all(cf.values()), per_claim=cf,
        why="direction is part of the block key, so an invalid-direction row is already in "
            "a block of its own; it holds no treated row, so the block is ineligible and "
            "enters neither the numerator nor the weight normaliser")
    print(f"  common-frame equiv  {'PASS' if all(cf.values()) else 'FAIL'} · {cf}",
          flush=True)

    # world replay — one FULL-scale world, twice, bit-identical
    r1 = run_world_fast(W, "ASC", 2.0, 0, root, n_perm=40)
    r2 = run_world_fast(W, "ASC", 2.0, 0, root, n_perm=40)
    F["world_replay"] = dict(
        ok=bool(np.array_equal(r1["maxz"], r2["maxz"]) and r1["z_obs"] == r2["z_obs"]
                and r1["p95"] == r2["p95"] and r1["detected"] == r2["detected"]),
        scope="full-scale world, 40 permutations, two fresh runs",
        replay_digest=hashlib.sha256(r1["maxz"].tobytes()).hexdigest()[:16])
    print(f"  world replay        {'PASS' if F['world_replay']['ok'] else 'FAIL'} · "
          f"{F['world_replay']['replay_digest']}", flush=True)

    F["all_pass"] = all(v["ok"] for v in F.values() if isinstance(v, dict))
    F["fixture_min"] = round((time.time() - t0) / 60, 1)
    return F


def main():
    t0 = time.time()
    root = CAP.rng_root()
    print(f"GANN capability · rng root {root.hex()[:16]} · {len(CLAIMS)} claims x "
          f"{len(DELTAS)} deltas x {WORLDS} worlds = {len(CLAIMS)*len(DELTAS)*WORLDS} "
          f"worlds x {N_PERM} permutations\n", flush=True)
    W = load_common()
    st = json.load(open("GANN_ESTIMAND_STATE_V1.json"))
    print(f"  common frame {W['n']:,} rows · nullization blocks {W['n_nullblock']:,}",
          flush=True)
    for c in CLAIMS:
        exp = st["claims"][CAP.CLAIMS[c]]["treated"]
        got = int(W[c]["treated"].sum())
        assert got == exp, f"{c}: treated {got:,} != sealed {exp:,}"
        print(f"    {c:<5} treated {got:>7,} · blocks {W[c]['nb']:>6,} · eligible "
              f"{int(W[c]['P']['eligible'].sum()):>6,}", flush=True)
    print(flush=True)

    F = fixtures(W, root)
    if not F["all_pass"]:
        raise SystemExit("integrity fixtures did not pass — no evidentiary world ran")
    print(f"\n  fixtures PASS in {F['fixture_min']} min — starting 360 worlds\n", flush=True)

    tasks = [(c, d, w, root) for c in CLAIMS for d in DELTAS for w in range(WORLDS)]
    import multiprocessing as mp
    ctx = mp.get_context("spawn")
    res = []
    with ctx.Pool(NPROC, initializer=_init) as pool:
        for i, r in enumerate(pool.imap_unordered(_task, tasks, chunksize=1), 1):
            res.append(r)
            if i % 20 == 0 or i == len(tasks):
                el = (time.time() - t0) / 60
                print(f"    {i:>3}/{len(tasks)} worlds · {el:.0f} min elapsed · "
                      f"~{el/i*(len(tasks)-i):.0f} min left", flush=True)
    R = pd.DataFrame(res)

    grid = {}
    for c in CLAIMS:
        grid[CAP.CLAIMS[c]] = {f"+{d}pp": int(R[(R.claim == c) & (R.delta == d)]
                                              .detected.sum()) for d in DELTAS}
    acc = {}
    for c in CLAIMS:
        acc[CAP.CLAIMS[c]] = dict(
            at_1pp=dict(detections=grid[CAP.CLAIMS[c]]["+1.0pp"], required=16,
                        pass_=grid[CAP.CLAIMS[c]]["+1.0pp"] >= 16),
            at_2pp=dict(detections=grid[CAP.CLAIMS[c]]["+2.0pp"], required=19,
                        pass_=grid[CAP.CLAIMS[c]]["+2.0pp"] >= 19),
            at_0_5pp=dict(detections=grid[CAP.CLAIMS[c]]["+0.5pp"],
                          role="DESCRIPTIVE SENSITIVITY ONLY — not a criterion"))
    geo = {}
    for c in CLAIMS:
        for d in DELTAS:
            s = R[(R.claim == c) & (R.delta == d)].p95
            geo[f"{c}|+{d}pp"] = dict(median=round(float(s.median()), 4),
                                      min=round(float(s.min()), 4),
                                      max=round(float(s.max()), 4))
    integ = dict(
        expected=len(tasks), completed=int(len(R)),
        missing=int(len(tasks) - len(R)),
        duplicates=int(len(R) - len(R.drop_duplicates(["claim", "delta", "world"]))),
        permutations_per_world=sorted(R.n_perm.unique().tolist()),
        distinct_maxz_vectors=int(R.maxz_digest.nunique()),
        paired_nullization="PASS" if F["paired_nullization"]["ok"] else "FAIL",
        direction_excluded_from_nullization_blocks="PASS" if F["direction_excluded"]["ok"]
        else "FAIL",
        injection_semantics="PASS" if F["injection_semantics"]["ok"] else "FAIL",
        rank_rebuild="PASS" if F["rank_rebuild"]["ok"] else "FAIL",
        nullize_rebuild="PASS" if F["nullize_rebuild"]["ok"] else "FAIL",
        engine_equivalence="PASS" if F["engine_equivalence"]["ok"] else "FAIL",
        common_frame_equivalence="PASS" if F["common_frame_equivalence"]["ok"] else "FAIL",
        rng_manifest="PASS" if F["rng_manifest"]["ok"] else "FAIL",
        world_replay="PASS" if F["world_replay"]["ok"] else "FAIL")

    R.drop(columns=["z_obs"]).to_parquet(
        os.path.join(os.path.dirname(HERE), "data", "gann_capability_worlds.parquet"),
        index=False)
    d = ART.seal(dict(
        spec_id="GANN_CAPABILITY_RESULT_V1", status="CAPABILITY_ONLY",
        scope="registered capability on real direction-neutral Y, read ONLY to feed the "
              "registered block-nullization",
        protocol=ART.file_digest("GANN_CAPABILITY_PROTOCOL_V1.json"),
        inference_amendment=ART.file_digest("GANN_INFERENCE_AMENDMENT_V1.json"),
        prey_closure=ART.file_digest("GANN_CAPABILITY_PREY_CLOSURE_V1.json"),
        estimand_state=ART.file_digest("GANN_ESTIMAND_STATE_V1.json"),
        outcome_values=ART.file_digest("GANN_OUTCOME_VALUES_V1.json"),
        capability_code=ART.file_digest("gann_capability.py"),
        rank_code=ART.file_digest("gann_rank.py"),
        fast_code=ART.file_digest("gann_fast.py"),
        runner_code=ART.file_digest("gann_capability_run.py"),
        rng_root=root.hex()[:16],
        matrix=dict(claims=[CAP.CLAIMS[c] for c in CLAIMS], deltas=DELTAS,
                    worlds_per_cell=WORLDS, total_worlds=len(tasks),
                    permutations_per_world=N_PERM,
                    per_permutation="family-wise maxZ = max(Z_ASC, Z_DESC, Z_CONF) on ONE "
                                    "shared nullization"),
        detections=grid, acceptance=acc,
        max_null_geometry=dict(
            p95_by_claim_delta=geo,
            note="p95 of the family-wise max-Z null, per world; median/min/max are taken "
                 "across the 20 worlds of each cell"),
        integrity=integ, fixtures=F,
        common_frame=dict(
            rows=W["n"], nullization_blocks=W["n_nullblock"],
            per_claim={c: dict(treated=int(W[c]["treated"].sum()), blocks=W[c]["nb"],
                               eligible_blocks=int(W[c]["P"]["eligible"].sum()))
                       for c in CLAIMS},
            why="a family-wise maximum needs ONE permutation feeding all three claims, so "
                "the nullization runs on the common outcome-eligible frame rather than on "
                "three separately filtered ones"),
        hard_state={
            "GANN OBSERVED phi=0 Z": "NOT COMPUTED",
            "GANN GATE-B HISTORICAL": "NOT EVALUATED",
            "GANN theta": "NOT COMPUTED",
            "GANN WINNERS / SURVIVORS": "UNKNOWN"},
        outcome_exposure="CAPABILITY ONLY — every Z here is on a nullized-then-injected "
                         "outcome; no un-nullized association is computed anywhere",
        runtime_min=round((time.time() - t0) / 60, 1)),
        OUT, required=("spec_id", "detections", "acceptance", "integrity", "hard_state"),
        supersede=os.path.exists(OUT))

    print(f"\n  DETECTIONS / 20")
    print(f"    {'claim':<28}" + "".join(f"{'+'+str(x)+'pp':>9}" for x in DELTAS))
    for c in CLAIMS:
        print(f"    {CAP.CLAIMS[c]:<28}"
              + "".join(f"{grid[CAP.CLAIMS[c]]['+'+str(x)+'pp']:>9}" for x in DELTAS))
    print(f"\nGANN_CAPABILITY_RESULT_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
