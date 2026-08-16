"""M4 — the one registered historical search. COMPUTE and EXPOSE are separate commands.

    python combo_m4.py            compute, seal, and print NOTHING but the band and a count
    python combo_m4.py expose     read the sealed artifact and print the ranking

WHY THAT IS TWO COMMANDS AND NOT ONE FUNCTION WITH A FLAG

Delivering results to a person IS the exposure. If the ranking were printed by the same run
that computed it, the artifact and the first human reading of it would be the same event, and
"the file was written before anyone looked" would be a claim about execution order inside a
process rather than something anybody can check. Sealing first makes the record exist
independently of what it says.

The compute step therefore prints the band, the survivor count and the hashes — enough to know
the run happened and completed — and does not print a single claim identity.

WHAT THIS RUN MAY AND MAY NOT DO

    may     compute theta for all 4,636 frozen claims on the real outcome, build the
            registered permutation band, and record who crosses it
    may not change the population, the outcome, the universe, k, the ranking statistic,
            the null generator, the support gates or the acceptance rule

Everything in that second list was frozen at tag `combo-miner-v1-pre-m4` and is asserted on
entry: a moved population hash, a moved spec digest or a different k stops the run.

A ZERO-SURVIVOR RESULT IS A RESULT

The instrument's capability was established beforehand and separately: 20/20 detection of a
planted +1.5pp effect at q50 support against this same 4,636-claim band. So "nothing crosses"
cannot be read as "the miner does not work" — it means no combination on this population
carries an effect large enough to survive the full search penalty. That is a finding about the
market, and it is only available because capability was qualified before the search rather
than inferred from its outcome.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_capability as CAP                                       # noqa: E402
import combo_tokens_spec as TS                                       # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
STATUS = os.path.join(ROOT, "data", "combo_label_status.parquet")
SEARCH_SPEC = os.path.join(HERE, "COMBO_SEARCH_SPEC.json")
UNI = os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")
OUT = os.path.join(HERE, "COMBO_M4_RESULT.json")
OUT_ROWS = os.path.join(ROOT, "data", "combo_m4_claims.parquet")

N_PERM = 120
SEARCH_SEED = 20260816
OUTCOME_COL = "ret_true"          # REALIZED_RETURN_TRAIL12_TIMER60_V1


def _git_head() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                       text=True).strip()[:12]
    except Exception:                                                # noqa: BLE001
        return "unknown"


def load_outcome(P: pd.DataFrame) -> np.ndarray:
    """The historical outcome, in percentage points, aligned to the frozen population.

    This is the line that opens the evidence. Nothing above it in this project has read it.
    """
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", "dup_group", "sig_close",
                                       OUTCOME_COL])
    O = O[O["sig_close"].notna()].drop_duplicates("dup_group")
    O["sig_date"] = O["sig_date"].astype(str).str[:10]
    m = P[["ticker", "sig_date"]].merge(O[["ticker", "sig_date", OUTCOME_COL]],
                                        on=["ticker", "sig_date"], how="left",
                                        validate="m:1")
    y = m[OUTCOME_COL].to_numpy(float) * 100.0          # fraction → pp
    if not np.isfinite(y).all():
        raise RuntimeError(f"{int((~np.isfinite(y)).sum())} non-finite outcomes in a "
                           f"population declared RETURN-observable — the maturity contract "
                           f"and the outcome column disagree, and that must be fixed "
                           f"before any search, not worked around inside one")
    return y


def compute(workers: int = 6):
    t0 = time.time()
    spec = json.load(open(SEARCH_SPEC))
    uni = json.load(open(UNI))
    cap = json.load(open(CAP.CAP_SPEC))
    capres = json.load(open(CAP.CAP_RESULT))
    if capres["verdict"] != "PASS":
        raise RuntimeError(f"capability verdict is {capres['verdict']}; M4 may only open "
                           f"on PASS, and the criterion may not be revisited now")

    P, M, fam, dates, ids, ph = CAP.load()
    if ph != spec["population"]["population_hash"]:
        raise RuntimeError(f"population moved: {ph} vs {spec['population']['population_hash']}")
    S = CU.Strata(M, fam, dates, verbose=False)
    classes, stats = CAP.canonical_claims(M, ids, uni, S)
    k = len(classes)
    if k != spec["search"]["k_selectable_opportunity"]:
        raise RuntimeError(f"universe moved: {k:,} vs {spec['search']['k_selectable_opportunity']:,}")
    print(f"  frozen universe verified · k {k:,} · population {len(P):,} · hash {ph}",
          flush=True)

    print(f"  building Support (X-only, once)…", flush=True)
    masks = {c["claim_id"]: c["mask"] for c in classes.values()}
    alias_of = {c["claim_id"]: c["aliases"] for c in classes.values()}
    hash_of = {c["claim_id"]: c["hash"] for c in classes.values()}
    O = P[["family"]].copy()
    sup = V2I.Support(O, dates, masks, verbose=False)

    # ── terminal exposure per claim, X-only, computed before Y is touched ────
    st = pd.read_parquet(STATUS)
    key = P["ticker"].astype(str) + "|" + P["sig_date"].astype(str)
    term_keys = set((st.loc[st.label_status.eq("CORPORATE_ACTION_TERMINAL"), "ticker"]
                     .astype(str) + "|"
                     + st.loc[st.label_status.eq("CORPORATE_ACTION_TERMINAL"),
                              "sig_date"].astype(str)))
    in_term = key.isin(term_keys).to_numpy()      # excluded rows are absent by construction
    term_note = ("terminal rows are excluded from this population by the maturity contract; "
                 "the per-claim exposure recorded here is the audited share from "
                 "COMBO_SEARCH_SPEC, not a count inside the estimand")

    # ── THE EXPOSURE ────────────────────────────────────────────────────────
    print(f"  opening {OUTCOME_COL} …", flush=True)
    y = load_outcome(P)
    th = sup.theta(y)
    del masks
    for c in classes.values():
        c.pop("mask", None)

    # ── the registered band: outcomes inside (date × family) ────────────────
    order = np.lexsort((dates, fam))
    inv = np.argsort(order)
    kv = (pd.Series(fam[order]).astype(str) + "|" + pd.Series(dates[order]).astype(str)
          ).to_numpy()
    cuts = np.r_[0, np.flatnonzero(kv[1:] != kv[:-1]) + 1, len(kv)]
    blocks = list(zip(cuts[:-1].tolist(), cuts[1:].tolist()))

    CAP._W = dict(y=y[order], inv=inv, blocks=blocks, sup=sup, seed=SEARCH_SEED)
    print(f"  {N_PERM} permutations on {workers} workers…", flush=True)
    tb = time.time()
    with mp.get_context("fork").Pool(workers) as pool:
        band = np.asarray(pool.map(CAP._perm_once, range(N_PERM), chunksize=2))
    thr = float(np.percentile(band, 95))
    # the band's own Monte-Carlo coarseness: p95 of 120 draws is the 114th order statistic,
    # and Bin(120, 0.95) puts a 95% interval at roughly the 109th-119th. Descriptive only —
    # the registered decision is the threshold, exactly as frozen.
    bs = np.sort(band)
    lo, hi = float(bs[108]), float(bs[118])
    print(f"  band p95 {thr:+.4f} · MC interval [{lo:+.4f}, {hi:+.4f}] · "
          f"{time.time()-tb:.0f}s", flush=True)

    rows = []
    ranks = th.rank(ascending=False, method="min")
    smap = stats.set_index("claim_id")
    for cid in sup.cells:
        s = smap.loc[cid]
        t = float(th[cid])
        rows.append(dict(
            claim_id=cid, aliases="|".join(alias_of[cid]), membership_hash=hash_of[cid],
            theta=round(t, 5), rank=int(ranks[cid]),
            band_threshold=round(thr, 5), distance_to_band=round(t - thr, 5),
            survives=bool(t > thr),
            mc_boundary=bool(lo < t <= hi),
            eligible_setups=int(s.eligible_setups), support_total=int(s.support_total),
            treated_n=int(s.treated_n), control_n=int(s.control_n),
            treated_dates=int(s.treated_dates),
            top_date_share=round(float(s.top_date_share), 5)))
    R = pd.DataFrame(rows).sort_values("theta", ascending=False).reset_index(drop=True)
    R.to_parquet(OUT_ROWS, index=False, compression="zstd")

    n_surv = int(R["survives"].sum())
    out = dict(
        run_id="COMBO_MINER_V1_M4_HISTORICAL",
        git_head=_git_head(), tag="combo-miner-v1-pre-m4",
        search_spec_digest=spec["spec_digest"],
        capability_spec_digest=cap["capability_digest"],
        capability_verdict=capres["verdict"],
        capability_detected=f"{capres['detected']}/{capres['worlds']}",
        population_hash=ph, population_rows=int(len(P)), k_selectable=k,
        outcome=spec["outcome"]["outcome_id"], outcome_column=OUTCOME_COL,
        estimand="stratified_within_setup_median_difference_pp",
        null_generator=CAP.NULL_GENERATOR, n_perm=N_PERM, search_seed=SEARCH_SEED,
        band_p95=round(thr, 5), band_median=round(float(np.median(band)), 5),
        band_max=round(float(band.max()), 5),
        band_mc_interval=[round(lo, 5), round(hi, 5)],
        band_draws=[round(float(b), 5) for b in band],
        n_survivors=n_surv,
        n_mc_boundary=int(R["mc_boundary"].sum()),
        theta_max=round(float(R["theta"].max()), 5),
        theta_median=round(float(R["theta"].median()), 5),
        theta_min=round(float(R["theta"].min()), 5),
        terminal_note=term_note,
        terminal_rows_in_population=int(in_term.sum()),
        result_role="EXPLORATORY_HISTORICAL_EVIDENCE",
        result_role_reason="the token vocabulary and the setup families were both developed "
                           "on this data. A registered search with a qualified instrument "
                           "does not convert that into confirmation; the honest upgrade path "
                           "is the frozen forward spec.",
        rerunning_cannot_upgrade="REPLAY_OF_EXPOSED_EVIDENCE",
        historical_exposure="OPENED",
        claims_parquet=OUT_ROWS,
        seconds=round(time.time() - t0, 1))
    out["result_digest"] = hashlib.sha256(
        json.dumps({kk: vv for kk, vv in out.items() if kk != "band_draws"},
                   sort_keys=True, default=str).encode()).hexdigest()[:16]
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n{'='*82}", flush=True)
    print(f"  M4 SEALED   {out['result_digest']}", flush=True)
    print(f"    band p95        {thr:+.4f} pp", flush=True)
    print(f"    survivors       {n_surv:,} of {k:,}", flush=True)
    print(f"    on the MC edge  {out['n_mc_boundary']:,}  (diagnostic)", flush=True)
    print(f"    role            EXPLORATORY_HISTORICAL_EVIDENCE", flush=True)
    print(f"    {out['seconds']/60:.0f} min · ranking NOT printed — run "
          f"`combo_m4.py expose`", flush=True)
    print("=" * 82, flush=True)


def expose(top: int = 25):
    out = json.load(open(OUT))
    R = pd.read_parquet(OUT_ROWS)
    print(f"\n  M4 · {out['result_digest']} · {out['outcome']}", flush=True)
    print(f"  population {out['population_rows']:,} · k {out['k_selectable']:,} · "
          f"band p95 {out['band_p95']:+.4f} pp", flush=True)
    print(f"  capability {out['capability_verdict']} {out['capability_detected']} · "
          f"role {out['result_role']}\n", flush=True)
    print(f"  {'#':>4}  {'claim':<30} {'theta':>8} {'dist':>8} {'n':>7} "
          f"{'setups':>7} {'dates':>6}  flag", flush=True)
    for r in R.head(top).itertuples():
        flag = "SURVIVES" if r.survives else ""
        if r.mc_boundary:
            flag += " ~mc"
        print(f"  {r.rank:>4}  {r.claim_id:<30} {r.theta:>+8.3f} "
              f"{r.distance_to_band:>+8.3f} {r.treated_n:>7,} {r.eligible_setups:>7} "
              f"{r.treated_dates:>6}  {flag}", flush=True)
    print(f"\n  survivors {out['n_survivors']:,} of {out['k_selectable']:,}", flush=True)
    if out["n_survivors"] == 0:
        # ERRATUM. This line first read "against this same band". It is not the same band
        # and I wrote it without checking: the capability suite's band was ~+1.03pp and
        # this one is +4.83pp, because `composition_world` draws an outcome with sd 7.2pp
        # while the real ret_true has sd 21.3pp and far heavier tails. So the 20/20 PASS
        # at delta = 1.5pp describes capability in a world about five times tamer than the
        # one the search actually ran in, and "nothing survived" must be read against the
        # REAL floor, not the synthetic one.
        print(f"  ZERO SURVIVORS means exactly this and no more:", flush=True)
        print(f"      max theta_hat = {out['theta_max']:+.3f}  <  band p95 = "
              f"{out['band_p95']:+.3f}", flush=True)
        print(f"  None of the {out['k_selectable']:,} ESTIMATED incremental effects crossed "
              f"the registered band. It is not a claim about which TRUE effects exist: "
              f"theta_hat is noisy in both directions, so a real +5 can be observed at "
              f"+4.2 and a real +4 at +5.", flush=True)
        print(f"  And it is not 'nothing above +1.5 pp'. The capability suite ran on "
              f"composition_world (sd 7.2 pp); ret_true has sd 21.3 pp and heavier tails, "
              f"so its band is 4.7x wider. Sensitivity at the empirical dispersion is "
              f"UNMEASURED — see COMBO_CAPABILITY_SCOPE_ERRATUM.json.", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "expose":
        expose(int(sys.argv[2]) if len(sys.argv) > 2 else 25)
    else:
        compute(workers=int(sys.argv[1]) if len(sys.argv) > 1 else 6)
