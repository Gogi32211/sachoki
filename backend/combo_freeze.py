"""M2.7 — the three numbers that must exist before a SearchSpec can be frozen.

1 · k_selectable_OPPORTUNITY — EQUIVALENCE AFTER ROUTING, NOT BEFORE

The earlier count deduplicated all 4,811 grammar-v1 candidates together and reported 4,734.
But M4 admits only the OPPORTUNITY_LEVEL family; the 97 MIXED and 1 DAY claims are deferred
because no calibrated null exists for them. A multiplicity count that includes claims the
search cannot select is not this search's k.

And it cannot be recovered by filtering the finished classes: an alias group may straddle
the routing boundary — `X ∧ MACRO_VIX_UP` selecting the same rows as some pure
opportunity-level claim is not forbidden by anything — and then "which class survives"
depends on which member happened to be its representative. So equivalence is recomputed on
the routed subset from scratch.

2 · TERMINAL CONCENTRATION — MEASURED, NOT ASSERTED

I claimed 318 terminal rows across 58 strata "cannot move any θ". That was an average
argument about a quantity that only matters if it is NOT average: 0.115% globally says
nothing about whether some combination's treated arm is 7% terminal. The exclusion is a
selection, and a selection is only harmless where it is thin. So the share is computed for
every claim's treated and control arm, on the full ontology population, and published as a
distribution rather than a mean.

This costs nothing and reads no outcome: terminal status is a property of the company's
history, and the arms are membership.

3 · THE FROZEN SPEC

Everything above plus the outcome and power protocol, written once, hashed, and not edited
after results are seen.

NO OUTCOME COLUMN IS READ ANYWHERE IN THIS FILE.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens_spec as TS                                       # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2_spec as V2                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
TOKENS = os.path.join(ROOT, "data", "combo_tokens.parquet")
STATUS = os.path.join(ROOT, "data", "combo_label_status.parquet")
UNI = os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")
OUT = os.path.join(HERE, "COMBO_SEARCH_SPEC.json")

LABEL_AS_OF = "2026-08-07"


def ontology_population(verbose=True):
    """ORDINARY ∪ CORPORATE_ACTION_TERMINAL — the rows the study is ABOUT.

    RETURN is estimated on the ORDINARY part only, because the terminal part has no
    resolvable payoff. Both are carried here so the exclusion can be measured instead of
    assumed away.
    """
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", "sig_close", "family",
                                       "dup_group"])
    O = O[O["sig_close"].notna()].drop_duplicates("dup_group")
    O = O[(O["sig_close"] >= 21) & (O["sig_close"] <= 89)].reset_index(drop=True)
    O["sig_date"] = O["sig_date"].astype(str).str[:10]
    st = pd.read_parquet(STATUS)
    st = st[st["label_status"].isin(["ORDINARY_TRAIL_EXIT", "ORDINARY_MAXH_TIMER",
                                     "CORPORATE_ACTION_TERMINAL"])]
    O = O.merge(st[["ticker", "sig_date", "label_status"]], on=["ticker", "sig_date"],
                how="inner", validate="m:1")
    K = pd.read_parquet(TOKENS)
    ids = [t.token_id for t in TS.REGISTRY]
    P = O.merge(K, on=["ticker", "sig_date"], how="inner", validate="m:1")
    P["terminal"] = P["label_status"].eq("CORPORATE_ACTION_TERMINAL").to_numpy()
    if verbose:
        print(f"\n  ontology population {len(P):,} "
              f"(RETURN-observable {int((~P.terminal).sum()):,} · "
              f"terminal {int(P.terminal.sum()):,})", flush=True)
    return P, np.ascontiguousarray(P[ids].to_numpy(dtype=np.uint8)), ids


def main():
    t0 = time.time()
    uni = json.load(open(UNI))
    P_all, M_all, ids = ontology_population()
    term = P_all["terminal"].to_numpy()
    ret_rows = ~term

    # the RETURN population — the one support and equivalence are defined on
    P = P_all[ret_rows].reset_index(drop=True)
    M = np.ascontiguousarray(M_all[ret_rows])
    fam = P["family"].astype(str).to_numpy()
    dates = P["sig_date"].to_numpy()
    ph = CU.population_hash(P)
    if ph != uni["population_hash"]:
        raise RuntimeError(f"population hash {ph} != frozen {uni['population_hash']}")
    print(f"  RETURN population {len(P):,} · hash {ph} · matches the counted universe",
          flush=True)

    S = CU.Strata(M, fam, dates, verbose=False)
    J, conf = CU.measures(S)
    col = {t: i for i, t in enumerate(ids)}
    nullreq = {t.token_id: t.null_requirement for t in TS.REGISTRY}
    idx_of = [np.flatnonzero(M[:, i]) for i in range(len(ids))]

    # ── candidates, straight from the counted universe ──────────────────────
    dropped = set(uni["universe"]["depth1"]["dropped_ids"]["degenerate_prevalence"]) | \
        set(uni["universe"]["depth1"]["dropped_ids"]["support"])
    alive1 = [t.token_id for t in TS.REGISTRY if t.token_id not in dropped]
    # one representative per exact depth-1 class (L6 ≡ L46x)
    sig1: dict = {}
    for t in alive1:
        sig1.setdefault(M[:, col[t]].tobytes(), []).append(t)
    alive1 = [v[0] for v in sig1.values()]
    pairs = [tuple(p) for p in uni["surviving_depth2"]]
    cands = [(t,) for t in alive1] + pairs
    print(f"  grammar v1 candidates {len(cands):,}", flush=True)

    # ── 1 · route FIRST, then deduplicate ───────────────────────────────────
    routed: dict = {}
    for c in cands:
        routed.setdefault(TS.combine_null([nullreq[t] for t in c]), []).append(c)
    opp = routed.get(TS.OPPORTUNITY_LEVEL, [])
    print(f"\n  1 · NULL ROUTING", flush=True)
    for k in (TS.OPPORTUNITY_LEVEL, TS.MIXED_LEVEL, TS.DAY_LEVEL):
        print(f"      {k:<20} {len(routed.get(k, [])):>6,}", flush=True)

    classes: dict = {}
    for c in opp:
        m = M[:, col[c[0]]].astype(bool)
        for t in c[1:]:
            m &= M[:, col[t]].astype(bool)
        idx = np.flatnonzero(m)
        ne, _cb, ok = S.eligible_mask(idx)
        classes.setdefault(S.claim_hash(idx, ok), []).append("+".join(c))
    k_opp = len(classes)
    aliases = sorted((v for v in classes.values() if len(v) > 1), key=len, reverse=True)
    print(f"\n      syntactic OPPORTUNITY claims   {len(opp):>6,}", flush=True)
    print(f"      DISTINCT after equivalence     {k_opp:>6,}   ← M4 k", flush=True)
    print(f"      redundant spellings            {len(opp)-k_opp:>6,} in {len(aliases)} "
          f"group(s)", flush=True)

    # was the routed-then-deduped count recoverable by filtering the global one?
    print(f"      (global dedup over all {len(cands):,} gave "
          f"{uni['universe']['claim_equivalence']['distinct_claims']:,} — a different "
          f"question)", flush=True)

    # ── 2 · terminal concentration, per arm, on the full ontology ───────────
    print(f"\n  2 · TERMINAL CONCENTRATION  ({int(term.sum())} rows, "
          f"{P_all.loc[term, 'ticker'].nunique()} tickers)", flush=True)
    rows = []
    for c in opp:
        m = M_all[:, col[c[0]]].astype(bool)
        for t in c[1:]:
            m &= M_all[:, col[t]].astype(bool)
        nt, nc = int(m.sum()), int((~m).sum())
        if nt == 0:
            continue
        rows.append((("+".join(c)), nt, int((m & term).sum()) / nt,
                     int(((~m) & term).sum()) / max(nc, 1)))
    T = pd.DataFrame(rows, columns=["claim", "n_treated", "share_treated",
                                    "share_control"])
    q = T["share_treated"].quantile([0.5, 0.9, 0.99, 1.0])
    print(f"      treated-arm terminal share · median {q[0.5]:.4%} · "
          f"p90 {q[0.9]:.4%} · p99 {q[0.99]:.4%} · max {q[1.0]:.4%}", flush=True)
    print(f"      control-arm terminal share · median "
          f"{T['share_control'].median():.4%}", flush=True)
    worst = T.nlargest(6, "share_treated")
    for r in worst.itertuples():
        print(f"          {r.claim:<26} n={r.n_treated:>7,}  "
              f"treated {r.share_treated:.3%}  control {r.share_control:.3%}", flush=True)
    over = int((T["share_treated"] > 0.01).sum())
    print(f"      claims whose treated arm is >1% terminal: {over:,} of {len(T):,}",
          flush=True)

    # ── 3 · the frozen spec ────────────────────────────────────────────────
    spec = {
        "spec_id": "COMBO_MINER_V1_SEARCH_SPEC",
        "frozen_before": "any read of a historical outcome column",
        "population": {
            "ontology_rows": int(len(P_all)),
            "return_observable_rows": int(len(P)),
            "population_hash": ph,
            "setup_families": int(S.K),
            "dates": int(S.D),
            "feature_data_as_of": LABEL_AS_OF,
            "label_source_snapshot_as_of": LABEL_AS_OF,
            "maturity_rule": "60 complete MARKET bars after entry inside the frozen "
                             "snapshot; calendar only; NO exception for cohorts whose "
                             "trail happened to fire early",
            "maturity_cutoff": json.load(open(os.path.join(
                HERE, "COMBO_LABEL_CONTRACT.json")))["maturity_cutoff"],
            "maturity_cutoff_derivation": "market calendar (union of trading dates across "
                                          "non-index names), not hardcoded",
        },
        "outcome": {
            "outcome_id": "REALIZED_RETURN_TRAIL12_TIMER60_V1",
            "return_source": "ret_true (unfiltered-bar path)",
            "ordinary_exit": "ATR × 12 trailing exit",
            "fallback_exit": "close at bar 60",
            "max_hold_bars": 60,
            "empirical_exit_mix_at_qualification_snapshot": {
                "trail_exit": 11681, "timer60_exit": 263626,
                "note": "descriptive property of this snapshot, NOT part of the outcome's "
                        "identity. 91% of exits are the timer, so this is in practice a "
                        "60-bar return policy with a rarely-firing trailing stop, and the "
                        "name says so."},
            "selection_basis": "corrected implementation of the intended trade-path label",
            "selection_basis_is_not": "superior historical performance",
            "legacy": {"ret": "LEGACY_REPRODUCTION_ONLY — not selectable in v1"},
        },
        "terminal_events": {
            "class": "CORPORATE_ACTION_TERMINAL",
            "n": int(term.sum()),
            "tickers": int(P_all.loc[term, "ticker"].nunique()),
            "terminal_payoff": "UNRESOLVED",
            "payoff_data_available": False,
            "payoff_data_checked": "corporate_actions.csv is a splice detector; Massive "
                                   "/v3/reference/tickers returns a ticker reference, not "
                                   "merger consideration",
            "return_treatment": "excluded from the RETURN numerical estimand",
            "ontology_treatment": "retained as a competing event, not as censoring",
            "selection_risk": "NOT_ESTIMATED_IN_V1",
            "return_coverage": f"{len(P):,} / {len(P_all):,} = {len(P)/len(P_all):.5f}",
            "limitation": "the RETURN estimand is CONDITIONAL on a resolvable "
                          "non-terminal outcome. Terminal events are not assumed to carry "
                          "zero return or zero selection effect.",
            "concentration_audit": {
                "treated_share_median": float(q[0.5]), "treated_share_p90": float(q[0.9]),
                "treated_share_p99": float(q[0.99]), "treated_share_max": float(q[1.0]),
                "claims_above_1pct": over, "claims_audited": int(len(T))},
        },
        "search": {
            "grammar": "BaseSetup + 0..2 contextual tokens, distinct families, "
                       "base setup REQUIRED",
            "universe": "fixed exhaustive, enumerated before any outcome exists",
            "adaptive_beam": "NOT USED — the universe is enumerable, so the search is not "
                             "outcome-adaptive and the null needs no search replay",
            "null_family_admitted": "OPPORTUNITY_LEVEL only",
            "deferred": {"MIXED_LEVEL": len(routed.get(TS.MIXED_LEVEL, [])),
                         "DAY_LEVEL": len(routed.get(TS.DAY_LEVEL, [])),
                         "reason": "no calibrated null; two separately calibrated 5% bands "
                                   "do not compose into 5% over their union"},
            "syntactic_opportunity_claims": len(opp),
            "k_selectable_opportunity": k_opp,
            "k_used_for": ["search-wide max band", "DSR", "FDR", "power-test universe"],
            "registry_digest": TS.digest(),
            "eligibility": V2.ELIGIBILITY,
            "support_floor": V2.SUPPORT_FLOOR,
            "ranking_statistic": "theta_hat descending — no stability, novelty or "
                                 "composite score, whose weights would themselves be a "
                                 "fitted model",
            "null_generator": "G1 within-stratum outcome permutation · REUSED, previously "
                              "qualified for opportunity-level claims",
            "band": "Monte-Carlo max statistic over the full fixed universe from 120 "
                    "permutations — NOT an exact quantile",
            "n_perm": 120,
        },
        "power_protocol": {
            "primary": {"needle_location": "q50 eligible-support claim, chosen X-only "
                                           "before any synthetic outcome exists",
                        "delta_pp": 1.5, "worlds": 20,
                        "PASS": ">= 18 / 20", "PARTIAL": "16-17 / 20",
                        "FAIL": "<= 15 / 20"},
            "detection": "T_needle > band_p95 over the FULL opportunity universe — not "
                         "top-K, not a positive theta",
            "stress_only": {"locations": ["q10", "q90"],
                            "deltas": [0.6, 1.5, 3.0, 6.0],
                            "note": "NOT an alternative route to PASS. One primary gate."},
            "world": "composition_only_negative + planted delta (combolab_v2)",
        },
        "mc_boundary": {
            "record": ["candidate_stat", "band_draws[120]", "band_threshold",
                       "distance_to_band"],
            "decision": "from the registered band only: PASS / FAIL",
            "mc_boundary_status": "DESCRIPTIVE DIAGNOSTIC ONLY in v1 — a finite-permutation "
                                  "decision procedure is a separate registration, and "
                                  "opening it here would start a new statistical branch on "
                                  "the eve of the search",
        },
        "historical_outcome_exposure": "NONE at freeze time",
    }
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    with open(OUT, "w") as f:
        json.dump(spec, f, indent=2, default=str)

    print(f"\n{'='*82}", flush=True)
    print(f"  COMBO_MINER_V1_SEARCH_SPEC   digest {spec['spec_digest']}", flush=True)
    print(f"    population   {len(P):,} RETURN-observable of {len(P_all):,} ontology",
          flush=True)
    print(f"    hash         {ph}", flush=True)
    print(f"    outcome      REALIZED_RETURN_TRAIL12_TIMER60_V1", flush=True)
    print(f"    k (M4)       {k_opp:,}", flush=True)
    print(f"    power        δ*=1.5pp · q50 · 20 worlds · PASS ≥18/20", flush=True)
    print(f"\n  WROTE {OUT}\n  {time.time()-t0:.0f}s · NO OUTCOME READ", flush=True)
    print("=" * 82, flush=True)


if __name__ == "__main__":
    main()
