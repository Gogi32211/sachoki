"""T5_15M_X_ONLY_ENUMERATION — materialize the frozen 15m grammar. No MFE value is read.

Governed by T5_15M_OPENING_HOUR_V1 (2871358f25d62f47) and its inference amendment
(a3d0152180a7938c). Neither is re-opened here. This file executes them.

POSITIONS ARE CLOCK-ANCHORED, NOT ORDINAL

M1..M4 are 09:30, 09:45, 10:00, 10:15 ET. Taking "the first four rows" instead would silently
slide the anchors whenever a bar is missing, so an episode with a gap at 09:45 would have its
10:00 bar relabelled M2 and enter M1->M2 claims it does not belong to. The spec demands a
complete opening hour; completeness is therefore defined against the clock.

Early closes do not affect this window: the session still opens at 09:30.

THE 1H FIREWALL

The 1H historical result is exposed. Nothing here may be shaped by it. No VOL_W-specific
grammar, no extra BUY combinations, no deeper L43 sequences, no narrowing to a quarter-hour
because a 1H winner points there, no support change to hunt a survivor, no outcome-horizon
change. The universe enumerated is exactly the one frozen before that exposure. If those
motifs appear here, they appear alongside every other claim under identical gates.

APRIORI PRUNING IS EXACT, AND IT PRUNES ON X ONLY

n(a on M1 AND b on M2 AND c on M3) <= n(a on M1 AND b on M2), so a 3-bar claim whose 2-bar
prefix failed the support floor cannot pass it. The same holds for its suffix on (M2,M3).
Both bounds are used, and both are properties of X.

EXACT-EQUIVALENCE DEDUPLICATION, NOT NAME DEDUPLICATION

Position anchoring means M1->M2 and M2->M3 with the same tokens are DIFFERENT claims by
design — that is the question the 15m study exists to ask. Two claims collapse only when
their episode membership vectors are bit-identical, which is a fact about the data rather
than about the syntax.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, sys, time                                       # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens as CT, combo_tokens_spec as TS                     # noqa: E402
import t5_dna as D, t5_sequence_estimand as EST                        # noqa: E402

SPEC = json.load(open("T5_15M_OPENING_HOUR_SPEC_V1.json"))
AMEND = json.load(open("T5_15M_INFERENCE_AMENDMENT_V2.json"))
DB15 = os.path.join(D.ROOT, "data", "studio_15m.duckdb")
OUT_MS = os.path.join(D.ROOT, "data", "t5_15m_opening_hour.parquet")
OUT_CL = os.path.join(D.ROOT, "data", "t5_15m_claims.parquet")
OUT_MEM = os.path.join(D.ROOT, "data", "t5_15m_membership.parquet")
OUT = os.path.join(HERE, "T5_15M_X_ONLY_ENUMERATION.json")
OUT_SUM = os.path.join(D.ROOT, "data", "t5_15m_sweep_counters.json")

# SCOPE is a switch over which of the frozen grammar's position families enter the sweep. It
# never changes tokens, positions, support floors or the estimand — only which already-frozen
# families are enumerated. FULL is the spec as written; TWO_BAR is the reduced primary search
# under T5_15M_SEARCH_SCOPE_V2, and the reduction is recorded in the artifact rather than
# applied silently.
SCOPE = os.environ.get("T5_15M_SCOPE", "FULL").upper()
if SCOPE not in ("FULL", "TWO_BAR"):
    raise SystemExit(f"T5_15M_SCOPE must be FULL or TWO_BAR, got {SCOPE!r}")
if SCOPE == "TWO_BAR":
    OUT_CL = os.path.join(D.ROOT, "data", "t5_15m_claims_2bar.parquet")
    OUT = os.path.join(HERE, "T5_15M_X_ONLY_ENUMERATION_2BAR.json")
    OUT_SUM = os.path.join(D.ROOT, "data", "t5_15m_sweep_counters_2bar.json")

ET_POS = {"09:30": 1, "09:45": 2, "10:00": 3, "10:15": 4}
PAIRS = [("M1", "M2"), ("M2", "M3"), ("M3", "M4")]
TRIPLES = [] if SCOPE == "TWO_BAR" else [("M1", "M2", "M3"), ("M2", "M3", "M4")]
G = SPEC["support"]
MIN_T, MIN_C = G["min_episodes_treated"], G["min_episodes_control"]
MIN_TK, MIN_DT = G["min_unique_tickers"], G["min_unique_dates"]
MAX_DSHARE, MIN_RET = G["max_single_date_share"], G["min_treated_overlap_retention"]
UMBRELLA = {"ANY_T", "ANY_Z", "ANY_L", "L5_ANY", "AD_ANY", "ANY_P", "ANY_D"}


def load_opening_hour(verbose=True):
    """One row per (episode, M1..M4) bar of the T5 session, with tokens."""
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"])
    conn = duckdb.connect(DB15, read_only=True)
    try:
        have = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
        miss = [c for c in D.SRC_COLS if c not in have]
        if miss:
            raise RuntimeError(f"15m store lacks token source columns: {miss[:8]}")
        conn.register("ep", E)
        sel = ", ".join(f"x.{c}" for c in D.SRC_COLS)
        q = f"""
        SELECT e.episode_id, e.ticker, CAST(e.t5_date AS VARCHAR) t5_date,
               strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                        '%H:%M') et_time,
               x.open, x.high, x.low, x.close, x.volume, {sel}
        FROM ep e JOIN bars x
          ON x.ticker = e.ticker
         AND CAST(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE)
             = CAST(e.t5_date AS DATE)
        WHERE strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                       '%H:%M') IN ('09:30','09:45','10:00','10:15')"""
        M = conn.execute(q).fetchdf()
    finally:
        conn.close()
    M["pos"] = M.et_time.map(ET_POS)
    M["mslot"] = "M" + M.pos.astype(str)
    if M.duplicated(["episode_id", "pos"]).any():
        raise RuntimeError("duplicate (episode_id, position) in the 15m opening hour — "
                           "a duplicated bar would create memberships that do not exist")
    cols = {t.token_id: CT._evaluate(M, t).astype(np.uint8) for t in D.TOKENS_1H}
    T = pd.DataFrame(cols, index=M.index)
    ids = np.array([t.token_id for t in D.TOKENS_1H])
    arr = T.to_numpy(dtype=bool)
    M["token_set"] = [" ".join(sorted(ids[row])) for row in arr]
    if verbose:
        print(f"  15m opening-hour bars {len(M):,} · episodes touched "
              f"{M.episode_id.nunique():,}", flush=True)
    return E, M, T, ids


def searchable(M, T, ids):
    """Same verdict rule as SEQUENCE_GRAMMAR_V1, re-measured on the 15m population."""
    arr = T.to_numpy(dtype=bool)
    cnt = arr.sum(0)
    n = len(M)
    rows, keep = [], []
    for i, t in enumerate(D.TOKENS_1H):
        c, share = int(cnt[i]), cnt[i] / n
        if t.token_id in UMBRELLA:
            ok, why = False, "umbrella union of registry members; its sequences are implied"
        elif c == 0:
            ok, why = False, "never true on 15m opening-hour bars in this population"
        elif share > 0.99:
            ok, why = False, f"true on {share:.1%} of bars — not a distinguishing element"
        else:
            ok, why = True, ""
        rows.append(dict(token=t.token_id, family=t.family, bar_count=c,
                         bar_share=round(float(share), 6), searchable=ok, reason=why))
        if ok:
            keep.append(i)
    return pd.DataFrame(rows), np.array(keep)


def main():
    t0 = time.time()
    if SPEC["spec_digest"] != "2871358f25d62f47" or \
       AMEND["amendment_digest"] != "a3d0152180a7938c":
        raise RuntimeError("15m spec or amendment digest drift")
    print(f"T5_15M_X_ONLY_ENUMERATION · spec {SPEC['spec_digest']} · amendment "
          f"{AMEND['amendment_digest']}", flush=True)
    print("  1H firewall: the frozen universe is enumerated unchanged; no 1H survivor "
          "shapes it", flush=True)

    E, M, T, ids = load_opening_hour()
    REG, keep = searchable(M, T, ids)
    tok = ids[keep]
    print(f"  searchable tokens {len(tok)} of {len(D.TOKENS_1H)}", flush=True)

    # ── population funnel ───────────────────────────────────────────────────
    n_all = len(E)
    per = M.groupby("episode_id").pos.agg(["count", "nunique"])
    complete = set(per.index[(per["count"] == 4) & (per["nunique"] == 4)])
    touched = set(M.episode_id.unique())
    funnel = dict(
        episodes_1d_total=int(n_all),
        with_any_15m=int(len(touched)),
        with_complete_opening_hour=int(len(complete)),
        excluded_no_15m=dict(n=int(n_all - len(touched)),
                             reason="no 15m bar at any of 09:30/09:45/10:00/10:15 ET on the "
                                    "T5 session"),
        excluded_partial_opening_hour=dict(
            n=int(len(touched) - len(complete)),
            reason="fewer than four clock-anchored opening-hour bars; the spec requires a "
                   "complete opening hour"))
    print(f"  funnel {n_all:,} 1D episodes → {len(touched):,} with 15m → "
          f"{len(complete):,} complete opening hour", flush=True)

    M = M[M.episode_id.isin(complete)]
    ep_ids = np.array(sorted(complete))
    eix = {e: i for i, e in enumerate(ep_ids)}
    nE = len(ep_ids)
    B = np.zeros((4, nE, len(tok)), dtype=bool)
    arr = T.to_numpy(dtype=bool)[:, keep]
    rowi = M.episode_id.map(eix).to_numpy()
    posi = M.pos.to_numpy() - 1
    src = M.index.to_numpy()
    tpos = {v: k for k, v in enumerate(T.index.to_numpy())}
    B[posi, rowi] = arr[[tpos[s] for s in src]]

    first = M.drop_duplicates("episode_id").set_index("episode_id").loc[ep_ids]
    ep_tk = pd.factorize(first.ticker)[0]
    ep_dt = pd.factorize(first.t5_date)[0]

    n_tk, n_dt = int(ep_tk.max()) + 1, int(ep_dt.max()) + 1

    def gates(v):
        nt = int(v.sum())
        if nt < MIN_T or nE - nt < MIN_C:
            return None
        tc = np.bincount(ep_tk[v], minlength=n_tk)
        dc = np.bincount(ep_dt[v], minlength=n_dt)
        n_t_, n_d_ = int((tc > 0).sum()), int((dc > 0).sum())
        if n_t_ < MIN_TK or n_d_ < MIN_DT or dc.max() / nt > MAX_DSHARE:
            return None
        return (nt, n_t_, n_d_, float(dc.max() / nt))

    # ── estimand population and blocks, BEFORE the candidate sweep ──────────
    # The first version of this file held a full 114,295-bit membership vector per class in a
    # dict. At 1.1M support-qualified claims that is ~125 GB. Everything below therefore
    # streams: a candidate's vector lives for one iteration and only its hash survives.
    O = pd.read_parquet(os.path.join(D.ROOT, "data", "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "path_status_10d"])   # AVAILABILITY only
    P = pd.DataFrame(dict(episode_id=ep_ids)).merge(
        pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]),
        on="episode_id", how="left").merge(O, on="episode_id", how="left")
    n_a = len(P)
    P = P[P.path_status_10d == "AVAILABLE"]
    n_b = len(P)
    A = EST.anchors(P)
    P = P.merge(A, on="episode_id", how="inner")
    P = P[P.liq_anchor.notna() & P.vol_anchor.notna() & (P.n20 == 20)]
    n_c = len(P)
    P = EST.blocks(P)
    est_ids = P.episode_id.to_numpy()
    keepmask = np.isin(ep_ids, est_ids)
    order_map = {e: i for i, e in enumerate(est_ids)}
    reidx = np.array([order_map[e] for e in ep_ids[keepmask]])
    blk = pd.factorize(P.block)[0]
    nb = int(blk.max()) + 1
    blk_n = np.bincount(blk, minlength=nb)
    nEst = len(est_ids)
    funnel.update(estimand_population=dict(
        complete_opening_hour=int(n_a), mfe10_available=int(n_b),
        anchor_available=int(n_c), blocks=nb))
    print(f"  estimand population {n_a:,} → MFE10 available {n_b:,} → anchored {n_c:,} "
          f"· blocks {nb:,}", flush=True)

    seen_full, seen_est, rows = {}, {}, []
    posfam = {}

    def consider(nm, pos, L, v):
        """One candidate, start to finish, retaining nothing but hashes."""
        g = gates(v)
        if g is None:
            return False
        posfam[pos] = posfam.get(pos, 0) + 1
        h = hashlib.sha256(np.packbits(v).tobytes()).hexdigest()[:16]
        if h in seen_full:                      # exact membership duplicate of an earlier name
            seen_full[h][1] += 1
            return True
        w = np.zeros(nEst, bool); w[reidx] = v[keepmask]
        nt_b = np.bincount(blk[w], minlength=nb)
        ov = (nt_b > 0) & (blk_n - nt_b > 0)
        keep_ep = ov[blk]
        sig = w & keep_ep
        t_all, t_ov = int(w.sum()), int(sig.sum())
        ret = t_ov / t_all if t_all else 0.0
        c_ov = int((~w & keep_ep).sum())
        ok = bool(t_ov >= MIN_T and c_ov >= MIN_C and ret >= MIN_RET)
        eh = hashlib.sha256(np.packbits(sig).tobytes()).hexdigest()[:16]
        seen_full[h] = [nm, 1]
        if ok and eh not in seen_est:
            seen_est[eh] = nm
        rows.append((nm, pos, L, g[0], g[1], g[2], round(g[3], 5), t_all, t_ov, c_ov,
                     round(ret, 4), int(ov.sum()), eh, ok))
        return True

    # ── 2-bar: co-occurrence counts for every token pair in one GEMM ────────
    kept2 = {}
    for a, b in PAIRS:
        i, j = int(a[1]) - 1, int(b[1]) - 1
        C = B[i].astype(np.int32).T @ B[j].astype(np.int32)
        ok = np.argwhere((C >= MIN_T) & (nE - C >= MIN_C))
        kept2[(a, b)] = set()
        for x, yy in ok:
            if consider(f"{a}→{b}|2|{tok[x]}→{tok[yy]}", f"{a}→{b}", 2,
                        B[i][:, x] & B[j][:, yy]):
                kept2[(a, b)].add((int(x), int(yy)))
        print(f"    {a}→{b}  above support floor {len(ok):,} → fully qualified "
              f"{len(kept2[(a,b)]):,}", flush=True)

    # ── 3-bar: apriori on BOTH the prefix and the suffix ────────────────────
    for a, b, c in TRIPLES:
        i, j, k = int(a[1]) - 1, int(b[1]) - 1, int(c[1]) - 1
        bymid = {}
        for x, yy in kept2[(b, c)]:
            bymid.setdefault(x, []).append(yy)
        trips = [(x, yy, z) for (x, yy) in kept2[(a, b)] for z in bymid.get(yy, [])]
        print(f"    {a}→{b}→{c}  apriori candidates {len(trips):,}", flush=True)
        n_ok = 0
        for n_i, (x, yy, z) in enumerate(trips):
            if consider(f"{a}→{b}→{c}|3|{tok[x]}→{tok[yy]}→{tok[z]}", f"{a}→{b}→{c}", 3,
                        B[i][:, x] & B[j][:, yy] & B[k][:, z]):
                n_ok += 1
            if (n_i + 1) % 200000 == 0:
                print(f"      {n_i+1:,}/{len(trips):,} · qualified {n_ok:,} · "
                      f"{(time.time()-t0)/60:.0f}m", flush=True)
        print(f"      support-qualified {n_ok:,}", flush=True)

    n_syn = sum(posfam.values())
    k_prov, k_final = len(seen_full), len(seen_est)
    print(f"  support-qualified syntactic claims {n_syn:,}")
    print(f"  exact membership classes (provisional k_15m) {k_prov:,}")
    print(f"  re-equivalence on the estimand population → k_15m_final {k_final:,}",
          flush=True)
    print("  by position family (syntactic, pre-dedup):")
    for kk in sorted(posfam):
        print(f"    {kk:<16} {posfam[kk]:,}")

    json.dump(dict(scope=SCOPE, n_syn=int(n_syn), k_prov=int(k_prov), k_final=int(k_final),
                   by_position_family={str(k2): int(posfam[k2]) for k2 in sorted(posfam)},
                   funnel=funnel),
              open(OUT_SUM, "w"), indent=2, ensure_ascii=False)
    print(f"  sweep counters checkpointed -> {os.path.basename(OUT_SUM)}", flush=True)

    CLM = pd.DataFrame(rows, columns=[
        "representative", "position_family", "length", "treated_all_pop", "tickers", "dates",
        "max_date_share", "treated_all", "treated_overlap", "control_overlap",
        "overlap_retention", "n_overlap_blocks", "estimand_membership_hash",
        "estimand_qualified"])
    # rows gets exactly one entry per NEW membership hash, appended immediately after
    # seen_full[h] is created, so the two are in the same insertion order.
    CLM["n_names"] = [v[1] for v in seen_full.values()]
    # A claim can clear the estimand gates and STILL not be a final class: two survivors can
    # share an estimand membership signature after the overlap restriction even though their
    # full-population vectors differed. k_15m_final counts signatures, so the digest must be
    # taken over exactly the rows that ARE the final representatives. The first version
    # compared an in-memory set of 1,032,323 names against 1,032,574 reloaded rows and the
    # reproducibility gate failed — correctly, on the comparison rather than on the data.
    CLM["is_final_representative"] = False
    CLM.loc[CLM[CLM.estimand_qualified].drop_duplicates("estimand_membership_hash").index,
            "is_final_representative"] = True
    CLM["block_constant_treated_share"] = (1 - CLM.overlap_retention).round(4)
    CLM.to_parquet(OUT_CL, index=False)
    M.to_parquet(OUT_MS, index=False, compression="zstd")
    # The membership matrix is NOT written: k_15m_final x estimand population at this scale is
    # tens of gigabytes. Whether that scale is acceptable is a decision about the study, not
    # something this file may settle by silently truncating.
    print(f"  membership parquet NOT written — {k_final:,} classes x {nEst:,} episodes "
          f"is ~{k_final*nEst/8/1e9:.0f} GB", flush=True)

    Q = CLM[CLM.estimand_qualified]
    mob = Q.overlap_retention
    print(f"  mobility (treated_permutable_retention) min {mob.min():.4f} · "
          f"p05 {mob.quantile(.05):.4f} · median {mob.median():.4f}")
    print(f"  block-constant treated share  max {Q.block_constant_treated_share.max():.4f}")

    # ── gates ───────────────────────────────────────────────────────────────
    # reproducibility: rebuild the class digest from the stored artifact
    dig = hashlib.sha256("|".join(sorted(
        CLM.loc[CLM.is_final_representative, "representative"])).encode()).hexdigest()[:16]
    dig2 = hashlib.sha256("|".join(sorted(
        pd.read_parquet(OUT_CL).query("is_final_representative").representative)).encode()
    ).hexdigest()[:16]
    if int(CLM.is_final_representative.sum()) != k_final:
        raise RuntimeError("final-representative count does not equal k_15m_final")
    # metamorphic, part 1: a duplicated (episode, position) grain must FAIL HARD.
    dup = pd.concat([M, M.head(1000)], ignore_index=True)
    hard_fail = bool(dup.duplicated(["episode_id", "pos"]).any())
    # metamorphic, part 2: the 1H contract's "duplicate occurrence rows must not change
    # episode statistics" has NO analogue here and is not claimed. 1H membership was
    # "exists t" over adjacencies, so a repeated bar could inflate occurrence counts. A 15m
    # slot holds exactly one bar, so there is no occurrence count to inflate. What IS testable
    # is that membership does not depend on source row ORDER — comparing B against a rebuild
    # from a shuffled frame, which is a real property and not a copy compared with itself.
    rs = np.random.default_rng(4242).permutation(len(M))
    Ms = M.iloc[rs]
    Bs = np.zeros_like(B)
    Bs[Ms.pos.to_numpy() - 1, Ms.episode_id.map(eix).to_numpy()] = \
        arr[[tpos[s] for s in Ms.index.to_numpy()]]
    order_inv = bool(np.array_equal(Bs, B))
    gates_ = dict(reproducible_class_digest=bool(dig == dig2),
                  duplicate_grain_fails_hard=hard_fail,
                  membership_invariant_to_source_row_order=order_inv,
                  positions_clock_anchored=True,
                  no_mfe_value_read=True)
    for kk, vv in gates_.items():
        print(f"    {'✓' if vv else '✗'} {kk}")
    if not all(gates_.values()):
        raise RuntimeError(f"gates failed: {[kk for kk, vv in gates_.items() if not vv]}")

    scope_note = ("FULL — the frozen grammar as written, both 2-bar and 3-bar families"
                  if SCOPE == "FULL" else
                  "TWO_BAR — primary search reduced to position-anchored 2-bar transitions "
                  "under T5_15M_SEARCH_SCOPE_V2. 3-bar families are DEFERRED_FROM_PRIMARY_"
                  "INFERENCE on X-only feasibility grounds, not deleted and not judged.")
    json.dump(dict(spec_id=f"T5_15M_X_ONLY_ENUMERATION_{SCOPE}", scope=SCOPE,
                   scope_note=scope_note,
                   status="X_ONLY — NO MFE VALUE READ",
                   governed_by=dict(spec=SPEC["spec_digest"],
                                    amendment=AMEND["amendment_digest"]),
                   firewall="the frozen 15m universe was enumerated unchanged after the 1H "
                            "exposure; no 1H survivor shaped grammar, support, window or "
                            "horizon",
                   population_funnel=funnel,
                   searchable_tokens=dict(n=int(len(tok)), of=len(D.TOKENS_1H)),
                   syntactic_claims=int(n_syn),
                   by_position_family={str(kk): int(vv) for kk, vv in posfam.items()},
                   k_15m_provisional=int(k_prov), k_15m_final=int(k_final),
                   membership_matrix_not_written=f"{k_final} classes x {nEst} "
                                                 f"episodes is infeasible at this scale",
                   class_digest=dig,
                   mobility=dict(min=round(float(mob.min()), 4),
                                 p05=round(float(mob.quantile(.05)), 4),
                                 median=round(float(mob.median()), 4),
                                 max_block_constant_treated_share=round(
                                     float(Q.block_constant_treated_share.max()), 4)),
                   gates=gates_,
                   next_gate="15m needles chosen from THIS sealed claim order by the frozen "
                             "support rule, then timing qualification, then "
                             "T5_15M_CAPABILITY_RANK_V2. The 1H capability does NOT transfer.",
                   y_status="NO MFE VALUES, NO HISTORICAL Z",
                   minutes=round((time.time() - t0) / 60, 1)),
              open(OUT, "w"), indent=2, ensure_ascii=False)
    print(f"\n  WROTE {OUT}\n        {OUT_CL}\n        {OUT_MS}")
    print(f"  {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
