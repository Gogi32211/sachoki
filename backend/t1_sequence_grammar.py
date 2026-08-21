"""T1 1H sequence grammar — X-only enumeration, frozen before any T1 Y access.

Reuses the T5 grammar machinery with T1 literals. Differences from t5_sequence_grammar are
exactly: input tables (t1_*), day label (T1_DAY), artifact names, and the reproducibility
gate — T5's hardcoded (863, ccdeb2b7a03e81d0) is replaced by gate E's two-run digest equality,
because T9's constants do not exist until this run creates them. After the first sealed run
the digest becomes T9's own frozen expectation.

    families    T1_INTRADAY · PREV_INTRADAY · CROSS_DAY (exactly one seam)
    lengths     2, 3 — no 4-bar primary search
    adjacency   STRICT_ADJACENT
    element     atomic canonical token presence on one 1H bar
    unit        episode-level binary
    support     treated >= 300 · tickers >= 50 · dates >= 50 · max date share <= 10%
                (control floors apply at the estimand stage, as in T5)
    apriori     2-bar survivors seed the 3-bar candidates (anti-monotone in support)

METAMORPHIC GATES (§6) — run here, not promised for later

    A  duplicate (episode_id, relative_day, session_position) grain -> hard fail
    B  replicating occurrence rows x2/x5/x10 leaves episode-level membership unchanged
       (sampled subpopulation; STRICT adjacency makes bar duplication a real threat, so this
       is measured rather than argued)
    C  source-row reorder does not change memberships (the second full run shuffles input)
    D  session positions are clock/calendar anchored (row_number over ts within session,
       validity = modal calendar length — inherited from the shared builder)
    E  two deterministic runs reproduce the same membership-class digest
    F  no outcome/MFE field is read — the loaded column list is the assertion
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens_spec as TS                                         # noqa: E402
import t5_sequence_grammar as G5                                       # noqa: E402
import t1_dna as D9                                                    # noqa: E402

SPEC = os.path.join(HERE, "T1_SEQUENCE_GRAMMAR_V1.json")
OUT = os.path.join(HERE, "T1_SEQUENCE_UNIVERSE_V1.json")
XFREEZE = os.path.join(HERE, "T1_DNA_X_V1.json")
SURV = os.path.join(D9.ROOT, "data", "t1_sequence_survivors_v1.parquet")
TOKS = os.path.join(D9.ROOT, "data", "t1_sequence_tokens_v1.parquet")

MIN_EP, MIN_TK, MIN_DT, MAX_DATE_SHARE = G5.MIN_EP, G5.MIN_TK, G5.MIN_DT, G5.MAX_DATE_SHARE
LENGTHS = G5.LENGTHS
FAMILIES = ("T1_INTRADAY", "PREV_INTRADAY", "CROSS_DAY")
LOAD_COLS = ["episode_id", "ticker", "t1_date", "relative_day", "session_date",
             "session_position", "bars_in_session", "token_set"]     # gate F: X only


def freeze_x():
    """T1_DNA_X_V1 — mirror of the T5 X-layer freeze, from t1 tables."""
    E = pd.read_parquet(D9.OUT_EP)
    has_t1 = E.n_t1_day_1h_bars.fillna(0) > 0
    has_pv = E.n_prev_day_1h_bars.fillna(0) > 0
    sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
    fz = dict(
        spec_id="T1_DNA_X_V1", family_id="T1_MICROSTRUCTURE_DNA_V1",
        setup="FINAL_PRIORITY_RESOLVED_T1",
        t1_definition_hash=D9.T1_DEF_HASH, t1_definition=D9.T1_DEFINITION,
        token_registry_hash=TS.digest(), registry_total=len(TS.REGISTRY),
        bars_applicable_tokens=len(D9.TOKENS_1H),
        frame_computed_excluded=len(TS.REGISTRY) - len(D9.TOKENS_1H),
        full_T1_population=int(len(E)),
        T1_day_1H_observable=int(has_t1.sum()),
        cross_day_observable=int((has_t1 & has_pv).sum()),
        coverage_role="OBSERVED_1H_SUBPOPULATION",
        future_outcomes="FORBIDDEN",
        artifacts={os.path.basename(p): sha(p) for p in (D9.OUT_EP, D9.OUT_MS, D9.AUDIT)})
    fz["freeze_digest"] = hashlib.sha256(
        json.dumps(fz, sort_keys=True, default=str).encode()).hexdigest()[:16]
    if os.path.exists(XFREEZE):
        old = json.load(open(XFREEZE))
        if old.get("freeze_digest") != fz["freeze_digest"]:
            raise RuntimeError("T1_DNA_X_V1 exists and differs — the X layer is frozen")
    else:
        json.dump(fz, open(XFREEZE, "w"), indent=2, default=str)
    return fz


def load_ms(shuffle_seed=None):
    M = pd.read_parquet(D9.OUT_MS, columns=LOAD_COLS)
    # gate A — duplicate source grain hard-fails
    dup = M.duplicated(["episode_id", "relative_day", "session_position"])
    if dup.any():
        raise RuntimeError(f"gate A: {int(dup.sum())} duplicate (episode_id, relative_day, "
                           f"session_position) rows — the source grain is broken")
    if shuffle_seed is not None:                      # gate C — reorder must not matter
        M = M.sample(frac=1.0, random_state=shuffle_seed).reset_index(drop=True)
    return M


def enumerate_grammar(M, verbose=True, collect_occ=False):
    """The T5 pipeline with T1 literals. Returns (universe, CLAIMS df, tokens)."""
    modal = (M.groupby(["session_date", "ticker"])["bars_in_session"].first()
             .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    M = M[M.bars_in_session == M.session_date.map(modal)]

    M2 = M.rename(columns={"t1_date": "t5_date"})     # searchable_tokens speaks T5 names
    REG = G5.searchable_tokens(M2)
    toks = REG.loc[REG.searchable_in_sequence_v1, "token"].tolist()
    tix = {t: i for i, t in enumerate(toks)}

    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, s in enumerate(M.token_set.to_numpy()):
        for t in s.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True

    M = M.reset_index(drop=True)
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    _first = M.drop_duplicates("episode_id").set_index("episode_id")
    ep_ticker = pd.factorize(_first.loc[ep_uniq, "ticker"])[0]
    ep_date = pd.factorize(_first.loc[ep_uniq, "t1_date"])[0]
    order = np.lexsort((M.session_position.to_numpy(),
                        (M.relative_day == "T1_DAY").to_numpy(), ep_codes))
    Ms = M.iloc[order].reset_index(drop=True)
    Bs = B[order]
    ep = ep_codes[order]
    rd = (Ms.relative_day == "T1_DAY").to_numpy()
    pos = Ms.session_position.to_numpy()

    def adjacencies(family, span):
        n = len(Ms)
        idx = np.arange(n - span + 1)
        ok = np.ones(len(idx), bool)
        for j in range(span):
            ok &= ep[idx + j] == ep[idx]
        if family == "T1_INTRADAY":
            for j in range(span):
                ok &= rd[idx + j]
            for j in range(span - 1):
                ok &= pos[idx + j + 1] == pos[idx + j] + 1
        elif family == "PREV_INTRADAY":
            for j in range(span):
                ok &= ~rd[idx + j]
            for j in range(span - 1):
                ok &= pos[idx + j + 1] == pos[idx + j] + 1
        else:                                          # CROSS_DAY — exactly one seam
            trans = np.zeros(len(idx), int)
            for j in range(span - 1):
                step = (~rd[idx + j]) & rd[idx + j + 1]
                same = rd[idx + j] == rd[idx + j + 1]
                trans += step
                ok &= step | (same & (pos[idx + j + 1] == pos[idx + j] + 1))
            ok &= trans == 1
        return idx[ok]

    universe, CLAIMS, OCC = {}, [], []
    for fam in FAMILIES:
        for L in LENGTHS:
            a = adjacencies(fam, L)
            if len(a) == 0:
                universe[f"{fam}|{L}"] = dict(adjacencies=0, support_ok=0, distinct=0,
                                              aliases=0, survivors=[])
                continue
            cols = [Bs[a + j] for j in range(L)]
            if L == 2:
                inst = (cols[0].astype(np.float32).T @ cols[1].astype(np.float32))
                cand = [(i, j) for i, j in zip(*np.nonzero(inst >= MIN_EP))]
            else:
                prev = universe.get(f"{fam}|2", {}).get("survivors", [])
                cand = []
                for (i, j) in prev:
                    m2 = cols[0][:, i] & cols[1][:, j]
                    if m2.sum() < MIN_EP:
                        continue
                    hit = cols[2][m2].sum(0)
                    cand += [(i, j, int(k)) for k in np.nonzero(hit >= MIN_EP)[0]]
            surv, sig, sup, stats = [], {}, [], []
            for c in cand:
                m = cols[0][:, c[0]]
                for j in range(1, L):
                    m &= cols[j][:, c[j]]
                e = np.unique(ep[a[m]])
                if len(e) < MIN_EP:
                    continue
                ntk = len(np.unique(ep_ticker[e]))
                if ntk < MIN_TK:
                    continue
                ud, dc = np.unique(ep_date[e], return_counts=True)
                share = dc.max() / len(e)
                if len(ud) < MIN_DT or share > MAX_DATE_SHARE:
                    continue
                surv.append(c)
                sup.append(len(e))
                name = "→".join(toks[x] for x in c)
                if collect_occ:
                    # one row PER WINDOW INSTANCE — the true occurrence grain. An episode with
                    # three qualifying windows contributes three rows; aggregation to episode
                    # support MUST collapse them, and gate B verifies exactly that.
                    OCC.append((f"{fam}|{L}|{name}", np.asarray(ep_uniq)[ep[a[m]]]))
                # membership_hash over the REAL episode ids, sorted. The first version hashed
                # factorize's local integer codes — first-appearance labels that change under
                # row shuffle — and gate C/E correctly refused: (T9 precedent)
                # while the true memberships were identical. Hash the membership, not its
                # labeling.
                e_ids = "|".join(sorted(ep_uniq[e].astype(str)))
                mh = hashlib.sha256(e_ids.encode()).hexdigest()[:16]
                sig.setdefault(mh, []).append(name)
                stats.append(dict(family=fam, length=L, canonical_sequence=name,
                                  membership_hash=mh, episode_n=int(len(e)),
                                  ticker_n=int(ntk), date_n=int(len(ud)),
                                  max_date_share=round(float(share), 6),
                                  token_idx=json.dumps(list(map(int, c)))))
            CLAIMS.extend(stats)
            aliases = sum(len(v) - 1 for v in sig.values() if len(v) > 1)
            universe[f"{fam}|{L}"] = dict(
                adjacencies=int(len(a)), stage1_candidates=len(cand),
                support_ok=len(surv), distinct=len(sig), aliases=aliases, survivors=surv,
                support_quantiles={q: int(np.quantile(sup, v)) for q, v in
                                   (("q10", .1), ("median", .5), ("q90", .9))} if sup else {})
            if verbose:
                print(f"  {fam:<14} {L}-bar · adj {len(a):>9,} · stage1 {len(cand):>7,} · "
                      f"support-ok {len(surv):>6,} · distinct {len(sig):>6,} · "
                      f"aliases {aliases}", flush=True)
    C = pd.DataFrame(CLAIMS)
    if len(C):
        C["equivalence_class_id"] = (C["family"] + "|" + C["length"].astype(str) + "|"
                                     + C.groupby(["family", "length", "membership_hash"],
                                                 sort=False).ngroup().astype(str))
        C["syntactic_aliases"] = C.groupby("equivalence_class_id")["canonical_sequence"] \
                                  .transform("size") - 1
    if collect_occ:
        occ = pd.DataFrame([(c, e) for c, arr in OCC for e in arr],
                           columns=["claim", "episode_id"])
        return universe, C, toks, occ
    return universe, C, toks


def class_digest(C):
    key = (C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence + "|"
           + C.membership_hash + "|" + C.episode_n.astype(str))
    return hashlib.sha256("\n".join(sorted(key)).encode()).hexdigest()[:16]


def gate_b_replication(M, factors=(2, 5, 10), n_episodes=3000, verbose=True):
    """Gate B — aggregation invariance at the OCCURRENCE-ROW grain.

    THE FIRST VERSION TESTED THE WRONG GRAIN AND FAILED HONESTLY. It replicated SOURCE BARS
    and re-ran the strict-adjacency detector: under replication a sorted episode reads
    [p1,p1,p2,p2,p3,p3], so every 3-bar window (p1,p2,p3) disappears by construction and the
    x2/x5/x10 runs all failed. That is not an aggregation bug — duplicated source grain is
    CORRUPT INPUT, and gate A already hard-fails it before the detector ever runs.

    What section B of the contract actually protects (the historical T5 bug: occurrence rows
    silently became episode support) is the aggregation AFTER detection. So the gate now
    collects the true occurrence rows — one row per qualifying window instance, an episode
    with three windows contributes three rows — replicates THOSE x2/x5/x10 with a shuffle,
    and requires episode-level support and membership to be identical. A pipeline that counts
    rows instead of distinct episodes fails this immediately."""
    eps = sorted(pd.unique(M.episode_id))[:n_episodes]
    Msub = M[M.episode_id.isin(set(eps))]
    _, C0, _, occ = enumerate_grammar(Msub, verbose=False, collect_occ=True)

    def agg(df):
        g = df.groupby("claim")["episode_id"]
        return {c: (frozenset(v), len(set(v))) for c, v in g.apply(set).items()}

    base = agg(occ)
    multi = int((occ.groupby(["claim", "episode_id"]).size() > 1).sum())
    out = {}
    for f in factors:
        rep = pd.concat([occ] * f, ignore_index=True).sample(
            frac=1.0, random_state=f).reset_index(drop=True)
        out[f"x{f}"] = bool(agg(rep) == base)
        if verbose:
            print(f"  gate B ×{f}: {'PASS' if out[f'x{f}'] else 'FAIL'} "
                  f"({len(base)} claims · {len(occ):,} occurrence rows · "
                  f"{multi:,} multi-window (claim,episode) pairs exercised)", flush=True)
    return out, len(base)


def main():
    t0 = time.time()
    fz = freeze_x()
    print(f"T1 grammar · X freeze {fz['freeze_digest']} · pop {fz['full_T1_population']:,} · "
          f"1H {fz['T1_day_1H_observable']:,} · cross-day {fz['cross_day_observable']:,}",
          flush=True)

    M = load_ms()                                     # gate A inside
    print(f"  gate A PASS · {len(M):,} source rows, grain unique", flush=True)

    u1, C1, toks = enumerate_grammar(M)
    d1 = class_digest(C1)

    u2, C2, _ = enumerate_grammar(load_ms(shuffle_seed=101), verbose=False)
    d2 = class_digest(C2)
    print(f"  gate C+E: run1 {d1} · run2(shuffled) {d2} · "
          f"{'PASS' if d1 == d2 else 'FAIL'}", flush=True)
    if d1 != d2:
        raise RuntimeError("gates C/E FAILED — memberships depend on row order or run")

    gb, nb = gate_b_replication(M)
    if not all(gb.values()):
        raise RuntimeError(f"gate B FAILED: {gb}")

    C1.to_parquet(SURV, index=False)
    pd.DataFrame(dict(token=toks)).to_parquet(TOKS, index=False)

    spec = dict(
        spec_id="T1_SEQUENCE_GRAMMAR_V1", family_id="T1_MICROSTRUCTURE_DNA_V1",
        x_freeze_digest=fz["freeze_digest"],
        adjacency="STRICT_ADJACENT", lengths=list(LENGTHS), four_bar="NOT_IN_V1",
        families=list(FAMILIES),
        element_semantics="atomic canonical token presence on a bar",
        occurrence_unit="episode-level binary membership",
        support=dict(min_episodes_treated=MIN_EP, min_unique_tickers=MIN_TK,
                     min_unique_dates=MIN_DT, max_single_date_share=MAX_DATE_SHARE,
                     control_floors="applied at the estimand stage, as in T5"),
        searchable_token_count=len(toks),
        totals=dict(support_qualified=int(len(C1)),
                    distinct=sum(v["distinct"] for v in u1.values()),
                    aliases=sum(v["aliases"] for v in u1.values())),
        membership_class_digest=d1,
        gates=dict(A="PASS", B=gb, C="PASS", D="inherited: row_number over ts within "
                   "session; validity = modal calendar length", E="PASS",
                   F=f"loaded columns only: {LOAD_COLS}"),
        outcome_exposure="NOT_EXPOSED")
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(spec, open(SPEC, "w"), indent=2, default=str)
    rep = {k: {kk: vv for kk, vv in v.items() if kk != "survivors"}
           for k, v in u1.items()}
    json.dump(dict(spec_digest=spec["spec_digest"], universe=rep,
                   seconds=round(time.time() - t0, 1)),
              open(OUT, "w"), indent=2, default=str)
    print(f"\n  T1_SEQUENCE_GRAMMAR_V1 {spec['spec_digest']} · "
          f"support-qualified {len(C1):,} · distinct {spec['totals']['distinct']:,} · "
          f"aliases {spec['totals']['aliases']} · {time.time()-t0:.0f}s · NO OUTCOME JOINED",
          flush=True)


if __name__ == "__main__":
    main()
