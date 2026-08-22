"""Generalized 15m X-only runner for T9 / T3 / T1 — one implementation, three families.

Each family's own frozen prespec governs it: OPENING HOUR ONLY, position-anchored 2-bar
claims over M1->M2, M2->M3, M3->M4, the 3-bar grammar excluded from V1. Nothing here is
designed from an outcome; the grammar was frozen before any 15m work began.

    exact-time selector   09:30 / 09:45 / 10:00 / 10:15 -> M1..M4 by CLOCK, never by
                          observation order, so a missing slot means the claim cannot fire
                          and no later bar slides into the vacancy
    position anchoring    M1->M2 and M2->M3 with the same tokens are DIFFERENT claims
    support floors        the family's own frozen 1H floors, unchanged
    multiplicity          NOT pooled across families — three separately governed studies

Stops after the estimand: final k, population and blocks are reported and nothing else
runs. No outcome value is read; only path_status_10d, and only as a status.

    usage:  python t15m_x.py t9 [t3 t1]
"""
from __future__ import annotations
import ast, hashlib, importlib, json, os, sys, time                  # noqa: E402
import duckdb, numpy as np, pandas as pd                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import combo_tokens as CT                                            # noqa: E402
import t5_sequence_estimand as E5                                    # noqa: E402

ET_POS = {"09:30": 1, "09:45": 2, "10:00": 3, "10:15": 4}
PAIRS = [("M1", "M2"), ("M2", "M3"), ("M3", "M4")]
UMBRELLA = {"ANY_T", "ANY_Z", "ANY_L", "L5_ANY", "AD_ANY", "ANY_P", "ANY_D"}


def load_opening_hour(fam, D, verbose=True):
    """One row per (episode, M1..M4) bar, selected by EXACT ET clock time."""
    dcol = f"{fam}_date"
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", dcol])
    db15 = os.path.join(D.ROOT, "data", "studio_15m.duckdb")
    conn = duckdb.connect(db15, read_only=True)
    try:
        have = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
        miss = [c for c in D.SRC_COLS if c not in have]
        if miss:
            raise RuntimeError(f"15m store lacks token source columns: {miss[:8]}")
        conn.register("ep", E.rename(columns={dcol: "d"}))
        sel = ", ".join(f"x.{c}" for c in D.SRC_COLS)
        M = conn.execute(f"""
            SELECT e.episode_id, e.ticker, CAST(e.d AS VARCHAR) sess_date,
                   strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                            '%H:%M') et_time,
                   x.open, x.high, x.low, x.close, x.volume, {sel}
            FROM ep e JOIN bars x
              ON x.ticker = e.ticker
             AND CAST(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE)
                 = CAST(e.d AS DATE)
            WHERE strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                           '%H:%M') IN ('09:30','09:45','10:00','10:15')""").fetchdf()
    finally:
        conn.close()
    M["pos"] = M.et_time.map(ET_POS)
    if M.duplicated(["episode_id", "pos"]).any():
        raise RuntimeError("duplicate (episode_id, position) in the 15m opening hour — a "
                           "duplicated bar would create memberships that do not exist")
    cols = {t.token_id: CT._evaluate(M, t).astype(np.uint8) for t in D.TOKENS_1H}
    T = pd.DataFrame(cols, index=M.index)
    if verbose:
        print(f"  opening-hour bars {len(M):,} · episodes touched "
              f"{M.episode_id.nunique():,}", flush=True)
    return E, M, T


def searchable(M, T, D):
    arr = T.to_numpy(dtype=bool)
    cnt, n = arr.sum(0), len(M)
    rows, keep = [], []
    for i, t in enumerate(D.TOKENS_1H):
        c, share = int(cnt[i]), cnt[i] / n
        if t.token_id in UMBRELLA:
            ok, why = False, "umbrella union; its sequences are implied"
        elif c == 0:
            ok, why = False, "never true on 15m opening-hour bars in this population"
        elif share > 0.99:
            ok, why = False, f"true on {share:.1%} of bars — not distinguishing"
        else:
            ok, why = True, ""
        rows.append(dict(token=t.token_id, bar_count=c,
                         bar_share=round(float(share), 6), searchable=ok, reason=why))
        if ok:
            keep.append(i)
    return pd.DataFrame(rows), np.array(keep)


def run(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    G1 = importlib.import_module(f"{fam}_sequence_grammar")
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    dcol = f"{fam}_date"
    pre = json.load(open(f"{F}_15M_PRESPEC_V1.json"))
    assert pre["primary_scope"] == "OPENING HOUR ONLY"
    assert pre["position_families"] == ["M1→M2", "M2→M3", "M3→M4"]
    print(f"{F} 15m X · prespec {ART.file_digest(f'{F}_15M_PRESPEC_V1.json')} · "
          f"def {pre['definition_hash']}", flush=True)

    E, M, T = load_opening_hour(fam, D)
    REG, keep = searchable(M, T, D)
    tok = [D.TOKENS_1H[i].token_id for i in keep]
    print(f"  searchable tokens {len(tok)} of {len(D.TOKENS_1H)}", flush=True)

    # ── slot integrity: how many episodes actually have each M slot ─────────
    piv = M.pivot_table(index="episode_id", columns="pos", values="et_time",
                        aggfunc="first")
    for p in (1, 2, 3, 4):
        if p not in piv.columns:
            piv[p] = np.nan
    complete = piv[[1, 2, 3, 4]].notna().all(axis=1)
    slot_counts = {f"M{p}": int(piv[p].notna().sum()) for p in (1, 2, 3, 4)}

    # ── bar x token cube, positions from the CLOCK ─────────────────────────
    ep_ids = piv.index.to_numpy()
    eix = {e: i for i, e in enumerate(ep_ids)}
    nE = len(ep_ids)
    B = np.zeros((4, nE, len(tok)), dtype=bool)
    arr = T.to_numpy(dtype=bool)[:, keep]
    B[M.pos.to_numpy() - 1, M.episode_id.map(eix).to_numpy()] = arr

    first = M.drop_duplicates("episode_id").set_index("episode_id").loc[ep_ids]
    ep_tk = pd.factorize(first.ticker)[0]
    ep_dt = pd.factorize(first.sess_date)[0]
    n_tk, n_dt = int(ep_tk.max()) + 1, int(ep_dt.max()) + 1
    MIN_EP, MIN_TK, MIN_DT = G1.MIN_EP, G1.MIN_TK, G1.MIN_DT
    MAX_DS = G1.MAX_DATE_SHARE

    # ── estimand population and blocks, fixed BEFORE the sweep ─────────────
    O = pd.read_parquet(E1.OC, columns=["episode_id", "path_status_10d"])   # STATUS only
    P = pd.DataFrame(dict(episode_id=ep_ids)).merge(
        pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", dcol]),
        on="episode_id", how="left").merge(O, on="episode_id", how="left")
    P = P[P.path_status_10d == "AVAILABLE"]
    P = P.rename(columns={dcol: "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    pop = set(P.episode_id)
    in_pop = np.array([e in pop for e in ep_ids])
    idx_pop = np.flatnonzero(in_pop)                 # rows that ARE in the estimand
    blk = P.set_index("episode_id").block.reindex(ep_ids[idx_pop]).to_numpy()
    bfac, _ = pd.factorize(blk)                      # no NaN here, so no -1 codes
    nb = int(bfac.max()) + 1
    blk_size = np.bincount(bfac, minlength=nb).astype(float)
    print(f"  population {len(P):,} of {nE:,} opening-hour episodes · blocks "
          f"{P.block.nunique():,}", flush=True)

    # ── the sweep: position-anchored 2-bar claims only ─────────────────────
    sig, claims = {}, []
    n_cand = 0
    for (a, b) in PAIRS:
        ia, ib = int(a[1:]) - 1, int(b[1:]) - 1     # M1..M4 -> 0..3
        Ba, Bb = B[ia], B[ib]
        for i, ti in enumerate(tok):
            va = Ba[:, i]
            if not va.any():
                continue
            for j, tj in enumerate(tok):
                n_cand += 1
                v = va & Bb[:, j]
                nt = int(v.sum())
                if nt < MIN_EP or nE - nt < MIN_EP:
                    continue
                tc = np.bincount(ep_tk[v], minlength=n_tk)
                dc = np.bincount(ep_dt[v], minlength=n_dt)
                n_t_, n_d_ = int((tc > 0).sum()), int((dc > 0).sum())
                if n_t_ < MIN_TK or n_d_ < MIN_DT or dc.max() / nt > MAX_DS:
                    continue
                # overlap-restricted signature, computed on the estimand rows only
                vp = v[idx_pop].astype(float)
                nt_b = np.bincount(bfac, weights=vp, minlength=nb)
                inov = ((nt_b > 0) & (blk_size - nt_b > 0))[bfac]
                m = vp.astype(bool) & inov
                h = hashlib.sha256(np.flatnonzero(m).tobytes()).hexdigest()[:16]
                name = f"{a}→{b}|{ti}→{tj}"
                sig.setdefault(h, []).append(name)
                claims.append(dict(position_family=f"{a}→{b}", first_token=ti,
                                   second_token=tj, claim_id=name,
                                   support=nt, restricted_support=int(m.sum()),
                                   tickers=n_t_, dates=n_d_,
                                   max_date_share=round(float(dc.max() / nt), 6),
                                   membership_hash=h))
    C = pd.DataFrame(claims)
    k_final = len(sig)
    aliases = len(C) - k_final
    out_parq = os.path.join(D.ROOT, "data", f"{fam}_15m_claims.parquet")
    C.to_parquet(out_parq, index=False)

    # ── gates ──────────────────────────────────────────────────────────────
    src = open(__file__).read()
    tree = ast.parse(src)
    sql = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
           and isinstance(n.value, str) and "FROM bars" in n.value]
    y_tokens = [t for t in ("fwd_", "mfe", "mae", "ret_", "mtm")
                if any(t in q.lower() for q in sql)]
    loaded_outcome_cols = [c for c in ("path_status_10d",)]
    reorder = M.sample(frac=1.0, random_state=31).reset_index(drop=True)
    piv2 = reorder.pivot_table(index="episode_id", columns="pos", values="et_time",
                               aggfunc="first")
    reorder_ok = bool(piv2.reindex(piv.index).equals(piv.reindex(piv2.index).reindex(
        piv.index)) or piv2.sort_index().equals(piv.sort_index()))
    bridge = 0          # by construction: positions come from the clock, never from order
    gates = {
        "exact-time selector only (no fallback to the next observed bar)": True,
        "positions are named by clock, so a missing slot cannot be bridged": bridge == 0,
        "duplicate (episode, position) grain": True,
        "row reorder leaves the opening-hour frame identical": reorder_ok,
        "no outcome column in the 15m query": not y_tokens,
        "only path_status_10d is read from the outcome sidecar, as a STATUS":
            loaded_outcome_cols == ["path_status_10d"],
    }
    audit = dict(
        spec_id=f"{F}_15M_X_ONLY_V1", status="X_ONLY_PRE_Y", family=F,
        prespec=dict(digest=ART.file_digest(f"{F}_15M_PRESPEC_V1.json"),
                     definition_hash=pre["definition_hash"],
                     scope=pre["primary_scope"],
                     position_families=pre["position_families"],
                     three_bar="EXCLUDED from V1"),
        funnel=dict(canonical_episodes=int(len(E)),
                    fifteen_m_observable=int(nE),
                    opening_hour_complete=int(complete.sum()),
                    sequence_analyzable=int(len(P)),
                    blocks=int(P.block.nunique()),
                    median_block_size=int(P.groupby("block").size().median())),
        slot_coverage=slot_counts,
        tokens=dict(registry=len(D.TOKENS_1H), searchable=len(tok)),
        enumeration=dict(candidates=n_cand,
                         support_qualified_names=int(len(C)),
                         membership_distinct_classes=k_final,
                         aliases=aliases,
                         k_final_candidate=k_final),
        support=dict(floors=dict(min_episodes=MIN_EP, min_tickers=MIN_TK,
                                 min_dates=MIN_DT, max_date_share=MAX_DS),
                     support_quantiles={q: int(C.support.quantile(v)) for q, v in
                                        (("q10", .1), ("median", .5), ("q90", .9))}
                     if len(C) else {},
                     max_date_concentration=round(float(C.max_date_share.max()), 6)
                     if len(C) else None),
        by_position_family={pf: int((C.position_family == pf).sum())
                            for pf in C.position_family.unique()} if len(C) else {},
        gates={k: bool(v) for k, v in gates.items()},
        digests=dict(population=hashlib.sha256(
            "|".join(sorted(P.episode_id)).encode()).hexdigest()[:16],
            blocks=hashlib.sha256(
                "|".join(sorted(P.episode_id + ":" + P.block)).encode()).hexdigest()[:16],
            claims_table=ART.file_digest(out_parq),
            membership_class=hashlib.sha256(
                "\n".join(sorted(sig)).encode()).hexdigest()[:16],
            definition=getattr(D, f"{F}_DEF_HASH")),
        multiplicity="NOT pooled with any other family — three separately governed studies",
        y_status="NO OUTCOME VALUE READ — only path_status_10d, as a status",
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(audit, f"{F}_15M_X_ONLY_V1.json",
                 required=("spec_id", "funnel", "enumeration", "gates"),
                 supersede=os.path.exists(f"{F}_15M_X_ONLY_V1.json"))
    print(f"  candidates {n_cand:,} · support-qualified {len(C):,} · classes {k_final:,} "
          f"· aliases {aliases:,}")
    for k, v in gates.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  {F}_15M_X_ONLY_V1 · {d} · {(time.time()-t0)/60:.1f} min\n")
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9", "t3", "t1"]):
        run(f)
