"""PT5 — Preview T5. A research VIEW over already-frozen T5 research. Nothing new is decided.

    PT5 is NOT a new trading rule, NOT forward validation, and touches no frozen artifact.
    It answers one question on a chart: "this is a canonical 1D T5 — did its frozen 1H and/or
    15m microstructure match one of the historically selected strengthening families?"

MEMBERSHIP IS READ FROM THE SEALED ARTIFACTS, NEVER RE-EVALUATED

Re-implementing the token engine here would create a second semantics that could drift from
the frozen one. So:

    1H   the sealed survivor-membership cache (t5_1h_survivor_membership.parquet), whose
         identity is guarded by T5_1H_MEMBERSHIP_CACHE_V1 — the OVERLAP-RESTRICTED sealed
         memberships, i.e. exactly what the frozen historical evidence used
    15m  the sealed engine bundle segments (the same reconstruction the forward-evaluator
         replay proved exact against the frozen family digest)

CLASSIFICATION, NOT RANKING

    PT5_BASE    canonical 1D T5 only
    PT5_1H      + at least one frozen 1H family
    PT5_15M     + at least one frozen 15m medoid
    PT5_STRONG  + both

No score is derived from Z, theta, support, alias counts or returns. Alias overlap does not
make a PT5 "stronger". The XR badge (VOL_W <-> VA→*) is DESCRIPTIVE ONLY — it is a
post-exposure historical compatibility observation, not confirmation, not replication, not
independent evidence, and it changes no classification.

X-ONLY

Inputs are the frozen membership sets and the episode table. No MFE, MAE, ret, future bars,
survivor Z, outcome rank or forward state is read. All memberships were computed from X at
the frozen build; this script only JOINS them.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402

DATA = os.path.join(D.ROOT, "data")
OUT_PARQ = os.path.join(DATA, "pt5_signals.parquet")
OUT_SPEC = "PT5_PREVIEW_V1.json"
CUTOFF = "2026-08-20"                     # official prospective validation cutoff
T5_DEF_HASH_EXPECTED = "2775322838e81bd7"

H1_FAMILIES = {                            # frozen 1H overlap-cluster medoids, verbatim
    "H1_A": "T5_INTRADAY|2|BUY→L5",
    "H1_B": "CROSS_DAY|2|VOL_W→L46x",
    "H1_C": "T5_INTRADAY|2|L43→T2",
    "H1_D": "PREV_INTRADAY|2|BUY→Z2",
}
# sealed member counts from T5_FORWARD_MEMBERSHIP_EVALUATOR_V1.historical_replay.h1.detail
H1_SEALED_COUNTS = {"T5_INTRADAY|2|BUY→L5": 322, "CROSS_DAY|2|VOL_W→L46x": 596,
                    "T5_INTRADAY|2|L43→T2": 5670, "PREV_INTRADAY|2|BUY→Z2": 423}

FORBIDDEN_COLS = {"mfe_10d", "mae_10d", "ret_10d", "mfe_atr_10d", "theta", "z_rank",
                  "max_z", "survivor_forward", "win_rate", "mean_return"}


class PT5GateFailure(RuntimeError):
    pass


def load_episodes():
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date",
                                           "t5_definition_hash"])
    bad = set(E.columns) & FORBIDDEN_COLS
    if bad:
        raise PT5GateFailure(f"outcome column reached PT5: {bad}")
    hashes = set(E.t5_definition_hash.unique())
    if hashes != {T5_DEF_HASH_EXPECTED}:
        raise PT5GateFailure(f"T5 definition hash drift: {hashes} != "
                             f"{{{T5_DEF_HASH_EXPECTED!r}}} — PT5 may only exist on the "
                             f"canonical priority-resolved T5")
    dup = E.duplicated(["ticker", "t5_date"])
    if dup.any():
        raise PT5GateFailure(f"duplicate (ticker, t5_date) grain: "
                             f"{E[dup][['ticker','t5_date']].head(3).to_dict('records')}")
    if E.episode_id.duplicated().any():
        raise PT5GateFailure("duplicate episode_id")
    return E


def load_h1_membership():
    T = pd.read_parquet(os.path.join(DATA, "t5_1h_survivor_membership.parquet"))
    memb = {}
    for fam, claim in H1_FAMILIES.items():
        s = set(T[(T.kind == "member") & (T.claim_id == claim)].episode_id)
        if len(s) != H1_SEALED_COUNTS[claim]:
            raise PT5GateFailure(
                f"1H membership does not reproduce the frozen sealed membership: {claim} has "
                f"{len(s)} members, sealed says {H1_SEALED_COUNTS[claim]}")
        memb[fam] = s
    claims_in_cache = set(T[T.kind == "member"].claim_id.unique())
    missing = set(H1_FAMILIES.values()) - claims_in_cache
    if missing:
        raise PT5GateFailure(f"frozen medoid(s) absent from the sealed cache: {missing}")
    # The cache legitimately holds all NINE sealed 1H survivors (the 9/840 exposure); the four
    # medoids are the frozen overlap-cluster representatives among them. PT5 uses exactly the
    # medoids per spec — the other five are within-cluster aliases and alias overlap must not
    # strengthen a PT5.
    return memb


def load_m15_membership():
    """The sealed engine-bundle segments — the same reconstruction the forward-evaluator
    replay proved EXACT against the frozen family digest. REP.support is the sealed treated
    count per representative, so equality with it is the reproduction gate."""
    import t5_15m_par_qualify as PQ
    REP = pd.read_parquet(os.path.join(DATA, "t5_15m_cluster_representatives.parquet"))
    z = np.load(PQ.BUNDLE)
    eidx, seg_ptr, csp = z["eidx"], z["seg_ptr"], z["claim_seg_ptr"]
    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids = P.episode_id.to_numpy()
    memb, meta = {}, {}
    for r in REP.itertuples():
        a, b = csp[r.j], csp[r.j + 1]
        s = set(ids[eidx[seg_ptr[a]:seg_ptr[b]]])
        if len(s) != int(r.support):
            raise PT5GateFailure(
                f"15m membership does not reproduce the frozen representative membership: "
                f"cluster {r.cluster} ({r.claim_id}) has {len(s)}, sealed support "
                f"{int(r.support)}")
        memb[int(r.cluster)] = s
        meta[int(r.cluster)] = dict(claim_id=r.claim_id,
                                    position_family=r.position_family,
                                    first_token=r.claim_id.split("|")[2].split("→")[0])
    return memb, meta, ART.file_digest(os.path.join(
        DATA, "t5_15m_cluster_representatives.parquet"))


def build(E, h1, m15, m15_meta):
    h1_fams = {}
    for fam, s in h1.items():
        for e in s:
            h1_fams.setdefault(e, []).append(fam)
    m15_hits = {}
    for cl, s in m15.items():
        for e in s:
            m15_hits.setdefault(e, []).append(cl)

    rows = []
    for r in E.itertuples():
        e = r.episode_id
        fams = sorted(h1_fams.get(e, []))
        cls = sorted(m15_hits.get(e, []))
        h1_any, m15_any = bool(fams), bool(cls)
        klass = ("PT5_STRONG" if h1_any and m15_any else
                 "PT5_15M" if m15_any else
                 "PT5_1H" if h1_any else "PT5_BASE")
        # XR badge: descriptive only. VOL_W 1H family AND any matched 15m medoid whose FIRST
        # token is VA — both read from frozen claim ids, no new rule invented.
        xr = ("H1_B" in fams) and any(m15_meta[c]["first_token"] == "VA" for c in cls)
        rows.append(dict(
            ticker=r.ticker, date=r.t5_date, episode_id=e,
            pt5_base=True, pt5_class=klass,
            pt5_h1_any=h1_any, pt5_h1_match_count=len(fams),
            pt5_h1_family=",".join(H1_FAMILIES[f].split("|")[2] for f in fams),
            pt5_h1_families=",".join(fams),
            pt5_m15_any=m15_any, pt5_m15_match_count=len(cls),
            pt5_m15_cluster_ids=",".join(map(str, cls)),
            pt5_m15_representatives=";".join(m15_meta[c]["claim_id"] for c in cls[:12]),
            pt5_xr_volw_va=bool(xr)))
    return pd.DataFrame(rows).sort_values(["ticker", "date"]).reset_index(drop=True)


def membership_digest(df):
    key = df[["episode_id", "pt5_class", "pt5_h1_families", "pt5_m15_cluster_ids",
              "pt5_xr_volw_va"]].astype(str).agg("|".join, axis=1)
    return hashlib.sha256("\n".join(sorted(key)).encode()).hexdigest()[:16]


def main():
    ART.smoke_test(verbose=False)
    print("PT5 · Preview T5 build", flush=True)
    E = load_episodes()
    h1 = load_h1_membership()
    m15, m15_meta, rep_digest = load_m15_membership()
    print(f"  episodes {len(E):,} · 1H members "
          f"{ {f: len(s) for f, s in h1.items()} } · 15m clusters {len(m15)}")

    df = build(E, h1, m15, m15_meta)
    dig1 = membership_digest(df)

    # gate: source-row reorder must not change membership
    E2 = E.sample(frac=1.0, random_state=17).reset_index(drop=True)
    dig2 = membership_digest(build(E2, h1, m15, m15_meta))
    if dig1 != dig2:
        raise PT5GateFailure(f"row-order dependence: {dig1} != {dig2}")

    # gate: base condition — PT5 exists ONLY where canonical T5 exists (by construction the
    # frame is built FROM the episode table; asserted anyway rather than trusted)
    if not df.episode_id.isin(set(E.episode_id)).all():
        raise PT5GateFailure("a PT5 row exists outside the canonical T5 episode table")
    if str(df.date.max()) > CUTOFF:
        raise PT5GateFailure(f"post-cutoff PT5 in the build: {df.date.max()} > {CUTOFF}")

    df.to_parquet(OUT_PARQ, index=False)
    counts = dict(PT5_BASE=int((df.pt5_class == "PT5_BASE").sum()),
                  PT5_1H=int((df.pt5_class == "PT5_1H").sum()),
                  PT5_15M=int((df.pt5_class == "PT5_15M").sum()),
                  PT5_STRONG=int((df.pt5_class == "PT5_STRONG").sum()),
                  XR_VOLW_VA=int(df.pt5_xr_volw_va.sum()))

    ART.seal(dict(
        spec_id="PT5_PREVIEW_V1", status="BUILT",
        role="RESEARCH PREVIEW — a visual window onto already-frozen historical research. "
             "Not a trading rule, not forward validation, not evidence.",
        base_condition=dict(setup="FINAL_PRIORITY_RESOLVED_T5",
                            t5_definition_hash=T5_DEF_HASH_EXPECTED,
                            enforced="hash asserted on every episode row; PT5 cannot exist "
                                     "off the canonical episode table"),
        h1=dict(families=H1_FAMILIES, sealed_counts=H1_SEALED_COUNTS,
                source="t5_1h_survivor_membership.parquet (overlap-restricted sealed "
                       "memberships, identity-guarded by T5_1H_MEMBERSHIP_CACHE_V1)"),
        m15=dict(representatives_artifact="T5_15M_CLUSTER_REPRESENTATIVES_V1",
                 representatives_digest=rep_digest, n_clusters=len(m15),
                 source="sealed engine-bundle segments — the reconstruction the "
                        "forward-evaluator replay proved exact",
                 reproduction_gate="len(members) == sealed REP.support for all 171"),
        classification=dict(classes=["PT5_BASE", "PT5_1H", "PT5_15M", "PT5_STRONG"],
                            no_score="no numeric strength from Z/theta/support/aliases/returns",
                            alias_overlap_does_not_strengthen=True),
        xr_badge=dict(field="pt5_xr_volw_va", label="XR: VOL_W ↔ VA",
                      status="DESCRIPTIVE ONLY — post-exposure historical compatibility; not "
                             "confirmation, not replication, not independent evidence; "
                             "changes no classification and no ranking"),
        cutoff=dict(official=CUTOFF, default_visibility=f"signal_session <= {CUTOFF}",
                    post_cutoff="hidden by default; behind 'Show post-cutoff PT5' with an "
                                "OBSERVATIONAL MODE warning"),
        x_only=dict(forbidden=sorted(FORBIDDEN_COLS),
                    inputs=["frozen membership sets", "episode table"]),
        counts=counts, n_rows=int(len(df)),
        membership_digest=dig1,
        reorder_invariance="PASS",
        parquet=os.path.basename(OUT_PARQ), parquet_digest=ART.file_digest(OUT_PARQ)),
        OUT_SPEC, required=("spec_id", "counts", "membership_digest"), supersede=True)

    print(f"  counts {counts}")
    print(f"  membership digest {dig1} · reorder-invariant · wrote {OUT_PARQ}")
    print(f"  PT5_PREVIEW_V1 · {ART.file_digest(OUT_SPEC)}")
    return dig1


if __name__ == "__main__":
    main()
