"""T5_FORWARD_MEDOID_IDENTITY_V1 — do the frozen forward rule memberships survive the
session-validity correction unchanged?

T5's historical Branch A rests on the enumeration input being bit-identical. The forward
path needs one more equality, because the forward rules are DERIVED from those 1H
memberships: the frozen survivor/medoid membership cache must be reproducible from the
SEALED T5 microstructure under the corrected market-wide session rule.

Read-only. The sealed microstructure is the source; the current mutable 1H store is never
used to regenerate family X — only expected_bars(date) comes from the sealed calendar.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D5                               # noqa: E402

OUT = "T5_FORWARD_MEDOID_IDENTITY_V1.json"
CACHE_ART = "T5_1H_MEMBERSHIP_CACHE_V1.json"
MEMB = "/Users/sachoki/Desktop/sachoki-desktop/data/t5_1h_survivor_membership.parquet"
CAL_PARQ = "/Users/sachoki/Desktop/sachoki-desktop/data/session_calendar_1h.parquet"


def main():
    t0 = time.time()
    ident = json.load(open(CACHE_ART))
    raw = pd.read_parquet(MEMB)
    # tidy layout: kind='population' carries the frozen population, kind='member' the
    # per-claim memberships keyed by the sealed j / claim_id
    pop = raw[raw["kind"] == "population"]
    memb = raw[raw["kind"] == "member"]
    claim_cols = sorted(memb.claim_id.unique())

    # ── the sealed microstructure, classified both ways ───────────────────
    M = pd.read_parquet(D5.OUT_MS, columns=["episode_id", "ticker", "relative_day",
                                            "session_date", "session_position",
                                            "bars_in_session", "ts_et"])
    cal = dict(zip(*(lambda C: (C.session_date, C.calendar_len.astype(int)))(
        pd.read_parquet(CAL_PARQ))))
    per = (M.groupby(["session_date", "ticker", "episode_id", "relative_day"])
             .bars_in_session.first().reset_index())
    fam_modal = (per.groupby(["session_date", "ticker"]).bars_in_session.first()
                    .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    old_valid = per.bars_in_session == per.session_date.map(fam_modal)
    new_valid = per.bars_in_session == per.session_date.map(cal)
    flips = int((old_valid ^ new_valid).sum())

    def frame(mask):
        ok = per[mask][["episode_id", "relative_day"]]
        F = M.merge(ok, on=["episode_id", "relative_day"], how="inner")
        return F.sort_values(["episode_id", "relative_day", "session_position"],
                             kind="stable").reset_index(drop=True)

    Fo, Fc = frame(old_valid), frame(new_valid)
    h = lambda F: hashlib.sha256(pd.util.hash_pandas_object(
        F.drop(columns=["ts_et"]), index=False).values.tobytes()).hexdigest()[:16]
    ho, hc = h(Fo), h(Fc)

    # invariant D on the corrected frame
    t = pd.to_datetime(Fc.ts_et)
    slot = np.floor((t.dt.hour * 60 + t.dt.minute - 570) / 60).astype(int).to_numpy()
    same = ((Fc.episode_id.values[1:] == Fc.episode_id.values[:-1])
            & (Fc.relative_day.values[1:] == Fc.relative_day.values[:-1]))
    posadj = Fc.session_position.values[1:] == Fc.session_position.values[:-1] + 1
    invD = int(((same & posadj) & ((slot[1:] - slot[:-1]) > 1)).sum())

    # ── the forward memberships themselves ────────────────────────────────
    ep_digest = hashlib.sha256("|".join(sorted(pop.episode_id)).encode()).hexdigest()[:16]
    per_claim = {}
    for c in claim_cols:
        g = memb[memb.claim_id == c]
        ids = sorted(g.episode_id)
        per_claim[f"j{int(g.j.iloc[0])}|{c}"] = dict(
            n=len(ids), j=int(g.j.iloc[0]),
            membership_hash=hashlib.sha256("|".join(ids).encode()).hexdigest()[:16])
    # every forward-membership episode must still live in a session-complete window under
    # the corrected rule — that is what "the rules did not move" means operationally
    corrected_eps = set(Fc.episode_id.unique())
    old_eps = set(Fo.episode_id.unique())
    lost = sorted(set(memb.episode_id) - corrected_eps)
    pop_lost = sorted(set(pop.episode_id) - corrected_eps)
    gained_pool = len(corrected_eps - old_eps)

    checks = {
        "cache file digest matches the sealed identity":
            ART.file_digest(MEMB) == ident["cache_file_digest"],
        "episode_id digest matches the sealed identity":
            ep_digest == ident["episode_id_digest"],
        "claim count matches the sealed identity": len(claim_cols) == ident["n_claims_used"],
        "session-validity flips on the T5 microstructure": flips == 0,
        "sealed vs corrected input frames are bit-identical": ho == hc,
        "invariant D on the corrected frame": invD == 0,
        "no forward-membership episode falls outside a corrected complete session":
            len(lost) == 0,
        "no frozen-population episode falls outside a corrected complete session":
            len(pop_lost) == 0,
        "corrected rule admits no episode the old rule excluded": gained_pool == 0,
    }
    ok = all(checks.values())
    body = dict(
        spec_id="T5_FORWARD_MEDOID_IDENTITY_V1",
        result="PASS" if ok else "FAIL",
        question="do the frozen 1H forward rule memberships survive the session-validity "
                 "correction unchanged?",
        answer=("YES — the corrected rule reclassifies no T5 session, the enumeration input "
                "frame is bit-identical, and every episode in the frozen forward membership "
                "still sits inside a complete session." if ok else "SEE FAILURES"),
        provenance="all comparisons are on the SAME sealed T5 microstructure vintage; the "
                   "currently mutable 1H store is never used to regenerate family X — only "
                   "expected_bars(date) is taken, from the sealed calendar",
        sealed_identity={k: ident[k] for k in
                         ("source_population_hash", "claim_order_hash",
                          "block_assignment_hash", "cache_file_digest",
                          "episode_id_digest", "n_claims_used", "n_population")},
        session_validity=dict(sessions=int(len(per)),
                              old_valid=int(old_valid.sum()),
                              corrected_valid=int(new_valid.sum()),
                              flips=flips),
        input_frames=dict(rows_old=int(len(Fo)), rows_corrected=int(len(Fc)),
                          digest_old=ho, digest_corrected=hc,
                          bit_identical=bool(ho == hc),
                          invariant_D=invD),
        forward_memberships=dict(
            file=os.path.basename(MEMB), digest=ART.file_digest(MEMB),
            n_population=int(len(pop)), n_member_rows=int(len(memb)),
            n_claims=len(claim_cols),
            population_episodes_outside_corrected=len(pop_lost),
            per_claim=per_claim,
            episodes_outside_corrected_complete_sessions=len(lost)),
        checks={k: bool(v) for k, v in checks.items()},
        scope=dict(
            unblocks="the SESSION-SEMANTICS reason for the T5 forward hold, and nothing else",
            still_held="SOURCE DATA VERSION — ONE_HOUR_DATA_VERSION_DIVERGENCE_V1 is an "
                       "independent incident and keeps forward outcome finalization, "
                       "promotion and status computation on hold",
            code_environment="prior status unchanged; this artifact makes no statement "
                             "about the code/environment axis"),
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id", "result", "checks", "forward_memberships"),
                 supersede=os.path.exists(OUT))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"\n  frozen population {len(pop):,} · member rows {len(memb):,} · "
          f"claims {len(claim_cols)}")
    for c, v in per_claim.items():
        print(f"    {c[:44]:<44} n={v['n']:>6} · {v['membership_hash']}")
    print(f"\nT5_FORWARD_MEDOID_IDENTITY_V1 · {d} · {body['result']}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
