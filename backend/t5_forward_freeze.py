"""Freeze the evaluator, the occurrence schema, and the lock-manifest rule. Three artifacts.

All three are frozen BEFORE any post-cutoff X exists, so none of their edge cases can be
settled while looking at forward data.

THE LOCK MANIFEST NEEDS TIE-BREAKERS NOW

Around the 20,000th episode there will be ties: several episodes complete maturity on the same
session. Without a declared ordering, which episode is #20,000 becomes a choice made at the
moment it matters. The ordering and its tie-breakers are therefore fixed here, and the
manifest itself is created only when the counter first reaches the target.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D, t5_forward_eval as EV          # noqa: E402
import t5_15m_par_qualify as PQ                                        # noqa: E402

SPEC = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
DATA = os.path.join(D.ROOT, "data")
EVAL_OUT = "T5_FORWARD_MEMBERSHIP_EVALUATOR_V1.json"
REPLAY_OUT = "T5_FORWARD_MEMBERSHIP_REPLAY_V1.json"
OCC_OUT  = "T5_FORWARD_OCCURRENCE_SCHEMA_V1.json"
LOCK_OUT = "T5_FORWARD_LOCK_MANIFEST_V1.json"


def replay_15m():
    """171 rules against the sealed overlap-restricted membership. Exact, or it fails."""
    REP = pd.read_parquet(os.path.join(DATA, "t5_15m_cluster_representatives.parquet"))
    z = np.load(PQ.BUNDLE)
    eidx, seg_ptr, csp = z["eidx"], z["seg_ptr"], z["claim_seg_ptr"]
    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids = P.episode_id.to_numpy(); blk = P.block.to_numpy()
    pop = set(ids)
    bsz = pd.Series(blk).value_counts().to_dict()
    M = pd.read_parquet(os.path.join(DATA, "t5_15m_opening_hour.parquet"),
                        columns=["episode_id", "pos", "token_set"])

    claims = REP.claim_id.tolist()
    batch = EV.eval_15m_batch(claims, M)
    bl = dict(zip(ids, blk))
    mism = []
    for r in REP.itertuples():
        raw = batch[r.claim_id] & pop
        cnt = {}
        for e in raw:
            cnt[bl[e]] = cnt.get(bl[e], 0) + 1
        keep = {b for b, n in cnt.items() if 0 < n < bsz[b]}
        got = {e for e in raw if bl[e] in keep}
        a, b_ = csp[r.j], csp[r.j + 1]
        want = set(ids[eidx[seg_ptr[a]:seg_ptr[b_]]])
        if got != want:
            mism.append(dict(claim=r.claim_id, missing=len(want - got), extra=len(got - want)))
    return len(REP), mism


def replay_1h():
    """4 rules against the sealed cache. Necessary condition: every sealed member reproduced."""
    C = pd.read_parquet(os.path.join(DATA, "t5_1h_survivor_membership.parquet"))
    COMP = json.load(open("T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1.json"))
    reps = [r["representative"] for r in COMP["per_cluster"]]
    M = pd.read_parquet(D.OUT_MS, columns=["episode_id", "ticker", "relative_day",
                                           "session_date", "session_position",
                                           "bars_in_session", "token_set"])
    tab = EV.session_len_table(M)
    out = []
    for claim in reps:
        want = set(C[(C.kind == "member") & (C.claim_id == claim)].episode_id)
        got = EV.eval_1h(claim, M, tab)
        out.append(dict(claim=claim, sealed=len(want), evaluated=len(got),
                        missing=len(want - got), extra_before_overlap=len(got - want)))
    return out


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    ev_hash = EV.evaluator_hash()
    print(f"T5_FORWARD_MEMBERSHIP_EVALUATOR_V1 · evaluator hash {ev_hash}", flush=True)

    props, members = EV.run_property_tests()
    for k, v in props.items():
        print(f"    {'✓' if v else '✗'} {k}")
    if not all(props.values()):
        raise RuntimeError("property tests failed")

    # ── the reference/candidate equivalence gate is NOT re-derived here ────
    # A weaker restatement of it inside the freeze would be the hole it exists to close, so the
    # freeze consumes T5_FORWARD_MEMBERSHIP_REPLAY_V1 and refuses if that artifact is absent,
    # failing, or was produced by a DIFFERENT evaluator than the one about to be sealed.
    if not os.path.exists(REPLAY_OUT):
        raise RuntimeError(f"{REPLAY_OUT} missing — run t5_forward_replay.py before freezing")
    RP = json.load(open(REPLAY_OUT))
    if RP["evaluator_hash"] != ev_hash:
        raise RuntimeError(f"replay ran against evaluator {RP['evaluator_hash']}, freezing "
                           f"{ev_hash} — the evaluator changed after its replay")
    if RP["result"] != "PASS":
        raise RuntimeError(f"membership replay result is {RP['result']} — refusing to freeze")
    cells = RP["equivalence_15m"]["cells_compared"] + RP["equivalence_1h"]["cells_compared"]
    print(f"    ✓ reference == candidate · {cells:,} cells · 0 mismatches "
          f"· {REPLAY_OUT} {ART.file_digest(REPLAY_OUT)}")

    n15, mism15 = replay_15m()
    print(f"    {'✓' if not mism15 else '✗'} 15m historical replay · {n15} rules · "
          f"mismatches {len(mism15)}")
    if mism15[:3]:
        for m in mism15[:3]:
            print(f"        {m}")

    r1h = replay_1h()
    miss1h = sum(r["missing"] for r in r1h)
    print(f"    {'✓' if miss1h == 0 else '✗'} 1H historical replay · 4 rules · "
          f"sealed members not reproduced {miss1h}")
    for r in r1h:
        print(f"        {r['claim']:<32} sealed {r['sealed']:>6,} · evaluated "
              f"{r['evaluated']:>6,} · missing {r['missing']} · "
              f"extra(pre-overlap) {r['extra_before_overlap']:,}")

    passed = all(props.values()) and not mism15 and miss1h == 0
    if not passed:
        raise RuntimeError("evaluator freeze REFUSED — a replay or property test failed")

    ART.seal(dict(
        spec_id="T5_FORWARD_MEMBERSHIP_EVALUATOR_V1", status="FROZEN",
        parent_spec="T5_FORWARD_VALIDATION_V1",
        parent_digest=ART.file_digest("T5_FORWARD_VALIDATION_V1.json"),
        frozen_before="any post-cutoff X exists",
        why_now="writing the evaluator after seeing forward X is not outcome leakage but is a "
                "real discretionary channel: boundary inclusivity, missing-value handling, "
                "timestamp alignment, session-validity edge cases and tie semantics would all "
                "be settled while looking at the data the rules must be applied to",
        inputs_allowed=["historical fixtures <= 2026-08-20", "synthetic fixtures",
                        "frozen rule definitions"],
        inputs_forbidden=["any post-cutoff episode", "any post-cutoff feature distribution",
                          "all outcomes"],
        evaluator_hash=ev_hash, evaluator_file="t5_forward_eval.py",
        property_tests=props, synthetic_members=members,
        historical_replay=dict(
            m15=dict(rules=n15, mismatches=len(mism15), exact=True,
                     method="raw rule evaluation, restricted to the sealed population and to "
                            "overlap blocks recomputed from the sealed block labels, compared "
                            "to the sealed membership from the engine bundle"),
            h1=dict(rules=len(r1h), sealed_members_not_reproduced=miss1h, detail=r1h,
                    method="necessary condition only: every sealed member must be reproduced. "
                           "The 1H block assignment was never persisted separately, so the "
                           "overlap restriction cannot be re-applied and the extra episodes "
                           "are REPORTED, not asserted to be zero.",
                    weaker_than_15m=True),
            asymmetry_is_stated="the two families are not replayed with equal strength"),
        reference_vs_candidate=dict(
            spec="T5_FORWARD_MEMBERSHIP_REPLAY_V1",
            digest=ART.file_digest(REPLAY_OUT),
            result=RP["result"], cells_compared=cells,
            cell_mismatches=RP["equivalence_15m"]["cell_mismatches"]
            + RP["equivalence_1h"]["cell_mismatches"],
            reference="eval_15m / eval_1h — canonical scalar, RETAINED as the frozen oracle",
            candidate="eval_15m_batch / eval_1h_chunked — production",
            equality="exact bitwise at (episode x claim) grain; no tolerance, membership is "
                     "boolean",
            not_re_derived_here="restating this gate more weakly inside the freeze would be "
                                "the very hole it exists to close",
            session_length_table=RP["session_length_table"],
            invariance=dict(claims_15m=RP["invariance_15m"], claims_1h=RP["invariance_1h"],
                            why="ingestion batch size is an operational parameter and must "
                                "never become a scientific one")),
        implementations=dict(
            reference_never_deleted="eval_15m / eval_1h stay in the module unchanged, so any "
                                    "future optimisation is checked against an oracle that has "
                                    "not moved",
            production="eval_15m_batch / eval_1h_chunked",
            licence="extensional equivalence on every admissible input")),
        EVAL_OUT, required=("spec_id", "evaluator_hash", "historical_replay",
                            "reference_vs_candidate"))

    ART.seal(dict(
        spec_id="T5_FORWARD_OCCURRENCE_SCHEMA_V1", status="FROZEN",
        grain="one row per (episode_id, claim_id) for every forward episode evaluated",
        columns=dict(
            episode_id="string", claim_id="string",
            is_treated="bool — the rule fired on this episode",
            is_control="bool — the episode is in an overlap block of this claim and is not "
                       "treated",
            support_block_id="int — T5 date x liq LOW/HIGH x vol LOW/HIGH, cut within the "
                             "forward window",
            evaluator_hash="string — which executable semantics produced this row",
            data_version="string", rule_digest="string"),
        why_long_form="175 members as a wide table would make support and overlap audits "
                      "awkward; long form counts treated and control with a filtered COUNT",
        immutability="a row is written once. A changed pipeline is a new data_version, never "
                     "a correction of an existing row.",
        support_counts="COUNT(*) FILTER (WHERE is_treated) and FILTER (WHERE is_control), on "
                       "maturity-complete episodes only"),
        OCC_OUT, required=("spec_id", "columns"))

    ART.seal(dict(
        spec_id="T5_FORWARD_LOCK_MANIFEST_V1", status="FROZEN_SPECIFICATION_ONLY",
        manifest_not_yet_created="the manifest is written when mature_count first reaches "
                                 "20,000; only the rule is frozen now",
        eligible_for_lock=["signal_ts > 2026-08-20",
                           "maturity_complete",
                           "data_quality_status == PASS"],
        ordering=["maturity_completion_session ASC", "signal_session ASC",
                  "episode_id ASC"],
        why_tie_breakers="around the 20,000th episode several will complete maturity on the "
                         "same session. Without a declared ordering, which one is #20,000 "
                         "becomes a choice made at the moment it matters.",
        locked_set="the first 20,000 rows under the frozen ordering",
        excluded="episode 20,001 onward is not in the validation run, whatever it looks like",
        manifest_columns=["episode_id", "signal_session", "maturity_completion_session",
                          "membership_hash", "evaluator_hash", "data_version_hash",
                          "calendar_hash"],
        lock_hash="sha256 over the ordered episode_id list plus the four hashes above",
        after_lock=["support eligibility", "k_eff", "frozen primary tests",
                    "max-Z correction over eligible members only",
                    "STATISTICAL_PASS / FAIL", "directional consistency"],
        one_deterministic_run="the input manifest hash is known before the test is run"),
        LOCK_OUT, required=("spec_id", "ordering", "eligible_for_lock"))

    print(f"\n  FROZEN")
    print(f"    {EVAL_OUT} · {ART.file_digest(EVAL_OUT)}")
    print(f"    {OCC_OUT} · {ART.file_digest(OCC_OUT)}")
    print(f"    {LOCK_OUT} · {ART.file_digest(LOCK_OUT)}")
    print(f"  {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
