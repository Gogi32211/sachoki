"""T5_FORWARD_MEMBERSHIP_REPLAY_V1 — extensional equivalence, asserted bitwise.

    reference   canonical scalar evaluator      eval_15m / eval_1h
    candidate   batch / vectorised evaluator    eval_15m_batch / eval_1h_chunked
    data        PRE-CUTOFF ONLY, signal session <= 2026-08-20
    required    exact bitwise membership equality on every (episode x claim) cell

WHY COUNTS ARE NOT ENOUGH

    treated_count_batch == treated_count_canonical

is satisfied by any pair of errors that cancel — one episode wrongly added, one wrongly
dropped. The gate is therefore the XOR of the two bitmaps, reported three ways so a failure
cannot hide in an aggregate:

    n_cell_mismatches     == 0
    n_episode_mismatches  == 0
    n_claim_mismatches    == 0

The output is boolean, so there is no tolerance and allclose() has no place here. On rules with
0.3% prevalence a one-episode deviation is a large relative error, which is exactly why every
claim also carries its own signature: treated counts on both sides, the XOR count, and an
order-free membership digest of each side.

THE DANGEROUS PLACES ARE THE BOUNDARIES

    >  vs  >=          NaN comparison        float32 vs float64
    <  vs  <=          NULL propagation      timestamp timezone
    between()          session boundary      15m opening-hour inclusivity

A vectorised rewrite that turns x >= 0.25 into x > 0.25 changes prevalence measurably on a rare
claim and changes nothing visible in a summary. These rules are token-membership rules rather
than threshold rules, so the live boundary here is the opening-hour completeness rule and the
1H session/day seam; both are exercised by synthetic fixtures AND by the full historical frame.

FEATURE-PIPELINE PARITY IS A SEPARATE TEST, DELIBERATELY

Both evaluators read the SAME materialised frozen X — the sealed parquets, byte-digested here —
never recomputed features. If the replay rebuilt features it would be testing two things at
once and a mismatch would have no attributable source.

    frozen X --> reference --> bitmap A
    frozen X --> candidate --> bitmap B          A == B, exactly

CHUNK SIZE IS AN OPERATIONAL PARAMETER, NOT A SCIENTIFIC ONE

Forward ingestion will change its batch size for entirely mundane reasons. That must not be a
change to the specification, so the invariance is asserted rather than assumed: claim-batches
of 1 / 16 / all, episode-chunks of 1 / 64 / 4096, plus shuffled and reverse-sorted row order —
every one produces the identical bitmap.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                     # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D, t5_forward_eval as EV          # noqa: E402

SPEC   = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
CUTOFF = SPEC["discovery_cutoff"]["last_eligible_historical_signal_session"]
DATA   = os.path.join(D.ROOT, "data")
M15P   = os.path.join(DATA, "t5_15m_opening_hour.parquet")
REPP   = os.path.join(DATA, "t5_15m_cluster_representatives.parquet")
SIGS   = os.path.join(DATA, "t5_forward_replay_signatures.parquet")
OUT    = "T5_FORWARD_MEMBERSHIP_REPLAY_V1.json"

CLAIM_BATCHES  = [1, 16, None]          # None = all claims in one call
EPISODE_CHUNKS = [64, 4096]
SINGLE_CHUNK_SAMPLE = 200               # chunk=1 over the full frame is 114k calls; sampled


class ReplayFailure(RuntimeError):
    pass


# ══════════════════════════════════════════════════════════════════════════
# BITMAPS — a membership dict becomes a boolean matrix over a fixed episode index
# ══════════════════════════════════════════════════════════════════════════
def bitmap(memb, claims, eps_index):
    B = np.zeros((len(eps_index), len(claims)), bool)
    for c, claim in enumerate(claims):
        s = memb.get(claim, set())
        if s:
            rows = [eps_index[e] for e in s if e in eps_index]
            B[rows, c] = True
    return B


def compare(A, B, claims, eps):
    """Three counters, because one aggregate can hide a failure the others cannot."""
    X = A ^ B
    return dict(
        cells_compared=int(A.size),
        cell_mismatches=int(X.sum()),
        episode_mismatches=int(X.any(axis=1).sum()),
        claim_mismatches=int(X.any(axis=0).sum()),
        first_offending_claims=[claims[i] for i in np.flatnonzero(X.any(axis=0))[:5]],
        first_offending_episodes=[eps[i] for i in np.flatnonzero(X.any(axis=1))[:5]])


def signatures(ref, cand, claims, family):
    rows = []
    for c in claims:
        a, b = ref.get(c, set()), cand.get(c, set())
        rows.append(dict(
            family=family, claim_id=c,
            n_treated_reference=len(a), n_treated_candidate=len(b),
            xor_mismatch_count=len(a ^ b),
            membership_digest_reference=EV.membership_digest(a),
            membership_digest_candidate=EV.membership_digest(b),
            passed=bool(a == b)))
    return rows


# ══════════════════════════════════════════════════════════════════════════
def run_15m(rep, report):
    print("  15m · loading frozen X", flush=True)
    M = pd.read_parquet(M15P, columns=["episode_id", "t5_date", "pos", "token_set"])
    if str(M.t5_date.max()) > CUTOFF:
        raise ReplayFailure(f"post-cutoff row in the replay frame: {M.t5_date.max()} > {CUTOFF}")
    report["pre_cutoff_only"] = dict(family="15M", max_signal_session=str(M.t5_date.max()),
                                     cutoff=CUTOFF, ok=True)
    M = M.drop(columns=["t5_date"])
    claims = rep.claim_id.tolist()
    eps = sorted(pd.unique(M.episode_id).tolist())
    ix = {e: i for i, e in enumerate(eps)}

    t = time.time()
    ref = {c: EV.eval_15m(c, M) for c in claims}
    print(f"       reference (scalar, {len(claims)} rules) {time.time()-t:.0f}s", flush=True)
    t = time.time()
    cand = EV.eval_15m_batch(claims, M)
    print(f"       candidate (batch)                 {time.time()-t:.0f}s", flush=True)

    A, B = bitmap(ref, claims, ix), bitmap(cand, claims, ix)
    cmp = compare(A, B, claims, eps)
    cmp["episodes_replayed"] = len(eps)
    cmp["claims"] = len(claims)
    report["equivalence_15m"] = cmp
    sig = signatures(ref, cand, claims, "15M")

    # ── invariance: claim-batch size ───────────────────────────────────────
    inv = {}
    for b in CLAIM_BATCHES:
        size = len(claims) if b is None else b
        got = {}
        for i in range(0, len(claims), size):
            got.update(EV.eval_15m_batch(claims[i:i + size], M))
        inv[f"claim_batch_{size}"] = int((bitmap(got, claims, ix) ^ B).sum())
        print(f"       claim batch {size:>4} · mismatches {inv[f'claim_batch_{size}']}",
              flush=True)

    # ── invariance: episode-chunk size ─────────────────────────────────────
    for ch in EPISODE_CHUNKS:
        t = time.time()
        got = EV.eval_15m_chunked(claims, M, ch)
        inv[f"episode_chunk_{ch}"] = int((bitmap(got, claims, ix) ^ B).sum())
        print(f"       episode chunk {ch:>5} · mismatches {inv[f'episode_chunk_{ch}']} "
              f"· {time.time()-t:.0f}s", flush=True)

    # chunk = 1 over a sample: the full frame would be 114k evaluator calls
    g = np.random.default_rng(4242)
    pick = set(np.array(eps)[g.choice(len(eps), SINGLE_CHUNK_SAMPLE, replace=False)].tolist())
    Msub = M[M.episode_id.isin(pick)]
    got1 = EV.eval_15m_chunked(claims, Msub, 1)
    sub_ix = {e: i for i, e in enumerate(sorted(pick))}
    want1 = {c: (ref[c] & pick) for c in claims}
    inv["episode_chunk_1_sampled"] = int(
        (bitmap(got1, claims, sub_ix) ^ bitmap(want1, claims, sub_ix)).sum())
    inv["episode_chunk_1_n_sampled"] = SINGLE_CHUNK_SAMPLE
    inv["episode_chunk_1_why_sampled"] = (
        f"chunk=1 over the full frame is {len(eps):,} evaluator calls; a "
        f"{SINGLE_CHUNK_SAMPLE}-episode sample is reported rather than silently skipped")
    print(f"       episode chunk     1 · sampled {SINGLE_CHUNK_SAMPLE} · "
          f"mismatches {inv['episode_chunk_1_sampled']}", flush=True)

    # ── invariance: row order ──────────────────────────────────────────────
    for name, Mo in (("shuffled", M.sample(frac=1.0, random_state=99).reset_index(drop=True)),
                     ("reverse_sorted",
                      M.sort_values(["episode_id", "pos"], ascending=False)
                       .reset_index(drop=True))):
        inv[f"row_order_{name}"] = int(
            (bitmap(EV.eval_15m_batch(claims, Mo), claims, ix) ^ B).sum())
        print(f"       row order {name:<15} · mismatches {inv[f'row_order_{name}']}", flush=True)

    report["invariance_15m"] = inv
    return sig, cmp, inv


def run_1h(report):
    print("  1H · loading frozen X", flush=True)
    M = pd.read_parquet(D.OUT_MS, columns=["episode_id", "ticker", "t5_date", "relative_day",
                                           "session_date", "session_position",
                                           "bars_in_session", "token_set"])
    if str(M.t5_date.max()) > CUTOFF:
        raise ReplayFailure(f"post-cutoff row in the replay frame: {M.t5_date.max()} > {CUTOFF}")
    report["pre_cutoff_only_1h"] = dict(family="1H", max_signal_session=str(M.t5_date.max()),
                                        cutoff=CUTOFF, ok=True)
    M = M.drop(columns=["t5_date"])
    COMP = json.load(open("T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1.json"))
    claims = [r["representative"] for r in COMP["per_cluster"]]
    eps = sorted(pd.unique(M.episode_id).tolist())
    ix = {e: i for i, e in enumerate(eps)}

    # ── the session-length table: derivation vs frozen input ───────────────
    tab = EV.session_len_table(M)
    same_rows = len(EV.session_complete_1h(M)) == len(EV.session_complete_1h(M, tab))
    report["session_length_table"] = dict(
        n_sessions=int(len(tab)),
        frozen_table_reproduces_derivation=bool(same_rows),
        why="derived from the frame, the modal session length makes 1H membership SESSION-local "
            "rather than episode-local: evaluate a subset of episodes and the mode could move. "
            "Supplied as a frozen input it is episode-local by construction, which is what "
            "makes chunked ingestion safe. The derivation is empirically robust — a 40-ticker "
            "subset reproduced it on 959/959 shared sessions — but robustness is not "
            "invariance, and the forward path is chunked.",
        tie_rule="ties resolve to the smallest modal value; fixed so it cannot be re-decided",
        unknown_session="raises UnknownSession rather than silently excluding the whole day",
        digest=hashlib.sha256("|".join(f"{k}:{v}" for k, v in tab.items()).encode()
                              ).hexdigest()[:16])

    t = time.time()
    ref = {c: EV.eval_1h(c, M, tab) for c in claims}
    print(f"       reference (scalar, {len(claims)} rules) {time.time()-t:.0f}s", flush=True)

    A = bitmap(ref, claims, ix)
    inv, cand = {}, None
    for ch in EPISODE_CHUNKS:
        t = time.time()
        got = EV.eval_1h_chunked(claims, M, ch, tab)
        cand = cand or got
        inv[f"episode_chunk_{ch}"] = int((bitmap(got, claims, ix) ^ A).sum())
        print(f"       episode chunk {ch:>5} · mismatches {inv[f'episode_chunk_{ch}']} "
              f"· {time.time()-t:.0f}s", flush=True)

    # the same chunking WITHOUT the frozen table — measured, not assumed
    drift = {}
    for ch in EPISODE_CHUNKS:
        got = {c: set() for c in claims}
        for part in EV.episode_chunks(M, ch):
            for c in claims:
                got[c] |= EV.eval_1h(c, part, None)
        drift[f"episode_chunk_{ch}"] = int((bitmap(got, claims, ix) ^ A).sum())
    report["session_length_table"]["chunked_without_frozen_table_mismatches"] = drift
    print(f"       chunked WITHOUT the frozen table · mismatches {drift}", flush=True)

    for name, Mo in (("shuffled", M.sample(frac=1.0, random_state=99).reset_index(drop=True)),):
        inv[f"row_order_{name}"] = int(
            (bitmap({c: EV.eval_1h(c, Mo, tab) for c in claims}, claims, ix) ^ A).sum())
        print(f"       row order {name:<15} · mismatches {inv[f'row_order_{name}']}", flush=True)

    cmp = compare(A, bitmap(cand, claims, ix), claims, eps)
    cmp["episodes_replayed"] = len(eps)
    cmp["claims"] = len(claims)
    report["equivalence_1h"] = cmp
    report["invariance_1h"] = inv
    return signatures(ref, cand, claims, "1H"), cmp, inv, tab


# ══════════════════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    ev = EV.evaluator_hash()
    print(f"T5_FORWARD_MEMBERSHIP_REPLAY_V1 · evaluator {ev}", flush=True)

    report = dict(
        spec_id="T5_FORWARD_MEMBERSHIP_REPLAY_V1",
        reference_implementation="eval_15m / eval_1h — canonical scalar. NEVER DELETED: it "
                                 "stays the frozen oracle so any future optimisation is "
                                 "checked against an implementation that has not moved.",
        candidate_implementation="eval_15m_batch / eval_1h_chunked — production, vectorised",
        licence="the candidate is admissible ONLY while it is extensionally equivalent to the "
                "reference; a faster path is an optimisation, never new semantics",
        equality="EXACT BITWISE, at (episode x claim) grain. Membership is boolean, so no "
                 "tolerance and no allclose() is involved.",
        why_not_counts="treated_count equality is satisfied by two errors that cancel",
        feature_pipeline_parity=dict(
            tested_separately=True,
            both_read="the same materialised frozen X, never recomputed features",
            why="rebuilding features inside the replay would test the pipeline and the "
                "evaluator at once, and a mismatch would have no attributable source",
            input_digests=dict(m15_opening_hour=ART.file_digest(M15P),
                               h1_microstructure=ART.file_digest(D.OUT_MS),
                               representatives=ART.file_digest(REPP))),
        evaluator_hash=ev, evaluator_file="t5_forward_eval.py")

    rep = pd.read_parquet(REPP)
    sig15, cmp15, inv15 = run_15m(rep, report)
    sig1h, cmp1h, inv1h, _ = run_1h(report)

    S = pd.DataFrame(sig15 + sig1h)
    S.to_parquet(SIGS, index=False)
    report["per_claim_signatures"] = dict(
        file=os.path.basename(SIGS), rows=int(len(S)),
        columns=["family", "claim_id", "n_treated_reference", "n_treated_candidate",
                 "xor_mismatch_count", "membership_digest_reference",
                 "membership_digest_candidate", "passed"],
        n_failed=int((~S.passed).sum()),
        digest=ART.file_digest(SIGS),
        why="on a 0.3%-prevalence rule a one-episode deviation is a large relative error, so "
            "every claim is audited individually rather than through a pooled count")

    inv_bad = {k: v for k, v in list(inv15.items()) + list(inv1h.items())
               if isinstance(v, int) and v and not k.endswith("n_sampled")}
    passed = (cmp15["cell_mismatches"] == 0 and cmp1h["cell_mismatches"] == 0
              and int((~S.passed).sum()) == 0 and not inv_bad)
    report["invariance_failures"] = inv_bad
    report["result"] = "PASS" if passed else "FAIL"

    n_cells = cmp15["cells_compared"] + cmp1h["cells_compared"]
    print(f"\n  episodes_replayed 15m   {cmp15['episodes_replayed']:>9,}")
    print(f"  episodes_replayed 1H    {cmp1h['episodes_replayed']:>9,}")
    print(f"  claims                  {cmp15['claims'] + cmp1h['claims']:>9,}")
    print(f"  cells_compared          {n_cells:>9,}\n")
    print(f"  cell_mismatches         {cmp15['cell_mismatches']+cmp1h['cell_mismatches']:>9,}")
    print(f"  episode_mismatches      "
          f"{cmp15['episode_mismatches']+cmp1h['episode_mismatches']:>9,}")
    print(f"  claim_mismatches        {cmp15['claim_mismatches']+cmp1h['claim_mismatches']:>9,}")
    print(f"\n  15m mismatches          {cmp15['cell_mismatches']:>9,}")
    print(f"  1H  mismatches          {cmp1h['cell_mismatches']:>9,}")
    print(f"  per-claim failures      {int((~S.passed).sum()):>9,}")
    print(f"  invariance failures     {len(inv_bad):>9,}  {inv_bad if inv_bad else ''}")
    print(f"\n  {report['result']} = exact equality")

    dig = ART.seal(report, OUT, required=("spec_id", "result", "equivalence_15m",
                                          "equivalence_1h", "per_claim_signatures"))
    print(f"\n  {OUT} · {dig}")
    print(f"  {(time.time()-t0)/60:.1f} min")
    if not passed:
        raise ReplayFailure("membership replay FAILED — the evaluator must not be frozen")


if __name__ == "__main__":
    main()
