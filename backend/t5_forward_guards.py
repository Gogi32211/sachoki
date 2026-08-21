"""T5_FORWARD_RUNTIME_INVARIANTS_V1 — checked on EVERY forward ingest, before any commit.

STATUS: DRAFT. Imported by the accrual process; nothing here computes a statistic.

Four invariants. Each one raises rather than warns, because a warning in an unattended nightly
job is a silent failure with extra steps.

    1  the session-length table's COMMITTED rows are unchanged
    2  evaluator_hash == the sealed evaluator
    3  unknown_session_count == 0 before a ledger row is committed
    4  occurrence rows are immutable once written

WHY INVARIANT 1 IS NOT A WHOLE-FILE DIGEST

The session-length table is APPEND-ONLY and grows every forward session, so its file digest
changes legitimately and a fixed digest would fail on the first correct run — a guard that
cries wolf gets deleted, which is worse than no guard. The invariant that actually holds is:

    every row committed earlier is still present with the SAME value

so the check is a prefix/superset check plus a digest over the historical origin rows, which
never change. New rows are expected; changed rows are drift.
"""
from __future__ import annotations
import hashlib, json, os                                              # noqa: E402
import pandas as pd                                                   # noqa: E402


class InvariantViolation(RuntimeError):
    pass


def _digest(pairs):
    return hashlib.sha256(
        "|".join(f"{k}:{v}" for k, v in sorted(pairs)).encode()).hexdigest()[:16]


# ── 1 ─────────────────────────────────────────────────────────────────────
def assert_session_length_stable(table_path, sealed_historical_digest, prior_committed=None):
    """Committed rows are immutable; new rows are expected.

    prior_committed: {session_date: expected_bars} as of the last ledger row, or None on the
    first run. Passing it makes the check cover forward rows too, not just historical ones."""
    T = pd.read_parquet(table_path)
    hist = [(str(r.session_date), int(r.expected_bars))
            for r in T[T.origin == "HISTORICAL"].itertuples()]
    got = _digest(hist)
    if got != sealed_historical_digest:
        raise InvariantViolation(
            f"session-length table: the HISTORICAL rows changed ({got} != "
            f"{sealed_historical_digest}). These are frozen pre-cutoff values; nothing in the "
            f"forward path may touch them.")
    if prior_committed:
        now = {str(r.session_date): int(r.expected_bars) for r in T.itertuples()}
        changed = {d: (v, now.get(d)) for d, v in prior_committed.items() if now.get(d) != v}
        if changed:
            raise InvariantViolation(
                f"session-length table: {len(changed)} previously committed row(s) changed, "
                f"first {list(changed.items())[:3]}. A committed session length is immutable — "
                f"a disagreement means an earlier commit ran on incomplete data.")
        missing = sorted(set(prior_committed) - set(now))
        if missing:
            raise InvariantViolation(f"session-length rows disappeared: {missing[:5]}")
    return got, int(len(T))


# ── 2 ─────────────────────────────────────────────────────────────────────
def assert_evaluator(current_hash, sealed_artifact="T5_FORWARD_MEMBERSHIP_EVALUATOR_V1.json"):
    """The machine that evaluates forward X must be the machine that was frozen and replayed.

    Not 'a compatible version'. The whole point of the freeze is that boundary semantics were
    settled before the data arrived; a changed file means they may have been settled after."""
    sealed = json.load(open(sealed_artifact))["evaluator_hash"]
    if current_hash != sealed:
        raise InvariantViolation(
            f"evaluator hash {current_hash} != sealed {sealed}. t5_forward_eval.py changed "
            f"after the freeze. Re-run t5_forward_replay.py and re-freeze deliberately, as a "
            f"recorded protocol amendment — do not proceed with the changed file.")
    return sealed


# ── 3 ─────────────────────────────────────────────────────────────────────
def assert_sessions_known(session_dates, table_path):
    """No ledger row is committed while any session in the batch lacks an established length.

    The alternative — evaluate what we can and hold the rest — would let the ledger record a
    count that silently excludes sessions, and the exclusion would be invisible downstream."""
    known = set(pd.read_parquet(table_path).session_date.astype(str))
    unknown = sorted(set(map(str, session_dates)) - known)
    if unknown:
        raise InvariantViolation(
            f"unknown_session_count = {len(unknown)} (first {unknown[:3]}). Establish the "
            f"session length for every session in the batch BEFORE evaluating membership. "
            f"Deriving it from the rows in hand is the frame-dependence the replay caught.")
    return 0


# ── 4 ─────────────────────────────────────────────────────────────────────
def assert_occurrence_append_only(occ_path, new_rows):
    """One row per (episode_id, claim_id), written once.

    A changed pipeline is a new data_version, never a correction of an existing row: recomputing
    an old episode's membership under a newer pipeline would make the forward sample partly
    retrospective."""
    if not os.path.exists(occ_path):
        return 0
    O = pd.read_parquet(occ_path, columns=["episode_id", "claim_id"])
    have = set(map(tuple, O.to_numpy()))
    dup = [t for t in map(tuple, new_rows[["episode_id", "claim_id"]].to_numpy()) if t in have]
    if dup:
        raise InvariantViolation(
            f"{len(dup)} occurrence row(s) already written, first {dup[:3]}. Occurrence rows "
            f"are immutable. If the pipeline changed, write a new data_version; do not rewrite "
            f"history.")
    return len(have)


# ══════════════════════════════════════════════════════════════════════════
def check_all(*, table_path, sealed_historical_digest, evaluator_hash, session_dates,
              occ_path, new_rows, prior_committed=None):
    """Every invariant, before anything is committed. Order matters: identity first, then
    completeness, then immutability."""
    ev = assert_evaluator(evaluator_hash)
    dig, n = assert_session_length_stable(table_path, sealed_historical_digest, prior_committed)
    unk = assert_sessions_known(session_dates, table_path)
    prior = assert_occurrence_append_only(occ_path, new_rows)
    return dict(evaluator_hash=ev, session_length_historical_digest=dig,
                session_length_rows=n, unknown_session_count=unk,
                occurrence_rows_before=prior, all_passed=True)
