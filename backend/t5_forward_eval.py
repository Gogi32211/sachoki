"""T5_FORWARD_MEMBERSHIP_EVALUATOR_V1 — frozen BEFORE any post-cutoff X is seen.

    inputs allowed    historical fixtures <= 2026-08-20, synthetic fixtures, frozen rules
    inputs forbidden  any post-cutoff episode, any post-cutoff feature distribution,
                      all outcomes

WHY THIS IS FROZEN NOW RATHER THAN WHEN THE FIRST FORWARD BAR ARRIVES

Writing the evaluator after seeing forward X is not outcome leakage, but it is a real
discretionary channel: boundary inclusivity (> versus >=), missing-value handling, timestamp
alignment, session-validity edge cases and tie semantics would all be settled while looking at
the very data the rules must be applied to. Freezing the implementation and its tests first
closes that channel. The 2026-08-21 episode then simply passes through a machine that already
exists.

THE STRONGEST TEST IS HISTORICAL REPLAY

A fresh evaluator that agrees with a synthetic fixture proves little. What matters is that it
reproduces the memberships that actually produced the sealed family digest:

    historical_membership_replay_mismatch == 0

The two families are not replayed with equal strength, and that asymmetry is stated rather
than averaged away:

    15m (171 rules)  full replay against the sealed OVERLAP-RESTRICTED membership, which is
                     reconstructible from the engine bundle and the sealed block labels
    1H  (4 rules)    the sealed cache is also overlap-restricted, but the 1H block assignment
                     was never persisted separately. So the replay asserts the necessary
                     condition it can prove: every sealed member is reproduced by the
                     evaluator. The extra episodes are reported, not asserted to be zero,
                     because they are the overlap restriction this test cannot re-apply.

DETERMINISM IS A PROPERTY, NOT A HOPE

Same X and same rule hash must give the same bitmap, row order must not matter, and no outcome
column can change it. Those three are tested as properties rather than assumed.

TWO IMPLEMENTATIONS, ONE SEMANTICS

    REFERENCE   eval_15m / eval_1h        scalar, literal, slow. Never deleted. The oracle.
    PRODUCTION  eval_15m_batch / chunked  fast. Legitimate only while it is extensionally
                                          equivalent to the reference on every admissible input.

T5_FORWARD_MEMBERSHIP_REPLAY_V1 asserts that equivalence BITWISE at (episode x claim) grain.
Equal treated counts are not sufficient: two errors can cancel.

THE 1H SESSION-LENGTH TABLE IS AN INPUT, NOT A DERIVATION

session_complete_1h originally took the modal bar count across whatever tickers happened to be
in the frame. That makes 1H membership session-local rather than episode-local: evaluate a
subset of episodes and the mode could move, changing which rows are 'complete'. It is
empirically robust (a 40-ticker subset reproduces the full modal table on 959/959 shared
sessions) but robustness is not invariance, and forward ingestion arrives in chunks.

So the table is now an explicit argument. Passing it makes the evaluator episode-local BY
CONSTRUCTION; omitting it reproduces the historical derivation exactly. An unknown session
date raises rather than silently excluding every row of it.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_dna as D                                                     # noqa: E402

ET_POS = {"09:30": 1, "09:45": 2, "10:00": 3, "10:15": 4}


# ══════════════════════════════════════════════════════════════════════════
# THE EVALUATOR — pure functions over X. No outcome, no state, no I/O.
# ══════════════════════════════════════════════════════════════════════════
def parse_rule(claim_id):
    slot, length, seq = claim_id.split("|")
    return dict(slot=slot, length=int(length), tokens=seq.split("→"))


class UnknownSession(RuntimeError):
    pass


def session_len_table(M):
    """The exchange's session length per date: the modal bar count across all tickers that
    traded it. An early close is a complete session with fewer bars; a ticker with 2 bars on a
    7-bar day is a gap. Ties resolve to the SMALLEST modal value (pandas .mode() sorts
    ascending), which is fixed here so it can never be re-decided later."""
    return (M.groupby(["session_date", "ticker"])["bars_in_session"].first()
            .groupby("session_date").agg(lambda s: s.mode().iloc[0]))


def session_complete_1h(M, session_len=None):
    """Rows whose session is complete. session_len=None derives the table from M, which is the
    historical behaviour and is session-local; passing a frozen table makes the evaluator
    episode-local and therefore chunk-invariant by construction."""
    if session_len is None:
        modal = session_len_table(M)
    else:
        modal = session_len
        miss = sorted(set(M["session_date"]) - set(modal.index))
        if miss:
            raise UnknownSession(
                f"{len(miss)} session date(s) absent from the frozen session-length table, "
                f"first {miss[:3]}. Refusing to treat them as incomplete: a missing row in the "
                f"table is a stale table, not an incomplete market session.")
    return M[M["bars_in_session"] == M["session_date"].map(modal)]


def eval_1h(claim_id, M, session_len=None):
    """REFERENCE implementation. Episodes where the token sequence occurs on ADJACENT observed
    bars inside the declared window. Membership is 'exists t', evaluated at episode grain."""
    r = parse_rule(claim_id)
    fam, toks, L = r["slot"], r["tokens"], r["length"]
    M = session_complete_1h(M, session_len)
    M = M.sort_values(["episode_id", "relative_day", "session_position"],
                      key=lambda s: s.map({"PREV_DAY": 0, "T5_DAY": 1})
                      if s.name == "relative_day" else s).reset_index(drop=True)
    ep = pd.factorize(M.episode_id)[0]
    isT5 = (M.relative_day == "T5_DAY").to_numpy()
    pos = M.session_position.to_numpy()
    has = [M.token_set.str.split().apply(lambda s, t=t: t in s).to_numpy() for t in toks]
    n = len(M)
    if n < L:
        return set()
    idx = np.arange(n - L + 1)
    ok = np.ones(len(idx), bool)
    for j in range(L):
        ok &= ep[idx + j] == ep[idx]
    if fam == "T5_INTRADAY":
        for j in range(L):
            ok &= isT5[idx + j]
        for j in range(L - 1):
            ok &= pos[idx + j + 1] == pos[idx + j] + 1
    elif fam == "PREV_INTRADAY":
        for j in range(L):
            ok &= ~isT5[idx + j]
        for j in range(L - 1):
            ok &= pos[idx + j + 1] == pos[idx + j] + 1
    elif fam == "CROSS_DAY":
        # the seam only: the previous session's LAST bar into the T5 session's FIRST
        seam = np.zeros(len(idx), bool)
        for j in range(L - 1):
            seam |= (~isT5[idx + j]) & isT5[idx + j + 1] & (pos[idx + j + 1] == 1)
        ok &= seam
    else:
        raise ValueError(f"unknown 1H family {fam}")
    for j in range(L):
        ok &= has[j][idx + j]
    return set(M.episode_id.to_numpy()[idx[ok]])


def eval_15m(claim_id, M):
    """REFERENCE implementation. Clock-anchored positions: M1..M4 are 09:30/09:45/10:00/10:15
    ET, and an episode qualifies only with a COMPLETE opening hour — four bars at the four
    clock times. Deliberately literal and slow. It is the oracle every optimisation is checked
    against, so it is never deleted and never 'improved'."""
    r = parse_rule(claim_id)
    a, b = r["slot"].split("→")
    ta, tb = r["tokens"]
    pa, pb = int(a[1]), int(b[1])
    per = M.groupby("episode_id").pos.nunique()
    complete = set(per.index[per == 4])
    M = M[M.episode_id.isin(complete)]
    A = M[M.pos == pa]
    B = M[M.pos == pb]
    sa = set(A.episode_id[A.token_set.str.split().apply(lambda s: ta in s)])
    sb = set(B.episode_id[B.token_set.str.split().apply(lambda s: tb in s)])
    return sa & sb


def eval_15m_batch(claim_ids, M):
    """PRODUCTION implementation. Many rules at once, via a token-presence index built once.

    Its licence to exist is extensional equivalence with eval_15m — identical membership on
    every admissible input, asserted bitwise at (episode x claim) grain by
    T5_FORWARD_MEMBERSHIP_REPLAY_V1. A fast path that quietly disagrees with the oracle is the
    failure mode this guards against, and equal treated counts do not rule it out.
    """
    per = M.groupby("episode_id").pos.nunique()
    complete = set(per.index[per == 4])
    Mc = M[M.episode_id.isin(complete)]
    need = set()
    for c in claim_ids:
        need.update(parse_rule(c)["tokens"])
    idx = {}
    for p_ in (1, 2, 3, 4):
        sub = Mc[Mc.pos == p_]
        eps = sub.episode_id.to_numpy()
        sets = sub.token_set.str.split().to_numpy()
        d = {t: set() for t in need}
        for e, ts in zip(eps, sets):
            for t in ts:
                if t in d:
                    d[t].add(e)
        idx[p_] = d
    out = {}
    for c in claim_ids:
        r = parse_rule(c)
        a, b = r["slot"].split("→")
        ta, tb = r["tokens"]
        out[c] = idx[int(a[1])][ta] & idx[int(b[1])][tb]
    return out


def episode_chunks(M, size):
    """Partition rows by WHOLE EPISODE. Both rules are episode-local — 15m by construction, 1H
    once the session-length table is supplied — so this partition cannot change any membership.
    Partitioning by raw row would, and is therefore not offered."""
    code, eps = pd.factorize(M.episode_id)
    order = np.argsort(code, kind="stable")     # stable: row order WITHIN an episode is kept
    Ms = M.iloc[order]
    cs = np.bincount(code, minlength=len(eps)).cumsum()
    for i in range(0, len(eps), size):
        j = min(i + size, len(eps))
        yield Ms.iloc[(cs[i - 1] if i else 0):cs[j - 1]]


def eval_15m_chunked(claim_ids, M, chunk):
    """Ingestion runs in chunks whose size is an OPERATIONAL choice. It must therefore not be a
    scientific one: the union over chunks equals the whole-frame result exactly."""
    out = {c: set() for c in claim_ids}
    for part in episode_chunks(M, chunk):
        for c, s in eval_15m_batch(claim_ids, part).items():
            out[c] |= s
    return out


def eval_1h_chunked(claim_ids, M, chunk, session_len):
    """session_len is REQUIRED here. Deriving it per chunk is the exact bug this argument
    exists to prevent, so the chunked path cannot fall back to the derivation."""
    if session_len is None:
        raise ValueError("eval_1h_chunked requires a frozen session-length table; deriving it "
                         "per chunk would make membership depend on the chunking")
    out = {c: set() for c in claim_ids}
    for part in episode_chunks(M, chunk):
        for c in claim_ids:
            out[c] |= eval_1h(c, part, session_len)
    return out


def membership_digest(episode_ids):
    """A membership's identity, order-free. Two evaluators agree only if these match."""
    return hashlib.sha256("|".join(sorted(map(str, episode_ids))).encode()).hexdigest()[:16]


def evaluator_hash():
    return hashlib.sha256(open(__file__, "rb").read()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════
# SYNTHETIC BOUNDARY FIXTURES — no market data, no outcomes
# ══════════════════════════════════════════════════════════════════════════
def synthetic_15m():
    """threshold-adjacent and malformed shapes the evaluator must handle identically."""
    rows = []
    def add(ep, pos, toks):
        rows.append(dict(episode_id=ep, pos=pos, token_set=" ".join(toks)))
    for p, t in ((1, ["VA"]), (2, ["VOL_B"]), (3, []), (4, [])):
        add("full_hit", p, t)                       # A on M1, B on M2 -> member
    for p, t in ((1, ["VA"]), (2, []), (3, ["VOL_B"]), (4, [])):
        add("wrong_slot", p, t)                     # B on M3 -> NOT a member
    for p, t in ((1, ["VA"]), (2, ["VOL_B"]), (3, [])):
        add("partial_hour", p, t)                   # only 3 bars -> excluded entirely
    for p, t in ((1, []), (2, []), (3, []), (4, [])):
        add("empty_tokens", p, t)
    for p, t in ((1, ["VA", "VOL_B"]), (2, ["VA", "VOL_B"]), (3, []), (4, [])):
        add("both_tokens_both_bars", p, t)          # member: A on M1 and B on M2
    return pd.DataFrame(rows)


def run_property_tests():
    """Same X + same rule -> same bitmap; row order irrelevant; outcome columns inert."""
    M = synthetic_15m()
    rule = "M1→M2|2|VA→VOL_B"
    a = eval_15m(rule, M)
    b = eval_15m(rule, M.sample(frac=1.0, random_state=7).reset_index(drop=True))
    M2 = M.copy(); M2["mfe_10d"] = np.random.default_rng(0).normal(size=len(M2))
    c = eval_15m(rule, M2)
    checks = {
        "deterministic": a == eval_15m(rule, M),
        "row_order_invariant": a == b,
        "outcome_column_inert": a == c,
        "expected_members": a == {"full_hit", "both_tokens_both_bars"},
        "wrong_slot_excluded": "wrong_slot" not in a,
        "partial_hour_excluded": "partial_hour" not in a,
        "empty_tokens_excluded": "empty_tokens" not in a,
    }
    return checks, sorted(a)
