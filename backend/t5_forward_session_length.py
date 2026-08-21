"""T5_FORWARD_SESSION_LENGTH_V1 — the last frame-dependent channel, closed before first X.

STATUS: DRAFT. Nothing is sealed until every gate below PASSES.

THIS LAYER SITS ABOVE THE FROZEN EVALUATOR AND DOES NOT TOUCH IT

t5_forward_eval.py is unchanged: it already takes the session-length table as an argument. This
module decides WHAT that argument is for a forward session. The dependency is added on top —
T5_FORWARD_MEMBERSHIP_EVALUATOR_V1, its replay, and the occurrence schema are NOT regenerated.

    session population definition -> floors + tie semantics -> self-replay 1275/1275
        -> seam PASS -> causality PASS -> FROZEN -> guards -> only then 2026-08-21 X

WHAT IS STILL OPEN AFTER THE EVALUATOR FREEZE

The frozen table covers 1,275 historical sessions. 2026-08-21 is not one, so first forward
evaluation raises UnknownSession. That is the correct failure — but a failure is not a rule,
and the tempting patch is the bug the replay just caught, in new clothes:

    mode(current_forward_rows)          <-- FORBIDDEN. frame-dependent.

THE FORWARD RULE IS THE HISTORICAL RULE, RESOLVED ONE SESSION AT A TIME

A calendar-template length would be chunk-independent but would NOT be the historical
statistic, and the seam at 2026-08-21 would become a semantic discontinuity rather than a
continuation. So: same statistic, same population, same tie rule — computed once per session,
before any episode is looked at.

'COMPLETE SET' IS A DEFINITION, NOT AN INTERPRETATION

    session_length_population(D) =
        every ticker having at least one 1H microstructure row with session_date == D
        bars_in_session taken per (D, ticker); ASSERTED constant within that key rather than
        silently .first()-ed, because two different values for one ticker-day is a data fault
        that a .first() would paper over

This is exactly the population the historical derivation used, so historical and forward
populations coincide by construction rather than by resemblance.

'COMPUTED ONCE' IS ONLY SAFE IF THE ONCE IS ON FINAL DATA

If the vendor can append late bars after the close, computing the mode at close freezes a
premature snapshot. So a session moves through explicit states and the mode is taken only in
the third:

    OPEN -> INGESTING -> SOURCE_FINAL -> SESSION_LENGTH_COMMITTED

SOURCE_FINAL is a PROXY for vendor finality, not a guarantee — no finality flag is available
from the source — and it is recorded as a proxy: ingest completed without error, the settle
window has elapsed, and two reads separated by that window return an identical row digest.

A correction arriving after commit does NOT rewrite the row. It becomes a new data_version and
an audit event; the committed value for this validation run stands. Rewriting it would make the
forward sample partly retrospective.

DECISION-TIME CAUSALITY — THE LEAK A PERFECT REPLAY CANNOT SEE

The rule reads the COMPLETE session D. That is causal only if every episode whose membership it
decides makes its decision after D closes. Historical replay knows the whole day at once and
will happily pass 20 million cells while a temporal leak sits underneath.

Entry semantics are NEXT_SESSION_OPEN_V1, so decision falls between the T5 close and the next
open, and T5_INTRADAY / CROSS_DAY / PREV_INTRADAY are all satisfied. That is an ARGUMENT. The
gate asserts it per episode instead of trusting it.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D, t5_forward_eval as EV          # noqa: E402
import t5_forward_causality as CZ                                      # noqa: E402

SPEC   = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
CUTOFF = SPEC["discovery_cutoff"]["last_eligible_historical_signal_session"]
DATA   = os.path.join(D.ROOT, "data")
TABLE  = os.path.join(DATA, "t5_forward_session_length.parquet")   # append-only
OUT    = "T5_FORWARD_SESSION_LENGTH_V1.json"
SEAM_N = 20
SETTLE_HOURS = 6          # between the two stability reads that stand in for vendor finality

STATES = ["OPEN", "INGESTING", "SOURCE_FINAL", "SESSION_LENGTH_COMMITTED"]


class SessionLengthDrift(RuntimeError):
    pass


class NonFinalSource(RuntimeError):
    pass


class TemporalLeak(RuntimeError):
    pass


# ══════════════════════════════════════════════════════════════════════════
# THE POPULATION — formal, and identical to the historical one by construction
# ══════════════════════════════════════════════════════════════════════════
def session_length_population(rows):
    """Every ticker with >=1 microstructure row for this session_date.

    bars_in_session must be constant within (session_date, ticker). The historical derivation
    used .first(), which would silently accept a ticker-day carrying two different values; here
    that is a hard error, because it is a data fault and not a tie."""
    g = rows.groupby("ticker")["bars_in_session"]
    n_uniq = g.nunique()
    bad = n_uniq[n_uniq > 1]
    if len(bad):
        raise SessionLengthDrift(
            f"{len(bad)} ticker(s) carry more than one bars_in_session value on this session, "
            f"first {bad.index[:3].tolist()}. This is a data fault, not a tie; .first() would "
            f"have hidden it.")
    return g.first()


def session_length_one(rows):
    """(expected_bars, n_tickers, modal_share). Same statistic and same tie rule as
    EV.session_len_table — ties resolve to the SMALLEST modal value — so applying this to a
    historical session must reproduce the frozen value exactly. Asserted, not assumed."""
    per = session_length_population(rows)
    modal = int(per.mode().iloc[0])
    return modal, int(len(per)), float((per == modal).mean())


# ══════════════════════════════════════════════════════════════════════════
# SOURCE FINALITY — a proxy, recorded as a proxy
# ══════════════════════════════════════════════════════════════════════════
def source_digest(rows):
    """Identity of one session's source rows, for the two-read stability check."""
    key = rows[["ticker", "session_position", "bars_in_session"]].sort_values(
        ["ticker", "session_position"])
    return hashlib.sha256(
        key.to_csv(index=False).encode()).hexdigest()[:16], int(len(rows))


def source_final(state, first_read, second_read, hours_between):
    """SOURCE_FINAL requires: ingest clean, settle window elapsed, two identical reads.

    Returns (bool, reason). No vendor finality flag exists, so this is the strongest available
    evidence and is labelled as evidence rather than as a guarantee."""
    if state != "INGESTING":
        return False, f"state is {state}, expected INGESTING"
    if hours_between < SETTLE_HOURS:
        return False, f"settle window not elapsed: {hours_between:.1f}h < {SETTLE_HOURS}h"
    if first_read != second_read:
        return False, f"source still moving: {first_read} != {second_read}"
    return True, "two identical reads across the settle window"


# ══════════════════════════════════════════════════════════════════════════
# COMMIT — append-only, immutable
# ══════════════════════════════════════════════════════════════════════════
def commit(session_date, length, meta, table_path=TABLE):
    T = pd.read_parquet(table_path) if os.path.exists(table_path) else pd.DataFrame(
        columns=["session_date", "expected_bars", "n_tickers", "modal_share",
                 "source_digest", "state", "data_version", "origin"])
    prior = T[T.session_date == session_date]
    if len(prior):
        if int(prior.expected_bars.iloc[0]) != int(length):
            raise SessionLengthDrift(
                f"{session_date}: committed at {int(prior.expected_bars.iloc[0])}, recomputed "
                f"as {length}. A committed session length is IMMUTABLE. If the source was "
                f"corrected, record a new data_version and an audit event — the committed "
                f"value for this validation run stands, because rewriting it would make the "
                f"forward sample partly retrospective.")
        return T, False
    row = dict(session_date=session_date, expected_bars=int(length),
               n_tickers=int(meta["n_tickers"]), modal_share=float(meta["modal_share"]),
               source_digest=meta.get("source_digest", ""),
               state="SESSION_LENGTH_COMMITTED",
               data_version=meta.get("data_version", ""), origin=meta.get("origin", "FORWARD"))
    T = pd.concat([T, pd.DataFrame([row])], ignore_index=True)
    T.to_parquet(table_path, index=False)
    return T, True


def floors():
    """Read the FROZEN floors. Never recompute them: re-deriving after seeing forward coverage
    would be forward-X-informed discretion."""
    a = json.load(open(OUT))
    return int(a["guards"]["min_tickers"]), float(a["guards"]["min_modal_share"])


def establish(rows, min_tickers, min_modal_share):
    length, n_tick, share = session_length_one(rows)
    if n_tick < min_tickers:
        return None, dict(status="SESSION_LENGTH_NOT_ESTABLISHED", reason="TOO_FEW_TICKERS",
                          n_tickers=n_tick, floor=min_tickers, modal_share=share)
    if share < min_modal_share:
        return None, dict(status="SESSION_LENGTH_NOT_ESTABLISHED", reason="AMBIGUOUS_MODE",
                          n_tickers=n_tick, modal_share=share, floor=min_modal_share)
    return length, dict(status="ESTABLISHED", n_tickers=n_tick, modal_share=share)


# ══════════════════════════════════════════════════════════════════════════
# GATE 1 — the forward rule reproduces the frozen historical table EXACTLY
# ══════════════════════════════════════════════════════════════════════════
def gate_self_replay(M, frozen):
    got, bad = {}, []
    for d, rows in M.groupby("session_date"):
        L = session_length_one(rows)[0]
        got[d] = L
        if d in frozen.index and int(frozen[d]) != L:
            bad.append(dict(session_date=str(d), frozen=int(frozen[d]), forward_rule=L))
    return got, bad


# ══════════════════════════════════════════════════════════════════════════
# GATE 2 — decision-time causality. The leak a perfect replay cannot see.
# ══════════════════════════════════════════════════════════════════════════
# GATE 2 (causality) lives in t5_forward_causality: G6A counterfactual + G6B runtime
# guard. An earlier, weaker version stood here and asserted only that the session had
# CLOSED. Two definitions of causality in one pipeline is worse than one, so it is
# removed rather than left unused.


# ══════════════════════════════════════════════════════════════════════════
# GATE 3 — the seam, reported so PASS cannot hide thin coverage
# ══════════════════════════════════════════════════════════════════════════
def gate_seam(M, claims, frozen, min_tickers, min_modal_share, n=SEAM_N):
    dates = sorted(pd.unique(M.session_date))[-n:]
    tab, refused = {}, []
    for d in dates:
        L, meta = establish(M[M.session_date == d], min_tickers, min_modal_share)
        if L is None:
            refused.append(dict(session_date=str(d), **meta))
        else:
            tab[d] = L
    span = M.groupby("episode_id")["session_date"].agg(["min", "max"])
    in_win = set(span.index[(span["min"] >= dates[0]) & (span["max"] <= dates[-1])])
    touch = set(span.index[(span["max"] >= dates[0]) & (span["min"] <= dates[-1])])
    edge = touch - in_win
    Mw = M[M.episode_id.isin(in_win)]
    cross = [c for c in claims if c.startswith("CROSS_DAY")]
    fwd = EV.eval_1h_chunked(claims, Mw, 64, pd.Series(tab))
    ref = {c: EV.eval_1h(c, Mw, frozen) for c in claims}
    mism = {c: len(fwd[c] ^ ref[c]) for c in claims}
    cross_cov = {c: len(ref[c]) for c in cross}
    return dict(
        sessions_tested=len(dates),
        episodes_eligible=len(in_win),
        episodes_excluded_window_edge=len(edge),
        cross_day_coverage=cross_cov,
        unknown_sessions=len(refused), refused=refused,
        session_length_mismatches=int(sum(
            1 for d in tab if d in frozen.index and int(frozen[d]) != tab[d])),
        membership_cell_mismatches=int(sum(mism.values())),
        per_claim=mism,
        edge_limitation="a T5 episode spans PREV_DAY and T5_DAY, so an episode with a leg "
                        "outside the window cannot be evaluated from the window alone. "
                        "CROSS_DAY is the least covered claim family at the first date, and "
                        "that weakness is printed rather than absorbed into PASS.")


# ══════════════════════════════════════════════════════════════════════════
def main():
    ART.smoke_test(verbose=False)
    print("T5_FORWARD_SESSION_LENGTH_V1 · DRAFT", flush=True)

    cols = ["episode_id", "ticker", "t5_date", "relative_day", "session_date", "ts_et",
            "session_position", "bars_in_session", "token_set"]
    M = pd.read_parquet(D.OUT_MS, columns=cols)
    if str(M.t5_date.max()) > CUTOFF:
        raise RuntimeError(f"post-cutoff row: {M.t5_date.max()} > {CUTOFF}")
    frozen = EV.session_len_table(M)

    # ── floors, from the PRE-CUTOFF distribution only, frozen here ─────────
    stats = []
    for d, rows in M.groupby("session_date"):
        L, n_tick, share = session_length_one(rows)
        dig, nrow = source_digest(rows)
        stats.append(dict(session_date=str(d), expected_bars=L, n_tickers=n_tick,
                          modal_share=share, source_digest=dig))
    S = pd.DataFrame(stats)
    min_tickers = int(max(5, np.floor(S.n_tickers.quantile(0.01))))
    min_share = float(np.floor(S.modal_share.quantile(0.01) * 100) / 100)
    print(f"  historical sessions {len(S):,}")
    print(f"  n_tickers    min {S.n_tickers.min()} · p01 {S.n_tickers.quantile(0.01):.0f} · "
          f"median {S.n_tickers.median():.0f}")
    print(f"  modal_share  min {S.modal_share.min():.3f} · p01 "
          f"{S.modal_share.quantile(0.01):.3f} · median {S.modal_share.median():.3f}")
    print(f"  FLOORS  n_tickers >= {min_tickers} · modal_share >= {min_share}   "
          f"(<= {CUTOFF} only)")

    # ── what the floors COST, measured rather than assumed ────────────────
    # The historical table was built with NO floor: every session got a length. The forward
    # rule applies one, so forward is STRICTER than historical. That asymmetry is conservative
    # — it holds episodes instead of inventing a length — but it is a real semantic difference
    # and is measured here instead of being glossed.
    refused = S[(S.n_tickers < min_tickers) | (S.modal_share < min_share)]
    Eall = pd.read_parquet(D.OUT_EP, columns=["episode_id", "t5_date", "prev_session_date"])
    rs = set(refused.session_date)
    held = Eall[Eall.t5_date.isin(rs) | Eall.prev_session_date.isin(rs)]
    floor_cost = dict(
        historical_sessions_refused=int(len(refused)),
        historical_sessions_total=int(len(S)),
        pct_sessions=round(100 * len(refused) / len(S), 2),
        historical_episodes_held=int(len(held)),
        historical_episodes_total=int(len(Eall)),
        pct_episodes=round(100 * len(held) / len(Eall), 2),
        refused_lengths=sorted(refused.expected_bars.unique().tolist()),
        interpretation="on this sample the refused sessions all resolve to the modal "
                       "full-session length, so the floors cost COVERAGE rather than "
                       "correctness here. Prospectively that is the point: forward you cannot "
                       "know whether thin evidence happens to be right.",
        asymmetry="the frozen historical table applied no floor; the forward rule does. "
                  "Forward is therefore stricter than historical, in the conservative "
                  "direction.",
        not_tuned="the p01 rule was declared before this cost was computed and is not being "
                  "relaxed now that it is known")
    print(f"  floor cost · {len(refused)}/{len(S)} sessions ({floor_cost['pct_sessions']}%) · "
          f"{len(held):,}/{len(Eall):,} episodes ({floor_cost['pct_episodes']}%) would be HELD")

    got, bad = gate_self_replay(M, frozen)
    print(f"  {'PASS' if not bad else 'FAIL'} self-replay · {len(got):,} sessions · "
          f"disagreements {len(bad)}")
    for b in bad[:5]:
        print(f"      {b}")

    # ── G6 · from t5_forward_causality, the single definition of causality ─
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "t5_date"])
    Mt = CZ.to_et(M[["episode_id", "session_date", "ts_et"]])
    cal = sorted(pd.unique(Mt.session_date))
    g6b = CZ.test_forward_guard()
    op = CZ.gate_opening_print(Mt, CZ.decision_deadlines(E, CZ.session_opens(Mt), cal)[1])
    caus = CZ.gate_6a(Mt, E, cal)
    print(f"  {'PASS' if all(g6b.values()) else 'FAIL'} G6B runtime guard · {g6b}")
    print(f"  {'PASS' if not op['violations'] else 'FAIL'} opening-print exclusion · "
          f"checked {op['episodes_checked']:,} · violations {op['violations']}")
    print(f"  {'PASS' if not caus['violations_late_for_decision'] else 'FAIL'} G6A · "
          f"checked {caus['episodes_checked']:,} · late {caus['violations_late_for_decision']} "
          f"· order {caus['violations_order']}")
    print(f"       headroom(binding) {caus['headroom_hours_binding']}")
    print(f"       max tolerable settle {caus['max_tolerable_settle_hours']}h vs "
          f"{caus['settle_hours']}h used")
    for k, v in caus["diagnostics_non_binding"].items():
        print(f"       [diag] {k} min {v['min']:.1f}h · would_fail {v['would_fail']:,}")
    caus["opening_print_exclusion"] = op
    caus["g6b_guard_tests"] = g6b

    COMP = json.load(open("T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1.json"))
    claims = [r["representative"] for r in COMP["per_cluster"]]
    seam = gate_seam(M, claims, frozen, min_tickers, min_share)
    print(f"  seam · sessions {seam['sessions_tested']} · eligible "
          f"{seam['episodes_eligible']:,} · excluded(edge) "
          f"{seam['episodes_excluded_window_edge']:,} · unknown {seam['unknown_sessions']} · "
          f"len-mism {seam['session_length_mismatches']} · cell-mism "
          f"{seam['membership_cell_mismatches']}")
    print(f"       cross_day_coverage {seam['cross_day_coverage']}")

    passed = (not bad                                                        # G4
              and seam["membership_cell_mismatches"] == 0                    # G5
              and seam["session_length_mismatches"] == 0                     # G5
              and seam["unknown_sessions"] == 0                              # G7
              and caus["violations_late_for_decision"] == 0                  # G6A
              and caus["violations_order"] == 0                              # G6A
              and op["violations"] == 0                                      # opening print
              and all(g6b.values()))                                         # G6B
    if not passed:
        raise RuntimeError("session-length rule NOT sealed — a gate failed. The specification "
                           "gets fixed before the first forward X, not the result after it.")

    T = S.assign(state="SESSION_LENGTH_COMMITTED", data_version="HIST", origin="HISTORICAL")[
        ["session_date", "expected_bars", "n_tickers", "modal_share", "source_digest",
         "state", "data_version", "origin"]]
    T.to_parquet(TABLE, index=False)

    ART.seal(dict(
        spec_id="T5_FORWARD_SESSION_LENGTH_V1", status="FROZEN",
        reseal_note=dict(
            supersedes_digest="021c53cd32236cd3",
            why="the first seal recorded the floors without their measured cost. Nothing "
                "referenced that digest and no forward X had been evaluated, so the artifact "
                "is completed rather than annotated. The re-seal is recorded here instead of "
                "being silent — a habit of quietly re-sealing what one has just learned about "
                "is the thing worth guarding against, not the single completion.",
            changed="added guards.measured_cost; no gate, floor, rule or threshold altered"),
        depends_on=dict(evaluator="T5_FORWARD_MEMBERSHIP_EVALUATOR_V1",
                        evaluator_digest=ART.file_digest(
                            "T5_FORWARD_MEMBERSHIP_EVALUATOR_V1.json"),
                        replay="T5_FORWARD_MEMBERSHIP_REPLAY_V1",
                        replay_digest=ART.file_digest("T5_FORWARD_MEMBERSHIP_REPLAY_V1.json"),
                        ledger_amendment="T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1",
                        ledger_amendment_digest=ART.file_digest(
                            "T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1.json"),
                        why_amendment_first="the amendment is what authorises the ledger to "
                                            "carry SESSION_LENGTH_LATE_FOR_DECISION and "
                                            "commit_latency, which this spec's G6B guard "
                                            "emits, so it is sealed before this one and "
                                            "referenced by digest"),
        added_on_top="this layer decides the session_len ARGUMENT the frozen evaluator already "
                     "accepts. t5_forward_eval.py is unchanged and no earlier artifact is "
                     "regenerated: a new dependency is added above, never by re-producing a "
                     "freeze below.",
        problem="the frozen table covers 1,275 historical sessions; 2026-08-21 is not one. "
                "UnknownSession is the right failure, but a failure is not a rule, and the "
                "tempting patch — mode(current_forward_rows) — is the frame-dependence the "
                "cell-level replay just caught, in a new form.",
        rule="mode over tickers of bars_in_session over session_length_population(D), computed "
             "ONCE after SOURCE_FINAL and BEFORE any membership evaluation for D, committed "
             "append-only, never recomputed",
        population=dict(
            definition="every ticker with at least one 1H microstructure row for session_date D",
            bars_in_session="asserted CONSTANT within (session_date, ticker); two values for "
                            "one ticker-day is a data fault, not a tie, and .first() would "
                            "have hidden it",
            identical_to_historical="by construction, not by resemblance"),
        state_machine=dict(
            states=STATES,
            mode_computed_in="SOURCE_FINAL",
            source_final=dict(
                is_a_proxy=True,
                why="the source exposes no finality flag, so finality is evidenced rather than "
                    "guaranteed, and is labelled as evidence",
                conditions=["ingest completed without error",
                            f"settle window of {SETTLE_HOURS}h elapsed after the close",
                            "two reads across that window return an identical row digest"]),
            late_correction="does NOT rewrite a committed row. It becomes a new data_version "
                            "and an audit event; the committed value for this validation run "
                            "stands, because rewriting it would make the forward sample partly "
                            "retrospective."),
        decision_time_causality=dict(
            why="the rule reads the COMPLETE session D, which is causal only if every episode "
                "it decides makes its decision after D closes. A historical replay knows the "
                "whole day at once and can pass 20 million cells with a temporal leak "
                "underneath.",
            entry_semantics="NEXT_SESSION_OPEN_V1",
            structural_argument="the decision is the NEXT session's open, so every session the "
                                "rule reads is already closed; T5_INTRADAY and CROSS_DAY bind "
                                "on the T5 close, PREV_INTRADAY earlier",
            runtime_assertions=["session_length_commit_ts <= membership_eval_ts",
                                "session length available at or before decision time"],
            result=caus),
        guards=dict(min_tickers=min_tickers, min_modal_share=min_share,
                    calibrated_on=f"pre-cutoff distribution only (<= {CUTOFF})",
                    why_now="choosing floors after seeing 08-21 coverage would be "
                            "forward-X-informed discretion",
                    never_recomputed="runtime reads them from this artifact",
                    measured_cost=floor_cost,
                    on_failure="SESSION_LENGTH_NOT_ESTABLISHED — the session's episodes are "
                               "HELD: not evaluated, not counted toward the 20,000, reported "
                               "in the DQ ledger. Never a fallback, never a default.",
                    may_be_established_later="yes, once the data completes and SOURCE_FINAL is "
                                             "reached; committed once, then immutable"),
        forbidden=["mode over the current evaluation chunk",
                   "any default or fallback length",
                   "recomputing or overwriting a committed session length",
                   "re-deriving the floors after any forward session is observed"],
        why_not_a_calendar_template=
            "chunk-independent but NOT the historical statistic. Historical membership came "
            "from a modal bar count; a published close time is a different rule, and the seam "
            "at 2026-08-21 would be a semantic discontinuity rather than a continuation.",
        gates=dict(self_replay=dict(sessions=len(got), disagreements=len(bad), exact=True),
                   causality=caus, seam=seam),
        historical_distribution=dict(
            n_tickers=dict(min=int(S.n_tickers.min()), p01=float(S.n_tickers.quantile(0.01)),
                           median=float(S.n_tickers.median())),
            modal_share=dict(min=float(S.modal_share.min()),
                             p01=float(S.modal_share.quantile(0.01)),
                             median=float(S.modal_share.median())),
            lengths=sorted(S.expected_bars.unique().tolist())),
        table=dict(file=os.path.basename(TABLE), rows=int(len(T)), append_only=True,
                   immutable_prefix="rows with origin == HISTORICAL",
                   digest=ART.file_digest(TABLE)),
        evaluator_hash=EV.evaluator_hash()),
        OUT, required=("spec_id", "rule", "population", "guards", "gates",
                       "decision_time_causality"))
    print(f"\n  FROZEN · {OUT} · {ART.file_digest(OUT)}")


if __name__ == "__main__":
    main()
