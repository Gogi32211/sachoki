"""T1_CLOCK_GAP_CLOSURE_V1 — does dense positional adjacency bridge a missing hour?

The review's question, stated exactly: the grammar admits a pair when
`session_position[j+1] == session_position[j] + 1`, and session_position is a DENSE rank
of observed bars. If an interior hour is missing, the next observed bar inherits the next
position, so a pair separated by a real gap can be admitted as STRICT_ADJACENT.

This module answers it on real data and by construction, and measures what a corrected
adjacency would change. It SEALS NOTHING ELSE: no artifact is rebuilt, no k is changed,
no capability is run, and no Y value is read.

    1  EARLY_CLOSE vs INTERIOR_MISSING_BAR   separated by hour SLOT, not by bar count
    2  grammar adjacency vs slot adjacency   for every position-adjacent pair
    3  synthetic removal test                delete one interior bar from valid sessions
                                             and see whether a sequence bridges the hole
    4  real-data census                      admitted spanning-gap windows
    5  seam validity                         PREV_LAST -> T1_FIRST proved without dense rank
    6  impact                                the enumeration re-run with slot-indexed
                                             positions, so `pos+1` MEANS the next hour
"""
from __future__ import annotations
import json, os, sys, time                                            # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t1_dna as D1                               # noqa: E402
import t1_sequence_grammar as G1                                      # noqa: E402

OUT = "T1_CLOCK_GAP_CLOSURE_V1.json"
SESSION_OPEN_MIN = 9 * 60 + 30            # 09:30 ET
SLOTS = 7                                 # 09:30..15:30, one per hour


def with_slots(M):
    ts = pd.to_datetime(M.ts_et)
    M = M.copy()
    M["slot"] = np.floor((ts.dt.hour * 60 + ts.dt.minute - SESSION_OPEN_MIN) / 60).astype(int)
    return M.sort_values(["episode_id", "relative_day", "session_position"],
                         kind="stable").reset_index(drop=True)


def pair_mask(M):
    same = ((M.episode_id.values[1:] == M.episode_id.values[:-1])
            & (M.relative_day.values[1:] == M.relative_day.values[:-1]))
    posadj = M.session_position.values[1:] == M.session_position.values[:-1] + 1
    return same & posadj


def main():
    t0 = time.time()
    # the grammar's own loader (gate A inside) plus ts_et, so the impact re-run consumes
    # exactly what the sealed enumeration consumed
    raw = G1.load_ms()
    raw = raw.merge(pd.read_parquet(D1.OUT_MS,
                                    columns=["episode_id", "relative_day",
                                             "session_position", "ts_et"]),
                    on=["episode_id", "relative_day", "session_position"], how="left")
    M = with_slots(raw)

    # ── 1 · EARLY_CLOSE vs INTERIOR_MISSING_BAR ──────────────────────────
    g = M.groupby(["episode_id", "relative_day"], sort=False)
    agg = g.slot.agg(["min", "max", "count", lambda s: int(s.max() - s.min() + 1)])
    agg.columns = ["slot_min", "slot_max", "n_bars", "span"]
    interior = agg.n_bars < agg.span                       # holes strictly inside the span
    truncated = (~interior) & (agg.n_bars < SLOTS)         # contiguous but short
    full = agg.n_bars == SLOTS
    item1 = dict(
        n_sessions=int(len(agg)),
        FULL_SESSION=int(full.sum()),
        TRUNCATED_contiguous_late_start_or_early_close=int(truncated.sum()),
        INTERIOR_MISSING_BAR=int(interior.sum()),
        rule="a session is INTERIOR_MISSING only when its observed bar count is smaller "
             "than the slot span it covers — a short but contiguous session is a late "
             "start or an early close, which is a different thing entirely",
        episodes_with_interior_gap=int(
            agg[interior].reset_index().episode_id.nunique()))

    # ── 2 · grammar adjacency vs slot adjacency ──────────────────────────
    pair = pair_mask(M)
    sd = M.slot.values[1:] - M.slot.values[:-1]
    gap = pair & (sd > 1)
    same_slot = pair & (sd == 0)
    u, c = np.unique(sd[pair], return_counts=True)
    item2 = dict(
        position_adjacent_pairs=int(pair.sum()),
        slot_delta_histogram={int(k): int(v) for k, v in zip(u, c)},
        pairs_that_bridge_a_missing_hour=int(gap.sum()),
        share=round(float(gap.sum() / pair.sum()), 6),
        pairs_inside_one_hour=int(same_slot.sum()),
        finding="the grammar predicate is pos[j+1] == pos[j] + 1 with pos a DENSE rank of "
                "observed bars, so these pairs ARE admitted as STRICT_ADJACENT even though "
                "an hour is missing between them")

    # ── 3 · synthetic removal test ───────────────────────────────────────
    full_keys = agg[full].index[:400]
    sub = M.set_index(["episode_id", "relative_day"]).loc[full_keys].reset_index()
    bridged = 0
    for (e, r), s in sub.groupby(["episode_id", "relative_day"], sort=False):
        s = s.sort_values("session_position")
        drop_i = 3                                     # remove the interior 4th hour
        kept = s.drop(s.index[drop_i]).copy()
        kept["session_position"] = np.arange(1, len(kept) + 1)   # dense re-rank, as built
        slots = kept.slot.values
        pos = kept.session_position.values
        adj = pos[1:] == pos[:-1] + 1
        bridged += int((adj & ((slots[1:] - slots[:-1]) > 1)).sum())
    item3 = dict(
        sessions_tested=int(len(full_keys)), removed="the interior 4th hour of each",
        sequences_bridging_the_hole=bridged,
        required=0,
        result="PASS" if bridged == 0 else "FAIL",
        reading="under dense re-ranking each removal creates exactly one adjacency across "
                "the hole, so a window spanning the removed hour is admitted; the required "
                "behaviour is that such windows become invalid/absent")

    # ── 5 · seam validity, independent of dense rank ─────────────────────
    piv = M.groupby(["episode_id", "relative_day"]).slot.agg(["min", "max"]).unstack()
    both = piv.dropna()
    prev_last = both[("max", "PREV_DAY")]
    t1_first = both[("min", "T1_DAY")]
    item5 = dict(
        both_side_episodes=int(len(both)),
        prev_last_slot_is_final_hour=int((prev_last == SLOTS - 1).sum()),
        t1_first_slot_is_opening_hour=int((t1_first == 0).sum()),
        seam_rule="CROSS_DAY requires exactly one PREV->T1 transition and never uses "
                  "pos+1 across the seam, so the seam does not depend on dense rank; the "
                  "two counts above show how often the seam joins a真 last hour to a真 "
                  "opening hour",
        seam_uses_position_arithmetic=False)

    # ── 6 · impact: re-enumerate with SLOT-indexed positions ─────────────
    fixed = M.copy()
    fixed["session_position"] = fixed.slot + 1        # now pos+1 MEANS the next hour
    print("  re-enumerating with slot-indexed positions …", flush=True)
    u_fix, C_fix, _ = G1.enumerate_grammar(fixed, verbose=False)
    C_now = pd.read_parquet(G1.SURV)
    key = lambda C: set(C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence)
    k_now, k_fix = key(C_now), key(C_fix)
    sup_now = C_now.set_index(C_now.family + "|" + C_now.length.astype(str) + "|"
                              + C_now.canonical_sequence).episode_n
    sup_fix = C_fix.set_index(C_fix.family + "|" + C_fix.length.astype(str) + "|"
                              + C_fix.canonical_sequence).episode_n
    common = sorted(k_now & k_fix)
    delta = (sup_now[common] - sup_fix[common])
    item6 = dict(
        sealed=dict(names=int(len(C_now)),
                    classes=int(C_now.equivalence_class_id.nunique()),
                    class_digest=G1.class_digest(C_now)),
        slot_indexed=dict(names=int(len(C_fix)),
                          classes=int(C_fix.equivalence_class_id.nunique()),
                          class_digest=G1.class_digest(C_fix)),
        names_only_in_sealed=sorted(k_now - k_fix)[:20],
        n_names_only_in_sealed=len(k_now - k_fix),
        names_only_in_slot_indexed=sorted(k_fix - k_now)[:20],
        n_names_only_in_slot_indexed=len(k_fix - k_now),
        common_names=len(common),
        support_changed=int((delta != 0).sum()),
        support_drop_total=int(delta[delta > 0].sum()),
        max_single_drop=int(delta.max()) if len(delta) else 0,
        reading="the slot-indexed enumeration is what STRICT_ADJACENT was meant to mean; "
                "the differences below are the size of the defect, not a proposal")

    ok = item2["pairs_that_bridge_a_missing_hour"] == 0 and item3["result"] == "PASS"
    body = dict(
        spec_id="T1_CLOCK_GAP_CLOSURE_V1",
        result="PASS" if ok else "DEFECT CONFIRMED",
        question="can dense positional adjacency bridge an interior missing hour?",
        answer=("NO — no admitted pair spans a missing hour" if ok else
                "YES. Dense positional adjacency admits pairs that span a missing hour. "
                "The grammar predicate compares positions, not clock slots, so an absent "
                "hour is silently closed up and the two surrounding bars become "
                "STRICT_ADJACENT."),
        item1_session_classification=item1,
        item2_adjacency_vs_slots=item2,
        item3_synthetic_removal=item3,
        item4_real_data_census=dict(
            admitted_spanning_gap_pairs=item2["pairs_that_bridge_a_missing_hour"],
            required=0,
            sessions_with_interior_gap=item1["INTERIOR_MISSING_BAR"],
            episodes_touched=item1["episodes_with_interior_gap"]),
        item5_seam=item5,
        item6_impact=item6,
        scope_note="the same builder and the same adjacency predicate are shared by T5, "
                   "T9 and T3, so this finding is not specific to T1",
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id", "result", "item2_adjacency_vs_slots",
                                      "item6_impact"), supersede=os.path.exists(OUT))
    print(f"1 sessions: full {item1['FULL_SESSION']:,} · truncated "
          f"{item1['TRUNCATED_contiguous_late_start_or_early_close']:,} · interior-gap "
          f"{item1['INTERIOR_MISSING_BAR']:,}")
    print(f"2 adjacency: {item2['position_adjacent_pairs']:,} pairs · bridging a missing "
          f"hour {item2['pairs_that_bridge_a_missing_hour']:,} ({item2['share']:.3%})")
    print(f"3 removal test: {item3['result']} · bridged {item3['sequences_bridging_the_hole']}"
          f" of {item3['sessions_tested']} sessions")
    print(f"6 impact: names {item6['sealed']['names']} -> {item6['slot_indexed']['names']} · "
          f"classes {item6['sealed']['classes']} -> {item6['slot_indexed']['classes']} · "
          f"support changed on {item6['support_changed']} names")
    print(f"\nT1_CLOCK_GAP_CLOSURE_V1 · {d} · {body['result']}")


if __name__ == "__main__":
    main()
