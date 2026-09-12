"""Coarse EMA engine — implementation of MASSIVE_COARSE_EMA_FEATURE_SPEC_V1 (ef01b67c22f6770f).

Twelve streams: periods 9/20/50/200 across 15m/1H/1D. Nothing else is constructible — a
request for an unregistered period or timeframe raises rather than being quietly served, so
the frozen search space is enforced by the API surface and not merely documented.

SEGMENTS ARE THE WHOLE DESIGN. EMA lives inside contiguous runs of COMPLETE coarse bars. A
contaminated interval ends the segment, emits nothing, and the next COMPLETE bar starts a new
segment seeded from its own close. There is no bridging, no forward fill, and no carry of
pre-gap state — which means the engine holds no value it could accidentally reuse across a
gap, because the state is deleted at the break rather than merely flagged.

Segmentation depends only on coverage, never on period, so all four periods of a given
(security, timeframe) break together. Validity does depend on period: age < p is
INITIALIZATION_SENSITIVE and only age >= p is VALID. EMA200 therefore needs 200 consecutive
COMPLETE bars before it means anything, and that is not relaxed for thinly covered securities.

Causality is structural. The recursion only ever reads close_t and the previous state, and each
row carries the input bar's own feature_information_available_at, so a value cannot exist
before the bar that produced it has closed.

NO RVOL. This engine has no volume input at all.
"""
from __future__ import annotations

REGISTERED_PERIODS = (9, 20, 50, 200)
REGISTERED_TFS = ("15m", "1H", "1D")
ELIGIBLE_STATE = "COMPLETE"


class EmaHold(Exception):
    """Fail-closed. Never downgraded."""


class CoarseEmaV1:
    def __init__(self, cohort_keys, spec_digest="ef01b67c22f6770f"):
        self.cohort_keys = set(cohort_keys)
        self.spec_digest = spec_digest
        self.state = {}          # (key, tf, period) -> [ema, age]
        self.seg = {}            # (key, tf) -> current segment id
        self.seg_len = {}        # (key, tf) -> current segment length
        self.next_seg = 0

    # ------------------------------------------------------------- guards
    def check_request(self, tf, period):
        if tf not in REGISTERED_TFS:
            raise EmaHold(
                f"GUARD unregistered_timeframe: {tf!r} is not in {list(REGISTERED_TFS)}. "
                f"A 1m-native EMA has no coarse-TF parent and is inadmissible by "
                f"construction under the charter's one_minute_role.")
        if period not in REGISTERED_PERIODS:
            raise EmaHold(
                f"GUARD unregistered_period: {period!r} is not in "
                f"{list(REGISTERED_PERIODS)}. The registry is frozen pre-Y; legacy ribbon "
                f"periods are LEGACY_REFERENCE_ONLY and may not enter production.")

    # ------------------------------------------------------------- update
    def update_session(self, tf, df, emit=False):
        """Advance every registered period for one timeframe over one session.

        `df` must be the coarse production rows for a single session and timeframe, and is
        processed in (security, interval_index) order. Returns emitted rows when emit=True.
        """
        for p in REGISTERED_PERIODS:
            self.check_request(tf, p)
        bad = set(df["security_key_v1"].unique()) - self.cohort_keys
        if bad:
            raise EmaHold(f"GUARD cohort_admission: {len(bad)} security(ies) outside the "
                          f"frozen cohort reached the EMA engine")
        allowed = {"COMPLETE", "UNOBSERVED_CONTAMINATED"}
        seen_states = set(df.coverage_state.unique())
        if not seen_states <= allowed:
            raise EmaHold(
                f"GUARD forbidden_input: coverage_state {seen_states - allowed} may not "
                f"reach EMA; only COMPLETE is consumed and only "
                f"UNOBSERVED_CONTAMINATED may appear as a break")
        if (df.timeframe != tf).any():
            raise EmaHold("GUARD timeframe_mismatch: rows from another timeframe")

        out = []
        cols = ["security_key_v1", "session_date", "interval_index", "bar_start", "bar_end",
                "feature_information_available_at", "coverage_state", "close"]
        d = df.sort_values(["security_key_v1", "interval_index"])[cols]
        for (k, sess, idx, bs, be, avail, cstate, close) in d.itertuples(index=False,
                                                                        name=None):
            sk = (k, tf)
            if cstate != ELIGIBLE_STATE:
                # break the segment: DELETE state rather than flag it, so there is nothing
                # left that a later bar could accidentally continue from
                for p in REGISTERED_PERIODS:
                    self.state.pop((k, tf, p), None)
                self.seg_len[sk] = 0
                if emit:
                    for p in REGISTERED_PERIODS:
                        out.append(dict(security_key_v1=k, session_date=sess,
                                        timeframe=tf, bar_start=int(bs), bar_end=int(be),
                                        feature_information_available_at=int(avail),
                                        ema_period=p, ema_value=None,
                                        ema_segment_id=None, ema_age_valid_bars=0,
                                        ema_validity_state="UNAVAILABLE",
                                        ema_reset_reason="UNOBSERVED_CONTAMINATED"))
                continue
            if self.seg_len.get(sk, 0) == 0:
                self.next_seg += 1
                self.seg[sk] = self.next_seg
            self.seg_len[sk] = self.seg_len.get(sk, 0) + 1
            for p in REGISTERED_PERIODS:
                key = (k, tf, p)
                st = self.state.get(key)
                if st is None:
                    ema, age, reason = float(close), 1, "SEGMENT_SEED"
                else:
                    a = 2.0 / (p + 1)
                    ema = a * float(close) + (1.0 - a) * st[0]
                    age, reason = st[1] + 1, None
                self.state[key] = [ema, age]
                if emit:
                    out.append(dict(
                        security_key_v1=k, session_date=sess, timeframe=tf,
                        bar_start=int(bs), bar_end=int(be),
                        feature_information_available_at=int(avail), ema_period=p,
                        ema_value=ema, ema_segment_id=self.seg[sk],
                        ema_age_valid_bars=age,
                        ema_validity_state=("VALID" if age >= p
                                            else "INITIALIZATION_SENSITIVE"),
                        ema_reset_reason=reason))
        return out
