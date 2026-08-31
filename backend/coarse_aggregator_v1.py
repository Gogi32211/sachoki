"""Coarse aggregator V1 — implementation of MASSIVE_1M_COARSE_AGGREGATION_SPEC_V1 (51540a1cefe2ec2b).

Interval geometry comes from the XNYS calendar every time. Nothing here knows that a normal
session is 390 minutes or that the close is at 16:00; it asks the schedule and divides. That is
why the early-close case needs no special branch — a 210-minute session simply yields 14
fifteen-minute intervals and a 3-full-plus-stub hour layout, and a phantom post-close interval
is unconstructible rather than merely forbidden.

COMPLETENESS IS STRICT AND IS NOT NEGOTIABLE HERE. One UNOBSERVED expected minute makes the
containing interval UNOBSERVED_CONTAMINATED, and a contaminated interval never carries OHLCV.
The spec is explicit that tolerance belongs to a later study specification, so this code has no
tolerance parameter at all — not defaulted to zero, absent. A knob that does not exist cannot
be quietly turned.

AVAILABILITY IS THE INTERVAL'S END, NEVER ITS START. `feature_information_available_at` equals
the close of the final constituent minute, so a coarse value cannot reach back to a 1m minute
inside its own window. This is the single property that makes a coarse-parent EMA causal, and
it is checked rather than assumed.
"""
from __future__ import annotations
import os                                                              # noqa: E402

REGISTERED_TF = {"15m": 15, "1H": 60, "1D": None}
DERIVED_ROOT = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_1m_base_v2"


class AggHold(Exception):
    """Fail-closed. Never downgraded."""


class CoarseAggregatorV1:
    def __init__(self, cal, cohort_keys, spec_digest="51540a1cefe2ec2b"):
        self.cal = cal
        self.cohort_keys = set(cohort_keys)
        self.spec_digest = spec_digest

    # ------------------------------------------------------------ geometry
    def session_bounds(self, day):
        import pandas as pd
        try:
            r = self.cal.schedule.loc[pd.Timestamp(day)]
        except KeyError:
            raise AggHold(f"GUARD calendar: {day} is not an XNYS session")
        o = int(r["open"].tz_convert("UTC").value // 10 ** 6)
        c = int(r["close"].tz_convert("UTC").value // 10 ** 6)
        return o, c, (c - o) // 60000

    def intervals(self, day, tf):
        """Left-closed, left-labelled, session-aligned. Derived, never assumed."""
        if tf not in REGISTERED_TF:
            raise AggHold(f"GUARD unregistered_timeframe: {tf!r} is not in "
                          f"{sorted(REGISTERED_TF)}; adding one is search-space expansion "
                          f"requiring a pre-Y amendment")
        o, c, n = self.session_bounds(day)
        w = REGISTERED_TF[tf]
        if w is None:
            return [dict(idx=0, start=o, end=c, minutes=n, stub=False)]
        out = []
        for i in range((n + w - 1) // w):
            s = o + i * w * 60000
            e = min(s + w * 60000, c)
            m = (e - s) // 60000
            out.append(dict(idx=i, start=s, end=e, minutes=m, stub=(m != w)))
        return out

    # ------------------------------------------------------------ aggregate
    def aggregate(self, day, minute_states, bars, tf, source_root=None,
                  with_ohlcv=True):
        if source_root is not None and os.path.realpath(source_root) != os.path.realpath(
                DERIVED_ROOT):
            raise AggHold(
                f"GUARD source_path: refusing input outside the production 1m base "
                f"({source_root}); supplemental and diagnostic sources are inadmissible")
        o, c, n = self.session_bounds(day)
        ivs = self.intervals(day, tf)
        w = REGISTERED_TF[tf]

        bad = set(minute_states["security_key_v1"].unique()) - self.cohort_keys
        if bad:
            raise AggHold(f"GUARD cohort_admission: {len(bad)} security(ies) outside the "
                          f"frozen 476 cohort reached aggregation")
        ms = minute_states
        if len(ms) and (ms.minute_ts.min() < o or ms.minute_ts.max() >= c):
            raise AggHold(
                f"GUARD input_scope: a minute outside the regular lattice "
                f"[{o}, {c}) reached aggregation — pre-market, post-market and "
                f"close-boundary rows are inadmissible")
        if len(ms) and (ms.session_date != day).any():
            raise AggHold("GUARD cross_session: aggregation may not cross an XNYS session "
                          "boundary")
        bad_state = set(ms.observation_state.unique()) - {"OBSERVED", "UNOBSERVED"}
        if bad_state:
            raise AggHold(f"GUARD observation_state: {bad_state}; STRUCTURAL_NO_TRADE "
                          f"capability is UNAVAILABLE and may not be derived")
        if bars is not None and len(bars):
            if bars.minute_ts.min() < o or bars.minute_ts.max() >= c:
                raise AggHold("GUARD input_scope: a bar outside the regular lattice reached "
                              "aggregation")

        idx = (0 if w is None else (ms.minute_ts.values - o) // (w * 60000))
        ms = ms.assign(_iv=idx)
        g = ms.groupby(["security_key_v1", "_iv"]).agg(
            expected=("minute_ts", "size"),
            unobserved=("observation_state", lambda s: int((s == "UNOBSERVED").sum())))
        g = g.reset_index()
        g["observed"] = g.expected - g.unobserved
        g["coverage_state"] = g.unobserved.gt(0).map(
            {True: "UNOBSERVED_CONTAMINATED", False: "COMPLETE"})

        meta = {i["idx"]: i for i in ivs}
        g["bar_start"] = g._iv.map(lambda i: meta[i]["start"])
        g["bar_end"] = g._iv.map(lambda i: meta[i]["end"])
        g["interval_minutes"] = g._iv.map(lambda i: meta[i]["minutes"])
        g["is_stub"] = g._iv.map(lambda i: meta[i]["stub"])
        # availability is the END, never the start — this is what makes a coarse parent causal
        g["feature_information_available_at"] = g.bar_end
        g["timeframe"] = tf
        g["session_date"] = day

        for i in g._iv.unique():
            if i not in meta:
                raise AggHold(f"GUARD phantom_interval: index {i} has no calendar interval "
                              f"on {day}")
        if w is not None:
            # Checked against the CALENDAR, not against the interval's own declaration.
            # An earlier version compared `stub` to `minutes` only, so padding BOTH of them
            # together looked self-consistent and slipped through. The session span cannot be
            # padded, so it is the thing to measure against.
            total = sum(i["minutes"] for i in ivs)
            if total != n:
                raise AggHold(
                    f"GUARD stub_padding: intervals declare {total} minutes but the calendar "
                    f"session is {n}; a stub may never be padded to {w}")
            for i in ivs:
                span = (i["end"] - i["start"]) // 60000
                if i["minutes"] != span:
                    raise AggHold(
                        f"GUARD stub_padding: interval {i['idx']} declares {i['minutes']} "
                        f"minutes but spans {span} on the calendar")
                if i["end"] > c or i["start"] < o:
                    raise AggHold(f"GUARD phantom_interval: interval {i['idx']} lies outside "
                                  f"the session")
                if i["stub"] != (i["minutes"] != w):
                    raise AggHold(f"GUARD stub_flag: interval {i['idx']} has {i['minutes']} "
                                  f"minutes and stub={i['stub']}")

        if not with_ohlcv or bars is None or not len(bars):
            g["open"] = g["high"] = g["low"] = g["close"] = g["volume"] = None
            return g.drop(columns=["_iv"]).assign(_iv=g._iv)

        b = bars.assign(_iv=(0 if w is None else (bars.minute_ts.values - o) // (w * 60000)))
        agg = b.sort_values("minute_ts").groupby(["security_key_v1", "_iv"]).agg(
            o_=("o", "first"), h_=("h", "max"), l_=("l", "min"), c_=("c", "last"),
            v_=("v", "sum"))
        g = g.merge(agg, left_on=["security_key_v1", "_iv"], right_index=True, how="left")
        # contaminated intervals never carry OHLCV
        m = g.coverage_state == "UNOBSERVED_CONTAMINATED"
        for col in ("o_", "h_", "l_", "c_", "v_"):
            g.loc[m, col] = None
        g = g.rename(columns=dict(o_="open", h_="high", l_="low", c_="close",
                                  v_="volume"))
        return g

    # ------------------------------------------------------------ validate
    def validate(self, g):
        if g.duplicated(subset=["security_key_v1", "timeframe", "_iv"]).any():
            raise AggHold("GUARD duplicate_interval: repeated (security, tf, interval)")
        if (g.coverage_state == "STRUCTURAL_NO_TRADE").any():
            raise AggHold("GUARD structural_no_trade: capability UNAVAILABLE")
        bad = g[(g.coverage_state == "UNOBSERVED_CONTAMINATED") & g["close"].notna()]
        if len(bad):
            raise AggHold(f"GUARD contaminated_ohlcv: {len(bad)} contaminated interval(s) "
                          f"carry OHLCV; a contaminated bar is not a complete observation")
        bad = g[(g.coverage_state == "COMPLETE") & (g.unobserved > 0)]
        if len(bad):
            raise AggHold(f"GUARD contaminated_marked_complete: {len(bad)} interval(s)")
        if (g.feature_information_available_at != g.bar_end).any():
            raise AggHold("GUARD premature_availability: a coarse bar may not become "
                          "available before its final constituent minute completes")
        if (g.bar_start >= g.bar_end).any():
            raise AggHold("GUARD interval_bounds: non-positive interval")
        return True
