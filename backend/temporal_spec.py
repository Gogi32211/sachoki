"""Temporal integrity — frozen before the runner, and deliberately NOT a statistical test.

The last barrier asks one binary question:

    did the system see a specific record before the moment it was entitled to see it?

An earlier draft of this spec asked something else — whether a poison feature built from the
future carries predictive power at decision time — and expected ≈ 0. That criterion would have
manufactured a false leak. `attach()` joins backwards, so at decision time it hands back the
PREVIOUS record, and

    Y_{t-1}  ⊥̸  Y_t

is honestly true in this data: regimes persist, momentum exists, trades overlap, and a market
day moves everything on it together. The machinery could be flawless and the predictive check
would still light up. Worse, it fails in the direction that looks like diligence.

So the primary criterion is IDENTITY, not correlation:

    decision_time_i <  available_time_i   ⇒  record_i MUST NOT be observable
    decision_time_i >= available_time_i   ⇒  record_i MAY be observable

Checked by `record_id`, so the test is about the actual byte and not about a statistic computed
from it. Predictive quantities appear only as SECONDARY diagnostics, and never decide PASS.

NO SamplingTarget HERE, and that is deliberate. `observed_before_available_time == 0` is a
contract result, not an estimate; it has no sampling distribution, no interval, and no
conditioning on a realised history. `sampling_target.py` exists to stop statistical semantics
being attached to the wrong experiment — attaching it to a deterministic assertion would be the
same mistake in the opposite direction. Rates, CIs and detection probabilities appear only in
the INTEGRATION layer below, and those do carry their target.

Two layers, kept apart because an access failure and a sensitivity failure need different fixes:

    ACCESS       can the byte be reached early                deterministic, binary
    INTEGRATION  is a known effect found only after publication   statistical

If ACCESS passes and INTEGRATION does not recover a strong synthetic signal after publication,
that is downstream plumbing or sensitivity — not a temporal leak. Reading one as the other is
the attribution error this project has now made three times.
"""
from __future__ import annotations

import hashlib
import json

# ── T1 · poison on real opportunities ────────────────────────────────────────
# A synthetic record is created for every opportunity, carrying a payload drawn INDEPENDENTLY of
# anything in the past and published only at the trade's exit. Independence matters: if the
# payload were a function of the past, the previous record would legitimately predict the next
# one and the secondary diagnostic would be uninterpretable.
T1 = dict(
    name="poison_access",
    record_key="opportunity_id",
    available_time="date_out",
    decision_time="date_in",
    primary=dict(
        own_future_record_visible=0,          # its own record, before its own exit
        any_record_with_available_gt_decision=0,
    ),
    secondary="predictive metric of the attached payload — DIAGNOSTIC ONLY, never PASS/FAIL",
)

# ── T2 · real SEC facts, correct and deliberately wrong anchors ──────────────
# The correct anchor is `filed`. The wrong anchor is `period_end`, which is what a naive join
# would use and what the fundamentals work was built to avoid.
#
# The acceptance criterion is NOT "filed shows no signal": a fact anchored at `filed` may
# genuinely predict returns, and that is research, not leakage. The criterion is exposure.
T2 = dict(
    name="sec_anchor",
    correct_anchor="filed",
    forbidden_anchor="period_end",
    primary=dict(records_observed_before_filed_under_correct_anchor=0),
    quantify_under_forbidden=("contaminated_opportunities", "median_lead_days",
                              "p90_lead_days", "max_lead_days"),
    secondary="Δ predictive performance between anchors — the economic consequence of "
              "intentional leakage, reported as a diagnostic, not as the verdict",
)

# ── T3 · synthetic PIT feed ──────────────────────────────────────────────────
# Born at t, published at t + LAG. A real filing shifted by a few days is a bad positive control:
# the market genuinely moves in those days, so a recovered "signal" may be real.
LAG_DAYS = 21
INJECTED_DELTA = 4.0          # pp, well above the search-layer sensitivity measured for v1
HORIZON = "path_sim_exit"

T3 = dict(
    name="synthetic_pit",
    lag_days=LAG_DAYS,
    access=dict(pre_publication_visibility=0.0, post_publication_exact_match=1.0),
    integration=dict(
        before_publication="injected effect NOT recovered",
        after_publication="injected effect recovered",
        injected_delta=INJECTED_DELTA,
        horizon=HORIZON,
        detection="bootstrap CI excludes zero and lower bound clears δ*/2",
    ),
)

LAYERS = {"ACCESS": ("T1.primary", "T2.primary", "T3.access"),
          "INTEGRATION": ("T3.integration",)}

# ACCESS is pass/fail with no tolerance. One record reachable early is a leak; there is no
# interval within which a leak is acceptable.
ACCESS_TOLERANCE = 0


def digest() -> str:
    return hashlib.sha256(json.dumps(
        {"T1": T1, "T2": T2, "T3": T3, "layers": {k: list(v) for k, v in LAYERS.items()},
         "access_tolerance": ACCESS_TOLERANCE}, sort_keys=True).encode()).hexdigest()


if __name__ == "__main__":
    print(f"TEMPORAL SPEC DIGEST  {digest()}")
    for t in (T1, T2, T3):
        print(f"\n  {t['name']}")
        for k, v in t.items():
            if k != "name":
                print(f"    {k:<28s} {v}")
