"""The fourth contract, tested: a gate that infers cannot decide from a number.

R6 read `r_diff >= -0.25` — a point estimate against a margin — and passed 67-75% of the time
when nothing had been done to returns. Invariant 6 was supposed to prevent exactly this and
did not, because it guards the REPORT layer while R6 lives in the DECISION layer. These are
the regressions for that gap, plus the acceptance criterion for v2 stated as a test rather
than a promise.
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio_verdict import (Estimate, DecisionContractError, Substreams, evidence,
                            materiality, non_inferiority, decide, PASS, FAIL, UNRESOLVED,
                            MATERIALITY, VALIDITY, NON_INFERIORITY)

R = []
def chk(name, fn):
    try:
        fn(); R.append((name, True, ""))
    except AssertionError as e:
        R.append((name, False, str(e)[:150]))
    except Exception as e:
        R.append((name, False, f"{type(e).__name__}: {str(e)[:130]}"))

def E(est, lo, hi, **kw):
    d = dict(level=0.95, estimand="median_return_lift", method="clustered-bootstrap",
             cluster_unit="date", n_raw=17798, n_eff=5210)
    d.update(kw)
    return Estimate(estimate=est, ci_low=lo, ci_high=hi, **d)


def _raw_floats_refused():
    for gate, args in ((evidence, (-0.17,)), (materiality, (-0.17, 0.30)),
                       (non_inferiority, (-0.17, 0.25))):
        try:
            gate(*args)
            raise AssertionError(f"{gate.__name__} decided from a bare float")
        except DecisionContractError:
            pass
chk("a bare float is refused by every inference gate", _raw_floats_refused)


def _provenance_required():
    for bad in (dict(method=""), dict(cluster_unit=""), dict(estimand="")):
        try:
            E(0.5, 0.2, 0.9, **bad)
            raise AssertionError(f"Estimate accepted empty {list(bad)[0]}")
        except DecisionContractError:
            pass
    try:
        Estimate(estimate=0.5, ci_low=0.6, ci_high=0.9, level=0.95, estimand="x",
                 method="m", cluster_unit="date", n_raw=1, n_eff=1)
        raise AssertionError("estimate outside its own interval was accepted")
    except DecisionContractError:
        pass
chk("provenance fields and interval sanity are enforced", _provenance_required)


def _null_means_equivalence():
    s, _ = materiality(E(0.02, -0.12, 0.15), 0.30)
    assert s == FAIL, f"interval inside ±0.30 should establish equivalence, got {s}"
    s, _ = materiality(E(0.10, -0.80, 1.00), 0.30)
    assert s == UNRESOLVED, f"a straddling interval is not a null, got {s}"
chk("NULL requires equivalence, not a wide interval around zero", _null_means_equivalence)


def _the_v1_defect_cannot_recur():
    """return_v1: estimate +0.547, interval clear of zero, δ*=0.30, every validity gate PASS.
    v1 rejected it because 0.547 < 1.0. v2 must not be able to."""
    d = decide(branch="return", effect=E(0.547, 0.32, 0.78), delta_star=0.30,
               temporal_stability=True, n_eff_ok=True, oos_reserved=True, multiplicity=True)
    assert d.status == "BUILD", f"v1's rejected effect got {d.status} in v2"
    assert d.blocking_layer is None
chk("the return_v1 defect cannot recur (+0.547 at δ*=0.30 now BUILDs)",
    _the_v1_defect_cannot_recur)


def _risk_v1_defect_cannot_recur():
    """risk_v1: Δp = 12pp resolved in 100% of replications, R1/R3/R4 all pass, and R2 alone
    refused because RR 1.35 < 1.5 on a 34% baseline."""
    d = decide(branch="risk", effect=E(11.9, 10.8, 13.0, estimand="delta_tail_loss"),
               delta_star=0.30, direction="positive",
               temporal_stability=True, n_eff_ok=True, oos_reserved=True, multiplicity=True)
    assert d.status == "VETO", f"risk_v1's rejected effect got {d.status}"
chk("the risk_v1 defect cannot recur (Δ=11.9 no longer blocked by a ratio)",
    _risk_v1_defect_cannot_recur)


def _governance_keeps_its_veto():
    d = decide(branch="return", effect=E(2.0, 1.5, 2.5), delta_star=0.30,
               temporal_stability=False, n_eff_ok=True, oos_reserved=True, multiplicity=True)
    assert d.status == "REJECT" and d.blocking_layer == VALIDITY, (
        "a large real effect on an unstable study must still be refusable")
chk("governance keeps its veto on validity grounds", _governance_keeps_its_veto)


def _acceptance_criterion():
    """No result may end as evidence YES + materiality YES + all validity PASS + rejected on
    magnitude. Magnitude IS materiality in v2, so the state is unreachable by construction —
    swept here rather than asserted."""
    import numpy as np
    for est in np.arange(0.31, 3.0, 0.07):
        d = decide(branch="return", effect=E(est, est - 0.005, est + 0.005),
                   delta_star=0.30, temporal_stability=True, n_eff_ok=True,
                   oos_reserved=True, multiplicity=True)
        assert d.status == "BUILD", (
            f"effect {est:+.2f} beyond δ* with clean validity was refused as {d.status}")
chk("acceptance criterion: no rejection on magnitude alone (swept 0.31→3.0)",
    _acceptance_criterion)


def _non_inferiority_is_a_test():
    p, _ = non_inferiority(E(-0.08, -0.16, 0.02, estimand="return_cost"), 0.25)
    assert p == PASS, "a tight interval clear of the margin should pass"
    u, _ = non_inferiority(E(-0.08, -0.41, 0.19, estimand="return_cost"), 0.25)
    assert u == UNRESOLVED, "a wide interval cannot establish non-inferiority"
    f, _ = non_inferiority(E(-0.60, -0.80, -0.40, estimand="return_cost"), 0.25)
    assert f == FAIL, "established harm must fail"
    d = decide(branch="risk", effect=E(1.0, 0.7, 1.3, estimand="delta_tail_loss"),
               delta_star=0.30, cost=E(-0.08, -0.41, 0.19, estimand="return_cost"),
               epsilon=0.25, temporal_stability=True, n_eff_ok=True, oos_reserved=True,
               multiplicity=True)
    assert d.status == "UNRESOLVED" and d.blocking_layer == NON_INFERIORITY
chk("non-inferiority answers PASS / FAIL / UNRESOLVED, not a coin flip",
    _non_inferiority_is_a_test)


def _substreams_are_independent():
    a, b = Substreams(7), Substreams(7)
    x1 = a("bootstrap_return").normal(size=5)
    _ = a("noninferiority").normal(size=99)          # a new component appears
    x2 = b("bootstrap_return").normal(size=5)
    assert (x1 == x2).all(), "adding a component shifted an existing stream"
    assert not (a("bootstrap_return").normal(size=5) ==
                a("bootstrap_risk").normal(size=5)).all(), "streams are not independent"
chk("named RNG substreams keep a paired comparison paired", _substreams_are_independent)


if __name__ == "__main__":
    print("=" * 96)
    print("  DECISION PROVENANCE — the fourth contract")
    print("=" * 96)
    for name, ok, why in R:
        print(f"  {'✅' if ok else '🔴'} {name}")
        if not ok:
            print(f"       {why}")
    n = sum(1 for _, ok, _ in R if ok)
    print("=" * 96)
    print(f"  {n}/{len(R)} PASS")
    sys.exit(0 if n == len(R) else 1)
