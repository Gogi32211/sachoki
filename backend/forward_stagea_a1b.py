"""A1b FORWARD WRAPPER — canonical 1m base -> 15m Stage-A surface.

This file WRAPS the frozen historical producer. It does not reimplement it.

  coverage_state / interval_index / bar_start / interval geometry / OHLCV aggregation
      -> coarse_aggregator_v1.CoarseAggregatorV1   (MASSIVE_1M_COARSE_PRODUCER_CODE_AUTHORITY_V1)

  frozen-476 allowlist / path routing / orchestration / projection
      -> this wrapper

The allowlist is the wrapper's job on purpose. The aggregator already RAISES on a
non-cohort key (GUARD cohort_admission); that is a guard against contamination, not an
admission policy. The forward contract is that raw data MAY exist for other securities
and is simply NOT ADMITTED, so the filter must happen before the aggregator sees it —
otherwise every forward session containing a new S&P name would hard-fail.

The aggregator's source_path guard is NOT bypassed here. `source_root` is passed through
untouched, so a caller pointing at anything other than the production 1m base is refused
by the parent producer exactly as it was historically.
"""
from __future__ import annotations
import os, sys, hashlib                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
from coarse_aggregator_v1 import CoarseAggregatorV1, AggHold, DERIVED_ROOT  # noqa: E402

COHORT_FILE = os.path.join(HERE, "massive_new_family_runtime",
                           "FORWARD_COHORT_476_security_key_v1.txt")
COHORT_DIGEST = "2d1ec872dcf74df4"          # FORWARD_COHORT_BINDING_V1 · 2cbb4b900d8a4a2e
COHORT_N = 476

# the 10 columns stage-A carries, in the order the historical table declares them
STAGEA_COLS = ["k", "session_date", "bar_start", "interval_index", "coverage_state",
               "open", "high", "low", "close", "volume"]


class ForwardHold(Exception):
    """Fail-closed. Never downgraded, never caught to continue."""


def load_cohort(path=COHORT_FILE):
    """The frozen 476 allowlist, verified against the sealed membership digest."""
    if not os.path.exists(path):
        raise ForwardHold(f"GUARD cohort_missing: {path}")
    blob = open(path, "rb").read()
    keys = [l for l in blob.decode().split("\n") if l]
    d = hashlib.sha256(blob).hexdigest()[:16]
    if d != COHORT_DIGEST:
        raise ForwardHold(f"GUARD cohort_digest: {d} != sealed {COHORT_DIGEST}")
    if len(keys) != COHORT_N or len(set(keys)) != COHORT_N:
        raise ForwardHold(f"GUARD cohort_size: {len(keys)} distinct {len(set(keys))}")
    if any(len(k) != 64 for k in keys):
        raise ForwardHold("GUARD cohort_form: a key is not 64 hex characters")
    return set(keys)


def allowlist(ms, bars, cohort):
    """Admit ONLY frozen-476 members. Non-members are dropped, never mapped, never
    substituted. Returns (ms, bars, n_rejected_rows, rejected_keys)."""
    m = ms.security_key_v1.isin(cohort)
    rej = sorted(set(ms.loc[~m, "security_key_v1"].unique()))
    ms2 = ms.loc[m].reset_index(drop=True)
    b2 = None
    if bars is not None and len(bars):
        b2 = bars.loc[bars.security_key_v1.isin(cohort)].reset_index(drop=True)
    return ms2, b2, int((~m).sum()), rej


def calendar():
    import exchange_calendars as xc
    return xc.get_calendar("XNYS")


def build_session(day, ms, bars, cohort, cal, tf="15m", source_root=None):
    """One session through the FROZEN producer, then projected to the stage-A columns.

    Raises AggHold from the parent producer untouched — including the fail-closed
    STRUCTURAL_NO_TRADE guard, which is a refusal to run, not a state it can emit.
    """
    ms, bars, n_rej, rej = allowlist(ms, bars, cohort)
    if not len(ms):
        return None, dict(rejected_rows=n_rej, rejected_keys=rej, rows=0)
    agg = CoarseAggregatorV1(cal, cohort)
    g = agg.aggregate(day, ms, bars, tf, source_root=source_root)
    agg.validate(g)
    g = g.rename(columns={"_iv": "interval_index", "security_key_v1": "k"})
    out = g[STAGEA_COLS].copy()
    out["interval_index"] = out["interval_index"].astype("int64")
    out["bar_start"] = out["bar_start"].astype("int64")
    return out, dict(rejected_rows=n_rej, rejected_keys=rej, rows=len(out))


def self_digest():
    return hashlib.sha256(open(os.path.abspath(__file__), "rb").read()).hexdigest()


if __name__ == "__main__":
    c = load_cohort()
    print(f"cohort OK  n={len(c)}  digest={COHORT_DIGEST}")
    print(f"parent producer 1m base root: {DERIVED_ROOT}")
    print(f"wrapper digest: {self_digest()[:16]}")
