"""The guard the temporal barrier found missing: order that the data does not contain.

The last validation stage failed on 51 rows, and the cause was not the as-of join. 253 of
289,467 opportunities carry `date_out == date_in` — same-day round trips, `hold = 1`, none with
an exit strictly before its entry. At day resolution their own outcome is "available" at the
decision moment, so a source keyed on the exit would hand a trade its own result.

The tempting fix is `assert date_out > date_in`. It is wrong. Inside the day the entry really
may have preceded the exit; what the data lacks is not validity but ORDER:

    date_out <  date_in                    the record is corrupt          → FATAL
    date_out == date_in at day resolution  ORDER(entry, exit) = UNKNOWN   → AMBIGUOUS

Deleting those 253 rows would convert a temporal ambiguity into a data-corruption claim and
throw away real executions. They stay. What becomes impossible is using them wrongly — and if an
intraday timestamp ever arrives (`entry 10:03`, `exit 15:42`) the ambiguity dissolves without
touching a single trade.

The general rule, of which `date_out` is only one instance:

    A system may not infer an ordering of events that its data does not contain.

So `available <= decision` is NOT sufficient once both sides have been collapsed to a date.
What is required is

    available_date < decision_date                    or
    available_phase < decision_phase  (both known)

and where neither holds, the record is forbidden as a FEATURE. That is broader and more useful
than a special case about exits.

LINEAGE. The strongest form of the guard is not about dates at all. An outcome-derived field —
`date_out`, realised return, MAE, MFE, stop_hit, target_hit — may never be the availability
field of a FEATURE source, at any resolution. In label space it is exactly what one wants. This
continues the split the project already runs on:

    FEATURE future access  FORBIDDEN
    LABEL future access    a separate space, by construction

That is what protects against "tomorrow someone registers a source keyed on date_out", which is
the scenario the barrier actually surfaced. No real source is keyed that way today: earnings and
insider are keyed on filing dates.

WHAT SEC DOES AND DOES NOT LICENSE. `filed` is a date with no intraday publication time. What is
proven is that a record is unreachable before its filing date; what is NOT proven is that it was
readable before the open on that date. The registry stores this as
`temporal_resolution = DAY, same_day_ordering = UNKNOWN`, which stops `2024-05-15` from quietly
becoming the fictitious timestamp `2024-05-15 00:00:00`.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

DAY, INTRADAY = "DAY", "INTRADAY"
UNKNOWN, KNOWN = "UNKNOWN", "KNOWN"

# Fields computed from what happened AFTER the decision. Legitimate as labels, never as the
# availability anchor of a feature.
OUTCOME_DERIVED = frozenset({
    "date_out", "ret", "ret_true", "mae", "mfe", "hold", "mtm", "stop_hit", "target_hit",
    "exit_price", "exit_date", "future_path", "realized_return",
})


class TemporalContractError(AssertionError):
    """An ordering was assumed that the data does not carry, or a label was used as a feature."""


# ── trade tables ─────────────────────────────────────────────────────────────
def check_trade_table(df: pd.DataFrame, *, entry="date_in", exit_="date_out",
                      resolution: str = DAY) -> dict:
    """FATAL only for an exit strictly before its entry. Same-day is ambiguous, not invalid."""
    a = pd.to_datetime(df[entry].astype(str).str[:10])
    b = pd.to_datetime(df[exit_].astype(str).str[:10])
    corrupt = int((b < a).sum())
    if corrupt:
        raise TemporalContractError(
            f"{corrupt:,} rows have {exit_} strictly before {entry}. That is not ambiguity — "
            f"the record cannot be repaired by finer timestamps.")
    same = (b == a).to_numpy()
    ambiguous = int(same.sum()) if resolution == DAY else 0
    return dict(n=len(df), corrupt=0, ambiguous=ambiguous,
                ambiguous_pct=ambiguous / max(len(df), 1),
                flag="SAME_DAY_ORDER_AMBIGUOUS" if ambiguous else "",
                mask=same if resolution == DAY else np.zeros(len(df), bool),
                note=("order(entry, exit) is UNKNOWN on these rows at day resolution; they are "
                      "valid trades and may not carry exit-derived features on the same "
                      "decision key") if ambiguous else "")


# ── source availability ──────────────────────────────────────────────────────
@dataclass(frozen=True)
class TemporalSpec:
    """What a source knows about WHEN it became knowable — and how precisely."""
    name: str
    availability_field: str
    availability_resolution: str = DAY
    decision_resolution: str = DAY
    temporal_phase: str = UNKNOWN          # KNOWN only with a real intraday ordering
    role: str = "feature"                  # feature | label
    field_lineage: tuple = ()

    def __post_init__(self):
        if self.role == "feature":
            bad = [f for f in (self.availability_field,) + tuple(self.field_lineage)
                   if f in OUTCOME_DERIVED]
            if bad:
                raise TemporalContractError(
                    f"{self.name}: {bad} are outcome-derived and cannot anchor or feed a "
                    f"FEATURE source at any resolution. In label space they are exactly right; "
                    f"here they would hand a decision its own result.")


def may_use_as_feature(spec: TemporalSpec, available_date, decision_date) -> tuple[bool, str]:
    """`available <= decision` is not enough once both sides are dates."""
    a = pd.Timestamp(str(available_date)[:10])
    d = pd.Timestamp(str(decision_date)[:10])
    if spec.role != "feature":
        return True, "label space"
    if a > d:
        return False, "not yet available"
    if a < d:
        return True, "strictly earlier day"
    if spec.availability_resolution == INTRADAY and spec.decision_resolution == INTRADAY \
            and spec.temporal_phase == KNOWN:
        return True, "same date, but intraday phase ordering is known"
    return False, ("FORBIDDEN_FOR_FEATURES: same calendar date, both sides at day resolution, "
                   "no phase ordering — the data does not say which came first")


def assert_registry(specs) -> None:
    """Every feature source must be able to prove ordering, or be refused up front."""
    for s in specs:
        if s.role == "feature" and s.availability_resolution == DAY \
                and s.decision_resolution == DAY and s.temporal_phase == KNOWN:
            raise TemporalContractError(
                f"{s.name}: claims KNOWN phase ordering at day resolution, which is not "
                f"something a date can support")
