"""The only forward surface that exists before the registered look: a day counter.

    WAITING FOR NOVEL EVIDENCE
    0 / 30 novel trading days until the first registered look

That is the whole screen, and the restraint is the feature. A panel that showed θ drifting while
we waited would be a nightly look with a progress bar attached, and every freeze above it —
estimand, support, bootstrap, decision rule, ranking, boundary, adapter — would be undone by the
person watching it.

So this route reads DATES and never outcomes. It loads one column, computes how many distinct
trading days lie beyond the frozen boundary, and hands back counts. `operational_status` applies
`assert_no_outcome_fields` to its own payload before returning it, so a future edit that adds a
"just a preview" field fails here rather than on the screen.
"""
from __future__ import annotations

import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PARQUET = os.path.join(os.path.dirname(HERE), "data", "opportunities.parquet")

_CACHE: dict = {"at": 0.0, "payload": None}
_TTL = 900.0            # the count moves once a day at most; a live counter would be theatre


def _novel_dates():
    """One column, and only the dates. The outcome column is not read on this path."""
    import pandas as pd                                               # noqa: PLC0415
    df = pd.read_parquet(PARQUET, columns=["sig_date"])
    return df["sig_date"].astype(str).str[:10].to_numpy()


def status(force: bool = False) -> dict:
    import forward_observation_policy as OP                           # noqa: PLC0415
    now = time.time()
    if not force and _CACHE["payload"] is not None and now - _CACHE["at"] < _TTL:
        return _CACHE["payload"]
    try:
        dates = _novel_dates()
        payload = OP.operational_status(dates)
        payload["source"] = "opportunities.parquet · sig_date only"
    except FileNotFoundError:
        payload = OP.operational_status()
        payload["source"] = "no snapshot readable"
    except OP.ForwardObservationPolicyError as e:
        payload = {"state": "NO_OBSERVATION_POLICY", "detail": str(e)}
    _CACHE.update(at=now, payload=payload)
    return payload


def build_router():
    from fastapi import APIRouter                                     # noqa: PLC0415

    router = APIRouter(prefix="/api/studio/forward", tags=["studio-forward"])

    @router.get("/status")
    def forward_status(refresh: bool = False):
        return status(force=refresh)

    return router
