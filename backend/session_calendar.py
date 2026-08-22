"""The ONE authoritative session-completeness helper for every 1H family.

Registered rule (frozen grammar contract, restored by SESSION_VALIDITY_REMEDIATION_
AMENDMENT_V1 and unified here by SESSION_FILTER_UNIFICATION_AMENDMENT_V1):

    expected_bars(date) = the modal bar count ACROSS ALL TICKERS THAT TRADED THAT DATE
    a ticker-session is COMPLETE iff its observed bar count equals expected_bars(date)

Ties resolve to the LARGEST tied value (SESSION_MODE_TIE_POLICY_V1) — never downward.

Grammar and estimand both call THIS module. Neither may recompute an expected session
length of its own; that divergence is what this helper exists to make impossible.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, os                                                    # noqa: E402
import pandas as pd                                                   # noqa: E402

CAL_PARQ = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "session_calendar_1h.parquet")
_CACHE = {}


def expected_bars_by_date() -> dict:
    """The sealed mapping date -> expected bar count. Read once, never recomputed here."""
    if "map" not in _CACHE:
        C = pd.read_parquet(CAL_PARQ)
        _CACHE["map"] = dict(zip(C.session_date.astype(str),
                                 C.calendar_len.astype(int)))
        _CACHE["digest"] = hashlib.sha256(
            "\n".join(f"{k}={v}" for k, v in sorted(_CACHE["map"].items()))
            .encode()).hexdigest()[:16]
    return _CACHE["map"]


def mapping_digest() -> str:
    """Identity of the mapping actually in use — both sides must report the same value."""
    expected_bars_by_date()
    return _CACHE["digest"]


def session_is_complete(frame: pd.DataFrame) -> pd.Series:
    """Boolean per row: does this row's session have exactly its scheduled bar count?

    `frame` needs `session_date` and `bars_in_session`. No modal is computed here — the
    expected length comes from the sealed market-wide mapping, so a family's own episode
    mix cannot influence it.
    """
    exp = frame["session_date"].astype(str).map(expected_bars_by_date())
    return frame["bars_in_session"] == exp


def complete_sessions(frame: pd.DataFrame) -> pd.DataFrame:
    """The rows of `frame` that belong to a complete session."""
    return frame[session_is_complete(frame)]
