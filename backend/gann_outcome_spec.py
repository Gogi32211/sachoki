"""GANN_OUTCOME_SPEC_V1 — the outcome definitions, sealed while Y is still unread.

    signal / contact   day D (the grid was already frozen at D-1)
    entry              the NEXT regular-session open after D

    primary            DIR_MFE_10D
                         LONG   max_{j=1..10}( High_j / Entry - 1 )
                         SHORT  max_{j=1..10}( 1 - Low_j / Entry )

    secondary          DIR_MFE_ATR_10D
                         LONG   max_{j=1..10}( High_j - Entry ) / ATR20[D-1]
                         SHORT  max_{j=1..10}( Entry - Low_j ) / ATR20[D-1]

                       ATR20 is taken at D-1, not at D: the scale must be fixed by the
                       state that existed BEFORE the contact, or the normaliser would
                       absorb part of the very bar that defined the event.

    post-exposure      DIR_MAE_10D, DIR_RET_10D — descriptive only, never selection

Availability is a STATUS, not a value: an event whose 10-session path is not fully
observable is NOT_YET_MATURE / NO_NEXT_SESSION_OPEN / TERMINAL_DURING_HORIZON and is
excluded from the primary population by that status alone.

No Y value is read anywhere in this module. It writes the definition, not the data.
"""
from __future__ import annotations
import json, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

OUT = "GANN_OUTCOME_SPEC_V1.json"


def main():
    body = dict(
        spec_id="GANN_OUTCOME_SPEC_V1", status="FROZEN",
        family_id="GANN_VIBRATION_GRID_V1",
        frozen_before="any GANN outcome value is read",
        x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
        signal="day D — the contact; the grid was frozen on data through D-1",
        entry="the next regular-session open after D",
        horizon_sessions=10,
        primary=dict(
            name="DIR_MFE_10D",
            LONG="max_{j=1..10}( High_j / Entry - 1 )",
            SHORT="max_{j=1..10}( 1 - Low_j / Entry )",
            reading="the favourable excursion in the direction fixed at D-1, so a claim "
                    "is tested on whether price moves AWAY from the line on the "
                    "pre-declared side"),
        secondary=dict(
            name="DIR_MFE_ATR_10D",
            LONG="max_{j=1..10}( High_j - Entry ) / ATR20[D-1]",
            SHORT="max_{j=1..10}( Entry - Low_j ) / ATR20[D-1]",
            normaliser="ATR20 at D-1",
            why_d_minus_1="the scale must come from the state that existed before the "
                          "contact; ATR at D would absorb part of the bar that defined "
                          "the event"),
        post_exposure_descriptive=["DIR_MAE_10D", "DIR_RET_10D"],
        availability_statuses=["AVAILABLE", "NOT_YET_MATURE", "NO_NEXT_SESSION_OPEN",
                               "TERMINAL_DURING_HORIZON", "DATA_GAP"],
        availability_rule="status only — an unavailable path is excluded by its status, "
                          "never by its value, and no value is inspected to decide it",
        direction_population="events with direction in {LONG, SHORT}; CONFLUENCE also "
                             "requires consensus, so INVALID_CONFLICT and "
                             "INVALID_EQUALITY are outside the directional population",
        forbidden=["reading any outcome value before the capability protocol and its "
                   "pre-Y gates are sealed and passed",
                   "using MAE or RET for selection",
                   "changing the horizon, the entry rule or the normaliser after any "
                   "outcome is seen"],
        outcome_exposure="NOT_EXPOSED — this artifact defines the outcome, it does not "
                         "read it",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "primary", "secondary", "entry"),
                 supersede=os.path.exists(OUT))
    print(f"GANN_OUTCOME_SPEC_V1 · {d}")


if __name__ == "__main__":
    main()
