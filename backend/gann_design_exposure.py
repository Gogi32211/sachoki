"""GANN_DESIGN_EXPOSURE_LEDGER_V1 — opened BEFORE any ticker is looked at.

Decision 13: USAR is design-exposed. Anything inspected while building the grid enters
this ledger BEFORE inspection and is excluded from the primary confirmatory population.
The ledger is opened now, empty except for the pre-declared entry, so that no later
addition can be mistaken for a pre-registration.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

OUT = "GANN_DESIGN_EXPOSURE_LEDGER_V1.json"


def main():
    entries = [dict(ticker="USAR", reason="the grid hypothesis and its Pine rendering "
                                          "were developed while looking at this chart",
                    declared_at="family opening, before any Gann V1 computation",
                    status="DESIGN_EXPOSED — excluded from the primary confirmatory "
                           "evidence population")]
    prior = json.load(open(OUT)) if os.path.exists(OUT) else None
    if prior:
        known = {e["ticker"] for e in prior.get("entries", [])}
        entries = prior["entries"] + [e for e in entries if e["ticker"] not in known]
    body = dict(
        spec_id="GANN_DESIGN_EXPOSURE_LEDGER_V1", status="OPEN",
        rule="a ticker enters this ledger BEFORE it is inspected, never after; entries "
             "are excluded from the primary confirmatory population and may appear only "
             "in a separately labelled descriptive set",
        entries=entries,
        n_entries=len(entries),
        opened_at=time.strftime("%Y-%m-%d %H:%M %Z") if not prior
        else prior.get("opened_at"),
        updated_at=time.strftime("%Y-%m-%d %H:%M %Z"),
        outcome_exposure="NOT_EXPOSED")
    d = ART.seal(body, OUT, required=("spec_id", "rule", "entries"),
                 supersede=os.path.exists(OUT))
    print(f"GANN_DESIGN_EXPOSURE_LEDGER_V1 · {d} · entries "
          + ", ".join(e["ticker"] for e in entries))


if __name__ == "__main__":
    main()
