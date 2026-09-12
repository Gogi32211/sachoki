"""MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 — three-valued minute availability.

WHAT THIS CORRECTS. The frozen charter says, of a minute with no vendor bar:

    "an absent minute means ZERO volume, not missing volume"

That is too strong, and in a way that manufactures data. It is correct ONLY when the
minute genuinely existed as a trading minute, the security was eligible in it, and the
source interval covering it is PROVEN complete — i.e. when the vendor had every
opportunity to emit a bar and legitimately did not, because nothing traded. A minute
absent because of a vendor outage, an uncertain pagination boundary, a coverage gap, or
unresolved eligibility is not a zero. Recording it as one converts missing data into a
measured value of zero, which is measurement error introduced by the pipeline itself and
is invisible downstream: a zero looks like an observation.

The distinction is not cosmetic. slot-RVOL and CUM-RVOL divide by per-slot baselines. A
structural no-trade belongs in that denominator as a real zero. An unobserved minute must
NOT — including it drags the baseline down and inflates every RVOL computed against it,
which is the same direction of error the charter's sparse-minute clause was written to
prevent.

    OBSERVED               a vendor bar exists
    STRUCTURAL_NO_TRADE    the minute existed, the security was eligible, the source
                           interval is proven complete, and no bar was legitimately
                           emitted                                        -> volume 0
    UNOBSERVED             any of those preconditions is unproven          -> NOT zero

This mirrors the three-valued availability discipline already used elsewhere in this
project (AVAILABLE_TRUE / AVAILABLE_FALSE / UNAVAILABLE), where the third state exists
precisely so that "we did not look" can never be recorded as "we looked and found nothing".

THE CHARTER FILE IS NOT REWRITTEN. It stays sealed at its own digest with its original
wording. This amendment is a separate frozen artifact that names the clause it narrows.

Frozen BEFORE any Massive ingestion. No outcome is touched. Nothing is downloaded.
"""
from __future__ import annotations
import os, sys, time                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

CHARTER = "MASSIVE_1M_STATE_TRANSITION_V1.json"


def payload():
    return dict(
        amendment_id="MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1",
        status="FROZEN BEFORE ANY MASSIVE INGESTION",
        reason="distinguish STRUCTURAL_NO_TRADE from UNOBSERVED source absence",
        amends=dict(
            artifact=CHARTER,
            digest=ART.file_digest(CHARTER) if os.path.exists(CHARTER) else None,
            clause="aggregation_contract.sparse_minutes.volume_rule",
            original_wording="an absent minute means ZERO volume, not missing volume",
            defect="true only when the minute was a real trading minute, the security was "
                   "eligible, and source coverage is proven complete. Applied "
                   "unconditionally it converts vendor outages, pagination gaps and "
                   "coverage holes into measured zeros.",
            charter_not_rewritten="the charter remains sealed at its own digest with its "
                                  "original text; this artifact narrows the clause"),

        states=dict(
            OBSERVED=dict(
                definition="a vendor bar exists for the minute",
                values="as returned by the vendor",
                enters_rvol_baseline=True),
            STRUCTURAL_NO_TRADE=dict(
                definition="an expected trading minute in which the security was eligible, "
                           "the covering source interval is proven complete, and no bar "
                           "was legitimately emitted because nothing traded",
                preconditions_ALL_REQUIRED=[
                    "the minute lies inside a session per the SEALED exchange calendar, "
                    "accounting for holidays and early closes",
                    "the security was index-eligible on that date per the PIT universe "
                    "contract",
                    "the source interval covering the minute PASSED the pagination "
                    "completeness gate (strict monotonicity, zero duplicates, zero "
                    "page-boundary gaps, deterministic replay)",
                    "no trading halt covering the minute is recorded",
                    "the vendor emitted no bar"],
                values=dict(volume=0, transaction_count=0, price="NULL", vwap="NULL"),
                forward_fill="NEVER — the absence of a trade is not the persistence of a "
                             "price",
                enters_rvol_baseline=True,
                note="this is a real observation whose value is zero"),
            UNOBSERVED=dict(
                definition="any STRUCTURAL_NO_TRADE precondition is unproven",
                triggers=["coverage incomplete", "pagination boundary uncertain",
                          "vendor gap or outage", "eligibility uncertain",
                          "source unavailable", "exchange calendar unresolved for the date",
                          "halt status unknown"],
                values=dict(volume="INVALID/UNAVAILABLE", transaction_count=
                            "INVALID/UNAVAILABLE", price="NULL", vwap="NULL"),
                explicitly_not_zero="an UNOBSERVED minute is NEVER coerced to zero volume, "
                                    "in storage, in aggregation, or in any feature",
                enters_rvol_baseline=False,
                note="this is the absence of an observation, not an observation")),

        default_state=dict(
            rule="UNOBSERVED is the DEFAULT for any absent minute",
            promotion="a minute is promoted to STRUCTURAL_NO_TRADE only when every "
                      "precondition is affirmatively demonstrated",
            why="the burden of proof runs toward the conservative state. A pipeline that "
                "defaults to zero and downgrades on discovered problems will silently "
                "record zeros for every problem it never discovered."),

        propagation=dict(
            rule="a coarser bar (15m/1H/1D) whose window contains ANY UNOBSERVED minute is "
                 "flagged UNOBSERVED_CONTAMINATED and carries the count of such minutes",
            why="summing volume over a window with unobserved minutes understates it, and "
                "the understatement is invisible in the result",
            usage="a contaminated bar is not silently dropped and not silently used; each "
                  "study declares its own tolerance, and the declaration is part of that "
                  "study's specification",
            vwap="a window whose observed volume is 0 has NULL vwap regardless of whether "
                 "the zero is structural or contaminated"),

        rvol_consequence=dict(
            slot_rvol="the per-slot baseline is computed over OBSERVED and "
                      "STRUCTURAL_NO_TRADE minutes only; UNOBSERVED minutes are excluded "
                      "from both numerator and denominator",
            cum_rvol="a cumulative curve crossing an UNOBSERVED minute is flagged from "
                     "that point forward for that session",
            reported="every RVOL-family feature reports the count of excluded UNOBSERVED "
                     "minutes alongside its value"),

        assert_contract_additions=[
            "every stored minute carries an explicit availability state from the three "
            "above; a null or missing state is a fatal ingestion error",
            "no row may carry availability=STRUCTURAL_NO_TRADE together with a non-null "
            "price or a non-zero volume",
            "no row may carry availability=UNOBSERVED together with any numeric volume, "
            "including zero",
            "a STRUCTURAL_NO_TRADE row must reference the completeness-gate result that "
            "licensed its promotion"],

        outcome_exposure="NOT_EXPOSED",
        authorises="nothing — this narrows a definition and starts no download",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))


def main():
    p = payload()
    d = ART.seal(p, "MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json",
                 required=("amendment_id", "status", "reason", "amends", "states",
                           "default_state", "propagation"),
                 supersede=os.path.exists("MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1.json"))
    print(f"MASSIVE_1M_DATA_CONTRACT_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  amends {p['amends']['artifact']} ({p['amends']['digest']}) "
          f"clause {p['amends']['clause']}")
    print(f"  states  : OBSERVED · STRUCTURAL_NO_TRADE (volume 0) · UNOBSERVED (NOT zero)")
    print(f"  default : UNOBSERVED — promotion requires ALL "
          f"{len(p['states']['STRUCTURAL_NO_TRADE']['preconditions_ALL_REQUIRED'])} "
          f"preconditions proven")


if __name__ == "__main__":
    main()
