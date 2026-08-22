"""Two provenance corrections, recorded BEFORE GANN_CAPABILITY_RESULT_V1 seals.

1  THE REGISTERED SECONDARY OUTCOME IS NOT MATERIALISED.

   GANN_OUTCOME_SPEC_V1 freezes four components. Two are percentage-scale and two are
   ATR-normalised with the anchor ATR20 at D-1. The 1D store has atr_14 and no ATR20
   column, so the registered secondary outcome COULD NOT BE BUILT. What was built from
   atr_14 is a different quantity that happens to have the same shape.

       MFE_ATR14_* != the registered secondary outcome

   The earlier progress note called this a "normaliser deviation". That wording was too
   soft: a deviation suggests the registered thing exists in an altered form. It does not
   exist. The status is therefore split three ways rather than blended into one.

   This does not touch the capability, which reads only the percentage-scale pair — the
   registered injection is additive in percentage points. The 360-world run continues.

   What it DOES bind: after historical exposure, MFE_ATR14 may not occupy the column the
   registered ATR20 secondary would have occupied, and may not carry any characterization
   wording that the registered secondary would have earned. There are two clean futures —
   add a PIT-correct ATR20[D-1] and materialise the registered secondary exactly, or leave
   the secondary formally UNAVAILABLE. A post-Y ATR14 -> ATR20 amendment is not one of them.

2  NaN IS A HARD PRECONDITION OF THE RANK INPUT, NOT A HANDLED CASE.

   gann_rank has no NaN policy. Under NaN its answer depends on how its stable lexsort
   happens to order the NaN rows among themselves: each NaN is its own tie group, so
   consecutive ranks are handed to particular rows and sum_rt moves with that ordering.
   That is unstable behaviour in the REFERENCE, which the vectorised twin exposed rather
   than introduced. Rather than define NaN semantics after the fact, the evidentiary domain
   is restricted to where the reference is well defined.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402


def outcome_status():
    d = json.load(open("GANN_OUTCOME_VALUES_V1.json"))
    d["component_status"] = {
        "MFE_LONG_10D": "MATERIALISED / CONFORMANT",
        "MFE_SHORT_10D": "MATERIALISED / CONFORMANT",
        "MFE_ATR20_LONG_10D": "NOT MATERIALISED — SOURCE FEATURE UNAVAILABLE",
        "MFE_ATR20_SHORT_10D": "NOT MATERIALISED — SOURCE FEATURE UNAVAILABLE",
        "MFE_ATR14_LONG_10D": "AUXILIARY / NON-REGISTERED — CANNOT SUBSTITUTE FOR ATR20",
        "MFE_ATR14_SHORT_10D": "AUXILIARY / NON-REGISTERED — CANNOT SUBSTITUTE FOR ATR20"}
    d["registered_secondary_outcome"] = dict(
        spec_name=["MFE_ATR_LONG_10D", "MFE_ATR_SHORT_10D"],
        anchor="ATR20 at D-1",
        status="UNAVAILABLE",
        reason="the 1D store carries atr_14 and no ATR20 column; the registered quantity "
               "was never computed",
        supersedes_wording="an earlier note called this a 'normaliser deviation'. It is not "
                           "a deviation, it is an absence — the registered component does "
                           "not exist in any form",
        permitted_futures=["add a PIT-correct ATR20[D-1] and materialise the registered "
                           "secondary exactly",
                           "leave the registered secondary formally UNAVAILABLE"],
        forbidden="a post-Y amendment redefining the ATR anchor from ATR20 to ATR14, and "
                  "any use of MFE_ATR14 in the place, column or characterization wording "
                  "the registered secondary would have held")
    d["capability_impact"] = ("NONE — the registered capability reads only the "
                              "percentage-scale pair, because the registered injection is "
                              "additive in percentage points")
    d["amended_at"] = time.strftime("%Y-%m-%d %H:%M %Z")
    return ART.seal(d, "GANN_OUTCOME_VALUES_V1.json",
                    required=("spec_id", "entry_semantics", "rows", "component_status"),
                    supersede=True)


def rank_input_contract():
    return ART.seal(dict(
        spec_id="GANN_RANK_INPUT_CONTRACT_V1", status="FROZEN",
        applies_to="every outcome vector entering gann_rank.z_rank or any qualified twin "
                   "of it",
        contract=["isfinite(Y) == TRUE for every row of the evaluated population",
                  "any NaN or +/-inf: HARD FAIL before ranking",
                  "no imputation",
                  "no NaN ordering semantics",
                  "no silent row removal"],
        why="gann_rank has no NaN policy. Under NaN its result depends on how its stable "
            "lexsort orders the NaN rows among themselves — each NaN is its own tie group, "
            "so consecutive ranks fall to particular rows and the treated midrank sum moves "
            "with that ordering. Rather than define NaN semantics after the fact, the "
            "evidentiary domain is restricted to where the reference is well defined.",
        discovered_by="the vectorised twin, whose sort is not stable, disagreed with the "
                      "reference on synthetic NaN input; the disagreement is a property of "
                      "the reference, not a defect of the twin",
        enforcement=dict(
            module="gann_fast.assert_finite",
            called="once per world in gann_capability_run.load_common, on both components "
                   "of the direction-neutral pair before any world runs",
            behaviour="raises; no world executes on a non-finite outcome"),
        evidentiary_state=dict(
            outcome_values=ART.file_digest("GANN_OUTCOME_VALUES_V1.json"),
            nan_count=0, path_complete="2,869,721 of 2,869,721",
            note="the contract is currently vacuous on this data, which is exactly the "
                 "state it is meant to guarantee"),
        reference_untouched="gann_rank.py is not modified; the contract sits in front of it",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "GANN_RANK_INPUT_CONTRACT_V1.json",
        required=("spec_id", "contract", "enforcement"),
        supersede=os.path.exists("GANN_RANK_INPUT_CONTRACT_V1.json"))


if __name__ == "__main__":
    print(f"GANN_OUTCOME_VALUES_V1 (amended) · {outcome_status()}")
    print(f"GANN_RANK_INPUT_CONTRACT_V1       · {rank_input_contract()}")
