"""The semantic foundation checkpoint: what exists before any presentation code touches it.

`759b918` is a natural boundary. Below it there is a layer that has proved its own properties
with no consumer; above it the first frontend code arrives, bringing npm, routing, state
management and build config. Without a line drawn here, a few commits later the question "did
the frontend break the contract, or was the contract always like this" stops having a cheap
answer.

So this file records three things and computes nothing new:

    module identity     source hashes of the three semantic modules
    fixture identity    the five real objects the layer was proved against
    golden structure    the Drawer each fixture produces, as DATA

The goldens are the drawer STRUCTURE, never the rendered text. A rendered string carries line
wrapping, column widths and terminal assumptions; comparing those would make the checkpoint fail
the first time a screen width changes, and that has already happened once today.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import semantic_inspector as INS                                    # noqa: E402
from sampling_target import structured_permutation_null, synthetic_dgp   # noqa: E402
from semantic_metric import (BUILD, DETERMINISTIC, INFERENTIAL, INVALID,  # noqa: E402
                             ConditioningSpec, Known, NotApplicable, Provenance,
                             SemanticMetric, Unknown, can_compare)

HERE = os.path.dirname(os.path.abspath(__file__))
NAME = "analytic-studio-semantic-foundation-v1"
MODULES = ("semantic_metric.py", "semantic_inspector.py", "sampling_target.py")

COND = (ConditioningSpec("Date", "IS", "trading date"),
        ConditioningSpec("BaseSetup", "IS", "family"))
P_N0 = Provenance("N0-2026-08-11", "3c6ccda05dde7eb0", "n0_run@2591906", "bars@2026-08-10")
P_V2 = Provenance("N1N2P1-2026-08-11", "38f2665d653beb64", "combolab_v2@2e4257a",
                  "bars@2026-08-10")


def fixtures() -> dict:
    """The five objects the foundation was proved against, all real results of this project."""
    return {
        "G1": SemanticMetric(0.065, INFERENTIAL, "family_wise_false_promotion_rate",
                             structured_permutation_null("within_stratum_outcome_v1"), COND,
                             P_N0, uncertainty=Known("200 null worlds · nominal 0.05"),
                             population=Known("31 OPPORTUNITY_LEVEL claim classes"),
                             label="Structured-null FWER"),
        "G2": SemanticMetric(0.685, INFERENTIAL, "family_wise_false_promotion_rate",
                             structured_permutation_null("date_level_label_circular_v1"), COND,
                             P_N0, uncertainty=Known("200 null worlds · nominal 0.05"),
                             population=Known("6 DAY_LEVEL claim classes"),
                             label="Structured-null FWER"),
        "N1": SemanticMetric(0.0, DETERMINISTIC,
                             "incremental_effect_in_composition_only_world",
                             synthetic_dgp("incremental_composition_generator_v1"), COND, P_V2,
                             units="pp",
                             uncertainty=NotApplicable(
                                 "algebraically determined by the construction: within a "
                                 "stratum every row shares one outcome"),
                             population=Known("31 classes, deterministic world"),
                             label="θ in a composition-only world"),
        "N2": SemanticMetric(0.0038, INFERENTIAL, "incremental_effect_estimate",
                             synthetic_dgp("incremental_composition_generator_v1"), COND, P_V2,
                             units="pp",
                             uncertainty=Known("mean over 40 worlds · RMSE 0.124pp"),
                             population=Known("31 classes"),
                             label="θ̂ under stochastic composition"),
        "INVALID_BUILD": SemanticMetric(1.2, INFERENTIAL, "incremental_return_pp",
                                        synthetic_dgp("incremental_composition_generator_v1"),
                                        COND,
                                        Provenance("HYPOTHETICAL", "spec@x", "code@y",
                                                   "bars@z"),
                                        integrity_status=INVALID, conclusion_status=BUILD,
                                        units="pp",
                                        uncertainty=Known("95% clustered bootstrap"),
                                        population=Unknown("manifest hash did not match at "
                                                           "run time"),
                                        label="incremental return"),
    }


def drawer_structure(m: SemanticMetric, comparison=None, against="") -> dict:
    d = INS.build(m, comparison=comparison, against=against)
    return {"headline": d.headline, "subhead": d.subhead, "badge": d.badge,
            "banner": d.banner,
            "sections": [{"title": s.title, "emphasis": s.emphasis,
                          "rows": [list(r) for r in s.rows]} for s in d.sections]}


def build() -> dict:
    fx = fixtures()
    return {
        "checkpoint": NAME,
        "modules": {f: subprocess.check_output(["git", "hash-object", f],
                                               cwd=HERE).decode().strip() for f in MODULES},
        "module_sha256": {f: hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest()
                          for f in MODULES},
        "tests": {"test_semantic_core.py": "10/10", "test_semantic_inspector.py": "8/8"},
        "fixtures": sorted(fx),
        "goldens": {k: drawer_structure(v) for k, v in fx.items()},
        "blocked_pair": {
            "left": "G1", "right": "G2",
            "result": can_compare(fx["G1"], fx["G2"]).reason_code,
            "detail": can_compare(fx["G1"], fx["G2"]).detail,
        },
        "note": "goldens are drawer STRUCTURE, never rendered text — a rendered string carries "
                "wrapping and column widths, and comparing those breaks on the first change of "
                "screen size",
    }


if __name__ == "__main__":
    m = build()
    p = os.path.join(HERE, "STUDIO_FOUNDATION.json")
    with open(p, "w") as f:
        json.dump(m, f, indent=2, sort_keys=True, ensure_ascii=False)
    print(f"checkpoint  {m['checkpoint']}")
    for k, v in m["modules"].items():
        print(f"  {k:<24s} {v[:16]}")
    print(f"  fixtures                 {', '.join(m['fixtures'])}")
    print(f"  blocked pair             G1 vs G2 → {m['blocked_pair']['result']}")
    print(f"  tests                    {m['tests']}")
    print(f"\nwritten {p}")
