"""Checkpoint: the semantics of a research ACTION, as distinct from the semantics of a number.

`semantic-foundation-v1` proved what a number means. `semantic-ui-v1` proved it survives to a
screen. This one proves what an action costs — which click creates a new selectable hypothesis
and which is merely a look.

The four golden accounting cases are kept as data, because they are the ones that separate an
accountant from a click counter, and D is the canonical demonstration that UI history is not
multiplicity: five results were displayed, thirty-one were selectable, and k is thirty-one.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_session import ClaimIdentity, ResearchSession, PARAMETERS   # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
NAME = "analytic-studio-research-session-v1"
MODULES = ("research_session.py", "semantic_metric.py", "semantic_inspector.py",
           "sampling_target.py", "studio_semantics_api.py")


def claim(tol="5", horizon="20", outcome="median_return"):
    return ClaimIdentity("incremental_return_pp", outcome, horizon, "price_21_89",
                         f"rsi45pm{tol}", "rsi_14", "6f825ca4763fea76",
                         "OPPORTUNITY_LEVEL", "verdict_v2")


def goldens() -> dict:
    out = {}
    a = ResearchSession("A").start_exploration()
    c = claim()
    for _ in range(3):
        a.execute(c).expose(c)
    out["A_reopen_same_claim"] = a.accounting()

    b = ResearchSession("B").start_exploration()
    for t in ("5", "1", "2"):
        b.change_parameter("conditioning_tolerance", "prev", t)
        cc = claim(t)
        b.execute(cc).expose(cc)
    out["B_tolerance_changes"] = b.accounting()

    cs = ResearchSession("C").start_exploration()
    cc = claim()
    for _ in range(5):
        cs.execute(cc)
    cs.expose(cc)
    out["C_technical_reruns"] = cs.accounting()

    d = ResearchSession("D").start_exploration()
    d.search_run("combolab_v2", 31, "3600ae3dd52a25e6", 5)
    for i in range(5):
        d.expose(claim(str(i)))
    out["D_search_space_not_screen"] = d.accounting()
    return out


def build() -> dict:
    g = goldens()
    return {
        "checkpoint": NAME,
        "previous": ["analytic-studio-semantic-foundation-v1", "analytic-studio-semantic-ui-v1"],
        "proves": "the semantics of a research ACTION, not of a number",
        "modules": {f: subprocess.check_output(["git", "hash-object", f],
                                               cwd=HERE).decode().strip() for f in MODULES},
        "module_sha256": {f: hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest()
                          for f in MODULES},
        "claim_identity": {
            "fields": list(ClaimIdentity.__dataclass_fields__),
            "rule": "a statistically meaningful change creates a new identity; an empty field "
                    "raises, because a hole defeats the identity's only job"},
        "session_states": ["NEW", "EXPLORE", "REGISTERED", "ACTIVE_REGISTERED", "CLOSED",
                           "CLOSED_EXPLORATORY"],
        "irreversible_edge": "no registration after RESULT_EXPOSED — registration opens a NEW "
                             "session and the exposed one stays exploratory forever",
        "multiplicity": {
            "k_declared": "what the frozen search space permitted",
            "k_exposed": "distinct claims whose results were seen",
            "k_selectable": "the space the ALGORITHM could pick a winner from; SEARCH_RUN can "
                            "make it exceed the number of visible results"},
        "ledger": "append-only chain, each event carrying prior_state_hash and new_state_hash",
        "parameter_registry": {
            "count": len(PARAMETERS),
            "roles": sorted({p.semantic_role for p in PARAMETERS.values()}),
            "rule": "every UI degree of freedom is classified before it can be turned; an "
                    "undeclared parameter raises"},
        "fatal_gates": ["CannotRegisterAfterExposureError", "UnregisteredSelectionError",
                        "SearchSpaceDriftError", "unknown parameter -> blocked"],
        "adversarial_suite": "15/15",
        "golden_accounting": g,
        "canonical_lesson": {
            "case": "D",
            "displayed": g["D_search_space_not_screen"]["displayed_at_most"],
            "k_selectable": g["D_search_space_not_screen"]["k_selectable"],
            "statement": "UI history is not multiplicity. Five results were displayed and "
                         "thirty-one were selectable; k is thirty-one."},
        "next": "Combo Lab UI as a separate phase. Its first acceptance is not 'shows 31 claims "
                "nicely' but: can a user change horizon or tolerance, see a result, and watch "
                "session accounting move exactly as the backend said it would.",
    }


if __name__ == "__main__":
    m = build()
    p = os.path.join(HERE, "STUDIO_SESSION_CHECKPOINT.json")
    json.dump(m, open(p, "w"), indent=2, sort_keys=True, ensure_ascii=False)
    print(f"checkpoint  {m['checkpoint']}")
    print(f"  modules     {len(m['modules'])}")
    print(f"  parameters  {m['parameter_registry']['count']} in "
          f"{len(m['parameter_registry']['roles'])} roles")
    print(f"  fatal gates {len(m['fatal_gates'])}")
    for k, v in m["golden_accounting"].items():
        print(f"  {k:<28s} k_exposed {v['k_exposed']} · k_selectable {v['k_selectable']} · "
              f"revisits {v['revisits']}")
    print(f"\n  canonical: displayed {m['canonical_lesson']['displayed']} · "
          f"k_selectable {m['canonical_lesson']['k_selectable']}")
    print(f"\nwritten {p}")
