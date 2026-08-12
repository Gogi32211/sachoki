"""The drawer: one renderer, any SemanticMetric, an epistemic passport for a number.

Its whole responsibility is to let a person answer six questions about a figure on screen:

    what kind of number is this
    what does it estimate
    which probabilistic experiment does it belong to
    under what conditions was it obtained
    may it be interpreted at all
    where did it come from

It computes nothing. Compatibility arrives as a finished `ComparisonResult` from the guard;
generator properties are looked up in the registries where they were declared, not derived here.
A renderer that computes is a second computational path, and two paths with different contracts
is the exact shape of the 2026-08-09 bug.

IT RENDERS BY TYPE, NOT AS ONE UNIVERSAL SHEET. A deterministic zero and an estimated +0.0038
are indistinguishable as glyphs; if they share a layout the reader has to find a small caption
to learn which is which, and will not. So DETERMINISTIC shows a basis and an explicit
NOT APPLICABLE for uncertainty, INFERENTIAL shows an interval and a method, DECISION shows the
layers it rests on.

INVALID BREAKS THE LAYOUT ON PURPOSE. A conclusion computed by an experiment that failed
integrity is still shown — it was computed, and hiding it would make investigation harder — but
it is shown as NOT INTERPRETABLE, above the recorded value, not as a footnote beneath it.

UNKNOWN AND NOT APPLICABLE MUST NOT LOOK ALIKE. "We failed to record the cluster unit" and
"a deterministic quantity has no sampling distribution" are different facts. If both render as
an em dash, the absence contract is lost at the presentation layer, which is where contracts
usually die.
"""
from __future__ import annotations

import os
import sys
import textwrap
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from semantic_metric import (DECISION, DESCRIPTIVE, DETERMINISTIC, INFERENTIAL,  # noqa: E402
                             INVALID, NONE_, ComparisonResult, Known, NotApplicable,
                             SemanticMetric, Unknown)

# ── declared properties of registered generators, looked up rather than derived ──
# The limitations of a null generator are part of what a number MEANS, not a footnote on the
# page that happens to display it. G2's circular shift preserves the flag's serial geometry and
# does NOT preserve calendar-era composition; a reader who does not see that will over-read the
# result. Sourced from the specs where these were frozen.
GENERATOR_NOTES = {
    "within_stratum_outcome_v1": {
        "null model": "Y ⊥ Cell | Date, BaseSetup",
        "mechanism": "outcomes permuted inside (date × family); membership frozen",
    },
    "date_level_label_circular_v1": {
        "null model": "Y ⊥ DayProperty, serial geometry of the property held",
        "mechanism": "daily label sequence circularly shifted; outcomes untouched; "
                     "membership, strata and weights recomputed",
        "preserves": "global prevalence · cyclic run structure · flag lag geometry",
        "does not preserve": "calendar-year prevalence — NOT a test conditional on Year/Regime",
    },
    "incremental_composition_generator_v1": {
        "null model": "synthetic world: Y = μ_setup + γ_date + ε, incremental truth planted",
        "mechanism": "operating characteristics under a generator we wrote — tests the code, "
                     "never the market",
    },
    "empirical_cluster_resampling": {
        "null model": "repeated sampling under the empirical cluster-resampling model",
        "does not mean": "what another independent five years would give — resampling observed "
                         "clusters cannot produce an unseen regime",
    },
}


@dataclass(frozen=True)
class Section:
    title: str
    rows: tuple            # ((label, value, note|""), ...)
    emphasis: str = ""     # "", "warning", "block"


@dataclass(frozen=True)
class Drawer:
    headline: str
    subhead: str
    badge: str
    banner: str            # non-empty only when the metric may not be read as usual
    sections: tuple


def _absence(x) -> tuple[str, str]:
    """Known / Unknown / NotApplicable render differently, deliberately."""
    if isinstance(x, Known):
        return str(x.value), ""
    if isinstance(x, Unknown):
        return "UNKNOWN", x.reason
    if isinstance(x, NotApplicable):
        return "NOT APPLICABLE", x.reason
    return str(x), ""


def _conditioning_section(m: SemanticMetric) -> Section:
    rows = []
    for c in m.conditioning:
        if c.operator == "WITHIN":
            rows.append((c.feature, f"{c.center} ±{c.tolerance}{c.unit}", f"hash {c.hash}"))
        else:
            rows.append((c.feature, f"{c.operator} {c.center}", f"hash {c.hash}"))
    rows.append(("condition set", m.conditioning_hash,
                 "a tolerance is part of the research specification; changing it creates a "
                 "different conditioning object"))
    return Section("CONDITIONING", tuple(rows))


def _generator_section(m: SemanticMetric) -> Section | None:
    gid = m.sampling_target.conditioned_on or m.sampling_target.kind
    notes = GENERATOR_NOTES.get(gid) or GENERATOR_NOTES.get(m.sampling_target.kind)
    rows = [("sampling target", str(m.sampling_target), "")]
    if notes:
        for k, v in notes.items():
            rows.append((k, v, ""))
    return Section("EXPERIMENT", tuple(rows))


def _provenance_section(m: SemanticMetric) -> Section:
    p = m.provenance
    return Section("PROVENANCE", (
        ("experiment", p.experiment_id, ""), ("spec", p.spec_hash, ""),
        ("code", p.code_hash, ""), ("data", p.data_version, "")))


def build(m: SemanticMetric, comparison: ComparisonResult | None = None,
          against: str = "") -> Drawer:
    """One drawer, shaped by the metric's own kind."""
    val = f"{m.value:.4g}{m.units}" if isinstance(m.value, (int, float)) else str(m.value)
    banner = ""
    headline = val
    subhead = m.label or m.estimand

    if m.integrity_status == INVALID:
        banner = ("NOT INTERPRETABLE — the conclusion was computed, but the underlying "
                  "experiment failed integrity requirements")
        if m.conclusion_status != NONE_:
            headline = m.conclusion_status
            subhead = "recorded conclusion, not a result"

    sections = []

    if m.semantic_type == DETERMINISTIC:
        u, why = _absence(m.uncertainty)
        sections += [
            Section("BASIS", (("kind", "a property of the construction, not an estimate", ""),
                              ("estimand", m.estimand, ""))),
            Section("UNCERTAINTY", ((("uncertainty"), u, why),)),
        ]
    elif m.semantic_type == INFERENTIAL:
        u, why = _absence(m.uncertainty)
        sections += [
            Section("MEANING", (("estimand", m.estimand, ""),)),
            Section("UNCERTAINTY", (("interval / design", u, why),)),
        ]
    elif m.semantic_type == DESCRIPTIVE:
        sections += [Section("MEANING", (("estimand", m.estimand, ""),
                                         ("kind", "observed, no inference claimed", "")))]
    elif m.semantic_type == DECISION:
        sections += [Section("VERDICT", (
            ("conclusion", m.renderable_conclusion, ""),
            ("integrity", m.integrity_status, ""),
            ("estimand", m.estimand, "")))]

    g = _generator_section(m)
    if g:
        sections.append(g)
    sections.append(_conditioning_section(m))
    pop, why = _absence(m.population)
    sections.append(Section("POPULATION", (("scope", pop, why),)))

    if comparison is not None:
        rows = [("direct comparison" + (f" with {against}" if against else ""),
                 "ALLOWED" if comparison.comparable else "BLOCKED", "")]
        if not comparison.comparable:
            rows += [("reason", comparison.reason_code, comparison.detail)]
            if comparison.left or comparison.right:
                rows += [("this", comparison.left, ""), ("other", comparison.right, "")]
        sections.append(Section("COMPARISON", tuple(rows),
                                emphasis="" if comparison.comparable else "block"))

    sections.append(_provenance_section(m))
    return Drawer(headline=headline, subhead=subhead,
                  badge=f"{m.semantic_type} · {m.integrity_status}",
                  banner=banner, sections=tuple(sections))


def render(d: Drawer, width: int = 74) -> str:
    """Terminal rendering, so the structure can be verified before any frontend exists."""
    out = [d.headline, d.subhead, d.badge]
    if d.banner:
        out += ["", "  ⛔ " + d.banner]
    for s in d.sections:
        out += ["", f"{s.title}"]
        for label, value, note in s.rows:
            out.append(f"  {label:<20s} {value}")
            if note:
                # wrap on words. A naive slice split "different conditioning object" across two
                # lines and the fixture caught it — the note is the part a reader must be able
                # to read, so breaking it mid-phrase defeats the section.
                for line in textwrap.wrap(note, width=max(width - 24, 30)):
                    out.append(f"  {'':<20s} {line}")
    return "\n".join(out)
