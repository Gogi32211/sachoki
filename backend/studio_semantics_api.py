"""Transport for Analytic Studio: the browser receives meaning, never operands.

`props.metric` instead of `props.value` disciplines a developer. It does not stop one, because
a serialised `{"metric": {"value": 0.065}}` still hands the component two floats and the minus
sign is right there. The type is advice; the payload is capability.

So the domain object and the transport object are deliberately different types:

    DOMAIN        SemanticMetric(value: float)          server only
    TRANSPORT     SemanticMetricView(display_value: str) crosses the wire
    PRESENTATION  MetricCard / Inspector

`display_value` is a STRING. `0.065` can be shown, copied, inspected and reasoned about by a
human — and cannot be subtracted, because no component ever receives a number.

WHERE LEGITIMATE ARITHMETIC LIVES. Not banned, but sanctioned. A component that needs a delta
asks for a `ComparisonArtifact`, which is produced ONLY after `ComparisonGuard` passes and which
arrives with its own display strings and a compatibility proof. For N0's G1/G2 that object
cannot be constructed at all — the guard raises — so no chart, no axis and no winner badge can
exist for that pair anywhere downstream.

THE NAME MATTERS. Calling the response `SemanticMetric` would invite someone, months from now,
to treat it as the serialisation of the domain object and add `value` back "for convenience".
`SemanticMetricView` is never a computational object, and that sentence is the whole contract.
"""
from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import semantic_inspector as INS                                    # noqa: E402
import studio_foundation as FOUND                                   # noqa: E402
from semantic_metric import (ComparisonSemanticsError, SemanticMetric,  # noqa: E402
                             assert_comparable, can_compare)


@dataclass(frozen=True)
class SemanticMetricView:
    """What crosses the wire. Every field is a string; there is nothing here to compute with."""
    metric_id: str
    display_value: str
    display_units: str
    label: str
    semantic_type: str
    integrity_status: str
    conclusion_status: str
    population_summary: str
    inspector_ref: str

    def __post_init__(self):
        for f, v in asdict(self).items():
            if not isinstance(v, str):
                raise TypeError(
                    f"SemanticMetricView.{f} is {type(v).__name__}; every field must be a "
                    f"string. A numeric field here would hand the presentation layer an "
                    f"operand, and the whole point is that it never receives one.")


@dataclass(frozen=True)
class ComparisonArtifact:
    """The only sanctioned route to a delta. Obtainable solely after the guard passes."""
    comparison_id: str
    left_display: str
    right_display: str
    difference_display: str
    ratio_display: str
    compatibility_proof: str


# ── the registry: domain objects live here and never leave ───────────────────
def _registry() -> dict:
    fx = FOUND.fixtures()
    return {
        "n0.g1.fwer_search": fx["G1"],
        "n0.g2.fwer_search": fx["G2"],
        "v2.n1.theta_deterministic": fx["N1"],
        "v2.n2.theta_estimate": fx["N2"],
    }


def _pop(m: SemanticMetric) -> str:
    from semantic_metric import Known
    p = m.population
    return str(p.value) if isinstance(p, Known) else str(p)


def to_view(metric_id: str, m: SemanticMetric) -> SemanticMetricView:
    """Domain → transport. This is the only place a float becomes text, and it is one-way."""
    return SemanticMetricView(
        metric_id=metric_id,
        display_value=f"{m.value:.4g}" if isinstance(m.value, (int, float)) else str(m.value),
        display_units=m.units,
        label=m.label or m.estimand,
        semantic_type=m.semantic_type,
        integrity_status=m.integrity_status,
        conclusion_status=m.renderable_conclusion,
        population_summary=_pop(m),
        inspector_ref=metric_id,
    )


def inspector(metric_id: str) -> dict:
    """The passport. It explains a number; it does not supply a computational primitive."""
    reg = _registry()
    if metric_id not in reg:
        raise KeyError(metric_id)
    m = reg[metric_id]
    d = INS.build(m)
    return {
        "metric_id": metric_id,
        "headline": d.headline if not isinstance(d.headline, (int, float)) else str(d.headline),
        "subhead": d.subhead, "badge": d.badge, "banner": d.banner,
        "sections": [{"title": s.title, "emphasis": s.emphasis,
                      "rows": [[str(x) for x in r] for r in s.rows]} for s in d.sections],
    }


def n0_screen() -> dict:
    """Two cards and the boundary between them. Nothing that could become a shared axis."""
    reg = _registry()
    ids = ["n0.g1.fwer_search", "n0.g2.fwer_search"]
    r = can_compare(reg[ids[0]], reg[ids[1]])
    return {
        "screen": "N0 · STRUCTURED NULL",
        "metrics": [asdict(to_view(i, reg[i])) for i in ids],
        "comparison": {
            "comparable": r.comparable, "reason_code": r.reason_code,
            "message": r.detail, "left": r.left, "right": r.right,
        },
    }


def comparison_artifact(left_id: str, right_id: str) -> ComparisonArtifact:
    """Raises for an incompatible pair, so no delta exists to serialise."""
    reg = _registry()
    a, b = reg[left_id], reg[right_id]
    assert_comparable(a, b)                     # raises ComparisonSemanticsError
    r = can_compare(a, b)
    return ComparisonArtifact(
        comparison_id=f"{left_id}|{right_id}",
        left_display=f"{a.value:.4g}{a.units}", right_display=f"{b.value:.4g}{b.units}",
        difference_display=f"{a.value - b.value:+.4g}{a.units}",
        ratio_display=f"{a.value / b.value:.4g}×" if b.value else "n/a",
        compatibility_proof=r.detail)


# ── FastAPI surface, mounted separately so nothing restarts implicitly ───────
def build_router():
    from fastapi import APIRouter, HTTPException
    router = APIRouter(prefix="/api/studio/semantics", tags=["studio-semantics"])

    @router.get("/n0")
    def _n0():
        return n0_screen()

    @router.get("/metric/{metric_id}")
    def _metric(metric_id: str):
        try:
            return inspector(metric_id)
        except KeyError:
            raise HTTPException(404, f"no metric {metric_id!r}")

    @router.get("/compare/{left_id}/{right_id}")
    def _compare(left_id: str, right_id: str):
        try:
            return asdict(comparison_artifact(left_id, right_id))
        except ComparisonSemanticsError as e:
            raise HTTPException(409, {"error": "COMPARISON_BLOCKED", "detail": str(e)})
        except KeyError as e:
            raise HTTPException(404, str(e))

    return router
