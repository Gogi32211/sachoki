"""company_graph/contract.py — what an edge in the dependency graph is allowed to claim.

The brief for this page contained its own most important sentence:

    "AI никогда не должен превращать «возможно является поставщиком» в CONFIRMED SUPPLIER"

That is a rule about behaviour, and rules about behaviour are the ones that quietly stop
being true. This module turns it into arithmetic instead.

THE CEILING RULE
    Every edge carries evidence. Every kind of evidence has a maximum confidence it can
    ever support. An edge's confidence is min(what the extractor claimed, what its
    evidence permits). A model that returns CONFIRMED for something it read in a news
    article does not produce a CONFIRMED edge — it produces a MEDIUM one, because
    NEWS_ARTICLE tops out at MEDIUM. The extractor cannot argue with this; it is applied
    after the extractor has spoken, in `Edge.__post_init__`.

    This is the same shape as the action ceiling in ranking_run.py: authority is inherited
    from the source, never asserted by the consumer.

TWO KINDS OF EDGE, AND THEY MUST NOT LOOK ALIKE
    EVIDENCED   there is a document, with a URL and a date, containing a quote that names
                both companies. Someone can go and read it.
    MODEL_PRIOR there is no document. The model believes it. This is not worthless — it is
                how we know what to search for — but it is a QUESTION, not an answer.

    MODEL_PRIOR edges are excluded from every risk calculation by construction (see
    `counts_toward_risk`). A concentration number computed partly from model guesses is
    worse than no number, because it looks like a measurement.

WHY PRODUCTS AND COMPONENTS ARE MODEL_PRIOR AND THAT IS FINE
    No database maps "GPU" → "HBM memory" → suppliers. The model does that decomposition,
    and it cannot be sourced. But the decomposition is not the output — it is the QUERY.
    "NVIDIA needs HBM" is a hypothesis; searching filings for companies that discuss both
    NVIDIA and HBM is how it becomes an edge with a citation. The unsourceable layer is
    demoted to what it is actually good at: telling us where to look.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

CONTRACT_VERSION = "company-graph/1.0"


# ── relationship vocabulary ───────────────────────────────────────────────────
# "A → B" is not a relationship. The brief was right to insist on this: SUPPLIES_TO and
# COMPETES_WITH between the same two tickers describe opposite exposures, and a graph that
# stores only adjacency cannot tell you which one you are looking at.

REL_TYPES = {
    # upstream — B depends on A
    "SUPPLIES_TO":          "A sells goods or services that B builds with",
    "PROVIDES_EQUIPMENT_TO": "A sells the machines B manufactures with",
    "PROVIDES_MATERIAL_TO": "A sells raw material B consumes",
    "MANUFACTURES_FOR":     "A physically makes what B sells under B's name",
    # downstream — A depends on B
    "CUSTOMER_OF":          "A buys from B (the mirror of SUPPLIES_TO)",
    # lateral
    "COMPETES_WITH":        "A and B sell into the same demand",
    "SUBSTITUTE_FOR":       "A's product can replace B's without B being a rival firm",
    "PARTNER_OF":           "joint development, licensing, or co-selling",
    # structural
    "OWNS":                 "A holds an equity stake in B",
    "DEPENDS_ON":           "asserted dependency with no more specific type known",
}

# which direction the risk flows: if the edge breaks, who feels it
# DEPENDS_ON belongs here: "A depends on B" puts B upstream of A, which is the whole
# point of recording it. Leaving it lateral made a single-source GPU dependency render as
# a sideways relationship in the first live run.
UPSTREAM_RELS = {"SUPPLIES_TO", "PROVIDES_EQUIPMENT_TO", "PROVIDES_MATERIAL_TO",
                 "MANUFACTURES_FOR", "DEPENDS_ON"}
DOWNSTREAM_RELS = {"CUSTOMER_OF"}
LATERAL_RELS = {"COMPETES_WITH", "SUBSTITUTE_FOR", "PARTNER_OF"}


# ── evidence tiers and the ceilings they impose ───────────────────────────────
CONFIDENCE_ORDER = ["LOW", "MEDIUM", "HIGH", "CONFIRMED"]


def _rank(c: str) -> int:
    return CONFIDENCE_ORDER.index(c) if c in CONFIDENCE_ORDER else 0


# The ordering here is not editorial. A customer-concentration disclosure is the only
# item on this list a company is compelled BY LAW to make and to quantify ("Customer A
# accounted for 19% of revenue"), which is why it is the single tier that can reach
# CONFIRMED. Everything below it is someone choosing what to say.
EVIDENCE_TIERS = {
    "FILING_DISCLOSURE": {
        "max_confidence": "CONFIRMED",
        "what": "a mandated disclosure in a 10-K/10-Q: customer concentration, "
                "named single-source supplier, segment revenue",
    },
    "FILING_MENTION": {
        "max_confidence": "HIGH",
        "what": "both companies named in the same filing passage, relationship stated "
                "in prose rather than disclosed as a required item",
    },
    "WEB_SEARCH_CITATION": {
        "max_confidence": "MEDIUM",
        "what": "a cited web source returned by search, with URL",
    },
    "NEWS_ARTICLE": {
        "max_confidence": "MEDIUM",
        "what": "a news item naming both parties",
    },
    "PEER_CLASSIFICATION": {
        "max_confidence": "MEDIUM",
        "what": "shared SIC/industry code — supports COMPETES_WITH only, and only weakly: "
                "two firms in SIC 3674 may not compete for the same customer at all",
    },
    "MODEL_PRIOR": {
        "max_confidence": "LOW",
        "what": "no document. The model asserts it. A search target, not a finding.",
    },
}

EVIDENCED_TIERS = set(EVIDENCE_TIERS) - {"MODEL_PRIOR"}


class ContractViolation(ValueError):
    """Raised when something tries to store a claim the contract does not permit."""


@dataclass(frozen=True)
class Evidence:
    """A document someone can go and read. `quote` is what makes it checkable."""
    tier: str
    source_url: str = ""
    source_label: str = ""          # "NVDA 10-K 2026-02-25", "Reuters", ...
    quote: str = ""                 # the passage the claim was read out of
    doc_date: Optional[str] = None  # when the SOURCE was published, not when we fetched it
    retrieved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self):
        if self.tier not in EVIDENCE_TIERS:
            raise ContractViolation(f"unknown evidence tier {self.tier!r}")
        # An evidenced tier without a URL is a model prior wearing a costume — this is the
        # exact failure the page exists to prevent, so it is refused at construction.
        if self.tier in EVIDENCED_TIERS and not self.source_url:
            raise ContractViolation(
                f"tier {self.tier} claims a document but carries no source_url. "
                f"An evidenced tier with nothing to read is a MODEL_PRIOR mislabelled.")

    @property
    def max_confidence(self) -> str:
        return EVIDENCE_TIERS[self.tier]["max_confidence"]


@dataclass
class Edge:
    """One typed, sourced, dated claim about two companies."""
    src: str                        # ticker, or CIK-prefixed id for unlisted firms
    dst: str
    rel_type: str
    evidence: Evidence
    claimed_confidence: str = "MEDIUM"
    component: str = ""             # what flows along the edge ("HBM memory", "foundry")
    product: str = ""               # which of dst's products it feeds
    share_pct: Optional[float] = None   # only from FILING_DISCLOSURE; else None
    valid_from: Optional[str] = None
    extractor: str = ""             # who made this claim, so a bad extractor is revocable

    # set in __post_init__ — never assigned by callers
    confidence: str = field(init=False, default="LOW")
    status: str = field(init=False, default="MODEL_PRIOR")
    ceiling_applied: bool = field(init=False, default=False)

    def __post_init__(self):
        if self.rel_type not in REL_TYPES:
            raise ContractViolation(f"unknown rel_type {self.rel_type!r}")
        if self.claimed_confidence not in CONFIDENCE_ORDER:
            raise ContractViolation(f"unknown confidence {self.claimed_confidence!r}")
        if self.src == self.dst:
            raise ContractViolation(f"self-edge on {self.src}")

        ceiling = self.evidence.max_confidence
        # THE CEILING RULE. The extractor's opinion is an upper bid, not a verdict.
        if _rank(self.claimed_confidence) > _rank(ceiling):
            self.confidence = ceiling
            self.ceiling_applied = True
        else:
            self.confidence = self.claimed_confidence
        self.status = "EVIDENCED" if self.evidence.tier in EVIDENCED_TIERS else "MODEL_PRIOR"

        # A percentage is a disclosure or it is invented. There is no third way to know
        # that a customer is 19% of revenue.
        if self.share_pct is not None and self.evidence.tier != "FILING_DISCLOSURE":
            raise ContractViolation(
                f"share_pct={self.share_pct} on a {self.evidence.tier} edge. A revenue "
                f"share is a mandated disclosure; from any other source it is a guess "
                f"with a decimal point on it.")

    @property
    def counts_toward_risk(self) -> bool:
        """Risk arithmetic sees evidenced edges only.

        A concentration score built partly from model guesses does not degrade gracefully —
        it reads as a measurement while being an opinion, which is worse than showing
        nothing at all.
        """
        return self.status == "EVIDENCED"

    @property
    def direction(self) -> str:
        if self.rel_type in UPSTREAM_RELS:
            return "UPSTREAM"
        if self.rel_type in DOWNSTREAM_RELS:
            return "DOWNSTREAM"
        return "LATERAL"

    def key(self) -> tuple:
        return (self.src, self.dst, self.rel_type, self.component)

    def to_row(self) -> dict:
        e = self.evidence
        return {
            "src": self.src, "dst": self.dst, "rel_type": self.rel_type,
            "direction": self.direction, "component": self.component,
            "product": self.product, "share_pct": self.share_pct,
            "confidence": self.confidence, "status": self.status,
            "ceiling_applied": self.ceiling_applied,
            "claimed_confidence": self.claimed_confidence,
            "evidence_tier": e.tier, "source_url": e.source_url,
            "source_label": e.source_label, "quote": e.quote[:2000],
            "doc_date": e.doc_date, "retrieved_at": e.retrieved_at,
            "valid_from": self.valid_from, "extractor": self.extractor,
            "contract_version": CONTRACT_VERSION,
        }


def model_prior(src: str, dst: str, rel_type: str, why: str = "", **kw) -> Edge:
    """Build an unsourced edge. Deliberately verbose to call: the friction is the point."""
    return Edge(src=src, dst=dst, rel_type=rel_type, claimed_confidence="LOW",
                evidence=Evidence(tier="MODEL_PRIOR", source_label="model prior", quote=why),
                **kw)


# ── country semantics ─────────────────────────────────────────────────────────
# The brief separated these and it matters more than it looks: a company can be American,
# listed in America, and unable to make anything if one Taiwanese fab stops. Collapsing
# these into one "country" field is how a supply-chain map ends up saying nothing.
COUNTRY_ROLES = {
    "HQ":            "where the company is headquartered (EDGAR business address)",
    "INCORPORATION": "where it is legally incorporated (often DE and meaningless)",
    "LISTING":       "where its shares trade",
    "MANUFACTURING": "where the physical making happens — usually NOT the same",
    "SUPPLY":        "where a company it depends on sits",
}

__all__ = ["CONTRACT_VERSION", "REL_TYPES", "UPSTREAM_RELS", "DOWNSTREAM_RELS",
           "LATERAL_RELS", "EVIDENCE_TIERS", "EVIDENCED_TIERS", "CONFIDENCE_ORDER",
           "Evidence", "Edge", "ContractViolation", "model_prior", "COUNTRY_ROLES"]
