"""company_graph/extract.py — turning a filing passage into a typed edge.

This is the only file in the package where a model is allowed to have an opinion, and it
is fenced on both sides.

THE QUOTE GATE
    Every edge must come back with a `quote`. Before the edge is built, the quote is
    searched for in the passages that were actually sent. If it is not there — verbatim,
    modulo whitespace — the edge is DISCARDED and counted as a rejection.

    This is not a validation nicety. It converts "please don't hallucinate" from an
    instruction into a property of the system. A model that invents a supplier has to
    invent a sentence to go with it, and that sentence will not be in the document. The
    instruction can be ignored; the check cannot.

    Rejections are counted and returned, not swallowed. A rising rejection rate is the
    signal that the extractor has started drifting, and it is only visible if nobody
    quietly discards the evidence of it.

WHAT THE MODEL IS ASKED, AND WHAT IT IS NOT ASKED
    It is asked: does THIS PASSAGE state a relationship between these two companies, and
    which one. It is not asked what it knows about the companies.

    The distinction is not pedantic — it was caught in real output. Synopsys genuinely
    supplies design tools to NVIDIA; every model knows that. But the passage EDGAR
    returned was about NVIDIA buying Synopsys shares. An extractor reporting
    "SUPPLIES_TO — well known" would produce a claim its own citation does not support,
    which is worse than no claim, because the citation makes it look checked.

    So the prompt makes NO_RELATIONSHIP a first-class answer, and the examples that teach
    the format include one.

WHY IT MAY NOT NAME A COMPANY
    The counterparty is fixed by the caller from EDGAR's filer index. The model classifies
    a relationship between two already-identified entities; it never resolves an entity.
    Company-name → ticker is where these graphs usually rot, and it is a lookup here, not
    a guess.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

from company_graph.contract import Edge, Evidence, ContractViolation

log = logging.getLogger(__name__)

EXTRACTOR_VERSION = "sec-passage-extractor/1.1"

# Measured, not assumed. Same 7 filings, same prompt, haiku-4.5 vs sonnet-5:
#
#   AMZN   haiku: COMPETES_WITH        sonnet: none      ← haiku read co-defendants in a
#                                                          patent suit as competition
#   MRVL   haiku: none                 sonnet: OWNS, PARTNER_OF
#   LCID   haiku: none                 sonnet: PARTNER_OF
#   CRWV / SNPS / CBRS                 identical
#
# Better precision AND better recall, so there is no trade to weigh. Note the false
# positive survived an explicit instruction not to treat co-appearance in a list as a
# relationship — the prompt fix was tried first and did not work, which is why the model
# tier is the fix rather than more wording.
_MODEL = os.environ.get("COMPANY_GRAPH_MODEL", "claude-sonnet-5")

_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _WS.sub(" ", (s or "")).strip().lower()


SYSTEM = """\
You classify business relationships stated in SEC filing text. You are given a passage \
from company A's filing that mentions company B, and you report ONLY what the passage \
itself says about how A and B are related.

You are NOT being asked what you know about these companies. Outside knowledge is a \
failure here, not a contribution: a claim you cannot support with a sentence from the \
passage is worse than no claim, because the citation makes it look verified.

If the passage does not state a business relationship — it is a stock table, a risk \
factor listing industry names, an index constituent, a passing comparison — return \
NO_RELATIONSHIP. That is a correct and common answer.

CO-APPEARANCE IN A LIST IS NOT A RELATIONSHIP. This is the most common way to be wrong \
here. Two companies named as co-defendants in the same lawsuit, listed together as \
members of an index or an industry, named in the same sentence as examples, or appearing \
side by side in a table of holdings have NO relationship established by that passage. \
Being sued by the same plaintiff is not competing. Return NO_RELATIONSHIP for these, \
even though both company names are plainly present.

THE RELATIONSHIP MUST BE BETWEEN A AND B, NOT BETWEEN B AND SOMEONE ELSE. Filings often \
describe what company B is doing with a THIRD company — "B announced a partnership with \
C", "B acquired C". That passage tells you about B and C. It says nothing about A and B, \
and reporting it as an A–B relationship attaches a real quote to a claim the quote does \
not make. Return NO_RELATIONSHIP unless the passage states how A itself stands to B.

Every relationship you report must include `quote`: an EXACT substring of the passage, \
copied character for character, that a reader can check. Do not paraphrase, do not fix \
typos, do not join two separate sentences. Quotes that are not found verbatim in the \
passage are discarded.

Respond with valid JSON only."""

_PROMPT = """\
COMPANY A (the filer, author of this text): {a_name}{a_tick}
COMPANY B (the company being asked about):  {b_name}{b_tick}

PASSAGES FROM A's {form} filed {date}:
{passages}

Relationship vocabulary, from A's point of view:
  SUPPLIES_TO           A sells B goods or services that B builds with
  PROVIDES_EQUIPMENT_TO A sells B the machines B manufactures with
  PROVIDES_MATERIAL_TO  A sells B raw material
  MANUFACTURES_FOR      A physically makes what B sells under B's name
  CUSTOMER_OF           A buys from B
  COMPETES_WITH         A and B sell into the same demand
  SUBSTITUTE_FOR        A's product can replace B's
  PARTNER_OF            joint development, licensing, co-selling
  OWNS                  A holds equity in B  (if B holds equity in A, that is B→A: \
report it as OWNS with "reversed": true)
  DEPENDS_ON            a dependency is stated but none of the above fits

Return JSON:
{{
  "relationships": [
    {{
      "rel_type": "<one of the above>",
      "reversed": false,
      "component": "<what flows along it, if the passage names it; else \\"\\">",
      "confidence": "LOW|MEDIUM|HIGH|CONFIRMED",
      "is_mandated_disclosure": false,
      "share_pct": null,
      "quote": "<exact substring of the passage>"
    }}
  ]
}}

Rules for the fields that decide how much this edge is trusted:
  confidence HIGH      the passage states the relationship directly
  confidence MEDIUM    it is clearly implied but not stated
  confidence LOW       it is suggested only
  is_mandated_disclosure  true ONLY when the passage is a required disclosure that \
quantifies dependence — a named customer as a percentage of revenue, a stated \
single-source supplier, a segment concentration. Not for ordinary prose.
  share_pct            a number ONLY when the passage states a percentage; else null.

If the passages state several distinct relationships, return several entries. The same \
pair of companies can legitimately be both a customer and an investee.
If none, return {{"relationships": []}}.
"""


def _parse_json(txt: str) -> Optional[dict]:
    if not txt:
        return None
    t = txt.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except Exception:                                                 # noqa: BLE001
        m = re.search(r"\{.*\}", t, re.S)
        if not m:
            return None
        try:
            return json.loads(m.group(0))
        except Exception:                                             # noqa: BLE001
            return None


def extract_edges(target: dict, filer: dict, passages: list[dict],
                  form: str = "", date: str = "", url: str = "",
                  model: str | None = None) -> dict:
    """Read passages from `filer`'s filing and return typed edges about `target`.

    `target` and `filer` are {'name', 'ticker'} dicts resolved from EDGAR — the model
    receives them as fixed facts and cannot introduce a third company.

    Returns {'edges': [...], 'rejected': [...], 'n_passages': int, 'raw': ...}. The
    rejections matter as much as the edges: they are how a drifting extractor becomes
    visible.
    """
    from claude_client import ask                                     # noqa: PLC0415

    if not passages:
        return {"edges": [], "rejected": [], "n_passages": 0, "error": "no passages"}

    joined = "\n\n---\n\n".join(p["text"] for p in passages)
    prompt = _PROMPT.format(
        a_name=filer.get("name", ""), a_tick=f" ({filer['ticker']})" if filer.get("ticker") else "",
        b_name=target.get("name", ""), b_tick=f" ({target['ticker']})" if target.get("ticker") else "",
        form=form or "filing", date=date or "", passages=joined)

    raw = ask(prompt, system=SYSTEM, max_tokens=1600, model=model or _MODEL)
    if raw is None:
        return {"edges": [], "rejected": [], "n_passages": len(passages),
                "error": "model unavailable"}
    data = _parse_json(raw)
    if not data or "relationships" not in data:
        return {"edges": [], "rejected": [], "n_passages": len(passages),
                "error": "unparseable model output", "raw": raw[:400]}

    haystack = _norm(joined)
    edges, rejected = [], []

    for rel in data.get("relationships") or []:
        quote = (rel.get("quote") or "").strip()

        # THE GATE. A quote that is not in the document did not come from the document.
        if not quote or _norm(quote) not in haystack:
            rejected.append({"reason": "quote not found in passages",
                             "rel_type": rel.get("rel_type"), "quote": quote[:200]})
            continue

        mandated = bool(rel.get("is_mandated_disclosure"))
        share = rel.get("share_pct")
        tier = "FILING_DISCLOSURE" if mandated else "FILING_MENTION"
        # A percentage is only storable as a disclosure; if the model gave one without
        # claiming the disclosure, the number is dropped and the edge kept.
        if share is not None and not mandated:
            share = None

        src, dst = filer.get("ticker") or f"CIK{filer.get('cik','')}", target.get("ticker", "")
        if rel.get("reversed"):
            src, dst = dst, src

        try:
            edges.append(Edge(
                src=src, dst=dst, rel_type=rel.get("rel_type", ""),
                claimed_confidence=(rel.get("confidence") or "MEDIUM").upper(),
                component=(rel.get("component") or "")[:120],
                share_pct=float(share) if share is not None else None,
                valid_from=date or None,
                extractor=EXTRACTOR_VERSION,
                evidence=Evidence(
                    tier=tier, source_url=url,
                    source_label=f"{filer.get('ticker') or filer.get('name','')} "
                                 f"{form} {date}".strip(),
                    quote=quote, doc_date=date or None),
            ))
        except (ContractViolation, ValueError, TypeError) as exc:
            rejected.append({"reason": f"contract: {exc}", "rel_type": rel.get("rel_type"),
                             "quote": quote[:200]})

    return {"edges": edges, "rejected": rejected, "n_passages": len(passages),
            "model": model or _MODEL, "extractor": EXTRACTOR_VERSION}


__all__ = ["extract_edges", "EXTRACTOR_VERSION"]
