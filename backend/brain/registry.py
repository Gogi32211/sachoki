"""brain/registry.py — the living FINDINGS REGISTRY (knowledge core of the decision brain).

Every VALIDATED discovery — an edge, a law, a discriminator, a null, a gate — is one record
here. This is the single source of truth the decision-spine reads: "what do we actually know,
and what should be DONE with it". New research auto-updates the brain by calling register().

Design goals: additive & isolated (no import of the live app), append-only-ish (update by id),
JSON-backed (git-diffable, human-readable), machine-queryable. This is Layer-9 (feedback) made
concrete: findings → knowledge → decisions.

A record:
  id          short slug (stable key)
  type        edge | law | discriminator | null | gate
  layer       which of the 9 decision layers it informs (0..9)
  direction   long | short | both | none
  action      what the brain DOES with it: signal | boost | disqualify | size | context | none
  title       one-liner
  definition  precise condition
  stats       {median, win, pf, years_pos, dsr, pbo, ...}  (optional)
  tier        core | watch | null
  status      live | research | retired
  col         edge_replay mask column, if any (links knowledge → live detector)
  source      memory slug / script that established it
  date        YYYY-MM-DD it was established
"""
from __future__ import annotations
import json
import os
from typing import Optional

_DIR = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.join(_DIR, "findings.json")

_VALID_TYPES = {"edge", "law", "discriminator", "null", "gate"}
_VALID_ACTIONS = {"signal", "boost", "disqualify", "size", "context", "none"}
_VALID_TIERS = {"core", "watch", "null"}


def _load() -> dict:
    if not os.path.exists(_PATH):
        return {"version": 1, "findings": []}
    with open(_PATH) as f:
        return json.load(f)


def _save(doc: dict) -> None:
    with open(_PATH, "w") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write("\n")


def register(finding: dict, *, replace: bool = True) -> dict:
    """Add or update a finding by id. Called by research when a discovery is validated —
    this is how new solutions update the brain. Returns the stored record.
    Minimal validation: id + type + title required; enums checked when present."""
    fid = finding.get("id")
    if not fid:
        raise ValueError("finding needs an id")
    if finding.get("type") and finding["type"] not in _VALID_TYPES:
        raise ValueError(f"type must be one of {_VALID_TYPES}")
    if finding.get("action") and finding["action"] not in _VALID_ACTIONS:
        raise ValueError(f"action must be one of {_VALID_ACTIONS}")
    if finding.get("tier") and finding["tier"] not in _VALID_TIERS:
        raise ValueError(f"tier must be one of {_VALID_TIERS}")
    doc = _load()
    items = doc["findings"]
    for i, rec in enumerate(items):
        if rec.get("id") == fid:               # UPDATE: merge partial fields (learn.py uses this)
            if not replace:
                return rec
            items[i] = {**rec, **finding}
            _save(doc)
            return items[i]
    # NEW record: now type + title are required
    if not finding.get("type") or not finding.get("title"):
        raise ValueError("a NEW finding needs id, type, title")
    items.append(finding)
    _save(doc)
    return finding


def record(id: str, type: str, title: str, *, definition: str = "", layer: Optional[int] = None,
           direction: str = "long", action: str = "context", tier: Optional[str] = None,
           status: str = "research", col: Optional[str] = None, source: str = "",
           stats: Optional[dict] = None, date: Optional[str] = None, **extra) -> dict:
    """Frictionless one-liner for research to push a finding into the brain — this is the
    'new solutions auto-update the AI' hook. Date defaults to today; status defaults to
    'research' (promote to 'live' once built/validated). Any validation script ends with a
    record(...) call; the decision-spine then sees it automatically. Example:
        from brain.registry import record
        record('my_edge','edge','My Edge', definition='...', tier='watch', status='live',
               col='E_my_edge', stats={'median':1.2,'pbo':0.1}, source='validate_my_edge.py')
    """
    if date is None:
        from datetime import date as _d
        date = _d.today().isoformat()
    rec = {"id": id, "type": type, "title": title, "definition": definition, "layer": layer,
           "direction": direction, "action": action, "tier": tier, "status": status,
           "col": col, "source": source, "stats": stats or {}, "date": date, **extra}
    return register({k: v for k, v in rec.items() if v is not None})


def query(*, type: Optional[str] = None, layer: Optional[int] = None,
          tier: Optional[str] = None, status: Optional[str] = None,
          action: Optional[str] = None, direction: Optional[str] = None) -> list:
    """Filter findings. All args are AND-combined; None = ignore."""
    out = []
    for r in _load()["findings"]:
        if type is not None and r.get("type") != type:            continue
        if layer is not None and r.get("layer") != layer:         continue
        if tier is not None and r.get("tier") != tier:            continue
        if status is not None and r.get("status") != status:      continue
        if action is not None and r.get("action") != action:      continue
        if direction is not None and r.get("direction") not in (direction, "both"): continue
        out.append(r)
    return out


def get(fid: str) -> Optional[dict]:
    for r in _load()["findings"]:
        if r.get("id") == fid:
            return r
    return None


# ── convenience views the decision-spine uses ────────────────────────────────────
def live_edges(direction: Optional[str] = None) -> list:
    """Edges the brain may fire on (tier core|watch, status live)."""
    return [r for r in query(type="edge", status="live", direction=direction)
            if r.get("tier") in ("core", "watch")]


def disqualifiers() -> list:
    """Laws/gates that say a hard NO (action=disqualify) — the AVOID rules."""
    return [r for r in _load()["findings"] if r.get("action") == "disqualify"]


def laws() -> list:
    """Established principles (type=law) — the brain's priors."""
    return query(type="law")


def summary() -> dict:
    """Counts by type/tier — a quick health read of the knowledge core."""
    doc = _load()["findings"]
    from collections import Counter
    return {
        "total": len(doc),
        "by_type": dict(Counter(r.get("type") for r in doc)),
        "by_tier": dict(Counter(r.get("tier") for r in doc if r.get("tier"))),
        "by_layer": dict(Counter(r.get("layer") for r in doc if r.get("layer") is not None)),
        "live_edges": len([r for r in doc if r.get("type") == "edge" and r.get("status") == "live"]),
        "disqualifiers": len([r for r in doc if r.get("action") == "disqualify"]),
    }


if __name__ == "__main__":
    import pprint
    pprint.pprint(summary())
