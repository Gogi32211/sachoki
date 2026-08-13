"""Edge Replay 2 — the same computation, wearing its passport.

NOT A SECOND ENGINE. The rows come from the identical `edge_replay()` call behind the identical
cache key that `/api/edge-replay` uses, so the two surfaces cannot disagree: a result computed
for one IS the result served to the other, byte for byte. `backtest == display == trade` was the
book's hardest-won identity, and a "Replay 2" that recomputed anything would be two engines one
drift apart from a quiet contradiction.

What this surface ADDS is everything the old panel's numbers never carried:

    PASSPORT     the four evidence axes, honestly filled. The book's families were MINED on
                 this data, so result_role is EXPLORATORY_HISTORICAL_EVIDENCE and the ceiling
                 excludes promote/book — a strong PF on this screen is a hypothesis, not an
                 edge. instrument_validation_basis is UNVALIDATED, which reads harsh and is
                 simply true in this vocabulary: the path-sim has survived reproduction
                 discipline for a year, and has never passed a registered qualification the way
                 ComboLab v2's kernel did. Writing VALIDATED because it feels earned is exactly
                 the move the vocabulary exists to refuse.

    DEFLATION    per-setup DSR joined from edge_overfit.json, and the family PBO beside the
                 table instead of in a JSON nobody opens.

    k            this surface counts what it is asked. Exit knobs × months × floors is a search
                 space like any other, and the old panel never recorded how much of it had been
                 tried. Charged on delivery, same counter core as Measure and the builder.

RERUNNING CANNOT UPGRADE. Same data, same claims, new machinery, cleaner label — that is
REPLAY_OF_EXPOSED_EVIDENCE, the laundering Gate 3B closed. These rows stay exploratory forever;
the honest path to a stronger word is the frozen forward spec, and the response names it.
"""
from __future__ import annotations

import hashlib
import json
import os

from pydantic import BaseModel

import evidence_status as ES
from scan_cache import cached as _scan_cached
from search_counter import SearchCounter

HERE = os.path.dirname(os.path.abspath(__file__))
OVERFIT = os.path.join(HERE, "edge_overfit.json")

COUNTER = SearchCounter(
    "replay2_search.jsonl", "edge-replay-2",
    note=("k counts DIFFERENT replay claims (setup universe + exit policy + window). The same "
          "knobs again is the same claim looked at twice."))

# The passport, shared by every historical row this surface can ever serve.
BOOK_STATUS = ES.EvidenceStatus(
    evidence_origin=ES.HISTORICAL_RESEARCH,
    instrument_validation_basis=ES.UNVALIDATED,
    application_maturity=ES.NOT_APPLICABLE,
    result_role=ES.EXPLORATORY_HISTORICAL_EVIDENCE)


class Replay2Request(BaseModel):
    """Module level — the PEP 563 + local-model pair degrades the body to a query parameter."""
    setup: str = "all"
    months: int = 36
    dv_floor: float = 3_000_000
    mode: str = "trail"
    stop: float = 0.10
    target: float = 0.25
    trail: float = 0.25
    maxh: int = 60
    atr_k: float = 12.0


def _overfit() -> dict:
    if not os.path.exists(OVERFIT):
        return {"rows": [], "pbo": {}, "as_of": "", "n_trials_assumed": None}
    with open(OVERFIT) as f:
        return json.load(f)


def _claim_hash(setup: str, req: Replay2Request, as_of: str) -> str:
    return hashlib.sha256(json.dumps(
        {"setup": setup, "months": req.months, "dv_floor": req.dv_floor, "mode": req.mode,
         "stop": req.stop, "target": req.target, "trail": req.trail, "maxh": req.maxh,
         "atr_k": req.atr_k, "as_of": as_of},
        sort_keys=True).encode()).hexdigest()[:12]


def run(req: Replay2Request) -> dict:
    from edge_replay import edge_replay                               # noqa: PLC0415

    # THE cache key from /api/edge-replay, verbatim (with_trades=False, slip=None). Shared on
    # purpose: one computation serves both panels, so they cannot disagree.
    key = (f"edgereplay:{req.setup}:{req.months}:{req.dv_floor}:{req.mode}:{req.stop}:"
           f"{req.target}:{req.trail}:{req.maxh}:False:None:{req.atr_k}")
    base = _scan_cached(key, lambda: edge_replay(
        setup=req.setup, months=req.months, dv_floor=req.dv_floor, mode=req.mode,
        stop=req.stop, target=req.target, trail=req.trail, maxh=req.maxh,
        with_trades=False, slip=None, atr_k=req.atr_k), ttl=3600)

    ov = _overfit()
    by_setup = {r["setup"]: r for r in ov.get("rows", [])}
    as_of = base.get("as_of", "")

    rows = []
    for r in base.get("rows", []):
        o = by_setup.get(r["setup"], {})
        rows.append({**r,
                     "dsr": o.get("dsr"), "sr_trade": o.get("sr_trade"), "psr0": o.get("psr0"),
                     "claim_hash": _claim_hash(r["setup"], req, as_of)})

    out = {
        "engine": ("edge_replay — the SAME computation and cache key as /api/edge-replay; "
                   "this surface adds labels, it does not recompute"),
        "as_of": as_of, "months": base.get("months"), "exit": base.get("exit"),
        "rows": rows,
        "passport": {
            **BOOK_STATUS.as_dict(),
            "why_unvalidated": ("the path-sim has survived reproduction discipline and has "
                                "never passed a registered qualification the way ComboLab v2's "
                                "kernel did. The word is earned there and not here — yet."),
            "why_exploratory_forever": ("these families were mined on this data. Re-running "
                                        "them through newer machinery is a replay of exposed "
                                        "evidence and cannot come back cleaner; the honest path "
                                        "to a stronger word is a frozen forward spec evaluated "
                                        "on bars that do not exist yet."),
        },
        "deflation": {"family_pbo": ov.get("pbo", {}).get("pbo"),
                      "oos_is_ratio": ov.get("pbo", {}).get("oos_is_ratio"),
                      "computed": ov.get("as_of", ""),
                      "n_trials_assumed": ov.get("n_trials_assumed"),
                      "source": "edge_overfit.json — Bailey & López de Prado DSR/PBO"},
    }
    # charged on delivery, once per distinct (setup universe + exit policy + window)
    out["search_accounting"] = COUNTER.safe_record(
        {"setup": req.setup, "months": req.months, "dv_floor": req.dv_floor, "mode": req.mode,
         "stop": req.stop, "target": req.target, "trail": req.trail, "maxh": req.maxh,
         "atr_k": req.atr_k},
        extra={"rows": len(rows)})
    return out


def build_router():
    from fastapi import APIRouter, HTTPException                      # noqa: PLC0415

    router = APIRouter(prefix="/api/studio/replay2", tags=["studio-replay2"])

    @router.post("")
    def _run(req: Replay2Request):
        try:
            return run(req)
        except Exception as e:                                        # noqa: BLE001
            raise HTTPException(500, detail=str(e))

    return router
