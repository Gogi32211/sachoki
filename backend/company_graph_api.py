"""company_graph_api.py — transport for the Company Intelligence page.

READ AND HARVEST ARE SEPARATE VERBS, AND THAT IS THE WHOLE DESIGN
    GET returns what is already stored, immediately, however little that is. POST starts a
    harvest. A first-time ticker therefore renders its profile and an empty graph with a
    progress line, rather than holding a request open for the ~40s a real pass takes.

    The alternative — GET that harvests on miss — makes the page's slowest path its
    default path and gives the user nothing to look at while it runs.

EVERY RESPONSE CARRIES ITS OWN LIMITS
    `coverage` is returned beside the graph on every read: how many filers mentioned the
    target, how many were actually processed, how many were dropped and why. A graph of 15
    edges from 14 of 37 candidates is a different object from a graph of 15 edges from all
    37, and nothing in the picture itself distinguishes them.

    This is the same reason the risk block refuses to compute percentages. See `risk`.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/company-intel", tags=["company-intel"])


# Module level, deliberately. A pydantic model defined inside the handler combines with
# `from __future__ import annotations` to make FastAPI resolve the annotation to a bare
# name it cannot see, and it silently degrades the body to a query parameter. That has
# cost this codebase debugging time twice already.
class BuildRequest(BaseModel):
    lookback_days: int = 400
    max_candidates: int = 40
    forms: Optional[list] = None


def _num(v):
    """DuckDB hands back NaN for SQL NULL in a DOUBLE column; JSON has no NaN."""
    try:
        if v is None:
            return None
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _clean(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        d = {k: (None if (isinstance(v, float) and v != v) else v) for k, v in r.items()}
        d["share_pct"] = _num(r.get("share_pct"))
        for k in ("ceiling_applied",):
            if k in d:
                d[k] = bool(d[k])
        out.append(d)
    return out


@router.get("/{ticker}")
def get_company_intel(ticker: str, include_quotes: bool = Query(True)):
    """Everything stored for this ticker, plus what is missing from it."""
    from company_graph import build, store
    from company_graph import harvest_sec as H

    ticker = ticker.upper().strip()
    store.init()

    prof = H.profile(ticker)
    if not prof:
        raise HTTPException(404, f"{ticker} is not in SEC's ticker index")

    edges = _clean(store.current_edges(ticker))
    if not include_quotes:
        for e in edges:
            e.pop("quote", None)

    run = store.last_run(ticker)
    run_out = None
    if run is not None:
        run_out = {k: (None if (isinstance(v, float) and v != v) else v)
                   for k, v in run.items() if k not in ("rejections",)}
        for k in ("started_at", "finished_at"):
            if run_out.get(k) is not None:
                run_out[k] = str(run_out[k])
        if run_out.get("params"):
            try:
                run_out["params"] = json.loads(run_out["params"])
            except Exception:                                         # noqa: BLE001
                pass

    ents = {e["cik"]: e for e in store.entities()}
    by_ticker = {e["ticker"]: e for e in ents.values() if e.get("ticker")}

    return {
        "ok": True,
        "ticker": ticker,
        "profile": prof,
        "edges": edges,
        "entities": list(ents.values()),
        "entity_by_ticker": by_ticker,
        "views": _views(ticker, edges),
        "risk": risk_block(ticker, edges),
        "coverage": _coverage(run_out),
        "last_run": run_out,
        "harvesting": build.is_running(ticker),
        "progress": build.progress(ticker),
    }


def _coverage(run: Optional[dict]) -> dict:
    """What this graph does NOT contain, stated rather than implied.

    A picture of 15 edges looks equally complete whether it came from 14 candidates or
    140. Without this block the reader has no way to tell, and the natural assumption is
    the flattering one.
    """
    if not run:
        return {"harvested": False,
                "note": "nothing harvested yet — the graph below is empty, not sparse"}
    # A RUNNING row has null counters. Reporting it as harvested made the page state
    # "all 0 filers were read and none stated a relationship" — a confident conclusion
    # drawn from a run that had not finished its first document.
    if run.get("status") == "RUNNING":
        return {"harvested": False, "in_progress": True,
                "note": "reading SEC filings now — counts appear as documents are read"}
    processed = run.get("n_processed") or 0
    found = run.get("n_candidates") or 0
    return {
        "harvested": True,
        "status": run.get("status"),
        "documents_matched": run.get("n_documents"),
        "filers_naming_target": found,
        "filers_read": processed,
        "filers_not_read": max(0, found - processed),
        "edges": run.get("n_edges"),
        "quotes_rejected": run.get("n_rejected"),
        "as_of": str(run.get("finished_at") or run.get("started_at") or ""),
        "note": (f"{processed} of {found} filers were read; "
                 f"{max(0, found - processed)} were not. Relationships in the unread "
                 f"filings are absent from this graph, not absent from the world.")
        if found > processed else
        "every filer that named this company in the search window was read",
    }


def classify(edge: dict, ticker: str) -> str:
    """Where the counterparty sits RELATIVE TO THE TARGET: upstream, downstream, lateral.

    The one place this is decided. An earlier version worked it out separately in the view
    builder and in the risk block, and they disagreed — the views showed two upstream
    dependencies while the risk block reported zero, on exactly the two edges that mattered
    most (the mandated disclosures). Neither was obviously wrong on its own.

    The rule the split kept losing: a relationship type has a direction, but WHICH SIDE OF
    THE EDGE THE TARGET IS ON flips it. "FN SUPPLIES_TO NVDA" and "NVDA CUSTOMER_OF FN"
    describe the same arrangement and both put FN upstream of NVDA.
    """
    from company_graph.contract import upstream_side

    side = upstream_side(edge["rel_type"])
    if not side:
        return "LATERAL"
    up_end = edge["src"] if side == "src" else edge["dst"]
    # the counterparty is upstream when the TARGET is not the upstream end
    return "DOWNSTREAM" if up_end == ticker else "UPSTREAM"


def counterparty(edge: dict, ticker: str) -> str:
    return edge["dst"] if edge["src"] == ticker else edge["src"]


def _views(ticker: str, edges: list[dict]) -> dict:
    """The brief's tabs, derived from one edge list rather than fetched separately."""
    up, down, comp, partners, owners, priors = [], [], [], [], [], []
    for e in edges:
        if e.get("status") == "MODEL_PRIOR":
            priors.append(e)
            continue
        rel = e["rel_type"]
        if rel == "COMPETES_WITH":
            comp.append(e)
        elif rel == "PARTNER_OF":
            partners.append(e)
        elif rel == "OWNS":
            owners.append(e)
        elif classify(e, ticker) == "UPSTREAM":
            up.append(e)
        else:
            down.append(e)
    return {"upstream": up, "downstream": down, "competitors": comp,
            "partners": partners, "ownership": owners, "model_priors": priors}


def risk_block(ticker: str, edges: list[dict]) -> dict:
    """Concentration and single points of failure — from EVIDENCED edges only.

    No percentages of exposure are produced. A country share computed over a partial graph
    is arithmetic performed on an unknown denominator, and it would be the most
    authoritative-looking number on the page. Counts are reported instead, next to the
    coverage that bounds them.
    """
    ev = [e for e in edges if e.get("status") == "EVIDENCED"]
    # same classifier the views use — see `classify` for what happened when it was not
    upstream = [e for e in ev if classify(e, ticker) == "UPSTREAM"]
    disclosed = [e for e in ev if e.get("evidence_tier") == "FILING_DISCLOSURE"]

    # a component named by exactly one counterparty in the evidence we hold
    by_component: dict[str, set] = {}
    for e in upstream:
        key = (e.get("component") or "").strip().lower()
        if key:
            by_component.setdefault(key, set()).add(counterparty(e, ticker))
    single = [{"component": k, "sole_source": list(v)[0]}
              for k, v in by_component.items() if len(v) == 1]

    countries: dict[str, int] = {}
    for e in ev:
        c = e.get("_country") or ""
        if c:
            countries[c] = countries.get(c, 0) + 1

    return {
        "evidenced_edges": len(ev),
        "model_prior_edges": len(edges) - len(ev),
        "upstream_dependencies": len(upstream),
        "mandated_disclosures": [
            {"src": e["src"], "dst": e["dst"], "rel_type": e["rel_type"],
             "share_pct": _num(e.get("share_pct")), "share_basis": e.get("share_basis", ""),
             "source_url": e.get("source_url"), "quote": e.get("quote", "")[:300]}
            for e in disclosed],
        "single_source_components": single,
        "countries_seen": countries,
        "no_percentages_note":
            "Exposure is reported as counts. A percentage over a partial graph divides by "
            "a denominator nobody knows, and would read as the most precise number here.",
    }


@router.post("/{ticker}/build")
def start_build(ticker: str, req: BuildRequest = BuildRequest()):
    from company_graph import build

    ticker = ticker.upper().strip()
    kw = {"lookback_days": req.lookback_days, "max_candidates": req.max_candidates}
    if req.forms:
        kw["forms"] = tuple(req.forms)
    return build.build_async(ticker, **kw)


@router.get("/{ticker}/progress")
def get_progress(ticker: str):
    from company_graph import build, store
    t = ticker.upper().strip()
    return {"ok": True, "ticker": t, "running": build.is_running(t),
            "progress": build.progress(t),
            "coverage": _coverage(_run_dict(store.last_run(t)))}


def _run_dict(run) -> Optional[dict]:
    if run is None:
        return None
    return {k: (None if (isinstance(v, float) and v != v) else v) for k, v in run.items()}


@router.get("/{ticker}/peers")
def get_peers(ticker: str, weeks: int = Query(9, ge=2, le=52)):
    """Weekly bars for every listed company in the ecosystem, on one comparable scale."""
    from company_graph import store
    from company_graph.peers import peer_table

    ticker = ticker.upper().strip()
    store.init()
    edges = _clean(store.current_edges(ticker))
    return peer_table(ticker, edges, classify, counterparty, weeks=weeks)


@router.get("/{ticker}/evidence")
def get_evidence(ticker: str, src: str = Query(...), dst: str = Query(...),
                 rel_type: str = Query("")):
    """Every document behind one relationship, newest first — including superseded ones.

    The graph view shows the current evidence per relationship. This shows the history,
    which is how you tell a claim that keeps being restated from one asserted once in 2024
    and never again.
    """
    from company_graph import store
    with store.connect(read_only=True) as c:
        q = ("SELECT * FROM edge WHERE graph_ticker = ? AND src = ? AND dst = ?"
             + (" AND rel_type = ?" if rel_type else "")
             + " ORDER BY COALESCE(doc_date,'') DESC, edge_id DESC")
        args = [ticker.upper(), src.upper(), dst.upper()] + ([rel_type] if rel_type else [])
        try:
            df = c.execute(q, args).fetchdf()
        except Exception as exc:                                      # noqa: BLE001
            raise HTTPException(500, f"evidence query failed: {exc}")
    return {"ok": True, "rows": _clean(df.to_dict("records") if len(df) else [])}


def build_router() -> APIRouter:
    return router


__all__ = ["router", "build_router"]
