"""brain/agents.py — the narrow LLM judgment nodes (only where synthesis beats math).

Two agents, both ADVISORY overlays on the deterministic spine, both fail-open (no API key ->
neutral, system runs unchanged). Crucially they are fed the brain's OWN validated laws/nulls
from the registry, so they reason WITH our knowledge — not generic LLM market opinions.

  regime_synth — annotates the deterministic regime, may only REDUCE risk (flag hazards)
  critic       — adversarial: tries to KILL a BUY using our laws/AVOID-rules; pass|caution|veto
"""
from __future__ import annotations
import json

try:
    from claude_client import ask_json
except Exception:
    def ask_json(*a, **k):
        return None

from . import registry


_REGIME_SYS = (
    "You are a risk-regime synthesizer for a LONG-ONLY US-equity swing system. You do NOT pick "
    "stocks. Given deterministic market facts, output a brief risk annotation. Be conservative: "
    "you may only REDUCE risk (flag hazards), never increase it. Respond ONLY with JSON."
)


def regime_synth(facts: dict, context: str = "") -> dict:
    """Annotate the regime; returns {annotation, risk_adjust (<=0), flags}. Fail-open = no change."""
    prompt = (
        f"Deterministic regime facts: {json.dumps(facts)}\n"
        f"Extra context: {context or 'none'}\n"
        f"Known system laws to respect: {[l['title'] for l in registry.laws()][:8]}\n"
        'Output JSON only: {"annotation":"<one sentence>", '
        '"risk_adjust": <number in [-0.5, 0], a multiplier REDUCTION; 0 = no change>, '
        '"flags": ["<hazard>", ...]}'
    )
    r = ask_json(prompt, system=_REGIME_SYS, max_tokens=300)
    if not isinstance(r, dict):
        return {"annotation": "(LLM off — deterministic regime only)", "risk_adjust": 0.0, "flags": []}
    try:
        r["risk_adjust"] = max(-0.5, min(0.0, float(r.get("risk_adjust", 0))))
    except Exception:
        r["risk_adjust"] = 0.0
    r.setdefault("flags", [])
    return r


_CRITIC_SYS = (
    "You are an ADVERSARIAL trade critic for a validated long-only swing system. Your job is to "
    "KILL weak trades, not cheerlead. Apply the system's OWN validated laws and AVOID-rules "
    "(provided). Default to 'pass' unless you find a concrete, specific concern grounded in those "
    "rules or the trade geometry. Do not invent generic market opinions. Respond ONLY with JSON."
)


def critic(decision: dict, regime: dict | None = None) -> dict:
    """Adversarially critique one BUY. Returns {verdict: pass|caution|veto, concerns:[...], which_rule}."""
    laws = [f"{l['id']}: {l.get('definition', '')[:140]}" for l in registry.laws()]
    disq = [f"{d['id']}: {d.get('definition', '')[:140]}" for d in registry.disqualifiers()]
    trade = {k: decision.get(k) for k in
             ("ticker", "edge", "edge_title", "tier", "entry", "stop", "target",
              "shares", "risk_dollars", "sector")}
    prompt = (
        f"Proposed BUY: {json.dumps(trade)}\n"
        f"Regime: {json.dumps(regime or {})}\n"
        f"System LAWS (apply): {laws}\n"
        f"AVOID rules (apply): {disq}\n"
        "Try to REFUTE this trade. If a law/AVOID-rule is violated or the geometry is poor, say so.\n"
        'Output JSON only: {"verdict":"pass"|"caution"|"veto", '
        '"concerns":["<concrete concern>", ...], "which_rule": "<rule id or null>"}'
    )
    r = ask_json(prompt, system=_CRITIC_SYS, max_tokens=400)
    if not isinstance(r, dict):
        return {"verdict": "pass", "concerns": [], "note": "LLM off — no critique"}
    if r.get("verdict") not in ("pass", "caution", "veto"):
        r["verdict"] = "pass"
    r.setdefault("concerns", [])
    return r


_POSTMORTEM_SYS = (
    "You are a trade forensics analyst for a validated long-only swing system. You are given a CLOSED "
    "trade, the deterministic autopsy already computed, and the edge's historical base-rate. Explain in "
    "2-4 plain sentences WHY this was a good buy or a failure, staying strictly consistent with the "
    "autopsy verdict/attribution and the system's laws — do NOT overturn a loss into 'it was fine' or a "
    "win into luck without evidence. End with one concrete takeaway. Respond ONLY with JSON."
)


def postmortem(trade: dict, analysis: dict) -> dict:
    """LLM narrative on top of the deterministic autopsy — the human-readable 'why it worked/failed'.
    Fail-open: with no LLM, returns the autopsy's own lesson as the narrative."""
    laws = [f"{l['id']}: {l.get('definition', '')[:120]}" for l in registry.laws()][:8]
    prompt = (
        f"Closed trade: {json.dumps({k: trade.get(k) for k in ('ticker','edge','edge_title','entry','stop','target','exit','pnl','reason','opened_regime')})}\n"
        f"Deterministic autopsy: {json.dumps(analysis)}\n"
        f"System laws: {laws}\n"
        'Output JSON only: {"narrative":"<2-4 sentences>", "takeaway":"<one actionable line>"}'
    )
    r = ask_json(prompt, system=_POSTMORTEM_SYS, max_tokens=400)
    if not isinstance(r, dict):
        return {"narrative": analysis.get("lesson", "(LLM off — deterministic autopsy only)"),
                "takeaway": analysis.get("lesson", ""), "llm": False}
    r.setdefault("narrative", analysis.get("lesson", ""))
    r.setdefault("takeaway", "")
    r["llm"] = True
    return r


def available() -> bool:
    """Is the LLM reachable (key present)? Cheap check via a tiny call is avoided; infer from client."""
    try:
        from claude_client import _client
        return _client() is not None
    except Exception:
        return False


# ── 🧠 OPUS FINAL DECIDER (2026-08-03, user request) ─────────────────────────────────────
# The user wants the LAST word on "what and when to buy" to come from the strongest model,
# with everything summarized in front of it. Guardrails keep it honest:
#   * it chooses ONLY among candidates the deterministic spine already validated and sized —
#     it may take or skip, never invent a ticker, never resize, never loosen a gate;
#   * it is fed the system's OWN laws + each candidate's full gate trace, so it reasons WITH
#     our evidence, not from generic market vibes;
#   * fail-open: no API key / bad response -> every candidate is taken (spine behavior).
_DECIDER_MODEL = "claude-opus-5"

_DECIDER_SYS = (
    "You are the FINAL decision maker for a validated, long-only, PAPER-TRADED US-equity swing "
    "system. The deterministic pipeline already validated, gated and sized each candidate; your "
    "job is the last synthesis: given the full evidence, decide for each candidate whether the "
    "pullback order is placed today (take) or not (skip). You may ONLY take or skip listed "
    "candidates — never add tickers, never change sizes/stops, never override a hard gate. "
    "Skip needs a concrete reason grounded in the provided laws, gate notes, regime or account "
    "state. When evidence is clean, take — do not invent caution. Respond ONLY with JSON."
)


def final_decider(allocated: list, regime: dict | None = None, account: dict | None = None,
                  open_questions: list | None = None) -> dict:
    """Last word on today's buys. Returns {decisions:{ticker:{action,reason}}, summary, model}."""
    if not allocated:
        return {"decisions": {}, "summary": "no candidates", "model": None}
    laws = [f"{l['id']}: {l.get('definition', '')[:120]}" for l in registry.laws()][:12]
    cands = []
    for d in allocated:
        g = d.get("gates") or {}
        notes = [f"{c['title']}: {c['note'][:80]}" for c in g.get("checks", [])
                 if c.get("applicable") and (c.get("mult", 1) != 1 or "⚠" in c.get("note", ""))]
        cands.append({k: d.get(k) for k in ("ticker", "edge", "edge_title", "tier", "entry",
                                            "stop", "target", "shares", "risk_dollars", "sector")}
                     | {"gate_mult": g.get("mult"), "gate_notes": notes,
                        "pullback_order": d.get("pullback")})
    prompt = (
        f"Candidates (validated+sized, pullback orders ready): {json.dumps(cands)}\n"
        f"Regime: {json.dumps(regime or {})}\n"
        f"Account (paper): {json.dumps(account or {})}\n"
        f"Open data-gaps the system has flagged: "
        f"{json.dumps([{k: q.get(k) for k in ('ticker', 'question')} for q in (open_questions or [])])}\n"
        f"System laws: {laws}\n"
        'Output JSON only: {"decisions": [{"ticker": "...", "action": "take"|"skip", '
        '"reason": "<one concrete sentence>"}], "summary": "<2-3 sentences: what today\'s set '
        'looks like and why>"}'
    )
    r = ask_json(prompt, system=_DECIDER_SYS, max_tokens=1500, model=_DECIDER_MODEL)
    if not isinstance(r, dict) or not isinstance(r.get("decisions"), list):
        return {"decisions": {}, "summary": "LLM off — spine decisions stand unchanged",
                "model": None}
    dec = {}
    for x in r["decisions"]:
        if isinstance(x, dict) and x.get("ticker"):
            act = x.get("action") if x.get("action") in ("take", "skip") else "take"
            dec[x["ticker"]] = {"action": act, "reason": str(x.get("reason", ""))[:300]}
    return {"decisions": dec, "summary": str(r.get("summary", ""))[:600],
            "model": _DECIDER_MODEL}


_ANSWER_SYS = (
    "You answer a paper-trading system's open data-gap questions. HARD RULES: this is a PAPER "
    "book — there are no real broker fills, so a fill-price question is answered with the plan "
    "price (that IS the paper fill). NEVER invent external facts: for catalyst/earnings "
    "questions you cannot verify, say exactly what you can and cannot know and give a policy "
    "answer (e.g. 'unverified — treat earnings week as elevated risk'). Respond ONLY with JSON."
)


def answer_requests(open_reqs: list, book: list | None = None) -> dict:
    """Opus answers the brain's open 'needs your input' questions. Returns
    {answers:[{id, value, note}], model}. Caller applies via requests.answer()."""
    if not open_reqs:
        return {"answers": [], "model": None}
    ctx = [{k: r.get(k) for k in ("id", "kind", "ticker", "question")} for r in open_reqs]
    positions = [{k: p.get(k) for k in ("ticker", "edge", "entry", "shares", "stop", "target",
                                        "opened")} for p in (book or [])]
    prompt = (
        f"Open questions: {json.dumps(ctx)}\n"
        f"Paper book (the only ground truth about our trades): {json.dumps(positions)}\n"
        "For fill_price: the paper fill IS the plan entry price — answer with that number only.\n"
        "For catalyst: answer ONLY from what is verifiable here; otherwise give the honest "
        "policy line, never a made-up date.\n"
        'Output JSON only: {"answers": [{"id": "<request id>", "value": "<the answer to '
        'record>", "note": "<why / confidence>"}]}'
    )
    r = ask_json(prompt, system=_ANSWER_SYS, max_tokens=1200, model=_DECIDER_MODEL)
    if not isinstance(r, dict) or not isinstance(r.get("answers"), list):
        return {"answers": [], "model": None, "note": "LLM off"}
    good = [a for a in r["answers"]
            if isinstance(a, dict) and a.get("id") and a.get("value") is not None]
    return {"answers": good, "model": _DECIDER_MODEL}
