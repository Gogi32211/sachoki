"""brain/introspect.py — the brain describing ITSELF. Builds a live 'neuron map': every module,
agent, knowledge group and data source is a node; the decision flow is the wiring; and the brain
computes — from its REAL current state — where it is strong and where it has GAPS. This is what the
🕸 Brain-Map tab renders, and the gaps feed the self-directed data-requests (requests.py).

Nothing here mutates state — it only reads registry / journal / edge_replay / regime and reports.
"""
from __future__ import annotations
from . import registry, journal


# ── static architecture (the wiring is known; status/counts/gaps are filled live) ──────────────
_LAYERS = [
    ("L0", "Data", "raw bars + engine signals (1d/1h/4h/15m DuckDB)", ["data"]),
    ("L1", "Universe", "tradeable set (price≥$5, ADV$ floor)", ["live"]),
    ("L2", "Regime", "market permission: risk-mult + which setups are allowed", ["regime", "regime_synth"]),
    ("L3", "Candidate", "which registry edges fired on each ticker today", ["live", "registry"]),
    ("L4", "Scoring", "pick the best fired edge by tier + historical median", ["spine", "registry"]),
    ("L5", "Entry/Size", "structural stop, R:R gate, 1%-risk sizing", ["sizing", "spine"]),
    ("L6", "Execution", "(manual) the user takes the trade", []),
    ("L7", "Position", "stop/target management on open trades", ["journal"]),
    ("L8", "Portfolio", "6%-risk / sector / drawdown envelope → allocate", ["portfolio"]),
    ("L9", "Feedback", "book → autopsy → two-way learning → memory", ["journal", "autopsy", "learn", "critic", "postmortem"]),
]

_MODULES = {
    "data":       ("Data plane", "L0", "reads OHLC + engine signals", "DuckDB 1d/1h/4h/15m (read-only)", "brain reads, never writes"),
    "registry":   ("Knowledge core", "L3", "the 65 validated findings — edges/laws/gates/nulls", "findings.json", "single source of truth for what we KNOW"),
    "regime":     ("Regime gate", "L2", "scores tape 0-3 → risk-mult + allowed setups", "SPY EMA200 + breadth", "long-only, conservative"),
    "live":       ("Universe runner", "L1/3", "maps today's edge fires → candidates", "edge_replay frame", "restricts to action=signal edges"),
    "spine":      ("Decision spine", "L4/5", "regime→candidate→sizing→portfolio → BUY/NO", "registry + sizing + portfolio", "deterministic"),
    "sizing":     ("Position sizer", "L5", "shares = risk$ / stop-distance, R:R≥1.5 gate", "price + swing-low", "1% risk/trade"),
    "portfolio":  ("Portfolio envelope", "L8", "caps total-risk 6% / sector 40% / drawdown", "journal book", "emergent position count"),
    "journal":    ("Book (L9)", "L7/9", "open/closed trades, drawdown, streak", "book.json", "feeds the risk envelope live"),
    "autopsy":    ("Trade forensics", "L9", "dissects each close vs the edge's base-rate", "closed trade + registry", "verdict + attribution + lesson"),
    "learn":      ("Learning loop", "L9", "calibrate(own trades) + revalidate(data)", "journal + edge_replay", "commits tier changes + lessons"),
    "nightly":    ("Auto-learn cron", "L9", "runs calibrate+revalidate daily 11:00 Tbilisi", "launchd", "continuous, unattended"),
}

_AGENTS = {
    "regime_synth": ("Regime synthesizer", "annotates the deterministic regime; may ONLY reduce risk",
                     "regime facts + our laws", "annotation + risk_adjust≤0 + flags"),
    "critic":       ("Adversarial critic", "tries to KILL each BUY using our laws + AVOID-rules",
                     "the trade + laws + disqualifiers", "pass | caution | veto"),
    "postmortem":   ("Forensics narrator", "explains in words why a close won or failed",
                     "closed trade + deterministic autopsy", "narrative + takeaway"),
}


def _llm_on() -> bool:
    try:
        from claude_client import _client
        return _client() is not None
    except Exception:
        return False


def _data_freshness() -> dict:
    """Last bar date per timeframe — how fresh each data neuron is."""
    out = {}
    try:
        import duckdb
        from studio.db import tf_db_path
        for tf in ("1d", "1h", "4h", "15m"):
            try:
                con = duckdb.connect(tf_db_path(tf), read_only=True)
                d = con.execute("SELECT max(date) FROM bars").fetchone()[0]
                con.close()
                out[tf] = str(d)[:10] if d else None
            except Exception:
                out[tf] = None
    except Exception:
        pass
    return out


def gaps() -> list:
    """Where the brain is BLIND or THIN — computed from real state. Each gap may carry `ask` (a
    question the user can answer) which requests.py turns into a self-directed data request."""
    g = []
    acct = journal.account_state()
    closed = journal.closed_trades()
    positions = journal.open_positions()
    edges = registry.live_edges()
    signal_edges = [e for e in edges if e.get("action") == "signal" and e.get("col")]
    ctx_edges = [e for e in edges if e.get("action") == "context"]

    # 1) outcome-learning idle until trades close
    if acct["n_closed"] == 0:
        g.append({"id": "no_outcomes", "severity": "info", "area": "L9 learning",
                  "title": "Outcome-learning idle — no closed trades yet",
                  "detail": "calibrate() can only learn once trades close. Data-learning (revalidate) already runs.",
                  "ask": {"kind": "outcome_logging",
                          "question": "Log your closed trades (take/close in the book) so I can learn from real outcomes."}})

    # 2) LLM agents off
    if not _llm_on():
        g.append({"id": "llm_off", "severity": "warn", "area": "agents",
                  "title": "LLM agents offline — critic/regime-synth/postmortem run fail-open",
                  "detail": "No ANTHROPIC_API_KEY in the backend env; the deterministic spine still works, "
                            "but the reasoning overlays are neutral.",
                  "ask": {"kind": "config",
                          "question": "Add ANTHROPIC_API_KEY to backend/.env to switch on the reasoning agents?"}})

    # 3) open positions without a recorded ACTUAL fill (entry came from the plan, not a fill)
    for p in positions:
        if not p.get("filled"):
            g.append({"id": f"fill_{p['ticker']}", "severity": "warn", "area": "L7 position",
                      "title": f"{p['ticker']}: entry is the PLAN price, not your real fill",
                      "detail": f"Book has entry ${p.get('entry')}. Autopsy/sizing are only exact with your true fill.",
                      "ask": {"kind": "fill_price", "ticker": p["ticker"],
                              "question": f"What price did you actually fill {p['ticker']}? (plan was ${p.get('entry')})"}})

    # 4) the brain has NO earnings/catalyst calendar — a real data blindspot on held names
    for p in positions:
        g.append({"id": f"catalyst_{p['ticker']}", "severity": "info", "area": "L0 data",
                  "title": f"{p['ticker']}: I can't see earnings/catalysts",
                  "detail": "No fundamental/event feed is wired. A binary event before target changes the risk.",
                  "ask": {"kind": "catalyst", "ticker": p["ticker"],
                          "question": f"Does {p['ticker']} have earnings or a known catalyst before your target?"}})

    # 5) knowledge coverage: context-only edges known but never fired standalone
    if ctx_edges:
        g.append({"id": "context_edges", "severity": "info", "area": "L3 knowledge",
                  "title": f"{len(ctx_edges)} edges are memory-only (not standalone triggers)",
                  "detail": "Broad/booster edges (" + ", ".join(e["id"] for e in ctx_edges) +
                            ") inform confluence but don't fire alone — by design, to stay selective."})

    # 6) watch-tier edges the brain trusts less
    watch = [e for e in signal_edges if e.get("tier") == "watch"]
    if watch:
        g.append({"id": "watch_tier", "severity": "info", "area": "L4 scoring",
                  "title": f"{len(watch)} firing edges are WATCH-tier (lower trust)",
                  "detail": "watch: " + ", ".join(e["id"] for e in watch) + " — sized at half until proven."})

    # 7) data staleness
    fresh = _data_freshness()
    for tf, d in fresh.items():
        if d is None:
            g.append({"id": f"stale_{tf}", "severity": "warn", "area": "L0 data",
                      "title": f"{tf} data unreadable", "detail": f"Could not read the {tf} DuckDB."})
    return g


def brain_map() -> dict:
    """The full self-portrait: neurons (nodes), wiring (flow), knowledge, data, agents, gaps."""
    summ = registry.summary()
    acct = journal.account_state()
    llm = _llm_on()

    # layers with live status
    layers = []
    for lid, name, role, mods in _LAYERS:
        # crude strength: a layer with wired modules is 'active'; L6 is manual; L9 richer now
        status = "manual" if lid == "L6" else "active"
        layers.append({"id": lid, "name": name, "role": role, "modules": mods, "status": status})

    modules = [{"key": k, "name": n, "layer": lyr, "does": does, "reads": reads, "note": note}
               for k, (n, lyr, does, reads, note) in _MODULES.items()]

    agents = [{"key": k, "name": n, "role": role, "inputs": inp, "output": out, "on": llm}
              for k, (n, role, inp, out) in _AGENTS.items()]

    edges = registry.live_edges()
    knowledge = {
        "total": summ["total"],
        "by_type": summ["by_type"],
        "by_tier": summ["by_tier"],
        "signal_edges": len([e for e in edges if e.get("action") == "signal" and e.get("col")]),
        "context_edges": len([e for e in edges if e.get("action") == "context"]),
        "laws": summ["by_type"].get("law", 0),
        "disqualifiers": summ["disqualifiers"],
        "collects": "every validated finding (edge/law/gate/null/discriminator) + every trade lesson",
    }

    data_sources = [{"tf": tf, "last_bar": d} for tf, d in _data_freshness().items()]

    # decision flow wiring (source -> target, what travels)
    flow = [
        ("data", "regime", "SPY EMA200 + breadth"),
        ("data", "live", "today's edge fires"),
        ("registry", "live", "which cols count"),
        ("regime", "spine", "risk-mult + allowed setups"),
        ("live", "spine", "candidate + fired edges"),
        ("registry", "spine", "edge tier + median"),
        ("spine", "sizing", "entry/stop/target"),
        ("sizing", "portfolio", "sized order"),
        ("journal", "portfolio", "open risk + drawdown"),
        ("portfolio", "journal", "allocated BUY → book"),
        ("journal", "autopsy", "closed trade"),
        ("registry", "autopsy", "edge base-rate"),
        ("autopsy", "learn", "outcome lesson"),
        ("data", "learn", "fresh frame (revalidate)"),
        ("learn", "registry", "tier changes + recent stats"),
        ("regime", "regime_synth", "annotate"),
        ("spine", "critic", "critique each BUY"),
        ("autopsy", "postmortem", "narrate close"),
    ]
    flow = [{"from": a, "to": b, "label": lab} for a, b, lab in flow]

    return {
        "layers": layers, "modules": modules, "agents": agents,
        "knowledge": knowledge, "data_sources": data_sources, "flow": flow,
        "account": acct, "llm_on": llm, "gaps": gaps(),
    }
