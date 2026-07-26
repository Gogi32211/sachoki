"""brain/live.py — run the decision spine over TODAY's real edge fires.

Bridges the live detectors (edge_replay.latest_edges_map — what fired on each ticker's latest
bars) to the brain: map display-codes -> registry edges, fetch price/ADV/sector, and ask
spine.decide() for a BUY/NO with its full chain. Read-only; isolated from the live app.
"""
from __future__ import annotations


def run_universe(*, max_age: int = 0, regime: dict | None = None,
                 open_positions: list[dict] | None = None, drawdown: float = 0.0,
                 losing_streak: int = 0, limit: int = 200, critique: bool = False) -> dict:
    """max_age: fires this many bars old or newer (0 = fired today). Returns
    {regime, as_of, n_candidates, decisions:[...BUY plans...]}."""
    import duckdb
    import edge_replay as er
    from studio.db import tf_db_path
    from . import spine, regime as regime_mod, journal

    reg = regime or regime_mod.current_regime()
    # 🤖 regime-synth agent (advisory, may only reduce risk) — optional
    synth = None
    if critique:
        from . import agents
        synth = agents.regime_synth(reg.get("facts", {}),
                                    context="; ".join(reg.get("reasons", [])))
        if synth.get("risk_adjust", 0) < 0:
            reg = {**reg, "risk_mult": max(0.0, reg["risk_mult"] + synth["risk_adjust"])}
    if reg.get("risk_mult", 0) <= 0:
        return {"regime": reg, "decisions": [], "note": "regime: no trading", "regime_synth": synth}

    # LIVE account state (L9): open book + drawdown + losing-streak feed the envelope.
    acct = journal.account_state()
    if open_positions is None:
        open_positions = journal.open_positions()
    if not drawdown:
        drawdown = acct["drawdown"]
    if not losing_streak:
        losing_streak = acct["losing_streak"]
    held = {p["ticker"] for p in open_positions}

    emap = er.latest_edges_map(build=True)             # {ticker: [(code, age), ...]} (build if cold)
    code2col = {code: col for code, col in er.DISPLAY_SETUPS + getattr(er, "MINED_DISPLAY", [])}
    # only registry edges have decision value — restrict to their cols
    reg_cols = {e["col"] for e in er_registry_cols()}

    fired = {}
    for t, fires in emap.items():
        cols = [code2col.get(c) for c, age in fires if age <= max_age]
        cols = [c for c in cols if c and c in reg_cols]
        if cols:
            fired[t] = cols
    if not fired:
        return {"regime": reg, "as_of": None, "n_candidates": 0, "decisions": [],
                "note": "no registry-edge fires today"}

    # price + 20d ADV$ for the fired tickers
    tickers = list(fired)
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    ph = ",".join("?" * len(tickers))
    px = con.execute(
        f"""WITH r AS (SELECT ticker, date, close, low, close*volume dvol,
              row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn
            FROM bars WHERE ticker IN ({ph}) AND close>=5)
          SELECT ticker, any_value(CASE WHEN rn=1 THEN close END) px,
                 avg(CASE WHEN rn<=20 THEN dvol END) adv,
                 min(CASE WHEN rn<=15 THEN low END) swing_low
          FROM r WHERE rn<=20 GROUP BY ticker""", tickers).fetchdf()
    maxd = con.execute("SELECT max(date) FROM bars WHERE universe<>'index'").fetchone()[0]
    con.close()
    price = {r.ticker: (float(r.px) if r.px is not None else None,
                        float(r.adv) if r.adv else None,
                        float(r.swing_low) if r.swing_low is not None else None)
             for r in px.itertuples()}
    try:
        _, smap = er._load_rs_ref()
    except Exception:
        smap = {}

    decisions = []
    for t, cols in fired.items():
        if t in held:                                  # already in the book — skip
            continue
        p, adv, swing = price.get(t, (None, None, None))
        if not p:
            continue
        d = spine.decide(t, cols, p, sector=smap.get(t, "?"), adv_dollars=adv, swing_low=swing,
                         open_positions=open_positions, drawdown=drawdown,
                         losing_streak=losing_streak, regime=reg)
        if d.get("decision") == "BUY":
            decisions.append(d)
    # rank by edge quality (core first, then by historical median)
    from . import registry, portfolio
    med = {e["id"]: e.get("stats", {}).get("median", 0) for e in registry.live_edges()}
    decisions.sort(key=lambda d: (d.get("tier") == "core", med.get(d.get("edge"), 0)), reverse=True)

    # sequential portfolio allocation: what actually fits TODAY within the 6% risk envelope.
    # Walk ranked decisions, add each to the running book if the portfolio still has room —
    # this naturally caps the actionable set (~6 at 1% risk each) instead of listing all 75.
    book = list(open_positions or [])
    allocated = []
    for d in decisions:
        cand = {"sector": d.get("sector", "?"), "risk_dollars": d["risk_dollars"],
                "position_value": d["position_value"]}
        pf = portfolio.portfolio_check(cand, book, drawdown=drawdown)
        if not pf["ok"] or pf["allowed_fraction"] < 1.0:
            continue                                    # no full slot -> skip (another may fit)
        book.append(cand)
        allocated.append(d)

    # 🤖 adversarial critic (advisory) — critique ONLY the allocated set (cheap), split off vetoes
    vetoed = []
    if critique and allocated:
        from . import agents
        kept = []
        for d in allocated:
            d["critic"] = agents.critic(d, regime=reg)
            (vetoed if d["critic"]["verdict"] == "veto" else kept).append(d)
        allocated = kept

    out = {"regime": {k: reg[k] for k in ("risk_mult", "setups", "reasons") if k in reg},
           "as_of": str(maxd)[:10], "n_candidates": len(fired), "n_eligible": len(decisions),
           "account": acct,                            # live account state (L9)
           "open_positions": open_positions,           # current book (skipped from candidates)
           "allocated": allocated,                     # buy THESE today (fit the risk budget)
           "watchlist": decisions[:limit]}             # all eligible, ranked (for context)
    if critique:
        out["regime_synth"] = synth
        out["vetoed"] = vetoed
        out["agents_on"] = True
    return out


def auto_take(max_age: int = 1, apply: bool = False) -> dict:
    """Paper 'auto-click +take': run today's decisions and OPEN each allocated BUY into the paper
    book automatically. NO real money / NO broker — writes ONLY the journal book.json. Idempotent:
    skips tickers already open (and run_universe already excludes held names from the candidates).
    apply=False previews what WOULD open. This lets the paper-trading + learning loop (fingerprint →
    autopsy → calibrate) run autonomously without a human clicking +take each morning."""
    from . import journal
    res = run_universe(max_age=max_age)
    allocated = res.get("allocated", [])
    held = {p["ticker"] for p in journal.open_positions()}
    taken, skipped = [], []
    for d in allocated:
        tk = d.get("ticker")
        if tk in held:
            skipped.append({"ticker": tk, "why": "already open"}); continue
        if not apply:
            taken.append({"ticker": tk, "edge": d.get("edge"), "shares": d.get("shares"),
                          "entry": d.get("entry"), "preview": True}); continue
        try:
            rec = journal.open_position({**d, "opened": res.get("as_of")})   # keeps edge/log/fingerprint
            taken.append({"ticker": tk, "edge": rec.get("edge"), "shares": rec.get("shares"),
                          "entry": rec.get("entry"), "opened": rec.get("opened")})
            held.add(tk)
        except Exception as e:
            skipped.append({"ticker": tk, "why": str(e)[:80]})
    return {"applied": apply, "as_of": res.get("as_of"), "n_allocated": len(allocated),
            "taken": taken, "skipped": skipped, "account": journal.account_state()}


def auto_close(apply: bool = False, trail: float = 0.25, maxhold: int = 60) -> dict:
    """Paper auto-EXIT: walk each open paper position forward on DAILY bars since it opened and close
    it on the validated exit rule — STRUCTURAL STOP (the position's own stop, stop-first) / 25%
    TRAILING stop from the peak (let winners run, as the edges were validated) / 60-bar time stop.
    Writes ONLY journal book.json (+ auto-autopsy on close → feeds calibrate). apply=False previews."""
    import duckdb
    from studio.db import tf_db_path
    from . import journal
    positions = journal.open_positions()
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    closed, kept = [], []
    for p in positions:
        tk = p["ticker"]; entry = float(p.get("entry") or 0); stop = float(p.get("stop") or 0)
        opened = str(p.get("opened", ""))[:10]
        if not entry:
            kept.append({"ticker": tk, "why": "no entry"}); continue
        bars = con.execute(
            "SELECT date, high, low, close FROM bars WHERE ticker=? "
            "AND substr(CAST(date AS VARCHAR),1,10) > ? ORDER BY date", [tk, opened]).fetchall()
        if not bars:
            kept.append({"ticker": tk, "why": "no bars since open (opened today)"}); continue
        peak = entry; ex_px = ex_why = ex_date = None
        for i, (d, hi, lo, cl) in enumerate(bars):
            hi, lo, cl = float(hi), float(lo), float(cl)
            if stop and lo <= stop:                       # structural stop first
                ex_px, ex_why, ex_date = stop, "structural stop", d; break
            peak = max(peak, hi)
            tstop = peak * (1 - trail)
            if lo <= tstop:                               # 25% trailing
                ex_px, ex_why, ex_date = round(tstop, 4), "trail-25%", d; break
            if i + 1 >= maxhold:                          # 60-bar time stop
                ex_px, ex_why, ex_date = cl, "60-bar time", d; break
        if ex_px is None:
            kept.append({"ticker": tk, "bars_held": len(bars), "peak": round(peak, 2),
                         "unrealized_pct": round((float(bars[-1][3]) / entry - 1) * 100, 2)}); continue
        if apply:
            rec = journal.close_position(tk, round(ex_px, 4), ex_why)
            a = rec.get("analysis", {})
            closed.append({"ticker": tk, "exit": rec["exit"], "pnl": rec["pnl"], "reason": ex_why,
                           "verdict": a.get("verdict"), "quality": a.get("quality")})
        else:
            closed.append({"ticker": tk, "would_close": round(ex_px, 2), "reason": ex_why,
                           "ret_pct": round((ex_px / entry - 1) * 100, 2), "preview": True})
    con.close()
    return {"applied": apply, "n_open": len(positions), "closed": closed, "kept": kept,
            "account": journal.account_state()}


def er_registry_cols() -> list:
    """registry edges the spine may FIRE on standalone: a live edge_replay col AND action=signal.
    action=context edges (broad/booster: atomic, zoneretest, parabola, p55, spring, t6_sc) stay in
    the brain's memory + revalidation but are not standalone buy-triggers — they inform confluence."""
    from . import registry
    return [e for e in registry.live_edges(direction="long")
            if e.get("col") and e.get("action") == "signal"]
