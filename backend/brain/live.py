"""brain/live.py — run the decision spine over TODAY's real edge fires.

Bridges the live detectors (edge_replay.latest_edges_map — what fired on each ticker's latest
bars) to the brain: map display-codes -> registry edges, fetch price/ADV/sector, and ask
spine.decide() for a BUY/NO with its full chain. Read-only; isolated from the live app.
"""
from __future__ import annotations



def _mtf_echo_map(tickers: list[str], last2: dict) -> dict:
    """{ticker: bool} — did ANY intraday TF (4h/1h/15m) print the strict REV-turn on the
    ticker's last daily bar (D) or the one before (D-1)?

    The REV-turn definition is copied VERBATIM from the Superchart ①②③ turn-echo annotator
    (main.py, turn_echo.py 2026-07-19): rolling-5 RSI min (shifted) < 38, RSI now 30-55,
    close up, RSI rising. Same definition on the display == same definition in the gate.

    HONESTY RULE: a ticker with no intraday rows is OMITTED (gate abstains), never False —
    absence of data is not absence of echo. Any DB error likewise omits rather than fails.
    """
    import numpy as np
    import pandas as pd
    import duckdb
    from studio.db import tf_db_path
    echo: dict = {}
    seen: dict = {}                       # ticker -> set of TF day-strings with a REV-turn
    have: set = set()                     # tickers with computable data on >=1 TF
    if not tickers:
        return echo
    ph = ",".join("?" * len(tickers))
    for tf in ("4h", "1h", "15m"):
        try:
            con = duckdb.connect(tf_db_path(tf), read_only=True)
            df = con.execute(
                f"SELECT ticker, date, close, rsi_14 FROM bars "
                f"WHERE ticker IN ({ph}) AND close >= 5 "
                f"AND date >= (SELECT max(date) FROM bars) - INTERVAL 15 DAY "
                f"ORDER BY ticker, date", tickers).fetchdf()
            con.close()
        except Exception:
            continue                       # this TF contributes nothing; others may
        for tk, g in df.groupby("ticker", sort=False):
            if len(g) < 6:
                continue
            have.add(tk)
            rs = g["rsi_14"].to_numpy(float); cl = g["close"].to_numpy(float)
            m5 = pd.Series(rs).rolling(5, min_periods=2).min().shift(1).to_numpy()
            up = np.concatenate([[False], cl[1:] > cl[:-1]])
            ri = np.concatenate([[False], rs[1:] > rs[:-1]])
            mm = (m5 < 38) & (rs >= 30) & (rs <= 55) & up & ri
            mm[np.isnan(m5) | np.isnan(rs)] = False
            if mm.any():
                days = {str(x)[:10] for x in g["date"][mm]}
                seen.setdefault(tk, set()).update(days)
    for tk in have:
        d01 = last2.get(tk)
        if not d01:
            continue
        echo[tk] = bool(seen.get(tk, set()) & set(d01))
    return echo


def _gex_states(tickers: list[str]) -> dict:
    """{ticker: {gex_net, gex_vrp, gex_age_days}} from the nightly 💠 forward log
    (gex_edge_log.parquet — snapshots on edge-fire days). REPORT-ONLY input: the log is
    days old (started 2026-07-22), so gates may describe, never size. Stale (>5d) or
    missing tickers are omitted -> the gate abstains."""
    import os
    import duckdb
    p = "/Users/sachoki/Desktop/sachoki-desktop/data/gex_edge_log.parquet"
    if not tickers or not os.path.exists(p):
        return {}
    try:
        con = duckdb.connect()
        ph = ",".join("?" * len(tickers))
        df = con.execute(
            f"""SELECT ticker, any_value(net_gex ORDER BY date DESC) net_gex,
                   any_value(vrp ORDER BY date DESC) vrp, max(date) d,
                   (SELECT max(date) FROM read_parquet('{p}')) mx
                FROM read_parquet('{p}') WHERE ticker IN ({ph})
                GROUP BY ticker""", tickers).fetchdf()
        con.close()
    except Exception:
        return {}
    from datetime import date as _date
    out = {}
    for r in df.itertuples():
        try:
            # dates arrive as ISO strings from the parquet (VARCHAR) — parse, don't subtract
            _p = lambda s: _date.fromisoformat(str(s)[:10])
            age = (_p(r.mx) - _p(r.d)).days
            if age > 5 or r.net_gex is None or r.net_gex != r.net_gex:
                continue
            out[r.ticker] = {"gex_net": float(r.net_gex),
                             "gex_vrp": float(r.vrp) if r.vrp == r.vrp else None,
                             "gex_age_days": int(age)}
        except Exception:
            continue
    return out


def _bar_states(tickers: list[str]) -> dict:
    """{ticker: state dict} for brain.gates, taken from the WARM edge_replay frame.

    Only keys we can compute honestly are included; anything else is omitted so the gate
    abstains loudly instead of passing silently. EMAs are derived here because the frame
    carries close but no ema columns, and Hurst is the variance-ratio estimator from the
    2026-07-30 study (std of k-step log returns scales as k^H).
    """
    import numpy as np
    import edge_replay as er          # module-level `er` lives inside run_universe, not here
    out: dict = {}
    try:
        grp, _ = er._frame(60, 3_000_000)
    except Exception:
        return out
    lags = (1, 2, 4, 8)
    x = np.log(np.asarray(lags, float)); xc = x - x.mean(); den = float((xc ** 2).sum())
    for t in tickers:
        g = grp.get(t)
        if g is None or len(g) < 60:
            continue
        c = g["close"]
        st: dict = {"close": float(c.iloc[-1])}
        try:
            st["month"] = int(str(g["date"].iloc[-1])[5:7])
        except Exception:
            pass
        for span, key in ((9, "e9"), (20, "e20"), (50, "e50"), (200, "e200")):
            if len(g) >= span:
                st[key] = float(c.ewm(span=span, adjust=False).mean().iloc[-1])
        for col, key in (("rs_intact", "rs_intact"), ("conso", "conso"),
                         ("iv_dry", "no_vol_event"), ("supp", "vol_extreme"),
                         ("h1_dr", "h1_dr"), ("iv_vspike", "iv_vspike"),
                         # 2026-08-06 macro/sector layer (gate_sector_lag, gate_macro_vix);
                         # both are computed in edge_replay._prep, so they ride the same frame
                         ("lead_in_lag", "lead_in_lag"), ("macro_vix_up", "macro_vix_up"),
                         # 2026-08-07 ADX regime (gate_adx_trend, report-only)
                         ("adx_trend_up", "adx_trend_up")):
            if col in g.columns:
                v = g[col].iloc[-1]
                if v == v:                       # not NaN
                    st[key] = bool(v)
        # Hurst, variance-ratio over the trailing 60 bars
        try:
            lp = np.log(c.replace(0, np.nan))
            num, ok = 0.0, True
            for j, k in enumerate(lags):
                sd = lp.diff(k).rolling(60, min_periods=30).std().iloc[-1]
                if not (sd == sd) or sd <= 0:
                    ok = False; break
                num += xc[j] * float(np.log(sd))
            if ok:
                st["hurst"] = round(num / den, 3)
        except Exception:
            pass
        out[t] = st
    # 🕯️ T1+NB indecision state (2026-08-03 suffix study): today's bar being a no-effort
    # both-wick T1 measured −1.24pp vs other T1s with the sign holding 6/6 years. Report-only
    # input; the mirror cell (T1G+NB) is a POSITIVE and lives in the edge book, not here.
    try:
        import duckdb as _dk
        from studio.db import tf_db_path as _tfp
        _ph = ",".join("?" * len(out))
        _con = _dk.connect(_tfp("1d"), read_only=True)
        _sfx = dict(_con.execute(
            f"""WITH b AS (SELECT ticker, date, any_value(full_suffix) sfx FROM bars
                  WHERE ticker IN ({_ph}) GROUP BY ticker, date),
                r AS (SELECT ticker, sfx,
                        row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn FROM b)
              SELECT ticker, sfx FROM r WHERE rn = 1""", list(out)).fetchall())
        _con.close()
        for t, st in out.items():
            sfx = _sfx.get(t)
            g = grp.get(t)
            if sfx and g is not None and len(g):
                st["t1_nb"] = bool(g["t"].iloc[-1] == "T1" and sfx == "NB")
    except Exception:
        pass
    # 📅 days since last SEC report event (2026-08-03 EDGAR study): the CAUSAL side of the
    # earnings-proximity finding — filings are public the moment they exist. Closes the
    # brain-map L0 "can't see earnings" gap.
    try:
        from datetime import date as _date
        from earnings_feed import load as _eload
        _ed = _eload()
        for t, st in out.items():
            ds = _ed.get(t) or []
            if not ds:
                continue
            try:
                last_bar = _date.fromisoformat(str(grp[t]["date"].iloc[-1])[:10])
                past = [d for d in ds if d <= last_bar.isoformat()]
                if past:
                    st["days_since_report"] = (last_bar - _date.fromisoformat(past[-1])).days
            except Exception:
                continue
    except Exception:
        pass
    # 💠 GEX context (2026-08-03, user request): report-only facts from the forward log —
    # net dealer gamma + VRP. Never sizes anything until the log matures (~4-6 months).
    try:
        for t, gx in _gex_states(list(out)).items():
            out[t].update(gx)
    except Exception:
        pass
    # 🕐 cross-TF echo (2026-08-03): tickers with intraday data get mtf_echo; the rest stay
    # unset so gate_mtf abstains for them instead of vetoing on missing data.
    try:
        last2 = {}
        for t, st in out.items():
            g = grp.get(t)
            ds = [str(x)[:10] for x in g["date"].iloc[-2:]] if g is not None else []
            if ds:
                last2[t] = ds
        for t, v in _mtf_echo_map(list(out), last2).items():
            out[t]["mtf_echo"] = v
    except Exception:
        pass                              # echo unavailable -> gate abstains, loudly
    return out

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

    fired, fire_age = {}, {}
    for t, fires in emap.items():
        pairs = [(code2col.get(c), age) for c, age in fires if age <= max_age]
        pairs = [(c, age) for c, age in pairs if c and c in reg_cols]
        if pairs:
            fired[t] = [c for c, _ in pairs]
            fire_age[t] = min(age for _, age in pairs)   # freshest registry fire = bar D
    if not fired:
        return {"regime": reg, "as_of": None, "n_candidates": 0, "decisions": [],
                "note": "no registry-edge fires today"}

    # price + 20d ADV$ for the fired tickers
    tickers = list(fired)
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    ph = ",".join("?" * len(tickers))
    px = con.execute(
        f"""WITH b AS (SELECT ticker, date, any_value(close) AS "close",
              any_value(low) AS "low", any_value(close*volume) AS dvol
            FROM bars WHERE ticker IN ({ph}) AND close>=5 GROUP BY ticker, date),
          -- GROUP BY first: multi-universe tickers store each bar once per universe (×3),
          -- which made rn count ROWS not DAYS (rn=2 was the SAME day, adv spanned ~7 days)
          r AS (SELECT *, row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn FROM b)
          SELECT ticker, any_value(CASE WHEN rn=1 THEN close END) px,
                 avg(CASE WHEN rn<=20 THEN dvol END) adv,
                 min(CASE WHEN rn<=15 THEN low END) swing_low,
                 any_value(CASE WHEN rn=1 THEN low END) low1,
                 any_value(CASE WHEN rn=2 THEN low END) low2,
                 any_value(CASE WHEN rn=1 THEN date END) d1,
                 any_value(CASE WHEN rn=2 THEN date END) d2
          FROM r WHERE rn<=20 GROUP BY ticker""", tickers).fetchdf()
    maxd = con.execute("SELECT max(date) FROM bars WHERE universe<>'index'").fetchone()[0]
    con.close()
    price = {r.ticker: (float(r.px) if r.px is not None else None,
                        float(r.adv) if r.adv else None,
                        float(r.swing_low) if r.swing_low is not None else None)
             for r in px.itertuples()}
    # bar-D anchor for the 🎯 pullback entry policy: the FIRE bar's own low + date
    fire_bar = {r.ticker: ((float(r.low1), str(r.d1)[:10]) if fire_age.get(r.ticker, 0) == 0
                           else (float(r.low2) if r.low2 == r.low2 else None, str(r.d2)[:10]))
                for r in px.itertuples()}
    try:
        _, smap = er._load_rs_ref()
    except Exception:
        smap = {}

    # ── bar STATE for the L4 gate layer ───────────────────────────────────────────
    # Read off the warm edge_replay frame (latest_edges_map already built it), so the gates
    # see exactly the same numbers the backtest did. A key we cannot compute honestly is
    # LEFT OUT — gates.evaluate() then abstains and says so, which is the whole point.
    #
    # mtf_echo (wired 2026-08-03): the strict intraday REV-turn on 4h/1h/15m on D or D-1,
    # the same definition as the Superchart ①②③ turn-echo — NOT the frame's h1_dr (that is
    # the narrow 1H dual-reclaim, ~0.42 fires/ticker-year, and would veto nearly everything).
    # Tickers without intraday data get no key -> gate_mtf abstains for them.
    bar_state = _bar_states(tickers)

    decisions = []
    for t, cols in fired.items():
        if t in held:                                  # already in the book — skip
            continue
        p, adv, swing = price.get(t, (None, None, None))
        if not p:
            continue
        d = spine.decide(t, cols, p, sector=smap.get(t, "?"), adv_dollars=adv, swing_low=swing,
                         bar_state=bar_state.get(t),
                         open_positions=open_positions, drawdown=drawdown,
                         losing_streak=losing_streak, regime=reg)
        if d.get("decision") == "BUY":
            # 🎯 pullback entry policy (project_entry_timing, +0.74..+1.89pp matched, 6/6yr):
            # don't chase the fire — queue a dip-and-reclaim order under the fire bar's low.
            fb = fire_bar.get(t)
            if fb and fb[0] is not None:
                d["pullback"] = {"below": round(fb[0], 4), "fire_date": fb[1],
                                 "expire_bars": 5}
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
    """Paper 'auto-click +take' — PULLBACK edition (2026-08-03, project_entry_timing).

    The old behavior opened every allocated BUY at once (= the measured LOSING policy of
    chasing the fire). Now each daily run does two things:
      1. FILL/EXPIRE yesterday's pending pullback orders (dip-and-reclaim under the fire
         bar's low within 5 bars → open at that bar's close; else expire).
      2. QUEUE a pending pullback order for each newly allocated BUY instead of buying it.
    NO real money / NO broker — writes ONLY brain/book.json + brain/pending.json.
    apply=False previews both steps. A fire that never dips is never bought — that cost is
    part of the measured policy (see brain/pending.py docstring)."""
    from . import journal, pending
    fills = pending.check_fills(apply=apply)             # step 1 — resolve existing orders
    res = run_universe(max_age=max_age)
    allocated = res.get("allocated", [])
    held = {p["ticker"] for p in journal.open_positions()}
    queued = {o["ticker"] for o in pending.list_pending()}
    # 🧠 step 1½ — OPUS FINAL DECIDER (2026-08-03): the last word on today's set. It sees every
    # candidate's full gate trace + regime + account + open data-gaps and may take or skip —
    # nothing else. Fail-open: with no API key the spine's decisions stand unchanged.
    opus = {"decisions": {}, "summary": None}
    if allocated:
        from . import agents, requests as breq
        try:
            open_qs = breq.list_requests(status="open")
        except Exception:
            open_qs = []
        opus = agents.final_decider(allocated, regime=res.get("regime"),
                                    account=res.get("account"), open_questions=open_qs)
    placed, skipped = [], []
    for d in allocated:
        tk = d.get("ticker")
        if tk in held:
            skipped.append({"ticker": tk, "why": "already open"});  continue
        if tk in queued:
            skipped.append({"ticker": tk, "why": "already pending"});  continue
        ov = opus["decisions"].get(tk)
        if ov and ov["action"] == "skip":
            skipped.append({"ticker": tk, "why": f"🧠 opus: {ov['reason']}"});  continue
        pb = d.get("pullback")
        if not pb:                                        # no fire-bar low -> no honest anchor
            skipped.append({"ticker": tk, "why": "no pullback anchor (fire bar low missing)"})
            continue
        o = pending.place(d, fire_date=pb["fire_date"], below=pb["below"], apply=apply)
        if o.get("skipped"):
            skipped.append({"ticker": tk, "why": o["skipped"]})
        else:
            placed.append({"ticker": tk, "edge": d.get("edge"), "below": pb["below"],
                           "fire_date": pb["fire_date"], "shares": d.get("shares"),
                           "preview": not apply})
    return {"applied": apply, "as_of": res.get("as_of"), "n_allocated": len(allocated),
            "taken": fills["filled"],                    # backward-compat: fills open positions
            "placed": placed, "expired": fills["expired"], "waiting": fills["waiting"],
            "skipped": skipped, "opus": opus.get("summary"), "opus_model": opus.get("model"),
            "account": journal.account_state()}


def auto_close(apply: bool = False, trail: float = 0.25, maxhold: int = 60,
               atr_k: float = 12.0) -> dict:
    """Paper auto-EXIT: walk each open paper position forward on DAILY bars since it opened and close
    it on the validated exit rule — STRUCTURAL STOP (the position's own stop, stop-first) /
    ATR-ADAPTIVE TRAILING stop from the peak / 60-bar time stop.
    ATR trail (2026-08-06, user-approved 6yr validation, 49/49 setups improved): per-position
    trail = clip(atr_k x ATR14%/close at entry, 15%, 60%) — a fixed 25% sits inside a volatile
    name's noise band and was the whole "segment weakness" artifact. atr_k=None reverts to the
    fixed `trail` fraction. Writes ONLY journal book.json. apply=False previews."""
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
        # per-position ATR trail from the entry-day state (fallback: fixed `trail`)
        pos_trail = trail
        if atr_k:
            try:
                row = con.execute(
                    "SELECT any_value(atr_14)/any_value(\"close\") FROM bars "
                    "WHERE ticker=? AND substr(CAST(date AS VARCHAR),1,10) <= ? "
                    "GROUP BY date ORDER BY date DESC LIMIT 1", [tk, opened]).fetchone()
                if row and row[0] and row[0] > 0:
                    pos_trail = min(0.60, max(0.15, atr_k * float(row[0])))
            except Exception:
                pass
        # GROUP BY date — multi-universe tickers duplicate each bar ×3, which made the 60-BAR
        # time stop fire after 60 ROWS (= 20 days for a triple-listed name)
        bars = con.execute(
            "SELECT date, any_value(high) h, any_value(low) l, any_value(close) c FROM bars "
            "WHERE ticker=? AND substr(CAST(date AS VARCHAR),1,10) > ? "
            "GROUP BY date ORDER BY date", [tk, opened]).fetchall()
        if not bars:
            kept.append({"ticker": tk, "why": "no bars since open (opened today)"}); continue
        peak = entry; ex_px = ex_why = ex_date = None
        for i, (d, hi, lo, cl) in enumerate(bars):
            hi, lo, cl = float(hi), float(lo), float(cl)
            if stop and lo <= stop:                       # structural stop first
                ex_px, ex_why, ex_date = stop, "structural stop", d; break
            peak = max(peak, hi)
            tstop = peak * (1 - pos_trail)
            if lo <= tstop:                               # ATR-adaptive (or fixed) trailing
                ex_px, ex_why, ex_date = round(tstop, 4), f"trail-{pos_trail*100:.0f}%", d; break
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
