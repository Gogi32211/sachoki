"""
ai_journal/capit_journal.py — a SEPARATE paper-trading journal for the validated
CAPITULATION-BOUNCE edge (L34/L46 + RSI<30 + CCI<-100, drawdown knife-guard).
Independent of the Atomic journal and the AI Journal — its own table, its own rules.
Opens paper positions from the capit scan (auto, with the date), sizes them by the
market regime, and grades them with the VALIDATED exit. Paper only — no real orders.

Exit rules (this is the key difference from the Atomic journal): a capitulation bounce
is HELD, not stopped — the rigorous gap-aware path-sim showed hold-20 +4.6% cost-adj
(5/6 yrs) while a -15% tight stop cut the bounce (+1.5%). So: entry = signal bar close
(paper fill), HOLD ~20 bars, NO tight stop, only a -35% CATASTROPHE floor (delisting
protection — the >-60%-at-entry knives are already excluded by the scan). Size = base ×
regime conv_mult. Dedup: one open position per ticker.
"""
from __future__ import annotations
import json
from .db import get_journal_conn, get_analytics_conn

HORIZON = 20                 # bars held (the validated hold)
CATASTROPHE_PCT = 0.35       # wide floor — only a true collapse exits early (no tight stop)
BASE_SIZE_PCT = 4.0          # % of (paper) capital per position before regime scaling
START_CAPITAL = 10_000.0


_COLS = ("id,ticker,universe,signal_date,open_date,opened_at,entry_px,floor_px,size_pct,"
         "cap_score,atoms,regime,status,mark_px,upnl_pct,close_date,exit_px,exit_reason,pnl_pct,verdict")


def _ensure():
    c = get_journal_conn()
    c.execute("""CREATE TABLE IF NOT EXISTS capit_position (
        id BIGINT PRIMARY KEY, ticker VARCHAR, universe VARCHAR, signal_date DATE, open_date DATE, opened_at TIMESTAMP,
        entry_px DOUBLE, floor_px DOUBLE, size_pct DOUBLE,
        cap_score INTEGER, atoms VARCHAR, regime VARCHAR,
        status VARCHAR, mark_px DOUBLE, upnl_pct DOUBLE,
        close_date DATE, exit_px DOUBLE, exit_reason VARCHAR, pnl_pct DOUBLE, verdict VARCHAR)""")
    cols = [r[0] for r in c.execute("DESCRIBE capit_position").fetchall()]
    if "signal_date" not in cols:   # migrate the pre-next-open-fill schema
        c.execute("ALTER TABLE capit_position ADD COLUMN signal_date DATE")
    return c


def _next_open(a, ticker, universe, after_date):
    """The OPEN of the first trading bar AFTER `after_date` (execution realism — a
    signal fired while the market is closed fills at the NEXT session's open, not the
    signal-bar close). Returns (date, open) or None if that bar hasn't printed yet."""
    r = a.execute("SELECT date, open FROM bars WHERE ticker=? AND universe=? AND date > ? ORDER BY date LIMIT 1",
                  [ticker, universe, after_date]).fetchone()
    return (str(r[0])[:10], float(r[1])) if r and r[1] else None


def open_from_scan(top: int = 20, min_score: int = 60, universe: str | None = None) -> dict:
    from .capit_scan import capit_scan
    scan = capit_scan(max_age_days=2, universe=universe)
    reg = scan["regime"]
    c = _ensure()
    a = get_analytics_conn()
    try:
        held = {r[0] for r in c.execute("SELECT ticker FROM capit_position WHERE status IN ('OPEN','PENDING')").fetchall()}
        nxt = (c.execute("SELECT coalesce(max(id),0) FROM capit_position").fetchone()[0] or 0) + 1
        opened, pending = [], []
        cand = [r for r in scan["rows"] if r["score"] >= min_score and r["ticker"] not in held][:top]
        for r in cand:
            size = round(BASE_SIZE_PCT * (reg.get("conv_mult") or 1.0), 2)
            fill = _next_open(a, r["ticker"], r["universe"], r["signal_date"])
            if fill:                                            # next session already printed → fill at open
                od, entry = fill
                c.execute(f"INSERT INTO capit_position ({_COLS}) VALUES (?,?,?,?,?,now(),?,?,?,?,?,?,'OPEN',?,0,NULL,NULL,NULL,NULL,'PENDING')",
                          [nxt, r["ticker"], r["universe"], r["signal_date"], od, entry,
                           round(entry * (1 - CATASTROPHE_PCT), 4), size, r["score"],
                           json.dumps(r["atoms"]), reg["label"], entry])
                opened.append(r["ticker"])
            else:                                               # market closed / no next bar yet → PENDING (filled on grade)
                c.execute(f"INSERT INTO capit_position ({_COLS}) VALUES (?,?,?,?,NULL,now(),NULL,NULL,?,?,?,?,'PENDING',NULL,NULL,NULL,NULL,NULL,NULL,'PENDING')",
                          [nxt, r["ticker"], r["universe"], r["signal_date"], size, r["score"],
                           json.dumps(r["atoms"]), reg["label"]])
                pending.append(r["ticker"])
            nxt += 1
    finally:
        a.close()
    c.close()
    return {"opened": opened, "pending": pending, "count": len(opened) + len(pending),
            "regime": reg["label"], "as_of": scan["as_of"]}


def grade() -> dict:
    """Fill any PENDING positions at the next session's open, then walk forward the OPEN
    positions. Exit = -35% catastrophe floor (gap-aware) OR the 20-bar horizon close."""
    c = _ensure()
    a = get_analytics_conn()
    # 1. fill PENDING at the next session's open (now that the bar may have printed)
    pend = c.execute("SELECT id,ticker,universe,signal_date FROM capit_position WHERE status='PENDING'").fetchdf()
    filled = 0
    for _, p in pend.iterrows():
        fill = _next_open(a, p["ticker"], p["universe"], p["signal_date"])
        if fill:
            od, entry = fill
            c.execute("""UPDATE capit_position SET status='OPEN', open_date=?, entry_px=?, floor_px=?, mark_px=? WHERE id=?""",
                      [od, entry, round(entry * (1 - CATASTROPHE_PCT), 4), entry, int(p["id"])])
            filled += 1
    rows = c.execute("SELECT id,ticker,universe,open_date,entry_px,floor_px FROM capit_position WHERE status='OPEN'").fetchdf()
    if len(rows) == 0:
        a.close(); c.close(); return {"graded": 0, "closed": 0, "filled": filled}
    closed = 0
    try:
        for _, p in rows.iterrows():
            bars = a.execute("""SELECT date,open,high,low,close FROM bars
                WHERE ticker=? AND universe=? AND date > ? ORDER BY date LIMIT ?""",
                [p["ticker"], p["universe"], p["open_date"], HORIZON]).fetchdf()
            if len(bars) == 0:
                continue
            entry, floor = float(p["entry_px"]), float(p["floor_px"])
            exit_px = exit_reason = exit_date = None
            for _, b in bars.iterrows():
                o, l = float(b["open"]), float(b["low"])
                if l <= floor:                                  # catastrophe floor (gap-aware)
                    exit_px = min(floor, o); exit_reason = "catastrophe"; exit_date = b["date"]; break
            if exit_px is None and len(bars) >= HORIZON:        # held to horizon — exit at close
                lastb = bars.iloc[-1]; exit_px = float(lastb["close"]); exit_reason = "hold20"; exit_date = lastb["date"]
            mark = float(bars.iloc[-1]["close"])
            if exit_px is not None:
                pnl = round((exit_px / entry - 1) * 100, 2)
                verdict = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
                c.execute("""UPDATE capit_position SET status='CLOSED', close_date=?, exit_px=?, exit_reason=?,
                    pnl_pct=?, verdict=?, mark_px=?, upnl_pct=? WHERE id=?""",
                    [exit_date, round(exit_px, 4), exit_reason, pnl, verdict, round(exit_px, 4), pnl, int(p["id"])])
                closed += 1
            else:
                c.execute("UPDATE capit_position SET mark_px=?, upnl_pct=? WHERE id=?",
                          [round(mark, 4), round((mark / entry - 1) * 100, 2), int(p["id"])])
    finally:
        a.close()
    c.close()
    return {"graded": int(len(rows)), "closed": closed, "filled": filled}


def replay(months: int = 6, universe: str | None = None, min_score: int = 60,
           dv_floor: float = 300_000, limit: int = 200, recipe: str = "B",
           entry_pct: float = 0.0, target_pct: float = 0.0, stop_pct: float = 0.0,
           hold: int = HORIZON, entry_win: int = 5) -> dict:
    """MANUAL params (all 0/default = the EXACT production journal, unchanged):
      entry_pct  > 0 → buy via a LIMIT that % below the signal-bar close (fills only if a
                       forward bar trades there within `entry_win` bars; else 'nofill').
                       0 → market fill at next-open (production).
      target_pct > 0 → take-profit that % above entry. 0 → no target (hold to horizon).
      stop_pct   > 0 → stop-loss that % below entry (stop-first vs target). 0 → no stop
                       (only the −35% catastrophe floor, production).
      hold           → max bars held before a time-stop close (default 20).
    Every trade also reports mfe/mae = the max up/down spike (%) between fill and exit."""
    """Historical backtest of the Capit edge over the last `months`, using the EXACT
    journal rules (entry = signal close, HOLD 20 bars, -35% catastrophe floor, one
    open per ticker). Builds the track record retroactively. Equal 4%-paper-bet sizing
    (matches the live journal's equity calc). Signals in the last ~20 trading days
    have no full forward window yet → reported as 'still open', excluded from stats."""
    import numpy as np, pandas as pd
    from datetime import date as _d, timedelta
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        uni = f"AND universe = '{universe}'" if universe in ("sp500", "nasdaq", "russell2k") else ""
        lookback_days = int(months * 31) + 70
        df = a.execute(f"""
            SELECT ticker, universe, date, open, high, low, close, volume, avg_vol_20d,
                   l_sig, rsi_14, cci_20, vol_bucket, sig_blue AS blue, sig_fri64 AS fri64, d_absorb_bear AS absb
            FROM bars
            WHERE avg_vol_20d > 0 {uni}
              AND date >= DATE '{as_of}' - INTERVAL {lookback_days} DAY
            ORDER BY ticker, universe, date
        """).fetchdf()
    finally:
        a.close()
    if len(df) == 0:
        return {"as_of": as_of, "months": months, "trades": [], "stats": {}, "by_month": []}
    g = df.groupby(["ticker", "universe"], sort=False)
    df["c20"] = g["close"].transform(lambda s: s.shift(20))
    df["chg20"] = (df.close / df.c20 - 1) * 100
    df["volx"] = df.volume / df.avg_vol_20d.replace(0, np.nan)
    for c in ("blue", "fri64", "absb"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(np.int8)
    # E2 recipe (track-record deep-dive): RSI floor 15, drawdown guard -45, exclude
    # FRI64-coil + absorption (path-sim artifacts: -0.76 / -1.59 mean); BLUE-only kept.
    # recipe='baseline' = the pre-deep-dive logic (for before/after comparison CSVs).
    core = df.l_sig.isin(["L34", "L46"]) & (df.cci_20 < -100) & (df.close * df.volume >= dv_floor)
    if recipe == "baseline":
        cap = core & (df.rsi_14 < 30) & ((df.c20.isna()) | (df.chg20 > -60))
    else:
        e2 = (core & (df.rsi_14 >= 15) & (df.rsi_14 < 30) & ((df.c20.isna()) | (df.chg20 > -45))
              & (df.fri64 == 0) & (df.absb == 0))
        if recipe == "A":      # E2 + strong-vol buckets (high-mean, tail-seeking)
            cap = e2 & df.vol_bucket.isin(["L", "VB", "W"])
        elif recipe == "B":    # PRODUCTION: E2 + shallow dip + robust knife exclusions ($1-2, new-list)
            cap = (e2 & df.chg20.notna() & (df.chg20 > -25)
                   & ~((df.close >= 1) & (df.close < 2)))
        else:                  # e2 (the base)
            cap = e2
    sc = np.full(len(df), 45.0)
    sc += np.where(df.chg20.between(-45, -10), 15, 0)
    sc += np.where(df.close <= df.open, 15, 0)
    sc += np.where((df.volx >= 1.5) & (df.volx < 5), 15, 0)
    sc += np.where(df.volx >= 7, -15, 0)
    sc += np.where(df.blue == 1, 12, 0)
    sc += np.where(df.rsi_14 < 20, 5, 0)
    df["score"] = np.clip(sc, 0, 100).astype(int)
    win_start = (_d.fromisoformat(as_of) - timedelta(days=int(months * 30.5))).isoformat()
    df["dstr"] = df.date.astype(str).str[:10]

    trades, still_open, nofill, seen = [], 0, 0, set()
    for (tk, u), grp in df.groupby(["ticker", "universe"], sort=False):
        o = grp.open.to_numpy(); hi = grp.high.to_numpy(); lo = grp.low.to_numpy(); c = grp.close.to_numpy()
        capf = cap.loc[grp.index].to_numpy(); scn = grp.score.to_numpy()
        chgA = grp.chg20.to_numpy(); vxA = grp.volx.to_numpy()   # derived (not DB cols) — kept for analysis
        ds = grp.dstr.to_numpy(); n = len(c)
        last = -999
        for i in range(n):
            if not capf[i] or scn[i] < min_score or ds[i] < win_start:
                continue
            if i - last < hold:          # one open per ticker (no re-entry while held)
                continue
            key = (tk, ds[i])            # dedup the SAME signal across universes (sp500∩nasdaq)
            if key in seen:
                continue
            if i + 1 >= n:               # next session hasn't printed → would be a PENDING fill
                still_open += 1; continue
            # ── ENTRY: market next-open (production) OR a LIMIT a fixed % below signal close ──
            if entry_pct > 0:
                limit_px = c[i] * (1 - entry_pct)         # ref = signal-bar close
                fj = None
                for j in range(i + 1, min(i + 1 + entry_win, n)):
                    if lo[j] <= limit_px:                 # gap-down fills at the open
                        entry = min(o[j], limit_px) if o[j] < limit_px else limit_px
                        fj = j; break
                if fj is None:                            # limit never reached in the window
                    nofill += 1; continue
            else:
                entry = o[i + 1]; fj = i + 1              # ENTRY = next session's OPEN
            if entry <= 0:
                continue
            last = i; seen.add(key)
            # ── EXIT: stop-first path-sim — downside (stop / catastrophe) checked before target ──
            floor   = entry * (1 - CATASTROPHE_PCT)
            stop_px = entry * (1 - stop_pct)   if stop_pct   > 0 else None
            tgt_px  = entry * (1 + target_pct) if target_pct > 0 else None
            hold_end = min(fj + hold, n - 1)
            exit_px = reason = exit_date = None
            mfe = mae = 0.0                                # max up / down spike between fill and exit
            for j in range(fj + 1, hold_end + 1):
                mfe = max(mfe, (hi[j] / entry - 1) * 100)
                mae = min(mae, (lo[j] / entry - 1) * 100)
                if stop_px is not None and lo[j] <= stop_px:
                    exit_px = min(stop_px, o[j]); reason = "stop"; exit_date = ds[j]; break
                if lo[j] <= floor:
                    exit_px = min(floor, o[j]); reason = "catastrophe"; exit_date = ds[j]; break
                if tgt_px is not None and hi[j] >= tgt_px:
                    exit_px = o[j] if o[j] >= tgt_px else tgt_px; reason = "target"; exit_date = ds[j]; break
            if exit_px is None:
                if fj + hold <= n - 1:
                    exit_px = c[hold_end]; reason = f"hold{hold}"; exit_date = ds[hold_end]
                else:
                    still_open += 1; continue             # not enough forward bars yet
            pnl = round((exit_px / entry - 1) * 100, 2)
            _chg = float(chgA[i]) if chgA[i] == chgA[i] else None
            _vx = float(vxA[i]) if vxA[i] == vxA[i] else None
            trades.append({"ticker": tk, "universe": u, "signal_date": ds[i], "open_date": ds[fj],
                           "close_date": exit_date, "entry": round(entry, 2), "exit": round(exit_px, 2),
                           "pnl": pnl, "reason": reason, "score": int(scn[i]), "month": ds[fj][:7],
                           "mfe": round(mfe, 1), "mae": round(mae, 1),
                           "chg20": round(_chg, 1) if _chg is not None else None,
                           "volx": round(_vx, 1) if _vx is not None else None})

    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    eq, curve = START_CAPITAL, []
    SIZE = BASE_SIZE_PCT / 100.0
    for t in sorted(trades, key=lambda x: x["close_date"]):
        eq += START_CAPITAL * SIZE * (t["pnl"] / 100.0)
        curve.append({"date": t["close_date"], "equity": round(eq, 0)})
    by_month = {}
    for t in trades:
        m = by_month.setdefault(t["month"], {"month": t["month"], "n": 0, "sum": 0.0, "w": 0})
        m["n"] += 1; m["sum"] += t["pnl"]; m["w"] += 1 if t["pnl"] > 0 else 0
    by_month = [{"month": v["month"], "n": v["n"], "avg_pnl": round(v["sum"] / v["n"], 2),
                 "win_rate": round(v["w"] / v["n"] * 100, 0)} for v in sorted(by_month.values(), key=lambda x: x["month"])]
    stats = {
        "n": len(trades), "win_rate": round(len(wins) / len(pnls) * 100, 1) if pnls else None,
        "avg_pnl": round(float(np.mean(pnls)), 2) if pnls else None,
        "median_pnl": round(float(np.median(pnls)), 2) if pnls else None,
        "best": round(max(pnls), 1) if pnls else None, "worst": round(min(pnls), 1) if pnls else None,
        "catastrophe_pct": round(sum(1 for t in trades if t["reason"] == "catastrophe") / len(trades) * 100, 1) if trades else None,
        "total_pct": round(sum(pnls), 1) if pnls else 0,
        "equity_end": round(eq, 0), "equity_pct": round((eq / START_CAPITAL - 1) * 100, 1),
        "still_open": still_open,
        # manual-mode extras (also harmlessly present in production: nofill=0, target_hit=None)
        "nofill": nofill,
        "fill_rate": round(len(trades) / (len(trades) + nofill) * 100, 1) if (len(trades) + nofill) else None,
        "target_hit_pct": round(sum(1 for t in trades if t["reason"] == "target") / len(trades) * 100, 1) if trades else None,
        "stop_hit_pct":   round(sum(1 for t in trades if t["reason"] == "stop")   / len(trades) * 100, 1) if trades else None,
        "avg_mfe": round(float(np.mean([t["mfe"] for t in trades])), 1) if trades else None,
        "avg_mae": round(float(np.mean([t["mae"] for t in trades])), 1) if trades else None,
    }
    return {"as_of": as_of, "months": months, "win_start": win_start,
            "stats": stats, "by_month": by_month, "curve": curve,
            "trades": sorted(trades, key=lambda x: x["close_date"], reverse=True)[:limit]}


def summary() -> dict:
    import numpy as np, pandas as pd
    c = _ensure()
    df = c.execute("SELECT * FROM capit_position ORDER BY status, open_date DESC").fetchdf()
    c.close()
    df = df.replace({np.nan: None})
    rows = df.to_dict("records") if len(df) else []
    def _clean(v):
        if isinstance(v, (np.integer,)): return int(v)
        if isinstance(v, (np.floating,)): return None if pd.isna(v) else float(v)
        return v
    rows = [{k: _clean(v) for k, v in r.items()} for r in rows]
    for r in rows:
        for k in ("open_date", "close_date", "opened_at"):
            if r.get(k) is not None:
                r[k] = str(r[k])[:19]
        try:
            r["atoms"] = json.loads(r.get("atoms") or "[]")
        except Exception:
            r["atoms"] = []
    op = [r for r in rows if r["status"] == "OPEN"]
    cl = [r for r in rows if r["status"] == "CLOSED"]
    pend = [r for r in rows if r["status"] == "PENDING"]
    live = {}
    if op:
        try:
            from data_polygon import fetch_snapshot
            live = fetch_snapshot([r["ticker"] for r in op])
        except Exception:
            live = {}
    try:
        from . import memory as _mem
        meta = _mem.load_ticker_meta()
    except Exception:
        meta = {}
    open_pnl_live = 0.0
    for r in op:
        m = meta.get(r["ticker"], {}) if isinstance(meta, dict) else {}
        r["mcap_bucket"] = m.get("mcap_bucket") or ""
        r["dollar_buy"] = round(START_CAPITAL * (r.get("size_pct") or 0) / 100.0, 0)
        lp = (live.get(r["ticker"]) or {}).get("price")
        if lp and r.get("entry_px"):
            r["now_px"] = round(float(lp), 4)
            r["upnl_pct"] = round((float(lp) / r["entry_px"] - 1) * 100, 2)
        else:
            r["now_px"] = r.get("mark_px")
        if r.get("upnl_pct") is not None and r.get("size_pct"):
            open_pnl_live += START_CAPITAL * (r["size_pct"] / 100.0) * (r["upnl_pct"] / 100.0)
    realized = [r["pnl_pct"] for r in cl if r.get("pnl_pct") is not None]
    wins = [x for x in realized if x > 0]
    eq = START_CAPITAL
    for r in cl:
        if r.get("pnl_pct") is not None and r.get("size_pct"):
            eq += START_CAPITAL * (r["size_pct"] / 100.0) * (r["pnl_pct"] / 100.0)
    try:
        from . import regime as _reg
        reg = _reg.compute_regime()
    except Exception:
        reg = {"label": "NEUTRAL", "score": None, "conv_mult": 1.0}
    stats = {
        "open": len(op), "closed": len(cl),
        "win_rate": round(len(wins) / len(realized) * 100, 1) if realized else None,
        "avg_pnl": round(sum(realized) / len(realized), 2) if realized else None,
        "total_realized_pct": round(sum(realized), 2) if realized else 0,
        "open_pnl_live": round(open_pnl_live, 2),
        "pending": len(pend),
        "equity": round(eq, 2),
        "equity_live": round(eq + open_pnl_live, 2),
    }
    return {"open": op, "closed": cl, "pending": pend, "stats": stats,
            "regime": {"label": reg["label"], "score": reg["score"], "conv_mult": reg["conv_mult"]}}
