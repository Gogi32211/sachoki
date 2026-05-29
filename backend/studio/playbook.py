"""
studio/playbook.py — the Playbook engine.

Turns the predefined candidate setups (studio.playbook_config) into a small set of
*validated*, regime-gated, tradeable plays + today's live matches. The funnel:

    REGIME GATE (wyc_phase)  →  SETUP confluence (signal flags, all = 1)  →
      BACKTEST GATE  (expectancy > 0  AND  profit_factor > 1  AND
                      positive in BOTH time-halves  AND  enough trades)  →
      RANK  →  today's live tickers currently matching (price/volume-filtered)

HARD RULE: a setup is only "in the Playbook" (passed = True) if it clears the
backtest gate. We still return the rejected candidates (with their stats + a
reject reason) so the failure is visible, not hidden — but only survivors get a
live watchlist.

Efficiency: the bars table is read ONCE for the chosen universe; every setup is
then simulated in-memory via seq_backtest._run_on_df (the same tested engine the
Seq Lab "Realized Backtest" uses). Single universe per call (no cross-universe
dedup headache); default sp500 (liquid + fast).
"""
from __future__ import annotations

import logging
import math
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn
from studio.signal_stats import _safe_universe
from studio.seq_backtest import _run_on_df, _safe_flags, _PHASES
from studio.playbook_config import PLAYBOOK_SETUPS, SIGNAL_ROLES

log = logging.getLogger(__name__)

# clamp bounds (mirror seq_backtest.backtest so in-memory runs match the endpoint)
_T_MIN, _T_MAX = 0.5, 200.0
_S_MIN, _S_MAX = 0.5, 100.0
_H_MIN, _H_MAX = 1, 120


def _clamp(v, lo, hi):
    try:
        return max(lo, min(hi, float(v)))
    except (TypeError, ValueError):
        return lo


def _gate(bt: dict, min_trades: int) -> tuple[bool, str]:
    """Apply the backtest gate. Returns (passed, reason_if_failed)."""
    if not bt or "error" in bt:
        return False, (bt or {}).get("error", "no backtest result")
    ov = bt.get("overall") or {}
    h1 = bt.get("first_half") or {}
    h2 = bt.get("second_half") or {}
    n = ov.get("n", 0)
    if n < min_trades:
        return False, f"too few trades (n={n} < {min_trades})"
    if (ov.get("expectancy") or 0) <= 0:
        return False, f"expectancy ≤ 0 ({ov.get('expectancy')})"
    pf = ov.get("profit_factor")
    if pf is None or pf <= 1:
        return False, f"profit factor ≤ 1 ({pf})"
    if (h1.get("expectancy") or -1) <= 0 or (h2.get("expectancy") or -1) <= 0:
        return False, (f"not positive in both halves "
                       f"(1st={h1.get('expectancy')}, 2nd={h2.get('expectancy')})")
    return True, ""


def _live_tickers(
    df: pd.DataFrame,
    flags: list[str],
    phase: Optional[str],
    min_price: float,
    min_volume: float,
    max_live: int,
) -> list[dict]:
    """Latest bar per ticker that currently matches the setup (gate + all flags = 1),
    filtered to tradeable liquidity. Returns most-recent-then-priciest first."""
    flags = [c for c in flags if c in df.columns]
    if df.empty or not flags:
        return []
    # last (most recent) row per ticker — df is ordered by (ticker, date)
    last = df.groupby("ticker", sort=False).tail(1)

    cond = np.ones(len(last), dtype=bool)
    for c in flags:
        cond &= (pd.to_numeric(last[c], errors="coerce").fillna(0).to_numpy() == 1)
    if phase and "wyc_phase" in last.columns:
        cond &= (last["wyc_phase"].to_numpy() == phase)

    close = pd.to_numeric(last["close"], errors="coerce").fillna(0).to_numpy()
    cond &= (close >= min_price)
    if "avg_vol_20d" in last.columns:
        vol = pd.to_numeric(last["avg_vol_20d"], errors="coerce").fillna(0).to_numpy()
        cond &= (vol >= min_volume)

    hit = last[cond]
    if hit.empty:
        return []
    hit = hit.sort_values(["date", "close"], ascending=[False, False]).head(max_live)

    def _flt(v):
        try:
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else round(f, 2)
        except (TypeError, ValueError):
            return None

    out = []
    for _, r in hit.iterrows():
        out.append({
            "ticker":    str(r["ticker"]),
            "date":      str(r["date"])[:10],
            "close":     _flt(r.get("close")),
            "wyc_phase": (None if pd.isna(r.get("wyc_phase")) else str(r.get("wyc_phase"))),
            "avg_vol_20d": _flt(r.get("avg_vol_20d")),
        })
    return out


def build_playbook(
    universe:   str   = "sp500",
    min_trades: int   = 30,
    min_price:  float = 5.0,
    min_volume: float = 100_000,
    max_live:   int   = 40,
) -> dict:
    """Evaluate every predefined setup against one universe; gate, rank, attach
    live matches. Returns {universe, setups:[...], n_passed, params, roles}."""
    uni = _safe_universe(universe) or "sp500"
    min_trades = max(1, min(100_000, int(min_trades)))
    max_live   = max(1, min(500, int(max_live)))

    # union of every flag any setup needs (whitelisted — drops anything unknown)
    all_flags = _safe_flags(sorted({f for s in PLAYBOOK_SETUPS for f in s["signals"]}))

    load_cols = ["ticker", "date", "open", "high", "low", "close",
                 "volume", "avg_vol_20d", "wyc_phase"] + all_flags
    sel = ", ".join(dict.fromkeys(load_cols))

    conn = get_conn(read_only=True)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        sel_cols = [c for c in dict.fromkeys(load_cols) if c in available]
        sel = ", ".join(sel_cols)
        df = conn.execute(
            f"SELECT {sel} FROM bars WHERE universe = '{uni}' AND close IS NOT NULL "
            f"ORDER BY ticker, date"
        ).fetchdf()
    finally:
        conn.close()

    if df.empty:
        return {"universe": uni, "setups": [], "n_passed": 0,
                "params": {"min_trades": min_trades, "min_price": min_price,
                           "min_volume": min_volume, "universe": uni},
                "roles": SIGNAL_ROLES,
                "error": f"no rows for universe '{uni}'"}

    results = []
    for s in PLAYBOOK_SETUPS:
        flags = _safe_flags(s["signals"])
        present = [c for c in flags if c in df.columns]
        missing = [c for c in s["signals"] if c not in present]
        phase = s.get("wyc_phase") if s.get("wyc_phase") in _PHASES else None
        target = _clamp(s.get("target_pct", 8), _T_MIN, _T_MAX)
        stop   = _clamp(s.get("stop_pct", 4),  _S_MIN, _S_MAX)
        hold   = int(_clamp(s.get("max_hold", 15), _H_MIN, _H_MAX))
        side   = "short" if s.get("side") == "short" else "long"

        if not present:
            bt = {"error": f"no usable signal columns (missing: {missing})"}
        else:
            bt = _run_on_df(df, present, phase, target, stop, hold, side)

        passed, reason = _gate(bt, min_trades)
        live = (_live_tickers(df, present, phase, min_price, min_volume, max_live)
                if passed else [])

        results.append({
            "id":        s["id"],
            "name":      s["name"],
            "group":     s.get("group", ""),
            "side":      side,
            "signals":   present,
            "missing":   missing,
            "wyc_phase": phase or "all",
            "rule":      {"target_pct": target, "stop_pct": stop, "max_hold": hold},
            "thesis":    s.get("thesis", ""),
            "backtest":  bt,
            "passed":    passed,
            "reject_reason": "" if passed else reason,
            "live_tickers": live,
            "n_live":    len(live),
        })

    def _key(r):
        ov = (r.get("backtest") or {}).get("overall") or {}
        exp = ov.get("expectancy")
        return (1 if r["passed"] else 0, exp if isinstance(exp, (int, float)) else -1e9)

    results.sort(key=_key, reverse=True)

    return {
        "universe":  uni,
        "setups":    results,
        "n_passed":  sum(1 for r in results if r["passed"]),
        "n_total":   len(results),
        "params": {
            "universe": uni, "min_trades": min_trades,
            "min_price": min_price, "min_volume": min_volume, "max_live": max_live,
        },
        "roles": SIGNAL_ROLES,
        "date_range": [str(df["date"].min())[:10], str(df["date"].max())[:10]],
    }
