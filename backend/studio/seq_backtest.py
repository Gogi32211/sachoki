"""
studio/seq_backtest.py — Realized backtest of a signal/entry condition.

Turns a statistical edge into a tradeable rule and measures what you'd ACTUALLY
have made — not idealised "to next pivot" returns.

Rule (long; lookahead-free):
  ENTRY : next bar's OPEN after a bar where ALL chosen signal flags are 1
          (+ optional Wyckoff phase / universe filter). One position per ticker
          at a time (no pyramiding) — new triggers ignored until the trade exits.
  EXIT  : whichever comes first, scanned bar-by-bar AFTER entry —
            • target  : intrabar high >= entry*(1+target_pct/100)
            • stop    : intrabar low  <= entry*(1-stop_pct/100)   (assumed first if
                        both hit the same bar — conservative)
            • time    : close of the max_hold-th bar
  side='short' mirrors the rule (entry short, target = down move, stop = up move).

Returns realised metrics: n trades, win%, avg/median return, expectancy, profit
factor, avg win/loss, max drawdown, avg bars held, exit-reason mix, and a
first-half / second-half time split (out-of-sample sniff test).

All inputs validated/whitelisted (no SQL injection; signal cols checked against
the known signal set).
"""
from __future__ import annotations

import logging
import math
from random import Random as _Random
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn
from studio.signal_stats import _safe_universe, ALL_SIGNALS

log = logging.getLogger(__name__)

# Fixed seed for the ticker-order shuffle used when the max_trades cap bites:
# deterministic (same query → same result) yet alphabet-unbiased (see _run_on_df).
_SAMPLE_SEED = 1234

_PHASES = {"MARKUP", "MKDN", "ACC_TR", "DIST_TR", "SPRING", "UTAD", "SOS", "SOW", "NEUTRAL"}
# extra boolean-ish columns a user may legitimately gate on (beyond ALL_SIGNALS)
_EXTRA_FLAGS = {
    "ad_fresh", "ad_cluster", "wyc_spring", "wyc_sos", "wyc_in_tr", "wyc_sow",
    "pb_stop_cause", "pb_wvf_confirm", "d_spring", "d_upthrust",
    "d_absorb_bull", "d_absorb_bear", "d_div_bull", "d_div_bear",
    "is_pivot_low_3", "is_pivot_high_3", "rsi_le_35", "rsi_ge_70",
    "price_gt_50", "price_gt_200", "price_lt_50", "price_lt_200",
    # 260528 reversal-study flags that ALL_SIGNALS doesn't list but the bars table
    # does have — needed so Playbook top/bottom setups (and the SeqBacktest panel)
    # don't silently DROP them in _safe_flags(). Verified present in the live DB.
    "sig_bias_dn", "d_blast_bear_grn", "wvf_spike", "bf_buy", "bf_sell",
}
_ALLOWED_FLAGS = set(ALL_SIGNALS) | _EXTRA_FLAGS


def _safe_flags(signals):
    out = []
    for s in (signals or []):
        c = str(s).strip().lower()
        if c in _ALLOWED_FLAGS:
            out.append(c)
    return out


def _f(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 3)
    except (TypeError, ValueError):
        return None


def _metrics(r, avg_hold: float) -> dict:
    """Realised metrics for a returns array `r` (percent per trade).

    `avg_hold` = mean bars held across ALL trades (passed in so the first/second
    half splits report the same overall hold, matching the original behaviour).
    """
    if len(r) == 0:
        return {"n": 0}
    r = np.asarray(r, dtype=float)
    wins = r[r > 0]; losses = r[r <= 0]
    eq = np.cumsum(r)                              # simple additive equity (%)
    peak = np.maximum.accumulate(eq)
    dd = float((eq - peak).min()) if len(eq) else 0.0
    pf = (wins.sum() / abs(losses.sum())) if losses.sum() != 0 else None
    return {
        "n": int(len(r)),
        "win_pct": round(float((r > 0).mean() * 100), 1),
        "avg_ret": round(float(r.mean()), 3),
        "median_ret": round(float(np.median(r)), 3),
        "expectancy": round(float(r.mean()), 3),
        "profit_factor": (round(pf, 2) if pf is not None else None),
        "avg_win": round(float(wins.mean()), 3) if len(wins) else None,
        "avg_loss": round(float(losses.mean()), 3) if len(losses) else None,
        "max_drawdown": round(dd, 2),
        "total_ret": round(float(r.sum()), 1),
        "avg_hold": round(float(avg_hold), 1),
    }


def _simulate(g: pd.DataFrame, trig_idx, target, stop, max_hold, side):
    """Walk one ticker's bars; return list of (entry_date, ret_pct, bars_held, reason)."""
    o = g["open"].to_numpy(); h = g["high"].to_numpy()
    l = g["low"].to_numpy();  c = g["close"].to_numpy()
    d = g["date"].to_numpy()
    n = len(g)
    trades = []
    busy_until = -1
    for i in trig_idx:
        if i <= busy_until or i + 1 >= n:
            continue
        entry = o[i + 1]
        if not entry or entry <= 0:
            continue
        if side == "long":
            tgt = entry * (1 + target / 100.0)
            stp = entry * (1 - stop / 100.0)
        else:
            tgt = entry * (1 - target / 100.0)
            stp = entry * (1 + stop / 100.0)
        ret = None; reason = "time"; held = 0
        last = min(i + max_hold, n - 1)
        for j in range(i + 1, last + 1):
            held = j - i
            if side == "long":
                hit_stop = l[j] <= stp
                hit_tgt  = h[j] >= tgt
            else:
                hit_stop = h[j] >= stp
                hit_tgt  = l[j] <= tgt
            if hit_stop:                       # stop priority (conservative)
                ret = -stop; reason = "stop"; break
            if hit_tgt:
                ret = target; reason = "target"; break
        if ret is None:                        # time exit at close of last bar
            exitp = c[last]
            ret = ((exitp / entry) - 1.0) * 100.0 if side == "long" else (1.0 - (exitp / entry)) * 100.0
            held = last - i
        trades.append((d[i + 1], float(ret), int(held), reason))
        busy_until = i + held                  # no pyramiding until exit
    return trades


def _run_on_df(
    df: pd.DataFrame,
    flags: list[str],
    phase: Optional[str],
    target: float,
    stop: float,
    max_hold: int,
    side: str,
    max_trades: int = 20000,
) -> dict:
    """Core simulation+metrics on an already-loaded bars DataFrame.

    `df` must carry ticker, date, open, high, low, close, the chosen flag columns
    and (if `phase`) wyc_phase, with a default RangeIndex (as fetchdf() returns) so
    positional trigger masking lines up. Shared by `backtest()` (which loads its
    own df) and `studio.playbook` (which loads ONCE and reuses this for every
    setup). Returns metrics dict, or {"error": ...} — caller attaches "params".

    If the condition would produce more than `max_trades` trades, tickers are
    sampled UNIFORMLY at random (fixed seed) up to that budget instead of keeping
    the alphabetical prefix the (ticker, date) ordering would otherwise leave. The
    result then carries `truncated=True` plus `n_trades`/`n_tickers`/`n_tickers_total`
    so callers/UI can label the metrics "PF on a sample of N".
    """
    flags = [c for c in flags if c in df.columns]
    if not flags:
        return {"error": "no valid signal flags present in data"}
    if phase and "wyc_phase" not in df.columns:
        phase = None

    trig = np.ones(len(df), dtype=bool)
    for c in flags:
        trig &= (pd.to_numeric(df[c], errors="coerce").fillna(0).to_numpy() == 1)
    if phase:
        trig &= (df["wyc_phase"].to_numpy() == phase)

    # Iterate tickers in a deterministic-random order. df is ORDER BY ticker, so
    # when the max_trades cap bites on a large universe (nasdaq ~3M, russell2k
    # ~4.4M rows) a plain break would keep only the alphabetically-earliest
    # tickers (A…) — an alphabetical-prefix sample that skews win%/PF/expectancy.
    # Shuffling the ticker order with a fixed seed keeps the retained sample
    # representative across the whole alphabet while staying reproducible (same
    # query → same numbers). `.indices` gives per-ticker row positions WITHOUT
    # copying every group's frame, so when we stop early we only materialise the
    # tickers we actually simulate.
    group_pos = df.groupby("ticker", sort=False).indices   # {ticker: ndarray[int]}
    order = list(group_pos)
    _Random(_SAMPLE_SEED).shuffle(order)

    all_trades = []
    n_used = 0                 # tickers that actually contributed ≥1 trade
    truncated = False
    for t in order:
        pos = group_pos[t]
        idx = np.nonzero(trig[pos])[0]
        if len(idx) == 0:
            continue
        g = df.iloc[pos].reset_index(drop=True)
        tk_trades = _simulate(g, idx, target, stop, max_hold, side)
        if tk_trades:
            n_used += 1
            all_trades.extend(tk_trades)
        if len(all_trades) > max_trades:
            truncated = True       # sampled, not exhaustive — surfaced in the result
            break

    if not all_trades:
        return {"error": "no trades triggered for that condition"}

    tdf = pd.DataFrame(all_trades, columns=["date", "ret", "held", "reason"]).sort_values("date")
    rets = tdf["ret"].to_numpy()
    avg_hold = float(tdf["held"].mean())

    # time split (out-of-sample sniff)
    mid = tdf["date"].iloc[len(tdf) // 2]
    first = tdf[tdf["date"] < mid]["ret"].to_numpy()
    second = tdf[tdf["date"] >= mid]["ret"].to_numpy()
    reasons = tdf["reason"].value_counts().to_dict()

    if truncated:
        log.info(
            "seq_backtest: hit max_trades=%d cap — reporting a representative sample "
            "of %d trades from %d of %d tickers (seed=%d, alphabet-unbiased)",
            max_trades, len(all_trades), n_used, len(order), _SAMPLE_SEED,
        )

    return {
        "overall": _metrics(rets, avg_hold),
        "first_half": _metrics(first, avg_hold),
        "second_half": _metrics(second, avg_hold),
        "exit_reasons": {k: int(v) for k, v in reasons.items()},
        "date_range": [str(tdf["date"].min())[:10], str(tdf["date"].max())[:10]],
        # Sampling transparency for the UI ("PF on a sample of N"): truncated=True
        # means the trade cap was hit and metrics are on a representative SAMPLE.
        "truncated": truncated,
        "n_trades": int(len(all_trades)),
        "n_tickers": int(n_used),
        "n_tickers_total": int(len(order)),
    }


def backtest(
    signals:   list[str],
    universe:  Optional[str] = None,
    wyc_phase: Optional[str] = None,
    target_pct: float = 10.0,
    stop_pct:   float = 5.0,
    max_hold:   int   = 20,
    side:       str   = "long",
    max_trades: int   = 20000,
) -> dict:
    """Realised backtest of an entry condition. Returns metrics + time split."""
    flags = _safe_flags(signals)
    if not flags:
        return {"error": "no valid signal flags — pick at least one known signal column"}
    uni    = _safe_universe(universe)
    phase  = wyc_phase if wyc_phase in _PHASES else None
    target = max(0.5, min(200.0, float(target_pct)))
    stop   = max(0.5, min(100.0, float(stop_pct)))
    max_hold = max(1, min(120, int(max_hold)))
    side   = "short" if side == "short" else "long"

    where = ["close IS NOT NULL"]
    if uni:
        where.append(f"universe = '{uni}'")
    cols = ["ticker", "date", "open", "high", "low", "close"] + flags + (["wyc_phase"] if phase else [])
    sel = ", ".join(dict.fromkeys(cols))      # dedupe, preserve order
    trig_cond = " AND ".join(f"COALESCE({c},0) = 1" for c in flags)
    if phase:
        trig_cond += f" AND wyc_phase = '{phase}'"

    conn = get_conn(read_only=True)
    try:
        df = conn.execute(
            f"SELECT {sel} FROM bars WHERE {' AND '.join(where)} ORDER BY ticker, date"
        ).fetchdf()
    finally:
        conn.close()
    if df.empty:
        return {"error": "no rows for that universe"}

    core = _run_on_df(df, flags, phase, target, stop, max_hold, side, max_trades)
    if "error" in core:
        return core
    core["params"] = {
        "signals": flags, "universe": uni or "all", "wyc_phase": phase or "all",
        "target_pct": target, "stop_pct": stop, "max_hold": max_hold, "side": side,
    }
    return core
