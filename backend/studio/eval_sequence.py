"""
studio/eval_sequence.py — score a Studio T/Z sequence with the backtest-expert skill.

Bridges the sequence analytics (studio.seq_lab) to the skill's 5-dimension
evaluator, auto-supplying the anti-mirage inputs a raw win% scorecard misses:

  • baseline_win          — the unconditional win% (significance gate)
  • num_strategies_tested — how many sequences cleared min_occ (Bonferroni gate)
  • avg_win / avg_loss     — conditional means (expectancy gate)
  • cost_per_trade_pct     — realistic round-trip cost (net-edge gate)

A sequence that looks "significant" only because n is huge is correctly forced
to Abandon once its gross edge falls below cost — the exact selection-bias
mirage the plain scorecard couldn't catch. Example (nasdaq, 4-bar color, fwd_10d):
  TZTZ  n=161,444  win=47.3% vs baseline 46.6%  → p≈0 "significant"
  …but gross edge 0.034%/trade ≤ 0.5% cost → net −0.466%/trade → Abandon (FORCED).

Drawdown note: these are independent forward-return samples, not a sequential
equity curve, so a portfolio "max drawdown" is ill-defined. We feed
abs(5th-percentile trade return) — a robust "typical bad trade" — rather than the
single worst trade (a delisting −99% bar would otherwise dominate). The raw worst
trade is surfaced for transparency; a true equity-curve drawdown is a future
refinement (see SKILL.md "Possible NEXT steps #4").

Usage (CLI):
  .venv/bin/python -m studio.eval_sequence --universe nasdaq --n-bars 4 \
      --horizon fwd_10d --min-occ 200 --rank 0 --cost 0.5
  .venv/bin/python -m studio.eval_sequence --universe nasdaq --seq TZTZ --horizon fwd_10d
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from studio.db import get_conn
from studio.seq_lab import seq_lab, _token_expr, _HORIZONS

# The skill lives outside the repo (~/.claude/skills); import its evaluator.
_SKILL_DIR = Path.home() / ".claude" / "skills" / "backtest-expert" / "scripts"


def _load_evaluator():
    if str(_SKILL_DIR) not in sys.path:
        sys.path.insert(0, str(_SKILL_DIR))
    try:
        from evaluate_backtest import evaluate  # type: ignore
        return evaluate
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            f"backtest-expert skill not found/importable at {_SKILL_DIR}: {e}"
        )


def _single_seq_stats(universe: Optional[str], n_bars: int, mode: str,
                      horizon: str, seq: str, dd_pctile: float) -> dict:
    """Exact stats for ONE sequence string (works for any seq, not just top-N)."""
    n_bars = max(2, min(6, int(n_bars)))
    mode = "signal" if mode == "signal" else "color"
    hcol = _HORIZONS.get(horizon, "fwd_1d")
    tok = _token_expr(mode)
    sep = "|" if mode == "signal" else ""

    base_clauses = [f"{hcol} IS NOT NULL"]
    if universe:
        base_clauses.append(f"universe = '{universe}'")
    base_where = " AND ".join(base_clauses)

    lag_parts = [f"LAG(tk, {k}) OVER w" for k in range(n_bars - 1, 0, -1)] + ["tk"]
    seq_concat = (f" || '{sep}' || ".join(lag_parts)) if sep else " || ".join(lag_parts)

    conn = get_conn(read_only=True)
    try:
        row = conn.execute(f"""
            WITH s AS (
                SELECT ticker, date, {tok} AS tk, {hcol} AS ret
                FROM bars WHERE {base_where}
            ),
            seqd AS (
                SELECT *, {seq_concat} AS seq
                FROM s WINDOW w AS (PARTITION BY ticker ORDER BY date)
            )
            SELECT COUNT(*) n,
                   ROUND(AVG(CASE WHEN ret > 0 THEN 1.0 ELSE 0 END)*100, 2) win,
                   ROUND(AVG(CASE WHEN ret > 0 THEN ret END), 3) avg_win,
                   ROUND(AVG(CASE WHEN ret < 0 THEN ret END), 3) avg_loss,
                   ROUND(quantile_cont(ret, {float(dd_pctile)}), 3) dd_pctile,
                   ROUND(MIN(ret), 2) worst
            FROM seqd WHERE seq = ?
        """, [seq]).fetchone()
    finally:
        conn.close()
    keys = ["n", "win", "avg_win", "avg_loss", "dd_pctile", "worst"]
    return dict(zip(keys, row))


def evaluate_sequence(
    universe: Optional[str] = None,
    n_bars: int = 4,
    mode: str = "color",
    horizon: str = "fwd_10d",
    min_occ: int = 200,
    sort: str = "win",
    seq: Optional[str] = None,
    rank: int = 0,
    cost_per_trade_pct: float = 0.5,
    num_parameters: int = 3,
    years_tested: int = 5,
    slippage_tested: bool = False,
    dd_pctile: float = 0.05,
) -> dict:
    """Run the analytics, then score the chosen sequence with the skill.

    Returns {seq, inputs, baseline_win, n_candidates, result} where `result` is
    the skill's structured verdict (score, verdict, significance, red_flags).
    """
    evaluate = _load_evaluator()

    # 1) analytics: baseline win%, ranked rows, honest candidate count
    res = seq_lab(universe=universe, n_bars=n_bars, mode=mode, horizon=horizon,
                  min_occ=min_occ, sort=sort, limit=max(rank + 1, 25))
    baseline_win = float(res["baseline"]["win"] or 0.0)
    n_candidates = int(res.get("n_candidates", 1) or 1)
    rows = res["rows"]

    # 2) pick the sequence to score
    if seq:
        target = seq.strip().upper()
    else:
        if not rows:
            raise ValueError("no sequences cleared min_occ — loosen filters")
        rank = max(0, min(rank, len(rows) - 1))
        target = rows[rank]["seq"]

    # 3) exact stats (incl. robust drawdown proxy) for that sequence
    st = _single_seq_stats(universe, n_bars, mode, horizon, target, dd_pctile)
    if not st["n"]:
        raise ValueError(f"sequence {target!r} has no occurrences under these filters")

    avg_win = float(st["avg_win"] or 0.0)
    avg_loss = abs(float(st["avg_loss"] or 0.0)) or 0.01   # guard: avg_loss must be > 0
    # robust drawdown: abs(5th-pctile trade), not the single delisting worst
    max_dd = abs(float(st["dd_pctile"] or st["worst"] or 0.0))

    inputs = {
        "total_trades": int(st["n"]),
        "win_rate": float(st["win"]),
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "max_drawdown_pct": max_dd,
        "years_tested": int(years_tested),
        "num_parameters": int(num_parameters),
        "slippage_tested": bool(slippage_tested),
        "baseline_win": baseline_win,
        "num_strategies_tested": n_candidates,
        "cost_per_trade_pct": float(cost_per_trade_pct),
    }
    result = evaluate(**inputs)
    return {
        "seq": target,
        "universe": universe or "all",
        "horizon": _HORIZONS.get(horizon, horizon),
        "raw_worst_trade_pct": float(st["worst"] or 0.0),
        "baseline_win": baseline_win,
        "n_candidates": n_candidates,
        "inputs": inputs,
        "result": result,
    }


def _print(out: dict) -> None:
    r = out["result"]; sig = r.get("significance", {})
    forced = "  (FORCED by significance/cost gate)" if r.get("verdict_overridden_by_significance") else ""
    i = out["inputs"]
    print(f"seq={out['seq']}  universe={out['universe']}  horizon={out['horizon']}")
    print(f"  n={i['total_trades']}  win={i['win_rate']}%  baseline={out['baseline_win']}%  "
          f"candidates_scanned={out['n_candidates']}")
    print(f"  avg_win={i['avg_win_pct']}%  avg_loss={i['avg_loss_pct']}%  "
          f"drawdown(p5)={i['max_drawdown_pct']}%  raw_worst={out['raw_worst_trade_pct']}%")
    print(f"\nVERDICT: {r['total_score']}/100 -> {r['verdict']}{forced}")
    if sig.get("p_value_vs_baseline") is not None:
        print(f"  p vs baseline={sig['p_value_vs_baseline']:.4f}  "
              f"Bonferroni p={sig['bonferroni_p']:.4f} -> "
              f"{'significant' if sig['significant_after_multiple_testing'] else 'NOT significant'}")
    if sig.get("cost_per_trade_pct"):
        print(f"  net edge after {sig['cost_per_trade_pct']}% cost: "
              f"{sig['net_expectancy_after_costs']:.3f}%/trade")
    for f in r.get("red_flags", []):
        print(f"  [{f['severity'].upper()}] {f['message']}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Score a Studio T/Z sequence with the backtest-expert skill.")
    p.add_argument("--universe", default=None, help="sp500|nasdaq|russell2k (default: all)")
    p.add_argument("--n-bars", type=int, default=4)
    p.add_argument("--mode", default="color", choices=["color", "signal"])
    p.add_argument("--horizon", default="fwd_10d")
    p.add_argument("--min-occ", type=int, default=200)
    p.add_argument("--sort", default="win", choices=["win", "n", "win_lo", "avg_lo"])
    p.add_argument("--seq", default=None, help="explicit sequence (e.g. TZTZ); else use --rank")
    p.add_argument("--rank", type=int, default=0, help="0 = top by --sort")
    p.add_argument("--cost", type=float, default=0.5, help="round-trip cost %% per trade")
    p.add_argument("--num-parameters", type=int, default=3)
    p.add_argument("--years-tested", type=int, default=5)
    p.add_argument("--slippage-tested", action="store_true")
    p.add_argument("--dd-pctile", type=float, default=0.05, help="drawdown percentile (0.05 = 5th)")
    return p.parse_args()


def main() -> int:
    a = parse_args()
    out = evaluate_sequence(
        universe=a.universe, n_bars=a.n_bars, mode=a.mode, horizon=a.horizon,
        min_occ=a.min_occ, sort=a.sort, seq=a.seq, rank=a.rank,
        cost_per_trade_pct=a.cost, num_parameters=a.num_parameters,
        years_tested=a.years_tested, slippage_tested=a.slippage_tested,
        dd_pctile=a.dd_pctile,
    )
    _print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
