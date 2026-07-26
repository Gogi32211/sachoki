"""
pump_backtest.py — Path-simulation backtest for the v2 pump scoring system.

Entry  : close of signal bar
Stop   : first bar where low  <= entry * (1 - stop_pct)   → loss = -stop_pct
Target : first bar where high >= entry * (1 + target_pct) → win  = +target_pct
Timeout: exit at close of bar MAX_BARS if neither hit

Rules:
  - trigger_score > 0 (mandatory)
  - total_score  >= MIN_SCORE
  - close in [0.30, 20.0]  (universe filter; wider than screener)
  - one signal per ticker per date (no duplicates)

Usage:
    cd backend && uv run python ../analysis/pump_backtest.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd
import numpy as np
from itertools import product

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
MIN_SCORE  = 9
MAX_BARS   = 15
STOPS      = [0.10, 0.15, 0.20]   # -10%, -15%, -20%
TARGETS    = [0.15, 0.25, 0.50]   # +15%, +25%, +50%

# ── Scoring SQL (copy from studio_api.py v2) ─────────────────────────────────

TRIGGER_SQL = """
    GREATEST(
      CASE WHEN sig_vol_20x > 0 THEN 5 ELSE 0 END,
      CASE WHEN sig_vol_10x > 0 THEN 4 ELSE 0 END,
      CASE WHEN sig_vol_5x  > 0 THEN 3 ELSE 0 END,
      0
    )
  + (CASE WHEN sig_va           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN sig_sc           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN sig_bc           > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN d_upthrust       > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN d_absorb_bear    > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN d_blast_bear_grn > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN sig_svs          > 0 THEN 2 ELSE 0 END)
"""

SETUP_SQL = """
    (CASE WHEN MAX(CASE WHEN l_sig = 'L3' THEN 1 ELSE 0 END) OVER w5 > 0 THEN 4 ELSE 0 END)
  + (CASE WHEN MAX(sig_bias_dn)       OVER w5 > 0 THEN 3 ELSE 0 END)
  + (CASE WHEN MAX(wyc_in_tr)         OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sig_dd_dn_green)   OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(wyc_spring)        OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sig_260308)        OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(sweet_spot_active) OVER w5 > 0 THEN 2 ELSE 0 END)
  + (CASE WHEN MAX(rsi_le_35)         OVER w5 > 0 THEN 1 ELSE 0 END)
  + (CASE WHEN MAX(sig_abs)           OVER w5 > 0 THEN 1 ELSE 0 END)
  + (CASE WHEN close BETWEEN 0.5 AND 7 THEN 1 ELSE 0 END)
"""

# ── Build LEAD columns for forward bars ─────────────────────────────────────

def lead_cols(n):
    parts = []
    for i in range(1, n + 1):
        parts.append(f"LEAD(low,  {i}) OVER w_tk AS fl{i}")
        parts.append(f"LEAD(high, {i}) OVER w_tk AS fh{i}")
        parts.append(f"LEAD(close,{i}) OVER w_tk AS fc{i}")
    return ",\n       ".join(parts)


def load_signals(con) -> pd.DataFrame:
    print("Loading signal bars…", flush=True)
    sql = f"""
    -- dedup: same ticker+date can appear in multiple universes; keep one row
    WITH deduped AS (
        SELECT * FROM bars
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
    ),
    raw AS (
        SELECT ticker, date, close,
               ({TRIGGER_SQL})  AS trigger_score,
               ({SETUP_SQL})    AS setup_score,
               {lead_cols(MAX_BARS)}
        FROM deduped
        -- NO close filter here: LEAD() must see all forward bars incl. pump bars >$20
        WINDOW w5   AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
               w_tk AS (PARTITION BY ticker ORDER BY date)
    )
    SELECT *,
           setup_score + trigger_score AS total_score
    FROM raw
    WHERE close BETWEEN 0.30 AND 20.0      -- signal bar filter applied AFTER leads
      AND trigger_score > 0
      AND setup_score + trigger_score >= {MIN_SCORE}
    ORDER BY ticker, date
    """
    df = con.execute(sql).df()
    print(f"  Signal bars: {len(df):,}  unique tickers: {df['ticker'].nunique():,}", flush=True)
    return df


# ── Path simulation ──────────────────────────────────────────────────────────

def simulate(df: pd.DataFrame, stop_pct: float, target_pct: float) -> pd.DataFrame:
    entry    = df["close"].values
    stop_px  = entry * (1 - stop_pct)
    tgt_px   = entry * (1 + target_pct)
    n        = len(df)

    outcome   = np.full(n, "timeout", dtype=object)
    bars_held = np.full(n, MAX_BARS,  dtype=int)
    exit_pnl  = np.zeros(n)

    for i in range(1, MAX_BARS + 1):
        lo_col = f"fl{i}"
        hi_col = f"fh{i}"
        cl_col = f"fc{i}"

        lo = df[lo_col].values
        hi = df[hi_col].values
        cl = df[cl_col].values

        pending = outcome == "timeout"
        if not pending.any():
            break

        # stop-first within the same bar (conservative)
        hit_stop = pending & (lo <= stop_px)
        hit_tgt  = pending & (hi >= tgt_px)
        # both hit same bar → stop wins (conservative path-sim)
        both     = hit_stop & hit_tgt

        stop_only = hit_stop & ~hit_tgt
        tgt_only  = hit_tgt  & ~hit_stop

        outcome[stop_only]  = "loss"
        bars_held[stop_only] = i
        exit_pnl[stop_only]  = -stop_pct

        outcome[tgt_only]   = "win"
        bars_held[tgt_only]  = i
        exit_pnl[tgt_only]   = target_pct

        outcome[both]       = "loss"
        bars_held[both]      = i
        exit_pnl[both]       = -stop_pct

    # timeouts: use close of last bar
    to = outcome == "timeout"
    last_cl = df[f"fc{MAX_BARS}"].values
    valid   = to & ~np.isnan(last_cl)
    exit_pnl[valid] = (last_cl[valid] - entry[valid]) / entry[valid]

    res = df[["ticker", "date", "close", "total_score", "setup_score", "trigger_score"]].copy()
    res["outcome"]   = outcome
    res["bars_held"] = bars_held
    res["pnl"]       = exit_pnl
    res["stop_pct"]  = stop_pct
    res["tgt_pct"]   = target_pct
    return res


# ── Summary stats ────────────────────────────────────────────────────────────

def summarize(res: pd.DataFrame) -> dict:
    total  = len(res)
    wins   = (res["outcome"] == "win").sum()
    losses = (res["outcome"] == "loss").sum()
    tos    = (res["outcome"] == "timeout").sum()
    wr     = wins / total if total else 0
    avg_pnl = res["pnl"].mean()
    med_pnl = res["pnl"].median()
    # expectancy = win_rate * avg_win + loss_rate * avg_loss
    avg_win  = res.loc[res["outcome"] == "win",  "pnl"].mean() if wins   else 0
    avg_loss = res.loc[res["outcome"] == "loss", "pnl"].mean() if losses else 0
    avg_to   = res.loc[res["outcome"] == "timeout", "pnl"].mean() if tos else 0
    exp = wr * avg_win + (losses / total) * avg_loss + (tos / total) * avg_to if total else 0
    return dict(
        n=total, wins=wins, losses=losses, timeouts=tos,
        win_rate=wr, avg_pnl=avg_pnl, med_pnl=med_pnl,
        avg_win=avg_win, avg_loss=avg_loss, avg_to=avg_to,
        expectancy=exp,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    con = duckdb.connect(DB, read_only=True)
    df  = load_signals(con)
    con.close()

    print(f"\n{'='*70}")
    print(f"PATH-SIMULATION BACKTEST  |  min_score={MIN_SCORE}  max_hold={MAX_BARS}d")
    print(f"{'='*70}")
    print(f"{'Stop':>6} {'Target':>7} {'N':>7} {'WinRate':>8} {'Expect':>8} {'AvgW':>7} {'AvgL':>7} {'AvgTO':>7}")
    print(f"{'-'*70}")

    grid_results = []
    best = None

    for stop, tgt in product(STOPS, TARGETS):
        res   = simulate(df, stop, tgt)
        stats = summarize(res)
        stats.update(stop_pct=stop, tgt_pct=tgt)
        grid_results.append(stats)

        wr_s  = f"{stats['win_rate']:.1%}"
        exp_s = f"{stats['expectancy']:+.2%}"
        aw_s  = f"{stats['avg_win']:+.1%}"
        al_s  = f"{stats['avg_loss']:+.1%}"
        at_s  = f"{stats['avg_to']:+.1%}"
        print(f"{stop:>5.0%} {tgt:>7.0%} {stats['n']:>7,} {wr_s:>8} {exp_s:>8} {aw_s:>7} {al_s:>7} {at_s:>7}")

        if best is None or stats["expectancy"] > best["expectancy"]:
            best = stats

    # ── Score-bucket breakdown for best params ───────────────────────────────
    print(f"\n{'='*70}")
    print(f"SCORE BREAKDOWN  stop={best['stop_pct']:.0%}  target={best['tgt_pct']:.0%}")
    print(f"{'='*70}")
    res_best = simulate(df, best["stop_pct"], best["tgt_pct"])
    buckets  = []
    for sc in sorted(res_best["total_score"].unique()):
        sub   = res_best[res_best["total_score"] == sc]
        stats = summarize(sub)
        buckets.append({"score": sc, **stats})

    bdf = pd.DataFrame(buckets)
    print(f"{'Score':>6} {'N':>7} {'WinRate':>8} {'Expect':>8} {'AvgW':>7} {'AvgL':>7}")
    print(f"{'-'*55}")
    for _, r in bdf.iterrows():
        print(f"{int(r['score']):>6} {int(r['n']):>7,} {r['win_rate']:>8.1%} {r['expectancy']:>+8.2%} {r['avg_win']:>+7.1%} {r['avg_loss']:>+7.1%}")

    # ── Year breakdown for best params ───────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"YEAR BREAKDOWN  stop={best['stop_pct']:.0%}  target={best['tgt_pct']:.0%}")
    print(f"{'='*70}")
    res_best["year"] = pd.to_datetime(res_best["date"]).dt.year
    print(f"{'Year':>6} {'N':>7} {'WinRate':>8} {'Expect':>8}")
    print(f"{'-'*40}")
    for yr in sorted(res_best["year"].unique()):
        sub   = res_best[res_best["year"] == yr]
        stats = summarize(sub)
        print(f"{yr:>6} {stats['n']:>7,} {stats['win_rate']:>8.1%} {stats['expectancy']:>+8.2%}")

    print(f"\nBest combo: stop={best['stop_pct']:.0%}  target={best['tgt_pct']:.0%}"
          f"  expectancy={best['expectancy']:+.2%}  win_rate={best['win_rate']:.1%}  n={best['n']:,}")


if __name__ == "__main__":
    main()
