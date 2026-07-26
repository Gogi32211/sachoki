"""
pump_dip_entry.py — Dip-entry backtest for pump scoring system.

Strategy:
  Signal fires on bar X → close = C (reference price)
  Wait for price to dip to C * (1 - dip_pct)  → entry price E
  From E: target = E * (1 + target_pct)
          stop   = E * (1 - stop_below_dip)
  If price never reaches dip → NO FILL (skip trade)

Tests combinations:
  dip_pct        : 0.15, 0.20
  target_pct     : 0.50
  stop_below_dip : 0.10, 0.15  (below DIP price, not original)

Usage:
    cd backend && uv run python ../analysis/pump_dip_entry.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd
import numpy as np
from itertools import product

DB        = "/Users/sachoki/Downloads/studio_analytics.duckdb"
MIN_SCORE = 9
MAX_BARS  = 20    # scan window for both dip + pump after signal

DIPS       = [0.15, 0.20]
TARGET_PCT = 0.50
STOPS_BELOW = [0.10, 0.15]

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


def lead_cols(n):
    parts = []
    for i in range(1, n + 1):
        parts.append(f"LEAD(low,  {i}) OVER w_tk AS fl{i}")
        parts.append(f"LEAD(high, {i}) OVER w_tk AS fh{i}")
    return ",\n       ".join(parts)


def load_signals(con):
    print("Loading signal bars…", flush=True)
    sql = f"""
    -- dedup: ticker+date appears in multiple universes; keep one row
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
        -- NO close filter: LEAD() must see all forward bars including pump bars >$20
        WINDOW w5   AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
               w_tk AS (PARTITION BY ticker ORDER BY date)
    )
    SELECT *, setup_score + trigger_score AS total_score
    FROM raw
    WHERE close BETWEEN 0.30 AND 20.0      -- signal bar filter AFTER leads
      AND trigger_score > 0
      AND setup_score + trigger_score >= {MIN_SCORE}
    ORDER BY ticker, date
    """
    df = con.execute(sql).df()
    print(f"  Signal bars: {len(df):,}  tickers: {df['ticker'].nunique():,}", flush=True)
    return df


def simulate_dip(df, dip_pct, stop_below, target_pct):
    """
    Row-by-row path simulation (explicit, correct):
      Phase 1: find first bar where low <= close*(1-dip_pct)  → fill
      Phase 2: from fill bar+1 onward, check stop/target from dip price
    """
    C      = df["close"].values
    n      = len(df)
    lows   = np.stack([df[f"fl{i}"].values for i in range(1, MAX_BARS+1)], axis=1)  # (n, 20)
    highs  = np.stack([df[f"fh{i}"].values for i in range(1, MAX_BARS+1)], axis=1)

    dip_px  = C * (1 - dip_pct)
    stop_px = dip_px * (1 - stop_below)
    tgt_px  = dip_px * (1 + target_pct)

    outcomes  = np.full(n, "no_fill", dtype=object)
    bars_held = np.zeros(n, dtype=int)
    pnl_arr   = np.zeros(n)
    fill_arr  = np.zeros(n, dtype=bool)

    for j in range(n):
        dp  = dip_px[j]
        sp  = stop_px[j]
        tp  = tgt_px[j]
        lo_row = lows[j]
        hi_row = highs[j]

        # phase 1: find fill bar
        fill_bar = -1
        for i in range(MAX_BARS):
            if np.isnan(lo_row[i]): continue
            if lo_row[i] <= dp:
                fill_bar = i  # 0-indexed
                break

        if fill_bar == -1:
            continue  # no fill

        fill_arr[j] = True
        outcomes[j] = "timeout"

        # phase 2: scan from fill_bar+1
        for i in range(fill_bar + 1, MAX_BARS):
            lo = lo_row[i]
            hi = hi_row[i]
            if np.isnan(lo): continue
            if lo <= sp:
                outcomes[j]  = "loss"
                bars_held[j] = i - fill_bar
                pnl_arr[j]   = -stop_below
                break
            if hi >= tp:
                outcomes[j]  = "win"
                bars_held[j] = i - fill_bar
                pnl_arr[j]   = target_pct
                break

    fill_rate = fill_arr.mean()
    res = df[["ticker", "date", "close", "total_score"]].copy()
    res["dip_pct"]    = dip_pct
    res["stop_below"] = stop_below
    res["target_pct"] = target_pct
    res["outcome"]    = outcomes
    res["bars_held"]  = bars_held
    res["pnl"]        = pnl_arr
    res["filled"]     = fill_arr
    return res, fill_rate


def summarize(res, only_filled=True):
    sub = res[res["outcome"] != "no_fill"] if only_filled else res
    if not len(sub):
        return {}
    total  = len(sub)
    wins   = (sub["outcome"] == "win").sum()
    losses = (sub["outcome"] == "loss").sum()
    tos    = (sub["outcome"] == "timeout").sum()
    wr     = wins / total
    avg_pnl = sub["pnl"].mean()
    exp = wr * sub.loc[sub["outcome"]=="win","pnl"].mean() \
        + (losses/total) * sub.loc[sub["outcome"]=="loss","pnl"].mean() \
        + (tos/total)    * sub.loc[sub["outcome"]=="timeout","pnl"].mean() if total else 0
    return dict(n=total, wins=wins, losses=losses, timeouts=tos,
                win_rate=wr, avg_pnl=avg_pnl, expectancy=exp)


def main():
    con = duckdb.connect(DB, read_only=True)
    df  = load_signals(con)
    con.close()

    print(f"\n{'='*72}")
    print(f"DIP-ENTRY BACKTEST  |  min_score={MIN_SCORE}  target=+{TARGET_PCT:.0%}  max_scan={MAX_BARS}d")
    print(f"{'='*72}")
    print(f"{'Dip':>6} {'StopBel':>8} {'FillRate':>9} {'Filled':>8} {'WinRate':>8} {'Expect':>8}")
    print(f"{'-'*72}")

    best = None
    all_res = []

    for dip, stop_b in product(DIPS, STOPS_BELOW):
        res, fill_rate = simulate_dip(df, dip, stop_b, TARGET_PCT)
        stats = summarize(res)
        stats["dip_pct"]    = dip
        stats["stop_below"] = stop_b
        stats["fill_rate"]  = fill_rate
        all_res.append((res, stats))

        print(f"{dip:>5.0%} {stop_b:>8.0%} {fill_rate:>9.1%} {stats['n']:>8,} "
              f"{stats['win_rate']:>8.1%} {stats['expectancy']:>+8.2%}")

        if best is None or stats["expectancy"] > best["expectancy"]:
            best = stats
            best_res = res

    # ── Score breakdown for best combo ───────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"SCORE BREAKDOWN  dip={best['dip_pct']:.0%}  stop_below={best['stop_below']:.0%}  target=+{TARGET_PCT:.0%}")
    print(f"{'='*72}")
    sub = best_res[best_res["outcome"] != "no_fill"]
    print(f"{'Score':>6} {'N':>7} {'FillRate':>9} {'Win%':>7} {'Expect':>8}")
    print(f"{'-'*55}")
    # fill rate by score = filled / total signals at that score
    for sc in sorted(df["total_score"].unique()):
        all_at_sc  = (df["total_score"] == sc).sum()
        sub_at_sc  = best_res[best_res["total_score"] == sc]
        filled_cnt = sub_at_sc["filled"].sum()
        fr         = filled_cnt / all_at_sc if all_at_sc else 0
        traded     = sub_at_sc[sub_at_sc["outcome"] != "no_fill"]
        if not len(traded): continue
        s = summarize(traded)
        print(f"{sc:>6} {len(traded):>7,} {fr:>9.1%} {s['win_rate']:>7.1%} {s['expectancy']:>+8.2%}")

    # ── Comparison: close-entry vs dip-entry (best params) ───────────────────
    print(f"\n{'='*72}")
    print("COMPARISON: Close-entry (−20% stop) vs Dip-entry (best combo)")
    print(f"{'='*72}")
    print(f"  Close-entry  stop=−20%  target=+50%:  win_rate= 5.3%  expect=+0.32%  coverage=100%")
    br = best
    print(f"  Dip-entry    dip=−{br['dip_pct']:.0%}   stop=−{br['stop_below']:.0%}  target=+50%:  "
          f"win_rate={br['win_rate']:.1%}  expect={br['expectancy']:+.2%}  coverage={br['fill_rate']:.1%}")

    print(f"\nConclusion: dip entry fills on {br['fill_rate']:.1%} of signals "
          f"(price must first drop {br['dip_pct']:.0%})")


if __name__ == "__main__":
    main()
