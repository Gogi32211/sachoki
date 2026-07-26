"""
signal_scan_10d.py — ბოლო 10 სავაჭრო დღის სრული სიგნალ-ანალიზი
1D + 1H timeframe + confluence (სწორი dedup-ით)

გამოყენება:
    cd backend && uv run python ../analysis/signal_scan_10d.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd
import numpy as np

DB1D = "/Users/sachoki/Downloads/studio_analytics.duckdb"
DB1H = "/Users/sachoki/Downloads/studio_1h.duckdb"

DAYS        = 10
MIN_TICKERS = 5
MIN_BARS    = 10

SKIP = {'ticker','date','open','high','low','close','volume','universe',
        'vwap','trade_count','avg_vol_20d','rel_vol','atr_14','l_sig','wyc_phase',
        'rsi_14','cci_14','cci_20'}


def get_numeric_signal_cols(con):
    rows = con.execute("DESCRIBE bars").fetchall()
    result = []
    for name, dtype, *_ in rows:
        if name in SKIP: continue
        if name.startswith('fwd_'): continue
        if any(t in dtype.upper() for t in ('VARCHAR','DATE','TIME','TIMESTAMP','CHAR')):
            continue
        result.append(name)
    return result


def load_deduped(con, date_min, is_1h=False):
    """Load deduplicated rows for the time window into a float DataFrame."""
    date_col = "date::date" if is_1h else "date"
    df = con.execute(f"""
        WITH deduped AS (
            SELECT * FROM bars
            WHERE {date_col} >= '{date_min}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
        )
        SELECT * FROM deduped
    """).df()
    return df


def signal_stats(df, sig_cols, n_rows, label):
    """Compute firing rate per signal on a pre-loaded float DataFrame."""
    stats = []
    for col in sig_cols:
        if col not in df.columns:
            continue
        try:
            col_arr = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            fired   = col_arr > 0
            n_b     = int(fired.sum())
            if n_b < MIN_BARS:
                continue
            n_t = int(df.loc[fired, 'ticker'].nunique())
            if n_t < MIN_TICKERS:
                continue
            stats.append({
                "signal": col,
                "n_bars": n_b,
                "n_tickers": n_t,
                "fire_rate": n_b / n_rows
            })
        except Exception:
            pass
    return pd.DataFrame(stats).sort_values("n_bars", ascending=False) if stats else pd.DataFrame()


# ── 1D ──────────────────────────────────────────────────────────────────────

def analyze_1d(con, sig_cols, days=DAYS):
    print(f"\n{'='*70}")
    print(f"1D ANALYSIS — last {days} trading days  ({len(sig_cols)} signals)")
    print(f"{'='*70}")

    dates = sorted([r[0] for r in con.execute(
        f"SELECT DISTINCT date FROM bars ORDER BY date DESC LIMIT {days}").fetchall()])
    date_min, date_max = min(dates), max(dates)
    print(f"  Date range: {date_min} → {date_max}  ({len(dates)} days)")

    print("  Loading 1D deduped data…", flush=True)
    df = load_deduped(con, date_min, is_1h=False)
    n_rows = len(df)
    n_tickers = df["ticker"].nunique()
    print(f"  Rows: {n_rows:,}   Unique tickers: {n_tickers:,}\n")

    stats_df = signal_stats(df, sig_cols, n_rows, "1D")
    print(f"  Active signals (≥{MIN_BARS} bars, ≥{MIN_TICKERS} tickers): {len(stats_df)}")

    print(f"\n  Top 40 signals (last {days} days, 1D):")
    print(f"  {'Signal':<32} {'Bars':>8} {'Tickers':>8} {'Rate':>7}")
    print(f"  {'-'*58}")
    for _, r in stats_df.head(40).iterrows():
        print(f"  {r['signal']:<32} {r['n_bars']:>8,} {r['n_tickers']:>8,} {r['fire_rate']:>7.2%}")

    # ── daily heatmap for top 20 ─────────────────────────────────────────────
    top20 = stats_df.head(20)["signal"].tolist()
    if top20:
        df["_date"] = pd.to_datetime(df["date"]).dt.date
        print(f"\n  Daily firing rate heatmap (top 20 × {days} days):")
        header = f"  {'Signal':<32}" + "".join(f" {str(d)[-5:]:>8}" for d in dates)
        print(header)
        print(f"  {'-'*max(len(header),70)}")
        for sig in top20:
            if sig not in df.columns: continue
            col_n = pd.to_numeric(df[sig], errors='coerce').fillna(0.0) > 0
            row_s = f"  {sig:<32}"
            for d in dates:
                mask = df["_date"] == d
                sub  = col_n[mask]
                row_s += f" {sub.mean():>8.1%}" if len(sub) else f" {'—':>8}"
            print(row_s)

    return df, stats_df


# ── Pump score today (1D) ────────────────────────────────────────────────────

def pump_score_today(con):
    TRIGGER = """
        GREATEST(
          CASE WHEN sig_vol_20x > 0 THEN 5 ELSE 0 END,
          CASE WHEN sig_vol_10x > 0 THEN 4 ELSE 0 END,
          CASE WHEN sig_vol_5x  > 0 THEN 3 ELSE 0 END,
          0
        )
      + (CASE WHEN sig_va > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN sig_sc > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN sig_bc > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN d_upthrust > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN d_absorb_bear > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN d_blast_bear_grn > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN sig_svs > 0 THEN 2 ELSE 0 END)
    """
    SETUP = """
        (CASE WHEN MAX(CASE WHEN l_sig = 'L3' THEN 1 ELSE 0 END) OVER w5 > 0 THEN 4 ELSE 0 END)
      + (CASE WHEN MAX(sig_bias_dn) OVER w5 > 0 THEN 3 ELSE 0 END)
      + (CASE WHEN MAX(wyc_in_tr) OVER w5 > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN MAX(sig_dd_dn_green) OVER w5 > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN MAX(wyc_spring) OVER w5 > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN MAX(sig_260308) OVER w5 > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN MAX(sweet_spot_active) OVER w5 > 0 THEN 2 ELSE 0 END)
      + (CASE WHEN MAX(rsi_le_35) OVER w5 > 0 THEN 1 ELSE 0 END)
      + (CASE WHEN MAX(sig_abs) OVER w5 > 0 THEN 1 ELSE 0 END)
      + (CASE WHEN close BETWEEN 0.5 AND 7 THEN 1 ELSE 0 END)
    """
    print(f"\n{'='*70}")
    print("PUMP SCORE v2 — latest bar per ticker (score ≥9, trigger_score > 0)")
    print(f"{'='*70}")
    df = con.execute(f"""
        WITH deduped AS (
            SELECT * FROM bars
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
        ),
        scored AS (
            SELECT ticker, date, close, rsi_14, l_sig, wyc_phase,
                   ({TRIGGER}) AS trigger_score,
                   ({SETUP})   AS setup_score
            FROM deduped
            WHERE close BETWEEN 0.30 AND 20.0
            WINDOW w5 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
        ),
        ranked AS (
            SELECT *, setup_score + trigger_score AS total_score,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM scored
        )
        SELECT * FROM ranked
        WHERE rn = 1 AND trigger_score > 0 AND total_score >= 9
        ORDER BY total_score DESC
        LIMIT 30
    """).df()
    if df.empty:
        print("  No setups found."); return
    print(f"  {'Ticker':<7} {'Date':<12} {'Close':>6} {'RSI':>5} {'Score':>6} {'S↺':>5} {'T!':>4}  Phase")
    print(f"  {'-'*62}")
    for _, r in df.iterrows():
        print(f"  {r['ticker']:<7} {str(r['date']):<12} {r['close']:>6.2f} "
              f"{r['rsi_14']:>5.1f} {int(r['total_score']):>6} {int(r['setup_score']):>5} "
              f"{int(r['trigger_score']):>4}  {r.get('wyc_phase','')}")


# ── 1H ──────────────────────────────────────────────────────────────────────

def analyze_1h(con1h, sig_cols, days=DAYS):
    print(f"\n{'='*70}")
    print(f"1H ANALYSIS — last {days} trading days  ({len(sig_cols)} signals)")
    print(f"{'='*70}")

    all_days = sorted(set(r[0] for r in con1h.execute(
        "SELECT DISTINCT date::date FROM bars ORDER BY date DESC LIMIT 200").fetchall()),
        reverse=True)
    # pick last `days` calendar days that have data
    unique_days = all_days[:days]
    date_min = min(unique_days)
    date_max = max(unique_days)
    print(f"  Date range: {date_min} → {date_max}  ({len(unique_days)} days)")

    print("  Loading 1H deduped data…", flush=True)
    df1h = load_deduped(con1h, date_min, is_1h=True)
    n_rows   = len(df1h)
    n_tickers = df1h["ticker"].nunique()
    print(f"  1H rows: {n_rows:,}   Unique tickers: {n_tickers:,}")
    print(f"  Avg bars per ticker: {n_rows/max(n_tickers,1):.1f}\n")

    stats_df = signal_stats(df1h, sig_cols, n_rows, "1H")
    print(f"  Active signals (1H): {len(stats_df)}")

    print(f"\n  Top 40 signals (1H, last {days} days):")
    print(f"  {'Signal':<32} {'H-Bars':>8} {'Tickers':>8} {'Rate':>8}")
    print(f"  {'-'*60}")
    for _, r in stats_df.head(40).iterrows():
        print(f"  {r['signal']:<32} {r['n_bars']:>8,} {r['n_tickers']:>8,} {r['fire_rate']:>8.3%}")

    # ── hourly density (avg hours/day the signal fires per ticker) ───────────
    df1h["_day"]  = pd.to_datetime(df1h["date"]).dt.date
    df1h["_hour"] = pd.to_datetime(df1h["date"]).dt.hour

    top10_1h = stats_df.head(10)["signal"].tolist()
    if top10_1h:
        print(f"\n  Avg hours/day signal fires per ticker (top 10):")
        for sig in top10_1h:
            if sig not in df1h.columns: continue
            col_n = pd.to_numeric(df1h[sig], errors='coerce').fillna(0.0) > 0
            # total fires / (n_tickers × n_days)
            density = col_n.sum() / max(n_tickers * len(unique_days), 1)
            # peak hour
            hourly = df1h.groupby("_hour").apply(lambda g: (pd.to_numeric(g[sig], errors='coerce').fillna(0.0) > 0).mean())
            peak_hr = int(hourly.idxmax()) if len(hourly) else 0
            print(f"    {sig:<32} {density:.3f} hrs/day  peak_hour={peak_hr:02d}:00")

    return df1h, stats_df


# ── 1D + 1H Confluence ───────────────────────────────────────────────────────

def analyze_confluence(df1d, df1h, stats1d, stats1h, days=DAYS):
    print(f"\n{'='*70}")
    print("1D + 1H CONFLUENCE — same signal, same ticker, same day (≥2 hourly bars)")
    print(f"{'='*70}")

    common_sigs = set(stats1d["signal"].tolist()) & set(stats1h["signal"].tolist())
    print(f"  Signals active on both TFs: {len(common_sigs)}")

    df1d["_date"] = pd.to_datetime(df1d["date"]).dt.date
    df1h["_day"]  = pd.to_datetime(df1h["date"]).dt.date

    latest_day = df1d["_date"].max()
    print(f"  Analysing confluence for last day: {latest_day}")

    today_1d = df1d[df1d["_date"] == latest_day]
    today_1h = df1h[df1h["_day"] == latest_day]

    if today_1h.empty:
        print("  No 1H data for this date."); return pd.DataFrame()

    rows = []
    for sig in common_sigs:
        if sig not in today_1d.columns or sig not in today_1h.columns: continue

        col_1d = pd.to_numeric(today_1d[sig], errors='coerce').fillna(0.0) > 0
        ticks_1d = set(today_1d.loc[col_1d, "ticker"])
        if not ticks_1d: continue

        col_1h = pd.to_numeric(today_1h[sig], errors='coerce').fillna(0.0) > 0
        h_counts = today_1h[col_1h].groupby(today_1h["ticker"]).size()
        ticks_1h = set(h_counts[h_counts >= 2].index)

        overlap = ticks_1d & ticks_1h
        for t in overlap:
            rows.append({"signal": sig, "ticker": t,
                         "day": str(latest_day), "h_bars": int(h_counts.get(t, 0))})

    if not rows:
        # try last few days instead of just latest
        print(f"  No confluence for {latest_day}, trying last {days} days…")
        days_1d = sorted(df1d["_date"].unique(), reverse=True)[:days]
        for day in days_1d:
            td1d = df1d[df1d["_date"] == day]
            td1h = df1h[df1h["_day"] == day]
            if td1h.empty: continue
            for sig in common_sigs:
                if sig not in td1d.columns or sig not in td1h.columns: continue
                col_1d = pd.to_numeric(td1d[sig], errors='coerce').fillna(0.0) > 0
                ticks_1d = set(td1d.loc[col_1d, "ticker"])
                if not ticks_1d: continue
                col_1h = pd.to_numeric(td1h[sig], errors='coerce').fillna(0.0) > 0
                h_counts = td1h[col_1h].groupby(td1h["ticker"]).size()
                ticks_1h = set(h_counts[h_counts >= 2].index)
                for t in ticks_1d & ticks_1h:
                    rows.append({"signal": sig, "ticker": t,
                                 "day": str(day), "h_bars": int(h_counts.get(t, 0))})

    if not rows:
        print("  No 1D+1H confluence found."); return pd.DataFrame()

    conf_df = pd.DataFrame(rows)

    # by ticker: count unique signals
    by_ticker = (conf_df.groupby(["ticker","day"])
                 .agg(n_signals=("signal","nunique"),
                      signals=("signal", lambda x: ", ".join(sorted(set(x))[:8])),
                      max_h_bars=("h_bars","max"))
                 .reset_index()
                 .sort_values(["n_signals","max_h_bars"], ascending=False))

    print(f"\n  Total confluence events: {len(conf_df):,}")
    print(f"  Unique tickers: {conf_df['ticker'].nunique()}")
    print(f"\n  Top tickers by # of confluent signals:")
    print(f"  {'Ticker':<8} {'Day':<12} {'Sigs':>5} {'MaxH':>5}  Signal list")
    print(f"  {'-'*80}")
    for _, r in by_ticker.head(25).iterrows():
        print(f"  {r['ticker']:<8} {r['day']:<12} {r['n_signals']:>5} {r['max_h_bars']:>5}  {r['signals'][:55]}")

    # top confluence signals
    sig_rank = (conf_df.groupby("signal")
                .agg(n_tickers=("ticker","nunique"), n_days=("day","nunique"), avg_h=("h_bars","mean"))
                .sort_values("n_tickers", ascending=False).head(20))
    print(f"\n  Most common confluence signals (last {days} days):")
    print(f"  {'Signal':<32} {'Tickers':>8} {'Days':>5} {'AvgH':>6}")
    print(f"  {'-'*55}")
    for sig, r in sig_rank.iterrows():
        print(f"  {sig:<32} {r['n_tickers']:>8} {r['n_days']:>5} {r['avg_h']:>6.1f}")

    return conf_df


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Opening DBs…")
    con1d = duckdb.connect(DB1D, read_only=True)
    con1h = duckdb.connect(DB1H, read_only=True)

    print("Getting signal columns…")
    sc1d = get_numeric_signal_cols(con1d)
    sc1h = get_numeric_signal_cols(con1h)
    sc_common = [c for c in sc1d if c in set(sc1h)]
    print(f"  1D: {len(sc1d)}  1H: {len(sc1h)}  Common: {len(sc_common)}")

    # 1D
    df1d, stats1d = analyze_1d(con1d, sc_common, days=DAYS)
    pump_score_today(con1d)

    # 1H
    df1h, stats1h = analyze_1h(con1h, sc_common, days=DAYS)

    # Confluence
    analyze_confluence(df1d, df1h, stats1d, stats1h, days=DAYS)

    con1d.close(); con1h.close()
    print(f"\n{'='*70}\nDONE.")


if __name__ == "__main__":
    main()
