"""
t1_variations.py — ANALYSIS ONLY. Enumerate every distinct T1 bar by the 9 Builder lines.
One row per realized signature: how many times it repeats (n) + price up/down afterwards.

Lines (T1 fixed):
  L=l_sig · suffix=full_suffix · body/wk=bar_body_wick · gap/rng=bar_gap_range ·
  l5=bar_line5 · volume=vol_bucket · EMA=P/- (sig_any_p) · RSI=10-wide band

price up   = median MFE_20d (best the price reached within 20 bars, %)
price down = median MAE_20d (worst drawdown within 20 bars, %)
+ fwd_10d / fwd_20d median close, win10.

Outputs:
  analysis/t1_variations.csv     — ALL realized signatures, sorted by n
  T1_VARIATIONS.md               — theoretical max + realized count + top 60 table

Run: cd backend && uv run python ../analysis/t1_variations.py
"""
import duckdb, numpy as np, pandas as pd, sys

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
CLIP = (-90, 500)
SIG = sys.argv[1] if len(sys.argv) > 1 else "T1"
con = duckdb.connect(DB, read_only=True)


def rsi_band(r):
    if not np.isfinite(r): return 'NA'
    if r < 20: return '<20'
    if r >= 80: return '80+'
    lo = int(r // 10 * 10)
    return f'{lo}-{lo+10}'


def main():
    df = con.execute("""
        SELECT * FROM (
            SELECT universe, ticker, date, rsi_14, fwd_10d, fwd_20d, mfe_20d, mae_20d,
                   coalesce(l_sig,'-') L, coalesce(full_suffix,'-') suffix,
                   coalesce(bar_body_wick,'-') body_wk, coalesce(bar_gap_range,'-') gap_rng,
                   coalesce(bar_line5,'-') l5, coalesce(vol_bucket,'-') volume,
                   CASE WHEN sig_any_p=1 THEN 'P' ELSE '-' END ema,
                   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
            FROM bars WHERE t_sig=?
        ) WHERE rn=1
    """, [SIG]).fetchdf()
    for c in ('fwd_10d', 'fwd_20d', 'mfe_20d', 'mae_20d', 'rsi_14'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df[np.isfinite(df.fwd_10d) & df.fwd_10d.between(*CLIP)].copy()
    df['rsi_band'] = df.rsi_14.map(rsi_band)
    N = len(df)

    LINES = ['L', 'suffix', 'body_wk', 'gap_rng', 'l5', 'volume', 'ema', 'rsi_band']

    # theoretical max = product of realized value-space of each line
    card = {c: df[c].nunique() for c in LINES}
    theo = 1
    for c in LINES:
        theo *= card[c]

    g = df.groupby(LINES)
    agg = g.agg(
        n=('fwd_10d', 'size'),
        up_mfe20=('mfe_20d', 'median'),
        down_mae20=('mae_20d', 'median'),
        fwd_10d=('fwd_10d', 'median'),
        fwd_20d=('fwd_20d', 'median'),
        win10=('fwd_10d', lambda s: round((s > 0).mean() * 100, 1)),
    ).reset_index()
    agg['pct'] = (agg['n'] / N * 100).round(3)
    for c in ('up_mfe20', 'down_mae20', 'fwd_10d', 'fwd_20d'):
        agg[c] = agg[c].round(2)
    agg = agg.sort_values('n', ascending=False).reset_index(drop=True)
    agg.insert(0, 'rank', agg.index + 1)
    realized = len(agg)

    cols = ['rank'] + LINES + ['n', 'pct', 'up_mfe20', 'down_mae20', 'fwd_10d', 'fwd_20d', 'win10']
    agg = agg[cols]
    agg.to_csv(f"/Users/sachoki/Desktop/sachoki-desktop/analysis/{SIG.lower()}_variations.csv", index=False)

    # MD summary
    out = [f"# {SIG} — ALL BAR VARIATIONS BY THE 9 LINES\n",
           f"**Total {SIG} signals:** {N:,} (2019-26, liquid, deduped).",
           f"**Theoretically possible distinct bars** (product of each line's realized value-space):",
           "",
           "| line | distinct values |", "|---|--:|"]
    for c in LINES:
        out.append(f"| {c} | {card[c]} |")
    out += [f"| **PRODUCT (theoretical max)** | **{theo:,}** |", "",
            f"**Actually realized distinct variations:** **{realized:,}** "
            f"(of {theo:,} possible — most combos never occur).",
            f"Full list → `analysis/{SIG.lower()}_variations.csv`. up=median MFE_20d (peak up), "
            f"down=median MAE_20d (peak down). Top 60 by frequency:\n",
            "| # | L | suffix | body | gap | l5 | vol | EMA | RSI | n | % | up | down | f10 | f20 | win |",
            "|--:|---|---|---|---|---|---|:-:|---|--:|--:|--:|--:|--:|--:|--:|"]
    for _, r in agg.head(60).iterrows():
        out.append(f"| {r['rank']} | {r.L} | {r.suffix} | {r.body_wk} | {r.gap_rng} | {r.l5} | "
                   f"{r.volume} | {r.ema} | {r.rsi_band} | {r.n:,} | {r.pct:.2f} | "
                   f"{r.up_mfe20:+.1f} | {r.down_mae20:+.1f} | {r.fwd_10d:+.2f} | {r.fwd_20d:+.2f} | {r.win10:.0f} |")
    report = "\n".join(out)
    with open(f"/Users/sachoki/Desktop/sachoki-desktop/{SIG}_VARIATIONS.md", "w") as f:
        f.write(report + "\n")
    print(f"theoretical max={theo:,}  realized={realized:,}  signals={N:,}")
    print(report[:2500])


if __name__ == "__main__":
    main()
