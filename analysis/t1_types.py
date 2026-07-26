"""
t1_types.py — ANALYSIS ONLY. Enumerate T1 "types" by the 9 Sequence-Builder lines and
report, per type: % share, peak move (MFE), trough (MAE), and fwd close at 10/20 days —
the Z11-style breakdown but for T1's own one-bar fingerprint.

Reality check first: the FULL 9-line signature has ~38k distinct combos (hopelessly
fragmented — top-30 cover ~6%). So there is NO ~30-type set that partitions T1 at full
resolution. Instead we partition on the 3 lines that actually discriminate T1 outcomes
(volume × RSI-band × gap/range — see T1_9LINE_ANALIZI) → ~40 well-populated types that
sum to 100%. The other 6 lines are reported as marginal modifiers.

Columns: n · %share · peakMFE_20d (median) · troughMAE_20d · fwd_10d · fwd_20d · win10.
⚠️ fwd/MFE are SCREENS (overstate vs a true stop/target path-sim).

Run: cd backend && uv run python ../analysis/t1_types.py
"""
import duckdb, numpy as np, pandas as pd

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
CLIP = (-90, 500)
con = duckdb.connect(DB, read_only=True)


def load():
    return con.execute("""
        SELECT * FROM (
            SELECT universe, ticker, date, rsi_14, fwd_10d, fwd_20d,
                   mfe_10d, mfe_20d, mae_20d,
                   coalesce(l_sig,'-') l_sig, coalesce(full_suffix,'-') suffix,
                   coalesce(bar_body_wick,'-') body, coalesce(bar_gap_range,'-') gap,
                   coalesce(bar_line5,'-') l5, coalesce(vol_bucket,'-') vol,
                   CASE WHEN sig_any_p=1 THEN 'P' ELSE '-' END ema,
                   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
            FROM bars WHERE t_sig='T1'
        ) WHERE rn=1
    """).fetchdf()


def rsi_band(r):
    if not np.isfinite(r): return 'r?'
    return 'OS<40' if r < 40 else 'MID40-60' if r < 60 else 'HI60+'


def gap_band(g):
    if g.startswith('G3'): return 'G3'
    return g if g in ('N', 'V', 'C') else 'N'


def med(s):
    s = pd.to_numeric(s, errors='coerce').dropna(); s = s[s.between(*CLIP)]
    return round(float(s.median()), 2) if len(s) else np.nan


def row_stats(g, total):
    f10 = pd.to_numeric(g.fwd_10d, errors='coerce').dropna(); f10 = f10[f10.between(*CLIP)]
    return dict(n=len(g), pct=round(len(g) / total * 100, 2),
                mfe20=med(g.mfe_20d), mfe10=med(g.mfe_10d), mae20=med(g.mae_20d),
                f10=med(g.fwd_10d), f20=med(g.fwd_20d),
                win=round(float((f10 > 0).mean() * 100), 1) if len(f10) else np.nan)


def main():
    df = load()
    for c in ('fwd_10d', 'fwd_20d', 'mfe_10d', 'mfe_20d', 'mae_20d', 'rsi_14'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df[np.isfinite(df.fwd_10d) & df.fwd_10d.between(*CLIP)].copy()
    df['rb'] = df.rsi_14.map(rsi_band)
    df['gb'] = df.gap.map(gap_band)
    N = len(df)

    # literal full-signature cardinality
    full = (df.l_sig + '|' + df.suffix + '|' + df.body + '|' + df.gap + '|' +
            df.l5 + '|' + df.vol + '|' + df.ema + '|' + df.rb)
    distinct = full.nunique()

    out = [f"# T1 — TYPES BY THE 9 LINES (% · peak · 10d · 20d)\n",
           f"**T1 total signals:** {N:,} (2019-26, liquid universe, deduped).",
           f"**Full 9-line signature distinct combos:** {distinct:,} — hopelessly fragmented "
           f"(top-30 ≈ 6%), so no ~30-type set partitions T1 at full resolution. Below: a clean "
           f"partition on the 3 discriminating lines (vol × RSI × gap), summing to 100%, then the "
           f"other 6 lines as marginal modifiers.\n",
           "peak = median MFE within window (best the price reached); trough = median MAE_20d; "
           "f10/f20 = median fwd close. **SCREENS** — path-sim needed for any tradeable cut.\n"]

    # ── PRIMARY TYPOLOGY: vol × rsi × gap (a true partition) ───────────────────────
    df['typ'] = df.vol + ' · ' + df.rb + ' · ' + df.gb
    rows = []
    for t, g in df.groupby('typ'):
        s = row_stats(g, N)
        if s['n'] >= 100:
            rows.append((t, s))
    rows.sort(key=lambda r: -r[1]['n'])
    cov = sum(r[1]['pct'] for r in rows)
    out.append(f"## PRIMARY — {len(rows)} types (vol · RSI · gap), n≥100, cover {cov:.0f}%\n")
    out.append("| # | type (vol·RSI·gap) | n | %share | peakMFE20 | peakMFE10 | troughMAE20 | f10 | f20 | win10 |")
    out.append("|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for i, (t, s) in enumerate(rows, 1):
        out.append(f"| {i} | {t} | {s['n']:,} | {s['pct']:.2f}% | {s['mfe20']:+.2f} | "
                   f"{s['mfe10']:+.2f} | {s['mae20']:+.2f} | {s['f10']:+.2f} | {s['f20']:+.2f} | {s['win']:.0f} |")

    # ── MARGINAL MODIFIERS: the other 6 lines (each value across all T1) ───────────
    marg = [("l_sig", "l_sig"), ("suffix", "suffix"), ("body", "body/wk"),
            ("l5", "bar_line5"), ("ema", "EMA P/D")]
    for col, name in marg:
        sub = []
        for v, g in df.groupby(col):
            s = row_stats(g, N)
            if s['n'] >= 150:
                sub.append((v, s))
        sub.sort(key=lambda r: -r[1]['f20'] if np.isfinite(r[1]['f20']) else 0)
        out.append(f"\n## MARGINAL — {name}  (value · %share · peak · f10 · f20 · win, sorted by f20)\n")
        out.append("| value | n | %share | peakMFE20 | f10 | f20 | win10 |")
        out.append("|---|--:|--:|--:|--:|--:|--:|")
        for v, s in sub:
            out.append(f"| {v} | {s['n']:,} | {s['pct']:.2f}% | {s['mfe20']:+.2f} | "
                       f"{s['f10']:+.2f} | {s['f20']:+.2f} | {s['win']:.0f} |")

    report = "\n".join(out)
    with open("/Users/sachoki/Desktop/sachoki-desktop/T1_TYPES_9LINE.md", "w") as f:
        f.write(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
