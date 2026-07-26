"""
t1_9line.py — ANALYSIS ONLY. Detailed T1 study decomposed across the 9 descriptor
"lines" that make up a bar's label in the TZ_WLNBB system:

  1 l_sig              — WLNBB absorption/vol line (L12 / L3 / L34 …)
  2 ne_suffix          — new extreme vs none (E / N)
  3 wick_suffix        — wick extension (U up / D down / B both / ∅)
  4 penetration_suffix — prev-body penetration (P upper / R lower / H both / ∅)
  5 close_suffix       — close vs prev body (A above / O below / I inside)
  6 bar_body_wick      — body size + wick shape (X/S/M + F/TB/BB/J …)
  7 bar_gap_range      — gap class + range vs ATR (G1/2/3 + N/V/C)
  8 bar_line5          — VIX-Fix / PSAR / RSI2 state (PB/PS · VR/VX · R2L/R2X/R2H/R2D)
  9 vol_bucket         — WLNBB volume class (W < L < N < B < VB)

For each line we group every T1 signal by the line's value and report forward-return
distribution (median fwd_10d / fwd_20d, win%, clip25 = mean clipped to p25-p75) and the
LIFT vs the universe baseline. Goal: find which of the 9 lines DISCRIMINATE T1 outcomes
(STATE) and which are flat NOISE (SHAPE) — and the best value within each.

⚠️ fwd_Nd are SCREENS (overstate vs a true stop/target path-sim). Use them to rank
dimensions and surface candidates; any tradeable combo must then pass path-sim.

Run: cd backend && uv run python ../analysis/t1_9line.py
"""
import duckdb, numpy as np, pandas as pd

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
SIG = "T1"
DIMS = ["l_sig", "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix",
        "bar_body_wick", "bar_gap_range", "bar_line5", "vol_bucket"]
LINE_NAMES = {
    "l_sig": "1 l_sig (WLNBB absorption line)",
    "ne_suffix": "2 ne_suffix (new-extreme E/N)",
    "wick_suffix": "3 wick_suffix (U/D/B)",
    "penetration_suffix": "4 penetration_suffix (P/R/H)",
    "close_suffix": "5 close_suffix (A/O/I)",
    "bar_body_wick": "6 bar_body_wick (body+wick shape)",
    "bar_gap_range": "7 bar_gap_range (gap+range)",
    "bar_line5": "8 bar_line5 (VIX/PSAR/RSI2)",
    "vol_bucket": "9 vol_bucket (W/L/N/B/VB)",
}
MIN_N = 150          # ignore values too thin to trust
CLIP = (-90, 500)

con = duckdb.connect(DB, read_only=True)


def load():
    cols = ", ".join(f"coalesce({d},'∅') AS {d}" for d in DIMS)
    return con.execute(f"""
        SELECT * FROM (
            SELECT universe, ticker, date, year(date) AS yr, rsi_14,
                   fwd_10d, fwd_20d, {cols},
                   row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
            FROM bars WHERE t_sig = '{SIG}'
        ) WHERE rn = 1
    """).fetchdf()


def baseline():
    return con.execute(f"""
        SELECT median(fwd_10d) m10, median(fwd_20d) m20,
               avg(CASE WHEN fwd_10d>0 THEN 1.0 ELSE 0 END)*100 win10
        FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN {CLIP[0]} AND {CLIP[1]}
    """).fetchdf().iloc[0]


def clip25(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    s = s[s.between(*CLIP)]
    if len(s) < 5:
        return np.nan
    lo, hi = s.quantile(.25), s.quantile(.75)
    return round(float(s.clip(lo, hi).mean()), 2)


def stats(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    s = s[s.between(*CLIP)]
    return dict(n=len(s), med=round(float(s.median()), 2),
                win=round(float((s > 0).mean() * 100), 1))


def main():
    df = load()
    for c in ("fwd_10d", "fwd_20d", "rsi_14"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df[np.isfinite(df.fwd_10d) & df.fwd_10d.between(*CLIP)].copy()
    base = baseline()
    b10, b20, bwin = float(base.m10), float(base.m20), float(base.win10)

    t1 = stats(df.fwd_10d); t1c = clip25(df.fwd_10d)
    m20 = round(float(df.fwd_20d.median()), 2)

    out = []
    out.append(f"# T1 — 9-LINE DETAILED ANALYSIS (fwd_Nd screen)\n")
    out.append(f"**T1 overall:** n={t1['n']:,} · med fwd_10d **{t1['med']:+.2f}%** (base {b10:+.2f}, "
               f"lift {t1['med']-b10:+.2f}) · med fwd_20d {m20:+.2f}% · win10 {t1['win']:.0f}% "
               f"(base {bwin:.0f}) · clip25 {t1c:+.2f}\n")
    out.append("Each line below: value · n · med10 · clip25 · win10 · **lift10** vs universe base "
               f"({b10:+.2f}). Sorted by med10. Only n≥{MIN_N}.\n")

    spread = {}   # dimension -> spread of med10 across its values (discrimination strength)
    for d in DIMS:
        rows = []
        for v, g in df.groupby(d, sort=False):
            st = stats(g.fwd_10d)
            if st["n"] < MIN_N:
                continue
            rows.append((v, st["n"], st["med"], clip25(g.fwd_10d), st["win"],
                         round(st["med"] - b10, 2)))
        if not rows:
            continue
        rows.sort(key=lambda r: r[2], reverse=True)
        meds = [r[2] for r in rows]
        spread[d] = round(max(meds) - min(meds), 2)
        out.append(f"\n## {LINE_NAMES[d]}  — spread {spread[d]:+.2f}pp")
        out.append("| value | n | med10 | clip25 | win10 | lift10 |")
        out.append("|---|--:|--:|--:|--:|--:|")
        for v, n, med, c25, win, lift in rows:
            out.append(f"| {v} | {n:,} | {med:+.2f} | {c25:+.2f} | {win:.0f} | **{lift:+.2f}** |")

    # discrimination ranking
    out.append("\n## Discrimination ranking (which of the 9 lines matters)\n")
    out.append("| line | med10 spread across values |")
    out.append("|---|--:|")
    for d, sp in sorted(spread.items(), key=lambda x: -x[1]):
        out.append(f"| {LINE_NAMES[d]} | {sp:+.2f}pp |")

    # per-year robustness for the single best (value with highest lift, n≥400)
    best = None
    for d in DIMS:
        for v, g in df.groupby(d, sort=False):
            st = stats(g.fwd_10d)
            if st["n"] >= 400:
                lift = st["med"] - b10
                if best is None or lift > best[3]:
                    best = (d, v, st["n"], round(lift, 2), g)
    if best:
        d, v, n, lift, g = best
        out.append(f"\n## Per-year robustness — best line/value: {d}={v} "
                   f"(n={n:,}, lift {lift:+.2f})\n")
        out.append("| year | n | med10 | win10 |")
        out.append("|---|--:|--:|--:|")
        for yr, gy in g.groupby("yr"):
            st = stats(gy.fwd_10d)
            if st["n"] >= 30:
                out.append(f"| {int(yr)} | {st['n']:,} | {st['med']:+.2f} | {st['win']:.0f} |")

    # RSI14 context (the known master key — not one of the 9 lines, shown for reference)
    out.append("\n## RSI14 context (master-key reference, not one of the 9 lines)\n")
    out.append("| RSI14 band | n | med10 | win10 |")
    out.append("|---|--:|--:|--:|")
    bands = [("<30", df.rsi_14 < 30), ("30-40", (df.rsi_14 >= 30) & (df.rsi_14 < 40)),
             ("40-50", (df.rsi_14 >= 40) & (df.rsi_14 < 50)),
             ("50-60", (df.rsi_14 >= 50) & (df.rsi_14 < 60)),
             ("60-70", (df.rsi_14 >= 60) & (df.rsi_14 < 70)), ("≥70", df.rsi_14 >= 70)]
    for name, mask in bands:
        st = stats(df[mask].fwd_10d)
        if st["n"] >= 30:
            out.append(f"| {name} | {st['n']:,} | {st['med']:+.2f} | {st['win']:.0f} |")

    report = "\n".join(out)
    with open("/Users/sachoki/Desktop/sachoki-desktop/T1_9LINE_ANALIZI.md", "w") as f:
        f.write(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
