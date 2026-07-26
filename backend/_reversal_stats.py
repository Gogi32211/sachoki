"""
Reversal / forward-return statistics on bar_line5 (last label line) and
full_suffix (second label line) signals.

Approach 4 = everything together:
  - forward-return distribution (mean/median +1d/+3d/+5d/+10d, win-rate)
  - forward-return flip / reversal rate (prior 5d trend vs forward 5d)
  - pivot hit-rate (signal bar is a local swing low/high within +/-3 bars)

Plus co-occurrence: bar_line5 x full_suffix combinations.

Output: REVERSAL_STATS_REPORT_<date>.txt
"""
import duckdb
import datetime as dt
import os

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_STATS_REPORT_260528.txt"

PIVOT_W = 3          # +/- bars for swing pivot confirmation
TREND_LOOKBACK = 5   # prior-trend window for reversal definition
MIN_N = 500          # minimum sample size for a group to be reported

con = duckdb.connect()
con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

print("Building windowed base table (forward/backward returns + pivots)...")
con.execute(f"""
CREATE TABLE base AS
SELECT
    ticker, date, universe, close,
    bar_line5, full_suffix,
    LEAD(close, 1)  OVER w AS c1,
    LEAD(close, 3)  OVER w AS c3,
    LEAD(close, 5)  OVER w AS c5,
    LEAD(close, 10) OVER w AS c10,
    LAG(close, {TREND_LOOKBACK}) OVER w AS cm,
    MIN(low)  OVER wp AS piv_min_low,
    MAX(high) OVER wp AS piv_max_high,
    low, high
FROM s.bars
WINDOW
    w  AS (PARTITION BY ticker ORDER BY date),
    wp AS (PARTITION BY ticker ORDER BY date
           ROWS BETWEEN {PIVOT_W} PRECEDING AND {PIVOT_W} FOLLOWING)
""")

# derived metrics view
con.execute("""
CREATE TABLE m AS
SELECT
    bar_line5, full_suffix, universe, date,
    (c1/close  - 1)*100 AS r1,
    (c3/close  - 1)*100 AS r3,
    (c5/close  - 1)*100 AS r5,
    (c10/close - 1)*100 AS r10,
    (close/cm  - 1)*100 AS rprev,
    CASE WHEN low  <= piv_min_low THEN 1 ELSE 0 END AS piv_low,
    CASE WHEN high >= piv_max_high THEN 1 ELSE 0 END AS piv_high,
    -- bullish reversal: was falling, then rises over next 5d
    CASE WHEN (close/cm - 1) < 0 AND (c5/close - 1) > 0 THEN 1 ELSE 0 END AS rev_up,
    -- bearish reversal: was rising, then falls over next 5d
    CASE WHEN (close/cm - 1) > 0 AND (c5/close - 1) < 0 THEN 1 ELSE 0 END AS rev_dn
FROM base
WHERE c10 IS NOT NULL AND cm IS NOT NULL AND close > 0
""")

total = con.execute("SELECT COUNT(*) FROM m").fetchone()[0]
dmin, dmax = con.execute("SELECT MIN(date), MAX(date) FROM m").fetchone()
print(f"Usable rows: {total:,}  range {dmin} .. {dmax}")

AGG = """
    COUNT(*)                              AS n,
    ROUND(AVG(r1),3)                      AS r1_mean,
    ROUND(AVG(r3),3)                      AS r3_mean,
    ROUND(AVG(r5),3)                      AS r5_mean,
    ROUND(AVG(r10),3)                     AS r10_mean,
    ROUND(MEDIAN(r5),3)                   AS r5_med,
    ROUND(100.0*AVG(CASE WHEN r5>0 THEN 1 ELSE 0 END),1) AS win5,
    ROUND(100.0*AVG(rev_up),1)            AS rev_up,
    ROUND(100.0*AVG(rev_dn),1)            AS rev_dn,
    ROUND(100.0*AVG(piv_low),1)           AS piv_lo,
    ROUND(100.0*AVG(piv_high),1)          AS piv_hi
"""

def fetch(group_expr, where=""):
    q = f"SELECT {group_expr} AS code, {AGG} FROM m {where} GROUP BY 1 HAVING COUNT(*) >= {MIN_N} ORDER BY n DESC"
    return con.execute(q).fetchdf()

line5_df = fetch("bar_line5", "WHERE bar_line5 <> ''")
suffix_df = fetch("full_suffix", "WHERE full_suffix <> ''")
combo_df = con.execute(f"""
    SELECT full_suffix || '  +  ' || bar_line5 AS code, {AGG}
    FROM m
    WHERE bar_line5 <> '' AND full_suffix <> ''
    GROUP BY full_suffix, bar_line5
    HAVING COUNT(*) >= {MIN_N}
    ORDER BY n DESC
""").fetchdf()

# baseline (all bars) for comparison
base_row = con.execute(f"SELECT 'ALL BARS' AS code, {AGG} FROM m").fetchdf()

# ---------- report formatting ----------
COLS = ["code","n","r1_mean","r3_mean","r5_mean","r10_mean","r5_med","win5","rev_up","rev_dn","piv_lo","piv_hi"]
HDR  = ["CODE","N","+1d%","+3d%","+5d%","+10d%","med5%","win5%","revUP%","revDN%","pivLO%","pivHI%"]
W    = [14, 9, 7, 7, 7, 7, 7, 6, 7, 7, 7, 7]

def fmt_table(df, sort_by=None, top=None):
    d = df.copy()
    if sort_by:
        d = d.sort_values(sort_by, ascending=False)
    if top:
        d = d.head(top)
    lines = []
    hdr = "".join(h.rjust(w) if i else h.ljust(w) for i,(h,w) in enumerate(zip(HDR,W)))
    lines.append(hdr)
    lines.append("-"*len(hdr))
    for _,row in d.iterrows():
        cells=[]
        for i,(c,w) in enumerate(zip(COLS,W)):
            v=row[c]
            if i==0:
                cells.append(str(v).ljust(w))
            elif c=="n":
                cells.append(f"{int(v):,}".rjust(w))
            else:
                cells.append(f"{v:.2f}".rjust(w) if c!="r5_med" else f"{v:.2f}".rjust(w))
        lines.append("".join(cells))
    return "\n".join(lines)

def section(title):
    return f"\n\n{'='*92}\n{title}\n{'='*92}\n"

R=[]
R.append("="*92)
R.append("REVERSAL & FORWARD-RETURN STATISTICS  —  bar_line5 (last line) & full_suffix (2nd line)")
R.append("="*92)
R.append(f"Generated      : {dt.datetime.now():%Y-%m-%d %H:%M}")
R.append(f"Data source    : studio_analytics.duckdb  (SP500 + NASDAQ + Russell2K, 5y)")
R.append(f"Usable bars    : {total:,}   (rows with full +10d forward & -5d prior context)")
R.append(f"Date range     : {dmin}  ..  {dmax}")
R.append("")
R.append("METHODOLOGY")
R.append("-"*92)
R.append(f"  +1d/+3d/+5d/+10d%  forward close-to-close return from the signal bar (%).")
R.append(f"  med5%              median +5d return.")
R.append(f"  win5%              share of bars with positive +5d return.")
R.append(f"  revUP%             BULLISH reversal: prior {TREND_LOOKBACK}d return < 0 AND +5d return > 0.")
R.append(f"  revDN%             BEARISH reversal: prior {TREND_LOOKBACK}d return > 0 AND +5d return < 0.")
R.append(f"  pivLO%             signal bar is a local swing LOW (lowest low within +/-{PIVOT_W} bars).")
R.append(f"  pivHI%             signal bar is a local swing HIGH (highest high within +/-{PIVOT_W} bars).")
R.append(f"  Groups shown only if N >= {MIN_N}.")
R.append("")
R.append("READING IT")
R.append("-"*92)
R.append("  High revUP% + high pivLO%  => the code marks BOTTOMS (bullish reversal).")
R.append("  High revDN% + high pivHI%  => the code marks TOPS   (bearish reversal).")
R.append("  Compare every row against the ALL-BARS baseline below to judge edge.")

R.append(section("0. BASELINE  (all usable bars, unconditional)"))
R.append(fmt_table(base_row))

R.append(section("1. LAST LINE  —  bar_line5  (PSAR / RSI2 / VIX-Fix)  sorted by frequency"))
R.append(fmt_table(line5_df))
R.append("\n  >> Strongest BULLISH-reversal line5 codes (by revUP%, N>=2000):")
R.append(fmt_table(line5_df[line5_df.n>=2000], sort_by="rev_up", top=8))
R.append("\n  >> Strongest BEARISH-reversal line5 codes (by revDN%, N>=2000):")
R.append(fmt_table(line5_df[line5_df.n>=2000], sort_by="rev_dn", top=8))

R.append(section("2. SECOND LINE  —  full_suffix  (NE / wick / penetration / close)  by frequency"))
R.append(fmt_table(suffix_df))
R.append("\n  >> Strongest BULLISH-reversal suffix codes (by revUP%, N>=2000):")
R.append(fmt_table(suffix_df[suffix_df.n>=2000], sort_by="rev_up", top=8))
R.append("\n  >> Strongest BEARISH-reversal suffix codes (by revDN%, N>=2000):")
R.append(fmt_table(suffix_df[suffix_df.n>=2000], sort_by="rev_dn", top=8))

R.append(section("3. CO-OCCURRENCE  —  full_suffix + bar_line5  (top 30 by frequency)"))
R.append(fmt_table(combo_df, top=30))
R.append("\n  >> Top BULLISH-reversal combos (by revUP%, N>=1000):")
R.append(fmt_table(combo_df[combo_df.n>=1000], sort_by="rev_up", top=12))
R.append("\n  >> Top BEARISH-reversal combos (by revDN%, N>=1000):")
R.append(fmt_table(combo_df[combo_df.n>=1000], sort_by="rev_dn", top=12))
R.append("\n  >> Highest +5d mean-return combos (by r5_mean, N>=1000):")
R.append(fmt_table(combo_df[combo_df.n>=1000], sort_by="r5_mean", top=12))
R.append("\n  >> Lowest +5d mean-return combos (most bearish, N>=1000):")
R.append(fmt_table(combo_df[combo_df.n>=1000].sort_values("r5_mean").head(12)))

# ---------- key findings ----------
base = base_row.iloc[0]
R.append(section("4. KEY FINDINGS  (auto-derived vs baseline)"))
def edge(df, col, label, n_min, ascending=False):
    d = df[df.n>=n_min].sort_values(col, ascending=ascending).head(5)
    out=[f"  {label}:"]
    for _,r in d.iterrows():
        out.append(f"    {str(r['code']):<26} {col}={r[col]:.2f}   (N={int(r['n']):,}, baseline {col}={base[col]:.2f})")
    return "\n".join(out)
R.append(edge(line5_df,"rev_up","line5 best bullish-reversal",2000))
R.append("")
R.append(edge(line5_df,"rev_dn","line5 best bearish-reversal",2000))
R.append("")
R.append(edge(line5_df,"piv_lo","line5 best swing-LOW marker",2000))
R.append("")
R.append(edge(line5_df,"piv_hi","line5 best swing-HIGH marker",2000))
R.append("")
R.append(edge(suffix_df,"rev_up","suffix best bullish-reversal",2000))
R.append("")
R.append(edge(suffix_df,"rev_dn","suffix best bearish-reversal",2000))
R.append("")
R.append(edge(combo_df,"rev_up","combo best bullish-reversal",1000))
R.append("")
R.append(edge(combo_df,"rev_dn","combo best bearish-reversal",1000))
R.append("")
R.append(edge(combo_df,"r5_mean","combo highest +5d return",1000))
R.append("")
R.append(edge(combo_df,"r5_mean","combo lowest +5d return",1000,ascending=True))

R.append("\n\n" + "="*92)
R.append("NOTES")
R.append("-"*92)
R.append("  - Returns are raw close-to-close, not market/beta adjusted. Compare to baseline row.")
R.append("  - Russell2K includes illiquid names; an avg-volume filter would sharpen edges.")
R.append("  - 'reversal' here = 5d-prior trend flip vs 5d-forward. pivot = +/-3 bar local extreme.")
R.append("  - bar_line5 tokens: PB=close>PSAR(bull), PS=close<PSAR(bear); R2L/R2H oversold/overbought,")
R.append("    R2X cross-up out of oversold, R2D cross-down out of overbought; VX/VR = VIX-Fix spike.")
R.append("="*92)

with open(OUT,"w") as f:
    f.write("\n".join(R))
print(f"Report written: {OUT}  ({os.path.getsize(OUT):,} bytes)")
