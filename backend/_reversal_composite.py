"""
FULL re-analysis keyed on composite_full_suffix (the column the Sequence Builder
actually matches). Every code in this report is directly verifiable in the builder:
  -> put the code in bar 0 (now), all LINE1-6 chips ON, pivot 3-3, universe "SP500 + NASDAQ".

Metrics mirror the builder exactly:
  HH% / HL% = next_pivot_is_hh_3 / (hh+hl)   [Williams pivot 3-3, CLEAN booleans]
  gainHH    = avg pct_to_next_hh_3   (builder "avg gain to HH")
  ddHL      = avg pct_to_next_hl_3   (builder "avg drawdown to HL")
  f5med     = MEDIAN(fwd_5d)  [robust, finite only]  -- builder shows MEAN (outlier-prone)
  f5avg*    = AVG(fwd_5d) finite & clipped |.|<=100%  -- cleaned approximation of builder mean
  win5      = share fwd_5d>0

NOTE: fwd_* columns are PERCENT but contaminated by inf (close=0) and penny-stock
extremes; the builder uses raw AVG so its Forward-returns block is noisy. Trust HH%/HL%.
"""
import duckdb, datetime as dt, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_STATS_COMPOSITE_260528.txt"
P=3
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

# finite filter for fwd/pct columns
FIN5  = "isfinite(fwd_5d)"
# aggregate expression (pivot 3-3). hh/hl from clean booleans; fwd robust.
AGG=f"""
  COUNT(*) AS n,
  SUM(next_pivot_is_hh_{P}) AS hh, SUM(next_pivot_is_hl_{P}) AS hl,
  ROUND(AVG(CASE WHEN isfinite(pct_to_next_hh_{P}) AND ABS(pct_to_next_hh_{P})<200 THEN pct_to_next_hh_{P} END),2) AS gainHH,
  ROUND(AVG(CASE WHEN isfinite(pct_to_next_hl_{P}) AND ABS(pct_to_next_hl_{P})<200 THEN pct_to_next_hl_{P} END),2) AS ddHL,
  ROUND(MEDIAN(CASE WHEN isfinite(fwd_5d) THEN fwd_5d END),3) AS f5med,
  ROUND(AVG(CASE WHEN isfinite(fwd_5d) AND ABS(fwd_5d)<=100 THEN fwd_5d END),3) AS f5avg,
  ROUND(100.0*AVG(CASE WHEN isfinite(fwd_5d) AND fwd_5d>0 THEN 1.0 WHEN isfinite(fwd_5d) THEN 0.0 END),1) AS win5
"""
def rows(group, where, having_n=500, order="n DESC", limit=None):
    lim=f"LIMIT {limit}" if limit else ""
    q=f"SELECT {group} AS code, {AGG} FROM s.bars WHERE {where} GROUP BY 1 HAVING COUNT(*)>={having_n} ORDER BY {order} {lim}"
    return con.execute(q).fetchdf()

def hhpct(r):
    d=(r['hh'] or 0)+(r['hl'] or 0)
    return round((r['hh'] or 0)/d*100,1) if d else 0.0
def hlpct(r):
    d=(r['hh'] or 0)+(r['hl'] or 0)
    return round((r['hl'] or 0)/d*100,1) if d else 0.0

COLW=[26,9,7,7,8,8,8,8,7]
HEAD=["CODE","N","HH%","HL%","gainHH","ddHL","f5med","f5avg","win5"]
def header():
    h="".join(x.ljust(COLW[0]) if i==0 else x.rjust(COLW[i]) for i,x in enumerate(HEAD))
    return h+"\n"+"-"*len(h)
def fmt(r):
    cells=[str(r['code']).ljust(COLW[0])]
    vals=[int(r['n']), hhpct(r), hlpct(r), r['gainHH'], r['ddHL'], r['f5med'], r['f5avg'], r['win5']]
    for i,v in enumerate(vals,1):
        if v is None: cells.append("-".rjust(COLW[i]))
        elif i==1: cells.append(f"{int(v):,}".rjust(COLW[i]))
        else: cells.append(f"{v:.2f}".rjust(COLW[i]))
    return "".join(cells)
def table(df, top=None):
    d=df.head(top) if top else df
    return "\n".join([header()]+[fmt(r) for _,r in d.iterrows()])

R=[]
def H(t): R.append("\n\n"+"="*96+f"\n{t}\n"+"="*96)

tot,dmin,dmax=con.execute("SELECT COUNT(*),MIN(date),MAX(date) FROM s.bars").fetchone()
R.append("="*96)
R.append("REVERSAL STATISTICS — COMPOSITE-SUFFIX EDITION (verifiable in Sequence Builder)")
R.append("="*96)
R.append(f"Generated : {dt.datetime.now():%Y-%m-%d %H:%M}   |   {tot:,} bars   |   {dmin}..{dmax}   |   pivot 3-3")
R.append("")
R.append("WHY THIS REPORT: the Sequence Builder matches the chart-exact column")
R.append("composite_full_suffix (close letter A/O/I appended), NOT the bare full_suffix used in")
R.append("the earlier Part 1-3 reports. So 'EB' there = the PARENT group; the real signals are")
R.append("EBO / EBA. This report uses composite codes so each row is reproducible in the builder.")
R.append("")
R.append("HOW TO VERIFY A ROW IN THE BUILDER:")
R.append("  bar 0 (now): suffix=<CODE-suffix>  l5=<CODE-line5> ; all LINE1-6 ON ; pivot 3-3 ;")
R.append("  universe 'SP500 + NASDAQ'. Builder 'Next pivot HH/HL %' == this report's HH%/HL%.")
R.append("")
R.append("METRICS (pivot 3-3):")
R.append("  HH%/HL% = next pivot is Higher-High / Higher-Low(pullback), of resolved pivots. CLEAN.")
R.append("  gainHH  = avg % up to that HH ; ddHL = avg % drawdown to that HL (finite, |.|<200%).")
R.append("  f5med   = MEDIAN 5d fwd return (robust) ; f5avg = mean 5d clipped |.|<=100% ; win5 = %>0.")
R.append("")
R.append("CAVEATS:")
R.append("  * Builder dropdown 'SP500 + NASDAQ' actually searches ALL universes incl. Russell2K")
R.append("    (uni='both' omits the filter). Russell2K penny stocks drive the fwd-return outliers.")
R.append("  * Builder 'Forward returns' uses RAW AVG(fwd_5d) with inf/extremes -> noisy. Trust HH%/HL%")
R.append("    + f5med here. Bullish bottom => HH% high; Bearish top => HL% high.")

# baseline
b=con.execute(f"SELECT 'ALL BARS' AS code,{AGG} FROM s.bars").fetchdf().iloc[0]
H("0. BASELINE (all bars)")
R.append(header()); R.append(fmt(b))

# 1. last line
H("1. LAST LINE — bar_line5 (PSAR/RSI2/VIX), by frequency")
l5=rows("bar_line5","bar_line5<>''")
R.append(table(l5))
R.append("\n  >> Most BULLISH last-line (HH%, N>=2000):")
R.append(table(l5[l5.n>=2000].assign(_k=l5[l5.n>=2000].apply(hhpct,axis=1)).sort_values("_k",ascending=False).drop(columns="_k"),top=8))
R.append("\n  >> Most BEARISH last-line (HL%, N>=2000):")
R.append(table(l5[l5.n>=2000].assign(_k=l5[l5.n>=2000].apply(hlpct,axis=1)).sort_values("_k",ascending=False).drop(columns="_k"),top=8))

# 2. composite suffix
H("2. SECOND LINE — composite_full_suffix (close-aware), by frequency")
sfx=rows("composite_full_suffix","composite_full_suffix<>''")
R.append(table(sfx))
R.append("\n  >> Most BULLISH suffix (HH%, N>=5000):")
R.append(table(sfx[sfx.n>=5000].assign(_k=sfx[sfx.n>=5000].apply(hhpct,axis=1)).sort_values("_k",ascending=False).drop(columns="_k"),top=10))
R.append("\n  >> Most BEARISH suffix (HL%, N>=5000):")
R.append(table(sfx[sfx.n>=5000].assign(_k=sfx[sfx.n>=5000].apply(hlpct,axis=1)).sort_values("_k",ascending=False).drop(columns="_k"),top=10))

# 3. close-suffix value across families
H("3. CLOSE-SUFFIX MATTERS — same parent (full_suffix), split by composite (A/O/I)")
R.append("  Proves the close letter is NOT redundant: A vs O often flip direction. (N>=2000)")
fam=con.execute(f"""
  SELECT full_suffix AS parent, composite_full_suffix AS code, {AGG}
  FROM s.bars
  WHERE full_suffix IN ('EB','NU','ND','NUR','NDP','NB','NR','NP','EU','ED','NH')
  GROUP BY 1,2 HAVING COUNT(*)>=2000 ORDER BY full_suffix, n DESC
""").fetchdf()
cur=None
for _,r in fam.iterrows():
    if r['parent']!=cur:
        cur=r['parent']; R.append(f"\n[parent full_suffix = {cur}]"); R.append(header())
    R.append(fmt(r))

# 4. co-occurrence
H("4. CO-OCCURRENCE — composite_full_suffix + bar_line5")
combo=con.execute(f"""
  SELECT composite_full_suffix || '  +  ' || bar_line5 AS code, {AGG}
  FROM s.bars WHERE composite_full_suffix<>'' AND bar_line5<>''
  GROUP BY 1 HAVING COUNT(*)>=1000 ORDER BY n DESC
""").fetchdf()
R.append("  Top 25 by frequency:")
R.append(table(combo,top=25))
R.append("\n  >> Top BULLISH combos (HH%, N>=1500):")
cb=combo[combo.n>=1500].copy(); cb["_k"]=cb.apply(hhpct,axis=1)
R.append(table(cb.sort_values("_k",ascending=False).drop(columns="_k"),top=15))
R.append("\n  >> Top BEARISH combos (HL%, N>=1500):")
cb["_k"]=cb.apply(hlpct,axis=1)
R.append(table(cb.sort_values("_k",ascending=False).drop(columns="_k"),top=15))

# 5. verifiable focus signals
H("5. VERIFIABLE FOCUS SIGNALS — type these in builder bar 0 (now)")
focus=[("EBO","VX-PS-R2L"),("EBO","PS-R2L"),("EBO","VR-PS-R2L"),
       ("EBA","VX-PB-R2H"),("EBA","PS-R2H"),("EUO","VX-PS-R2L"),
       ("EU","VX-PB-R2H"),("EUA","VX-PB-R2H"),("NDPO","VX-PS-R2L"),("NURO","VX-PS-R2L")]
R.append(header())
for sf,l in focus:
    d=con.execute(f"SELECT '{sf} + {l}' AS code,{AGG} FROM s.bars WHERE composite_full_suffix='{sf}' AND bar_line5='{l}'").fetchdf()
    if d.iloc[0]['n']>0: R.append(fmt(d.iloc[0]))
    else: R.append(f"{sf} + {l}".ljust(26)+"  (0 rows — combo absent)")

R.append("\n\n"+"="*96)
R.append("NOTES")
R.append("-"*96)
R.append("  - Pivot 3-3 = Williams 3 bars left/right. Same as builder default.")
R.append("  - For a SHORT read HL% (pullback) high + ddHL more negative.")
R.append("  - composite tokens: close A=above prev body / O=below / I=inside (appended when bar is")
R.append("    'interesting': E+both-wick, or N+wick/penetration). Bare codes (EU/ED) keep no close.")
R.append("="*96)

open(OUT,"w").write("\n".join(R))
print("written",OUT,os.path.getsize(OUT),"bytes")

# ---- verification vs builder backend ----
print("\n=== verify against query_exact_sequence (signal on bar 0) ===")
from studio.signal_stats import query_exact_sequence as qx
strict={f"line{i}":True for i in range(1,7)}
for sf,l in [("EBO","VX-PS-R2L"),("EBA","VX-PB-R2H")]:
    r=qx(bars=[{},{},{"suffix":sf,"line5":l}], universe=None, strictness=strict, pivot_lr=3)
    o=r.get("outcomes",{})
    mine=con.execute(f"SELECT COUNT(*) n, SUM(next_pivot_is_hh_3) hh, SUM(next_pivot_is_hl_3) hl FROM s.bars WHERE composite_full_suffix='{sf}' AND bar_line5='{l}'").fetchdf().iloc[0]
    mh=round((mine['hh'])/((mine['hh'])+(mine['hl']))*100,1)
    print(f"{sf}+{l}: builder matches={r.get('matches')} HH%={o.get('hh_pct')} HL%={o.get('hl_pct')} | mine n={int(mine['n'])} HH%={mh}")
