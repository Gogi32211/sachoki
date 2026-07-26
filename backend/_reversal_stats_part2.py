"""
PART 2 — extended reversal research (feasible items only):
  #2 Volume filter        (volume vs avg_vol_20d)
  #3 Sequence context     (T-signal within prior 3 bars before line5 fires)
  #4 Wyckoff phase filter  (wyc_phase split for bullish/bearish combos)
  #5 Win-rate vs return    (scatter as ASCII grid + ranked quality table)

  #1 Timeframe breakdown -> NOT POSSIBLE: DB holds daily bars only (no intraday).

Output: REVERSAL_STATS_PART2_260528.txt
"""
import duckdb, datetime as dt, os

DB  = "/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_STATS_PART2_260528.txt"
PIVOT_W, TREND = 3, 5

con = duckdb.connect()
con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

print("Building base...")
con.execute(f"""
CREATE TABLE base AS
SELECT ticker, date, universe, close, low, high,
    bar_line5, full_suffix, wyc_phase,
    volume, avg_vol_20d,
    LEAD(close,1)  OVER w AS c1,
    LEAD(close,3)  OVER w AS c3,
    LEAD(close,5)  OVER w AS c5,
    LEAD(close,10) OVER w AS c10,
    LAG(close,{TREND}) OVER w AS cm,
    MIN(low)  OVER wp AS pml,
    MAX(high) OVER wp AS pmh,
    MAX(CASE WHEN t_sig<>'' THEN 1 ELSE 0 END)
        OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS t_prior3
FROM s.bars
WINDOW w  AS (PARTITION BY ticker ORDER BY date),
       wp AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN {PIVOT_W} PRECEDING AND {PIVOT_W} FOLLOWING)
""")
con.execute("""
CREATE TABLE m AS
SELECT bar_line5, full_suffix, wyc_phase, t_prior3,
    full_suffix||'  +  '||bar_line5 AS combo,
    CASE WHEN avg_vol_20d>0 AND volume>avg_vol_20d THEN 'HIVOL' ELSE 'LOVOL' END AS volreg,
    (c1/close-1)*100 AS r1,(c3/close-1)*100 AS r3,(c5/close-1)*100 AS r5,(c10/close-1)*100 AS r10,
    CASE WHEN low<=pml THEN 1 ELSE 0 END AS piv_low,
    CASE WHEN high>=pmh THEN 1 ELSE 0 END AS piv_high,
    CASE WHEN (close/cm-1)<0 AND (c5/close-1)>0 THEN 1 ELSE 0 END AS rev_up,
    CASE WHEN (close/cm-1)>0 AND (c5/close-1)<0 THEN 1 ELSE 0 END AS rev_dn
FROM base WHERE c10 IS NOT NULL AND cm IS NOT NULL AND close>0
""")
total = con.execute("SELECT COUNT(*) FROM m").fetchone()[0]
print("rows", total)

AGG = """COUNT(*) AS n, ROUND(AVG(r5),3) AS r5, ROUND(MEDIAN(r5),3) AS r5med,
ROUND(AVG(r10),3) AS r10, ROUND(100.0*AVG(CASE WHEN r5>0 THEN 1 ELSE 0 END),1) AS win5,
ROUND(100.0*AVG(rev_up),1) AS revup, ROUND(100.0*AVG(rev_dn),1) AS revdn,
ROUND(100.0*AVG(piv_low),1) AS pivlo, ROUND(100.0*AVG(piv_high),1) AS pivhi"""

COLS=["label","n","r5","r5med","r10","win5","revup","revdn","pivlo","pivhi"]
HDR =["GROUP","N","+5d%","med5%","+10d%","win5%","revUP%","revDN%","pivLO%","pivHI%"]
W   =[34,9,7,7,7,6,7,7,7,7]
def row_fmt(label,r):
    cells=[str(label).ljust(W[0])]
    vals=[r["n"],r["r5"],r["r5med"],r["r10"],r["win5"],r["revup"],r["revdn"],r["pivlo"],r["pivhi"]]
    for i,v in enumerate(vals,1):
        cells.append((f"{int(v):,}" if i==1 else f"{v:.2f}").rjust(W[i]))
    return "".join(cells)
def header():
    h="".join(x.ljust(W[0]) if i==0 else x.rjust(W[i]) for i,x in enumerate(HDR))
    return h+"\n"+"-"*len(h)

R=[]
R.append("="*100)
R.append("REVERSAL RESEARCH — PART 2 (volume / sequence / Wyckoff phase / win-rate vs return)")
R.append("="*100)
R.append(f"Generated : {dt.datetime.now():%Y-%m-%d %H:%M}   |   usable bars: {total:,}   |   daily TF only")
R.append("Same definitions as Part 1: revUP=prior5d<0 & fwd5d>0 ; revDN=prior5d>0 & fwd5d<0 ;")
R.append("pivLO/HI = local extreme within +/-3 bars ; returns = raw close-to-close %.")

# focus combos
BULL = ["NDP  +  VX-PS-R2L","ED  +  VX-PS-R2L","EB  +  VR-PB-R2L","EDP  +  VX-PS-R2L","EB  +  PS-R2L"]
BEAR = ["EU  +  VX-PB-R2H","EUR  +  VX-PB-R2H","EB  +  VR-PB-R2H","EU  +  VR-PB-R2H"]

def grp(where, by):
    return con.execute(f"SELECT {by} AS g, {AGG} FROM m WHERE {where} GROUP BY 1 ORDER BY n DESC").fetchdf()

# ---------- #1 note ----------
R.append("\n\n"+"="*100)
R.append("#1  TIMEFRAME BREAKDOWN  —  NOT POSSIBLE")
R.append("="*100)
R.append("  The studio DB stores only DAILY (1d) bars. 4h/1h analysis would require re-fetching")
R.append("  intraday history from the Massive API and a separate enrichment pipeline. Out of scope")
R.append("  for the current dataset. All numbers below are 1d.")

# ---------- #2 volume ----------
R.append("\n\n"+"="*100)
R.append("#2  VOLUME FILTER  —  HIVOL (volume > 20d avg) vs LOVOL, per focus combo")
R.append("="*100)
R.append(header())
for c in BULL+BEAR:
    d=grp(f"combo='{c}'","volreg")
    R.append(f"[{c}]")
    for reg in ["HIVOL","LOVOL"]:
        sub=d[d.g==reg]
        if len(sub): R.append("  "+row_fmt(reg,sub.iloc[0]))
R.append("\n  Reading: if HIVOL row shows higher +5d / revUP than LOVOL, the edge is volume-confirmed.")

# ---------- #3 sequence ----------
R.append("\n\n"+"="*100)
R.append("#3  SEQUENCE CONTEXT  —  T-signal fired within prior 3 bars (before the line5 bar)")
R.append("="*100)
R.append("  Split each oversold-reversal line5 family by whether ANY T-signal (T1..T12) printed in")
R.append("  the 3 bars immediately before. t_prior3=1 means 'T-signal preceded'.")
R.append(header())
for fam in ["VX-PS-R2L","VR-PS-R2L","PS-R2L","PS-R2X"]:
    d=grp(f"bar_line5='{fam}'","CASE WHEN t_prior3=1 THEN 'T-prior=YES' ELSE 'T-prior=no ' END")
    R.append(f"[line5={fam}]")
    for lab in ["T-prior=YES","T-prior=no "]:
        sub=d[d.g==lab]
        if len(sub): R.append("  "+row_fmt(lab,sub.iloc[0]))
R.append("\n  Reading: higher revUP/+5d on 'T-prior=YES' => a preceding T-signal strengthens the bottom.")

# ---------- #4 wyckoff ----------
R.append("\n\n"+"="*100)
R.append("#4  WYCKOFF PHASE  —  bullish & bearish focus combos split by wyc_phase")
R.append("="*100)
PHASES=["MARKUP","MKDN","ACC_TR","DIST_TR","SPRING","UTAD","SOS","NEUTRAL"]
for c in BULL[:3]+BEAR[:2]:
    R.append(f"\n[{c}]")
    R.append(header())
    d=grp(f"combo='{c}'","wyc_phase")
    d=d[d.n>=200].sort_values("n",ascending=False)
    for _,r in d.iterrows():
        if r["g"] in PHASES or True:
            R.append("  "+row_fmt(r["g"],r))
R.append("\n  Reading: the phase with the highest revUP%/+5d (and decent N) is where the combo works best.")

# also aggregate: bullish family across phases
R.append("\n  -- Aggregate: ALL oversold-reversal bars (line5 LIKE '%R2L') by phase --")
R.append(header())
d=grp("bar_line5 LIKE '%R2L'","wyc_phase")
for _,r in d[d.n>=1000].sort_values("revup",ascending=False).iterrows():
    R.append("  "+row_fmt(r["g"],r))

# ---------- #5 win-rate vs return scatter ----------
R.append("\n\n"+"="*100)
R.append("#5  WIN-RATE vs +5d RETURN  —  combos with N>=5000")
R.append("="*100)
sc=con.execute(f"""SELECT combo AS g,{AGG} FROM m
   WHERE bar_line5<>'' AND full_suffix<>'' GROUP BY combo HAVING COUNT(*)>=5000""").fetchdf()
# quality score: standardized win5 + standardized r5
import statistics as st
w_mu,w_sd=sc.win5.mean(),sc.win5.std()
r_mu,r_sd=sc.r5.mean(),sc.r5.std()
sc["q"]=((sc.win5-w_mu)/w_sd + (sc.r5-r_mu)/r_sd)
sc=sc.sort_values("q",ascending=False)
R.append(f"  Field means: win5%={w_mu:.2f} (sd {w_sd:.2f}), +5d%={r_mu:.3f} (sd {r_sd:.3f}). "
         f"q = z(win5)+z(+5d).")
R.append("\n  TOP-RIGHT (best win-rate AND return):")
R.append(header())
for _,r in sc.head(15).iterrows(): R.append("  "+row_fmt(r["g"],r))
R.append("\n  BOTTOM-LEFT (worst):")
R.append(header())
for _,r in sc.tail(8).iterrows(): R.append("  "+row_fmt(r["g"],r))

# ASCII scatter
R.append("\n\n  ASCII SCATTER  (x = win5%, y = +5d mean%).  '#'=multiple combos, '*'=single.")
xs,ys=sc.win5.tolist(),sc.r5.tolist()
xmin,xmax=min(xs),max(xs); ymin,ymax=min(ys),max(ys)
GW,GH=64,20
grid=[[" "]*GW for _ in range(GH)]
for x,y in zip(xs,ys):
    cx=int((x-xmin)/(xmax-xmin+1e-9)*(GW-1))
    cy=int((y-ymin)/(ymax-ymin+1e-9)*(GH-1))
    cy=GH-1-cy
    grid[cy][cx]="#" if grid[cy][cx]!=" " else "*"
R.append(f"   +5d% (top={ymax:.2f}, bottom={ymin:.2f})")
for row in grid:
    R.append("   |"+"".join(row))
R.append("   +"+"-"*GW)
R.append(f"    win5%: left={xmin:.1f}  ...  right={xmax:.1f}")

R.append("\n\n"+"="*100)
R.append("NOTES")
R.append("-"*100)
R.append("  - #1 timeframe not possible (daily-only DB).")
R.append("  - Volume regime uses avg_vol_20d already enriched in DB.")
R.append("  - t_prior3 looks at the 3 bars strictly BEFORE the signal bar (no lookahead).")
R.append("  - Phases NEUTRAL/SOS/UTAD/SPRING are rare; rows shown only if N>=200.")
R.append("="*100)

open(OUT,"w").write("\n".join(R))
print("written",OUT,os.path.getsize(OUT),"bytes")
