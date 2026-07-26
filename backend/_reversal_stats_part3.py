"""
PART 3 — filter stacking / funnel:
  A) Triple filter (T-prior=NO + HIVOL + MKDN) applied stepwise to top bullish combos.
  B) EB+VX-PS-R2L HIVOL  — the most balanced top-right bottom signal (detail + phase split).
  C) EU+VX-PB-R2H HIVOL  — practically a short signal (detail + phase split + Z-prior context).

Output: REVERSAL_STATS_PART3_260528.txt
"""
import duckdb, datetime as dt, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_STATS_PART3_260528.txt"
PIVOT_W,TREND=3,5
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")
print("base...")
con.execute(f"""
CREATE TABLE base AS
SELECT ticker,date,close,low,high,bar_line5,full_suffix,wyc_phase,volume,avg_vol_20d,
  LEAD(close,1) OVER w AS c1, LEAD(close,3) OVER w AS c3, LEAD(close,5) OVER w AS c5,
  LEAD(close,10) OVER w AS c10, LAG(close,{TREND}) OVER w AS cm,
  MIN(low) OVER wp AS pml, MAX(high) OVER wp AS pmh,
  MAX(CASE WHEN t_sig<>'' THEN 1 ELSE 0 END) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS t_prior3,
  MAX(CASE WHEN z_sig<>'' THEN 1 ELSE 0 END) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS z_prior3
FROM s.bars
WINDOW w AS (PARTITION BY ticker ORDER BY date),
       wp AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN {PIVOT_W} PRECEDING AND {PIVOT_W} FOLLOWING)
""")
con.execute("""
CREATE TABLE m AS
SELECT bar_line5,full_suffix,wyc_phase,t_prior3,z_prior3,
  full_suffix||'  +  '||bar_line5 AS combo,
  CASE WHEN avg_vol_20d>0 AND volume>avg_vol_20d THEN 1 ELSE 0 END AS hivol,
  (c1/close-1)*100 AS r1,(c3/close-1)*100 AS r3,(c5/close-1)*100 AS r5,(c10/close-1)*100 AS r10,
  CASE WHEN low<=pml THEN 1 ELSE 0 END AS piv_low,
  CASE WHEN high>=pmh THEN 1 ELSE 0 END AS piv_high,
  CASE WHEN (close/cm-1)<0 AND (c5/close-1)>0 THEN 1 ELSE 0 END AS rev_up,
  CASE WHEN (close/cm-1)>0 AND (c5/close-1)<0 THEN 1 ELSE 0 END AS rev_dn
FROM base WHERE c10 IS NOT NULL AND cm IS NOT NULL AND close>0
""")
print("rows",con.execute("SELECT COUNT(*) FROM m").fetchone()[0])

AGG="""COUNT(*) AS n, ROUND(AVG(r5),3) AS r5, ROUND(MEDIAN(r5),3) AS r5med,
ROUND(AVG(r10),3) AS r10, ROUND(100.0*AVG(CASE WHEN r5>0 THEN 1 ELSE 0 END),1) AS win5,
ROUND(100.0*AVG(rev_up),1) AS revup, ROUND(100.0*AVG(rev_dn),1) AS revdn,
ROUND(100.0*AVG(piv_low),1) AS pivlo, ROUND(100.0*AVG(piv_high),1) AS pivhi"""
COLS=["label","n","r5","r5med","r10","win5","revup","revdn","pivlo","pivhi"]
HDR=["FILTER STEP","N","+5d%","med5%","+10d%","win5%","revUP%","revDN%","pivLO%","pivHI%"]
Wd=[40,9,8,8,8,7,8,8,8,8]
def line(lab,r):
    out=[str(lab).ljust(Wd[0])]
    vals=[r["n"],r["r5"],r["r5med"],r["r10"],r["win5"],r["revup"],r["revdn"],r["pivlo"],r["pivhi"]]
    for i,v in enumerate(vals,1):
        out.append((f"{int(v):,}" if i==1 else f"{v:.2f}").rjust(Wd[i]))
    return "".join(out)
def head():
    h="".join(x.ljust(Wd[0]) if i==0 else x.rjust(Wd[i]) for i,x in enumerate(HDR))
    return h+"\n"+"-"*len(h)
def q(where):
    return con.execute(f"SELECT {AGG} FROM m WHERE {where}").fetchdf().iloc[0]

R=[]
R.append("="*112)
R.append("REVERSAL RESEARCH — PART 3 (filter stacking: T-prior=NO + HIVOL + MKDN)")
R.append("="*112)
R.append(f"Generated : {dt.datetime.now():%Y-%m-%d %H:%M}   |   daily TF   |   defs as Part 1/2")
R.append("hivol = volume > 20d avg ; t_prior3 = any T1..T12 in prior 3 bars ; MKDN = Wyckoff markdown.")
R.append("Watch med5% vs +5d%: if median stays ~0 while mean is high, the edge is tail-driven, not consistent.")

# ---------- A) funnel ----------
R.append("\n\n"+"="*112)
R.append("A.  TRIPLE-FILTER FUNNEL  —  stepwise edge change for top bullish bottom combos")
R.append("="*112)
combos=["NDP  +  VX-PS-R2L","ED  +  VX-PS-R2L","EDP  +  VX-PS-R2L","EB  +  VX-PS-R2L"]
for c in combos:
    cw=f"combo='{c}'"
    R.append(f"\n[{c}]")
    R.append(head())
    R.append(line("1. ALL (combo only)",            q(cw)))
    R.append(line("2. + HIVOL",                      q(cw+" AND hivol=1")))
    R.append(line("3. + HIVOL + MKDN",               q(cw+" AND hivol=1 AND wyc_phase='MKDN'")))
    R.append(line("4. + HIVOL + MKDN + T-prior=NO",  q(cw+" AND hivol=1 AND wyc_phase='MKDN' AND t_prior3=0")))

# also the broad oversold family, fully stacked (large N robustness)
R.append("\n\n[ALL oversold-reversal bars: line5 LIKE '%R2L']")
R.append(head())
fw="bar_line5 LIKE '%R2L'"
R.append(line("1. ALL R2L",                       q(fw)))
R.append(line("2. + HIVOL",                        q(fw+" AND hivol=1")))
R.append(line("3. + HIVOL + MKDN",                 q(fw+" AND hivol=1 AND wyc_phase='MKDN'")))
R.append(line("4. + HIVOL + MKDN + T-prior=NO",    q(fw+" AND hivol=1 AND wyc_phase='MKDN' AND t_prior3=0")))
R.append("\n  >> Each step should keep med5% rising if the edge is real (not just a fat tail).")

# ---------- B) EB+VX-PS-R2L HIVOL ----------
R.append("\n\n"+"="*112)
R.append("B.  EB + VX-PS-R2L  HIVOL  —  most BALANCED bottom signal (high win + high pivLO)")
R.append("="*112)
cw="combo='EB  +  VX-PS-R2L'"
R.append(head())
R.append(line("ALL",            q(cw)))
R.append(line("HIVOL",          q(cw+" AND hivol=1")))
R.append(line("HIVOL + T-prior=NO", q(cw+" AND hivol=1 AND t_prior3=0")))
R.append("\n  phase split (HIVOL):")
R.append(head())
for ph in con.execute(f"SELECT wyc_phase,COUNT(*) c FROM m WHERE {cw} AND hivol=1 GROUP BY 1 HAVING COUNT(*)>=100 ORDER BY c DESC").fetchdf()["wyc_phase"]:
    R.append(line(ph, q(cw+f" AND hivol=1 AND wyc_phase='{ph}'")))
R.append("\n  Note: EB = wick on BOTH sides (volatility/absorption bar). High pivLO means it pins")
R.append("  the local low well; win5 ~50% makes it tradeable as a swing entry rather than a tail bet.")

# ---------- C) EU+VX-PB-R2H HIVOL short ----------
R.append("\n\n"+"="*112)
R.append("C.  EU + VX-PB-R2H  HIVOL  —  practically a SHORT signal (top reversal)")
R.append("="*112)
cw="combo='EU  +  VX-PB-R2H'"
R.append(head())
R.append(line("ALL",                 q(cw)))
R.append(line("HIVOL",               q(cw+" AND hivol=1")))
R.append(line("HIVOL + Z-prior=NO",  q(cw+" AND hivol=1 AND z_prior3=0")))
R.append(line("HIVOL + Z-prior=YES", q(cw+" AND hivol=1 AND z_prior3=1")))
R.append("\n  phase split (HIVOL):")
R.append(head())
for ph in con.execute(f"SELECT wyc_phase,COUNT(*) c FROM m WHERE {cw} AND hivol=1 GROUP BY 1 HAVING COUNT(*)>=100 ORDER BY c DESC").fetchdf()["wyc_phase"]:
    R.append(line(ph, q(cw+f" AND hivol=1 AND wyc_phase='{ph}'")))
R.append("\n  For a SHORT, the relevant edge is revDN% (top flip) and NEGATIVE +5d%. Higher |negative|")
R.append("  +5d with high revDN => stronger short. Z-prior context mirrors the bullish T-prior finding.")

# broad bearish family stacked
R.append("\n\n[ALL overbought-reversal bars: line5 LIKE '%R2H']")
R.append(head())
fw="bar_line5 LIKE '%R2H'"
R.append(line("1. ALL R2H",                       q(fw)))
R.append(line("2. + HIVOL",                        q(fw+" AND hivol=1")))
R.append(line("3. + HIVOL + MARKUP",               q(fw+" AND hivol=1 AND wyc_phase='MARKUP'")))
R.append(line("4. + HIVOL + MARKUP + Z-prior=NO",  q(fw+" AND hivol=1 AND wyc_phase='MARKUP' AND z_prior3=0")))

R.append("\n\n"+"="*112)
R.append("NOTES")
R.append("-"*112)
R.append("  - Triple filter shrinks N fast; rows with small N are noisier — weigh by N.")
R.append("  - For bullish use revUP%/+5d>0/pivLO%; for bearish use revDN%/+5d<0/pivHI%.")
R.append("  - T-prior=NO (bullish) and Z-prior=NO (bearish) = the reversal bar is FRESH, not late.")
R.append("="*112)
open(OUT,"w").write("\n".join(R)); print("written",OUT,os.path.getsize(OUT))
