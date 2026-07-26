"""
4BF (ULTRA v2 break-fail buy/sell) reversal analysis.
  4BF up = bf_buy=1 ; 4BF down = bf_sell=1.
Per-bar 4BF state: U (buy only), D (sell only), B (both), N (neither).
Same metrics as composite report (pivot 3-3): N, HH%, HL%, gainHH, ddHL, f5med, f5avg, win5.
DIRECTION KEY: HIGH HL% + +f5med = bullish base ; HIGH HH% + -f5med = topping.
Universe: all 3.  Min N 200 for chains.
"""
import duckdb, datetime as dt, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_4BF_260528.txt"
MINN=200
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

print("building 4BF state base...")
con.execute("""
CREATE TABLE b AS
WITH x AS (
  SELECT ticker, date, bf_buy, bf_sell, bar_line5, composite_full_suffix,
         CAST(next_pivot_is_hh_3 AS INT) hh, CAST(next_pivot_is_hl_3 AS INT) hl,
         pct_to_next_hh_3 pHH, pct_to_next_hl_3 pHL, fwd_5d,
         CASE WHEN bf_buy=1 AND bf_sell=1 THEN 'B'
              WHEN bf_buy=1 THEN 'U'
              WHEN bf_sell=1 THEN 'D' ELSE 'N' END AS st
  FROM s.bars
)
SELECT *,
  LAG(st,1) OVER w s1, LAG(st,2) OVER w s2, LAG(st,3) OVER w s3,
  ROW_NUMBER() OVER w rn
FROM x WINDOW w AS (PARTITION BY ticker ORDER BY date)
""")
N=con.execute("SELECT COUNT(*) FROM b WHERE rn>=4").fetchone()[0]
print("usable rows:",N)

AGG="""
  COUNT(*) n, SUM(hh) hh, SUM(hl) hl,
  ROUND(AVG(CASE WHEN isfinite(pHH) AND ABS(pHH)<200 THEN pHH END),2) gainHH,
  ROUND(AVG(CASE WHEN isfinite(pHL) AND ABS(pHL)<200 THEN pHL END),2) ddHL,
  ROUND(MEDIAN(CASE WHEN isfinite(fwd_5d) THEN fwd_5d END),3) f5med,
  ROUND(AVG(CASE WHEN isfinite(fwd_5d) AND ABS(fwd_5d)<=100 THEN fwd_5d END),3) f5avg,
  ROUND(100.0*AVG(CASE WHEN isfinite(fwd_5d) AND fwd_5d>0 THEN 1.0 WHEN isfinite(fwd_5d) THEN 0.0 END),1) win5
"""
def hhp(r):
    d=(r['hh'] or 0)+(r['hl'] or 0); return round((r['hh'] or 0)/d*100,1) if d else 0.0
def hlp(r):
    d=(r['hh'] or 0)+(r['hl'] or 0); return round((r['hl'] or 0)/d*100,1) if d else 0.0
CW=[30,9,7,7,8,8,8,8,7]
HD=["GROUP","N","HH%","HL%","gainHH","ddHL","f5med","f5avg","win5"]
def header(lbl="GROUP"):
    HD[0]=lbl
    h="".join(x.ljust(CW[0]) if i==0 else x.rjust(CW[i]) for i,x in enumerate(HD)); return h+"\n"+"-"*len(h)
def fmt(label,r):
    cells=[str(label)[:CW[0]].ljust(CW[0])]
    vals=[int(r['n']),hhp(r),hlp(r),r['gainHH'],r['ddHL'],r['f5med'],r['f5avg'],r['win5']]
    for i,v in enumerate(vals,1):
        cells.append(("-" if v is None else (f"{int(v):,}" if i==1 else f"{v:.2f}")).rjust(CW[i]))
    return "".join(cells)
def one(where,label):
    r=con.execute(f"SELECT {AGG} FROM b WHERE rn>=4 AND {where}").fetchdf().iloc[0]
    return fmt(label,r) if (r['n'] or 0)>0 else f"{label[:CW[0]].ljust(CW[0])}  (0 rows)"

R=[]
R.append("="*100)
R.append("4BF (ULTRA v2 break-fail) REVERSAL ANALYSIS — single bar, 4-bar chains, alternation")
R.append("="*100)
R.append(f"Generated: {dt.datetime.now():%Y-%m-%d %H:%M} | pivot 3-3 | usable rows {N:,} | all 3 universes")
R.append("4BF state per bar: U=4BF-up only, D=4BF-down only, B=both, N=neither.")
R.append("DIRECTION KEY: HIGH HL% + +f5med = bullish base ; HIGH HH% + -f5med = topping/exhaustion.")

# baseline
R.append("\n\n"+"="*100); R.append("0. BASELINE + SINGLE-BAR 4BF"); R.append("="*100)
R.append(header("signal @ bar0"))
R.append(one("1=1","ALL BARS"))
R.append(one("st='U'","4BF-up  (bf_buy=1)"))
R.append(one("st='D'","4BF-down (bf_sell=1)"))
R.append(one("st='B'","4BF both (buy&sell)"))

# co-occurrence with line5 at bar0
R.append("\n  4BF-up @ bar0, split by bar0 line5 (top 12 by N):")
R.append(header("4BF-up + line5"))
d=con.execute(f"SELECT bar_line5 g,{AGG} FROM b WHERE rn>=4 AND st='U' AND bar_line5<>'' GROUP BY 1 HAVING COUNT(*)>=1000 ORDER BY n DESC LIMIT 12").fetchdf()
for _,r in d.iterrows(): R.append(fmt(r['g'],r))
R.append("\n  4BF-down @ bar0, split by bar0 line5 (top 12 by N):")
R.append(header("4BF-down + line5"))
d=con.execute(f"SELECT bar_line5 g,{AGG} FROM b WHERE rn>=4 AND st='D' AND bar_line5<>'' GROUP BY 1 HAVING COUNT(*)>=1000 ORDER BY n DESC LIMIT 12").fetchdf()
for _,r in d.iterrows(): R.append(fmt(r['g'],r))

# ---------- 4-bar state chains ----------
R.append("\n\n"+"="*100); R.append("1. 4BF 4-BAR STATE CHAINS (b-3 -> b-2 -> b-1 -> b0)"); R.append("="*100)
def chains(st0, sort_metric):
    q=f"SELECT s3,s2,s1,{AGG} FROM b WHERE rn>=4 AND st='{st0}' GROUP BY s3,s2,s1 HAVING COUNT(*)>={MINN}"
    df=con.execute(q).fetchdf()
    df["_k"]=df.apply(hhp if sort_metric=="HH" else hlp,axis=1)
    df=df.sort_values("_k",ascending=False).head(15)
    out=[header("chain -> b0")]
    for _,r in df.iterrows():
        out.append(fmt(f"{r['s3']}{r['s2']}{r['s1']}->{st0}",r))
    return out
R.append("\n[bar0=4BF-up (U) — top 15 prior-state chains by HL% (bullish base)]")
R+=chains("U","HL")
R.append("\n[bar0=4BF-down (D) — top 15 prior-state chains by HH% (topping)]")
R+=chains("D","HH")

# ---------- alternation ----------
R.append("\n\n"+"="*100); R.append("2. ALTERNATION — what happens when 4BF up/down flips over recent bars"); R.append("="*100)
R.append("flips = # of adjacent U<->D switches within the 4-bar window (b-3..b0). More flips = choppier.")
# flips expression
FLIP=("( (CASE WHEN (s3='U' AND s2='D') OR (s3='D' AND s2='U') THEN 1 ELSE 0 END)"
      " + (CASE WHEN (s2='U' AND s1='D') OR (s2='D' AND s1='U') THEN 1 ELSE 0 END)"
      " + (CASE WHEN (s1='U' AND st='D') OR (s1='D' AND st='U') THEN 1 ELSE 0 END) )")
R.append("\n[bar0=4BF-up (U), grouped by # U<->D flips in window]")
R.append(header("U @ b0 | flips"))
d=con.execute(f"SELECT {FLIP} flips,{AGG} FROM b WHERE rn>=4 AND st='U' GROUP BY 1 ORDER BY 1").fetchdf()
for _,r in d.iterrows(): R.append(fmt(f"flips={int(r['flips'])}",r))
R.append("\n[bar0=4BF-down (D), grouped by # U<->D flips in window]")
R.append(header("D @ b0 | flips"))
d=con.execute(f"SELECT {FLIP} flips,{AGG} FROM b WHERE rn>=4 AND st='D' GROUP BY 1 ORDER BY 1").fetchdf()
for _,r in d.iterrows(): R.append(fmt(f"flips={int(r['flips'])}",r))

# explicit named alternation / run / flip patterns
R.append("\n[Named 4BF patterns (b-3 -> b-2 -> b-1 -> b0)]")
R.append(header("pattern"))
pats=[("U","U","U","U","run-up momentum"),
      ("D","D","D","D","run-down momentum"),
      ("U","D","U","D","alternating end-D"),
      ("D","U","D","U","alternating end-U"),
      ("D","D","D","U","fresh flip UP after down-run"),
      ("U","U","U","D","fresh flip DOWN after up-run"),
      ("D","D","U","U","down->up turn"),
      ("U","U","D","D","up->down turn")]
for a,b2,c,d2,name in pats:
    r=con.execute(f"SELECT {AGG} FROM b WHERE rn>=4 AND s3='{a}' AND s2='{b2}' AND s1='{c}' AND st='{d2}'").fetchdf().iloc[0]
    lab=f"{a}{b2}{c}{d2}  {name}"
    R.append(fmt(lab,r) if (r['n'] or 0)>0 else f"{lab[:CW[0]].ljust(CW[0])}  (0 rows)")

R.append("\n\n"+"="*100); R.append("NOTES"); R.append("-"*100)
R.append("  - 4BF = ULTRA v2 break-fail-out signal (bf_buy / bf_sell). Often fires consecutively.")
R.append("  - flips counts only direct U<->D switches; N (neither) bars don't add a flip.")
R.append("  - Compare run-momentum vs alternation: does chop (high flips) kill the edge?")
R.append("="*100)
open(OUT,"w").write("\n".join(R)); print("written",OUT,os.path.getsize(OUT),"bytes")
