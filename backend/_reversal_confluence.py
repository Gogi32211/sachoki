"""
LAYER 2 — CONFLUENCE + SEQUENCE of the top reversal signals (swing-based).
Unit = confirmed pivots; outcome = fwd_swing_ret_3 (% to next pivot). NO day windows.
  A) Confluence at LL lows : pairs/triples of top BOTTOM signals firing on the SAME bar.
  B) Confluence at HH highs: pairs of top TOP signals (shorts).
  C) Sequence at LL lows   : does a bottom signal work AT the low vs fired in prior 1-2 bars?
Metrics: n, swg_med, win20 (% swing>=+/-20%), win30, lift vs baseline.
"""
import duckdb, datetime as dt, itertools
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_CONFLUENCE_260528.txt"
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

BOT=['pb_stop_cause','sig_bias_dn','wyc_in_tr','sig_bc','wyc_sos','d_spring',
     'd_absorb_bull','sig_l88','d_blast_bull_red','rsi_le_35','d_surge_bull_red','d_flip_bull']
TOP=['sig_vol_20x','sig_vol_10x','sig_sc','d_blast_bear_grn','vix_range','sig_vol_5x',
     'd_upthrust','wvf_spike','sig_260308','d_absorb_bear','sig_dd_dn_green']

con.execute("""CREATE TABLE lows AS SELECT * FROM s.bars
               WHERE is_pivot_low_3=1 AND swing_type_3='LL' AND isfinite(fwd_swing_ret_3)""")
con.execute("""CREATE TABLE highs AS SELECT * FROM s.bars
               WHERE is_pivot_high_3=1 AND swing_type_3='HH' AND isfinite(fwd_swing_ret_3)""")
nlow=con.execute("SELECT COUNT(*) FROM lows").fetchone()[0]
nhigh=con.execute("SELECT COUNT(*) FROM highs").fetchone()[0]
bl=con.execute("SELECT MEDIAN(fwd_swing_ret_3) FROM lows").fetchone()[0]
bh=con.execute("SELECT MEDIAN(fwd_swing_ret_3) FROM highs").fetchone()[0]

def stat(tbl, where, sign):
    th20=f"fwd_swing_ret_3>20" if sign>0 else "fwd_swing_ret_3<-20"
    th30=f"fwd_swing_ret_3>30" if sign>0 else "fwd_swing_ret_3<-30"
    r=con.execute(f"""SELECT COUNT(*) n, MEDIAN(fwd_swing_ret_3) m,
        AVG(CASE WHEN {th20} THEN 1.0 ELSE 0 END)*100 w20,
        AVG(CASE WHEN {th30} THEN 1.0 ELSE 0 END)*100 w30
        FROM {tbl} WHERE {where}""").fetchone()
    return r  # n,m,w20,w30

def line(label,r,base):
    n,m,w20,w30=r
    if not n: return f"{label[:30].ljust(30)}  (0)"
    return (label[:30].ljust(30)+f"{int(n):>8,}"+f"{m:>8.1f}"+f"{w20:>7.0f}"+f"{w30:>7.0f}"+f"{(m-base):>+7.1f}")
HEAD="combo                                  n swg_med win20 win30   lift"

R=[]
R.append("="*74)
R.append("LAYER 2 — REVERSAL CONFLUENCE + SEQUENCE (swing-based, pivot 3-3)")
R.append("="*74)
R.append(f"Generated: {dt.datetime.now():%Y-%m-%d %H:%M} | LL lows={nlow:,} (base swg +{bl:.1f}%) | HH highs={nhigh:,} (base {bh:.1f}%)")
R.append("swg_med=median swing %; win20/30=% reaching +/-20/30%; lift=swg_med - baseline.")

# ---- A: confluence pairs at bottoms ----
R.append("\n\n"+"="*74); R.append("A. BOTTOM CONFLUENCE — pairs on the SAME LL low (top 20 by win20, N>=150)"); R.append("="*74)
R.append(HEAD); R.append("-"*len(HEAD))
res=[]
for a,b in itertools.combinations(BOT,2):
    r=stat("lows",f"{a}=1 AND {b}=1",+1)
    if r[0] and r[0]>=150: res.append((f"{a} + {b}",r))
res.sort(key=lambda x:-x[1][2])
for lab,r in res[:20]: R.append(line(lab,r,bl))

R.append("\n  >> Best TRIPLES (among d_spring/rsi_le_35/wyc_in_tr/d_absorb_bull/pb_stop_cause/sig_bias_dn, N>=80):")
R.append(HEAD); R.append("-"*len(HEAD))
core=['d_spring','rsi_le_35','wyc_in_tr','d_absorb_bull','pb_stop_cause','sig_bias_dn']
tri=[]
for a,b,c in itertools.combinations(core,3):
    r=stat("lows",f"{a}=1 AND {b}=1 AND {c}=1",+1)
    if r[0] and r[0]>=80: tri.append((f"{a}+{b}+{c}",r))
tri.sort(key=lambda x:-x[1][2])
for lab,r in tri[:12]: R.append(line(lab,r,bl))

# ---- B: confluence pairs at tops ----
R.append("\n\n"+"="*74); R.append("B. TOP CONFLUENCE — pairs on the SAME HH high (top 20 by win20, N>=120)"); R.append("="*74)
R.append(HEAD); R.append("-"*len(HEAD))
res=[]
for a,b in itertools.combinations(TOP,2):
    r=stat("highs",f"{a}=1 AND {b}=1",-1)
    if r[0] and r[0]>=120: res.append((f"{a} + {b}",r))
res.sort(key=lambda x:-x[1][2])
for lab,r in res[:20]: R.append(line(lab,r,bh))

# ---- C: sequence (at-low vs prior bars) ----
R.append("\n\n"+"="*74); R.append("C. SEQUENCE — bottom signal AT the low vs fired in prior 1-2 bars"); R.append("="*74)
R.append("For each signal: [AT low] sig@bar0 ; [prior] sig@bar-1 or -2 (not nec. at low) ;")
R.append("[window] sig fired on any of bar-2..bar0. Shows if the signal is timing-precise.")
lagcols=BOT[:8]
sel=["is_pivot_low_3 ipl","swing_type_3 st","fwd_swing_ret_3 fwd"]
for c in lagcols:
    sel+= [f"{c} {c}_0", f"LAG({c},1) OVER w {c}_1", f"LAG({c},2) OVER w {c}_2"]
con.execute(f"CREATE TABLE sb AS SELECT {', '.join(sel)} FROM s.bars WINDOW w AS (PARTITION BY ticker ORDER BY date)")
def cstat(where,sign=1):
    r=con.execute(f"""SELECT COUNT(*) n, MEDIAN(fwd) m,
       AVG(CASE WHEN fwd>20 THEN 1.0 ELSE 0 END)*100 w20, AVG(CASE WHEN fwd>30 THEN 1.0 ELSE 0 END)*100 w30
       FROM sb WHERE ipl=1 AND st='LL' AND isfinite(fwd) AND {where}""").fetchone()
    return r
R.append("\n"+HEAD); R.append("-"*len(HEAD))
for c in lagcols:
    R.append(line(f"{c}  [AT low]", cstat(f"{c}_0=1"), bl))
    R.append(line(f"{c}  [prior -1/-2]", cstat(f"({c}_1=1 OR {c}_2=1) AND {c}_0=0"), bl))
    R.append(line(f"{c}  [window -2..0]", cstat(f"({c}_0=1 OR {c}_1=1 OR {c}_2=1)"), bl))
    R.append("")

R.append("="*74); R.append("NOTES"); R.append("-"*74)
R.append("  - Confluence rarer => smaller N; weigh win20/win30 with N. lift vs LL/HH baseline.")
R.append("  - Sequence: if [window] keeps high win20 ~ [AT low], the signal allows earlier entry;")
R.append("    if [prior] alone is weak, the signal must coincide with the low to matter.")
R.append("="*74)
open(OUT,"w").write("\n".join(R))
import os; print("written",OUT,os.path.getsize(OUT))
