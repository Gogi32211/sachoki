"""
DATA-DRIVEN REVERSAL SIGNAL SCREEN.
Unit of analysis = confirmed Williams 3-3 pivots (the actual turning points).
  Bullish bottoms : pivot LOW with swing_type_3='LL'  (a lower-low = downtrend bottom)
                    outcome = fwd_swing_ret_3 (% rally to the NEXT pivot high)
  Bearish tops    : pivot HIGH with swing_type_3='HH' (a higher-high = uptrend top)
                    outcome = fwd_swing_ret_3 (% drop to the NEXT pivot low; negative)

For EVERY binary signal (value AT the pivot bar) we measure, vs the LL/HH baseline:
  n         = how many such pivots had the signal on
  fwd_med   = MEDIAN forward swing return  (robust; this IS the reversal size)
  big%      = P(forward swing > +20%)  for bottoms  /  P(< -15%) for tops
  lift_med  = fwd_med - baseline_med
This surfaces which signals genuinely mark the best reversals — let all 216 compete.
"""
import duckdb, datetime as dt
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_SIGNAL_SCREEN_260528.txt"
MINN=300
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

# discover binary signal columns (exclude structural/leakage cols)
info=con.execute("PRAGMA table_info('s.bars')").fetchdf() if False else con.execute("SELECT * FROM (DESCRIBE s.bars)").fetchdf()
cols=info['column_name'].tolist() if 'column_name' in info else info[info.columns[0]].tolist()
EXCLUDE={'is_pivot_low_3','is_pivot_high_3','is_pivot_low_5','is_pivot_high_5',
         'next_pivot_is_hl_3','next_pivot_is_hh_3','next_pivot_is_hl_5','next_pivot_is_hh_5'}
def is_lookahead(c):
    cl=c.lower()
    # forward-outcome target flags: NOT real-time signals (price already hit X / dropped X ahead)
    return cl.startswith('hit_') or cl.startswith('drop_') or cl.startswith('fwd_') or '_2x_' in cl
sigs=[]
for c in cols:
    if c in EXCLUDE or is_lookahead(c): continue
    try:
        mn,mx,ones,nn=con.execute(f"SELECT MIN({c}),MAX({c}),SUM(CASE WHEN {c}=1 THEN 1 ELSE 0 END),COUNT({c}) FROM s.bars").fetchone()
    except: continue
    if mn is not None and mn>=0 and mx==1 and nn>0:
        rate=ones/nn
        if 0.0005<=rate<=0.5: sigs.append(c)
print("signals to screen:",len(sigs))

# build pivot subsets in memory
con.execute("""CREATE TABLE lows AS SELECT * FROM s.bars
               WHERE is_pivot_low_3=1 AND swing_type_3='LL' AND isfinite(fwd_swing_ret_3)""")
con.execute("""CREATE TABLE highs AS SELECT * FROM s.bars
               WHERE is_pivot_high_3=1 AND swing_type_3='HH' AND isfinite(fwd_swing_ret_3)""")
nlow=con.execute("SELECT COUNT(*) FROM lows").fetchone()[0]
nhigh=con.execute("SELECT COUNT(*) FROM highs").fetchone()[0]
base_low=con.execute("SELECT MEDIAN(fwd_swing_ret_3), AVG(CASE WHEN fwd_swing_ret_3>20 THEN 1.0 ELSE 0 END)*100, AVG(CASE WHEN fwd_swing_ret_3>10 THEN 1.0 ELSE 0 END)*100 FROM lows").fetchone()
base_high=con.execute("SELECT MEDIAN(fwd_swing_ret_3), AVG(CASE WHEN fwd_swing_ret_3<-20 THEN 1.0 ELSE 0 END)*100, AVG(CASE WHEN fwd_swing_ret_3<-10 THEN 1.0 ELSE 0 END)*100 FROM highs").fetchone()
print(f"LL lows={nlow:,} base_med={base_low[0]:.2f} | HH highs={nhigh:,} base_med={base_high[0]:.2f}")

def screen(tbl, sign, base_med):
    # sign=+1 bottoms (swing up): win@ +10/+20/+30 ; sign=-1 tops: win@ -10/-20/-30
    def th(x): return f"fwd_swing_ret_3>{x}" if sign>0 else f"fwd_swing_ret_3<{-x}"
    parts=[]
    for c in sigs:
        parts.append(f"SUM(CASE WHEN {c}=1 THEN 1 ELSE 0 END) AS n_{c}")
        parts.append(f"MEDIAN(CASE WHEN {c}=1 THEN fwd_swing_ret_3 END) AS m_{c}")
        for x in (10,20,30):
            parts.append(f"AVG(CASE WHEN {c}=1 AND {th(x)} THEN 1.0 WHEN {c}=1 THEN 0.0 END)*100 AS w{x}_{c}")
    row=con.execute(f"SELECT {', '.join(parts)} FROM {tbl}").fetchdf().iloc[0]
    out=[]
    for c in sigs:
        n=int(row[f"n_{c}"] or 0)
        if n<MINN: continue
        m=row[f"m_{c}"]
        if m is None: continue
        out.append((c,n,round(float(m),2),
                    round(float(row[f"w10_{c}"]),1),round(float(row[f"w20_{c}"]),1),round(float(row[f"w30_{c}"]),1),
                    round(float(m)-base_med,2)))
    return out

print("screening bottoms...")
low_res=screen("lows",+1, base_low[0])
print("screening tops...")
high_res=screen("highs",-1, base_high[0])

def tbl(res, sort_desc=True, top=30):
    res=sorted(res, key=lambda x:x[6], reverse=sort_desc)[:top]
    W=[24,8,8,7,7,7,8]
    H=["signal","n","swg_med","win10","win20","win30","lift"]
    head="".join(h.ljust(W[0]) if i==0 else h.rjust(W[i]) for i,h in enumerate(H))
    lines=[head,"-"*len(head)]
    for c,n,m,w10,w20,w30,l in res:
        lines.append(c.ljust(W[0])+f"{n:,}".rjust(W[1])+f"{m:.1f}".rjust(W[2])
                     +f"{w10:.0f}".rjust(W[3])+f"{w20:.0f}".rjust(W[4])+f"{w30:.0f}".rjust(W[5])+f"{l:+.1f}".rjust(W[6]))
    return "\n".join(lines)

R=[]
R.append("="*82)
R.append("DATA-DRIVEN REVERSAL SIGNAL SCREEN — all binary signals ranked by reversal lift")
R.append("="*82)
R.append(f"Generated: {dt.datetime.now():%Y-%m-%d %H:%M} | pivot 3-3 | all 3 universes | {len(sigs)} signals screened")
R.append("Unit = confirmed pivots. Outcome = fwd_swing_ret_3 (% move to NEXT pivot — STRUCTURAL,")
R.append("NOT day-based). All metrics are swing %, never fixed 1d/5d/10d windows. Min N "+str(MINN)+".")
R.append("")
R.append(f"BULLISH BOTTOMS  : LL pivot lows, n={nlow:,}.  Baseline swing_med=+{base_low[0]:.1f}%  win10={base_low[2]:.0f}%  win20={base_low[1]:.0f}%")
R.append(f"BEARISH TOPS     : HH pivot highs, n={nhigh:,}.  Baseline swing_med={base_high[0]:.1f}%  win10={base_high[2]:.0f}%  win20={base_high[1]:.0f}%")
R.append("swg_med = median swing %; win10/20/30 = % of pivots whose swing reaches +/-10/20/30%;")
R.append("lift = signal swg_med minus baseline. Bottoms: bigger +lift & higher win = stronger rally.")

R.append("\n\n"+"="*82)
R.append("A. TOP 30 BULLISH-BOTTOM SIGNALS (biggest rally lift above LL baseline)")
R.append("="*82)
R.append(tbl(low_res, sort_desc=True, top=30))
R.append("\n  >> WORST 12 (signals that mark FAILED/weak LL lows — avoid as longs):")
R.append(tbl(low_res, sort_desc=False, top=12))

R.append("\n\n"+"="*82)
R.append("B. TOP 30 BEARISH-TOP SIGNALS (biggest drop = most NEGATIVE lift below HH baseline)")
R.append("="*82)
R.append(tbl(high_res, sort_desc=False, top=30))   # most negative lift first
R.append("\n  >> Tops that actually keep rising (weak shorts — avoid):")
R.append(tbl(high_res, sort_desc=True, top=12))

R.append("\n\n"+"="*82)
R.append("NOTES")
R.append("-"*82)
R.append("  - Signal measured AT the pivot bar (the turning bar). Confirmation lags 3 bars but the")
R.append("    signal value is known in real time -> this is the fingerprint of good reversals.")
R.append("  - fwd_swing_ret_3 is robust-median; penny extremes handled by median + big% hit-rate.")
R.append("  - NEXT LAYER (run on request): pairwise confluence of the top signals at LL lows, and")
R.append("    2-3 bar sequences leading INTO the LL (does signal X on bar-1 + Y on bar0 lift more?).")
R.append("="*82)
open(OUT,"w").write("\n".join(R))
import os; print("written",OUT,os.path.getsize(OUT),"bytes")
