"""l46_l34_prebreakout.py — test the user's observation: L46 (supply) & L34 (demand)
cluster BEFORE breakouts, in sequence. Honest test = pre-breakout frequency vs the
GLOBAL base rate (L46=23.7%, L34=9.7%). Breakout = close > prior-20d-high & up bar.
Also: sequence position, and does an L34 right before the break = bigger move?
ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
W=20  # pre-breakout window
a=get_analytics_conn(read_only=True)
BASE=dict(a.execute("""SELECT l_sig, 1.0*count(*)/sum(count(*)) over() FROM bars
   WHERE l_sig IS NOT NULL GROUP BY l_sig""").fetchall())
b46, b34 = BASE.get('L46',0)*100, BASE.get('L34',0)*100
df=a.execute("""SELECT ticker,universe,date,high,low,close,open,l_sig,close AS px,fwd_10d
   FROM bars ORDER BY ticker,universe,date""").fetchdf(); a.close()
df["L46"]=(df.l_sig=="L46").astype(np.int8)
df["L34"]=(df.l_sig=="L34").astype(np.int8)

rows46=[]; rows34=[]; seqs=[]; fwd_with=[]; fwd_without=[]; lastL34=[]; lastL46=[]
small_rows=[]  # low-price (<$5) proxy for pump-prone names
for (tk,uni),g in df.groupby(["ticker","universe"],sort=False):
    n=len(g)
    if n < W+5: continue
    hi=g.high.to_numpy(); cl=g.close.to_numpy(); op=g.open.to_numpy()
    L46=g.L46.to_numpy(); L34=g.L34.to_numpy(); px=g.px.to_numpy(); f10=g.fwd_10d.to_numpy()
    # prior-20d high (exclusive of current bar)
    prior_hi=pd.Series(hi).rolling(W).max().shift(1).to_numpy()
    for t in range(W, n):
        if not (cl[t] > prior_hi[t] and cl[t] > op[t]): continue   # breakout bar
        win46=L46[t-W:t]; win34=L34[t-W:t]
        rows46.append(win46.mean()*100); rows34.append(win34.mean()*100)
        # last occurrence (bars before breakout) within window
        p34=np.where(win34==1)[0]; p46=np.where(win46==1)[0]
        if len(p34): lastL34.append(W-p34[-1])
        if len(p46): lastL46.append(W-p46[-1])
        # sequence: any L46 earlier than the last L34 (supply→demand) in window
        if len(p34) and len(p46) and p46[0] < p34[-1]: seqs.append(1)
        else: seqs.append(0)
        # forward conditioning: L34 in last 5 bars before breakout?
        if L34[t-5:t].sum() >= 1: fwd_with.append(f10[t])
        else: fwd_without.append(f10[t])
        if px[t] < 5: small_rows.append((win46.mean()*100, win34.mean()*100))

def clean(x):
    x=np.array(x,dtype=float); return x[np.isfinite(x)]
nbrk=len(rows46)
print(f"### L46/L34 frequency in the {W} bars BEFORE a breakout  (n={nbrk:,} breakouts)")
print(f"  base rate (ALL bars):   L46 {b46:.1f}%   L34 {b34:.1f}%")
print(f"  pre-breakout window:    L46 {np.mean(rows46):.1f}%   L34 {np.mean(rows34):.1f}%")
print(f"  → LIFT vs base:         L46 {np.mean(rows46)-b46:+.1f}pp   L34 {np.mean(rows34)-b34:+.1f}pp")
if small_rows:
    s=np.array(small_rows); print(f"  low-price <$5 (pump proxy): L46 {s[:,0].mean():.1f}%  L34 {s[:,1].mean():.1f}%  (n={len(s):,})")
print(f"\n### SEQUENCE & POSITION")
print(f"  % breakouts with L46→L34 (supply-then-demand) in window: {np.mean(seqs)*100:.1f}%")
print(f"  avg bars-before-breakout of LAST L34: {np.mean(lastL34):.1f}   LAST L46: {np.mean(lastL46):.1f}")
print(f"  breakouts with >=1 L34 in last 5 bars: {len(fwd_with):,}/{nbrk:,} ({100*len(fwd_with)/nbrk:.0f}%)")
fw, fo = clean(fwd_with), clean(fwd_without)
print(f"\n### DOES L34-before-breakout = BIGGER move? (forward 10d)")
print(f"  breakout WITH L34 in last 5 bars:  median {np.median(fw):+.2f}%  mean {np.mean(np.clip(fw,-25,25)):+.2f}%  n={len(fw):,}")
print(f"  breakout WITHOUT:                  median {np.median(fo):+.2f}%  mean {np.mean(np.clip(fo,-25,25)):+.2f}%  n={len(fo):,}")
print("\ndone")
