"""T*L34 → key-level test (2026-07-23) — same machinery as the T*L46 study, but for
L34 (institutional-absorption line; RED-L34 = close<open is the validated one). Tests
the bar's HIGH and LOW (the range diapason the user pointed to) + close, vs a matched
RANDOM control, split RED vs GREEN. HOLD-rate = level rejected price (acted as S/R).
"""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

TOL, AWAY, REACT, LOOKFWD = 0.004, 0.012, 3, 120

c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
df = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close>=5 AND universe<>'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker,date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END)=1
    )
    SELECT ticker, date, open, high, low, close, volume,
           coalesce(t_sig,'') tt, coalesce(l_sig,'') ll
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
o=df.open.to_numpy(float); h=df.high.to_numpy(float); lo=df.low.to_numpy(float); cl=df.close.to_numpy(float)
vol=df.volume.to_numpy(float); tt=df.tt.to_numpy(); ll=df.ll.to_numpy(); tk=df.ticker.to_numpy(); n=len(df)
is_T=np.char.startswith(tt.astype(str),'T')
idx=defaultdict(list)
for i,t in enumerate(tk): idx[t].append(i)
last={t:ii[-1] for t,ii in idx.items()}
print(f"frame {len(df):,}", flush=True)

def test_level(anchor_i, level):
    tl=last[tk[anchor_i]]; end=min(anchor_i+LOOKFWD,tl); tests=held=0; j=anchor_i+1
    while j<=end-REACT:
        straddle=lo[j]<=level*(1+TOL) and h[j]>=level*(1-TOL)
        away=abs(cl[j-1]-level)/level>=AWAY
        if straddle and away:
            tests+=1
            after=cl[j+REACT]
            if cl[j-1]>level:           # support test (approached from above)
                if after>level: held+=1
            else:                        # resistance test (from below)
                if after<level: held+=1
            j+=REACT+1
        else: j+=1
    return tests,held

def test_zone(anchor_i, hi, low):
    """Range-zone version: a test = price enters the [low,hi] band from outside; HOLD =
    rejected back to the side it came from within REACT bars."""
    tl=last[tk[anchor_i]]; end=min(anchor_i+LOOKFWD,tl); tests=held=0; j=anchor_i+1
    while j<=end-REACT:
        inzone=h[j]>=low and lo[j]<=hi
        prev_above=cl[j-1]>hi*(1+0.002); prev_below=cl[j-1]<low*(1-0.002)
        if inzone and (prev_above or prev_below):
            tests+=1; after=cl[j+REACT]
            if prev_above and after>hi: held+=1
            elif prev_below and after<low: held+=1
            j+=REACT+1
        else: j+=1
    return tests,held

def run(anchor_list, level_fn, label, zone=False):
    tt_=hh=wt=0
    for i in anchor_list:
        if zone:
            te,he=test_zone(i, h[i], lo[i])
        else:
            L=level_fn(i)
            if L is None or L<=0: continue
            te,he=test_level(i,L)
        tt_+=te; hh+=he
        if te>0: wt+=1
    hold=100*hh/tt_ if tt_ else 0; freq=100*wt/len(anchor_list) if anchor_list else 0
    print(f"  {label:28} n={len(anchor_list):5} · tested={freq:4.1f}% · avg touches={tt_/max(1,wt):.2f} · HOLD={hold:4.1f}% ({hh}/{tt_})")

anch=[i for i in range(n) if is_T[i] and ll[i]=='L34' and cl[i]*vol[i]>=3e6]
red=[i for i in anch if cl[i]<o[i]]; grn=[i for i in anch if cl[i]>=o[i]]
print(f"T*L34 anchors: {len(anch)} (red {len(red)} / green {len(grn)})\n")
rng=np.random.default_rng(34)
pool=[i for i in range(n) if cl[i]*vol[i]>=3e6 and i<last[tk[i]]-REACT]
rand=list(rng.choice(pool, min(len(anch),len(pool)), replace=False))

print("══ CLOSE level ══")
run(anch, lambda i: cl[i], "T*L34 close")
run(rand, lambda i: cl[i], "RANDOM close")
print("\n══ HIGH level (range top) ══")
run(anch, lambda i: h[i], "T*L34 high"); run(red, lambda i: h[i], "T*L34red high"); run(grn, lambda i: h[i], "T*L34grn high")
run(rand, lambda i: h[i], "RANDOM high")
print("\n══ LOW level (range bottom) ══")
run(anch, lambda i: lo[i], "T*L34 low"); run(red, lambda i: lo[i], "T*L34red low"); run(grn, lambda i: lo[i], "T*L34grn low")
run(rand, lambda i: lo[i], "RANDOM low")
print("\n══ RANGE ZONE (high↔low band) ══")
run(anch, None, "T*L34 zone", zone=True)
run(red,  None, "T*L34red zone", zone=True)
run(rand, None, "RANDOM zone", zone=True)
