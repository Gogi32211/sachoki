"""AT THE REVERSAL TURN ONLY (min-5 RSI<40, up bar, RSI rising): per score, value-range
bins with fwd15 mean + up-rate P(fwd15>+15%). Nothing else."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
a=get_analytics_conn()
have=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
extra=[c for c in ["bx_up","sig_strong","d_absorb_bull"] if c in have]
fb="final_bull_score," if "final_bull_score" in have else ""
csel=",".join(f"coalesce({c},0) {c}" for c in extra)
df=a.execute(f"""WITH r AS (SELECT ticker,date,close,rsi_14,close*volume dv,
   turbo_score,prebreak_v2,prebreak_v3,rtb_total,beta_score,{fb}{csel},
   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
   SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
fwd=[];rp=[];cp=[];m5=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); c=g.close.to_numpy(float); rs=g.rsi_14.to_numpy(float)
    f=np.full(len(c),np.nan)
    if len(c)>15: f[:-15]=c[15:]/c[:-15]-1.0
    fwd.append(pd.Series(f,index=g.index))
    rp.append(pd.Series(np.concatenate([[np.nan],rs[:-1]]),index=g.index))
    cp.append(pd.Series(np.concatenate([[np.nan],c[:-1]]),index=g.index))
    m5.append(pd.Series(pd.Series(rs).rolling(5,min_periods=2).min().to_numpy(),index=g.index))
df["fwd15"]=pd.concat(fwd).values; df["rp"]=pd.concat(rp).values
df["cp"]=pd.concat(cp).values; df["m5"]=pd.concat(m5).values
rsi=df.rsi_14.fillna(50)
osv=np.select([rsi<30,rsi<35,rsi<50,rsi<60,rsi<70],[20,15,8,0,-8],default=-18)
pzv=np.select([df.close<8,df.close<21,df.close<89],[-12,-6,10],default=3)
gc=lambda c: df[c] if c in df else 0
df["uv3"]=(12*gc("bx_up")+8*gc("sig_strong")+15*gc("d_absorb_bull")).clip(upper=25)+osv+pzv
D=df[(df.fwd15.notna())&(df.dv>=3_000_000)&(df.rsi_14.notna())&(df.cp.notna())].copy()
REV=(D.m5<40)&(D.close>D.cp)&(D.rsi_14>D.rp)
Z=D[REV]
zb_up=(Z.fwd15>0.15).mean()*100; zb_m=Z.fwd15.mean()*100
print(f"REVERSAL turns: n={len(Z)} · zone base: up-rate {zb_up:.1f}% · fwd15 mean {zb_m:+.2f}%\n")
SC=[("SCORE","final_bull_score" if "final_bull_score" in D else None),
    ("V3(prebreak)","prebreak_v3"),("UV3","uv3"),("turbo","turbo_score"),
    ("v2(prebreak)","prebreak_v2"),("rtb","rtb_total"),("beta","beta_score")]
for lab,col in SC:
    if col is None or col not in Z:
        print(f"{lab}: (SCORE ar inaxeba bars-shi — turbo-s identuria, ix. turbo)\n"); continue
    z=Z[Z[col].notna()].copy()
    if len(z)<1000: print(f"{lab}: too few\n"); continue
    try: z["_b"]=pd.qcut(z[col].rank(method='first'),8,labels=False)
    except: continue
    print(f"{lab}:")
    rows=[]
    for b_ in range(8):
        s=z[z._b==b_]
        if len(s)<150: continue
        rows.append((s[col].min(),s[col].max(),(s.fwd15>0.15).mean()*100,s.fwd15.mean()*100,len(s)))
    best=max(rows,key=lambda x:x[2])
    for lo,hi,ur,mn,n in rows:
        mk=' ←' if (lo,hi)==(best[0],best[1]) else ''
        print(f"   {lo:5.0f}–{hi:<5.0f}  up-rate {ur:4.1f}%  fwd15 {mn:+5.1f}%{mk}")
    print()
print("(ULTRA bars-shi ar inaxeba istoriulad — universe-masshtabze ver itvleba)")
