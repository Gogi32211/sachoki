import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, numpy as np, pandas as pd
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)

def run(o,h,l,cl, fixed):
    n=len(o)
    top = h[0] if fixed else 0.0
    bot = l[0] if fixed else 0.0
    is_bull_pb=is_bear_pb=False; pot_top=pot_bot=0.0; bull_i=bear_i=0
    plus=np.zeros(n,bool); minus=np.zeros(n,bool)
    lvl_p=np.full(n,np.nan); lvl_m=np.full(n,np.nan)
    for i in range(1,n):
        if cl[i-1]>o[i-1] and not is_bear_pb: is_bear_pb=True; pot_top=o[i-1]; bull_i=i-1
        if cl[i-1]<o[i-1] and not is_bull_pb: is_bull_pb=True; pot_bot=o[i-1]; bear_i=i-1
        if is_bull_pb:
            if o[i]<pot_bot: pot_bot=o[i]; bear_i=i
            if cl[i]<o[i] and o[i]>pot_bot: pot_bot=o[i]; bear_i=i
        if is_bear_pb:
            if o[i]>pot_top: pot_top=o[i]; bull_i=i
            if cl[i]>o[i] and o[i]<pot_top: pot_top=o[i]; bull_i=i
        if l[i] < bot:
            bot=l[i]
            if is_bear_pb and (i-bull_i)!=0:
                is_bear_pb=False; plus[i]=True; lvl_p[i]=pot_top
            elif cl[i-1]>o[i-1] and cl[i]<o[i]:
                is_bear_pb=False; plus[i]=True; lvl_p[i]=pot_top
        if h[i] > top:
            top=h[i]
            if is_bull_pb and (i-bear_i)!=0:
                is_bull_pb=False; minus[i]=True; lvl_m[i]=pot_bot
            elif cl[i-1]<o[i-1] and cl[i]>o[i]:
                is_bull_pb=False; minus[i]=True; lvl_m[i]=pot_bot
    return plus,minus,lvl_p,lvl_m

tks=[r[0] for r in c.execute("SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker").fetchall()]
res={False:{'p':0,'m':0,'pp':0}, True:{'p':0,'m':0,'pp':0}}
rows=[]
for t in tks:
    d=c.execute("""SELECT date,open,high,low,close,fwd_5d FROM bars
                   WHERE ticker=? AND universe='sp500' ORDER BY date""",[t]).fetchdf()
    if len(d)<60: continue
    o,h,l,cl=[d[x].values.astype(float) for x in ('open','high','low','close')]
    f=d.fwd_5d.values
    for fixed in (False,True):
        p,m,lp,lm=run(o,h,l,cl,fixed)
        res[fixed]['p']+=int(p.sum()); res[fixed]['m']+=int(m.sum())
        ev=[('P' if p[i] else 'M') for i in range(len(p)) if p[i] or m[i]]
        res[fixed]['pp']+=sum(1 for i in range(1,len(ev)) if ev[i-1]=='P' and ev[i]=='P')
        if fixed:
            for i in np.where(p)[0]:
                if not np.isnan(f[i]): rows.append(('+CISD',f[i],d.date.iloc[i]))
            for i in np.where(m)[0]:
                if not np.isnan(f[i]): rows.append(('-CISD',f[i],d.date.iloc[i]))
print(f"{'':<10}{'+CISD':>9}{'-CISD':>9}{'P then P':>11}")
print(f"{'as-is':<10}{res[False]['p']:>9,}{res[False]['m']:>9,}{res[False]['pp']:>11,}")
print(f"{'FIXED':<10}{res[True]['p']:>9,}{res[True]['m']:>9,}{res[True]['pp']:>11,}")
r=pd.DataFrame(rows,columns=['sig','fwd','date']); r['yr']=pd.to_datetime(r.date).dt.year
base=c.execute("SELECT MEDIAN(fwd_5d) FROM bars WHERE universe='sp500' AND fwd_5d IS NOT NULL").fetchone()[0]
print(f"\nFIXED engine, forward 5d — baseline {base:+.3f}%")
for s in ('+CISD','-CISD'):
    g=r[r.sig==s]; y=g.groupby('yr')['fwd'].median()
    print(f"  {s:<8} n={len(g):>7,}  median {g.fwd.median():+.3f}%  Δ {g.fwd.median()-base:+.3f}"
          f"  yrs+ {(y>0).sum()}/{len(y)}  worst {y.min():+.2f}")
