import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
cols=[r[0] for r in c.execute('DESCRIBE bars').fetchall()]
edges=[x for x in cols if x.startswith('sig_') and not x.startswith('sig_cisd')]
q=f"""SELECT date, fwd_5d, sig_cisd_plus_struct AS cis,
       {', '.join(edges[:60])}
     FROM bars WHERE universe='sp500' AND fwd_5d IS NOT NULL"""
df=c.execute(q).fetchdf()
df['date']=pd.to_datetime(df.date)
base=df.fwd_5d.median(); cis=df.cis==1
print(f"baseline {base:+.3f}%   +CISD alone {df.loc[cis,'fwd_5d'].median():+.3f}%   n={cis.sum():,}\n")
print("DECIDING TEST — is +CISD independent of what we already have?")
print(f"{'edge':<26}{'n edge':>9}{'P(edge|CISD)':>14}{'lift':>7}")
rows=[]
pc=cis.mean()
for e in edges:
    if e not in df.columns: continue
    m=(df[e]==1)
    n=int(m.sum())
    if n<500: continue
    p_e=m.mean(); p_e_given=m[cis].mean()
    lift=p_e_given/p_e if p_e>0 else 0
    rows.append((e,n,p_e_given*100,lift))
rows.sort(key=lambda r:-r[3])
for e,n,p,l in rows[:6]+rows[-4:]:
    print(f"{e:<26}{n:>9,}{p:>13.2f}%{l:>7.2f}")
