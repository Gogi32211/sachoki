"""second_breakout_5065.py — the user's "second breakout" zone: L34/L46 + coil at
RSI 50-65 (NOT capitulation — a mid-range markup leg). Test what works HERE: green
candle (close>open), CCI sign, breakout signals. Maybe close>open matters HERE (momentum)
where it hurt at capitulation. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
from ai_journal.db import get_analytics_conn
OOS="2024-09-01"
def wl(w,n,z=1.96):
    if n==0:return 0
    p=w/n;d=1+z*z/n;c=p+z*z/(2*n);m=z*((p*(1-p)/n+z*z/(4*n*n))**.5);return max(0,(c-m)/d)
a=get_analytics_conn(read_only=True)
BMED=dict(a.execute("SELECT universe,median(fwd_10d) FROM bars WHERE fwd_10d IS NOT NULL GROUP BY universe").fetchall())
df=a.execute("""SELECT universe,date,l_sig,open,close,rsi_14,cci_20,fwd_10d,fwd_20d,mfe_10d,mae_10d,
  sig_fri64,sig_blue,sig_fri34,bo_up,bx_up,vbo_up,eb_bull,sig_best,load,sq
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
   AND l_sig IN ('L34','L46') AND rsi_14 IS NOT NULL AND cci_20 IS NOT NULL""").fetchdf();a.close()
for c in ["sig_fri64","sig_blue","sig_fri34","bo_up","bx_up","vbo_up","eb_bull","sig_best","load","sq"]:
    df[c]=df[c].fillna(0).astype(np.int8)
df["exc"]=df.fwd_10d-df.universe.map(BMED); df["yr"]=pd.to_datetime(df.date).dt.year
df["GO"]=df.close>df.open
g=lambda n: df[n]==1
coil=g("sig_fri64")|g("sig_blue")|g("sig_fri34")
R5065=(df.rsi_14>=50)&(df.rsi_14<65)
def row(m,lab,path=False):
    s=df[m];n=len(s)
    if n<50:return f"  {lab:30} n={n} <50"
    e=s.exc;w=int((e>0).sum());med=float(e.median());m25=float(e.clip(-25,25).mean())
    py=ny=0;yc=[]
    for y in range(2021,2027):
        sy=e[s.yr==y]
        if len(sy)>=10:ny+=1;py+=float(sy.median())>0;yc.append(f"{y%100}:{float(sy.median()):+.1f}")
    out=f"  {lab:30} {n:>6} {w/n*100:>5.1f} {med:>+6.2f} {m25:>+6.2f} {wl(w,n)*100:>5.1f} {py}/{ny}"
    if path:
        isv=float(e[~(s.date.astype(str)>=OOS)].median());oo=float(e[s.date.astype(str)>=OOS].median())
        out+=f"\n      └ MFE{float(s.mfe_10d.median()):+.1f}/MAE{float(s.mae_10d.median()):.1f} IS{isv:+.1f}/OOS{oo:+.1f} tk:{s.universe.count() and s['date'].nunique() and len(s)} yr["+" ".join(yc)+"]"
    return out
hdr=f"  {'pattern':30} {'n':>6} {'win%':>5} {'medL':>6} {'m25L':>6} {'wLB':>5} +yr"
print("### RSI 50-65 'second breakout' zone — L34/L46 + coil")
print(hdr)
print(row(coil&R5065, "coil @ RSI50-65 (base)"))
print(row(coil&R5065&df.GO, "+ GREEN (close>open)"))
print(row(coil&R5065&~df.GO, "+ RED"))
print(row(coil&R5065&df.GO&(df.cci_20>0), "+ GREEN + CCI>0"))
print(row(coil&R5065&df.GO&(df.cci_20>100), "+ GREEN + CCI>100"))
print(row(coil&R5065&df.GO&(g("bo_up")|g("bx_up")|g("vbo_up")|g("eb_bull")), "+ GREEN + breakout-sig", path=True))
print(row(coil&R5065&df.GO&g("load"), "+ GREEN + LOAD"))
print(row(coil&R5065&df.GO&g("sq"), "+ GREEN + squeeze"))
print("\n### CONTRAST — the two zones side by side (best of each)")
print(hdr)
print(row(coil&(df.rsi_14<30)&(df.rsi_14>=10)&(df.cci_20<-100), "ZONE1 capit: RSI10-30 CCI<-100 (RED ok)", path=True))
print(row(coil&R5065&df.GO&(df.cci_20>0), "ZONE2 2nd-brk: RSI50-65 GREEN CCI>0", path=True))
print("\nlegend: GO=close>open. done")
