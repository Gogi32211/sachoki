"""Does ultra_score rank? Test its highest-WEIGHT components' forward path-sim over 6yr.
Each component is a +N-point flag in ultra_score.py; a flag that EARNS its points must
show a positive forward LIFT over the liquid-universe baseline, ideally era-stable. Exit:
journal_bench rule (next-open, -15%/+100%/20-bar, stop-first, gap-aware). MEASUREMENT.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
import journal_bench as JB

# (label, bars-col, ultra weight) — the biggest levers of the score
COMP=[("ROCKET","rocket",20),("BUY_2809","sig_buy",20),("BX_UP","bx_up",12),
      ("EB_BULL","eb_bull",10),("BO_UP","bo_up",10),("BE_UP","be_up",10),
      ("ABS","sig_abs",10),("STR","sig_strong",8),("VA","sig_va",8),("SVS","sig_svs",8),
      ("L34","l34",6),("FRI34","sig_fri34",6),("d_absorb_bull","d_absorb_bull",15)]
a=get_analytics_conn()
allcols=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
COMP=[(l,c,w) for (l,c,w) in COMP if c in allcols]
sel=",".join(f"coalesce({c},0) {c}" for _,c,_ in COMP)
df=a.execute(f"""
  WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,{sel},
       row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
     FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
  SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
df["date"]=df["date"].astype(str).str[:10]; df["yr"]=df["date"].str[:4]
# forward outcome per bar (journal_bench rule)
parts=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    parts.append(pd.DataFrame({"i":g.index,"ret":JB._outcomes(g,0.15,1.00)},index=g.index).assign(tk=tk))
df["ret"]=pd.concat([p["ret"] for p in parts]).values
D=df[(df.ret.notna())&(df.dv>=3_000_000)].copy()
base=D.ret.mean()*100; basew=(D.ret>0).mean()*100
print(f"liquid baseline (all bars): mean {base:+.2f}%  win {basew:.0f}%  n={len(D)}\n")
print(f"{'component':16}{'wt':>4}{'n':>8}{'mean':>8}{'lift':>7}{'win':>6} | {'TRAIN':>7}{'TEST':>7} {'yrs+':>5}  $21-89")
print("-"*92)
def m(s):
    s=s.dropna()
    return (s.mean()*100 if len(s) else float('nan'))
for lab,col,wt in sorted(COMP,key=lambda x:-x[2]):
    sub=D[D[col]==1]
    if len(sub)<50:
        print(f"{lab:16}{wt:>4}{len(sub):>8}  too few"); continue
    lift=m(sub.ret)-base
    tr=m(sub[sub.yr.isin(['2021','2022','2023'])].ret)-m(D[D.yr.isin(['2021','2022','2023'])].ret)
    te=m(sub[sub.yr.isin(['2024','2025','2026'])].ret)-m(D[D.yr.isin(['2024','2025','2026'])].ret)
    yp=yt=0
    for y in ['2021','2022','2023','2024','2025','2026']:
        sy=sub[sub.yr==y].ret.dropna(); by=D[D.yr==y].ret.dropna()
        if len(sy)>=15 and len(by): yt+=1; yp+=int(sy.mean()>by.mean())
    q=sub[(sub.close>=21)&(sub.close<89)]
    qm=m(q.ret) if len(q)>=30 else float('nan')
    print(f"{lab:16}{wt:>4}{len(sub):>8}{m(sub.ret):>+8.2f}{lift:>+7.2f}{(sub.ret>0).mean()*100:>5.0f}% | {tr:>+7.2f}{te:>+7.2f} {yp:>3}/{yt}   {qm:+.2f}%")
# reference axes that we KNOW rank
print("\nREFERENCE (validated axes):")
for lab,mask in [("RSI<35",D.rsi_14<35),("RSI<30",D.rsi_14<30),("RSI>70",D.rsi_14>70),
                 ("$21-89",(D.close>=21)&(D.close<89)),("$5-21",(D.close>=5)&(D.close<21))]:
    sub=D[mask]
    print(f"  {lab:10} n={len(sub):7} mean {m(sub.ret):+.2f}% (lift {m(sub.ret)-base:+.2f})  win {(sub.ret>0).mean()*100:.0f}%")
