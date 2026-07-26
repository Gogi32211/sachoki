"""BUY-signal × PRECEDING-sequence conditioner analysis (2026-07-20, user redesign).
For every historical fire of each BUY-row signal (🟢REV, 🔵BRK, ▲4H, L-heavy=red-L34,
EDGE-any, EDGE🟢, FLY, ✦FLY-fresh): take the 2/3/4-bar token sequence ENDING one bar
BEFORE the fire, in TWO vocabularies (coarse = T/Z only · fine = T/Z+L), and measure
the simple historical outcome: % up vs down over fwd 20 bars + median.
Boosters/suppressors vs each signal's own baseline; TRAIN 21-23 / TEST 24-26; min-n."""
import numpy as np, pandas as pd, duckdb, sys
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')

print("pull daily frame…",flush=True)
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,beta_score,turbo_score,
  coalesce(z_sig,'') z, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
  coalesce(CAST(sig_fly_abcd AS INT),0)+coalesce(CAST(sig_fly_cd AS INT),0)
   +coalesce(CAST(sig_fly_bd AS INT),0)+coalesce(CAST(sig_fly_ad AS INT),0) flyn,
  coalesce(buy_score,0) bscore,
  coalesce(close_suffix,'') sx, coalesce(bar_body_wick,'') bw,
  coalesce(bar_gap_range,'') gr, coalesce(bar_line5,'') q5, coalesce(vol_bucket,'') vb,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT ticker, strftime(date,'%Y-%m-%d') d, open,high,low,close, close*volume dv,
        rsi_14, beta_score, turbo_score, z, t, l, bscore, sx, bw, gr, q5, vb,
        CAST(flyn>0 AS INT) fly,
        lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20
 FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
print(f"frame {len(D):,}",flush=True)

print("4h REV day-set…",flush=True)
c=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_4h.duckdb',read_only=True)
r4=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C4=set(zip(r4.ticker,r4.d))
print("1h REV day-set…",flush=True)
c=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_1h.duckdb',read_only=True)
r1=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C1=set(zip(r1.ticker,r1.d))

print("edge frame (60,3M)…",flush=True)
import edge_replay as ER
grp,_=ER._frame(60,3_000_000)
E_ANY=set(); E_PREM=set()
PREM=("E_qzcapit","E_dl1","E_rtb_base","E_p55")
for tk,g in grp.items():
    ds=g["date"].astype(str).str[:10].to_numpy()
    anym=np.zeros(len(g),bool); prem=np.zeros(len(g),bool)
    for code,col in ER.DISPLAY_SETUPS:
        if col in g: anym|=g[col].to_numpy(bool)
    for col in PREM:
        if col in g: prem|=g[col].to_numpy(bool)
    for d0 in ds[anym]: E_ANY.add((tk,d0))
    for d0 in ds[prem]: E_PREM.add((tk,d0))
print(f"edge-any days {len(E_ANY):,} · premium {len(E_PREM):,}",flush=True)

rows=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o=g.open.to_numpy(float); cl=g.close.to_numpy(float); f20=g.f20.to_numpy(float)
    rs=g.rsi_14.to_numpy(float); bt=g.beta_score.to_numpy(float); tb=g.turbo_score.to_numpy(float)
    bsc=g.bscore.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.d.tolist()
    z=g.z.tolist(); t=g.t.tolist(); l=g.l.tolist(); fly=g.fly.to_numpy(int)
    LY={k:[x if x else '-' for x in g[k].tolist()] for k in ('sx','bw','gr','q5','vb')}
    # tokens
    ct=[ (z[i] if z[i] else ('-'+t[i] if t[i] else '-')) for i in range(n) ]           # coarse T/Z
    ft=[ ct[i] + (l[i] if l[i] else '') for i in range(n) ]                            # fine T/Z+L
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    pc=np.concatenate([[np.nan],cl[:-1]]); prs=np.concatenate([[np.nan],rs[:-1]])
    lastfly=-99
    for i in range(6,n):
        if fly[i]:
            fresh = (i-lastfly)>15
            lastfly=i
        else: fresh=False
        if dv[i]<3e6 or not (f20[i]==f20[i]): continue
        up=cl[i]>pc[i]
        rev=(m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and up and rs[i]>prs[i] and bt[i]<=13
        brk=(prs[i]<50<=rs[i]) and up and tb[i]<=28
        lh=(l[i]=='L34') and cl[i]<o[i]
        h4=(tk,ds[i]) in C4
        ea=(tk,ds[i]) in E_ANY
        ep=ea and ((tk,ds[i]) in E_PREM) and rev
        flyb=bool(fly[i])
        score=bsc[i]>=60                                        # digits base (score-day)
        h1=(tk,ds[i]) in C1                                     # △ 1H REV
        turn=up and rs[i]>prs[i] and rs[i]<55 and (h4 or h1)    # ①②③ loose turn + echo
        # 2026-07-20b: keep EVERY liquid bar — the signal-agnostic "anyb" table turns
        # the ctx layer into a per-bar mini-forecast (user request).
        r20=f20[i]/cl[i]-1
        # 2026-07-20e (user fix): the sequence ENDS ON the current bar (robust-seq
        # convention — completed pattern, forecast starts next bar), not one before it.
        c2=" ".join(ct[i-1:i+1]); c3=" ".join(ct[i-2:i+1]); c4=" ".join(ct[i-3:i+1])
        f2=" ".join(ft[i-1:i+1]); f3=" ".join(ft[i-2:i+1]); f4=" ".join(ft[i-3:i+1])
        LP={}
        for k,seq in LY.items():
            for d in (2,3,4):
                LP[f"{k}{d}"]=" ".join(seq[i-d+1:i+1])
        rows.append((ds[i][:4],cl[i],rev,brk,lh,h4,ea,ep,flyb,fresh,score,turn,h1,True,r20,
                     c2,c3,c4,f2,f3,f4,
                     LP["sx2"],LP["sx3"],LP["sx4"],LP["bw2"],LP["bw3"],LP["bw4"],
                     LP["gr2"],LP["gr3"],LP["gr4"],LP["q52"],LP["q53"],LP["q54"],
                     LP["vb2"],LP["vb3"],LP["vb4"]))
R=pd.DataFrame(rows,columns=["yr","px","rev","brk","lh","h4","ea","ep","fly","flyf",
                             "score","turn","h1","anyb","r20","c2","c3","c4","f2","f3","f4",
                             "sx2","sx3","sx4","bw2","bw3","bw4","gr2","gr3","gr4",
                             "q52","q53","q54","vb2","vb3","vb4"])
print(f"signal-bars: {len(R):,}",flush=True)
R.to_parquet('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/buyseq_fires.parquet')

TR=('2021','2022','2023'); TE=('2024','2025','2026')
SIGS=[("🟢REV","rev"),("🔵BRK","brk"),("▲4H","h4"),("△1H","h1"),("L-heavy","lh"),
      ("EDGE-any","ea"),("EDGE🟢","ep"),("FLY","fly"),("✦FLY-fresh","flyf"),
      ("score≥60","score"),("①②③turn","turn"),("ANY-bar","anyb")]
def stats(s):
    return len(s), 100*(s.r20>0).mean(), 100*s.r20.median()
for label,col in SIGS:
    S=R[R[col]]
    if len(S)<200: print(f"\n══ {label}: n={len(S)} too few ══"); continue
    n0,u0,m0=stats(S)
    ntr,utr,mtr=stats(S[S.yr.isin(TR)]); nte,ute,mte=stats(S[S.yr.isin(TE)])
    print(f"\n══════ {label} — baseline n={n0:,} up {u0:.1f}% med {m0:+.2f}% | TR up {utr:.1f} · TE up {ute:.1f} ══════")
    for vocab,cols,minn in (("COARSE T/Z",("c2","c3","c4"),80),("FINE T/Z+L",("f2","f3","f4"),50)):
        cand=[]
        for pc_ in cols:
            gg=S.groupby(pc_)
            for seq,sg in gg:
                if len(sg)<minn or 'nan' in seq: continue
                n,u,m=stats(sg)
                a_tr=sg[sg.yr.isin(TR)]; a_te=sg[sg.yr.isin(TE)]
                if len(a_tr)<20 or len(a_te)<20: continue
                _,utr2,_=stats(a_tr); _,ute2,_=stats(a_te)
                cons=(utr2>u0 and ute2>u0) or (utr2<u0 and ute2<u0)
                cand.append((seq,len(pc_.replace('c','').replace('f','')) and int(pc_[1]),n,u,m,utr2,ute2,cons))
        cand=[c for c in cand]
        boost=sorted([c for c in cand if c[3]>u0],key=lambda x:-x[3])[:6]
        supp=sorted([c for c in cand if c[3]<u0],key=lambda x:x[3])[:4]
        print(f"  ── {vocab} (cells {len(cand)}):")
        for seq,dep,n,u,m,ut,ue,cons in boost:
            print(f"    ↑ {seq:24} d{dep} n={n:5} up {u:.1f}% (+{u-u0:.1f}) med {m:+.2f}% | TR {ut:.0f}/TE {ue:.0f}{' ✓' if cons else ' ⚠era'}")
        for seq,dep,n,u,m,ut,ue,cons in supp:
            print(f"    ↓ {seq:24} d{dep} n={n:5} up {u:.1f}% ({u-u0:.1f}) med {m:+.2f}% | TR {ut:.0f}/TE {ue:.0f}{' ✓' if cons else ' ⚠era'}")
