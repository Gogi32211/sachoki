"""t1_volume.py — ANALYSIS ONLY. How vol_bucket (W<L<N<B<VB) affects T1/T1G.
Per signal/universe/bucket: n, med fwd_10d, lift vs baseline, mean, win%, p25,
medMFE/medMAE/RR. Plus bucket × w2_evr interaction. Percent units."""
import duckdb, numpy as np, pandas as pd
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
UNIS=("sp500","nasdaq","russell2k"); BUCKETS=["W","L","N","B","VB"]
con=duckdb.connect(DB,read_only=True)

def baseline(u):
    return con.execute(f"SELECT median(fwd_10d) FROM bars WHERE universe='{u}' AND fwd_10d BETWEEN -90 AND 500").fetchone()[0]

def load(sig):
    df=con.execute(f"""SELECT universe,vol_bucket,fwd_10d,mfe_10d,mae_10d,w2_evr,
        year(date) yr FROM (SELECT *,row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
        FROM bars WHERE t_sig='{sig}') WHERE rn=1""").fetchdf()
    df["fwd_10d"]=pd.to_numeric(df.fwd_10d,errors="coerce")
    return df[np.isfinite(df.fwd_10d)&df.fwd_10d.between(-90,500)].copy()

def stats(s):
    f=pd.to_numeric(s.fwd_10d,errors="coerce"); mfe=pd.to_numeric(s.mfe_10d,errors="coerce"); mae=pd.to_numeric(s.mae_10d,errors="coerce").abs()
    md=float(f.median()); rr=float(mfe.median()/mae.median()) if mae.median() else np.nan
    return dict(n=len(f),med=round(md,2),mean=round(float(f.clip(-90,500).mean()),2),
                win=round(float((f>0).mean()*100),1),p25=round(float(f.quantile(.25)),2),
                mfe=round(float(mfe.median()),1),rr=round(rr,2))

print("=== HOW vol_bucket AFFECTS T1 / T1G (fwd_10d) ===")
for sig in ("T1G","T1"):
    print(f"\n##### {sig} #####")
    for u in UNIS:
        b=baseline(u); df=load(sig); d=df[df.universe==u]
        print(f"\n {u}  (baseline med {round(b,3)})")
        print(f"   {'bkt':3} {'n':>7} {'med':>6} {'lift':>6} {'mean':>6} {'win%':>5} {'p25':>6} {'MFE':>5} {'RR':>5}")
        for bk in BUCKETS:
            s=d[d.vol_bucket==bk]
            if len(s)<30: print(f"   {bk:3} n<30"); continue
            st=stats(s); print(f"   {bk:3} {st['n']:>7} {st['med']:>6} {round(st['med']-b,2):>6} {st['mean']:>6} {st['win']:>5} {st['p25']:>6} {st['mfe']:>5} {st['rr']:>5}")

print("\n\n=== BUCKET × w2_evr interaction (T1G) — does w2_evr rescue VB / boost B? ===")
for u in ("nasdaq","russell2k"):
    b=baseline(u); df=load("T1G"); d=df[df.universe==u]
    print(f"\n {u} (base {round(b,3)}):")
    for bk in ("B","VB"):
        s=d[d.vol_bucket==bk]; s_w2=s[s.w2_evr==1]
        a=stats(s);
        if len(s_w2)>=30:
            w=stats(s_w2); print(f"   {bk:3} plain  med {a['med']:>6} (n{a['n']})   +w2_evr med {w['med']:>6} (n{w['n']}, win{w['win']})")
        else:
            print(f"   {bk:3} plain  med {a['med']:>6} (n{a['n']})   +w2_evr n<30")
con.close()
