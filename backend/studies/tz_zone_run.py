import pandas as pd, numpy as np
df=pd.read_parquet('/tmp/tz_zone.parquet'); df['date']=pd.to_datetime(df['date'])
df['yr']=df.date.dt.year
T=df[df.t_sig.notna()&(df.t_sig!='')].copy()
base=df.fwd_5d.median()
ZONES=['ABOVE the wick','IN the wick','in the body','in the lower wick','BELOW everything']

print(f"baseline, every bar            {base:+.3f}%")
print(f"all T signals                  {T.fwd_5d.median():+.3f}%   n={len(T):,}\n")
print(f"{'where the T bar CLOSED':<22}{'n':>8}{'median fwd5':>13}{'vs T':>9}{'yrs+':>7}{'worst':>8}")
for z in ZONES:
    s=T[T.zone==z]
    if len(s)<200: continue
    yr=s.groupby('yr')['fwd_5d'].median()
    print(f"  {z:<20}{len(s):>8}{s.fwd_5d.median():>12.3f}%{s.fwd_5d.median()-T.fwd_5d.median():>+9.3f}"
          f"{(yr>0).sum():>4}/{len(yr)}{yr.min():>8.2f}")

print(f"\nper T code — the split that matters (ABOVE vs IN):")
print(f"{'code':<7}{'n above':>9}{'above':>9}{'n in':>8}{'in':>9}{'gap':>9}")
for code in T.t_sig.value_counts().head(12).index:
    s=T[T.t_sig==code]
    a=s[s.zone=='ABOVE the wick']; i=s[s.zone=='IN the wick']
    if len(a)<100 or len(i)<100: continue
    print(f"{code:<7}{len(a):>9}{a.fwd_5d.median():>8.3f}%{len(i):>8}{i.fwd_5d.median():>8.3f}%"
          f"{i.fwd_5d.median()-a.fwd_5d.median():>+8.3f}")
