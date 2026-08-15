import pandas as pd, numpy as np
df=pd.read_parquet('/tmp/wz_l.parquet')
SIGS=['held','broke','fell','lo_held','wickflip']
N=len(df)
for L in ('L34','L46'):
    df[L]=(df.l_sig==L)
print(f"population n={N:,}   P(L34)={df.L34.mean()*100:.2f}%   P(L46)={df.L46.mean()*100:.2f}%\n")
print(f"{'signal':<10}{'n':>8}   {'P(L34|sig)':>11}{'lift':>7}   {'P(L46|sig)':>11}{'lift':>7}")
for s in SIGS:
    m=df[s].astype(bool); n=int(m.sum())
    r=[]
    for L in ('L34','L46'):
        p=df.loc[m,L].mean(); base=df[L].mean()
        # 95% CI on the conditional proportion, normal approx
        se=np.sqrt(p*(1-p)/n)
        r.append((p*100, p/base, (p-1.96*se)/base, (p+1.96*se)/base))
    print(f"{s:<10}{n:>8}   {r[0][0]:>10.2f}%{r[0][1]:>7.2f}   {r[1][0]:>10.2f}%{r[1][1]:>7.2f}")
    print(f"{'':<10}{'':>8}   {'':>11}[{r[0][2]:.2f},{r[0][3]:.2f}]{'':>1}{'':>11}[{r[1][2]:.2f},{r[1][3]:.2f}]")
