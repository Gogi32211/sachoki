import pandas as pd, numpy as np
df=pd.read_parquet('/tmp/wz_l.parquet')
SIGS=['held','broke','fell','lo_held','wickflip']
df['L']=df.l_sig.fillna('—')

def cramers_v(a, b):
    ct = pd.crosstab(a, b).values
    chi2 = ((ct - np.outer(ct.sum(1), ct.sum(0))/ct.sum())**2 /
            np.maximum(np.outer(ct.sum(1), ct.sum(0))/ct.sum(), 1e-9)).sum()
    n = ct.sum(); r, k = ct.shape
    return np.sqrt(chi2 / (n * (min(r, k) - 1)))

print(f"{'signal':<10}{'Cramér V':>10}{'top-2 L codes cover':>22}   reading")
for s in SIGS:
    m = df[s].astype(bool)
    v = cramers_v(m, df.L)
    d = df.loc[m,'L'].value_counts(normalize=True)*100
    cover = d.head(2).sum()
    note = ("a re-encoding of the L ladder" if cover > 90 else
            "partly overlapping" if cover > 60 else "independent of it")
    print(f"{s:<10}{v:>10.3f}{cover:>21.1f}%   {note}")

print("\npopulation top-2 for comparison:",
      f"{(df.L.value_counts(normalize=True)*100).head(2).sum():.1f}%")
