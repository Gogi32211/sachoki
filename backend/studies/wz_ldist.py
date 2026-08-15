import pandas as pd, numpy as np
df=pd.read_parquet('/tmp/wz_l.parquet')
SIGS=['held','broke','fell','lo_held','wickflip']
pop = df.l_sig.fillna('—').value_counts(normalize=True)*100
top = list(pop.head(7).index)
print(f"{'':<10}" + "".join(f"{c:>8}" for c in top) + "     (% of that signal's bars)")
print(f"{'POPULATION':<10}" + "".join(f"{pop.get(c,0):>8.1f}" for c in top))
print()
for s in SIGS:
    d = df.loc[df[s].astype(bool), 'l_sig'].fillna('—').value_counts(normalize=True)*100
    line = f"{s:<10}" + "".join(f"{d.get(c,0):>8.1f}" for c in top)
    # how concentrated is it — the single code that dominates
    topcode = d.idxmax(); topshare = d.max()
    print(line + f"   → {topshare:.0f}% are {topcode}")
