"""Does the mirror survive deduplication?"""
import numpy as np
from naked_study import NakedStudy
st = NakedStudy("post-fix sanity: the gap/return mirror", n_trials=2,
                columns=("t_sig",), horizons=(1, 10), min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
print(f"\n  rows {len(d):,} · tickers {d.ticker.nunique():,} · "
      f"duplicate (ticker,date): {len(d) - len(d[['ticker','date']].drop_duplicates()):,}")
print(f"  calendar-adjacent previous bar: {d['prev_ok'].mean():.2%}")
nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
gap = (nxo / d["close"].to_numpy() - 1) * 100
r1 = d["r1"].to_numpy() * 100
ok = np.isfinite(gap) & np.isfinite(r1)
for lo_, hi_, lbl in ((-100, -0.5, "gap < -0.5%"), (0, 100, "gap >= 0")):
    m = ok & (gap >= lo_) & (gap < hi_)
    print(f"  {lbl:14s} n={m.sum():>9,}  1-bar win {100*(r1[m]>0).mean():>6.2f}%  "
          f"median {np.median(r1[m]):>+6.3f}%")
print("\n  (before the fix these read 73.25% and 26.18%)")
