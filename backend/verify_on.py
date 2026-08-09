"""Regression: re-run the T6 claim with the control matched on gap depth too.

Before the fix, the framework printed SIGNAL because the control matched price, liquidity and
year but not the gap the cell was selected on. With `on=gap` the two arms sit at the same gap
depth, and the answer should collapse to what the close[i+1] check already showed: nothing.
"""
import numpy as np
from naked_study import NakedStudy

st = NakedStudy("regression — T6 with the control matched on gap depth",
                n_trials=4, columns=("t_sig", "z_sig"), horizons=(1, 10),
                min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
Z = d["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T != "") & (T != "nan"), T, Z)
nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
gap = (nxo / d["close"].to_numpy() - 1) * 100
ok = np.isfinite(gap)
m = (tok == "T6") & ok & (gap < -0.5)
st.population(n_boot=200)
print("\n  --- OLD behaviour: control matched on price × liquidity × year only ---")
st.signal("T6 · gap<-0.5  (no gap matching)", m, n_boot=300)
print("\n  --- FIXED: control also matched on gap depth ---")
st.signal("T6 · gap<-0.5  (gap-matched)", m, n_boot=300, on=np.nan_to_num(gap))
