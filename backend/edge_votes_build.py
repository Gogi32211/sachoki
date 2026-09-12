"""EDGE-family vote per (ticker, session): does ANY of the 8 de-duplicated edge families fire?

JOINT_STACK_V1 needs one vote for the whole EDGE row, so the 8 families that
edge_replay._FAMILIES already de-duplicates (capit/retest/spring/gap/atomic/oseq/l43/engulf)
collapse to a single boolean — otherwise the one validated confluence law would dominate a count
that is supposed to test the OTHER layers. conf_n is carried along for the descriptive read.
Chunked over tickers because _prep materialises several hundred columns.
"""
import sys, time, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
import pandas as pd
import edge_replay as ER

OUT = "/Users/sachoki/Desktop/sachoki-desktop/data/edge_votes_v1.parquet"
t0 = time.time()
out = ER._pull(months=70, dv_floor=3_000_000)
df = out[0] if isinstance(out, tuple) else out
print(f"pulled {len(df):,} rows · {df['ticker'].nunique():,} tickers ({time.time()-t0:.0f}s)", flush=True)
tks = sorted(df["ticker"].unique())
parts, CH = [], 300
for i in range(0, len(tks), CH):
    b = set(tks[i:i + CH])
    d = ER._prep(df[df["ticker"].isin(b)].copy())
    parts.append(d[["ticker", "date", "conf_n", "conf_anyfam"]].copy())
    del d
    print(f"  {min(i+CH, len(tks))}/{len(tks)} ({time.time()-t0:.0f}s)", flush=True)
E = pd.concat(parts, ignore_index=True)
E["session"] = E["date"].astype(str).str[:10]
E = E[["ticker", "session", "conf_n", "conf_anyfam"]]
E.to_parquet(OUT + ".tmp", index=False)
import os; os.replace(OUT + ".tmp", OUT)
print(f"WROTE {OUT} · {len(E):,} rows ({time.time()-t0:.0f}s)")
print("conf_n dist:", E["conf_n"].value_counts().sort_index().to_dict())
print("anyfam rate:", round(float(E["conf_anyfam"].mean())*100, 3), "%")
