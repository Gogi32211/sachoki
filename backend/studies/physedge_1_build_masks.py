"""IN PROGRESS — physics/shape states crossed with the book's edges.

Kept so the work is not lost to a /tmp wipe. NOT a result: the gates this book
requires have not been run. What exists is a first screen on median fwd_5d,
which in this same session has already been shown to disagree with a path
simulation that used real stops.

Still to do: path-sim with stops · worst-year per cell · the other timeframes
(4h/1h/15m are backfilled) · the other universes · a multiplicity correction.

1_build_masks  pulls via edge_replay._pull and runs _prep to get the E_* masks
2_cross        joins the physics/shape columns and crosses them with the edges
3_windows      the same aggregate on the mining window and on the reserved one

No conclusion is recorded here on purpose.
"""
import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import pandas as pd, numpy as np
import edge_replay as ER
df, as_of = ER._pull(months=72, dv_floor=1e6)
print('pulled', len(df), 'rows', df.date.min(), '→', df.date.max()); sys.stdout.flush()
df = ER._prep(df)
E = [x for x in df.columns if x.startswith('E_') and df[x].dtype == bool]
print('masks:', len(E)); sys.stdout.flush()
keep = ['ticker','date','close','fwd_5d','phys_r','phys_regime','phys_e','phys_c',
        'bar_body_wick'] + E
keep = [k for k in keep if k in df.columns]
df[keep].to_parquet('/tmp/edge_frame.parquet', index=False)
f = sorted(((e, int(df[e].sum())) for e in E), key=lambda kv: -kv[1])
print('\ntop edges:'); [print(f'  {k:<26}{v:>8,}') for k,v in f[:12]]
