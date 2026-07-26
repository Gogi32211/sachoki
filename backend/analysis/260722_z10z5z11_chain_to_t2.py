"""From each wt_evr+L34red anchor (Z10/Z5/Z11), walk FORWARD bar-by-bar (properly
deduped BEFORE any lead/walk — the earlier session bug) until the code hits T2 or
T2G (terminal), collecting the full path. Reports step-length distribution + the
most common complete paths, separately per starting Z-code. Cap 20 bars (else
'never reached')."""
import duckdb
import pandas as pd
from collections import Counter

MAXSTEP = 20
c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)

# 1) anchors: (ticker, date) where wt_evr+L34red fires with z_sig in Z10/Z5/Z11
anchors = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, z_sig AS anchor_z, universe, volume, close
    FROM deduped
    WHERE l_sig='L34' AND close<open AND wt_evr=1 AND universe<>'index'
      AND close*volume >= 3000000 AND z_sig IN ('Z10','Z5','Z11')
""").fetchdf()
anchors["date_s"] = anchors["date"].astype(str).str[:10]
print(f"anchors: {len(anchors)}")

tickers = sorted(anchors.ticker.unique())
placeholders = ",".join(f"'{t}'" for t in tickers)

# 2) full deduped per-ticker code sequence (one row per ticker,date, properly ordered)
seq = c.execute(f"""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5 AND ticker IN ({placeholders})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, coalesce(t_sig,'') tt, coalesce(z_sig,'') zz
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"seq rows: {len(seq):,}")

seq["code"] = seq.apply(lambda r: r.tt if r.tt else (r.zz if r.zz else ""), axis=1)
seq["date_s"] = seq["date"].astype(str)
idx_by_tk = {}
for i, t in enumerate(seq.ticker.to_numpy()):
    idx_by_tk.setdefault(t, []).append(i)

date_to_idx = {}
for t, idxs in idx_by_tk.items():
    for j, i in enumerate(idxs):
        date_to_idx[(t, seq.date_s.iloc[i])] = i

codes = seq.code.to_numpy()

def walk(tk, start_i, tk_last_i):
    """Return (path list incl. anchor, steps_to_terminal or None)."""
    path = [codes[start_i]]
    i = start_i
    steps = 0
    while i < tk_last_i and steps < MAXSTEP:
        i += 1
        steps += 1
        path.append(codes[i])
        if codes[i] in ("T2", "T2G"):
            return path, steps
    return path, None

results = {"Z10": [], "Z5": [], "Z11": []}
for _, r in anchors.iterrows():
    tk = r.ticker; az = r.anchor_z
    key = (tk, r.date_s)
    if key not in date_to_idx:
        continue
    i = date_to_idx[key]
    tk_last_i = idx_by_tk[tk][-1]
    path, steps = walk(tk, i, tk_last_i)
    results[az].append((tuple(path), steps))

for z in ("Z10", "Z5", "Z11"):
    rows = results[z]
    n = len(rows)
    print(f"\n═══════════ anchor {z} (n={n}) ═══════════")
    reached = [r for r in rows if r[1] is not None]
    never = n - len(reached)
    print(f"reached T2/T2G within {MAXSTEP} bars: {len(reached)} ({100*len(reached)/n:.1f}%) | never: {never} ({100*never/n:.1f}%)")

    steplen = Counter(r[1] for r in reached)
    print("step-length distribution (bars from anchor to T2/T2G):")
    for k in sorted(steplen):
        print(f"  {k:2} bars: {steplen[k]:4} ({100*steplen[k]/len(reached):.1f}%)")

    pathcnt = Counter(r[0] for r in reached)
    print(f"\ntop 15 complete paths (of {len(set(pathcnt))} unique):")
    for path, cnt in pathcnt.most_common(15):
        print(f"  {' → '.join(path):60}  n={cnt:4}  ({100*cnt/len(reached):.1f}%)")
