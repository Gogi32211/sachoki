"""Full branching TREE (trie) of every forward path from each wt_evr+L34red anchor
(Z10/Z5/Z11) until it hits terminal T2/T2G. Every branch shown, indented, with the
share of the anchor population that flows through each node. Rare tails (< MIN_SHOW
cases) collapsed into '(+k rarer)'. Full flat path list dumped to CSV so nothing is
hidden. Dedup BEFORE walk (universe-dupe bug fixed)."""
import duckdb
import pandas as pd
from collections import Counter, defaultdict

MAXSTEP = 20
MIN_SHOW = 5        # a node must carry >= this many cases to be printed individually
OUT_CSV = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/z_chains_full.csv"

c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
anchors = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, z_sig AS anchor_z, volume, close
    FROM deduped
    WHERE l_sig='L34' AND close<open AND wt_evr=1 AND universe<>'index'
      AND close*volume >= 3000000 AND z_sig IN ('Z10','Z5','Z11')
""").fetchdf()
anchors["date_s"] = anchors["date"].astype(str).str[:10]
tickers = sorted(anchors.ticker.unique())
placeholders = ",".join(f"'{t}'" for t in tickers)
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

seq["code"] = seq.apply(lambda r: r.tt if r.tt else (r.zz if r.zz else "·"), axis=1)
seq["date_s"] = seq["date"].astype(str).str[:10]
codes = seq.code.to_numpy()
idx_by_tk = defaultdict(list)
for i, t in enumerate(seq.ticker.to_numpy()):
    idx_by_tk[t].append(i)
date_to_idx = {}
for t, idxs in idx_by_tk.items():
    for i in idxs:
        date_to_idx[(t, seq.date_s.iloc[i])] = i

def walk(start_i, last_i):
    path = [codes[start_i]]
    i = start_i; steps = 0
    while i < last_i and steps < MAXSTEP:
        i += 1; steps += 1
        path.append(codes[i])
        if codes[i] in ("T2", "T2G"):
            return tuple(path)
    return None   # never reached

paths_by_z = {"Z10": [], "Z5": [], "Z11": []}
for _, r in anchors.iterrows():
    key = (r.ticker, r.date_s)
    if key not in date_to_idx:
        continue
    i = date_to_idx[key]
    p = walk(i, idx_by_tk[r.ticker][-1])
    if p is not None:
        paths_by_z[r.anchor_z].append(p)

# ── trie printer ─────────────────────────────────────────────────────────────
def build_trie(paths):
    """nested dict: node -> {child_code: subtrie}, plus '_n' count at each node."""
    root = {"_n": len(paths), "_kids": {}}
    for p in paths:
        cur = root
        for code in p[1:]:            # skip the anchor (p[0]) — it's the root
            kids = cur["_kids"]
            if code not in kids:
                kids[code] = {"_n": 0, "_kids": {}}
            kids[code]["_n"] += 1
            cur = kids[code]
    return root

def print_trie(node, total, prefix="", depth=0):
    kids = node["_kids"]
    items = sorted(kids.items(), key=lambda kv: -kv[1]["_n"])
    shown = [kv for kv in items if kv[1]["_n"] >= MIN_SHOW]
    rare = [kv for kv in items if kv[1]["_n"] < MIN_SHOW]
    for j, (code, sub) in enumerate(shown):
        share = 100 * sub["_n"] / total
        term = code in ("T2", "T2G")
        mark = " ⟵ TERMINAL" if term else ""
        bar = "  " * depth
        print(f"  {bar}{'└' if (j==len(shown)-1 and not rare) else '├'} {code:5} n={sub['_n']:4} ({share:4.1f}% of anchor){mark}")
        if not term:
            print_trie(sub, total, prefix, depth + 1)
    if rare:
        rare_n = sum(kv[1]["_n"] for kv in rare)
        bar = "  " * depth
        print(f"  {bar}└ (+{len(rare)} rarer branches, {rare_n} cases, {100*rare_n/total:.1f}%)")

rows_csv = []
for z in ("Z10", "Z5", "Z11"):
    paths = paths_by_z[z]
    n = len(paths)
    print(f"\n{'═'*70}\n  ANCHOR {z}  —  {n} reached-T2/T2G paths  ({len(set(paths))} unique)\n{'═'*70}")
    root = build_trie(paths)
    print(f"  {z} (root, n={n})")
    print_trie(root, n)
    for p in paths:
        rows_csv.append({"anchor": z, "path": " → ".join(p), "len": len(p)})

# full flat dump
dfp = pd.DataFrame(rows_csv)
agg = dfp.groupby(["anchor", "path", "len"]).size().reset_index(name="n").sort_values(["anchor", "n"], ascending=[True, False])
agg.to_csv(OUT_CSV, index=False)
print(f"\nfull path list ({len(agg)} unique across anchors) → {OUT_CSV}")
