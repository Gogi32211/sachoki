"""Path-sim on the Z10/Z5/Z11 (wt_evr+L34red) chain branches (2026-07-22).

CAUSAL segmentation — NO classify-by-future: you can't know at the anchor bar what
bar+1 will be, so we ENTER AT bar+1 (its close = the confirming signal, entry = bar+2
open) and segment by bar+1's code. Compares:
  A. baseline — enter at the ANCHOR bar (unconditional, entry = anchor+1 open)
  B. bar+1 = a T-code  (fast reversal branch: T1G/T4/T1/T5/T3/T9/...)
  C. bar+1 = a Z-code  (wander branch: Z2G/Z2/Z6/Z11/...)
  D. each individual bar+1 code (n>=40)
Path-sim = trail25 / -15% stop / 60-bar / 15bps, gap-realistic, per-year + TRAIN/TEST.
Dedup BEFORE walk."""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

S_ = 0.0015
c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
anchors = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, z_sig AS anchor_z
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
    SELECT ticker, date, open, high, low, close,
           coalesce(t_sig,'') tt, coalesce(z_sig,'') zz
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()

seq["code"] = seq.apply(lambda r: r.tt if r.tt else (r.zz if r.zz else "·"), axis=1)
seq["date_s"] = seq["date"].astype(str).str[:10]
seq["yr"] = seq["date_s"].str[:4]
o = seq.open.to_numpy(float); h = seq.high.to_numpy(float); lo_ = seq.low.to_numpy(float); cl = seq.close.to_numpy(float)
codes = seq.code.to_numpy(); yr = seq.yr.to_numpy()
idx_by_tk = defaultdict(list)
for i, t in enumerate(seq.ticker.to_numpy()):
    idx_by_tk[t].append(i)
last_i = {t: idxs[-1] for t, idxs in idx_by_tk.items()}
date_to_idx = {(t, seq.date_s.iloc[i]): i for t, idxs in idx_by_tk.items() for i in idxs}

def pathsim(s, tk_last):
    """Enter at open[s+1], trail25/-15/60/15bps. Returns pct or None if no room."""
    if s >= tk_last:
        return None
    e = o[s + 1] * (1 + S_)
    if e <= 0:
        return None
    pk = e; hd = e * 0.85; end = min(s + 61, tk_last + 1); r = None
    for q in range(s + 1, end):
        if q > s + 1 and o[q] <= hd: r = o[q] / e - 1 - S_; break
        if lo_[q] <= hd: r = -0.15 - S_; break
        pk = max(pk, h[q]); ts = pk * 0.75
        if q > s + 1 and o[q] <= ts: r = o[q] / e - 1 - S_; break
        if lo_[q] <= ts: r = ts / e - 1 - S_; break
    return r if r is not None else cl[end - 1] / e - 1 - S_

# Build the entry table: for each anchor, record (anchor entry) and (bar+1 entry w/ code)
recs = []
for _, a in anchors.iterrows():
    key = (a.ticker, a.date_s)
    i = date_to_idx.get(key)
    if i is None:
        continue
    tl = last_i[a.ticker]
    # A: anchor entry
    ps_a = pathsim(i, tl)
    if ps_a is not None:
        recs.append({"kind": "anchor", "code": a.anchor_z, "az": a.anchor_z, "ps": ps_a, "yr": yr[i + 1] if i + 1 <= tl else yr[i]})
    # B/C/D: bar+1 entry
    if i + 1 <= tl:
        b1 = codes[i + 1]
        ps_b = pathsim(i + 1, tl)
        if ps_b is not None and b1 not in ("·", ""):
            recs.append({"kind": "bar1", "code": b1, "az": a.anchor_z, "ps": ps_b, "yr": yr[i + 2] if i + 2 <= tl else yr[i + 1]})

R = pd.DataFrame(recs)
print(f"records: {len(R)}  (anchor={sum(R.kind=='anchor')}, bar1={sum(R.kind=='bar1')})\n")

def stat(df, label, min_n=40):
    n = len(df)
    if n < min_n:
        print(f"  {label:30} n={n:4}  ⚠ below min-n({min_n})")
        return
    pm = df.ps.mean() * 100; pmed = df.ps.median() * 100; win = (df.ps > 0).mean() * 100
    tr = df[df.yr.isin(["2021", "2022", "2023"])]; te = df[df.yr.isin(["2024", "2025", "2026"])]
    ptr = tr.ps.mean() * 100 if len(tr) >= 20 else float("nan")
    pte = te.ps.mean() * 100 if len(te) >= 20 else float("nan")
    yrs_pos = sum(1 for y in ["2021","2022","2023","2024","2025","2026"]
                  if (df.yr == y).sum() >= 10 and df[df.yr == y].ps.mean() > 0)
    yrs_tot = sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (df.yr == y).sum() >= 10)
    print(f"  {label:30} n={n:4}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%  TR {ptr:+5.2f} TE {pte:+5.2f}  {yrs_pos}/{yrs_tot}yr+")

print("═══ A. BASELINE: enter at the anchor bar (unconditional) ═══")
stat(R[R.kind == "anchor"], "anchor (Z10+Z5+Z11)")
for z in ["Z10", "Z5", "Z11"]:
    stat(R[(R.kind == "anchor") & (R.az == z)], f"anchor {z}")

print("\n═══ B/C. enter at bar+1, T-branch vs Z-branch (causal) ═══")
b1 = R[R.kind == "bar1"].copy()
b1["is_t"] = b1.code.str.startswith("T")
stat(b1[b1.is_t], "bar+1 = any T-code (reversal)")
stat(b1[~b1.is_t], "bar+1 = any Z-code (wander)")

print("\n═══ D. enter at bar+1, each individual code (n>=40) ═══")
for code, grp in sorted(b1.groupby("code"), key=lambda kv: -kv[1].ps.mean()):
    if len(grp) >= 40:
        stat(grp, f"bar+1 = {code}")

print("\n═══ split T-branch by anchor (does the anchor still matter once T confirms?) ═══")
for z in ["Z10", "Z5", "Z11"]:
    stat(b1[(b1.is_t) & (b1.az == z)], f"{z} → T-code")
