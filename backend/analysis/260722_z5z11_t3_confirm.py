"""Isolate the era-stable branch: wt_evr+L34red on Z5 or Z11 anchor (NOT Z10) →
wait for bar+1 → enter iff bar+1 == T3. Full validation: per-year, TRAIN/TEST,
price buckets, random-same-n z-control, and the looser variants for context
(T3 from any anchor incl Z10; Z5/Z11 → any T; drop the T3 gate) so we see exactly
what each condition buys. Path-sim trail25/-15/60/15bps, entry = bar+2 open."""
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

# Build bar+1 entry records with anchor + bar+1 code + entry price bucket
recs = []
allv_ps = []   # population for z-control: every bar+1 entry regardless of code
for _, a in anchors.iterrows():
    i = date_to_idx.get((a.ticker, a.date_s))
    if i is None:
        continue
    tl = last_i[a.ticker]
    if i + 1 > tl:
        continue
    b1 = codes[i + 1]
    ps = pathsim(i + 1, tl)
    if ps is None or b1 in ("·", ""):
        continue
    entry_close = cl[i + 1]
    recs.append({"az": a.anchor_z, "b1": b1, "ps": ps,
                 "yr": yr[i + 2] if i + 2 <= tl else yr[i + 1],
                 "px": entry_close})
R = pd.DataFrame(recs)

def block(df, label, min_n=25):
    n = len(df)
    if n < min_n:
        print(f"  {label:34} n={n:4}  ⚠ n<{min_n}")
        return
    pm = df.ps.mean() * 100; pmed = df.ps.median() * 100; win = (df.ps > 0).mean() * 100
    print(f"  {label:34} n={n:4}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%")

def per_year(df, label):
    print(f"\n── {label} — per year ──")
    for y in ["2021", "2022", "2023", "2024", "2025", "2026"]:
        block(df[df.yr == y], y, min_n=1)
    tr = df[df.yr.isin(["2021","2022","2023"])]; te = df[df.yr.isin(["2024","2025","2026"])]
    block(tr, "TRAIN 21-23", min_n=1); block(te, "TEST 24-26", min_n=1)

# ── THE SETUP: Z5/Z11 anchor → bar+1 == T3 ──
setup = R[(R.az.isin(["Z5", "Z11"])) & (R.b1 == "T3")]
print(f"═══ FLAGSHIP: (Z5|Z11) → T3-confirm  (n={len(setup)}) ═══")
block(setup, "flagship")
per_year(setup, "flagship")

print("\n═══ price buckets (flagship) ═══")
block(setup[setup.px < 21], "px $5-21")
block(setup[(setup.px >= 21) & (setup.px < 89)], "px $21-89")
block(setup[setup.px >= 89], "px $89+")

print("\n═══ ablation — what each condition buys ═══")
block(R[R.b1 == "T3"], "T3 from ANY anchor (incl Z10)")
block(R[(R.az.isin(["Z5","Z11"]))], "Z5|Z11 → ANY bar+1")
block(R[(R.az == "Z10") & (R.b1 == "T3")], "Z10 → T3 (the excluded one)")
block(R[R.b1.str.startswith("T")], "any anchor → any T")

# ── random-same-n z-control: draw n bar+1-entries at random from the full pool ──
pool = R.ps.to_numpy()
n_set = len(setup)
rng = np.random.default_rng(7)
rand_means = np.array([100 * np.mean(rng.choice(pool, n_set, replace=False)) for _ in range(2000)])
z = (setup.ps.mean() * 100 - rand_means.mean()) / rand_means.std()
print(f"\n═══ random-same-n z-control (2000 draws from all {len(pool)} bar+1 entries) ═══")
print(f"  flagship ps {setup.ps.mean()*100:+.2f}% vs random {rand_means.mean():+.2f}% (sd {rand_means.std():.2f}) → z = {z:+.2f}")
print(f"  pct of random draws >= flagship: {100*np.mean(rand_means >= setup.ps.mean()*100):.1f}%")
