"""Robustness battery for the (Z5|Z11)+wt_evr+L34red → bar+1==T3 flagship (2026-07-22).

Tests, in order of importance:
  1. ANCHOR ABLATION — does wt_evr+L34red actually matter, or is it just Z5/Z11→T3?
     (If the bare version works as well, the absorption story is overfit decoration.)
  2. CONFIRM WIDENING — {T3} vs {T3,T9} vs {T3,T9,T11} (n boost, robustness)
  3. EXIT-RULE ROBUSTNESS — flagship under hold-20 / trail35-stop25 / trail20-stop10
     (guard: is the edge a path-sim artifact or real across exit families?)
  4. BOOTSTRAP CI — honest uncertainty on the thin n
  5. MULTIPLE-TESTING note (we scanned ~11 bar+1 codes × 3 anchors)

Full deduped bars loaded once; everything derived in-memory. dv>=3M, non-index, $5+."""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
df = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close >= 5 AND universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
    )
    SELECT ticker, date, open, high, low, close, volume,
           coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
           coalesce(wt_evr,0) wt_evr
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"deduped bars: {len(df):,}", flush=True)

o = df.open.to_numpy(float); h = df.high.to_numpy(float); lo_ = df.low.to_numpy(float); cl = df.close.to_numpy(float)
vol = df.volume.to_numpy(float)
tt = df.tt.to_numpy(); zz = df.zz.to_numpy(); ll = df.ll.to_numpy()
wt = df.wt_evr.to_numpy(); yr = df.date.astype(str).str[:4].to_numpy()
tkarr = df.ticker.to_numpy()
n = len(df)
idx_by_tk = defaultdict(list)
for i, t in enumerate(tkarr):
    idx_by_tk[t].append(i)
last_i = np.empty(n, dtype=int)
for t, idxs in idx_by_tk.items():
    for i in idxs:
        last_i[i] = idxs[-1]

def pathsim(s, tl, trail=0.25, stop=0.15, hold=60, fixed_hold=None):
    """Enter at open[s+1]. trailing-stop `trail` off peak, hard stop `stop`, max `hold`
    bars. If fixed_hold set, ignore stops and just exit at open[s+1+fixed_hold]."""
    if s >= tl:
        return None
    e = o[s + 1] * (1 + 0.0015)
    if e <= 0:
        return None
    if fixed_hold is not None:
        q = min(s + 1 + fixed_hold, tl)
        return o[q] / e - 1 - 0.0015 if q > s + 1 else cl[q] / e - 1 - 0.0015
    pk = e; hd = e * (1 - stop); end = min(s + 1 + hold, tl + 1); r = None
    for q in range(s + 1, end):
        if q > s + 1 and o[q] <= hd: r = o[q] / e - 1 - 0.0015; break
        if lo_[q] <= hd: r = -stop - 0.0015; break
        pk = max(pk, h[q]); ts = pk * (1 - trail)
        if q > s + 1 and o[q] <= ts: r = o[q] / e - 1 - 0.0015; break
        if lo_[q] <= ts: r = ts / e - 1 - 0.0015; break
    return r if r is not None else cl[end - 1] / e - 1 - 0.0015

# ── enumerate anchor bars: z in {Z5,Z11}, dv>=3M; collect bar+1 code + flags ──
def collect(anchor_zs, need_wt_l34red, confirm_codes, **simkw):
    recs = []
    for i in range(n):
        if zz[i] not in anchor_zs:
            continue
        if cl[i] * vol[i] < 3e6:
            continue
        if need_wt_l34red and not (wt[i] == 1 and ll[i] == "L34" and cl[i] < o[i]):
            continue
        tl = last_i[i]
        if i + 1 > tl:
            continue
        b1 = tt[i + 1] if tt[i + 1] else zz[i + 1]
        if b1 not in confirm_codes:
            continue
        ps = pathsim(i + 1, tl, **simkw)
        if ps is None:
            continue
        recs.append((ps, yr[i + 2] if i + 2 <= tl else yr[i + 1], cl[i + 1]))
    return pd.DataFrame(recs, columns=["ps", "yr", "px"])

def summ(D, label):
    if len(D) == 0:
        print(f"  {label:40} n=0"); return
    pm = D.ps.mean() * 100; pmed = D.ps.median() * 100; win = (D.ps > 0).mean() * 100
    tr = D[D.yr.isin(["2021","2022","2023"])]; te = D[D.yr.isin(["2024","2025","2026"])]
    ptr = tr.ps.mean() * 100 if len(tr) else float("nan")
    pte = te.ps.mean() * 100 if len(te) else float("nan")
    yp = sum(1 for y in ["2021","2022","2023","2024","2025","2026"]
             if (D.yr == y).sum() >= 5 and D[D.yr == y].ps.mean() > 0)
    yt = sum(1 for y in ["2021","2022","2023","2024","2025","2026"] if (D.yr == y).sum() >= 5)
    print(f"  {label:40} n={len(D):4}  ps {pm:+6.2f}%  med {pmed:+6.2f}%  win {win:4.1f}%  TR{ptr:+5.1f} TE{pte:+5.1f}  {yp}/{yt}yr+")

T3 = {"T3"}
print("\n═══ 1. ANCHOR ABLATION — does wt_evr+L34red matter? (all Z5|Z11 → T3) ═══")
flag = collect({"Z5","Z11"}, True,  T3);  summ(flag, "Z5|Z11 +wt_evr+L34red → T3 (FLAGSHIP)")
bare = collect({"Z5","Z11"}, False, T3);  summ(bare, "Z5|Z11 (bare) → T3")
print("  → incremental value of the absorption anchor = flagship − bare")

print("\n═══ 2. CONFIRM WIDENING (anchor = Z5|Z11 +wt_evr+L34red) ═══")
summ(collect({"Z5","Z11"}, True, {"T3"}),            "→ T3 only")
summ(collect({"Z5","Z11"}, True, {"T3","T9"}),       "→ T3 or T9")
summ(collect({"Z5","Z11"}, True, {"T3","T9","T11"}), "→ T3 or T9 or T11")
summ(collect({"Z5","Z11"}, True, {"T3","T5","T9","T11"}), "→ T3/T5/T9/T11")

print("\n═══ 3. EXIT-RULE ROBUSTNESS (flagship set) ═══")
summ(collect({"Z5","Z11"}, True, T3),                              "trail25 / stop15 / 60bar (default)")
summ(collect({"Z5","Z11"}, True, T3, trail=0.35, stop=0.25),      "trail35 / stop25 / 60bar")
summ(collect({"Z5","Z11"}, True, T3, trail=0.20, stop=0.10),      "trail20 / stop10 / 60bar")
summ(collect({"Z5","Z11"}, True, T3, fixed_hold=20),              "fixed hold 20 bars (no stop)")
summ(collect({"Z5","Z11"}, True, T3, fixed_hold=10),              "fixed hold 10 bars (no stop)")

print("\n═══ 4. BOOTSTRAP CI on flagship ps-mean (10k resamples) ═══")
ps = flag.ps.to_numpy()
rng = np.random.default_rng(11)
boot = np.array([rng.choice(ps, len(ps), replace=True).mean() * 100 for _ in range(10000)])
lo95, hi95 = np.percentile(boot, [2.5, 97.5])
p_le0 = 100 * np.mean(boot <= 0)
print(f"  flagship n={len(ps)}  ps-mean {ps.mean()*100:+.2f}%  95% CI [{lo95:+.2f}%, {hi95:+.2f}%]  P(mean<=0)={p_le0:.1f}%")

print("\n═══ 5. MULTIPLE-TESTING context ═══")
print("  branches scanned to find this: ~11 bar+1 codes × (Z10/Z5/Z11) ≈ 33 cells.")
print("  Bonferroni-ish: a z=+2.46 single-test p≈0.007 → ×33 ≈ 0.23 (not stringent-significant).")
print("  BUT era-split (6/6yr +, TRAIN & TEST both +) is the stronger evidence than raw z.")
