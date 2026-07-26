"""Ad-hoc 4-bar sequence test (user request, 2026-07-22): T4L3 -> Z1L46 -> Z2L46 -> T9L34.
Fine token (TZ+L) per bar = (t_sig if non-empty else z_sig) + l_sig, mirrors buyseq_context.make_tokens.
Sequence ENDS ON the T9L34 bar (established convention — the anchor bar is the last one,
not excluded). Forward measured path-sim-style (trail25/-15%/60bar/15bps, next-open entry)
from the bar AFTER the anchor. Standard reporting: n, up%, path-sim mean/median, per-year,
TRAIN(2021-23)/TEST(2024-26), 2022-excluded slice, random-same-n z-control."""
import numpy as np, pandas as pd, duckdb, time
t0 = time.time(); S_ = 0.0015
SEQ = ["T4L3", "Z1L46", "Z2L46", "T9L34"]

a = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
D = a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,
    coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
    row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
    FROM bars WHERE close>=5 AND universe<>'index')
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
print(f"frame {len(D):,} ({time.time()-t0:.0f}s)", flush=True)

D["code"] = np.where(D.tt != "", D.tt, D.zz) + D.ll
tk = D.ticker.to_numpy(); code = D.code.to_numpy()
o = D.open.to_numpy(float); h = D.high.to_numpy(float); lo_ = D.low.to_numpy(float); c = D.close.to_numpy(float)
n = len(D)
dep = len(SEQ)

# per-bar path-sim (entry = next bar's open, same standard as all-vs-all scripts)
ps = np.full(n, np.nan)
i = 0
while i < n:
    j = i
    while j + 1 < n and tk[j + 1] == tk[i]: j += 1
    for b in range(i, j):
        e = o[b + 1] * (1 + S_)
        if e <= 0: continue
        pk = e; hd = e * 0.85; end = min(b + 61, j + 1); r = None
        for q in range(b + 1, end):
            if q > b + 1 and o[q] <= hd: r = o[q] / e - 1 - S_; break
            if lo_[q] <= hd: r = -0.15 - S_; break
            pk = max(pk, h[q]); ts = pk * 0.75
            if q > b + 1 and o[q] <= ts: r = o[q] / e - 1 - S_; break
            if lo_[q] <= ts: r = ts / e - 1 - S_; break
        ps[b] = r if r is not None else c[end - 1] / e - 1 - S_
    i = j + 1
print(f"path-sim done ({time.time()-t0:.0f}s)", flush=True)

f20 = pd.Series(c).groupby(tk).shift(-20).to_numpy()
r20 = f20 / c - 1
yr = D.date.astype(str).str[:4].to_numpy()

# find matches: bar idx i is the anchor (T9L34) iff code[i-dep+1:i+1] == SEQ AND all same ticker
hits = []
for i in range(dep - 1, n):
    if tk[i - dep + 1] != tk[i]:
        continue
    ok = True
    for k in range(dep):
        if code[i - dep + 1 + k] != SEQ[k]:
            ok = False; break
    if ok:
        hits.append(i)
hits = np.array(hits, dtype=int)
print(f"raw sequence matches: {len(hits)}", flush=True)

valid = hits[~np.isnan(ps[hits]) & ~np.isnan(r20[hits])]
print(f"with forward data: {len(valid)}", flush=True)

if len(valid) == 0:
    print("NO MATCHES with forward data — sequence too rare or malformed token.")
    raise SystemExit

vy = yr[valid]
vps = ps[valid]; vup = r20[valid] > 0

def block(mask, label):
    nn = int(mask.sum())
    if nn == 0:
        print(f"  {label}: n=0")
        return
    print(f"  {label}: n={nn} up%={100*vup[mask].mean():.1f} ps_mean={100*vps[mask].mean():+.2f}% ps_med={100*np.median(vps[mask]):+.2f}%")

print("\n== ALL YEARS ==")
block(np.ones(len(valid), bool), "2021-2026")
print("\n== PER YEAR ==")
for y in ["2021","2022","2023","2024","2025","2026"]:
    block(vy == y, y)
print("\n== TRAIN(21-23) vs TEST(24-26) ==")
block(np.isin(vy, ["2021","2022","2023"]), "TRAIN")
block(np.isin(vy, ["2024","2025","2026"]), "TEST")
print("\n== EXCLUDING 2022 (bear market) ==")
block(vy != "2022", "ex-2022")

# tickers + dates for inspection
print("\n== SAMPLE HITS (last 15) ==")
samp = D.iloc[valid][["ticker","date"]].copy()
samp["ps"] = vps; samp["up20"] = vup
for _, row in samp.tail(15).iterrows():
    print(f"  {row.ticker:6} {str(row.date)[:10]}  ps={100*row.ps:+.2f}%  up20={'Y' if row.up20 else 'n'}")

# random-same-n control (any bar with forward data, dv-matched not needed for a quick z-control)
rng = np.random.default_rng(42)
allv = np.arange(n)[~np.isnan(ps) & ~np.isnan(r20)]
NREP = 500
rand_means = np.array([100*np.mean(ps[rng.choice(allv, len(valid), replace=False)]) for _ in range(NREP)])
zc = (100*vps.mean() - rand_means.mean()) / rand_means.std()
print(f"\n== RANDOM-SAME-N CONTROL ({NREP} reps) ==")
print(f"  seq ps_mean={100*vps.mean():+.2f}% vs random ps_mean={rand_means.mean():+.2f}% (std {rand_means.std():.2f}) -> z={zc:+.2f}")
print(f"\ndone ({time.time()-t0:.0f}s)")
