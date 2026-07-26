"""CONF vs existing score systems (2026-07-22, user request): does the all-vs-all CONF
score add anything over turbo_score/ultra_score/ultra_score_v3/buy_score/gog_score/
beta_score/rtb_total/aes_score/prebreak_score/profile_score?

Standard: dv>=3M, close>=5, non-index, per-bar path-sim (ps, trail25/-15/60/15bps) +
fwd-20 up% (r20). For each score: Spearman rank-IC vs ps AND r20 (full available
population), Q5-Q1 ps decile spread, coverage. Then pairwise Spearman correlation
between CONF and each other score (redundant vs orthogonal). CONF computed LIVE via
conf_score.py (the actual serving code, not a re-derivation) so this is apples-to-apples
with what the screener shows."""
import numpy as np, pandas as pd, duckdb, time, os, sys
from scipy import stats as sps
t0 = time.time(); S_ = 0.0015

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # backend/
import conf_score as cs
raw = cs.needed_raw_columns(include_ext=False)   # core-only CONF for this comparison
rawsel = ", ".join(f'coalesce(CAST("{c}" AS TINYINT),0) AS "{c}"' for c in raw)

SCORE_COLS = ["turbo_score", "ultra_score", "ultra_score_v3", "buy_score", "gog_score",
              "beta_score", "rtb_total", "aes_score", "prebreak_score", "profile_score",
              "final_bull_score"]
scoresel = ", ".join(f'"{c}"' for c in SCORE_COLS)

a = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
D = a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,cci_20,
    coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
    coalesce(bar_gap_class,'') gap, coalesce(vol_bucket,'') vb, coalesce(wyc_phase,'') wp,
    coalesce(setup_tokens,'') sut, coalesce(context_tokens,'') cxt,
    {scoresel}, {rawsel},
    lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20,
    row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
    FROM bars WHERE close>=5 AND universe<>'index')
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
print(f"frame {len(D):,} cols {len(D.columns)} ({time.time()-t0:.0f}s)", flush=True)

o=D.open.to_numpy(float);h=D.high.to_numpy(float);lo_=D.low.to_numpy(float);c=D.close.to_numpy(float)
tk=D.ticker.to_numpy(); n=len(D); ps=np.full(n,np.nan)
i=0
while i<n:
    j=i
    while j+1<n and tk[j+1]==tk[i]: j+=1
    for b in range(i,j):
        e=o[b+1]*(1+S_)
        if e<=0: continue
        pk=e; hd=e*0.85; end=min(b+61,j+1); r=None
        for q in range(b+1,end):
            if q>b+1 and o[q]<=hd: r=o[q]/e-1-S_; break
            if lo_[q]<=hd: r=-0.15-S_; break
            pk=max(pk,h[q]); ts=pk*0.75
            if q>b+1 and o[q]<=ts: r=o[q]/e-1-S_; break
            if lo_[q]<=ts: r=ts/e-1-S_; break
        ps[b]=r if r is not None else c[end-1]/e-1-S_
    i=j+1
print(f"path-sim done ({time.time()-t0:.0f}s)", flush=True)

r20 = (D.f20 / D.close - 1).to_numpy()
dv = (D.close * D.volume).to_numpy()
hav = (dv >= 3e6) & ~np.isnan(r20) & ~np.isnan(ps)
print(f"eligible bars (dv>=3M etc): {hav.sum():,}", flush=True)

# ── CONF (core, live-serving code) — MEMORY-SAFE chunked aggregation ──────────
# cs.compute()'s detail/top-k step stacks one float64 array PER CELL (869 cells x
# 4.2M rows = ~29GB) — this box has 16GB RAM. Skip detail entirely; only need the
# final score. Uses the exact same cs.cells()/cs.build_features() (live-serving
# code), just a lighter aggregator, processed in row-chunks to bound memory.
e20 = pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=20, adjust=False).mean()).to_numpy()
e50 = pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=50, adjust=False).mean()).to_numpy()
e200 = pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=200, adjust=False).mean()).to_numpy()
Dx = D.copy()
Dx["e20"] = e20; Dx["e50"] = e50; Dx["e200"] = e200
Dx["tt"] = D.tt; Dx["zz"] = D.zz; Dx["ll"] = D.ll; Dx["gap"] = D.gap; Dx["vb"] = D.vb; Dx["wp"] = D.wp

Q = cs.cells()
F = cs.build_features(Dx)   # booleans, 1 byte/elem — ~270 x 4.2M x 1B ≈ 1.1GB, fine whole
CHUNK = 700_000
conf = np.zeros(n, dtype=np.float32)
for start in range(0, n, CHUNK):
    end = min(start + CHUNK, n)
    bull = np.zeros(end - start, dtype=np.float32)
    bear = np.zeros(end - start, dtype=np.float32)
    side_best = {"BULL": {}, "BEAR": {}}
    for _, r in Q.iterrows():
        if r.a not in F or r.b not in F:
            continue
        m = F[r.a][start:end] & F[r.b][start:end]
        if not m.any():
            continue
        cand = np.where(m, np.float32(r.ps), np.float32(0.0))
        for feat in (r.a, r.b):
            prev = side_best[r.dir].get(feat)
            side_best[r.dir][feat] = cand if prev is None else np.where(np.abs(cand) > np.abs(prev), cand, prev)
    for arr in side_best["BULL"].values():
        bull += np.maximum(arr, 0)
    for arr in side_best["BEAR"].values():
        bear += np.minimum(arr, 0)
    conf[start:end] = bull + bear
    print(f"  CONF chunk {start:,}-{end:,} done ({time.time()-t0:.0f}s)", flush=True)
print(f"CONF computed ({time.time()-t0:.0f}s) — nonzero {100*(conf!=0).mean():.1f}%", flush=True)

SCORES = {c2: D[c2].to_numpy(float) for c2 in SCORE_COLS}
SCORES["CONF"] = conf

def spearman_ic(x, mask):
    m = mask & ~np.isnan(x)
    if m.sum() < 500: return None, int(m.sum())
    rho, _ = sps.spearmanr(x[m], ps[m])
    return rho, int(m.sum())

def spearman_ic_r20(x, mask):
    m = mask & ~np.isnan(x)
    if m.sum() < 500: return None
    rho, _ = sps.spearmanr(x[m], r20[m])
    return rho

def decile_spread(x, mask):
    m = mask & ~np.isnan(x)
    if m.sum() < 2000: return None, None, None
    xv = x[m]; pv = ps[m]
    try:
        dec = pd.qcut(xv, 10, labels=False, duplicates="drop")
    except Exception:
        return None, None, None
    d0 = pv[dec == 0].mean(); d9 = pv[dec == dec.max()].mean()
    return 100*d0, 100*d9, 100*(d9-d0)

print("\n══ Spearman rank-IC vs path-sim (full available population) ══")
results = []
for name, x in SCORES.items():
    mask = hav.copy()
    if name == "CONF":
        pass  # keep zeros in — additive score, 0 = "no confluence" is meaningful
    rho, nn = spearman_ic(x, mask)
    ricr20 = spearman_ic_r20(x, mask)
    d0, d9, spread = decile_spread(x, mask)
    cov = 100 * (~np.isnan(x) & hav).sum() / hav.sum()
    results.append((name, rho, ricr20, nn, cov, d0, d9, spread))

results.sort(key=lambda r: -(r[1] or -99))
print(f"{'score':18} {'IC(ps)':>8} {'IC(r20)':>8} {'n':>10} {'cov%':>6} {'D0 ps':>8} {'D9 ps':>8} {'spread':>8}")
for name, rho, ricr20, nn, cov, d0, d9, spread in results:
    rs = f"{rho:+.4f}" if rho is not None else "  n/a"
    r2 = f"{ricr20:+.4f}" if ricr20 is not None else "  n/a"
    d0s = f"{d0:+.2f}" if d0 is not None else "n/a"
    d9s = f"{d9:+.2f}" if d9 is not None else "n/a"
    sps_ = f"{spread:+.2f}" if spread is not None else "n/a"
    print(f"{name:18} {rs:>8} {r2:>8} {nn:>10,} {cov:>5.1f}% {d0s:>8} {d9s:>8} {sps_:>8}")

print("\n══ CONF-only subset (conf != 0) — how it looks when it actually fires ══")
mconf = hav & (conf != 0)
rho_c, nn_c = spearman_ic(conf, mconf)
d0c, d9c, spreadc = decile_spread(conf, mconf)
print(f"  n={nn_c:,}  IC(ps)={rho_c:+.4f}  D0={d0c:+.2f}%  D9={d9c:+.2f}%  spread={spreadc:+.2f}pp")

print("\n══ pairwise Spearman correlation: CONF vs each existing score (redundancy check) ══")
for name, x in SCORES.items():
    if name == "CONF": continue
    m = hav & ~np.isnan(x) & ~np.isnan(conf)
    if m.sum() < 500:
        print(f"  CONF vs {name:18} n/a (n={m.sum()})"); continue
    rho, _ = sps.spearmanr(conf[m], x[m])
    print(f"  CONF vs {name:18} rho={rho:+.4f}  (n={m.sum():,})")

print(f"\ndone ({time.time()-t0:.0f}s)")
