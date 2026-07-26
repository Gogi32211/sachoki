"""Follow-up to 260722_conf_vs_scores.py: the full-population Spearman IC is diluted
for every ADDITIVE/sparse score (CONF nonzero only 19.3% of bars) by a huge tie-block
at 0/inactive. Redo the comparison CONDITIONAL on each score being active (!=0) — the
fairer "when this system actually has something to say" comparison, matching how
CONF's conditional IC (+0.1347, n=451,769) was measured."""
import numpy as np, pandas as pd, duckdb, time, os, sys
from scipy import stats as sps
t0 = time.time(); S_ = 0.0015

SCORE_COLS = ["turbo_score", "ultra_score", "ultra_score_v3", "buy_score", "gog_score",
              "beta_score", "rtb_total", "aes_score", "prebreak_score", "profile_score",
              "final_bull_score"]
scoresel = ", ".join(f'"{c}"' for c in SCORE_COLS)

a = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
D = a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,volume,
    {scoresel},
    lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20,
    row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
    FROM bars WHERE close>=5 AND universe<>'index')
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
print(f"frame {len(D):,} ({time.time()-t0:.0f}s)", flush=True)

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

print(f"\n{'score':18} {'active%':>8} {'n_active':>10} {'IC(ps)':>8} {'IC(r20)':>8} {'D0 ps':>8} {'D9 ps':>8} {'spread':>8}")
for name in SCORE_COLS:
    x = D[name].to_numpy(float)
    active = hav & ~np.isnan(x) & (x != 0)
    pct = 100 * active.sum() / hav.sum()
    if active.sum() < 2000:
        print(f"{name:18} {pct:>7.1f}% {active.sum():>10,}   n/a")
        continue
    rho_ps, _ = sps.spearmanr(x[active], ps[active])
    rho_r20, _ = sps.spearmanr(x[active], r20[active])
    xv = x[active]; pv = ps[active]
    try:
        dec = pd.qcut(xv, 10, labels=False, duplicates="drop")
        d0 = 100 * pv[dec == 0].mean(); d9 = 100 * pv[dec == dec.max()].mean()
        spread = d9 - d0
    except Exception:
        d0 = d9 = spread = np.nan
    print(f"{name:18} {pct:>7.1f}% {active.sum():>10,} {rho_ps:>+8.4f} {rho_r20:>+8.4f} {d0:>+8.2f} {d9:>+8.2f} {spread:>+8.2f}")

print(f"\n(for reference — CONF conditional, from prior run: n=451,769 IC(ps)=+0.1347 D0=-3.20 D9=+3.95 spread=+7.15)")
print(f"done ({time.time()-t0:.0f}s)")
