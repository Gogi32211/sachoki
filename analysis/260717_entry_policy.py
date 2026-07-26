"""PHASE 1 — does the ENTRY POLICY matter, as a FAMILY, on top of an edge?

User's idea: edges give us STATE (what/where), but we lack ENTRY TRIGGERS (when to buy).
Before mining rare candlestick patterns (overfitting risk), test the FAMILIES the patterns
are proxies for — large n, honest. Edge fires on bar D; enter under one of:

  IMM      next-open after D           (current default — the baseline)
  GRN      first green bar in (D,D+5]  (any follow-through)
  FTH      first bar close>close[D] & green   (follow-through ABOVE the edge close)
  BRK      first bar close>high[D]     (breakout above the edge bar — expected DEAD)
  PB       first bar low≤low[D] & green (pullback-undercut then reclaim — expected BEST)
  ENG      first bullish-engulf bar    (a specific strong confirmation pattern)

Every policy enters NEXT-OPEN after its trigger bar (causal, no lookahead) and exits on
the SAME rule for all (trail 25% / -15% initial / 60-bar, gap-aware, SLIP 15bps). Two reads:
  (A) FULL — policy on all fires (fewer trades: non-triggering fires are skipped) = the net
      selection+timing effect vs IMM on all fires.
  (B) MATCHED — on the fires where the policy TRIGGERED, policy-entry vs what IMM would have
      returned on that same fire = the pure timing/fill effect, selection removed.
Per year + TRAIN 21-23 / TEST 24-26. Run on several edges: a family effect must repeat.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
import edge_replay as ER

S = 0.0015
TRAIL, HARD, MAXH, WIN = 0.25, 0.15, 60, 5   # WIN = bars after D a trigger may appear


def sim(entry_idx, o, hi, lo, cl, n):
    """Enter at open[entry_idx+1]; trail 25% off peak + -15% hard initial; gap-aware; 60-bar."""
    if entry_idx + 1 >= n:
        return None
    entry = o[entry_idx + 1]
    if entry <= 0:
        return None
    entry *= (1 + S)
    pk = entry
    hard = entry * (1 - HARD)
    end = min(entry_idx + 1 + MAXH, n)
    for j in range(entry_idx + 1, end):
        if j > entry_idx + 1 and o[j] <= hard:          # gap through hard stop
            return o[j] / entry - 1 - S
        if lo[j] <= hard:
            return -HARD - S
        pk = max(pk, hi[j]); ts = pk * (1 - TRAIL)
        if j > entry_idx + 1 and o[j] <= ts:            # gap through trail
            return o[j] / entry - 1 - S
        if lo[j] <= ts:
            return ts / entry - 1 - S
    return cl[end - 1] / entry - 1 - S


def trigger(policy, D, o, hi, lo, cl, n):
    """First trigger-bar index for `policy` in (D, D+WIN], or None. IMM = D itself."""
    if policy == "IMM":
        return D
    for j in range(D + 1, min(D + WIN + 1, n)):
        green = cl[j] > o[j]
        if policy == "GRN" and green:
            return j
        if policy == "FTH" and green and cl[j] > cl[D]:
            return j
        if policy == "BRK" and cl[j] > hi[D]:
            return j
        if policy == "PB" and lo[j] <= lo[D] and green:
            return j
        if policy == "ENG" and green and cl[j] > hi[j - 1] and o[j] < cl[j - 1] \
                and (cl[j] - o[j]) > abs(cl[j - 1] - o[j - 1]):
            return j
    return None


POLICIES = ["IMM", "GRN", "FTH", "BRK", "PB", "ENG"]
EDGES = [("G3-Abs", "E_g3abs"), ("QZ-Capit", "E_qzcapit"),
         ("Atomic", "E_atomic"), ("Cluster3", None)]

grp, as_of = ER._frame(72, 3_000_000)
print(f"as_of {as_of} · {len(grp)} tickers\n", flush=True)


def stats(a):
    a = np.asarray([x for x in a if x is not None])
    if len(a) < 20:
        return None
    return dict(n=len(a), mean=a.mean() * 100, med=np.median(a) * 100,
                win=(a > 0).mean() * 100, pf=(a[a > 0].sum() / -a[a <= 0].sum()) if (a <= 0).any() else np.nan)


for ename, ecol in EDGES:
    recs = []   # (yr, policy, ret, imm_ret, triggered)
    for tk, g in grp.items():
        g = g.reset_index(drop=True); n = len(g)
        if n < 30:
            continue
        o = g["open"].to_numpy(float); hi = g["high"].to_numpy(float)
        lo = g["low"].to_numpy(float); cl = g["close"].to_numpy(float)
        yr = g["date"].astype(str).str[:4].to_numpy()
        emask = (g["conf_n"] >= 3).to_numpy() if ecol is None else g[ecol].to_numpy(bool)
        fires = np.flatnonzero(emask)
        last_entry = -99
        for D in fires:
            if D - last_entry < 5:            # de-dupe overlapping fires (5-bar cooldown)
                continue
            last_entry = D
            imm = sim(D, o, hi, lo, cl, n)
            for p in POLICIES:
                j = trigger(p, D, o, hi, lo, cl, n)
                r = sim(j, o, hi, lo, cl, n) if j is not None else None
                recs.append((yr[D], p, r, imm, r is not None))
    R = pd.DataFrame(recs, columns=["yr", "pol", "ret", "imm", "trig"])
    base_all = stats(R[R.pol == "IMM"]["ret"])
    print("=" * 104)
    print(f"{ename}: {base_all['n']} edge fires (deduped)  ·  entry-policy comparison  "
          f"(trail25/-15%/60bar/slip15)")
    print("=" * 104)
    print(f"{'policy':6} {'cover%':>7} | (A) FULL: {'n':>5} {'mean':>7} {'med':>7} {'win':>6} {'PF':>5} "
          f"| (B) MATCHED vs IMM: {'n':>5} {'polμ':>7} {'immμ':>7} {'Δμ':>7} {'yrs+':>5}")
    for p in POLICIES:
        sub = R[R.pol == p]
        cov = sub["trig"].mean() * 100
        full = stats(sub[sub.trig]["ret"]) if p != "IMM" else base_all
        # matched: fires where THIS policy triggered — its ret vs the imm ret on the same fire
        m = sub[sub.trig & sub["imm"].notna() & sub["ret"].notna()]
        dmu = (m["ret"].mean() - m["imm"].mean()) * 100 if len(m) else np.nan
        # per-year sign of the matched delta
        yp = 0; yt = 0
        for y, gy in m.groupby("yr"):
            if len(gy) < 15:
                continue
            yt += 1; yp += int((gy["ret"].mean() - gy["imm"].mean()) > 0)
        fa = f"{full['n']:5} {full['mean']:+6.2f} {full['med']:+6.2f} {full['win']:5.1f}% {full['pf']:4.2f}" if full else "  too few"
        mb = (f"{len(m):5} {m['ret'].mean()*100:+6.2f} {m['imm'].mean()*100:+6.2f} {dmu:+6.2f} {yp}/{yt}"
              if len(m) >= 20 else "  too few")
        print(f"{p:6} {cov:6.1f}% | {fa} | {mb}")
    print()
