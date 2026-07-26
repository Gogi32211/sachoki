"""
validate_zone_episodes.py — the PROPERLY-designed Goga-shape experiment (2026-07-08),
fixing the 5 design flaws of tests #1-9:
  1. population: close>=1 & dv>=500k (the accumulation->launch habitat; RGTI@$0.75 excluded
     before!) — tradability caveat acknowledged
  2. unit = ZONE-EPISODE (first signal day after >=5 quiet days), not bar (pseudo-replication)
  3. outcome = EVENT-BASED over 120 bars: P(+50% before -30%), P(+100% before -30%),
     P(-30% first), P(|±50%| either way) — the campaign question, not next-week path-sim
  4. volatility-agnostic metric included (bigmove)
  5. control = RANDOM episodes MATCHED on (year × price-bucket) — kills the cheap-stock
     lottery confound (fib price law)
Shapes = the 7-segment (89..55/55-34/34-21/21-13/13-8/8-5/5-1) fill%% lexicon. READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))

SEGS = [(56, 89), (35, 55), (22, 34), (14, 21), (9, 13), (6, 8), (1, 5)]   # old -> new
HORIZON = 120
QUIET = 5            # a new episode needs >=5 days without the class firing
PBUCK = [1, 3, 8, 21, 89, 1e9]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def _pull():
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        return a.execute("""
            WITH r AS (SELECT ticker,date,open,high,low,close,volume,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=1 AND avg_vol_20d>0)
            SELECT ticker, CAST(date AS VARCHAR)[:10] date, open,high,low,close,
                   close*volume dv
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()


def main():
    t0 = time.time()
    df = _pull()
    df["yr"] = df["date"].str[:4]
    print(f"rows {len(df):,} · tickers {df.ticker.nunique():,} ({time.time()-t0:.0f}s)", flush=True)
    g = df.groupby("ticker", sort=False)
    L = df["low"].to_numpy(float); H = df["high"].to_numpy(float)
    fills = [np.zeros(len(df)) for _ in SEGS]
    for k in range(1, 90):
        po = g["open"].shift(k).to_numpy(float); pc = g["close"].shift(k).to_numpy(float)
        sw = (((po >= L) & (po <= H)) | ((pc >= L) & (pc <= H))) & ~np.isnan(po) & ~np.isnan(pc)
        for si, (lo, hi) in enumerate(SEGS):
            if lo <= k <= hi:
                fills[si] += sw
    F = [fills[si] / (hi - lo + 1) for si, (lo, hi) in enumerate(SEGS)]
    f1, f2, f3, f4, f5, f6, f7 = F
    allmax = np.maximum.reduce(F); allmean = np.mean(F, axis=0)
    old5max = np.maximum.reduce(F[:5])
    print(f"fills done ({time.time()-t0:.0f}s)", flush=True)

    classes = {
        "CRESCENDO":  (f7 >= .6) & (f6 >= .6) & (f5 >= .4) & (((f1 + f2) / 2) <= .2),
        "ESCAPE-OLD": (f1 >= .5) & (((f5 + f6 + f7) / 3) <= .15) & (f2 <= .4),
        "ZONE-BIRTH": (f7 >= .2) & (old5max <= .15) & (f6 <= .4),
        "RETEST":     (np.maximum(f1, f2) >= .25) & (((f3 + f4 + f5) / 3) <= .10) & ((f6 + f7) > 0) & (f7 <= .6),
        "BLUE-SKY":   (allmax <= .25) & (allmean <= .10),
        "FULL-COIL":  (allmean >= .45),
    }

    tick = df["ticker"].to_numpy(); o = df["open"].to_numpy(float)
    hi_ = df["high"].to_numpy(float); lo_ = df["low"].to_numpy(float)
    dv = df["dv"].to_numpy(float); cl = df["close"].to_numpy(float)
    yr = df["yr"].to_numpy(); n = len(df)
    new_tk = np.r_[True, tick[1:] != tick[:-1]]
    eligible = (dv >= 500_000) & (cl >= 1)

    def episodes(mask):
        """first signal day after >=QUIET quiet days, per ticker (causal)."""
        m = pd.Series(mask & ~new_tk, index=df.index)
        prior = m.astype(float).groupby(df["ticker"]).transform(
            lambda s: s.shift(1).rolling(QUIET, min_periods=1).sum()).fillna(0).to_numpy()
        return np.where(mask & (prior == 0) & eligible)[0]

    def outcomes(idx):
        """event race from next-day open over HORIZON bars."""
        res = []
        for i in idx:
            if i + 2 >= n or tick[i + 1] != tick[i]:
                continue
            ep = o[i + 1]
            if ep <= 0:
                continue
            w50 = w100 = l30 = big = False
            end = min(i + 1 + HORIZON, n)
            for j in range(i + 1, end):
                if tick[j] != tick[i]:
                    break
                up = hi_[j] / ep - 1; dn = lo_[j] / ep - 1
                if not (w50 or l30):
                    if dn <= -0.30: l30 = True
                    elif up >= 0.50: w50 = True
                if w50 and not w100 and not l30 and up >= 1.00:
                    w100 = True
                if up >= 0.50 or dn <= -0.50:
                    big = True
            res.append((w50, w100, l30, big, yr[i]))
        return res

    def summarize(res):
        if not res:
            return None
        arr = np.array([(a, b, c, d) for a, b, c, d, _ in res], dtype=float)
        return {"n": len(res), "win50": arr[:, 0].mean() * 100, "win100": arr[:, 1].mean() * 100,
                "lose30": arr[:, 2].mean() * 100, "big": arr[:, 3].mean() * 100}

    # matched random pool: eligible bars with a forward runway
    pool = np.where(eligible)[0]
    pool = pool[pool < n - 5]
    pb = np.digitize(cl, PBUCK)          # price bucket per row
    pool_strat = {}
    for i in pool:
        pool_strat.setdefault((yr[i], pb[i]), []).append(i)

    def matched_random(idx, seed):
        r = np.random.default_rng(seed)
        out = []
        need = {}
        for i in idx:
            need[(yr[i], pb[i])] = need.get((yr[i], pb[i]), 0) + 1
        for key, cnt in need.items():
            cand = pool_strat.get(key, [])
            if not cand:
                continue
            out.extend(r.choice(cand, size=min(cnt, len(cand)), replace=False))
        return np.array(out)

    print(f"\n{'class':12s}{'n':>7} | {'win50%':>7}{'win100%':>8}{'lose30%':>8}{'big±50%':>8} | matched-random (2 seeds)")
    per_year_store = {}
    for nm, mask in classes.items():
        idx = episodes(mask)
        res = outcomes(idx)
        s = summarize(res)
        if s is None:
            print(f"{nm:12s} n=0"); continue
        rands = []
        for seed in (1, 2):
            rr = summarize(outcomes(matched_random(idx, seed + hash(nm) % 97)))
            rands.append(rr)
        rstr = " · ".join(f"w50 {r['win50']:.1f} w100 {r['win100']:.1f} l30 {r['lose30']:.1f}" for r in rands if r)
        print(f"{nm:12s}{s['n']:>7} | {s['win50']:>6.1f}%{s['win100']:>7.1f}%{s['lose30']:>7.1f}%{s['big']:>7.1f}% | {rstr}", flush=True)
        per_year_store[nm] = res
    # per-year win50 for each class
    print("\nper-year win50% (class):")
    for nm, res in per_year_store.items():
        by = {}
        for a, b, c, d, y in res:
            by.setdefault(y, []).append(a)
        line = " ".join(f"{y[2:]}:{np.mean(v)*100:4.1f}(n{len(v)})" for y, v in sorted(by.items()))
        print(f"  {nm:12s} {line}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
