"""
seq_revalidate_v2.py — HONEST re-validation of the Robust-Seq rule DB (option B).

Design (2026-07-06, per user spec):
  · MINE on TRAIN 2021-2023 ONLY (frozen), min_n=50 in train, depths 2/3/4.
  · TEST on 2024-2026 (true OOS): per-year med20 AND trail25 stop-first path-sim
    (the tradeable number — precomputed once for every liquid bar).
  · DSR per rule: TEST path-sim trades deflated against N = all mined candidates.
  · Verdict tiers:  🏆 OOS✓ = TEST path-sim med>0 AND ≥2/3 TEST yrs med20>0
                    🔬 in-sample-only = mined but failed OOS.
Output: seq_rules_v2.json (tiers + per-year detail) + printed summary.
READ-ONLY on bars.
"""
from __future__ import annotations
import json, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import seq_analytics as SA
from overfit_stats import sharpe, dsr

OUT = os.path.join(os.path.dirname(__file__), "seq_rules_v2.json")
SLIP = 0.0015
TRAIN_YRS = {"2021", "2022", "2023"}
TEST_YRS = {"2024", "2025", "2026"}


def pathsim_all(tf="1d", dv_floor=3_000_000):
    """trail25 stop-first return for EVERY liquid bar (entry next open), once."""
    import duckdb
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        d = a.execute(f"""
            WITH r AS (SELECT ticker, date, open, high, low, close, volume,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0)
            SELECT ticker, CAST(date AS VARCHAR) date, open, high, low, close,
                   close*volume dv FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    o = d.open.to_numpy(float); h = d.high.to_numpy(float)
    l = d.low.to_numpy(float); c = d.close.to_numpy(float)
    tick = d.ticker.to_numpy(); n = len(d)
    ret = np.full(n, np.nan)
    for i in range(n):
        if i + 1 >= n or tick[i + 1] != tick[i]:
            continue
        ep = o[i + 1] * (1 + SLIP)
        if ep <= 0:
            continue
        pk = ep; end = i + 1; r = None
        for j in range(i + 1, min(i + 61, n)):
            if tick[j] != tick[i]:
                break
            end = j; tsl = pk * 0.75
            if j > i + 1 and o[j] <= tsl:
                r = o[j] / ep - 1 - SLIP; break
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                r = ts / ep - 1 - SLIP; break
        ret[i] = r if r is not None else c[end] / ep - 1 - SLIP
    d["ps"] = ret
    d["key"] = d.ticker + "|" + d.date.str[:10]
    return dict(zip(d["key"], d["ps"]))


def main():
    t0 = time.time()
    print("pulling frame (tokens + fwd)…", flush=True)
    df = SA._pull(tf="1d")
    df["key"] = df.ticker + "|" + df.date.astype(str).str[:10]
    print(f"frame {len(df):,} ({time.time()-t0:.0f}s) — path-sim all bars…", flush=True)
    psmap = pathsim_all()
    df["ps"] = df["key"].map(psmap)
    print(f"path-sim mapped ({time.time()-t0:.0f}s)", flush=True)

    # sequence strings for all depths, once
    d = df.sort_values(["ticker", "date"]).copy()
    g = d.groupby("ticker")["tok"]
    laggs = {k: g.shift(k).astype(str) for k in (1, 2, 3)}
    seqs = {2: laggs[1] + " " + d.tok,
            3: laggs[2] + " " + laggs[1] + " " + d.tok,
            4: laggs[3] + " " + laggs[2] + " " + laggs[1] + " " + d.tok}

    rules = {}
    N_mined = 0
    for depth, s in seqs.items():
        d["seq"] = s
        sub = d[d.r20.notna() & ~d.seq.str.contains("nan", na=False)]
        tr = sub[sub.yr.isin(TRAIN_YRS)]
        te = sub[sub.yr.isin(TEST_YRS)]
        # mine on TRAIN
        grp = tr.groupby("seq")
        cand = []
        for sq, sg in grp:
            if len(sg) < 50:
                continue
            ym = sg.groupby("yr")["r20"].median()
            if sg.r20.median() <= 0 or (ym > 0).sum() < 2:
                continue                       # loose in-sample gate; real gate = OOS
            cand.append(sq)
        N_mined += len(cand)
        print(f"depth {depth}: mined {len(cand)} candidates ({time.time()-t0:.0f}s)", flush=True)
        te_g = {k: v for k, v in te.groupby("seq") if k in set(cand)}
        tr_g = {k: v for k, v in grp if k in set(cand)}
        for sq in cand:
            sg_tr = tr_g[sq]; sg_te = te_g.get(sq)
            rec = {"depth": depth,
                   "n_train": int(len(sg_tr)),
                   "med20_train": round(sg_tr.r20.median() * 100, 2),
                   "train_yrs": {y: round(v * 100, 2) for y, v in sg_tr.groupby("yr")["r20"].median().items()}}
            if sg_te is None or len(sg_te) < 20:
                rec.update({"tier": "no-oos-data", "n_test": int(0 if sg_te is None else len(sg_te))})
            else:
                ps = sg_te.ps.dropna()
                ym_te = sg_te.groupby("yr")["r20"].median()
                rec.update({
                    "n_test": int(len(sg_te)),
                    "med20_test": round(sg_te.r20.median() * 100, 2),
                    "ps_med_test": round(ps.median() * 100, 2),
                    "ps_mean_test": round(ps.mean() * 100, 2),
                    "ps_win_test": round((ps > 0).mean() * 100, 1),
                    "test_yrs": {y: round(v * 100, 2) for y, v in ym_te.items()},
                    "sr_test": round(sharpe(ps.to_numpy()), 4),
                })
                ok = (ps.median() > 0) and int((ym_te > 0).sum()) >= 2
                rec["tier"] = "OOS_VERIFIED" if ok else "in-sample-only"
            rules[sq] = rec
    # DSR for the verified tier (deflate vs ALL mined)
    fam = [r["sr_test"] for r in rules.values() if "sr_test" in r]
    for sq, r in rules.items():
        if r.get("tier") == "OOS_VERIFIED":
            # rebuild trade series? sr stored; dsr needs series — approximate via psr on stats
            pass
    # second pass for DSR with actual series (only verified, bounded set)
    ver = [sq for sq, r in rules.items() if r.get("tier") == "OOS_VERIFIED"]
    print(f"\nmined {N_mined} → OOS-verified {len(ver)} ({time.time()-t0:.0f}s) — DSR pass…", flush=True)
    for depth, s in seqs.items():
        d["seq"] = s
        te = d[d.yr.isin(TEST_YRS) & d.seq.isin([v for v in ver if rules[v]["depth"] == depth])]
        for sq, sg in te.groupby("seq"):
            ps = sg.ps.dropna().to_numpy()
            if len(ps) >= 20:
                rules[sq]["dsr"] = dsr(ps, fam, n_trials=max(N_mined, len(fam)))["dsr"]
    with open(OUT, "w") as f:
        json.dump({"as_of": time.strftime("%Y-%m-%d"), "mined": N_mined,
                   "train": "2021-2023", "test": "2024-2026", "min_n_train": 50,
                   "rules": rules}, f)
    # summary
    R = pd.DataFrame([{**{"seq": k}, **v} for k, v in rules.items()])
    ver_df = R[R.tier == "OOS_VERIFIED"].sort_values("ps_med_test", ascending=False)
    print(f"\n===== SUMMARY =====")
    print(f"mined(train,n>=50,loose): {N_mined} · OOS_VERIFIED: {len(ver_df)} ({len(ver_df)/max(N_mined,1)*100:.1f}%)")
    print(f"in-sample-only: {(R.tier=='in-sample-only').sum()} · no-oos-data: {(R.tier=='no-oos-data').sum()}")
    if len(ver_df):
        print(f"\nTOP-25 OOS-verified by TEST path-sim median:")
        print(f"{'seq':34s} {'d':>2s} {'nTr':>5s} {'nTe':>5s} {'medTr':>6s} {'medTe':>6s} {'psMed':>6s} {'psWin':>5s} {'DSR':>5s}")
        for _, r in ver_df.head(25).iterrows():
            print(f"{r.seq:34s} {r.depth:>2d} {r.n_train:>5d} {r.n_test:>5d} {r.med20_train:>6.2f} "
                  f"{r.med20_test:>6.2f} {r.ps_med_test:>6.2f} {r.ps_win_test:>5.1f} {r.get('dsr', float('nan')):>5.2f}")
        print(f"\nDSR>=0.6 among verified: {(ver_df.get('dsr', pd.Series(dtype=float)) >= 0.6).sum()}")
    print(f"\ndone {time.time()-t0:.0f}s → {OUT}")


if __name__ == "__main__":
    main()
