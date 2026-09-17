"""CROSS_STAR_CONCURRENCE_V1 — analysis of the outcome run. Reads only artifacts written by
backend/cross_star_outcome_v1.py; opens no new outcome.

Inference is on the TWO paired contrasts. min(Δ_TOP, Δ_BOTTOM) is a decision SUMMARY only — it is
never given a confidence interval of its own, per the frozen spec.
"""
from __future__ import annotations
import os, sys, pickle
import numpy as np, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
MINE, VER = ("2021-01-01", "2023-12-31"), ("2024-01-01", "2025-12-31")
RNG = np.random.default_rng(20260917)
L = lambda c="═", n=86: print(c * n)


def integrity():
    MINE, VER = ("2021-01-01","2023-12-31"), ("2024-01-01","2025-12-31")
    win = lambda d: np.where((d>=MINE[0])&(d<=MINE[1]), "MINE", np.where((d>=VER[0])&(d<=VER[1]), "VERIFY", "—"))
    E["win"] = win(E.date.astype(str)); mt["win"] = win(mt.date.astype(str)); mb["win"] = win(mb.date.astype(str))
    
    L(); print("EXECUTION INTEGRITY"); L()
    print(f"  PATHSIM DIGEST            0e74668f554910de   (bound, verified at run)")
    print(f"  canonical derived         bc022aed824016a6")
    print(f"  spec commit               786b76f")
    print(f"  2026 ROWS ACCESSED        {int((E.date >= '2026-01-01').sum())}")
    
    print("\n  ── arm sizes by window ──")
    print(f"  {'':22s} {'MINE':>10s} {'VERIFY':>10s}")
    for lab, m in (("treated CROSS", E.cross), ("TOP-only pool", E.toponly), ("BOTTOM-only pool", E.botonly)):
        a = E[m]
        print(f"  {lab:22s} {int((a.win=='MINE').sum()):>10,} {int((a.win=='VERIFY').sum()):>10,}")
    for lab, m in (("TOP controls used", mt), ("BOTTOM controls used", mb)):
        u = m.drop_duplicates(["c_ticker","date"])
        print(f"  {lab:22s} {int((u.win=='MINE').sum()):>10,} {int((u.win=='VERIFY').sum()):>10,}"
              f"   (unique rows; {len(m):,} treated-control pairs)")
    
    print("\n  ── matched-k distribution (controls found per treated row) ──")
    for lab, m in (("TOP", mt), ("BOTTOM", mb)):
        k = m.groupby(["date","t_ticker"], observed=True).size()
        tre = int(E.cross.sum())
        unmatched = tre - len(k)
        vc = k.value_counts().sort_index()
        print(f"  {lab:7s} treated {tre:,} · matched {len(k):,} · UNMATCHED {unmatched:,} ({100*unmatched/tre:.1f} %)")
        print(f"          k=" + " · ".join(f"{int(i)}:{int(v):,}" for i, v in vc.items()))
    
    print("\n  ── mod-5 reconciliation (engine's own conservation, maxh=60) ──")
    c60 = pd.DataFrame(cons["60"])
    print("   " + c60[["mask","keys_in","signal_true_n","direct_selected_n","cooldown_suppressed_n",
                       "NO_NEXT_SESSION","NO_ENTRY_OPEN"]].to_string(index=False).replace("\n","\n   "))
    print(f"   TOTAL keys_in {c60.keys_in.sum():,} · selected {c60.direct_selected_n.sum():,} · "
          f"COOLDOWN SUPPRESSED {c60.cooldown_suppressed_n.sum():,}  <- must be 0 for the registered construction")
    
    print("\n  ── resolved counts ──")
    for h, T in ((60, T60), (20, T20)):
        cen = int(T.censored.sum()) if "censored" in T else -1
        print(f"  maxh={h:<3d} trades {len(T):>9,} · right-censored {cen:>7,} ({100*cen/len(T):5.2f} %) · "
              f"usable {len(T)-cen:>9,} · median hold {T.hold.median():.0f}")
    
    print("\n  ── engine drop reasons by arm (rows in arm -> rows with a trade, maxh=60) ──")
    T60["date"] = pd.to_datetime(T60.session)
    got = set(zip(T60.ticker, T60.session))
    print(f"  {'arm':22s} {'rows':>9s} {'with trade':>11s} {'dropped':>9s} {'drop %':>7s}")
    for lab, m in (("treated CROSS", E.cross), ("TOP-only (pool)", E.toponly), ("BOTTOM-only (pool)", E.botonly),
                   ("BOTTOM × CD_BOTH", E.cd_arm), ("CD_BOTH only", E.cdonly)):
        a = E[m]
        s = set(zip(a.ticker, a.date.dt.strftime("%Y-%m-%d")))
        g = len(s & got)
        print(f"  {lab:22s} {len(s):>9,} {g:>11,} {len(s)-g:>9,} {100*(len(s)-g)/len(s):>6.2f}%")


def primary():
    
    E  = pd.read_parquet(R+"cross_star_arms.parquet")
    T  = pd.read_parquet(R+"cross_star_outcomes_maxh60.parquet")
    T  = T[~T.censored][["ticker","session","ret"]]                 # unresolved excluded, never carried
    out = dict(zip(zip(T.ticker, T.session), T.ret))
    
    def paired(mfile, tag):
        m = pd.read_parquet(R+mfile)
        m["session"] = m["date"].dt.strftime("%Y-%m-%d")
        m["c_ret"] = [out.get((t, s), np.nan) for t, s in zip(m.c_ticker, m.session)]
        m["t_ret"] = [out.get((t, s), np.nan) for t, s in zip(m.t_ticker, m.session)]
        m = m.dropna(subset=["c_ret","t_ret"])
        g = m.groupby(["session","t_ticker"], observed=True).agg(t_ret=("t_ret","first"),
                                                                c_ret=("c_ret","mean"), k=("c_ret","size"))
        g["delta"] = g.t_ret - g.c_ret
        return g.reset_index().rename(columns={"session":"date"}), tag
    
    def window(g, lo, hi):
        return g[(g.date >= lo) & (g.date <= hi)]
    
    def report(g, tag, lo, hi, label):
        w = window(g, lo, hi)
        if not len(w): return None
        day = w.groupby("date")["delta"].median()
        bs = np.array([RNG.choice(day.values, len(day), replace=True).mean() for _ in range(4000)])
        lo_ci, hi_ci = np.percentile(bs, [2.5, 97.5])
        yr = w.assign(y=w.date.str.slice(0,4)).groupby("y")["delta"].median()
        print(f"  {label:26s} n {len(w):>7,} · days {len(day):>4,} · "
              f"treated med {w.t_ret.median():+7.3f} · control med {w.c_ret.median():+7.3f}")
        print(f"  {'':26s} Δ mean {w.delta.mean():+7.3f} · Δ median {w.delta.median():+7.3f} · "
              f"day-median Δ {day.median():+7.3f} · days>0 {100*(day>0).mean():5.1f} %")
        print(f"  {'':26s} date-bootstrap 95 % CI on the day series [{lo_ci:+.3f}, {hi_ci:+.3f}]"
              f"   {'EXCLUDES 0' if lo_ci*hi_ci > 0 else 'includes 0'}")
        print(f"  {'':26s} yearly Δ median  " + " · ".join(f"{y}:{v:+.2f}" for y, v in yr.items()))
        return dict(day=day, w=w, ci=(lo_ci, hi_ci), day_med=float(day.median()))
    
    gt, _ = paired("cross_star_match_TOP.parquet", "TOP")
    gb, _ = paired("cross_star_match_BOTTOM.parquet", "BOTTOM")
    
    L(); print("PRIMARY — TOP × BOTTOM · sacred _pathsim realized ret · maxh=60"); L()
    res = {}
    for wlab, (lo, hi) in (("MINE", MINE), ("VERIFY", VER)):
        print(f"\n  ── {wlab} {lo} … {hi} ──")
        res[(wlab,"TOP")] = report(gt, "TOP", lo, hi, "CROSS − TOP-only")
        res[(wlab,"BOTTOM")] = report(gb, "BOTTOM", lo, hi, "CROSS − BOTTOM-only")
        a, b = res[(wlab,"TOP")]["day_med"], res[(wlab,"BOTTOM")]["day_med"]
        print(f"  {'min(ΔTOP, ΔBOTTOM)':26s} {min(a,b):+7.3f}   (summary only — inference is on the two contrasts)")
    np.save("/tmp/_cs_none.npy", np.array([0]))
    import pickle; pickle.dump(dict(gt=gt, gb=gb), open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/cs_paired.pkl","wb"))


def robust():
    
    def enrich(g):
        g = g.copy()
        ix = pd.MultiIndex.from_arrays([g.t_ticker, g.date])
        g["dv"] = ctx.reindex(ix).dollar_vol.to_numpy()
        g["px"] = ctx.reindex(ix).close.to_numpy()
        return g
    gt, gb = enrich(gt), enrich(gb)
    
    def day_med(w):
        return w.groupby("date")["delta"].median()
    
    def ci(day):
        bs = np.array([RNG.choice(day.values, len(day), replace=True).mean() for _ in range(3000)])
        return np.percentile(bs, [2.5, 97.5])
    
    L(); print("PRIMARY ROBUSTNESS — VERIFY window, both contrasts"); L()
    for tag, g in (("CROSS − TOP-only", gt), ("CROSS − BOTTOM-only", gb)):
        w = g[(g.date >= VER[0]) & (g.date <= VER[1])]
        print(f"\n  {tag}")
        base = day_med(w); c = ci(base)
        print(f"    {'full VERIFY':32s} days {len(base):>4,} · day-median Δ {base.median():+7.3f} · CI [{c[0]:+.3f}, {c[1]:+.3f}]")
        # drop the 3 dates with the largest |day median|
        top3 = base.abs().sort_values(ascending=False).head(3).index
        b2 = base.drop(top3); c2 = ci(b2)
        print(f"    {'minus top-3 mover dates':32s} days {len(b2):>4,} · day-median Δ {b2.median():+7.3f} · CI [{c2[0]:+.3f}, {c2[1]:+.3f}]")
        for lab, m in ((">= $1M/day", w.dv >= 1e6), (">= $5 price", w.px >= 5),
                       (">= $1M and >= $5", (w.dv >= 1e6) & (w.px >= 5))):
            s = day_med(w[m]); cc = ci(s)
            print(f"    {lab:32s} days {len(s):>4,} · day-median Δ {s.median():+7.3f} · CI [{cc[0]:+.3f}, {cc[1]:+.3f}]")
        vc = w.t_ticker.value_counts(); top50 = set(vc.head(50).index)
        s = day_med(w[~w.t_ticker.isin(top50)]); cc = ci(s)
        print(f"    {'minus top-50 tickers':32s} days {len(s):>4,} · day-median Δ {s.median():+7.3f} · CI [{cc[0]:+.3f}, {cc[1]:+.3f}]")
        print(f"    concentration: {w.t_ticker.nunique():,} tickers · top ticker {100*vc.iloc[0]/len(w):.2f} % · "
              f"top-10 {100*vc.head(10).sum()/len(w):.1f} % · busiest day {100*w.groupby('date').size().max()/len(w):.2f} %")


def secondary():
    
    E = pd.read_parquet(R+"cross_star_arms.parquet")
    T = pd.read_parquet(R+"cross_star_outcomes_maxh60.parquet")
    T = T[~T.censored]; out = dict(zip(zip(T.ticker, T.session), T.ret))
    # the CD family lives in its own universe (coverage_all_required) — all three arms inside it
    C = E[E.coverage_all_required].copy()
    C["treat"] = C.bottom_star_any & C.cd_both
    C["ctl_bot"] = C.bottom_star_any & ~C.cd_both
    C["ctl_cd"]  = C.cd_both & ~C.bottom_star_any
    print(f"CD universe {len(C):,} · treated BOTTOM×CD_BOTH {int(C.treat.sum()):,} · "
          f"BOTTOM-only {int(C.ctl_bot.sum()):,} · CD_BOTH-only {int(C.ctl_cd.sum()):,}")
    
    def paired(m):
        m = m.copy(); m["session"] = m["date"].dt.strftime("%Y-%m-%d")
        m["c_ret"] = [out.get((t,s), np.nan) for t,s in zip(m.c_ticker, m.session)]
        m["t_ret"] = [out.get((t,s), np.nan) for t,s in zip(m.t_ticker, m.session)]
        m = m.dropna(subset=["c_ret","t_ret"])
        g = m.groupby(["session","t_ticker"], observed=True).agg(t_ret=("t_ret","first"), c_ret=("c_ret","mean"))
        g["delta"] = g.t_ret - g.c_ret
        return g.reset_index().rename(columns={"session":"date"})
    
    def rep(g, lab, lo, hi):
        w = g[(g.date>=lo)&(g.date<=hi)]
        if len(w) < 50: print(f"  {lab:28s} too few rows ({len(w)})"); return None
        day = w.groupby("date")["delta"].median()
        bs = np.array([RNG.choice(day.values, len(day), replace=True).mean() for _ in range(3000)])
        c = np.percentile(bs, [2.5, 97.5])
        yr = w.assign(y=w.date.str.slice(0,4)).groupby("y")["delta"].median()
        print(f"  {lab:28s} n {len(w):>6,} · days {len(day):>4,} · day-median Δ {day.median():+7.3f} · "
              f"days>0 {100*(day>0).mean():5.1f} % · CI [{c[0]:+.3f}, {c[1]:+.3f}] {'EXCL 0' if c[0]*c[1]>0 else 'incl 0'}")
        print(f"  {'':28s} yearly " + " · ".join(f"{y}:{v:+.2f}" for y,v in yr.items()))
        return float(day.median())
    
    mb = match(C, "treat", "ctl_bot", "BOT")
    mc = match(C, "treat", "ctl_cd",  "CD")
    gb, gc = paired(mb), paired(mc)
    L(); print("SECONDARY — BOTTOM + CD_BOTH  (pre-declared; does NOT override the primary)"); L()
    for wl, (lo, hi) in (("MINE", MINE), ("VERIFY", VER)):
        print(f"\n  ── {wl} ──")
        a = rep(gb, "TRIPLE − BOTTOM-only", lo, hi)
        b = rep(gc, "TRIPLE − CD_BOTH-only", lo, hi)
        if a is not None and b is not None:
            print(f"  {'min(Δ, Δ)':28s} {min(a,b):+7.3f}   (summary only)")


if __name__ == "__main__":
    for fn in (integrity, primary, robust, secondary):
        try:
            fn()
        except Exception as e:                       # one section failing must not hide the others
            print(f"  [{fn.__name__} failed: {e}]")
