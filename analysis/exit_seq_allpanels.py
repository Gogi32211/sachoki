"""exit_seq_allpanels.py — DEEP validation across ALL 6 miner panels:
3 vol classes (spike>=5x / spike 2-5x / VB) x 2 directions (exit_up / exit_down).
For every top 'holds' sequence the panel shows, recompute the REAL discriminators:
forward expectancy (mean), median, per-year regime, IS/OOS, Wilson-LB.
Punchline = how many LB-holds actually carry positive expectancy. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.zone_events import _seq_sql, _leadin_cols, exit_sequences, _wilson_lb
from ai_journal.db import get_analytics_conn

OOS_FROM = "2024-09-01"
ZONES = [("spike", 5.0, "spike>=5x"), ("spike25", 5.0, "spike 2-5x"), ("vb", 5.0, "VB class")]
DIRS  = ["exit_up", "exit_down"]

def load(zone_def, vol_min, depth=4):
    sigs = _leadin_cols()
    a = get_analytics_conn()
    df = a.execute(_seq_sql(vol_min, depth, sigs, zone_def=zone_def)).fetchdf(); a.close()
    df = df.drop_duplicates(["ticker", "e_date", "et"])
    df = df[df["ev_seq"] == 1]
    df["fwd_10d"] = pd.to_numeric(df["fwd_10d"], errors="coerce")
    df = df[np.isfinite(df["fwd_10d"]) & df["fwd_10d"].between(-90, 500)].copy()
    df["yr"] = pd.to_datetime(df["e_date"]).dt.year
    df["oos"] = df["e_date"].astype(str) >= OOS_FROM
    return df

def toks_of(combo):
    out = []
    for p in (combo.get("a"), combo.get("b"), combo.get("c")):
        if p and "@-" in p:
            sg, off = p.split("@-")
            out.append((sg, int(off)))
    return out

def mask(pop, toks):
    m = pd.Series(True, index=pop.index)
    for sg, k in toks:
        c = f"e{k}_{sg}"
        m = m & (pop[c] == 1) if c in pop.columns else (m & False)
    return m

def deep(pop, toks, bw, bm):
    sub = pop[mask(pop, toks)]
    f = sub["fwd_10d"]
    n = len(f)
    if n < 25: return None
    wins = int((f > 0).sum())
    win = wins / n * 100
    is_ = sub[~sub.oos]["fwd_10d"]; oo = sub[sub.oos]["fwd_10d"]
    peryr = {}
    for y in range(2021, 2027):
        sy = sub[sub.yr == y]["fwd_10d"]
        peryr[y] = round((sy > 0).mean()*100) if len(sy) >= 8 else None
    pos_years = sum(1 for y,v in peryr.items() if v is not None and (f.median() and v >= 50))
    nyears = sum(1 for v in peryr.values() if v is not None)
    return dict(n=n, win=round(win,1), lift_win=round(win-bw,1),
                med=round(float(f.median()),2), lift_med=round(float(f.median())-bm,2),
                mean=round(float(f.clip(-90,500).mean()),2),
                oos_win=round((oo>0).mean()*100,1) if len(oo) else None,
                n_oos=len(oo), oos_lb=round(_wilson_lb((oo>0).sum(), len(oo))*100,1) if len(oo) else None,
                peryr=peryr, pos_years=pos_years, nyears=nyears)

summary = []
for zone_def, vol_min, zlabel in ZONES:
    pop_all = load(zone_def, vol_min)
    for et in DIRS:
        pop = pop_all[pop_all.et == et]
        if len(pop) < 50: continue
        f = pop["fwd_10d"]
        bw, bm, bmean = round((f>0).mean()*100,1), round(float(f.median()),2), round(float(f.clip(-90,500).mean()),2)
        res = exit_sequences(event_type=et, depth=4, horizon=10, vol_min=vol_min,
                             min_n=30, top=25, ways=3, first_only=True, zone_def=zone_def)
        rows = []
        for c in res.get("best", []):
            d = deep(pop, toks_of(c), bw, bm)
            if d: rows.append((c.get("sequence"), d))
        # gates
        lb_hold  = [r for r in rows if r[1]["oos_lb"] is not None and r[1]["oos_lb"] > bw]
        exp_pos  = [r for r in rows if r[1]["mean"] > 0 and r[1]["med"] > 0]
        real     = [r for r in lb_hold if r[1]["mean"] > 0 and r[1]["med"] > 0
                    and r[1]["pos_years"] >= max(4, r[1]["nyears"]-1)]
        print(f"\n{'='*120}\n### {zlabel}  ·  {et}   base: win {bw}%  median {bm}  mean {bmean}  (n={len(pop)}, exits)")
        print(f"  sequences>=25n: {len(rows)}   LB-holds: {len(lb_hold)}   expectancy>0: {len(exp_pos)}   REAL(LB+exp+5/6yr): {len(real)}")
        print(f"  {'sequence':46} {'n':>4} {'win':>5} {'med':>6} {'mean':>7} {'oosLB':>6} {'+yr':>5}  verdict")
        for seq, d in sorted(rows, key=lambda r:-r[1]["mean"])[:14]:
            s = " ".join(f"{x['bar']}:{x['signal']}" for x in (seq or []))
            v = "REAL" if (seq,d) in real else ("exp+ noLB" if d["mean"]>0 and d["med"]>0 else
                 ("LB-trap" if (seq,d) in lb_hold else "—"))
            print(f"  {s[:46]:46} {d['n']:>4} {d['win']:>5} {d['med']:>6} {d['mean']:>+7} "
                  f"{('LB'+str(d['oos_lb'])):>6} {str(d['pos_years'])+'/'+str(d['nyears']):>5}  {v}")
        summary.append((zlabel, et, bmean, len(rows), len(lb_hold), len(exp_pos), len(real)))

print(f"\n\n{'#'*100}\n### SUMMARY — LB-holds vs REAL expectancy edge, per panel")
print(f"  {'panel':22} {'base_mean':>9} {'#seq':>5} {'#LBhold':>8} {'#exp>0':>7} {'#REAL':>6}")
for zl, et, bmean, nr, nlb, nexp, nreal in summary:
    print(f"  {zl+' '+et:22} {bmean:>+9} {nr:>5} {nlb:>8} {nexp:>7} {nreal:>6}")
print("\ndone")
