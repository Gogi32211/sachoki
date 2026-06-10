"""exit_seq_robust.py — robustness pass: is the median-lift real once the +500%
lottery tail is removed? Tight-clip mean (25%), median lift vs first-exit base,
and HONEST per-year coverage (>=6 samples). ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.zone_events import _seq_sql, _leadin_cols
from ai_journal.db import get_analytics_conn
OOS_FROM = "2024-09-01"

def load(zone_def, vol_min=5.0, depth=4):
    a = get_analytics_conn()
    df = a.execute(_seq_sql(vol_min, depth, _leadin_cols(), zone_def=zone_def)).fetchdf(); a.close()
    df = df.drop_duplicates(["ticker","e_date","et"]); df = df[df.ev_seq==1]
    df["fwd_10d"] = pd.to_numeric(df["fwd_10d"], errors="coerce")
    df = df[np.isfinite(df.fwd_10d) & df.fwd_10d.between(-90,500)].copy()
    df["yr"] = pd.to_datetime(df.e_date).dt.year
    return df

def mask(pop, toks):
    m = pd.Series(True, index=pop.index)
    for sg,k in toks:
        c=f"e{k}_{sg}"; m = m & (pop[c]==1) if c in pop.columns else (m & False)
    return m

# the candidates that survived (REAL or strong) in the all-panels pass
CAND = {
 ("spike","exit_up"): [
   ("ABS→EB→PSAR",      [("sig_abs",2),("eb_bull",1),("psar_bull",1)]),
   ("ABS→EB→LVBO",      [("sig_abs",2),("eb_bull",1),("pb_lvbo",0)]),
   ("ABS→EB→CONSO",     [("sig_abs",2),("eb_bull",1),("sig_conso",1)]),
   ("Δ↑→PARA·p→CONSO",  [("d_surge_bull",2),("para_prep",1),("sig_conso",0)]),
 ],
 ("spike","exit_down"): [
   ("L34→Ab→R2L",       [("l34",2),("d_absorb_bull",2),("r2l_os",0)]),
   ("L34→Ab→c=O",       [("l34",2),("d_absorb_bull",2),("close_o",0)]),
   ("R2L→GAP↑→V×10",    [("r2l_os",2),("gap_up",1),("sig_vol_10x",0)]),
   ("atomic→atomic→GAP↑",[("atomic",2),("atomic",1),("gap_up",0)]),
 ],
 ("spike25","exit_down"): [
   ("VBO↑→R2L→atomic",  [("vbo_up",3),("r2l_os",2),("atomic",2)]),
   ("VBO↑→R2L→GAP↑",    [("vbo_up",3),("r2l_os",2),("gap_up",2)]),
   ("SQ→c=O→V×10",      [("sq",3),("close_o",2),("sig_vol_10x",0)]),
 ],
 ("vb","exit_down"): [
   ("SQ→PARA·p→PSAR",   [("sq",1),("para_prep",1),("psar_bull",0)]),
   ("c=O→LVBO→CONSO",   [("close_o",3),("pb_lvbo",2),("sig_conso",2)]),
   ("c=O→LVBO→c=O",     [("close_o",3),("pb_lvbo",2),("close_o",1)]),
 ],
 ("vb","exit_up"): [
   ("VBO↑→BO↑→W·SOS",   [("vbo_up",1),("bo_up",1),("w2_sos",0)]),
   ("T1G→EB→VBO↑",      [("sig_t1g",2),("eb_bull",1),("vbo_up",1)]),
 ],
}

for (zone,et), seqs in CAND.items():
    pop = load(zone); pop = pop[pop.et==et]
    bf = pop.fwd_10d
    b_med = float(bf.median()); b_m25 = float(bf.clip(-25,25).mean())
    print(f"\n### {zone} · {et}   base: median {b_med:+.2f}  clip25-mean {b_m25:+.2f}  (n={len(pop)})")
    print(f"  {'sequence':20} {'n':>4} {'win':>5} {'medLift':>8} {'m25':>6} {'m25Lift':>8}   per-year win% (>=6 samp)")
    for name, toks in seqs:
        sub = pop[mask(pop,toks)]; f = sub.fwd_10d; n=len(f)
        if n<25: print(f"  {name:20} n={n} <25"); continue
        med=float(f.median()); m25=float(f.clip(-25,25).mean())
        yrs=[]
        for y in range(2021,2027):
            sy=sub[sub.yr==y].fwd_10d
            yrs.append(f"{y%100}:{round((sy>0).mean()*100)}({len(sy)})" if len(sy)>=6 else f"{y%100}:–")
        print(f"  {name:20} {n:>4} {round((f>0).mean()*100,1):>5} {med-b_med:>+8.2f} {m25:>+6.2f} {m25-b_m25:>+8.2f}   "+" ".join(yrs))
print("\ndone")
