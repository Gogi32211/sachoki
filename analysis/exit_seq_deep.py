"""exit_seq_deep.py — DEEP validation of the top exit-sequence-miner patterns,
TZ-style: forward LIFT + expectancy + per-year regime + IS/OOS, not just win%.
Separates real edge from high-win-rate noise. ANALYSIS ONLY."""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.zone_events import _seq_sql, _leadin_cols
from ai_journal.db import get_analytics_conn
OOS_FROM = "2024-09-01"

def load(zone="spike", depth=4):
    a = get_analytics_conn()
    df = a.execute(_seq_sql(5.0, depth, _leadin_cols(), zone)).fetchdf(); a.close()
    df = df.drop_duplicates(["ticker", "e_date", "et"])
    df["fwd_10d"] = pd.to_numeric(df["fwd_10d"], errors="coerce")
    df = df[np.isfinite(df["fwd_10d"]) & df["fwd_10d"].between(-90, 500)].copy()
    df["yr"] = pd.to_datetime(df["e_date"]).dt.year
    df["oos"] = df["e_date"].astype(str) >= OOS_FROM
    return df

def stats(s, base_win, base_med):
    f = s["fwd_10d"]
    if len(f) < 10: return None
    return dict(n=len(f), win=round((f > 0).mean() * 100, 1), med=round(float(f.median()), 2),
                mean=round(float(f.clip(-90, 500).mean()), 2),
                lift_win=round((f > 0).mean() * 100 - base_win, 1),
                lift_med=round(float(f.median()) - base_med, 2),
                is_win=round((s.loc[~s.oos, "fwd_10d"] > 0).mean() * 100, 1) if (~s.oos).sum() else None,
                oos_win=round((s.loc[s.oos, "fwd_10d"] > 0).mean() * 100, 1) if s.oos.sum() else None,
                n_oos=int(s.oos.sum()))

def mask(df, toks):
    m = pd.Series(True, index=df.index)
    for sig, k in toks:
        c = f"e{k}_{sig}"
        m = m & (df[c] == 1) if c in df.columns else m & False
    return m

SEQS_UP = [
    ("L34@-2 → VBO↑@-1 → GAP↑@-1", [("l34",2),("vbo_up",1),("gap_up",1)]),
    ("ABS@-2 → EB↑@-1 → PSAR@-1",  [("sig_abs",2),("eb_bull",1),("psar_bull",1)]),
    ("ABS@-2 → EB↑@-1 → CONSO@-1", [("sig_abs",2),("eb_bull",1),("sig_conso",1)]),
    ("ABS@-2 → EB↑@-1 → LVBO@0",   [("sig_abs",2),("eb_bull",1),("pb_lvbo",0)]),
    ("LVBO@-3 → T1G@0 → CONSO@0",  [("pb_lvbo",3),("sig_t1g",0),("sig_conso",0)]),
    ("GAP↑@-3 → BE↑@-2 → T2G@0",   [("gap_up",3),("be_up",2),("sig_t2g",0)]),
    ("Δ↑@-2 → PARA·p@-1 → CONSO@0",[("d_surge_bull",2),("para_prep",1),("sig_conso",0)]),
    ("T2G@-2 → PARA·p@0 → PARA@0", [("sig_t2g",2),("para_prep",0),("para_start",0)]),
    ("CONSO@-3 → ABS@-2 → EB↑@-1", [("sig_conso",3),("sig_abs",2),("eb_bull",1)]),
    # structural 2-token cores (recurring families)
    ("[core] ABS@-1 → EB↑@0",      [("sig_abs",1),("eb_bull",0)]),
    ("[core] EB↑@-1 → CONSO@0",    [("eb_bull",1),("sig_conso",0)]),
    ("[atom] GAP↑@-1 (alone)",     [("gap_up",1)]),
]

def run(df, label, seqs):
    up = df[df.et == "exit_up"]
    bw, bm = round((up.fwd_10d > 0).mean()*100, 1), round(float(up.fwd_10d.median()), 2)
    print(f"\n############ {label}  ·  base win {bw}% / base med {bm} (n={len(up)}) ############")
    print(f"{'sequence':34} {'n':>4} {'win%':>5} {'Lwin':>5} {'med':>6} {'Lmed':>5} {'mean':>6} {'IS→OOS':>12} {'n_oos':>5}")
    rows=[]
    for name, toks in seqs:
        sub = up[mask(up, toks)]
        st = stats(sub, bw, bm)
        if not st: print(f"{name:34}  n<10"); continue
        rows.append((name, st, sub))
        print(f"{name:34} {st['n']:>4} {st['win']:>5} {st['lift_win']:>+5} {st['med']:>6} {st['lift_med']:>+5} {st['mean']:>6} {str(st['is_win'])+'→'+str(st['oos_win']):>12} {st['n_oos']:>5}")
    return rows, up, bw

df = load("spike", 4)
rows, up, bw = run(df, "EXIT↑ (breakout)", SEQS_UP)

# per-year for the 3 best by lift_med
print("\n=== PER-YEAR win% (top sequences by lift_med) ===")
top = sorted([r for r in rows if r[1]['n']>=25], key=lambda r:-r[1]['lift_med'])[:5]
for name, st, sub in top:
    yr=[]
    for y in range(2021,2027):
        sy=sub[sub.yr==y]
        yr.append(f"{y}:{round((sy.fwd_10d>0).mean()*100) if len(sy)>=8 else '—'}({len(sy)})")
    print(f"  {name:34} "+"  ".join(yr))
print("\ndone")
