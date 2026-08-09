"""Is maxh=60 the right holding cap for the whole board, or just an untested default? (2026-08-09)

`maxh=60` has never been measured. It has been the default in every path-sim since the engine
was written, and the CLA<0 study accidentally exposed it: on that cell, 60 → 90 bars nearly
doubled the mean (+9.20 → +17.15) with 6/6 years intact. ⚡ATR×12 was born exactly this way —
one cell raised a question that turned out to be a book-wide law.

So: sweep maxh over 40/60/90/120 on every setup with enough trades, ⚡ATR×12 exit, and count
how many improve on median AND on worst year. A plateau across 90-120 means the cap was
genuinely cutting moves short; a single peak at 90 means curve-fitting.

⚠ THE COUNTERWEIGHT, and it may well outweigh the gain: the portfolio study established that
SLOT CONTENTION is the binding constraint, not signal quality. The board fires ~114×/day and at
the current envelope the account only ever takes 0.03% of them. Extending the median hold from
60 to 90 bars occupies each slot 50% longer, i.e. it cuts the number of trades the account can
take by roughly a third. A per-trade median that improves while the trade count falls can still
LOSE money at the account level. Per-trade numbers are structurally blind to this, so the mean
hold is reported for every cell and the decision must not be made on median alone.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

HORIZONS = [40, 60, 90, 120]
MIN_N = 150
N_TRIALS = 8
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS} · horizons {HORIZONS} · min n {MIN_N}\n",
      flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(er.SETUPS)} setups in the registry", flush=True)


def stats(tr):
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    return dict(n=len(tr), med=tr["ret"].median() * 100, mean=tr["ret"].mean() * 100,
                win=w.mean() * 100,
                pf=(tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                hold=tr["hold"].mean(), yrs=int((ym > 0).sum()), nyr=len(ym),
                worst=ym.min())


rows, skipped = [], []
for i, (name, col) in enumerate(er.SETUPS, 1):
    base = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(base) < MIN_N:
        skipped.append((name, len(base)))
        continue
    rec = {"setup": name}
    for H in HORIZONS:
        tr = base if H == 60 else er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, H,
                                              atr_k=12.0)
        s = stats(tr)
        for k, v in s.items():
            rec[f"{k}_{H}"] = v
    rows.append(rec)
    if i % 20 == 0:
        print(f"  ...{i}/{len(er.SETUPS)} setups", flush=True)

R = pd.DataFrame(rows)
print(f"\nmeasured {len(R)} setups · skipped {len(skipped)} for n<{MIN_N} "
      f"(NOT a silent cap — listed at the end)", flush=True)

# ── the headline counts, each horizon vs the current default 60 ───────────────
print("\n===== 1. how many setups improve vs the current maxh=60 =====", flush=True)
print(f"  {'horizon':>8s} {'med better':>12s} {'worst better':>14s} {'both':>7s} "
      f"{'Δmed avg':>10s} {'Δworst avg':>11s} {'Δhold avg':>10s}", flush=True)
for H in HORIZONS:
    if H == 60:
        continue
    dm = R[f"med_{H}"] - R["med_60"]
    dw = R[f"worst_{H}"] - R["worst_60"]
    dh = R[f"hold_{H}"] - R["hold_60"]
    print(f"  {H:>8d} {int((dm>0).sum()):>5d}/{len(R):<6d} "
          f"{int((dw>0).sum()):>6d}/{len(R):<7d} {int(((dm>0)&(dw>0)).sum()):>7d} "
          f"{dm.mean():>+10.2f} {dw.mean():>+11.2f} {dh.mean():>+10.1f}", flush=True)

# ── plateau check ─────────────────────────────────────────────────────────────
print("\n===== 2. plateau — do 90 and 120 agree? =====", flush=True)
agree = ((R["med_90"] > R["med_60"]) & (R["med_120"] > R["med_60"])).sum()
only90 = ((R["med_90"] > R["med_60"]) & (R["med_120"] <= R["med_60"])).sum()
print(f"  both 90 and 120 beat 60: {agree}/{len(R)}  ({agree/len(R)*100:.0f}%)", flush=True)
print(f"  only 90 beats 60 (peak, not plateau): {only90}/{len(R)}", flush=True)
print(f"  median Δ 60→90 {(R['med_90']-R['med_60']).median():+.2f}  ·  "
      f"90→120 {(R['med_120']-R['med_90']).median():+.2f}", flush=True)

# ── pooled family view ────────────────────────────────────────────────────────
print("\n===== 3. pooled across every measured setup =====", flush=True)
print(f"  {'H':>4s} {'med':>7s} {'mean':>7s} {'win':>6s} {'pf':>6s} {'hold':>6s} "
      f"{'worst':>7s}", flush=True)
for H in HORIZONS:
    print(f"  {H:>4d} {R[f'med_{H}'].median():>+7.2f} {R[f'mean_{H}'].median():>+7.2f} "
          f"{R[f'win_{H}'].median():>6.1f} {R[f'pf_{H}'].median():>6.2f} "
          f"{R[f'hold_{H}'].median():>6.1f} {R[f'worst_{H}'].median():>+7.2f}", flush=True)

# ── ⚠ the account-level counterweight ─────────────────────────────────────────
print("\n===== 4. ⚠ the slot cost — what longer holds take AWAY =====", flush=True)
h60 = R["hold_60"].median(); h90 = R["hold_90"].median(); h120 = R["hold_120"].median()
print(f"  median hold: 60→{h60:.1f} bars · 90→{h90:.1f} · 120→{h120:.1f}", flush=True)
for H, hh in [(90, h90), (120, h120)]:
    thr = hh / h60
    dm = (R[f"med_{H}"] - R["med_60"]).median()
    print(f"  maxh {H}: hold ×{thr:.2f} → the account takes ~{(1/thr)*100:.0f}% as many "
          f"trades, for {dm:+.2f}pp more per trade", flush=True)
    print(f"           break-even needs Δmed ≥ {(thr-1)*R['med_60'].median():+.2f}pp "
          f"— {'CLEARS' if dm >= (thr-1)*R['med_60'].median() else 'FAILS'} it", flush=True)

# ── biggest movers, both directions ───────────────────────────────────────────
print("\n===== 5. per-setup, 60 → 90 =====", flush=True)
R["d90"] = R["med_90"] - R["med_60"]
show = ["setup", "n_60", "med_60", "med_90", "d90", "worst_60", "worst_90",
        "hold_60", "hold_90"]
print("  --- 15 biggest gains ---", flush=True)
print(R.nlargest(15, "d90")[show].to_string(index=False, float_format=lambda x: f"{x:.2f}"),
      flush=True)
print("\n  --- 10 biggest losses ---", flush=True)
print(R.nsmallest(10, "d90")[show].to_string(index=False, float_format=lambda x: f"{x:.2f}"),
      flush=True)

R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "maxh_sweep.csv"),
         index=False)
print(f"\nfull table → maxh_sweep.csv", flush=True)
if skipped:
    print(f"\nskipped for n<{MIN_N}: " + ", ".join(f"{n}({c})" for n, c in skipped[:40]),
          flush=True)
print("\nDONE", flush=True)
