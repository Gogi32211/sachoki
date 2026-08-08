"""Step 3 — the only defensible question left after the multiplicity control.

Step 2b verdict: the gate-passing COUNT is 12.4% noise vs 19.3% observed at 2-bar
(1.56x, ~36% real) and 17.4% vs 16.9% at 3-bar (0.97x — the 3-bar league's survivor
count is INDISTINGUISHABLE from noise). So "passed the gates" proves nothing on its own.
Two things can still be real:
  (a) individual cells whose median clears the NOISE CEILING (best cell any shuffle
      produced: +2.57 / +2.68), and
  (b) MOTIFS — sub-patterns that recur across several different top cells. Noise does
      not cluster into a shared motif; that recurrence is itself the evidence.

The step-2 top-3-bar list is dominated by one motif: T4|L3 → Z9|L25 (and Z9|L25 as an
ending) appears in 4 of the top 8. This script tests that motif as a FAMILY (pooled over
all prefixes), plus the L46-triple Z1G|L46 → Z2G|L46 → T5|L46, against:
  - their own single-bar components (is the sequence more than the sum of its parts?)
  - a within-motif permutation ceiling
  - per-year and 2024-26 OOS
"""
import os, sys
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)

con = duckdb.connect(ANALYTICS_DB, read_only=True)
D = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS cl,
           coalesce(any_value(t_sig),'') AS t, coalesce(any_value(z_sig),'') AS z,
           coalesce(any_value(l_sig),'') AS l, coalesce(any_value(full_suffix),'') AS sfx,
           any_value(rsi_14) AS rsi
    FROM bars WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
D = D[D["ticker"].isin(BANK)].reset_index(drop=True)
tk = D["ticker"].to_numpy(); cl = D["cl"].to_numpy(float)
f = np.full(len(D), np.nan); f[:-10] = cl[10:] / cl[:-10] - 1
b = np.zeros(len(D), bool); b[:-10] = tk[10:] != tk[:-10]; f[b] = np.nan
F = np.clip(f * 100, -60, 60)
Y = D["dt"].str[:4].to_numpy()
tz = np.where(D["t"].to_numpy() != "", D["t"].to_numpy(), D["z"].to_numpy())
ls = np.where(D["l"].to_numpy() != "", D["l"].to_numpy(), "·")
ST = np.where(tz != "", np.char.add(np.char.add(tz.astype(str), "|"), ls.astype(str)), "")


def sb(a, k, fill=""):
    out = np.full(len(a), fill, dtype=object); out[k:] = a[:-k]
    bad = np.zeros(len(a), bool)
    for j in range(1, k + 1):
        t = np.zeros(len(a), bool); t[j:] = tk[j:] != tk[:-j]; bad |= t
    out[bad] = fill
    return out


S0 = ST; S1 = sb(ST, 1); S2 = sb(ST, 2)
ok = (~np.isnan(F)) & (S0 != "")
GLOB = np.nanmedian(F[ok])
rng = np.random.default_rng(11)
print(f"🏦 bars {int(ok.sum()):,} · baseline {GLOB:+.3f}\n", flush=True)


def stat(mask, label, ceiling=True):
    m = ok & mask
    n = int(m.sum())
    if n < 40:
        print(f"  {label:46s} n={n} — too thin", flush=True); return None
    v = F[m]; y = Y[m]
    ym = pd.Series(v).groupby(pd.Series(y)).median()
    med = float(np.median(v))
    oos = v[y >= "2024"]
    om = float(np.median(oos)) if len(oos) >= 25 else float("nan")
    # permutation ceiling: what median would a random set of the same size show?
    ceil = ""
    if ceiling:
        idx = np.flatnonzero(ok)
        draws = [float(np.median(rng.choice(F[idx], size=n, replace=False))) for _ in range(200)]
        p = float(np.mean([d >= med for d in draws]))
        ceil = f"  p={p:.3f}" + ("" if p >= 0.005 else " ***")
    print(f"  {label:46s} n={n:>5d} med{med:>+7.2f} Δ{med-GLOB:>+6.2f} "
          f"yr{int((ym>0).sum())}/{len(ym)} worst{ym.min():>+6.2f} OOS{om:>+6.2f}{ceil}", flush=True)
    return dict(n=n, med=med, pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()), oos=om)


print("===== MOTIF A: the Z9|L25 family =====", flush=True)
stat(S0 == "Z9|L25", "Z9|L25 alone (entry bar)")
stat((S1 == "T4|L3") & (S0 == "Z9|L25"), "T4|L3 → Z9|L25  (the 2-bar motif)")
stat((S1 == "T4|L3"), "T4|L3 alone (prior bar)")
stat((S2 == "Z3|L25") & (S1 == "T4|L3") & (S0 == "Z9|L25"), "Z3|L25 → T4|L3 → Z9|L25 (step-2 top)")
stat((S1 == "T4|L3") & (S0 == "Z9|L25") & (S2 != "Z3|L25"), "OTHER prefix → T4|L3 → Z9|L25")
print("  — is it T4 specifically, or any prior bar? —", flush=True)
for pre in ["T4|L12", "T4|L34", "T4|L46", "T4|L25", "T1|L3", "T3|L3", "T5|L3", "Z3|L3"]:
    stat((S1 == pre) & (S0 == "Z9|L25"), f"{pre} → Z9|L25", ceiling=False)
print("  — is it Z9|L25 specifically, or any Z9 / any L25? —", flush=True)
for end in ["Z9|L12", "Z9|L46", "Z9|L34", "Z9|L3", "Z9|L5"]:
    stat((S1 == "T4|L3") & (S0 == end), f"T4|L3 → {end}", ceiling=False)

print("\n===== MOTIF B: the all-L46 triple =====", flush=True)
stat((S2 == "Z1G|L46") & (S1 == "Z2G|L46") & (S0 == "T5|L46"), "Z1G|L46 → Z2G|L46 → T5|L46")
stat((S1 == "Z2G|L46") & (S0 == "T5|L46"), "Z2G|L46 → T5|L46 (2-bar core)")
stat(S0 == "T5|L46", "T5|L46 alone")
print("  — does the all-L46 chain matter, or just the TZ codes? —", flush=True)
tzs = np.array([s.split("|")[0] if s else "" for s in ST], dtype=object)
z1g = sb(tzs, 2); z2g = sb(tzs, 1)
stat((z1g == "Z1G") & (z2g == "Z2G") & (tzs == "T5"), "Z1G → Z2G → T5 (any L-lines)", ceiling=False)
stat((z1g == "Z1G") & (z2g == "Z2G") & (tzs == "T5") &
     ~((S2 == "Z1G|L46") & (S1 == "Z2G|L46") & (S0 == "T5|L46")), "  same, NOT all-L46", ceiling=False)

print("\n===== control: the step-2 suppressors =====", flush=True)
stat((S1 == "T6|L3") & (S0 == "Z1|L5"), "T6|L3 → Z1|L5  (0/6 in step 2)")
stat((S1 == "Z2|L46") & (S0 == "T1G|L34"), "Z2|L46 → T1G|L34 (0/6)")
print("\nDONE", flush=True)
