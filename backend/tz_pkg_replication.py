"""Re-analysis of the 5YR TZ ANALYTICS package with what we know now (2026-08-07).

The package holds 232,952 sequence rules — (universe, signal, seq3) with medians, win/fail
and a per-year regime flag — computed INDEPENDENTLY on 1D, 4H and 1H. Its own conclusions
were drawn without a multiplicity control, and we have since learned (the 🏦 sweep) that a
233k-hypothesis search manufactures thousands of "GOOD" cells from pure noise.

But the package contains something it never used: the SAME rule measured in up to 9
independent cells (3 timeframes x 3 universes). Noise does not replicate across
independently computed slices. That is a free, very strong multiplicity filter.

  1. merge all three TFs; a cell counts as TESTED only if n >= 100 (so "absent" is not
     confused with "failed")
  2. replication score = # of tested cells where m10 > 0 AND status != REJECT
  3. PERMUTATION CONTROL: shuffle the outcome columns within each (tf, universe) block and
     recount — how many rules reach each replication level by chance?
  4. survivors decomposed into tokens and checked against the edges we already own
  5. finalists get path-simmed on OUR data with the validated ⚡ATR×12 exit
"""
import os, sys, glob
import numpy as np
import pandas as pd

SP = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
      "aba6fbf4-ff3b-4f32-9bd8-48f188d02d96/scratchpad/tz")
MIN_N = 100

frames = []
for tf in ["1D", "4H", "1H"]:
    d = pd.read_csv(os.path.join(SP, tf, "rule_database_sequences_5yr.csv"))
    d["tf"] = tf
    frames.append(d)
A = pd.concat(frames, ignore_index=True)
print(f"all rules: {len(A):,} · tested (n>={MIN_N}): {(A['n']>=MIN_N).sum():,}", flush=True)

A = A[A["n"] >= MIN_N].copy()
A["cell"] = A["tf"] + "/" + A["universe"]
A["ok"] = (A["m10"] > 0) & (A["status"] != "REJECT")
A["stable"] = A["regime"] == "STABLE"
A["rule"] = A["signal"] + " ← " + A["seq3"]

g = A.groupby("rule")
R = pd.DataFrame({
    "tested": g["cell"].nunique(),
    "good": g["ok"].sum(),
    "stable": g["stable"].sum(),
    "n_tot": g["n"].sum(),
    "m10_med": g["m10"].median(),
    "win_med": g["win"].median(),
    "fail_med": g["fail"].median(),
    "tfs": g["tf"].nunique(),
    "unis": g["universe"].nunique(),
})
R["rep"] = R["good"]
print(f"distinct rules with >=1 tested cell: {len(R):,}", flush=True)
print(f"  tested in >=4 cells: {(R['tested']>=4).sum():,} · >=6: {(R['tested']>=6).sum():,}",
      flush=True)

# ── permutation control ─────────────────────────────────────────────────────────
rng = np.random.default_rng(17)
obs = {k: int(((R["tested"] >= 4) & (R["rep"] >= k)).sum()) for k in range(3, 10)}
sh_counts = {k: [] for k in obs}
for it in range(5):
    B = A.copy()
    # shuffle the OUTCOME within each (tf, universe) block — keeps every cell size,
    # every rule's presence pattern, and each block's overall success rate intact
    B["ok"] = B.groupby("cell")["ok"].transform(lambda s: rng.permutation(s.to_numpy()))
    gb = B.groupby("rule")
    Rb = pd.DataFrame({"tested": gb["cell"].nunique(), "rep": gb["ok"].sum()})
    for k in obs:
        sh_counts[k].append(int(((Rb["tested"] >= 4) & (Rb["rep"] >= k)).sum()))

print("\n===== REPLICATION vs CHANCE (rules tested in >=4 of 9 cells) =====", flush=True)
print(f"  {'rep>=k':>7s} {'observed':>9s} {'shuffled':>10s} {'enrichment':>11s} {'excess':>8s}",
      flush=True)
for k in sorted(obs):
    mu = float(np.mean(sh_counts[k]))
    enr = obs[k] / mu if mu > 0 else float("inf")
    print(f"  {k:>7d} {obs[k]:>9,} {mu:>10.1f} {enr:>11.2f} {obs[k]-mu:>8.0f}", flush=True)

# ── the survivors ───────────────────────────────────────────────────────────────
S = R[(R["tested"] >= 6) & (R["rep"] >= R["tested"] - 1) & (R["stable"] >= 2)]
S = S.sort_values(["rep", "m10_med"], ascending=False)
print(f"\n===== SURVIVORS: tested>=6 cells, good in all-but-one, STABLE in >=2 =====",
      flush=True)
print(f"  {len(S)} rules\n", flush=True)
print(f"  {'rule':38s} {'cells':>6s} {'good':>5s} {'stbl':>5s} {'n':>8s} {'m10':>7s} "
      f"{'win':>6s} {'fail':>6s}", flush=True)
for r, row in S.head(30).iterrows():
    print(f"  {r:38s} {int(row['tested']):>6d} {int(row['good']):>5d} {int(row['stable']):>5d} "
          f"{int(row['n_tot']):>8,} {row['m10_med']:>+7.2f} {row['win_med']:>6.1f} "
          f"{row['fail_med']:>6.1f}", flush=True)

# per-TF consistency of the survivors (is it a 1D thing that leaks, or truly fractal?)
print("\n===== survivors: per-TF median m10 =====", flush=True)
sub = A[A["rule"].isin(S.index)]
pt = sub.pivot_table(index="rule", columns="tf", values="m10", aggfunc="median")
pt["ALL"] = S["m10_med"]
print(pt.head(30).round(2).to_string(), flush=True)

S.to_csv("/Users/sachoki/Desktop/sachoki-desktop/backend/tz_pkg_survivors.csv")
print("\nsaved -> backend/tz_pkg_survivors.csv", flush=True)

# ── token decomposition: what are these made of? ─────────────────────────────────
print("\n===== token frequency: survivors vs all tested rules =====", flush=True)
def toks(idx):
    out = []
    for r in idx:
        sig, seq = r.split(" ← ")
        out.extend([t.strip() for t in seq.split("|")] + [sig])
    return pd.Series(out).value_counts(normalize=True) * 100
ts, ta = toks(S.index), toks(R[R["tested"] >= 4].index)
cmp = pd.DataFrame({"survivors%": ts, "all%": ta}).dropna()
cmp["lift"] = cmp["survivors%"] / cmp["all%"]
print(cmp.sort_values("lift", ascending=False).head(14).round(2).to_string(), flush=True)
print("\nDONE", flush=True)
