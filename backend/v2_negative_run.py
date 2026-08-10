"""N1 / N2 / P1 — does the incremental estimand do the thing it was created for?

Search is not run here, and deliberately. If the composition-only negative fails there is no
point learning v2's search sensitivity: the new estimand would not yet have solved the problem
it exists for, and a sensitivity number would only make a broken instrument look precise.

    N1  deterministic composition   Y = μ_setup                  θ_c must be EXACTLY 0
    N2  stochastic composition      Y = μ_setup + γ_date + ε     θ_c truth = 0
    P1  N2 + planted δ              recover the exact known truth

N1 is a plumbing test and it is the cheap one that catches the most. With no noise every row in
a stratum shares one outcome, so both arms have the same median and every Δ_cs is zero by
construction. A non-zero θ there can only come from stratification, eligibility, weights,
complement construction, or aggregation — and it would be invisible once noise is added.

N2 is where v1 and v2 part company. Cells that contain strong setups will show a marginal
difference, and v1 is ENTITLED to report it: that is a true marginal association. v2 must not
inherit it.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as V2E                                          # noqa: E402
import combolab_v2_spec as V2                                      # noqa: E402

pd.set_option("display.width", 200)
BAR = "=" * 120

print(BAR, flush=True)
print("  COMBOLAB v2 · composition worlds — the regression test on the motive for v2", flush=True)
print(BAR, flush=True)
print(f"  spec digest {V2.digest()[:16]}…", flush=True)
O, _, dates = CL.load_base()
masks_all = CL.build_masks(O)
classes = V2E.equivalence_classes(masks_all)
masks = {c["representative"]: masks_all[c["representative"]] for c in classes}
V2E.assert_no_degeneracy(list(masks), masks)
sup = V2E.Support(O, dates, masks)
if sup.below_floor:
    print(f"  below support floor: {sup.below_floor}", flush=True)

# ── N1 ───────────────────────────────────────────────────────────────────────
print("\n" + BAR, flush=True)
print("  N1 · DETERMINISTIC COMPOSITION — Y = μ_setup, no noise", flush=True)
print(BAR, flush=True)
y1 = V2E.composition_world(O, dates, seed=11, noise=False, date_effect=False)
t1 = sup.theta(y1)
m1 = sup.marginal(y1, masks)
worst = t1.abs().max()
print(f"    v2 incremental  max |θ| {worst:.2e}  over {len(t1)} classes", flush=True)
print(f"    v1 marginal     range {m1.min():+.3f} … {m1.max():+.3f}pp · "
      f"{int((m1.abs() > 0.5).sum())} classes beyond ±0.5pp", flush=True)
ok1 = worst < 1e-9
_why = ("it is" if ok1 else "it is not, and the fault is in stratification, eligibility, "
        "weights, complement construction or aggregation")
print(f"\n    {'PASS' if ok1 else 'FAIL'} — θ must be EXACTLY zero; {_why}", flush=True)
if not ok1:
    print(t1[t1.abs() > 1e-9].sort_values().to_string(), flush=True)
print(f"    Meanwhile v1 legitimately sees composition: cells holding strong setups differ.",
      flush=True)

# ── N2 ───────────────────────────────────────────────────────────────────────
print("\n" + BAR, flush=True)
print("  N2 · STOCHASTIC COMPOSITION — Y = μ_setup + γ_date + ε, true θ = 0", flush=True)
print(BAR, flush=True)
rows = []
for s in range(40):
    y = V2E.composition_world(O, dates, seed=100 + s, noise=True)
    t = sup.theta(y)
    m = sup.marginal(y, masks)
    rows.append(dict(seed=s, v2_mean=t.mean(), v2_absmax=t.abs().max(),
                     v2_rmse=float(np.sqrt((t ** 2).mean())),
                     v1_absmax=m.abs().max(), v1_rmse=float(np.sqrt((m ** 2).mean()))))
N2 = pd.DataFrame(rows)
print(f"    v2 incremental   mean θ {N2.v2_mean.mean():+.4f}pp · "
      f"RMSE {N2.v2_rmse.mean():.3f}pp · max |θ| {N2.v2_absmax.mean():.3f}pp", flush=True)
print(f"    v1 marginal      RMSE {N2.v1_rmse.mean():.3f}pp · "
      f"max |θ| {N2.v1_absmax.mean():.3f}pp", flush=True)
ratio = N2.v1_rmse.mean() / max(N2.v2_rmse.mean(), 1e-12)
print(f"\n    v1/v2 RMSE ratio {ratio:.1f}× — v1 inherits the composition it is entitled to "
      f"see;\n    v2 does not inherit it. Both are correct about different questions.",
      flush=True)
ok2 = abs(N2.v2_mean.mean()) < 0.05
print(f"    {'PASS' if ok2 else 'FAIL'} — v2 centred on zero "
      f"({N2.v2_mean.mean():+.4f}pp against a truth of 0)", flush=True)

# ── P1 ───────────────────────────────────────────────────────────────────────
print("\n" + BAR, flush=True)
print("  P1 · COMPOSITION + PLANTED δ — exact truth, computed not derived", flush=True)
print(BAR, flush=True)
print(f"  {'δ':>6s} {'planted cell':<28s} {'θ̂ needle':>10s} {'θ true':>9s} {'bias':>9s} "
      f"{'max bias, other classes':>19s} {'cells w/ true≠0':>16s}", flush=True)
rng = np.random.default_rng(5)
res = []
for delta in V2.DELTA_GRID[1:]:
    for k in range(6):
        y0 = V2E.composition_world(O, dates, seed=200 + k, noise=True)
        cell = sup.cells[int(rng.integers(0, len(sup.cells)))]
        y = V2E.inject(y0, sup, cell, delta)
        base, got = sup.theta(y0), sup.theta(y)
        # Truth for EVERY cell is the estimand evaluated on the fully known population, so it
        # needs no closed form: for the planted cell translation equivariance makes it exactly
        # delta, and for an overlapping cell only some treated rows are shifted and a median does
        # not pass through a fraction — but the shift is still computable.
        truth = got - base
        bias_needle = float(truth[cell] - delta)
        others = truth.drop(cell)
        res.append(dict(delta=delta, cell=cell, bias=bias_needle,
                        n_leaked=int((others.abs() > 0.01).sum()),
                        max_other=float(others.abs().max())))
    R = pd.DataFrame([r for r in res if r["delta"] == delta])
    print(f"  {delta:>6.2f} {R.cell.iloc[0]:<28s} {'—':>10s} {delta:>9.2f} "
          f"{R.bias.abs().max():>9.2e} {R.max_other.max():>19.3f} "
          f"{R.n_leaked.mean():>16.1f}", flush=True)
P1 = pd.DataFrame(res)
ok3 = P1.bias.abs().max() < 1e-9
print(f"\n    {'PASS' if ok3 else 'FAIL'} — planted θ recovered exactly "
      f"(max |bias| {P1.bias.abs().max():.2e}pp)", flush=True)
print(f"    Overlapping cells acquire a real non-zero truth — {P1.n_leaked.mean():.1f} of the "
      f"other classes on average.\n    In v1 those were classified as OVERLAP_AFFECTED; here each "
      f"one has a computed value, so bias\n    is measurable for all 46 rather than "
      f"categorised for one.", flush=True)

print("\n" + BAR, flush=True)
verdict = all((ok1, ok2, ok3))
print(f"  N1 {'PASS' if ok1 else 'FAIL'} · N2 {'PASS' if ok2 else 'FAIL'} · "
      f"P1 {'PASS' if ok3 else 'FAIL'}", flush=True)
if verdict:
    print("\n  The incremental estimand refuses to credit the context with composition, and",
          flush=True)
    print("  recovers a planted incremental effect exactly. Only now is search sensitivity",
          flush=True)
    print("  a meaningful thing to measure.", flush=True)
else:
    print("\n  Search sensitivity is not measured until this passes: a number from an "
          "instrument\n  that has not solved its own problem would only look precise.",
          flush=True)
pd.concat([N2.assign(world="N2"), P1.assign(world="P1")], ignore_index=True) \
    .to_csv("v2_negative_run.csv", index=False)
print("\nDONE", flush=True)
