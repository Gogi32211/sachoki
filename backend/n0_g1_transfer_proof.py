"""Execution-equivalence proof for the transferred N0/G1 result.

N0/G1 measured FWER 0.065 over exactly the 31 OPPORTUNITY_LEVEL classes that are now the whole
search space. Reusing that number instead of re-running 200 worlds is legitimate ONLY if the
statistical path is identical, and after 1bf62ca the search module formally changed — the
null-family guard was added to it.

"The guard is only wrapped around the old path" is a claim about code, and this project has
spent two days learning that claims about code are not measurements of code. So the path is
executed again on pre-existing world indices and compared bit for bit against what was stored.

Not a re-validation and not a smaller re-run: 200 worlds are not repeated, and the transferred
number is not recomputed. What is proven is that the machine that produced it and the machine
running now are the same machine on the same inputs.

The comparison is exact, which means the file has to be read exactly: see the note on
`float_precision` below. The first attempt at this proof failed on a 1-ULP difference that the
CSV reader had invented, and reporting that as "the transfer is void" would have been a false
alarm produced entirely by the measurement of the measurement.

Compared for each replayed world:

    band          the max-statistic threshold
    n_clears      cells above it
    n_promoted    cells surviving the bootstrap
    n_final       cells reaching BUILD

Any difference invalidates the transfer, and then N0/G1 must be re-run in full.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as E                                             # noqa: E402
import n0_run as R                                                  # noqa: E402
import n0_spec as N0                                                # noqa: E402
from v2_decision_run import Frozen                                  # noqa: E402

N_REPLAY = 5
BAR = "=" * 112

print(BAR, flush=True)
print(f"  N0/G1 TRANSFER — execution equivalence on {N_REPLAY} pre-existing worlds", flush=True)
print(BAR, flush=True)

# float_precision="round_trip" is not a detail. pandas' default C parser can land one ULP away
# from the value that was written, and the first run of this proof reported NOT EQUIVALENT on a
# world where every printed figure matched — the whole discrepancy was 4.4e-16, introduced by
# reading the file rather than by any change to the code. A bit-for-bit comparison that travels
# through CSV has to ask for a bit-for-bit reader.
stored = pd.read_csv("n0_run.csv", float_precision="round_trip")
G1s = stored[stored["gen"] == "G1"].reset_index(drop=True)
print(f"  stored G1 worlds: {len(G1s)} · FWER_search recorded "
      f"{(G1s.n_promoted > 0).mean():.3f}", flush=True)

O, y_real, dates = CL.load_base(False)
ma = CL.build_masks(O)
classes = E.equivalence_classes(ma, verbose=False)
masks = {c["representative"]: ma[c["representative"]] for c in classes}
g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
fam = E.assert_one_null_family(list(g1))
print(f"  replaying family {fam} · {len(g1)} classes", flush=True)

fam_arr = O["family"].astype(str).to_numpy().astype("U")
_, gi = np.unique(dates, return_inverse=True)
n_dates = int(gi.max() + 1)
key = np.char.add(np.char.add(dates.astype("U"), "|"), fam_arr)
order = np.argsort(key, kind="stable")
ks = key[order]
st = np.r_[0, np.flatnonzero(ks[1:] != ks[:-1]) + 1, len(ks)]
segs = list(zip(st[:-1], st[1:]))
inv = np.empty_like(order)
inv[order] = np.arange(len(order))

sup = E.Support(O, dates, g1, verbose=False)
froz = {c: Frozen(sup, c, gi) for c in sup.cells}
meta = sup.meta.set_index("cell")

print(f"\n  {'world':>6s} {'band':>18s} {'clears':>14s} {'promoted':>12s} {'final':>10s}",
      flush=True)
ok = True
for w in range(N_REPLAY):
    rng = R.krng("G1", w, "outer_null_world")
    yy = y_real[order].copy()
    for a, b in segs:
        if b - a > 1:
            yy[a:b] = rng.permutation(yy[a:b])
    y = yy[inv]
    got = R.run_pipeline(y, froz, sup.cells, meta, gi, n_dates,
                         int(R.krng("G1", w, "inner_chance_band").integers(1, 2**31 - 1)),
                         band_segs=segs, inv=inv, order=order)
    exp = G1s.iloc[w]
    same_band = float(got["band"]) == float(exp["band"])
    same = (same_band and got["n_clears"] == exp["n_clears"]
            and got["n_promoted"] == exp["n_promoted"] and got["n_final"] == exp["n_final"])
    ok &= same
    print(f"  {w:>6d} {got['band']:>9.6f}/{exp['band']:<8.6f} "
          f"{got['n_clears']:>6d}/{int(exp['n_clears']):<6d} "
          f"{got['n_promoted']:>6d}/{int(exp['n_promoted']):<4d} "
          f"{got['n_final']:>5d}/{int(exp['n_final']):<3d}  "
          f"{'MATCH' if same else 'DIFFERS'}", flush=True)

print("\n" + BAR, flush=True)
if ok:
    print("  EQUIVALENT — the guard sits around the OPPORTUNITY_LEVEL path without altering it.",
          flush=True)
    print(f"  N0/G1 transfers: FWER_band {(G1s.n_clears>0).mean():.3f} · "
          f"FWER_search {(G1s.n_promoted>0).mean():.3f} · "
          f"FWER_final {(G1s.n_final>0).mean():.3f} over {len(G1s)} worlds, k = {len(g1)}.",
          flush=True)
else:
    print("  NOT EQUIVALENT — the transfer is void and N0/G1 must be re-run in full.",
          flush=True)
sys.exit(0 if ok else 1)
