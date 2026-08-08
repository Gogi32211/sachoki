"""Step 2b — how many of step-2's survivors are real? (multiplicity control)

Step 2 tested 1,742 two-bar and 3,647 three-bar cells and 337 / 612 passed the gates
(19% / 17%). That pass-rate only means something against the rate PURE NOISE would
produce under the same gates. So: shuffle fwd10 across bars (kills all real structure,
keeps every cell size and every year/OOS split intact), re-run the identical gate
counting, repeat. Excess = observed − shuffled = the honest count of real findings, and
observed/shuffled is the enrichment factor.

Also reports, per shuffle, the best cell noise produces — the bar a real finding must clear.
"""
import os, sys
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB

BASE = os.path.dirname(os.path.abspath(__file__))
MIN_N = 100
NSHUF = 5

SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)
con = duckdb.connect(ANALYTICS_DB, read_only=True)
D = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS cl,
           coalesce(any_value(t_sig),'') AS t, coalesce(any_value(z_sig),'') AS z,
           coalesce(any_value(l_sig),'') AS l
    FROM bars WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
D = D[D["ticker"].isin(BANK)].reset_index(drop=True)
tk = D["ticker"].to_numpy(); cl = D["cl"].to_numpy(float)
f = np.full(len(D), np.nan); f[:-10] = cl[10:] / cl[:-10] - 1
bad = np.zeros(len(D), bool); bad[:-10] = tk[10:] != tk[:-10]; f[bad] = np.nan
F = np.clip(f * 100, -60, 60)
Y = D["dt"].str[:4].to_numpy()
tz = np.where(D["t"].to_numpy() != "", D["t"].to_numpy(), D["z"].to_numpy())
ls = np.where(D["l"].to_numpy() != "", D["l"].to_numpy(), "·")
state = np.where(tz != "", np.char.add(np.char.add(tz.astype(str), "|"), ls.astype(str)), "")
vocab = sorted(set(state[state != ""])); V = {s: i + 1 for i, s in enumerate(vocab)}
code = np.array([V.get(s, 0) for s in state], dtype=np.int64); NV = len(vocab) + 1


def shift_back(a, k):
    out = np.zeros(len(a), dtype=np.int64); out[k:] = a[:-k]
    b = np.zeros(len(a), dtype=bool)
    for j in range(1, k + 1):
        t = np.zeros(len(a), dtype=bool); t[j:] = tk[j:] != tk[:-j]; b |= t
    out[b] = 0
    return out


c1 = shift_back(code, 1); c2 = shift_back(code, 2)
ok = (~np.isnan(F)) & (code > 0)
GLOB = np.nanmedian(F[ok])
K2 = c1 * NV + code
K3 = c2 * NV**2 + c1 * NV + code


def count_pass(keys, valid, vals):
    m = ok & valid
    d = pd.DataFrame({"k": keys[m], "f": vals[m], "y": Y[m]})
    n = d.groupby("k")["f"].size()
    keep = n[n >= MIN_N].index
    d = d[d["k"].isin(keep)]
    med = d.groupby("k")["f"].median()
    ym = d.groupby(["k", "y"])["f"].median().unstack()
    pos = (ym > 0).sum(axis=1)
    ny = ym.notna().sum(axis=1)
    worst = ym.min(axis=1)
    oos = d[d["y"] >= "2024"].groupby("k")["f"].median()
    gate = ((pos >= 4) & (worst >= -2) & (med > GLOB + 0.3)
            & (oos.reindex(med.index) > GLOB))
    return len(keep), int(gate.sum()), float(med.max())


rng = np.random.default_rng(7)
print(f"🏦 bars {int(ok.sum()):,} · baseline {GLOB:+.3f} · gates: yrs>=4/6, worst>=-2, "
      f"med>base+0.3, OOS>base\n", flush=True)
for lab, K, valid in [("2-bar", K2, c1 > 0), ("3-bar", K3, (c1 > 0) & (c2 > 0))]:
    cells, obs, obs_best = count_pass(K, valid, F)
    sh_pass, sh_best = [], []
    for _ in range(NSHUF):
        vals = F.copy()
        idx = np.flatnonzero(ok)
        vals[idx] = rng.permutation(vals[idx])
        _, p, b = count_pass(K, valid, vals)
        sh_pass.append(p); sh_best.append(b)
    mu = float(np.mean(sh_pass)); sd = float(np.std(sh_pass))
    print(f"===== {lab} · cells tested {cells:,} =====", flush=True)
    print(f"  observed passing : {obs:>5d}  ({100*obs/cells:.1f}%)   best cell med {obs_best:+.2f}",
          flush=True)
    print(f"  shuffled passing : {mu:>7.1f} ± {sd:.1f}  ({100*mu/cells:.1f}%)   "
          f"best noise cell {np.mean(sh_best):+.2f} (max {np.max(sh_best):+.2f})", flush=True)
    print(f"  ENRICHMENT       : {obs/mu:.2f}x   → est. real findings ≈ {obs-mu:.0f} "
          f"({100*(obs-mu)/max(obs,1):.0f}% of survivors)\n", flush=True)

print("DONE", flush=True)
