"""Structured-null placebo — does the search stay quiet when nothing is findable?

The needle test showed ComboLab finds a hidden ~3pp effect about half the time and a 0.60pp
effect almost never. This is the other half of the question, and δ = 0 could not answer it: that
world was real data with no ADDED effect, so it kept every real association the market has.

Three worlds, frozen in `placebo_spec.py` before this file existed, and they do NOT mean the
same thing:

    A   marginal_date_v1                          promotions ARE false discoveries
    B1  conditional_hierarchical_setup_v1         promotions are NOT errors
    B2  conditional_hierarchical_setup_price_v1   promotions are NOT errors

Under B the nuisance structure is held fixed, so a marginal association can genuinely survive —
Cell correlates with BaseSetup and PriceBucket, and a marginal claim is entitled to report it.
B measures how much of the search's output is nuisance-explained, which is a decomposition and
not an error rate. The metric names differ accordingly and the word FWER does not appear in B's
output, because `FWER_B2 = 18%` would read as an error rate to anyone who had not read the spec.

ComboLab is untouched: the engine is the sealed `6f7e5ee` that passed the needle acceptance.

THE FALLBACK IS PART OF THE RESULT. B2's median stratum is 2 observations, which permutes to the
identity half the time, so rows in strata below MIN_PERMUTABLE fall back a conditioning level.
Fallen-back rows permute among THEMSELVES at the coarser key — not into the level-0 pools, which
would undo permutations already performed. Every run prints what fraction moved at which level,
because a B2 run that permuted 61% of rows at full conditioning is a different experiment from
one that permuted 95%.
"""
from __future__ import annotations

import hashlib
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_spec as SPEC                                        # noqa: E402
import placebo_spec as PS                                           # noqa: E402
from needle_acceptance import verify_seal                           # noqa: E402
from sampling_target import descriptive_metric, structured_permutation_null  # noqa: E402

pd.set_option("display.width", 215)


def _rng(generator_id: str, rep: int, stream: str) -> np.random.Generator:
    """Named, deterministic, and independent of execution order and of replication count.

    Derived from (generator_id, rep, stream), so adding replications cannot change the world any
    existing replication saw, and the outer permutation that builds the null cannot become
    coupled to the permutations ComboLab uses for its own chance band.
    """
    if stream not in PS.RNG_STREAMS:
        raise ValueError(f"{stream!r} is not a declared stream: {PS.RNG_STREAMS}")
    h = hashlib.sha256(f"{generator_id}|{rep}|{stream}".encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "big"))


def strata_keys(O: pd.DataFrame, dates: np.ndarray, level: tuple) -> np.ndarray:
    parts = []
    for k in level:
        if k == "date":
            parts.append(dates)
        elif k == "family":
            parts.append(O["family"].astype(str).to_numpy())
        elif k == "price_bucket":
            pb = pd.cut(O["sig_close"].astype(float),
                        [PS.PRICE_BUCKETS[0][0]] + [b[1] for b in PS.PRICE_BUCKETS],
                        include_lowest=True).astype(str).to_numpy()
            parts.append(pb)
        else:
            raise ValueError(k)
    if len(parts) == 1:
        return np.asarray(parts[0]).astype(str)
    out = np.asarray(parts[0]).astype(str)
    for p in parts[1:]:
        out = np.char.add(np.char.add(out, "|"), np.asarray(p).astype(str))
    return out


def build_null(v: np.ndarray, keys_by_level: list, rng) -> tuple[np.ndarray, dict]:
    """Permute outcomes inside strata, falling back a level where a stratum is too small."""
    out = v.copy()
    pending = np.arange(len(v))                 # rows not yet permuted
    profile = {}
    for lvl, keys in enumerate(keys_by_level):
        if len(pending) == 0:
            break
        sub = keys[pending]
        order = np.argsort(sub, kind="stable")
        s_sorted, idx_sorted = sub[order], pending[order]
        starts = np.r_[0, np.flatnonzero(s_sorted[1:] != s_sorted[:-1]) + 1, len(s_sorted)]
        moved, still = 0, []
        for a, b in zip(starts[:-1], starts[1:]):
            block = idx_sorted[a:b]
            if len(block) >= PS.MIN_PERMUTABLE:
                out[block] = out[rng.permutation(block)]
                moved += len(block)
            else:
                still.append(block)
        profile[f"lvl{lvl}"] = moved / len(v)
        pending = np.concatenate(still) if still else np.array([], int)
    profile["unchanged"] = len(pending) / len(v)
    return out, profile


def run_generator(lab, O, v0, dates, gid: str, n_rep: int) -> pd.DataFrame:
    g = PS.GENERATORS[gid]
    levels = [g["strata"]] + [tuple(f) for f in g["fallback"]]
    keys_by_level = [strata_keys(O, dates, lv) for lv in levels]
    rows, t0 = [], time.time()
    for rep in range(n_rep):
        v, profile = build_null(v0, keys_by_level, _rng(gid, rep, "outer_null_world"))
        band_seed = int(_rng(gid, rep, "inner_chance_band").integers(1, 2**31 - 1))
        R, band = lab.search(v, band_seed, _rng(gid, rep, "inner_bootstrap"),
                             n_perm=CL.N_PERM, n_boot=CL.N_BOOT, workers=8)
        r = dict(generator=gid, rep=rep, band_p95=band["band_p95"],
                 best_est=float(R["estimate"].iloc[0]),
                 n_clears_band=int(R["clears_band"].sum()),
                 n_promoted=int(R["promoted"].sum()),
                 n_final=int((R["verdict"] == "BUILD").sum()))
        r.update({f"pct_{k}": v_ for k, v_ in profile.items()})
        rows.append(r)
        if (rep + 1) % 25 == 0:
            print(f"    {gid:<42s} {rep+1:>3d}/{n_rep}  ({time.time()-t0:.0f}s)", flush=True)
    return pd.DataFrame(rows)


def summarise(G: pd.DataFrame, gid: str) -> dict:
    is_a = gid == "marginal_date_v1"
    pre = "FWER" if is_a else "promotion_survival_rate"
    n = len(G)
    s = {"generator": gid, "n": n,
         f"{pre}_band": (G.n_clears_band > 0).mean(),
         f"{pre}_search": (G.n_promoted > 0).mean(),
         f"{pre}_final": (G.n_final > 0).mean(),
         "E_n_promoted": G.n_promoted.mean(),
         "p_zero": (G.n_promoted == 0).mean(),
         "p_one": (G.n_promoted == 1).mean(),
         "p_two_plus": (G.n_promoted >= 2).mean(),
         "max_promoted": int(G.n_promoted.max())}
    return s


def main():
    m = verify_seal()
    bar = "=" * 132
    print(bar, flush=True)
    print(f"  STRUCTURED-NULL PLACEBO · {PS.N_REPLICATIONS} replications × "
          f"{len(PS.GENERATORS)} generators", flush=True)
    print(bar, flush=True)
    print(f"  combolab seal  {m['freeze_commit'][:16]}… intact — the engine is not touched here",
          flush=True)
    print(f"  placebo digest {PS.digest()[:16]}… · MIN_PERMUTABLE {PS.MIN_PERMUTABLE} · "
          f"α {PS.ALPHA} · tolerance {PS.TOLERANCE_BAND}", flush=True)
    print(bar, flush=True)

    O, v0, dates = CL.load_base()
    lab = CL.ComboLab(O, dates, CL.build_masks(O))

    parts, summ = [], []
    for gid in PS.GENERATORS:
        print(f"\n  {gid} — {PS.GENERATORS[gid]['h0']}", flush=True)
        G = run_generator(lab, O, v0, dates, gid, PS.N_REPLICATIONS)
        parts.append(G)
        summ.append(summarise(G, gid))
    D = pd.concat(parts, ignore_index=True)
    D["combolab_freeze"] = m["freeze_commit"]
    D.to_csv("placebo_run.csv", index=False)

    print("\n" + bar, flush=True)
    print("  FALLBACK PROFILE — part of the result, not an appendix", flush=True)
    print(bar, flush=True)
    pcts = [c for c in D.columns if c.startswith("pct_")]
    print(D.groupby("generator")[pcts].mean().to_string(float_format=lambda x: f"{x:.1%}"),
          flush=True)

    print("\n" + bar, flush=True)
    print("  A · FALSE DISCOVERY — promotions here ARE errors", flush=True)
    print(bar, flush=True)
    A = [s for s in summ if s["generator"] == "marginal_date_v1"][0]
    for k in PS.METRICS_A:
        val = A[k]
        print(f"    {k:<30s} {val:>8.3f}" if isinstance(val, float)
              else f"    {k:<30s} {val:>8d}", flush=True)
    lo, hi = PS.TOLERANCE_BAND
    inside = lo <= A["FWER_band"] <= hi
    print(f"\n    FWER_band {A['FWER_band']:.3f} vs preregistered tolerance "
          f"[{lo:.2f}, {hi:.2f}] → {'INSIDE' if inside else 'OUTSIDE'}", flush=True)
    ineq = A["FWER_final"] <= A["FWER_search"] <= A["FWER_band"]
    print(f"    {PS.STRUCTURAL_INEQUALITY}: "
          f"{A['FWER_final']:.3f} ≤ {A['FWER_search']:.3f} ≤ {A['FWER_band']:.3f} → "
          f"{'HOLDS' if ineq else 'VIOLATED — implementation or decision-flow defect'}",
          flush=True)

    print("\n" + bar, flush=True)
    print("  B · NUISANCE DECOMPOSITION — promotions here are NOT errors", flush=True)
    print(bar, flush=True)
    B = pd.DataFrame([s for s in summ if s["generator"] != "marginal_date_v1"])
    print(B.to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)
    print("\n    Read as: of the search behaviour ComboLab shows, how much persists when the",
          flush=True)
    print("    structure its marginal band erases is held fixed. Not a false-positive rate.",
          flush=True)

    print("\n" + bar, flush=True)
    for s in summ:
        key = "FWER_search" if s["generator"] == "marginal_date_v1" \
            else "promotion_survival_rate_search"
        mm = descriptive_metric(
            "conditional_false_promotion_rate" if s["generator"] == "marginal_date_v1"
            else "conditional_promotion_survival_rate", float(s[key]),
            target=structured_permutation_null(s["generator"]), n_replications=int(s["n"]))
        print(f"  {mm}", flush=True)
    print("\n  Every rate above is conditional on its own null generator. A rate under one null",
          flush=True)
    print("  model says nothing about another, and sampling_target refuses the comparison.",
          flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
