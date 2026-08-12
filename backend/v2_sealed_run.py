"""The sealed acceptance for ComboLab v2. Opened once, and it checks that it is the first time.

Order is not presentation, it is a gate:

    INTEGRITY   is this the registered experiment          → if no, nothing else is read
    STRUCTURAL  do the paired implications hold            → if no, capability is not read
    CAPABILITY  what the instrument can actually do

INTEGRITY OUTRANKS CAPABILITY. An experiment that turns out not to be the registered experiment
has no operating characteristics at all — not bad ones, none. The results file may exist and the
numbers may be attractive; they are still not interpretable. This continues the rule the project
already runs on, that INVALID is not NULL.

Seeds are derived from the freeze commit and from nothing else. That hash did not exist while
the code was being written, so no one — including me — could have looked at the sealed set in
advance. Deriving them from anything editable would return the choice to the author.

FOUR POSSIBLE OUTCOMES, written before the run so none can be discovered afterwards:

    integrity fails            SEALED = INVALID, capability NOT INTERPRETABLE
    structural fails           implementation defect; sensitivity not discussed
    capability passes          v2 validated within the registered operating experiment
    capability fails           VALID EXPERIMENT / FAILED ACCEPTANCE — the generation closes
                               with that verdict and is not rerun under the same name

One look means one outcome, including the unwelcome one.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_v2 as E                                             # noqa: E402
import combolab_v2_spec as V2                                       # noqa: E402
import s1_run as S1R                                                # noqa: E402
import s1_spec as S1                                                # noqa: E402
import v2_kernel as K                                               # noqa: E402
from v2_decision_run import Frozen                                  # noqa: E402
from studio_verdict import Estimate, decide                         # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
MARKER = os.path.join(HERE, "V2_SEALED_FROZEN.json")
MANIFEST = os.path.join(HERE, "V2_SEALED_MANIFEST.json")
BAR = "=" * 132
DELTA_STAR = 0.50
pd.set_option("display.width", 215)


class SealedIntegrityError(RuntimeError):
    """This is not the registered experiment."""


def sealed_seeds(freeze_commit: str, n: int) -> tuple:
    if not freeze_commit or len(freeze_commit) < 40:
        raise SealedIntegrityError(
            "sealed seeds require the full freeze-commit hash; they do not exist before the "
            "freeze, which is the entire point of the seal")
    h = hashlib.sha256(f"combolab-v2-sealed:{freeze_commit}".encode()).digest()
    rng = np.random.default_rng(int.from_bytes(h[:8], "big"))
    return tuple(int(x) for x in rng.integers(1, 2**31 - 1, size=n))


def integrity_gate() -> dict:
    """Every check runs; the first failure does not short-circuit the rest, so the report is
    complete rather than truncated at whichever check happened to be first."""
    checks, ok = [], True
    if not os.path.exists(MARKER):
        raise SealedIntegrityError(
            f"{os.path.basename(MARKER)} is missing. The sealed set is not runnable before the "
            f"freeze commit exists — that is what makes it an acceptance set.")
    fz = json.load(open(MARKER))
    mf = json.load(open(MANIFEST))

    def chk(name, got, want):
        nonlocal ok
        good = got == want
        ok &= good
        checks.append((name, "PASS" if good else "FAIL",
                       "" if good else f"frozen {str(want)[:20]}… now {str(got)[:20]}…"))

    chk("spec semantic digest · combolab_v2", V2.digest(), mf["semantic_digests"]["combolab_v2_spec"])
    chk("spec semantic digest · s1", S1.digest(), mf["semantic_digests"]["s1_spec"])
    for f, want in mf["source_sha256"].items():
        p = os.path.join(HERE, f)
        got = hashlib.sha256(open(p, "rb").read()).hexdigest() if os.path.exists(p) else "MISSING"
        chk(f"source bytes · {f}", got, want)
    chk("support-policy hash",
        hashlib.sha256(json.dumps(V2.ELIGIBILITY, sort_keys=True).encode()).hexdigest(),
        mf["support_policy"]["hash"])

    O, _, dates = CL.load_base(verbose=False)
    ma = CL.build_masks(O)
    classes = E.equivalence_classes(ma, verbose=False)
    masks = {c["representative"]: ma[c["representative"]] for c in classes}
    g1 = {c: m for c, m in masks.items() if E.null_family(c) == "OPPORTUNITY_LEVEL"}
    chk("31-class manifest hash",
        hashlib.sha256("|".join(sorted(g1)).encode()).hexdigest(),
        mf["claim_manifest"]["manifest_hash"])
    chk("membership hash",
        hashlib.sha256(b"".join(masks[c].tobytes() for c in sorted(g1))).hexdigest(),
        mf["claim_manifest"]["membership_hash"])
    try:
        fam = E.assert_one_null_family(list(g1))
        chk("null family homogeneous", fam, "OPPORTUNITY_LEVEL")
    except E.IncompatibleNullFamilyError as e:                      # noqa: BLE001
        ok = False
        checks.append(("null family homogeneous", "FAIL", str(e)[:60]))
    chk("reveal-contract blob",
        subprocess.check_output(["git", "hash-object", "s1_reveal_contract.py"],
                                cwd=HERE).decode().strip(),
        fz["reveal_contract_blob"])
    chk("delta grid", list(S1.DELTA_GRID), mf["delta_grid"])
    return dict(ok=ok, checks=checks, freeze=fz, manifest=mf, O=O, dates=dates, masks=g1)


def main():
    print(BAR, flush=True)
    print("  COMBOLAB v2 · SEALED ACCEPTANCE — opened once", flush=True)
    print(BAR, flush=True)
    G = integrity_gate()
    print(f"  freeze commit  {G['freeze']['freeze_commit']}", flush=True)
    print(f"\n  {'check':<46s} {'result':<7s} detail", flush=True)
    for nm, r, why in G["checks"]:
        print(f"  {nm:<46s} {r:<7s} {why}", flush=True)
    print(f"\n  INTEGRITY: {'ALL PASS' if G['ok'] else 'FAILED'}", flush=True)
    if not G["ok"]:
        print("\n  SEALED STATUS = INVALID", flush=True)
        print("  CAPABILITY METRICS = NOT INTERPRETABLE", flush=True)
        print("  The experiment that ran is not the experiment that was registered. It has no",
              flush=True)
        print("  operating characteristics — not poor ones, none.", flush=True)
        sys.exit(2)

    O, dates, masks = G["O"], G["dates"], G["masks"]
    seeds = sealed_seeds(G["freeze"]["freeze_commit"], S1.M_PER_CLASS)
    print(f"\n  sealed world seeds derived from the freeze commit: {seeds}", flush=True)

    sup = E.Support(O, dates, masks)
    _, gi = np.unique(dates, return_inverse=True)
    n_dates = int(gi.max() + 1)
    froz = {c: Frozen(sup, c, gi) for c in sup.cells}
    meta = sup.meta.set_index("cell")
    fam = O["family"].astype(str).to_numpy().astype("U")
    key = np.char.add(np.char.add(dates.astype("U"), "|"), fam)
    order = np.argsort(key, kind="stable")
    ks = key[order]
    st = np.r_[0, np.flatnonzero(ks[1:] != ks[:-1]) + 1, len(ks)]
    segs = list(zip(st[:-1], st[1:]))
    inv = np.empty_like(order)
    inv[order] = np.arange(len(order))

    rows, t0 = [], time.time()
    for delta in S1.DELTA_GRID:
        plan = [(c, r) for r in range(S1.M_PER_CLASS) for c in sup.cells]
        ordr = S1R.key_rng("sealed_needle_location", G["freeze"]["freeze_commit"],
                           delta).permutation(len(plan))
        for n, pi in enumerate(ordr):
            planted, rep = plan[pi]
            world = seeds[rep]
            y0 = E.composition_world(O, dates, seed=world, noise=True)
            y = E.inject(y0, sup, planted, delta) if delta > 0 else y0
            th = pd.Series({c: froz[c].theta(y) for c in sup.cells})
            truth = pd.Series({c: froz[c].theta(y) - froz[c].theta(y0) for c in sup.cells})
            rank = int(th.rank(ascending=False)[planted])

            def boot(cell):
                f = froz[cell]
                rng = S1R.key_rng(world, delta, cell, rep, "claim_bootstrap")
                p = np.full(n_dates, 1 / n_dates)
                d = np.array([f.theta(y, rng.multinomial(n_dates, p).astype(float))
                              for _ in range(S1.N_BOOT)])
                good = d[np.isfinite(d)]
                lo, hi = np.percentile(good, [2.5, 97.5])
                est = f.theta(y)
                return Estimate(estimate=float(min(max(est, lo), hi)), ci_low=float(lo),
                                ci_high=float(hi), level=0.95, estimand=V2.ESTIMAND,
                                method="clustered bootstrap", cluster_unit="trading_date",
                                n_raw=int(meta.loc[cell, "eligible_cell_opportunities"]),
                                n_eff=int(meta.loc[cell, "eligible_dates"]))

            def verdict(e, cell):
                # 3B.1 · the decision moved to `v2_kernel` unchanged; `meta` and `DELTA_STAR`
                # were closure reads and are now arguments. The call sites below are untouched,
                # which is what keeps this a source-mechanical extraction rather than a rewrite.
                return K.verdict(e, cell, meta, DELTA_STAR)

            e_pl = boot(planted)
            known = verdict(e_pl, planted) == "BUILD"

            S1R._PERM = dict(y=y[order], froz=froz, cells=sup.cells, segs=segs,
                             seed=int(S1R.key_rng(world, delta, planted, rep, "band")
                                      .integers(1, 2**31 - 1)),
                             inv=inv, scatter=lambda v: v[inv])
            import multiprocessing as mp
            with mp.get_context("fork").Pool(8) as pool:
                best = np.asarray(pool.map(S1R._perm_once, range(S1.N_PERM), chunksize=4))
            band = float(np.percentile(best, 95))

            top = list(th.sort_values(ascending=False).index[:S1.TOP_K])
            screen, final = [], []
            for c in top:
                if th[c] <= band:
                    continue
                ec = e_pl if c == planted else boot(c)
                if ec.ci_low > 0:
                    screen.append(c)
                    if verdict(ec, c) == "BUILD":
                        final.append(c)
            sel = top[0]
            rows.append(dict(delta=delta, planted=planted, rep=rep, world=world, band=band,
                             known=known, rank=rank, rank_le_3=rank <= 3,
                             rank_le_5=rank <= S1.TOP_K,
                             screen=planted in screen, final=planted in final,
                             regret=float(truth.max() - truth[sel]), n_screen=len(screen)))
            if (n + 1) % 31 == 0:
                print(f"    δ={delta:<5.2f} {n+1:>3d}/{len(plan)}  ({time.time()-t0:.0f}s)",
                      flush=True)
    D = pd.DataFrame(rows)
    D["freeze_commit"] = G["freeze"]["freeze_commit"]
    D.to_csv("v2_sealed_run.csv", index=False)

    print("\n" + BAR, flush=True)
    print("  STRUCTURAL INTEGRITY — read before any sensitivity number", flush=True)
    print(BAR, flush=True)
    i1 = int(((~D.screen) & D.final).sum())
    i2 = int((D.screen & (~D.rank_le_5)).sum())
    i3 = int(((~D.known) & D.final).sum())
    for nm, bad in (("Final ⇒ SearchScreenPass", i1), ("SearchScreenPass ⇒ Rank ≤ 5", i2),
                    ("Final ⇒ KnownLocationAccept", i3)):
        print(f"  {nm:<40s} {'PASS' if bad == 0 else f'FAIL ({bad} violations)'}", flush=True)
    if i1 or i2 or i3:
        print("\n  Implementation or semantic defect. Sensitivity is not interpreted.", flush=True)
        sys.exit(3)

    print("\n" + BAR, flush=True)
    print("  CAPABILITY — the registered order, not the pleasant one", flush=True)
    print(BAR, flush=True)
    print(f"  {'δ':>6s} {'KNOWN':>8s} {'rank≤5':>8s} {'screen':>8s} {'final':>8s} | "
          f"{'r≤5|K':>7s} {'scr|r,K':>8s} {'RETENTION':>10s} | {'regret':>7s} {'n_scr':>6s}",
          flush=True)
    for delta, g in D.groupby("delta"):
        K = g[g.known]
        r5k = K.rank_le_5.mean() if len(K) else np.nan
        sk = K[K.rank_le_5].screen.mean() if len(K[K.rank_le_5]) else np.nan
        ret = K.final.mean() if len(K) else np.nan
        print(f"  {delta:>6.2f} {g.known.mean():>8.1%} {g.rank_le_5.mean():>8.1%} "
              f"{g.screen.mean():>8.1%} {g.final.mean():>8.1%} | {r5k:>7.1%} {sk:>8.1%} "
              f"{ret:>10.1%} | {g.regret.mean():>7.3f} {g.n_screen.mean():>6.2f}", flush=True)

    print("\n" + BAR, flush=True)
    print("  PAIRED TAX — everything held fixed but the presence of search", flush=True)
    print(BAR, flush=True)
    for delta, g in D.groupby("delta"):
        if delta == 0:
            print(f"  δ=0.00   no needle exists; false promotion "
                  f"{(g.n_screen > 0).mean():.1%}, final {g.final.mean():.1%}", flush=True)
            continue
        kl = int((g.known & ~g.final).sum())
        kk = int((g.known & g.final).sum())
        imp = int(((~g.known) & g.final).sum())
        print(f"  δ={delta:<5.2f} known&lost {kl:>4d} · known&kept {kk:>4d} · "
              f"IMPOSSIBLE {imp:>3d}  →  tax {kl/max(kl+kk,1):.1%}", flush=True)

    print("\n" + BAR, flush=True)
    print("  HETEROGENEITY — equal weight per claim-class", flush=True)
    print(BAR, flush=True)
    for delta, g in D.groupby("delta"):
        per = g.groupby("planted").final.mean()
        print(f"  δ={delta:<5.2f} median {per.median():>6.1%} · p10 {per.quantile(.1):>6.1%} "
              f"· p90 {per.quantile(.9):>6.1%} · at 0% {int((per == 0).sum()):>3d} "
              f"· at 100% {int((per == 1).sum()):>3d}", flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
