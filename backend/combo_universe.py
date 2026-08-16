"""M2/M3 — the dependency map, and the exact size of the candidate universe.

THE QUESTION THIS ANSWERS, AND WHY IT COMES BEFORE ANY MINER

A beam search is an ADAPTIVE procedure: which depth-3 candidates exist at all depends on
what won at depth 2, which depends on Y. Its statistic is

    T(Y) = max over c in A(Y) of Score(c, Y)

so a valid null must re-run the whole search on every permuted outcome — 121 executions of
the miner, not 121 re-rankings of one fixed list. That is the single most expensive fact in
the design, and it is only unavoidable if the space genuinely cannot be enumerated.

If the structurally valid universe is small enough to enumerate ONCE, then C is fixed
before Y exists, the band goes back to max over a constant set, and the entire adaptive-null
problem disappears. So the honest order is: measure the space, then choose. Assuming a beam
is necessary would buy a hard statistical problem without checking whether it was for sale.

WHAT PRUNES, AND ON WHAT AUTHORITY

    exclusivity      DECLARED where a theorem (two `eq` on one column), MEASURED otherwise
    distinct family  grammar: two answers to one question are not an interaction
    degeneracy       identical membership vectors are one hypothesis with two names
    implication      A ⊆ B means A∧B is A. Not a near-duplicate — the same set
    near-duplicate   Jaccard ≥ 0.98: two names, one statistical object in all but rounding
    support          the frozen ComboLab v2 eligibility, applied within every stratum

SUPPORT IS ANTI-MONOTONE, AND THAT IS WHAT MAKES DEPTH 3 ENUMERABLE

n(A∧B∧C) ≤ n(A∧B) for every C, so a pair whose cell arm is already too small in too few
strata cannot be rescued by a third token. Apriori expansion is therefore EXACT, not a
heuristic: it drops only candidates that provably fail. Critically, it prunes on X alone —
support, never outcome — so the surviving universe is still a constant with respect to Y,
and enumerating it this way does NOT make the search adaptive. That distinction is the
whole point; a prune that consulted Y would silently reintroduce T(Y) = max over A(Y).

HOW IT IS COMPUTED, BECAUSE THE FIRST VERSION WAS TOO SLOW TO FINISH

Written as nested loops over rows this is ~10¹¹ operations and does not return. Every
per-stratum pairwise count is instead one matrix product per stratum:

    G[k] = M_kᵀ M_k        G[k][a,b] = |A ∧ B| among the rows of setup family k

72 small GEMMs give every pair's support in every stratum at once, and the global counts
fall out as G.sum(0). Conditional Jaccard inside a setup then costs nothing — it is three
lookups in G[k] — which is what makes the redundancy question askable at all rather than
in principle.

A CANDIDATE IS ITS MEMBERSHIP, NOT ITS NAME

v2 learned this one level down: 46 manifest entries carried 37 distinct membership vectors,
and the nine aliases were not cosmetic — they occupied top-K slots, crowded other claims
out, and produced ties the ranking had to break on sort order. The same trap exists one
level up and no pairwise test can see it. `A∧B` and `A∧B'` can be the identical row set
while B and B' are far apart globally; `A∧B∧C` collapses onto `A∧B` whenever A∧B ⊆ C, which
is a statement about the CONJUNCTION and not about any pair inside it.

So every candidate gets a claim_membership_hash taken from what it actually selects:

    (its eligible stratum set, its treated assignment restricted to those strata)

Two candidates with the same hash are ONE selectable claim with two spellings. What this
changes, precisely — and it is less than it looks:

    permutation band     UNAFFECTED. max(T₁…T_n) is identical over duplicates, exactly as
                         v2 recorded for the 46 → 37 case.
    explicit k           CHANGED. DSR and any FDR layer must be told the distinct count.
    ranking              CHANGED. Duplicates take real top-K slots and tie deterministically.

NO OUTCOME IS READ ANYWHERE. Not values, not missingness, not column names: the population
is loaded without any outcome column at all, which is checkable rather than argued —
`ret` has 0 NaN in 1,140,344 rows, so the frozen v1 filter selected nothing and the row set
is bit-identical either way. population_hash() records the result so the claim survives a
rebuild of the table.
"""
from __future__ import annotations

import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens as CT                                            # noqa: E402
import combo_tokens_spec as TS                                       # noqa: E402
import combolab_v2_spec as V2                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
TOKENS = os.path.join(ROOT, "data", "combo_tokens.parquet")
OUT_JSON = os.path.join(HERE, "COMBO_UNIVERSE.json")
OUT_JSON_MATURE = os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")
LABEL_STATUS = os.path.join(ROOT, "data", "combo_label_status.parquet")
MATURE = os.environ.get("COMBO_MATURE") == "1"   # M2.6 research population
OUT_PAIRS = os.path.join(ROOT, "data", "combo_pair_dependency.parquet")

NEAR_DUP_J = 0.98         # two names, one statistical object
IMPLICATION_CONF = 0.999  # A ⊆ B, allowing for a handful of encoding oddities
MIN_SETUPS = V2.SUPPORT_FLOOR["min_eligible_setups"]
N_MIN = V2.ELIGIBILITY["n_min"]
D_MIN = V2.ELIGIBILITY["dates_min"]
CONC = V2.ELIGIBILITY["max_single_date_share"]


# ── population ───────────────────────────────────────────────────────────────
def population_hash(df: pd.DataFrame) -> str:
    """Identity of the row set, independent of column order and of any outcome."""
    k = (df["ticker"].astype(str) + "|" + df["sig_date"].astype(str).str[:10]
         + "|" + df["family"].astype(str))
    return __import__("hashlib").sha256("\n".join(sorted(k)).encode()).hexdigest()[:16]


def load_population(verbose: bool = True, mature: bool = None):
    """The frozen ComboLab base population, joined to its tokens — WITHOUT reading Y.

    combo_lab.load_base writes `dropna(subset=["ret", "sig_close"])`. Reproducing that
    literally would let an outcome-derived field decide which rows the search may see —
    a weaker contract than this project can afford, because "we only looked at whether Y
    exists" is exactly the kind of exemption that is impossible to audit later.

    MEASURED, and that is why the reference is simply gone rather than defended:
    `ret` has 0 NaN in all 1,140,344 rows, so the clause selected nothing. Loading the
    parquet without any outcome column at all yields a bit-identical row set —
    287464f95cc49774 both ways. The frozen v1 population is therefore outcome-independent
    as a FACT about the data, not as an argument about the code, and this function no
    longer names an outcome column at any point.

    population_hash() is recorded in the artifact so the claim stays checkable if the
    table is ever rebuilt: a changed hash means the population moved, whatever the
    surrounding prose says.
    """
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", "sig_close", "family",
                                       "dup_group"])
    O = O[O["sig_close"].notna()].drop_duplicates("dup_group")
    O = O[(O["sig_close"] >= 21) & (O["sig_close"] <= 89)].reset_index(drop=True)
    O["sig_date"] = O["sig_date"].astype(str).str[:10]

    # M2.6 · keep only cohorts whose full MAXH window was observable, and whose name did
    # not terminate inside it. The rule is CALENDAR-only, so nothing about the realized
    # path decides membership — a maturity rule that spared the trades which happened to
    # exit early would select on the outcome it is meant to protect.
    if (MATURE if mature is None else mature):
        st = pd.read_parquet(LABEL_STATUS)
        keep = set(zip(st.loc[st.label_status.str.startswith("ORDINARY"), "ticker"],
                       st.loc[st.label_status.str.startswith("ORDINARY"), "sig_date"]))
        before = len(O)
        O = O[[t in keep for t in zip(O["ticker"], O["sig_date"])]].reset_index(drop=True)
        if verbose:
            print(f"  M2.6 maturity filter: {before:,} → {len(O):,} "
                  f"(−{before-len(O):,}, −{(before-len(O))/before:.2%})", flush=True)

    K = pd.read_parquet(TOKENS)
    ids = [t.token_id for t in TS.REGISTRY]
    P = O.merge(K, on=["ticker", "sig_date"], how="inner", validate="m:1")
    if len(P) < 0.98 * len(O):
        raise RuntimeError(f"token join lost {1-len(P)/len(O):.1%} of the population")
    M = np.ascontiguousarray(P[ids].to_numpy(dtype=np.uint8))
    fam = P["family"].astype(str).to_numpy()
    dates = P["sig_date"].to_numpy()
    ph = population_hash(P)
    if verbose:
        print(f"\n  M2 · base population {len(P):,} rows · "
              f"{len(np.unique(fam))} setup families · "
              f"{len(np.unique(dates)):,} dates · hash {ph}", flush=True)
    return P, M, fam, dates, ids, ph


# ── the per-stratum count tensor ─────────────────────────────────────────────
class Strata:
    """G[k][a,b] = |A ∧ B| inside setup family k, plus the date structure of each arm.

    Everything the eligibility rule needs about pairs is a lookup in G. Only triples have
    to touch rows again, and then only the ones whose parent pair already survived.
    """

    def __init__(self, M, fam, dates, verbose=True):
        t0 = time.time()
        self.n, self.T = M.shape
        self.fams, fi = np.unique(fam, return_inverse=True)
        self.udates, di = np.unique(dates, return_inverse=True)
        self.fi, self.di = fi, di
        self.K, self.D = len(self.fams), len(self.udates)
        self.fam_size = np.bincount(fi, minlength=self.K).astype(np.int64)
        # per-family date histogram — the control arm is this minus the cell arm
        self.FD = np.zeros((self.K, self.D), dtype=np.int64)
        np.add.at(self.FD, (fi, di), 1)
        self.fam_dates = (self.FD > 0).sum(1)

        self.G = np.empty((self.K, self.T, self.T), dtype=np.int64)
        for k in range(self.K):
            Mk = M[fi == k].astype(np.float32)
            self.G[k] = np.rint(Mk.T @ Mk).astype(np.int64)
        self.inter = self.G.sum(0)
        self.cnt = np.diag(self.inter).copy()
        if verbose:
            print(f"  strata tensor {self.K}×{self.T}×{self.T} "
                  f"({self.G.nbytes/1e6:.0f} MB) · {time.time()-t0:.0f}s", flush=True)

    # ── eligibility ─────────────────────────────────────────────────────────
    def eligible_from_counts(self, ncell: np.ndarray) -> np.ndarray:
        """Cheap necessary condition per stratum: both arms have enough ROWS."""
        return (ncell >= N_MIN) & ((self.fam_size - ncell) >= N_MIN)

    def eligible_mask(self, idx: np.ndarray) -> tuple[int, bool, np.ndarray]:
        """Exact eligible-stratum set for a candidate, given its row indices.

        Returns (n_eligible, complement_bound, ok) where `ok` is the per-stratum boolean.
        The date work is done as one dense (K × D) histogram of the cell arm; the control
        arm is the family histogram minus it, so both arms are exact and neither needs a
        second pass over rows.
        """
        cell = np.zeros((self.K, self.D), dtype=np.int64)
        np.add.at(cell, (self.fi[idx], self.di[idx]), 1)
        ncell = cell.sum(1)
        nctrl = self.fam_size - ncell
        ok = (ncell >= N_MIN) & (nctrl >= N_MIN)
        comp_bound = bool(((ncell >= N_MIN) & (nctrl < N_MIN)).any())
        if not ok.any():
            return 0, comp_bound, ok
        ctrl = self.FD - cell
        ok &= ((cell > 0).sum(1) >= D_MIN) & ((ctrl > 0).sum(1) >= D_MIN)
        if not ok.any():
            return 0, comp_bound, ok
        with np.errstate(divide="ignore", invalid="ignore"):
            ok &= (cell.max(1) <= CONC * np.maximum(ncell, 1))
            ok &= (ctrl.max(1) <= CONC * np.maximum(nctrl, 1))
        return int(ok.sum()), comp_bound, ok

    def claim_hash(self, idx: np.ndarray, ok: np.ndarray) -> bytes:
        """What the candidate actually SELECTS: eligible strata + treated rows in them.

        Not the global mask. Two candidates whose masks differ only outside their eligible
        strata pose the same question of the same rows, and the estimand cannot tell them
        apart — so neither may the search-space count.
        """
        keep = ok[self.fi[idx]]
        rows = np.zeros(self.n, dtype=np.uint8)
        rows[idx[keep]] = 1
        h = __import__("hashlib").sha256()
        h.update(np.packbits(ok).tobytes())
        h.update(np.packbits(rows).tobytes())
        return h.digest()


# ── dependency measures ──────────────────────────────────────────────────────
def measures(S: Strata):
    cnt, inter = S.cnt, S.inter
    union = cnt[:, None] + cnt[None, :] - inter
    with np.errstate(divide="ignore", invalid="ignore"):
        J = np.where(union > 0, inter / np.maximum(union, 1), 0.0)
        conf = np.where(cnt[:, None] > 0, inter / np.maximum(cnt[:, None], 1), 0.0)
    return J, conf


def conditional_overlap(S: Strata, ids, J, verbose=True):
    """max over setups of J(A,B | family=s), for pairs that look distinct globally.

    Two tokens can be far apart across the whole population and nearly the same thing
    inside the setups where a miner would condition on them — and a global Jaccard alone
    would let such a pair through as two hypotheses. Every number here is three lookups
    in G, so the question costs nothing once the tensor exists.
    """
    big = np.flatnonzero(S.fam_size >= 2000)
    if not len(big):
        return pd.DataFrame()
    Gb = S.G[big]                                     # (B, T, T)
    d = np.diagonal(Gb, axis1=1, axis2=2)             # (B, T)  |A| inside each setup
    inter = Gb
    uni = d[:, :, None] + d[:, None, :] - inter
    with np.errstate(divide="ignore", invalid="ignore"):
        Jk = np.where(uni >= 200, inter / np.maximum(uni, 1), 0.0)
    best = Jk.max(0)
    arg = Jk.argmax(0)

    fam_of = np.array([t.family for t in TS.REGISTRY])
    rows = []
    for a, b in itertools.combinations(range(len(ids)), 2):
        if fam_of[a] == fam_of[b]:
            continue
        if best[a, b] > J[a, b] + 0.05 and best[a, b] >= 0.30:
            rows.append(dict(token_a=ids[a], token_b=ids[b],
                             jaccard_global=round(float(J[a, b]), 4),
                             jaccard_max_within_setup=round(float(best[a, b]), 4),
                             setup=str(S.fams[big[arg[a, b]]])))
    out = (pd.DataFrame(rows).sort_values("jaccard_max_within_setup", ascending=False)
           if rows else pd.DataFrame(columns=["token_a", "token_b", "jaccard_global",
                                              "jaccard_max_within_setup", "setup"]))
    if verbose:
        print(f"\n  conditional redundancy · {len(out)} cross-family pair(s) at least "
              f"0.05 more similar inside some setup than globally (and ≥0.30 there)",
              flush=True)
        for r in out.head(12).itertuples():
            print(f"      {r.token_a:<14} {r.token_b:<14} "
                  f"J {r.jaccard_global:.3f} → {r.jaccard_max_within_setup:.3f} "
                  f"in {r.setup}", flush=True)
    return out


def cramers_v(df: pd.DataFrame, cols: list, verbose: bool = True) -> pd.DataFrame:
    """Field-level association, bias-corrected.

    Measured between the raw CATEGORICAL COLUMNS, not between tokens. Two tokens are two
    answers; whether `phys_c` and `phys_h` are two dimensions or one dimension written
    twice is a question about the fields themselves.
    """
    rows = []
    for a, b in itertools.combinations(cols, 2):
        ct = pd.crosstab(df[a].astype("string").fillna(""),
                         df[b].astype("string").fillna(""))
        if ct.shape[0] < 2 or ct.shape[1] < 2:
            continue
        obs = ct.to_numpy(dtype=float)
        nn = obs.sum()
        exp = np.outer(obs.sum(1), obs.sum(0)) / nn
        chi2 = float(((obs - exp) ** 2 / np.maximum(exp, 1e-12)).sum())
        phi2 = chi2 / nn
        r, k = obs.shape
        # Bergsma–Wicher correction: at 570k rows and 20-level fields the raw statistic is
        # inflated by table size alone, and every pair would read "associated".
        phi2c = max(0.0, phi2 - (k - 1) * (r - 1) / (nn - 1))
        rc = r - (r - 1) ** 2 / (nn - 1)
        kc = k - (k - 1) ** 2 / (nn - 1)
        v = float(np.sqrt(phi2c / max(min(kc - 1, rc - 1), 1e-12)))
        rows.append(dict(field_a=a, field_b=b, levels_a=r, levels_b=k,
                         cramers_v=round(min(v, 1.0), 4)))
    out = pd.DataFrame(rows).sort_values("cramers_v", ascending=False)
    if verbose and len(out):
        print(f"\n  field association · top 14 of {len(out)} pairs "
              f"(Cramér's V, bias-corrected)", flush=True)
        for r in out.head(14).itertuples():
            print(f"      {r.field_a:<16} {r.field_b:<16} V={r.cramers_v:.3f}  "
                  f"({r.levels_a}×{r.levels_b} levels)", flush=True)
    return out


# ── the count ────────────────────────────────────────────────────────────────
def count_universe(S: Strata, M, ids, J, conf, *, verbose=True):
    T = TS.REGISTRY
    fam_of = np.array([t.family for t in T])
    excl = np.array([t.exclusion_group for t in T])
    nullreq = np.array([t.null_requirement for t in T])
    cnt, inter = S.cnt, S.inter
    n_rows = S.n
    idx_of = [np.flatnonzero(M[:, i]) for i in range(len(T))]
    comp_binds = 0
    claim_of: dict = {}          # candidate → what it actually selects
    rep: dict = {}

    # ── depth 1 ──────────────────────────────────────────────────────────────
    t0 = time.time()
    alive1, drop1 = [], {"degenerate_prevalence": [], "support": []}
    for i in range(len(T)):
        if cnt[i] == 0 or cnt[i] > 0.99 * n_rows:
            drop1["degenerate_prevalence"].append(ids[i])
            continue
        ne, cb, ok = S.eligible_mask(idx_of[i])
        comp_binds += cb
        if ne < MIN_SETUPS:
            drop1["support"].append(ids[i])
            continue
        alive1.append(i)
        claim_of[(i,)] = S.claim_hash(idx_of[i], ok)
    rep["depth1"] = dict(declared=len(T), surviving=len(alive1),
                         dropped={k: len(v) for k, v in drop1.items()},
                         dropped_ids=drop1, seconds=round(time.time() - t0, 1))
    if verbose:
        print(f"\n  DEPTH 1 · {len(T)} declared → {len(alive1)} selectable "
              f"({time.time()-t0:.0f}s)", flush=True)
        for k, v in drop1.items():
            if v:
                print(f"      −{len(v):>3} {k}: {' '.join(v[:16])}"
                      + (" …" if len(v) > 16 else ""), flush=True)

    # ── exact degeneracy among the survivors ────────────────────────────────
    sig: dict = {}
    for i in alive1:
        sig.setdefault(M[:, i].tobytes(), []).append(i)
    groups = [[ids[i] for i in v] for v in sig.values() if len(v) > 1]
    rep["depth1_equivalence"] = dict(names=len(alive1), classes=len(sig),
                                     redundant_aliases=len(alive1) - len(sig),
                                     groups=groups)
    if verbose:
        print(f"      {len(alive1)} names → {len(sig)} distinct membership vectors"
              + (f" · {len(alive1)-len(sig)} alias(es)" if groups else ""), flush=True)
        for g in groups:
            print(f"          ≡ {' ≡ '.join(g)}", flush=True)
    alive1 = [v[0] for v in sig.values()]              # one representative per class
    alive1.sort()

    # ── depth 2 ──────────────────────────────────────────────────────────────
    t0 = time.time()
    reasons = dict(same_family=0, exclusive_declared=0, exclusive_measured=0,
                   implication=0, near_duplicate=0, support_rows=0, support_dates=0)
    structural, examined = [], 0
    for a, b in itertools.combinations(alive1, 2):
        examined += 1
        if fam_of[a] == fam_of[b]:
            reasons["same_family"] += 1
        elif excl[a] and excl[a] == excl[b]:
            reasons["exclusive_declared"] += 1
        elif inter[a, b] == 0:
            reasons["exclusive_measured"] += 1
        elif conf[a, b] >= IMPLICATION_CONF or conf[b, a] >= IMPLICATION_CONF:
            reasons["implication"] += 1
        elif J[a, b] >= NEAR_DUP_J:
            reasons["near_duplicate"] += 1
        else:
            structural.append((a, b))
    # support, stage A — pure lookup in G, no rows touched
    stageA = [(a, b) for a, b in structural
              if S.eligible_from_counts(S.G[:, a, b]).sum() >= MIN_SETUPS]
    reasons["support_rows"] = len(structural) - len(stageA)
    # support, stage B — dates and concentration, exact
    survivors2 = []
    for a, b in stageA:
        idx = np.intersect1d(idx_of[a], idx_of[b], assume_unique=True)
        ne, cb, ok = S.eligible_mask(idx)
        comp_binds += cb
        if ne >= MIN_SETUPS:
            survivors2.append((a, b))
            claim_of[(a, b)] = S.claim_hash(idx, ok)
    reasons["support_dates"] = len(stageA) - len(survivors2)
    rep["depth2"] = dict(examined=examined, structural=len(structural),
                         after_row_support=len(stageA), surviving=len(survivors2),
                         dropped=reasons, seconds=round(time.time() - t0, 1))
    if verbose:
        print(f"\n  DEPTH 2 · {examined:,} pairs → {len(survivors2):,} selectable "
              f"({time.time()-t0:.0f}s)", flush=True)
        for k, v in reasons.items():
            if v:
                print(f"      −{v:>8,} {k}", flush=True)

    # ── depth 3, apriori over depth-2 survivors ─────────────────────────────
    t0 = time.time()
    r3 = dict(same_family=0, exclusive_declared=0, exclusive_measured=0, implication=0,
              near_duplicate=0, support_rows=0, support_dates=0)
    Mf = M.astype(np.float32)
    seen3, staged = set(), []
    for a, b in survivors2:
        idx_ab = np.intersect1d(idx_of[a], idx_of[b], assume_unique=True)
        # counts of A∧B∧C for EVERY c at once, over the pair's rows only
        n_abc = M[idx_ab].sum(0, dtype=np.int64)
        for c in alive1:
            if c <= b:
                continue
            key = (a, b, c)
            if key in seen3:
                continue
            seen3.add(key)
            if fam_of[c] in (fam_of[a], fam_of[b]):
                r3["same_family"] += 1
            elif excl[c] and excl[c] in (excl[a], excl[b]):
                r3["exclusive_declared"] += 1
            elif inter[a, c] == 0 or inter[b, c] == 0:
                r3["exclusive_measured"] += 1
            elif max(conf[a, c], conf[c, a], conf[b, c], conf[c, b]) >= IMPLICATION_CONF:
                r3["implication"] += 1
            elif max(J[a, c], J[b, c]) >= NEAR_DUP_J:
                r3["near_duplicate"] += 1
            elif n_abc[c] < N_MIN * MIN_SETUPS:
                r3["support_rows"] += 1          # necessary: ≥5 strata × ≥100 rows each
            else:
                staged.append((a, b, c, idx_ab))
    surv3 = []
    for a, b, c, idx_ab in staged:
        idx = idx_ab[M[idx_ab, c] > 0]
        ne, cb, ok = S.eligible_mask(idx)
        comp_binds += cb
        if ne >= MIN_SETUPS:
            surv3.append((a, b, c))
    r3["support_dates"] = len(staged) - len(surv3)
    rep["depth3"] = dict(examined=len(seen3), staged=len(staged), surviving=len(surv3),
                         dropped=r3, seconds=round(time.time() - t0, 1),
                         apriori="only depth-2 survivors extended; support is "
                                 "anti-monotone, so the prune is exact and uses X only")
    if verbose:
        print(f"\n  DEPTH 3 · {len(seen3):,} triples reachable from surviving pairs → "
              f"{len(surv3):,} selectable ({time.time()-t0:.0f}s)", flush=True)
        for k, v in r3.items():
            if v:
                print(f"      −{v:>8,} {k}", flush=True)

    # ── Gate B · claim equivalence over the WHOLE grammar-v1 universe ───────
    # Depth-1 degeneracy was checked above on raw masks. That is not enough: two PAIRS can
    # select the same rows without either pairwise test firing, and the count that goes
    # into DSR must be the number of distinct questions, not the number of spellings.
    # Depth 3 is deliberately not deduplicated — it is outside grammar v1 and is reported
    # as a sizing measurement, not as a searchable manifest.
    t0 = time.time()
    label = {}
    for c in [(i,) for i in alive1] + survivors2:
        label[c] = "+".join(ids[i] for i in c)
    classes: dict = {}
    for c in [(i,) for i in alive1] + survivors2:
        classes.setdefault(claim_of[c], []).append(label[c])
    alias_groups = sorted((v for v in classes.values() if len(v) > 1), key=len,
                          reverse=True)
    rep["claim_equivalence"] = dict(
        candidates=len(alive1) + len(survivors2),
        distinct_claims=len(classes),
        redundant_spellings=len(alive1) + len(survivors2) - len(classes),
        n_alias_groups=len(alias_groups),
        largest_groups=[g for g in alias_groups[:40]],
        seconds=round(time.time() - t0, 1))
    if verbose:
        n_c = len(alive1) + len(survivors2)
        print(f"\n  GATE B · claim equivalence over grammar v1", flush=True)
        print(f"      {n_c:,} candidates → {len(classes):,} DISTINCT selectable claims "
              f"· {n_c - len(classes):,} redundant spelling(s) in "
              f"{len(alias_groups):,} group(s)", flush=True)
        for g in alias_groups[:10]:
            print(f"          ≡ {' ≡ '.join(g[:4])}"
                  + (f"  (+{len(g)-4} more)" if len(g) > 4 else ""), flush=True)

    def route(cands):
        out = {}
        for c in cands:
            r = TS.combine_null([nullreq[i] for i in c])
            out[r] = out.get(r, 0) + 1
        return out

    rep["null_routing"] = dict(depth1=route([(i,) for i in alive1]),
                               depth2=route(survivors2), depth3=route(surv3))
    rep["complement_arm_binds"] = int(comp_binds)
    rep["grammar_v1_total"] = len(alive1) + len(survivors2)
    rep["grammar_v1_opportunity_level"] = (
        rep["null_routing"]["depth1"].get(TS.OPPORTUNITY_LEVEL, 0)
        + rep["null_routing"]["depth2"].get(TS.OPPORTUNITY_LEVEL, 0))
    return rep, alive1, survivors2, surv3


def main():
    t0 = time.time()
    P, M, fam, dates, ids, pop_hash = load_population()
    S = Strata(M, fam, dates)
    J, conf = measures(S)
    cnt = S.cnt

    prev = pd.DataFrame({"token_id": ids,
                         "family": [t.family for t in TS.REGISTRY],
                         "n": cnt, "prevalence": cnt / len(P)})
    print(f"\n  prevalence · 8 rarest and 8 commonest of {len(ids)}", flush=True)
    ps = prev.sort_values("n")
    for r in list(ps.head(8).itertuples()) + [None] + list(ps.tail(8).itertuples()):
        if r is None:
            print("      …", flush=True)
            continue
        print(f"      {r.token_id:<16} {r.family:<16} {r.n:>8,}  {r.prevalence:>7.2%}",
              flush=True)

    impl = [(ids[a], ids[b], int(cnt[a]), float(conf[a, b]))
            for a in range(len(ids)) for b in range(len(ids))
            if a != b and cnt[a] > 0 and conf[a, b] >= IMPLICATION_CONF]
    near = [(ids[a], ids[b], float(J[a, b]))
            for a, b in itertools.combinations(range(len(ids)), 2)
            if J[a, b] >= NEAR_DUP_J]
    print(f"\n  implication · {len(impl)} ordered pair(s) with A ⊆ B "
          f"(conf ≥ {IMPLICATION_CONF})", flush=True)
    for a, b, na, c in sorted(impl, key=lambda x: -x[2])[:16]:
        print(f"      {a:<16} ⊆ {b:<16} n(A)={na:>7,}  conf={c:.4f}", flush=True)
    print(f"\n  near-duplicates · {len(near)} pair(s) with Jaccard ≥ {NEAR_DUP_J}",
          flush=True)
    for a, b, j in sorted(near, key=lambda x: -x[2])[:16]:
        print(f"      {a:<16} ≈ {b:<16} J={j:.4f}", flush=True)

    fields = sorted({t.column for t in TS.REGISTRY
                     if t.source == "bars" and t.kind in ("eq", "sw", "ct", "ne", "ldig")})
    src, _ = CT.source_frame(verbose=False)
    cv = cramers_v(src, fields)
    co = conditional_overlap(S, ids, J)

    rep, a1, s2, s3 = count_universe(S, M, ids, J, conf)

    pd.DataFrame([dict(token_a=ids[a], token_b=ids[b],
                       family_a=TS.REGISTRY[a].family, family_b=TS.REGISTRY[b].family,
                       n_a=int(cnt[a]), n_b=int(cnt[b]), n_both=int(S.inter[a, b]),
                       jaccard=round(float(J[a, b]), 6),
                       conf_a_to_b=round(float(conf[a, b]), 6),
                       conf_b_to_a=round(float(conf[b, a]), 6))
                  for a, b in itertools.combinations(range(len(ids)), 2)]
                 ).to_parquet(OUT_PAIRS, index=False, compression="zstd")

    out = dict(spec_version=TS.SPEC_VERSION, registry_digest=TS.digest(),
               population_rows=int(len(P)), population_hash=pop_hash,
               setup_families=int(S.K), dates=int(S.D),
               eligibility=V2.ELIGIBILITY, support_floor=V2.SUPPORT_FLOOR,
               near_dup_jaccard=NEAR_DUP_J, implication_conf=IMPLICATION_CONF,
               prevalence=prev.set_index("token_id")["n"].to_dict(),
               n_implications=len(impl), implications=[list(x) for x in impl],
               n_near_duplicates=len(near), near_duplicates=[list(x) for x in near],
               cramers_v=cv.to_dict("records"),
               conditional_redundancy=co.to_dict("records"),
               universe=rep,
               surviving_depth2=[[ids[a], ids[b]] for a, b in s2],
               outcome_read="NONE — no outcome column is loaded at any point",
               seconds=round(time.time() - t0, 1))
    dest = OUT_JSON_MATURE if MATURE else OUT_JSON
    with open(dest, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n{'='*82}", flush=True)
    print(f"  CANDIDATE UNIVERSE   registry {TS.digest()}", flush=True)
    print(f"    depth 1   {rep['depth1']['surviving']:>10,}", flush=True)
    print(f"    depth 2   {rep['depth2']['surviving']:>10,}", flush=True)
    print(f"    depth 3   {rep['depth3']['surviving']:>10,}", flush=True)
    print(f"    ─────────────────────────", flush=True)
    ce = rep["claim_equivalence"]
    print(f"    grammar v1 (Setup + ≤2)     {rep['grammar_v1_total']:>10,}", flush=True)
    print(f"    of which OPPORTUNITY_LEVEL  "
          f"{rep['grammar_v1_opportunity_level']:>10,}", flush=True)
    print(f"    DISTINCT selectable claims  {ce['distinct_claims']:>10,}"
          f"   ← k_selectable", flush=True)
    print(f"    redundant spellings         {ce['redundant_spellings']:>10,}",
          flush=True)
    for d in ("depth1", "depth2", "depth3"):
        print(f"    null {d}  " + " · ".join(
            f"{k.split('_')[0]}={rep['null_routing'][d].get(k,0):,}"
            for k in (TS.OPPORTUNITY_LEVEL, TS.MIXED_LEVEL, TS.DAY_LEVEL)), flush=True)
    print(f"    complement arm bound eligibility {rep['complement_arm_binds']:,} time(s)",
          flush=True)
    print(f"\n  WROTE {dest}\n  WROTE {OUT_PAIRS}", flush=True)
    print(f"  {out['seconds']}s · NO OUTCOME READ", flush=True)
    print("=" * 82, flush=True)


if __name__ == "__main__":
    main()
