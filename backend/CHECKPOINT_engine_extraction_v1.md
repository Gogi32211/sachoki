# analytic-studio-combolab-engine-extraction-v1

```
ENGINE EXTRACTION             QUALIFIED
SEALED EXECUTION EQUIVALENCE  PASS
FRESH-PROCESS REPRODUCTION    PASS

HISTORICAL APPLICATION        NOT QUALIFIED
HISTORICAL EXECUTION          BLOCKED

oracle                        ff53151
oracle hash                   7c421ae062742d06
spec                          real-y qualification 601f359bf5f47184 — NOT YET RUN
```

## The claim, and deliberately not a word wider

> `run_v2(SEALED_ACCEPTANCE)` reproduces the old sealed computation on the frozen fixture,
> across all 31 registered OPPORTUNITY_LEVEL cells, in one process and again across two fresh
> ones.

It says nothing about whether the instrument is suited to real returns. The historical mode
exists, has a provider, and is refused — because an enum member is not a licence.

## What was found on the way, and none of it by reading code

**The v2 sealed acceptance never touched real returns.** It ran on `Y = μ_setup + γ_date + ε`
with a planted needle and measured detection. There was no "already computed ranking" to adapt,
and pointing the validated instrument at `O["ret"]·100` is a FIRST USE rather than a replay of a
validated result.

**`boot` was not extractable verbatim.** A static dependency inventory showed it keyed its RNG on
`(world, δ, cell, rep)` — coordinates that exist only because a needle was planted. The leak sat
four positional arguments deep inside `S1R.key_rng(...)`, and a reviewer scanning for "does this
use delta" meets `delta_star` two lines later and moves on.

**Removing a coupling moves the error to whoever now assembles.** After the inversion the kernel
cannot confuse the coordinates because it never sees them — which made the wrapper the only place
they can be mismatched, and it had nothing to catch a δ=1.5 outcome running under a δ=0.6
context. Writing the adversarial fixture is what revealed the missing identity.

**A guard reported clean because it could not see what it was extended to watch.** The first
inventory read free names only, so `self.world` was invisible and the provider — the one object
that must carry the coordinates — showed none.

**A guard flagged its own documentation.** The display-parameter scan searched raw source and
matched the paragraph forbidding those names. Rewritten to walk the AST.

## The ladders

```
3B.1  Frozen · support_hash · verdict     SOURCE-MECHANICAL
      41 computational lines verbatim, oracle untouched and unedited

3B.2  boot                                TRACE-PROVEN
      semantic key · stream requests · ordered sampled geometry ·
      bootstrap bits · intervals · verdicts        EXACT
      31/31 cells at δ=0 · 6 targeted cells at δ=1.5
      geometry invariant to the outcome, on a perturbation that moved every estimate

4C    run_v2(SEALED_ACCEPTANCE)            ORCHESTRATION EQUIVALENT
      one process   EXACT   31 cells · 100.6s + 101.7s
      fresh × 2     EXACT   102.0s + 102.3s
      negative controls: swap cells · flip one θ bit · change one verdict — all CAUGHT
```

The negative controls matter because `legacy_projection()` became part of the proof. A
projection that dropped verdict stages would pass every positive test there is.

## Two identities, different shapes on purpose

```
OutcomeIdentity = f(world, δ)               two outer reps share one outcome vector
RNGIdentity     = f(world, δ, cell, rep)
```

Binding `rep` into the outcome would be a false provenance link.

## What is still true and still blocked

- `HISTORICAL_RESEARCH` raises `HistoricalApplicationNotQualifiedError`. Two gates stand in
  front: this one, now passed, and the registered real-y numerical qualification, not run.
- The 31 real-y θ pinned in `V2_CORE_ORACLE.json` remain `ENGINE_QUALIFICATION_EVIDENCE`
  forever. Qualifying the engine does not re-qualify what it already produced.
- The old implementation stays as the reference path. It is removed after `run_v2()` reproduces
  the oracle from a clean checkout, not after this checkpoint.
- Six DAY_LEVEL claims are NOT_IN_SEARCH_SPACE by declaration, not by filtering.

## Suites

```
v2_engine              12/12    v2_engine_contract      14/14
v2_kernel_deps         10/10    v2_bootstrap_equiv       9/9
v2_sealed_wrapper       7/7     v2_extraction_equiv      7/7
outcome_integrity      12/12    evidence_status         10/10
```
