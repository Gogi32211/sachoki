# COMBO_M4_EXPOSURE_RECORD

The boundary. Everything before it was computed without ever reading a historical outcome;
everything after it was not, and no later work can undo that.

```
BEFORE      tag combo-miner-v1-pre-m4   commit 0f03622
            historical_exposure = NONE

EXPOSURE    combo_m4.py · load_outcome() · ret_true
            REALIZED_RETURN_TRAIL12_TIMER60_V1
            275,307 rows of the frozen population

AFTER       tag combo-miner-v1-m4-exposed
            historical_exposure = OPENED
```

## What was run

Exactly one registered search, with the frozen instrument asserted on entry — a moved
population hash, a moved spec digest or a different `k` would have stopped it.

```
SearchSpec        fa7b938d01258644
CapabilitySpec    f1194df7be07c3ee     (verdict PASS, required to open M4)
population        275,307 · ca2c502963ce9b85
k                 4,636
outcome           REALIZED_RETURN_TRAIL12_TIMER60_V1
estimand          stratified within-setup median difference, pp
null              within_stratum_outcome_v1 · outcomes inside (date × family)
permutations      120 · seed 20260816
```

## Result

```
M4 result digest  e53a792e70bc07ff
band p95          +4.8302 pp     MC interval [+4.7055, +5.0958]
max theta_hat     +4.4110 pp     GR_G3+RSI_0_30
survivors         0 of 4,636
role              EXPLORATORY_HISTORICAL_EVIDENCE
```

What this supports, stated at exactly its own strength:

> `max θ̂ = +4.411 < band = +4.830` — none of the 4,636 **estimated** incremental effects
> crossed the registered search-wide band on this population.

It is **not** a statement about which true effects exist. θ̂ is noisy in both directions.

## COMPUTE and EXPOSE were separate commands

`combo_m4.py` sealed the artifact and printed the band, the survivor count and the hashes —
no claim identity. `combo_m4.py expose` read the sealed file afterwards. Delivering results to
a person *is* the exposure, so the record had to exist independently of what it said.

## The erratum that came with the result

`COMBO_CAPABILITY_SCOPE_ERRATUM.json`. The capability PASS (20/20 at δ = +1.5 pp) was
qualified under `composition_world`, whose outcome has sd 7.2 pp. `ret_true` has sd 21.3 pp
and much heavier tails, and its band is **4.67× wider**. The capability result stands within
its own scope; what was wrong was carrying it across.

Search tax was identical in both runs — same 4,636 claims, same `k`. Only the noise geometry
changed. Detection difficulty is therefore not a function of `k` alone.

## What may not happen now

Re-running M4, changing δ, reducing `k`, switching horizon, or testing `GR_G3+RSI_0_30`
without the search tax. Rerunning cannot upgrade the label — that is
`REPLAY_OF_EXPOSED_EVIDENCE`, and the honest path to a stronger word remains the frozen
forward spec.

The open question is not "does Combo Miner work" — that was settled before the search, which
is the only order in which it can be settled. It is: **what effect size can this instrument
see in the noise that is actually present in the data?** That work is post-exposure, carries
no confirmatory standing, and is named as such.
