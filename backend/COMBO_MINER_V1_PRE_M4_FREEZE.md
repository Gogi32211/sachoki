# COMBO_MINER_V1_PRE_M4_FREEZE

The state of the instrument at the moment **before any historical outcome was read**.

This file exists so that "which code and which specification opened the historical evidence"
has one unambiguous answer. After Y is opened there must be no room for *we tweaked the code
a little but the methodology is the same* — the tag below is what M4 ran with, and anything
later is a different instrument.

## Specifications

| | digest |
|---|---|
| `COMBO_SEARCH_SPEC.json` | `fa7b938d01258644` |
| `COMBO_CAPABILITY_SPEC.json` | `f1194df7be07c3ee` |
| token registry (`combo_tokens_spec.py`) | `08b73d474f17a778` |

## Population

```
RETURN-observable        275,307
ontology (incl. terminal) 275,625
population_hash          ca2c502963ce9b85
setup families                57
dates                      1,244
feature_data_as_of                2026-08-07
label_source_snapshot_as_of       2026-08-07
maturity_cutoff                   2026-05-12   (derived from the market calendar)
```

Maturity is **calendar only**: 60 complete market bars after entry must fall inside the frozen
snapshot. No exception for cohorts whose trailing stop happened to fire early — that exception
would select on the outcome.

## Outcome

```
REALIZED_RETURN_TRAIL12_TIMER60_V1
  ordinary exit   ATR × 12 trailing
  fallback exit   close at bar 60
  exit mix        trail 11,681 · timer 263,626   (descriptive, not part of the identity)
  legacy          `ret` = LEGACY_REPRODUCTION_ONLY
```

The name says `TIMER60` because 91% of exits are the timer. A label called `TRAIL` alone would
promise a mechanism that mostly does not happen.

## Terminal events

```
CORPORATE_ACTION_TERMINAL   318 rows · 33 tickers
terminal_payoff             UNRESOLVED   (no consideration data exists in this repo)
RETURN treatment            excluded from the numerical estimand
ontology treatment          competing event, NOT censoring
RETURN coverage             275,307 / 275,625 = 99.885%
SELECTION_RISK              NOT_ESTIMATED_IN_V1
concentration audit         treated-arm share: median 0.106% · max 0.805% · 0 claims >1%
```

The RETURN estimand is therefore **conditional on a resolvable non-terminal outcome**. Terminal
events are not assumed to carry zero return or zero selection effect.

## Search

```
grammar          BaseSetup + 0..2 contextual tokens · distinct families · base setup REQUIRED
universe         fixed exhaustive, enumerated before any outcome existed
adaptive beam    NOT USED
null family      OPPORTUNITY_LEVEL only  (deferred: MIXED 97 · DAY 1)
k                4,636 DISTINCT selectable claims
ranking          theta_hat descending — no stability, novelty or composite score
null generator   within_stratum_outcome_v1 · outcomes inside (date × family)
band             Monte-Carlo max over 120 permutations — NOT an exact quantile
```

## Primary capability qualification

```
PASS · detected 20 / 20
delta                    +1.5 pp
needle                   L1+H1_DR   (q50 eligible-support, X-only, chosen once, unique)
theta_needle             min +1.310 · median +1.422 · max +1.672
band_p95                 min +0.857 · median +1.034 · max +1.163
margin                   min +0.220 · median +0.463 · max +0.635
one-sided 95% lower bd   ~86.1%   (exact, Clopper-Pearson)
historical exposure      NONE
```

Read as a **measured detection rate in a registered finite suite**, not as the true power.

## Code frozen by this tag

```
combo_tokens_spec.py      the vocabulary and the grammar
combo_tokens.py           M0 coverage audit + M1 sidecar
combo_universe.py         M2 dependency map + M3 universe count
combo_qualify.py          Gate C cost qualification
combo_label_contract.py   M2.6 maturity and terminal ontology
combo_freeze.py           M2.7 routed equivalence, terminal audit, SearchSpec
combo_capability.py       primary capability qualification
```

Data artifacts (`combo_tokens.parquet`, `combo_pair_dependency.parquet`,
`combo_label_status.parquet`) are regenerable from this code and the frozen snapshot, and are
excluded from git by the existing `data/` rule.

## What has NOT happened

`REALIZED_RETURN_TRAIL12_TIMER60_V1` has never been read. Not its values, not its missingness,
not as a filter. Every number above was computed from prices, dates, membership and synthetic
outcomes.
