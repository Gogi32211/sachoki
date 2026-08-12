# analytic-studio-combolab-results-surface-v1

```
research-session-v1         an action has a statistical cost
combolab-explore-v1         the cost survives mouse → ledger
evidence-boundary-v1        which data may count as confirmation is known
mandatory-data-gateway-v1   a read cannot quietly leave the accounting
parameter-surface-v1        every degree of freedom has one backend semantics
control-surface-v1          all 22 UI controls use exactly that semantics
results-surface-v1          the number of rows on screen is part of the contract
```

## Two corrections the table forced, before a line of it existed

**`SELECTION_PATH_CHANGE` moved the wrong thing.** The first version had it move
`search_space_hash`, and the results table made the error obvious: re-ranking does not change
what the algorithm could pick from — thirty-one stays thirty-one — and neither does showing ten
rows instead of five. What changes is which claims, and how many, become reachable by a person.
The consequence is EXPOSURE, counted by the ledger, not a specification hash. `changes` is now
empty and the effect is `EXPOSURE_CHANGED`.

**`displayed_top_k` stopped being cosmetic.** By the predicate already written for display
sorting — presentation only if it cannot change which claims become inspectable — five rows
becoming ten is five more claims a person can reach. On `CONTROL_SURFACE` it is still a view,
because nothing is displayed by it. On `RESULTS_SURFACE` it is a selection path. The two
lookalike numbers are now costed for different reasons:

```
displayed_top_k    the exposed set grows      k_exposed moves · k_selectable does not
selection_top_k    the selectable set grows   k_selectable and search_space_hash move
```

## Exposure is delivery

```
a claim is EXPOSED when its result is made available to the researcher
through a sanctioned research surface
```

Not "someone read it". Whether a person's eyes reached the fifth row is unmeasurable and
unreproducible, and a system that tried would be guessing in the direction that suits it. The
tab closing early does not refund anything.

Which forces the transport rule: **`rows` is the authorised set, not the ranking.** Ship
thirty-one rows and let the client render five, and thirty-one were exposed — they are in the
response, in memory, in devtools, one keystroke from being read. The invariant
`displayed_count == len(rows)` is asserted on construction in Python and again in the TypeScript
decoder.

```
SearchRunArtifact   server-only · all 31 ranked ids · policy · provenance
SearchRunView       the authorised subset · counts · freshness
```

Exposure accounting is transactional with delivery: the rows that leave `search()` are the rows
the ledger charged for, in the same call.

## Staleness is anchored to the specification, not to the ledger

The first attempt compared `input_state_hash` against the session's event-state hash, and every
run was stale the instant it existed — a search appends its own events, so the counter had
always moved by the time anyone could look. Growing the ledger is not changing the question.
`specification_hash` covers claim, space, policy and display policy.

A stale run stays readable and stops being actionable. Those are different rights, and
collapsing them either erases the history or lets a verdict attach to a specification nobody is
looking at.

## The transport rule is narrower than N0's, on purpose

```
forbidden   effect.value · ci_low · ci_high · any raw estimate
required    effect.display_value · uncertainty.display_value · inspector_ref
allowed     rank · selectable_count · displayed_count · hashes
```

Reusing "no numeric leaves anywhere" here would have been cargo-culting it. Nobody derives a
verdict by subtracting two ranks; the ban belongs to statistical operands.

## Seven browser scenarios, on the real screen

```
1 initial run          selectable 31 · ranked 31 · exposed 5 · DOM rows 5
                       k_selectable 31 · k_exposed 5

2 displayed 5 → 10     plan SELECTION_PATH_CHANGE · EXPOSURE_CHANGED
                       the standing table went STALE
                       after rerun: DOM 10 · exposed 10 · k_exposed 10
                       k_selectable still 31

3 selection 31 → 37    plan SEARCH_SPACE_CHANGE · space hash shown
                       after rerun: selectable 37 · DOM still 10
                       k_selectable 37 · k_exposed 20

4 row inspector        sampling target · null family · evidence claim hash ·
                       decision spec hash · provenance

5 transport            statistical numeric leaves: []   counters still numbers
                       effect reads "+0.53 pp" as text

6 round trip           (control-surface-v1) screen returns, ledger does not

7 stale run            promote on FRESH 200 · turn a knob · promote 409
                       StaleSearchRunError · next_action RERUN
```

`displayed rows ≠ exposed universe ≠ selectable universe`, proven on a human screen rather than
only in the ledger.

## The numbers are a fixture

There is no search engine behind this yet. Rows are derived deterministically from claim
identities so the contract can be exercised end to end, and every artifact carries
`data_provenance = SYNTHETIC_FIXTURE`, shown in the table header and in the passport. A
screenshot of this table cannot be mistaken for a finding.

## Suites

```
research_session       23/23     session_http           45/45
session_transport       9/9      research_durability    13/13
evidence_boundary      19/19     data_gateway           14/14
parameter_surface      22/22     search_run             12/12
research_path_isolation 6/6      semantics_api          12/12
```

## Hardening, added after the checkpoint was first written

**The fixture label became a prohibition.** It was a badge: the header said SYNTHETIC_FIXTURE
and `promote` answered 200. The screenshot was defended and the workflow was not — the screen
said "this is not a finding" while the server let one be treated as a finding.

```
evidence_origin      SYNTHETIC_FIXTURE · HISTORICAL_RESEARCH · FROZEN_FORWARD

read-only  inspect · rerun · change_controls        available under every origin
outward    promote · freeze · register_verdict · forward · book

SYNTHETIC_FIXTURE     read-only
HISTORICAL_RESEARCH   + promote · freeze · register_verdict
FROZEN_FORWARD        + forward · book
```

Origin is checked BEFORE freshness, and that order matters: staleness asks whether this run is
current, origin asks whether the evidence can ever support the action. A fresh fixture answering
"re-run and you may promote" would be a lie in the helpful direction. The view publishes
`allowed_actions`, and the table renders that decision rather than offering a button that will
answer 409.

**Exposure identity, proven on controlled overlap.** `k_exposed 5 → 10` was the right number and
that is not the same as the rule being right. The identity is
`(evidence_claim_hash, decision_spec_hash)` — never `run_id`, `rank` or a row position, all of
which change for free between neighbouring specifications.

```
run A exposes  A B C D E      k_exposed 5 · revisits 0
run B exposes  C D E F G      k_exposed 7 · revisits 3        not 10
same set reversed             k_exposed unchanged · revisits +5
same rows, new decision rule  k_exposed +3 · revisits 0
```

Live: one specification searched twice leaves `k_exposed` at 5 and grows revisits. Real ranking
makes this acute — adjacent top-N windows overlap heavily, and a run-keyed counter would charge
full price for a list that moved by one.

## Not done, named

- The rows are synthetic. Connecting a real search is the next thing, and it changes no contract
  here — `rank_and_authorise` is where an engine plugs in.
- The comparison guard is enforced on N0 and not yet reachable from a results row, so scenario 5
  was verified as "no operand crosses" rather than as a blocked delta between two rows.
- `ISOLATED` remains a separate future milestone. `ENFORCED_IN_PROCESS` is what is claimed.
