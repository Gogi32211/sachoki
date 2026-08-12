# analytic-studio-evidence-boundary-v1

```
semantic-foundation-v1    what a number means
semantic-ui-v1            the meaning survives transport and UI
research-session-v1       what an action costs statistically
combolab-explore-v1       a UI action actually pays that cost
evidence-boundary-v1      REGISTERED is a commitment about DATA, not a flag
```

Before this, `REGISTERED` meant "the hypothesis was named in advance". It now means two things,
and the second is the one that makes a verdict confirmatory:

> the system knows which data took part in forming this claim, and it froze in advance the data
> on which its confirmation may be computed.

## The two holes that were open, and are not now

**The boundary was an argument to the evaluation.** `validate()` accepted a window, so a
researcher could see a result and then pick the window that flattered it, presenting the choice
as the plan. The boundary is now declared before `register()`, hashed into `SESSION_FROZEN`, and
`validate(session_id)` loads it from the ledger. Passing a different one raises
`EvidenceBoundaryDriftError` — the same shape as `SearchSpaceDriftError`, for the same reason.

**Forwardness was a caller assertion.** `data_available_at_registration` arrived in the request
body: the strongest verdict in the system rested on the one field the caller most benefits from
misstating. It is gone from the caller's side. `freeze_boundary()` asks the source catalog, and
two fields are produced by the server or the freeze does not happen:

```
registered_at                    server clock at SESSION_FROZEN
data_cutoff_at_registration      the source's own latest observation
data_snapshot_id                 file size + mtime + cutoff, so a rebuild is a new snapshot
```

A source that cannot state its cutoff blocks the freeze. There is no default, because any
default would decide FORWARD on the system's behalf.

## Declared is not actual, and actual governs

The old `DEV_WINDOW` was a global constant, and replacing it with a request field would only
have moved the problem: a researcher who declares 2024–2025 and reads March 2026 gets a CLEAN
verdict on the declaration. So the declaration and the measurement are separate objects.

```
DataAccessSpec      what a study says it will read      declared
ExposureFootprint   what the access layer recorded      actual
```

Contamination reads the footprint. The symmetry with the search side is exact and deliberate:

```
SEARCH      k_declared          what the frozen space permitted
            k_actual            what the algorithm ranked

EVIDENCE    window_declared     DataAccessSpec
            footprint_actual    ExposureFootprint
```

The access layer RECORDS rather than refuses. Blocking an out-of-spec read would teach callers
to declare wider windows, which costs them nothing and tells everyone else less. Recording makes
the overreach visible: the footprint carries `exceeded_declaration`, the verdict counts
`overreaching_reads`, and the reason text names it.

## Verdict order, by what each one requires

```
INVALID_BOUNDARY   structural — nothing can be said
CONTAMINATED       a fact in the record; the most specific thing known
FORWARD            rests on time, therefore not on the ledger being complete
UNKNOWN            a footprint is missing, so CLEAN cannot be earned
CLEAN              rests on the ledger being complete
```

`UNKNOWN` sits directly above `CLEAN` and is never promoted into it, because that promotion
would be earned by an ABSENCE of information. `FORWARD` outranks `UNKNOWN` for the opposite
reason: it is not earned by absence at all.

## What SESSION_FROZEN records

```
claim_hash · search_space_hash · search_space_size · research_family_id
evidence_boundary_hash · development_access_spec_hash · development_footprint_hash
validation_target_id · registered_at_server · data_snapshot_at_registration
state_hash · code_hash
```

A registered study is now a reproducible commitment rather than a position in a state machine.

## Live, on the real database

```
explorer s0001   declares bars_1d/russell[2024-01-01..2025-12-31]
                 a helper touches 2026-01-01..2026-03-01

register + validate 2026-01-01..2026-06-30
  CONTAMINATED · eligible=False · overreaching_reads=2
  "Some of those reads went beyond what their session declared, which is why the
   declared window is not what this is measured against."

register + validate 2026-09-01..2026-12-31
  FORWARD · eligible=True · cutoff 2026-08-11 (read from studio_analytics.duckdb)
```

## Suites

```
research_session      23/23    domain, adversarial
session_transport      9/9     the transport boundary
session_http          34/34    the real ASGI routing layer
research_durability   13/13    tested through damage
evidence_boundary     19/19    incl. 3 REPRODUCTION cases
semantics_api         12/12
```

## The reproduction cases carried here

Each guard was shown a defect and required to fail on it before being trusted:

```
family-scoped exposure registry   → wrongly approves the laundering path
UNKNOWN ranked below CLEAN        → an unrecorded exposure becomes eligible
declared-window contamination     → the overreach case comes back clean
```

## Still open, and named rather than left implicit

- The access layer is driven by the API telling it what was read. Nothing yet forces a query
  path to go through it, so a new code path can still read data without recording a footprint.
  It would produce UNKNOWN rather than a false CLEAN, which is the safe failure, and it is not
  the same as being unable to bypass.
- `DataAccessSpec.fields` is carried and hashed but nothing consumes it; column-level
  contamination is not modelled.
- `k_family_selectable` sums distinct spaces because the ledger records sizes, not members. It
  is flagged as a BOUND when more than one space is involved.
- Sessions restore per request. Correct, and O(ledger) per call; it will need an index long
  before the ledger is large.
