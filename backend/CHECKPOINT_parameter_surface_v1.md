# analytic-studio-combolab-parameter-surface-v1

```
semantic-foundation-v1     what a number means
semantic-ui-v1             the meaning survives transport and UI
research-session-v1        what an action costs statistically
combolab-explore-v1        a UI action actually pays that cost
evidence-boundary-v1       which data took part in forming the claim
mandatory-data-gateway-v1  whether data can be seen outside the accounting at all
parameter-surface-v1       twenty-two knobs are not twenty-two ways around one ledger
```

## The acceptance statement

Two parameters of the same semantic role behave identically, with no per-parameter code
anywhere. Wiring controls one at a time is how a single ledger acquires twenty different paths
through it — this one updates the hash, that one forgot, the third previews one thing and
commits another — and every path is a place where the accounting can differ from the screen.

`t1` drives every member of a role through the same call and compares field by field. `t1b`
shows it its defect first: one parameter given its own branch, exactly the way a hand-wired
control ends up, and the comparison must break.

```
PRESENTATION_ONLY     5   column_order · displayed_top_k · layout ·
                          sort_by_displayed_column · theme
CLAIM_CHANGE          8   base_setup_conditioning · conditioning_feature ·
                          conditioning_tolerance · date_range · horizon ·
                          outcome_metric · universe · weighting
DESIGN_CHANGE         1   support_cutoff
SEARCH_SPACE_CHANGE   5   rank_metric · selection_top_k · setup_subset ·
                          sort_by_new_outcome_metric · top_k
POLICY_CHANGE         3   direction · equivalence_margin · null_family
```

## Three hashes, one classifier

```
claim_hash            everything that can change the answer
search_space_hash     what the algorithm was permitted to choose among
decision_policy_hash  the rules by which a winner is called a winner
```

`preview()` and `apply()` do not each decide what a change means. `classify()` decides and both
call it, so a preview cannot promise what the ledger will not keep — asserted over HTTP, not
only in process.

## A UI number carries no role

`displayed_top_k` 5 → 10 shows fewer rows of the same ranked 31 and is a view.
`selection_top_k` 31 → 37 changes the multiplicity a verdict must survive. Both read as "a
number went up", so they are two parameters in the registry rather than one control whose
meaning depends on which code path reads it.

## The round trip

```
tolerance ±5 → ±1 · horizon 20 → 40 · sort A → B · horizon 40 → 20 · ±1 → ±5
```

Live, through the real server:

```
screen back at start   horizon no_op=True, value=20
ledger did not rewind  k_exposed 0 → 3 · claim changes 4 · presentation charged 0
```

```
current state = initial state   ⇏   research history = initial history
```

The cheapest laundering path of all the ones found so far: it needs no new mechanism, only the
assumption that state and history are the same thing.

## Canonicalisation is part of identity

`"20"`, `" 20 "` and `20` are one horizon. If they hashed differently, reopening a specification
would invent a new claim and `k` would drift upward on whitespace. Reselecting the value already
set is a no-op with effect NONE, so clicking the current setting costs nothing.

## Defects this milestone produced and closed

- **Two registries.** `displayed_top_k` and `selection_top_k` were declared in the surface and
  not in the ledger's `PARAMETERS`, so they were turnable on screen and refused by
  `change_parameter`. Two sources of truth about one knob — the exact defect this milestone
  exists to prevent, arriving through the file meant to prevent it. One registry now, in
  `research_session`, asserted by `registry_has_one_home()`.

- **A value patched onto a written event.** The first caller appended `CONDITION_CHANGED` and
  then set `payload["value"]` on the in-memory object. The row on disk did not have it, so a
  restart replayed the ledger into the wrong settings. Fourth occurrence of the same shape;
  `value` now travels in the event at write time, and `t34` restarts the process to check it.

- **A literal route swallowed by a path parameter.** `GET /parameters` answered
  `"no session parameters"` because `/{sid}` was declared first and FastAPI matches in
  declaration order. Nothing below the request could see it. `t29`.

## Suites

```
research_session       23/23     session_http           40/40
session_transport       9/9      research_durability    13/13
evidence_boundary      19/19     data_gateway           14/14
parameter_surface      16/16     research_path_isolation 6/6
semantics_api          12/12
```

## Not done, named

- The UI still renders two knobs. The backend serves all 22 with their roles at
  `GET /api/studio/session/parameters`; the screen has not been widened yet.
- Cosmetic settings are deliberately not in the ledger, so they do not survive a restart. That
  is correct — presentation state is client state — and it means the client owns them.
- `ISOLATED` remains a separate future milestone. `ENFORCED_IN_PROCESS` is what is claimed.
