# analytic-studio-combolab-control-surface-v1

```
research-session-v1         an action has a statistical cost
combolab-explore-v1         the cost survives mouse → ledger
evidence-boundary-v1        which data may count as confirmation is known
mandatory-data-gateway-v1   a read cannot quietly leave the accounting
parameter-surface-v1        every degree of freedom has one backend semantics
control-surface-v1          all 22 UI controls use exactly that semantics
```

## The plan between preview and commit

Sharing the classifier removed one disagreement and not the other. The gap is human: a person
reads what a change costs, thinks, and clicks — and in between the session may have moved. Same
classifier, different transition, and the change applied is not the one anybody approved.

`preview` now returns a `ChangePlan` pinned to `prior_state_hash` and to the
`parameter_registry_hash` it was classified under. `commit` presents `plan_hash`, and the
backend refuses on any mismatch rather than recomputing:

```
StaleChangePlanError → next_action REPREVIEW
```

Plans are single use, cannot be pointed at a different knob, and are lost on restart — an
unknown plan is refused exactly like a stale one, because a plan whose state cannot be checked
is a plan that proves nothing.

## Generated, not hand-written

```
GET /session/{id}/parameters
        ↓  ParameterDefinitionView × 22
   one ParameterControl, branching only on ui_kind
        ↓  previewParameter()
   ChangePlanView shown to the user
        ↓  commitParameter(plan_hash)
```

The frontend branches on `ui_kind` (NUMBER · ENUM · MULTI · TEXT) and never on `parameter_id`
or on `semantic_role`. The role is displayed as a badge and is never derived — a NUMBER control
looks the same whether the number is a view or a multiplicity, which is exactly why the role
travels beside it.

Grouped by meaning rather than as one long form: Hypothesis · Population & design · Search ·
Decision · View.

## Exhaustive browser matrix — all 22, not one per role

Every parameter driven through preview → plan → commit, with the session accounting read before
and after each one:

```
tested        22
violations     0

CLAIM_CHANGE          recorded · claim moved · exposed +1 · claim changes +1
DESIGN_CHANGE         recorded · claim moved · exposed +1 · design changes +1
SEARCH_SPACE_CHANGE   recorded · space moved · search-space changes +1
POLICY_CHANGE         recorded · policy moved · policy changes +1
PRESENTATION_ONLY     NOT recorded · nothing moved · zero ledger cost
```

Each role has exactly ONE behaviour signature across all its members. A role with two signatures
would mean a control had grown its own path.

## Two defects the matrix found

Both were invisible to a per-role spot check, which is the argument for driving all 22.

**A whole accounting category stopped at the transport boundary.** `DESIGN_CHANGE` was counted
in the ledger and absent from `ResearchSessionView`, so `support_cutoff` — the only member of
its role — moved a counter nothing could display. Added as `changes_design`, and shown.

**Two notions of "the claim".** Six of the eight `CLAIM_CHANGE` parameters moved the surface's
`claim_hash` while the ledger exposed a `ClaimIdentity` built from `horizon` and `tolerance`
alone. The ledger saw the same claim twice and `k_exposed` did not move — an under-count, which
is the direction that flatters. `conditioning_hash` now carries `surface.claim_hash`, which
covers every claim-identity parameter by construction, and `decision_policy_version` carries the
policy hash. A policy change therefore also produces a distinct exposed claim: that counts MORE,
and between the two directions the conservative one is the only defensible default.

## The two lookalike numbers, through the real UI

```
displayed_top_k  5 → 10    view    · effect NONE    · no hash line · k_selectable unchanged
selection_top_k 31 → 37    search space · SEARCH_SPACE_CHANGED · space hash shown
                                     · k_selectable 32 → 37 · search-space changes +1
```

On screen they are two number inputs side by side. The badge and the plan are the only things
that separate them, and both come from the backend.

## The round trip, in the browser

```
±5 → ±1 · 20 → 40 · sort → pf · 40 → 20 · ±1 → ±5

screen   horizon 20, tolerance 5    identical to the start
ledger   claims exposed 0 → 3 · claim changes 4 · presentation cost 0
```

```
current state = initial state   ⇏   research history = initial history
```

## TOCTOU, in the browser

```
turn horizon 20 → 60          plan af4d908d5c527b30 shown
session moves underneath      universe → sp500, by another caller
click apply                   REFUSED · StaleChangePlanError
                              horizon still 20 — nothing was applied
                              "Turn the control again to get a current preview"
```

## Suites

```
research_session       23/23     session_http           45/45
session_transport       9/9      research_durability    13/13
evidence_boundary      19/19     data_gateway           14/14
parameter_surface      16/16     research_path_isolation 6/6
semantics_api          12/12
```

## Not done, named

- The screen renders controls and accounting, not results. There is no results table yet, so
  `displayed_top_k` has nothing to draw fewer of; its role is proven through the plan and the
  ledger rather than through rows on screen.
- Cosmetic settings are deliberately absent from the ledger and therefore do not survive a
  restart. Presentation state is client state.
- `ISOLATED` remains a separate future milestone. `ENFORCED_IN_PROCESS` is what is claimed.
