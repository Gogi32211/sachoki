# analytic-studio-combolab-explore-v1

The fourth checkpoint of the Analytic Studio line. Each one answers a different question, and
each is only meaningful because the one below it holds.

```
semantic-foundation-v1    what a number means
semantic-ui-v1            the meaning survives transport and UI
research-session-v1       what an action costs statistically
combolab-explore-v1       a UI action actually pays that cost
```

The first three were contracts in Python. This one is an operational statement: a mouse click
travels through classification, claim identity, the ledger, the multiplicity accountant, the
transport boundary and into the DOM, and the number that matters survives the whole path.

```
user knob → backend classification → claim identity → ledger
          → multiplicity accountant → transport → DOM
```

## The golden end-to-end fixture

Four actions in the browser, counters read out of the DOM, not out of a response body.

```
START                     exposed 0 · selectable 0  · revisits 0 · claim changes 0
tolerance ±5 → ±1         exposed 1 · selectable 31 · revisits 0 · claim changes 1
reopen this result        exposed 1 · selectable 31 · revisits 1 · claim changes 1
horizon 20 → 40           exposed 2 · selectable 31 · revisits 1 · claim changes 2
```

Three readings carry the checkpoint.

**`selectable 31` while the screen shows five.** The algorithm ranked thirty-one cells and the
UI displays five. A frontend that counted the cards it received would report five, and five is
the number that makes a finding look significant when it is not. It was 31 at every step and 5
at none, in no frame and for no interval.

**`reopen` moved `revisits` and not `exposed`.** Looking at the same specification again is free
and is recorded as free. The accountant charges for new claims, not for clicks.

**Both knobs were classified `CLAIM_CHANGE` before they ran.** The preview endpoint answers what
a change means while the result does not yet exist, so the classification cannot be revised once
the number turns out to be attractive.

Replayed as `test_session_http.py::t7`, so the fixture is executable and not a screenshot.

## What this checkpoint does NOT claim

- `REGISTERED` is not in the UI. Nothing here is confirmatory; `confirmatory_eligible` is `NO`
  at every step and cannot become `YES` in an exploratory session.
- Two of the twenty declared parameters are wired. The other eighteen are unreachable from this
  screen, which is why `k_selectable` is a constant here.
- Sessions are in-memory. A restart loses them, which is correct for an exploratory working
  object and wrong for anything registered — that is a prerequisite of the next phase, not this
  one.
- The `31` is the declared size of the ComboLab v2 search space, carried as a constant. This
  slice proves the accountant transports it, not that a search of that size ran behind it.

## Frozen with this tag

```
backend/research_session.py            the ledger and the three k
backend/studio_session_api.py          transport; models at module scope
backend/test_research_session.py       15 · domain, adversarial
backend/test_session_transport.py       9 · the transport boundary
backend/test_session_http.py           13 · the real ASGI routing layer
frontend/src/studio/                   the strict TypeScript island
```

Suites at the tag: 15/15 · 9/9 · 13/13 · semantics 12/12 · tsc and the negative type tests pass.

## The incident this checkpoint carries

`FASTAPI_LOCAL_MODEL_RESOLUTION`, recorded in `INTEGRITY_LEDGER.md`, and the fifth integrity
layer it named:

```
computation → artifact → transport → presentation → integration
```

Every layer was individually correct and the binding between them was not. The rule that came
out of it — an HTTP acceptance test must travel the real routing layer — is now enforced by
`test_session_http.py`, and the rule that came out of *diagnosing* it: a regression guard must
be run against the reintroduced defect before it is trusted, because the first version of that
guard passed on the broken code.

## Next, in order

```
1. SessionFork contract          a legal path out of a frozen session
2. REGISTERED state in the UI    with the refusal stated, not a disabled button
3. adversarial UI tests          register-after-exposure, mutate-after-freeze
4. the remaining parameters      only after the state machine is proven in the UI
```

`v2.1 DAY_LEVEL` is methodological backlog and is deliberately untouched: this line builds a
research interface around a v2 that is already closed.

---

# Addendum · SessionFork and the REGISTERED state

Added after the tag above, in the order agreed: fork contract → registered UI → adversarial paths.

## The rule the browser corrected

The fork was first implemented so that a child could still preregister when the lineage had
exposed nothing — the same rule as `register()`, read over the lineage instead of the session.
That looked principled and the browser showed it was wrong: a fork of a freshly registered
parent walked straight into the confirmatory track.

The rule now is the one that was specified: **a forked session can never register**, whatever
the parent had seen. Two reasons, and the second is the one the first version missed.

- A fork inherits a CHOICE of specification. The parent existed for a reason and someone had
  been looking at something. Preregistering that starting point claims it arrived from nowhere.
- Allowing it opens a path nothing accounts for: two registered studies, siblings in one
  lineage, each declaring `k = 31`, with no record connecting the pair. Cross-session
  multiplicity does not exist yet, so forks stay out of the confirmatory track entirely.

The cost is one extra step — to preregister, open a session with no parent and state the
specification from nothing. That step is the point.

## What a refusal has to contain

Nothing on the screen is disabled. The register button stays live on a session that can never
register; the knobs stay live on a frozen study. A greyed control teaches nothing and invites
the user to find the click order that avoids the grey. Pressing it returns the backend's own
sentence plus `next_action`:

```
SessionStateError                    → FORK          continue the work, exploratory
CannotRegisterAfterExposureError     → NEW_SESSION   the only route back to confirmatory
anything else                        → NONE          the move has no honest form
```

`next_action` exists because the first version offered a fork to a session that forking cannot
help, which sends the user around a loop. Which legal move exists is a property of the rule that
fired, not of the error class.

## Adversarial paths, in the browser

```
1  fresh EXPLORE            declared 0  · confirmatory NO
2  preregister              declared 31 · confirmatory YES · mode REGISTERED
3  turn a knob              REFUSED SessionStateError · offers FORK
                            state_hash unchanged — the refusal did not move the ledger
4  fork with no reason      REFUSED · "a fork must say why"
5  fork with a reason       s0001 → s0002 · EXPLORE · declared 0 · confirmatory NO
                            panel shows the parent and what was inherited
6  child preregisters       REFUSED CannotRegisterAfterExposureError
                            offers NEW_SESSION, not FORK
```

Suites after the addendum: domain 23/23 · transport 9/9 · HTTP 21/21 · semantics 12/12.

## Also fixed here

`register()` declared the search space before checking whether registration was allowed, so a
refused registration left a `SEARCH_SPACE_DECLARED` event describing a registration that never
happened. Split into `assert_registerable()`, which raises without touching the ledger. A
refusal must cost nothing, the same rule as a preview. Guarded by `t16`.
