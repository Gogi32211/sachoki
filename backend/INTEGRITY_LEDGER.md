# Integrity ledger

Incidents are recorded, not erased. An incident that left no trace teaches nothing, and "there
was never a mismatch" is exactly what a reader would conclude six months from now.

---

## 2026-08-11 · RUNNING_ARTIFACT_DRIFT

```
type        RUNNING_ARTIFACT_DRIFT
effect      NONE_ON_COMPUTATION
reason      the Python process had already imported the frozen S1 spec
detected    runtime digest != working-tree digest
remedy      working tree restored to the runtime spec (b532a82)
status      RESOLVED
```

`s1_spec.py` was edited while the S1-31 run was in flight, to add the
`NO_NEW_PRIMARY_ENDPOINT_AFTER_REVEAL` commitment. The run itself was untouched: the module had
been imported at launch and the new constants were declarative, read by no code path. But for a
few minutes the on-disk artifact stopped describing the experiment that was actually running:

```
run printed   s1 digest d4a9bb1e72228200
disk became   811eb797e98568c1
```

Caught by comparing the digest the runner printed at start against the file on disk. The spec was
reverted to the version the run imported, and the commitment moved to `s1_reveal_contract.py`,
which nothing imports.

### The contract this produced

> **No mutable working-tree artifact describing a running experiment may change before that run
> completes. Execution semantics and reveal semantics are hashed separately.**

Two independent fixations, because they answer different questions:

```
EXECUTION CONTRACT      what the machine computed
  s1_spec semantic digest   d4a9bb1e72228200
  s1_spec source blob       617680e15c01db5fb45b26db4132a0d27121f446

INTERPRETATION CONTRACT what we are permitted to conclude once we see it
  s1_reveal_contract blob   e6a6e5ada48b7242fa671eca03b928418ea11490
```

Without the second, the semantic spec would stay pinned while the reveal policy drifted freely
between today's commit and the sealed reveal. Git would let that be investigated afterwards; the
integrity gate should forbid it beforehand.

### Why both a semantic digest and a source hash

```
semantic digest identical  ⇏  source artifact identical
```

A digest covers the payload it was told to cover. This project has already had the other half of
that lesson twice today: constants added outside the digest payload left it unchanged while the
file moved, and a CSV round-trip changed a float by one ULP while every printed figure matched.
Requirement for the next launch/freeze protocol — not retrofitted into the running S1:

```
spec_semantic_digest
spec_source_blob_hash
```

---

## Reading order, as an absolute gate

```
STRUCTURAL INTEGRITY
  Final ⇒ ScreenPass
  ScreenPass ⇒ Rank ≤ 5
  Final ⇒ Known

    3/3 PASS  →  CAPABILITY may be read
    any FAIL  →  recall is not interpreted at all
```

The same logical priority already registered for sealed acceptance: integrity outranks
capability, and a number computed by an experiment that was not the registered experiment has no
operating characteristics to discuss.

---

## 2026-08-12 · TAILWIND_CONTENT_SCOPE_GAP

```
type        TAILWIND_CONTENT_SCOPE_GAP
cause       content glob covered js/jsx but not ts/tsx
effect      Studio classes absent from the generated CSS
symptom     transparent Inspector drawer overlaying the cards
detected_by browser inspection of the production build
remedy      include ts/tsx in the content glob; move components onto project MD3 tokens
status      RESOLVED
```

`tailwind.config.js` scanned `./src/**/*.{js,jsx}`. The Analytic Studio island is `.ts/.tsx`, so
not one of its classes was generated, and the passport drawer rendered with no background over
the cards it was meant to sit beside.

### The fourth integrity layer

Everything below the browser passed:

```
tsc                 PASS
vite build          PASS
transport tests     12/12, payload correct, zero numeric leaves
integration gate    6/6 over HTTP
```

Correct computation, correct artifacts, correct transport — and the meaning still arrived
unreadable. That is a layer this project had not named:

```
computation → artifact → transport → PRESENTATION
```

The first three were already defended by contracts. The fourth has no test below the browser
that can see it, which is why "open the production build and look at it" is a step of its own
and not a formality. It found this on the first try.

### Also recorded

Components were moved off the foreign `neutral-*` palette onto the project's own MD3 tokens
(`bg-md-surface-con`, `text-md-on-surface-var`, `border-md-warning`). An island should speak the
application's language rather than import its own; the transparent drawer was the visible half
of that mistake and the palette mismatch was the other.

---

## 2026-08-12 · FASTAPI_LOCAL_MODEL_RESOLUTION

```
INTEGRATION_INTEGRITY_INCIDENT
type        FASTAPI_LOCAL_MODEL_RESOLUTION
cause       PEP 563 string annotations + request model in local function scope
            (neither is a defect alone; the pair is)
effect      body parameter degraded to a query field → HTTP 422
            AND the OpenAPI schema could not be built → /openapi.json 500, app-wide
detected_by real browser → FastAPI request path
missed_by   direct Python tests / TS / transport unit tests
remedy      request models moved to module scope
status      RESOLVED
```

### The first diagnosis was wrong, and the test that proved it

The entry above originally read `cause: request-body model declared in local function scope`.
That is not sufficient. The claim was checked by reintroducing the defect in a throwaway module,
and a locally scoped pydantic model **works**:

```
plain annotations + local model    openapi HTTP 200    POST 200
PEP563 (future) + local model      openapi HTTP 500    POST 422 loc ["query","b"]
```

`studio_session_api.py` carries `from __future__ import annotations`. PEP 563 makes every
annotation a string, FastAPI resolves it against the module globals, a class defined inside a
function is not there, and the parameter becomes
`Annotated[ForwardRef('ChangeBody'), Query(...)]` — a query field with an unresolvable type.

A guard that passes on the fixed code proves nothing. This one was run against the reintroduced
defect, and the first version of it was **blind** — it read `/openapi.json` and asserted on the
list of offending parameters, but under the real defect that request 500s before any list
exists. The guard was rewritten only after watching it fail.

### The symptom nobody noticed

Two things broke, and only the visible one was diagnosed. The 422 was on three endpoints; the
schema failure was on the entire application, because `get_openapi()` walks every route and
raises on the first it cannot resolve. `/docs` was down for the whole backend, and no test,
no browser check and no person noticed — the defect was found through the one endpoint someone
happened to be clicking.

A single unresolvable annotation is an app-wide outage of the schema surface. That is now `t1`.

### The fifth integrity layer

```
computation → artifact → transport → presentation → INTEGRATION
```

Tailwind was `presentation`: everything arrived at the browser correctly and the meaning was
destroyed visually. This is `integration`: the domain functions, the transport models and the
frontend were each correct in isolation, and the real framework binding between them was
something other than what all three assumed.

That is why nothing below the browser could see it. The tests called the Python functions
directly, so they exercised every layer except the one that was broken:

```
research_session tests   15/15   never touched the router
transport tests           9/9    called to_view() and change_and_run() directly
tsc / vite build          PASS   the frontend was correct
```

### The regression rule this produces

**An HTTP API acceptance test must travel the real ASGI routing layer, not call the handler or
domain function directly.** A test that imports the function proves the function; only a test
that issues a request proves the endpoint. `TestClient` is enough — this did not need a browser,
and the browser should not be the first thing that discovers a binding defect.

Implemented as `test_session_http.py` (13 checks), which drives the mounted app through
`TestClient`: schema generation, the binding malformation stated structurally, the four-action
golden fixture, and the 404/409 paths.

And a second rule, from how the diagnosis went wrong:

**A regression guard must be run against the reintroduced defect before it is trusted.** The
first version of `t1` passed on the broken code. A guard is a claim about a failure it has never
been shown to detect until someone shows it one.

---

## 2026-08-12 · THREE DEFECTS THE CONTRACTS CAUGHT, AND ONE GREEN TEST THAT PROVED NOTHING

Recorded together because they were found the same way: by a rule refusing something, rather
than by anyone looking.

### 1 · A state change with no event · caught by the hash chain

`start_exploration()` assigned `self.state = EXPLORE` and appended nothing. Invisible while the
ledger lived in RAM, and fatal the moment it went to disk: a restored session would read back
`NEW` and refuse every action a live one allowed.

The store refused the very first durable append with `ChainBreakError` — the prior hash
described a state no event had ever created. The fix is stronger than adding an event: the
transition now happens INSIDE `_append` (`_new_state=`), so a state change and its record cannot
come apart. Nothing else in the system had noticed; the chain did.

### 2 · A data footprint lost on restore · caught by failing closed

Sessions stamped their data window onto exposure events only. A session persisted before its
first exposure was restored with no window, and from then on every exposure it wrote had an
unknown footprint.

`EvidenceBoundary` reads unknown as contaminated, so instead of silently approving evidence it
could not vouch for, it answered `UNKNOWN — 39 exposures were recorded without a data
footprint`. That message is what found the bug. Had `UNKNOWN` been treated as clean for
convenience, the defect would have produced confirmatory verdicts on data nobody could account
for. The window is now carried on `SESSION_STARTED` and restored with the session.

### 3 · A test that passed for the wrong reason · caught by asserting the exact verdict

`t25` — the acceptance statement of the whole milestone — asserted

```
status in ("CONTAMINATED", "INVALID_BOUNDARY")
```

and passed on `INVALID_BOUNDARY`, because the validation window it picked overlapped the
declared development window. The laundering path it claimed to test was never exercised. Green,
and worth nothing.

That is the THIRD false PASS in this project:

```
string-vs-structure     a test read rendered text across a line wrap
OpenAPI guard           could not run when OpenAPI was the broken thing
this one                a disjunction satisfied by the uninteresting branch
```

All three share a shape: the assertion was wider than the claim. The rule that follows sits
beside the reproduction rule and is not the same one —

**An acceptance test asserts the exact verdict it is named after. A disjunction in an assertion
is a place where the test can pass without the system working.**

### The reproduction discipline, now applied

`test_evidence_boundary.py` carries two `REPRODUCTION` cases that fail on the defect before the
guard is trusted: a family-scoped exposure registry (which wrongly approves the laundering path)
and an `UNKNOWN` ranked below `CLEAN`. Each asserts that the broken version returns the wrong
answer, so a guard that stops discriminating fails loudly instead of going quiet.

```
DefectReproduction  →  must fail  →  Guard  →  Fix  →  must pass
```

---

## 2026-08-12 · THE SAME DEFECT SHAPE, THREE TIMES: STATE THAT DID NOT TRAVEL IN AN EVENT

```
type        SESSION_STATE_NOT_PERSISTED
occurrences 3
symptom     a restored session silently lost a property and behaved differently from a live one
detected_by the hash chain (1), a fail-closed verdict (2, 3)
remedy      anything a session needs after a restart travels in an event, or it does not survive
status      RESOLVED — and recorded as a shape, not as three bugs
```

1. `start_exploration()` set `self.state = EXPLORE` and appended nothing. A restored session read
   back `NEW`. Caught by `ChainBreakError` on the first durable append: the prior hash described
   a state no event had created. Fixed by moving the transition inside `_append`, so a state
   change and its record cannot come apart.

2. The data window was stamped only on exposure events. A session restored before its first
   exposure lost it and wrote footprint-less exposures from then on. Caught by `UNKNOWN`
   reporting "39 exposures with no data footprint" instead of approving evidence it could not
   vouch for.

3. `access_spec` was set on the session object after construction and never written to an event.
   After a restart the access layer was never constructed, so no footprint was recorded at all —
   and the contamination check that should have said CONTAMINATED said UNKNOWN. Same cause, same
   detector, one milestone later.

The third occurrence is the reason this is recorded as a shape. Two of the three were found by a
contract refusing something rather than by anyone looking, which is the argument for fail-closed
in one line: the weakest bookkeeping produced the weakest verdict, and the weakest verdict was
loud.

**Rule.** A session property that survives a restart must be written into an event at the moment
it is set. Assigning it to the object is not persistence, and the gap is invisible until the
process dies.

---

## 2026-08-12 · TWO ASSERTIONS THAT WERE COMMITMENTS

Recorded because both were caller-supplied, both decided a verdict, and neither looked wrong.

```
data_available_at_registration    decided FORWARD, the strongest verdict in the system
DEV_WINDOW / declared window      decided CONTAMINATED vs CLEAN
```

The first arrived in the request body. A caller could state any cutoff and certify a historical
window as forward validation — and the earlier `t9` documented this as a known gap rather than
fixing it, which is worth noting: a test that records a hole keeps it visible and does not close
it. It is now derived by the server from the source itself, and a source that cannot state its
cutoff blocks the freeze rather than falling back.

The second was subtler because a declaration feels like data. Declare 2024–2025, let a helper
read March 2026, validate from January 2026: CLEAN on the declaration, CONTAMINATED on the
truth. The fix is the same distinction the search side already had — `k_declared` against
`k_actual` — so the evidence side now carries `DataAccessSpec` against `ExposureFootprint`, and
the actual one governs.

**Rule.** If a field decides a verdict, ask who supplies it. A value asserted by the party the
verdict is about is a commitment to be recorded and checked, never an input to be trusted.

---

## 2026-08-12 · THE GUARD FIRED ON THE ACCOUNTING ITSELF

```
type        GATEWAY_GUARD_CAUGHT_ITS_OWN_MACHINERY
symptom     SourceUnavailableError: source 'bars_1d' could not be read —
            duckdb.connect was called while an execution is open
cause       the cutoff provider reads the same protected database
remedy      a shared `internal()` marker in the LOWER module, used by both
status      RESOLVED — and kept as evidence
```

The first real execution through the gateway failed because establishing the source cutoff
opens the very database the guard defends. The instinct is to read this as an over-broad guard
needing an exception. It is the opposite: a barrier that lets its own author's infrastructure
through by accident is a barrier with a shape, and a shape is something to route around. This
one had no shape — it stopped everything, including the code that measures.

The marker lives in `data_access`, the lower module, rather than in `data_gateway`. Putting it
in the gateway would have made `data_access` import the gateway to ask permission, and a cycle
between the thing being measured and the thing measuring it is its own kind of defect.

---

## 2026-08-12 · WHAT "CANNOT BYPASS" ACTUALLY MEANS HERE

Two words are kept apart deliberately, and every receipt carries which one applies:

```
ENFORCED_IN_PROCESS   while an execution is open, the direct constructors into protected
                      sources raise. Research code in this process has no other route.
                      Someone who removes the guard obviously does — same process.

ISOLATED              a separate data service; the research worker holds no OS-level access
                      to the files at all. Then "every read is observable" is a property of
                      the deployment, not of a patched function.
```

This milestone implements the first. `test_research_path_isolation.py` adds a static scan that
does not depend on execution timing, and its own docstring states the limit: it covers a
DECLARED list of modules, so a new research module has to be added to be covered. That is a
maintenance weakness, recorded rather than hidden behind a green tick.

### The distinction the milestone is actually about

```
footprints   what was read
receipt      proof that the reads were completely registered
```

Fail-closed on a MISSING footprint defends against obvious incompleteness. It cannot see the
dangerous case:

```
recorded A · recorded B · unrecorded C
```

Nothing is absent, so nothing looks wrong, and the verdict is CLEAN while C has already read the
validation window. Hence `AccessCompleteness` as a separate axis that GATES contamination rather
than being weighed against it, and hence `read_count=0, complete=true` being a positive statement
distinct from "no footprints were recorded".

Both guards were shown their defects before being trusted: dropping the completeness gate
certifies the bypass as FORWARD, and dropping lineage propagation leaves the raw window clean
after a derived artifact was read.
