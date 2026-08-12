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
