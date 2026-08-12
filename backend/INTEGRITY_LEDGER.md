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
