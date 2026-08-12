# analytic-studio-mandatory-data-gateway-v1

```
semantic-foundation-v1     what a number means
semantic-ui-v1             the meaning survives transport and UI
research-session-v1        what an action costs statistically
combolab-explore-v1        a UI action actually pays that cost
evidence-boundary-v1       which data took part in forming the claim
mandatory-data-gateway-v1  whether data can be seen outside the accounting at all
```

## The case the previous milestone could not see

Fail-closed on a missing footprint catches obvious incompleteness. The dangerous case is not
obvious:

```
legal read A   → footprint recorded
legal read B   → footprint recorded
raw read C     → the access layer never saw it
```

Nothing is absent. The ledger looks like a finished history of A and B, and the verdict comes
back CLEAN while C has already read the validation window. "A bypass would give UNKNOWN" was
only ever true when the system knew a bypass had happened.

## Two facts, kept apart

```
footprints   what was read
receipt      proof that the reads were completely registered
```

Without the second, `0 footprints` is ambiguous — an execution that honestly read nothing looks
exactly like one whose instrumentation never fired. With it, `read_count=0, complete=true` is a
positive statement.

```
AccessCompleteness   COMPLETE · UNKNOWN · VIOLATED

COMPLETE + no overlap   → CLEAN / FORWARD possible
UNKNOWN                 → CLEAN / FORWARD forbidden
VIOLATED                → integrity invalid
```

`ConfirmatoryEligible ⇒ AccessCompleteness = COMPLETE`. It gates contamination rather than being
weighed against it.

## The third pair in the family

```
SEARCH      k_declared          k_actual
EVIDENCE    window_declared     footprint_actual
ACCESS      reads_expected      reads_observed / completeness_attested
```

## What research code holds

Not a path, not a connection — a `DataAccessCapability` and a `DatasetHandle`. A capability that
contained a path would be a suggestion.

```
ctx.data.open("bars_1d").read(start, end)      not      duckdb.connect(path)
```

Derived artifacts carry lineage: reading `opportunities.parquet` for 2026 records a footprint
for `bars_1d` 2026 as well, because that is what it is, one materialisation later.

## Honest naming of the guarantee

```
ENFORCED_IN_PROCESS   implemented here. Direct constructors raise while an execution is open.
ISOLATED              a separate data service; not implemented, and not claimed.
```

Every receipt carries `guarantee`, and a test asserts no module claims the stronger word.

## Adversarial suite

```
A  gateway read           footprint + COMPLETE receipt
B  zero read              COMPLETE, read_count=0 — not UNKNOWN
C  direct duckdb          DirectDataAccessError · VIOLATED
D  direct parquet         blocked · VIOLATED
E  partial bypass         VIOLATED — not CLEAN on A and B          ← acceptance
F  unregistered source    UnregisteredSourceAccessError
F2 unauthorised source    CapabilityError
G  derived artifact       lineage propagated to bars_1d
H  restart / double close  one receipt, same hash
I  receipt missing        UNKNOWN — "but we have several footprints" refused
J  guarantee naming       ENFORCED_IN_PROCESS, never ISOLATED
K  guard scope            outside an execution the application reads normally
```

Three reproductions fail on the defect first: completeness ignored certifies the bypass as
FORWARD, lineage ignored leaves the raw window clean, and the static scan trips on a direct read.

## Live

```
change through the gateway            0.06s
EXECUTION_STARTED×1 · DATA_ACCESSED×1 · EXECUTION_RECEIPT×1
validate 2026-09-01..2026-12-31       FORWARD · eligible=True · access=COMPLETE
```

## Suites

```
research_session       23/23      session_http           34/34
session_transport       9/9       research_durability    13/13
evidence_boundary      19/19      data_gateway           14/14
research_path_isolation 5/5       semantics_api          12/12
```

## Still open, named

- The static scan covers a DECLARED module list. A new research module must be added to it.
- The runtime guard patches `duckdb.connect` and `pandas.read_parquet`. A read through another
  library, or a subprocess, is not covered.
- Under ISOLATED both of the above stop mattering, because the worker would hold no path.
