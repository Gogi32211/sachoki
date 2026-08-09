# The guards — what each one is, and the failure that created it

Every guard below refuses rather than warns. The reason is empirical: in each case the analyst
**already knew the rule**, had it written down, and broke it anyway. A rule that depends on
being remembered at the moment of temptation is not a control.

---

## 1. Matched baseline — `Study.baseline()` / `Study.cell()`

**Refuses when:** the row universe changed between `baseline()` and `cell()`.

**The failure.** A new dataset covered 2024-02 → 2026-08. Its cells were compared against a
baseline computed over 2021-2026. Because the shorter window happened to be a rising market,
the baseline was +0.09 there but +1.43 inside the window — so every cell arrived with **+1.34
of unearned lift**. Several cells looked like discoveries. Recomputing the baseline on the
same window turned the best of them from "+1.08 over baseline" into "+0.25", i.e. nothing.

**Why it is so productive a source of error:** the mistake feels like diligence. You restrict
the data honestly because the source only covers a period — and the restriction itself
becomes the finding.

**The rule:** the baseline is not a constant. It is a property of the rows you are drawing
from. When the rows change, it changes.

---

## 2. Population context before interpretation — `Study.describe()`

**Refuses when:** `cell(..., requires=[col])` thresholds a column never passed to `describe`.

**The failure.** In a case study, down-bars closed at 46% of their range and up-bars at 82%.
This was reported as an absorption signature — "sellers cannot close it on the low". Measured
across the whole population, the median of that same statistic was **+38**, and the observed
value was **+36** — *below average*. The "signature" was the ordinary behaviour of every
instrument, because on an up bar the close is mechanically nearer the high.

**The rule:** a raw level is not evidence. Its percentile is. Any number described as high,
low, unusual or extreme must be accompanied by where it sits in its own population.

---

## 3. Controls generated from the hypothesis — `Study.controls()`

**Refuses nothing — it removes the need to remember.**

**The failure.** An ordered chain A → B → C inside a window was tested with two quality
filters and produced a strong result. The missing control was "C with the same two filters,
no chain". It gave **87% of the full result** — and a *random* row with just those two filters
gave **86%**. The chain contributed +0.55 of +3.94, at the cost of shrinking the sample 17×.
Six earlier patterns had been deleted for exactly this reason; two of their gates turned out
to run backwards once tested against their own parts.

**The rule:** the competitor of a compound hypothesis is not the naive baseline. It is
**the same hypothesis minus one component** — and especially *the filters without the pattern*.

---

## 4. Pre-registered trial budget — `Study(n_trials=...)`

**Refuses when:** more cells are scored than were registered.

**Why:** a multiplicity correction computed after seeing how many things you tried is not a
correction, it is a rationalisation. The number of trials must be fixed while it can still
hurt you.

**Corollary:** when a study legitimately grows, re-register with the true total. Every
deflated statistic then weakens — correctly.

---

## 5. Purged / embargoed splits — `purged_splits()`

**Refuses:** plain k-fold is simply not provided.

**Why:** when a row's outcome spans H periods forward, rows within H of a fold boundary share
information across it. A chronological split is necessary but *not sufficient* — the overlap
must be purged. With long horizons (60-90 periods) this is not a rounding error; the
overlapping region can be a large fraction of the data.

---

## 6. Intervals on every estimate — `bootstrap_ci()`

**Why:** a median with n=97 and a median with n=97,000 are printed identically and mean
entirely different things. The interval is what stops a thin cell being read as a finding.

---

## 7. Declare the lag of every feature

**Not automatable — it must be a discipline.**

**The failure it prevents:** a regulatory dataset is stamped with a *settlement date*, but is
published ~8 business days later. Joining on settlement date lets each row see a figure that
was not public for another week and a half. The study was built to join on
`known_from = settlement + 12 days` from the start; joining on the natural-looking date field
would have produced a strong, entirely false result.

**The rule:** every feature answers "when did this become knowable?", not "when did it
happen?". Anything published with delay, revised after the fact, or derived from a
confirmation that arrives later (pivot labels, cluster assignments, survivor lists) is a
lookahead channel.

---

## 8. No silent truncation

**Why:** a study that quietly drops the bottom decile, keeps only the top-N, or skips units
with too little data reads afterwards as if it covered everything. Print what was dropped and
why, next to the result — not in a footnote.

---

## 9. Plateau, not peak

**Why:** with ~20 parameter variants on a few hundred observations, *something* will look
best. The only interpretable result is a **neighbourhood** of parameters that agree. A single
spiking cell surrounded by disagreeing neighbours is noise, and should be reported as noise
even when it is the largest number in the table.

---

## 10. The verdict cannot overstate — `Study.verdict()`

**Refuses:** returning BUILD unless L1, L2 and L3 all pass, and always names the deciding gate.

**Why:** the last step of a study is where honest work most often becomes a press release.
Making the summary a computed object rather than a sentence removes the discretion at exactly
the point where enthusiasm peaks.
