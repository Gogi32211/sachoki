# LIQUID_CONCURRENCE_V1 — PRE-REGISTRATION

**Written 2026-09-18, before any outcome in the registered window has been read.**
No result appears in this document. Nothing here may be changed once the first outcome is read.

## Why this exists

`CROSS_STAR_CONCURRENCE_V1` (`1118aa2`) closed with NO INCREMENTAL CONFIRMATION: the TOP×BOTTOM
composite is *worse* than TOP-only. But it also measured the **raw** post-signal response and, on a
post-hoc segment sweep, found that response is not uniform:

| segment (2021–2025, descriptive) | 20d median | % up | MINE | VERIFY |
|---|---:|---:|---:|---:|
| all crosses | +0.10 % | 50.3 % | −0.53 % | +0.75 % |
| $ volume < $1M | −0.74 % | 47.6 % | −1.36 % | +0.52 % |
| **$ volume ≥ $100M** | **+0.42 %** | **51.9 %** | **+0.24 %** | +0.57 % |
| russell2k · $2-21 · ≥$10M | +0.69 % | 52.6 % | +0.33 % | +1.02 % |

⚠️ **Search burden, stated plainly: roughly fifteen segments were inspected on data whose outcomes
had already been read.** At these sample sizes a best-of-fifteen cell near +0.5 % is exactly what
noise produces — [[feedback-placebo-and-cell-size]] measured size-matched placebos spanning
−0.07…+0.44 pp at 18k trades. **Nothing above is evidence.** It is a hypothesis, and 2021–2025 can
no longer test it.

## Hypothesis

> Among same-day TOP×BOTTOM concurrences, does the **liquid** subset outperform a same-day,
> same-bucket baseline of eligible names over the following 20 trading days?

## Two endpoints — kept separate, never conflated

They answer different questions and both are reported. Neither substitutes for the other.

### ENDPOINT 1 — DESCRIPTIVE (the original question)

> *"After concurrence, did price usually rise or fall, and by how much?"*

Among TOP×BOTTOM events in the frozen primary segment, report **RET1 · RET3 · RET5 · RET10 ·
RET20**, and for each: **N · mean · median · % > 0 · P25 · P75**.

**No controls. No matching. No market adjustment.** `RET_N = Close[t+N]/Close[t] − 1`, plus the
`open(t+1) → close(t+N)` variant reported beside it. This endpoint is descriptive: it states what
happened, and it cannot by itself separate the signal from the market.

### ENDPOINT 2 — RELATIVE (the pre-registered test)

> *"Did a concurrence beat the day's ordinary name in the same segment?"*

This is the one the decision rule is written against.

## Primary — the relative test, frozen

**Segment (PRIMARY): `close ≥ $21`.** Chosen over the narrower russell2k cell deliberately: it has
prior support in the book's own laws ([[feedback-price-bucket-always]],
[[project-fib-price-zones]] — "quality $21-89, <$8 = lottery"), so it is not purely a post-hoc cut,
and it carries **9,682** registered-window events against the narrow cell's 1,132.

**Statistic (PRIMARY).** Day-neutral by construction, so a bull market cannot manufacture it:

```
For each session d:
    diff_d = median(RET20 of cross rows in the segment on d)
           − median(RET20 of ALL eligible non-cross rows in the segment on d)
Primary = median over days of diff_d, with a DATE-CLUSTERED bootstrap 95 % CI.
```

`RET20 = Close[t+20] / Close[t] − 1`, trading-session offsets, canonical 1D prices, unresolved
horizons NaN and never carried forward. Days with < 20 eligible rows in the segment are dropped.
This is **not** the incremental-vs-TOP-only comparison already settled — the comparator is the day's
typical eligible name, not the other signal family.

**There is no matched-control `k` in this study.** The comparator is the **entire** same-day
eligible non-cross pool inside the segment — every such row that day, not a sampled or nearest-matched
subset. (The `k = 5` of `CROSS_STAR_CONCURRENCE_V1` was a matching parameter and has no counterpart
here; the letter is not reused for that meaning.)

## Registered window — and what it costs

```
PRIMARY READ    2026-01-01 → 2026-06-30      (2026 H1)
RESERVED        2026-07-01 onward            — stays SEALED for confirmation
```

2026 has never been opened by any study. Reading H1 **spends it for this question and it cannot be
reused.** H2 and everything after remain untouched, so a PASS can be confirmed once on data this
document has also never seen. Canonical prices end 2026-09-03, so a 20-day horizon in H1 resolves
fully.

Event counts, feature-side only, no outcome touched: 2026 H1 carries 11,182 crosses overall, **6,940
in the `≥ $21` primary segment**, 820 in the secondary cell.

## Secondary — one, diagnostic

**`russell2k · $2 ≤ close < $21 · $ volume ≥ $10M`** — the best-of-fifteen cell, registered so it
cannot be promoted afterwards. Same statistic, same window. **It stays secondary whatever it shows**,
and with 820 events it is underpowered by design; it is registered for honesty, not for a verdict.

**Multiplicity budget: 2 registered claims** (one primary segment, one secondary). This is a count of
hypotheses, not an estimator parameter — nothing in the statistic depends on it.

## Decision rule — frozen

**LIQUID CONCURRENCE CONFIRMED** requires *all* of:
1. **relative criterion** — primary day-median `diff_d` **> 0** with the date-clustered 95 % CI
   **excluding 0**;
2. **absolute directional criterion** — `P(RET20 > 0 | CROSS)` **> 50 %** in the segment. This is
   Endpoint 1's own statistic and is deliberately a *different kind* of claim from `diff_d`: one says
   price rose, the other says it rose more than the day's ordinary name. Both are required; they are
   never merged;
3. survives removing the **3 largest absolute-mover dates**;
4. not driven by concentration — no single ticker > 2 % and no single day > 3 % of events.

**DIRECTIONAL, NOT CONFIRMED** — (1) positive but the CI includes 0.
**NOT CONFIRMED** — primary ≤ 0.
**DATA-QUALITY-BOUNDED** — the result depends materially on censored, illiquid or corporate-action rows.

## Explicitly not done

No production change of any kind. No new signal, no veto, no threshold search, no re-cut of the
segment, no third window. If the primary fails, **the reserved window is not opened to look for a
better answer** — that would convert the reserve into a second search. The narrow cell may not be
substituted for the primary.

Code will be `backend/liquid_concurrence_v1.py`, reusing the sealed feature table and the canonical
price authority; no new outcome implementation.
