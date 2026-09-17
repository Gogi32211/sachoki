# LIQUID_CONCURRENCE_V1

Pre-registered 2026-09-18 (`0af6c02`, corrected `164fe2c`) **before any outcome in the registered
window was read**. Read once on 2026 H1, 2026-09-18. Nothing above the results line was changed.

## VERDICT — **NOT CONFIRMED**

| | |
|---|---|
| **relative criterion** — `median(diff_d) > 0`, CI excluding 0 | ❌ **−0.166 pp**, CI [−0.946, +0.244] **includes 0** |
| **absolute directional** — `P(RET20 > 0 \| CROSS) > 50 %` | ✅ **56.2 %** |

The frozen rule reads *"NOT CONFIRMED — primary ≤ 0"*. `median(diff_d) = −0.166 ≤ 0`. **NOT
CONFIRMED.** Per the pre-registration, **the reserved 2026-07-01+ window was NOT opened**, and the
secondary segment cannot substitute.

⭐ **The two endpoints disagree, and that is the whole point of having registered both.** Price *did*
rise after a concurrence — 20-day median **+1.44 %**, **56.2 %** positive. It just did not rise more
than the day's ordinary eligible liquid name. The absolute move is the market of 2026 H1; the signal
added nothing to it.

## Results — 2026 H1, primary segment `close ≥ $21`, 6,940 events (0 without a canonical price)

### Endpoint 1 — descriptive (no controls, no matching, no market adjustment)

| Horizon | N resolved | mean | median | % > 0 | P25 | P75 |
|---|---:|---:|---:|---:|---:|---:|
| 1d | 6,940 | −0.02 % | −0.00 % | 49.6 % | −1.39 % | +1.30 % |
| 3d | 6,938 | −0.10 % | −0.11 % | 48.6 % | −2.70 % | +2.40 % |
| 5d | 6,938 | +0.26 % | +0.13 % | 51.1 % | −3.06 % | +3.41 % |
| 10d | 6,937 | +0.52 % | +0.51 % | 53.2 % | −4.36 % | +5.15 % |
| **20d** | 6,932 | **+1.56 %** | **+1.44 %** | **56.2 %** | −5.51 % | +8.32 % |

`open(t+1) → close(t+20)`: mean +1.61 %, median +1.60 %, 56.4 % positive — so nothing here depends
on a same-close fill. Note that **mean ≈ median** at every horizon, unlike the 2021–2025 read where
the mean was fourteen times the median: this is a broad shift, not a tail.

### Endpoint 2 — relative (the registered test)

| | |
|---|---:|
| sessions used | 123 |
| **median(diff_d)** | **−0.166 pp** |
| mean(diff_d) | −0.328 pp |
| **date-clustered 95 % CI** | **[−0.946, +0.244] pp — includes 0** |
| days with diff_d > 0 | 46.3 % |
| minus 3 largest movers | −0.134 pp, CI [−0.471, +0.468] — includes 0 |

### Secondary — `russell2k · $2-21 · ≥$10M`, 820 events *(cannot substitute for the primary)*

Descriptive 20d: mean +0.07 %, median −0.06 %, 49.4 % positive — flat. Relative: median(diff_d)
+0.951 pp, CI **[−1.167, +2.028] includes 0**, 53.4 % of days positive, minus movers +0.714 pp, CI
includes 0. Underpowered by design at 118 sessions and, as registered, **not a verdict**.

## What this settles

## Why this existed

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
