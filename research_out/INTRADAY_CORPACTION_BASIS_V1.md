# INTRADAY_CORPACTION_BASIS_V1

**Audit only. No outcome opened, no production change, no remediation attempted.** Opened
2026-09-22 after the acceptance run's parity gate failed on a cause that was neither the writer nor
the warm-up.

A **seam** here is a bar-to-bar close ratio outside `[0.625, 1.6]`. No equity moves that far between
two adjacent *intraday* bars, so on 1H/4H/15m such a step is a change of corporate-action **basis**,
not a price move.

## §1 · Our footprint vs the pre-existing condition

These are two different faults with two different owners and they must not be pooled.

| | 1H | 4H | 15m *(control)* |
|---|---|---|---|
| tickers carrying ≥1 seam | **490 / 3,203 (15.3%)** | **511 / 3,203 (16.0%)** | 439 / 3,203 (13.7%) |
| total seam steps | 1,066 | 1,123 | 901 |
| **repair footprint** (2026-06-29 boundary) | **19 tickers** | **26 tickers** | — |
| repair footprint (window-end boundary) | 1 | 2 | — |
| **pre-existing** | 483 tickers · 1,046 steps | 498 tickers · 1,094 steps | 438 · 898 |

⭐ **The 15m store is the control that separates them.** It shares the universe, the vendor and the
`adjusted=true` contract, but it **never underwent the 2026 backfill** — the replace covered 1H and
4H only. So:

```
tickers with a 2026-06-29 seam:   1h 19 · 4h 27 · 15m 1
```

The 06-29 cluster is **absent from the untouched store**. Of the 4H tickers, only **HON** also shows
it in 15m — a real corporate action that happened to fall on that date. Every other one is our
boundary: the replaced window `2026-06-29 … 2026-09-02` was re-fetched with `adjusted=true` and
therefore carries **today's** basis, while the bars on either side carry the basis of their own
write date.

**Repair footprint: 19 tickers on 1H, 26 on 4H** — ALP, APH, BURU, BYND, CLBK, CXAI, FFAI, GORO,
GOSS, GPUS, HUBC, IESC, MNST, MVIS, NFE, NXXT, REAX, SEGG, SFBS (+ ABTC, CRWD, EVMN, SDOT, SNEX,
SVC, WLFC on 4H). Under 1% of the universe.

The same control says the opposite about the rest: **71% of 1H seams also appear in 15m** at the
same (ticker, date). A repair-specific cause cannot produce that; a store-architecture cause
predicts exactly it.

**The pre-existing condition is not a bug in the repair, it is a property of the design.** Any
incrementally written store using `adjusted=true` freezes every bar at the basis current on its own
write date, so each corporate action leaves a step wherever the writer happened to be. It predates
the backfill and would exist without it. What the backfill did was **add ~20–26 tickers to a
population of ~490–510 and make the seam visible** by putting one clean window next to the rest.

## §2 · Mechanism, and a worked case

`NXXT`, 4H, straight out of the store:

```
2026-09-01 17:30  close  2.20  ->  2026-09-02 13:30  close  0.21   ÷10.5   ← replace-window end
2026-09-04 17:30  close  0.22  ->  2026-09-08 13:30  close  2.10   ×9.5    ← the real reverse split
```

**A price that falls ×10 and returns ×10 four days later is not a price path.** The first step is a
write-vintage boundary, the second is the corporate action itself landing in a differently-written
region. At the 2026-06-29 boundary, **16 of 27 ratios sit within 2% of an integer** (2:1, 3:1, 4:1,
10:1 …) — the split-factor signature; the rest carry a genuine price move layered on top of the
factor, which is what a boundary between two real bars should look like.

## §3 · Blast radius

| store | rows on a seam-carrying ticker | share |
|---|---|---|
| 1H | 3,714,602 of 25,557,033 | **14.5%** |
| 4H | 1,104,513 of 7,351,669 | **15.0%** |
| 15m | 11,844,168 of 92,382,345 | **12.8%** |

This is an **upper bound on exposure, not a count of wrong values.** A bar is only wrong if something
recomputes across the seam. Two regimes:

- **Values written by the enricher are fine.** They are computed inside one fetched frame, one
  vendor call, one basis. That is exactly why the rebuilt writer gate (`6f9abf7`) now measures
  against the fresh frame — and why it reports 0.000 where the full-history reference reported 6.5.
- **Anything recomputing a long window from the store is not.** `mtf_rev_build.wilder_rsi14` over
  full history is the clearest case, and it is what produced the 2026-09-22 false alarm. Any
  research that reads months of intraday `close` on one of these ~490–510 tickers inherits it.

## §4 · What this audit does NOT settle

⚠️ **The 1D store's status is UNRESOLVED, and my earlier statement that it is "unaffected (different
writer)" was an assumption stated as a fact. It is retracted.** Measured at the same threshold,
`studio_analytics` shows 5,235 steps on **1,591 of 5,733 tickers (27.8%)** — *more* than the intraday
stores. But that number is **not evidence of seams**: on daily bars a ±60% move is an ordinary
event (biotech readouts, sub-$1 names), so `[0.625, 1.6]` is simply not a seam detector at that
resolution. The 1D store may be clean, may be worse, or may be identical; this section does not know,
and a proper test needs a discriminator that separates a real daily gap from a basis step.

Also open, and deliberately not attempted here: whether to re-fetch full history for the affected
tickers, whether to store an unadjusted series plus a split table instead of frozen adjusted prices,
and whether the ~20–26 boundary tickers should be repaired on their own. None of that is a research
question, and none of it was authorised.

## Status

Finding recorded. **No outcome opened. Nothing changed in production.** The writer's acceptance gate
no longer depends on a full-history reference (`6f9abf7`), so this condition can no longer make a
healthy nightly run report NOT PASS — which was the only part of it that was blocking anything.
