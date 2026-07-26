# Pre-breakout discriminator — scale validation

_Source: studio_analytics.duckdb · window=15 bars (causal) · horizon=10d · episode de-dup=5 bars · candidates = bars with a bullish T-signal._

_Total de-duped episodes (all universes, finite forwards): **1,119,221**_

Metrics are positive-skew aware: P(+X%) is on max-10d-high gain (mfe_10d); RR = median(MFE)/median(|MAE|); win% on close fwd_10d. Median alone understates these lottery setups.

---

# EXECUTIVE SUMMARY & VERDICT

## TL;DR
The composite 0–4 score **is a real, large, OOS-stable discriminator of EXPLOSIVE-move probability** P(+50% high in 10d) in micro/small-caps (russell2k, nasdaq) — **monotonic 0→3, n≫30 at every bucket through 3, ~4–5× lift HIGH-vs-LOW, and virtually unchanged when the design tickers PAVS/WNW/GLOO are held out.** But it is **NOT a directional / win-rate edge**: median return, mean, win% and RR all *decline* as the score rises. These are lose-small-often / hit-the-tail-rarely setups. It belongs as a **scanner "explosive-candidate" flag, not as a weight folded into the win-rate-style turbo/ultra score.**

The case-study hypothesis ("**spring + range_exp** carry the edge") is **only half right** — see §Per-feature.

## 1. Does the composite separate winners? — YES (P(+50%), monotonic, OOS-confirmed)

| set | LOW(<2) P(+50%) | HIGH(≥2) P(+50%) | lift | monotonic 0→3 | score-4 |
|---|---|---|---|---|---|
| russell2k | 1.8% (n577k) | **9.2%** (n21.3k) | **5.1×** | 1.2→3.4→8.7→16.6 ✓ | n=10 ✗ (can't conclude) |
| nasdaq | 2.6% (n396k) | **10.4%** (n20.2k) | **4.0×** | 1.8→4.4→9.9→16.2 ✓ | n=13 ✗ |
| OOS (ex-3) | 1.9% (n1.08M) | **9.6%** (n42.4k) | **5.1×** | 1.3→3.6→9.1→16.3 ✓ | n=23 ✗ |
| sp500 | 0.1% | 0.5% | 5× but absolute ~0 | flat/noise | n=0 |

P(+25%) and P(+100%) rise the same way (russell2k P(+100%): 0.3→1.1→3.5→6.9). **sp500 large-caps don't make +50% moves in 10 days at all**, so the discriminator has nothing to separate there — exclude that universe.

## 2. The catch — it is the WRONG axis for win-rate

Every "central tendency" metric goes the *opposite* way (OOS):

| bucket | median fwd% | mean fwd% | win% | RR |
|---|---|---|---|---|
| LOW (<2) | −0.27 | +0.14 | **47.9** | 0.95 |
| HIGH (≥2) | −2.45 | −0.60 | **41.6** | 0.83 |

A HIGH-score bar is *more* likely to be red at +10d and has a *worse* median MFE/MAE. The score buys you a fatter right tail (the +50/+100% lottery) at the cost of a lower hit-rate and negative median. **This is exactly why it must not be added to a win-rate-maximising scorer** — it would degrade it. It is a tail-probability axis, tradeable only with small size + run-the-winners + structured stops.

## 3. Per-feature individual lift — the hypothesis is HALF wrong (OOS)

| feature | n (ON) | P(+50%) ON | OFF | **Δ P(+50%)** | median fwd ON | win% ON | verdict |
|---|---|---|---|---|---|---|---|
| **range_exp** | 1,152 | 20.4% | 2.2% | **+18.2** | −11.5 | 28.9 | **strongest per-flag, but ultra-rare (0.1%)** |
| **acc_tr** | 54,528 | 10.7% | 1.8% | **+8.9** | −3.44 | 38.8 | **biggest *reliable* contributor (large n)** |
| spring_reclaim | 142,898 | 4.5% | 1.9% | **+2.6** | −0.49 | 47.4 | weak alone — over-credited by the case study |
| bull_vb | 181,143 | 3.7% | 1.9% | **+1.8** | −0.61 | 46.0 | weakest |

**Finding vs the PAVS/WNW/GLOO case study:**
- **range_exp** *does* carry a huge P(+50%) edge (+18pp) — but it fires on **~0.1% of candidates** (557 russell / 607 nasdaq). It is essentially "this name already printed a 150%-range demand bar," i.e. it flags *already-volatile* names. Real but too rare to be the backbone, and arguably momentum-continuation rather than truly *pre*-breakout.
- **spring_reclaim ALONE is weak (+2.6pp).** The case study mis-attributed the edge to spring because in those 3 charts spring happened to coincide with the explosion. At scale, spring is the second-weakest flag.
- **acc_tr is the real workhorse**: +8.9pp P(+50%) on n=54k — the only feature that is both strong *and* common. (It also tanks win% to 38.8% — it selects accumulation bases that either rip or keep bleeding.)
- So the edge lives in **acc_tr + range_exp + the composite stacking**, *not* in spring. The features are complementary (acc_tr = common base context; range_exp = rare high-octane confirm; spring/bull_vb = minor tilts), which is why the *sum* is cleanly monotonic even though two of four flags are individually weak.

## 4. VERDICT

**Promote to a SCANNER FEATURE — with stated scope and honest framing — yes; as a production *scoring-engine* weight — no.**

- ✅ The composite P(+50%) lift is **large (4–5×), monotonic 0→3, n≫30, and OOS-stable** (ex-PAVS/WNW/GLOO essentially identical). That clears the bar to ship as a **"explosive-move probability" scanner flag** for **russell2k / nasdaq only**, surfacing HIGH(≥2) candidates.
- ⚠️ Ship it **labelled as a tail-probability axis, not a win signal** — and keep it **separate from turbo_score/ultra_score** (it is anti-correlated with win-rate and would hurt those).
- ⚠️ **Score 4 stays a hypothesis** — n=10–23 everywhere, below the n≥30 floor; do not special-case it yet.
- ⚠️ **spring_reclaim is not the carrier of the edge** (the original case-study read). If trimming to a leaner flag, keep **acc_tr (backbone) + range_exp (rare high-conviction confirm)**; spring/bull_vb add little.
- ❌ **Do not apply to sp500** — large-caps don't produce the move the flag predicts.
- Suggested next step (not built here): test **acc_tr × range_exp** as a 2-flag gate and a HIGH(≥2)-only scanner list against the live Setups board, with explicit small-size / let-it-run position rules.

_Data caveats: (a) PAVS & GLOO appear in **both** nasdaq and russell2k in the store, so the in-sample case-study table double-lists them; the per-universe and OOS tables are unaffected (OOS excludes all 3 tickers entirely). (b) `range_exp ≥1.5` is an extreme threshold by design — it is rare and behaves like "already explosive." (c) Forward/excursion values were de-duplicated on (ticker,date) and filtered to finite; clipped to [−100,+500]% for means only. No production code was modified._

---


## russell2k  (n=598,682 episodes)

### Per-score bucket + HIGH vs LOW

| bucket | n | med fwd% | mean fwd% | P(+25%) | P(+50%) | P(+100%) | RR | win% |
|---|---|---|---|---|---|---|---|---|
| score 0 | 423639 | -0.17 | 0.12 | 5.0 | 1.2 | 0.3 | 0.97 | 48.4 |
| score 1 | 153694 | -0.45 | 0.32 | 10.5 | 3.4 | 1.1 | 0.95 | 47.0 |
| score 2 | 19841 | -1.91 | -0.28 | 20.8 | 8.7 | 3.5 | 0.85 | 42.7 |
| score 3 | 1498 | -6.77 | -2.04 | 32.6 | 16.6 | 6.9 | 0.81 | 35.2 |
| score 4 | 10 | -26.43 | -27.07 | 40.0 | 10.0 | 10.0 | 0.46 | 10.0 |
| | | | | | | | | |
| **LOW (<2)** | 577333 | -0.24 | 0.17 | 6.5 | 1.8 | 0.5 | 0.96 | 48.0 |
| **HIGH (>=2)** | 21349 | -2.18 | -0.41 | 21.6 | 9.2 | 3.7 | 0.84 | 42.2 |


### Per-feature individual lift

| feature | state | n | med fwd% | P(+50%) | P(+100%) | win% |
|---|---|---|---|---|---|---|
| acc_tr | ON | 26862 | -3.29 | 10.4 | 4.2 | 38.9 |
| acc_tr | OFF | 571820 | -0.2 | 1.6 | 0.4 | 48.2 |
| **acc_tr Δ** | ON−OFF | | -3.09 | 8.8 | 3.8 | -9.3 |
| bull_vb | ON | 95807 | -0.56 | 3.4 | 1.1 | 46.3 |
| bull_vb | OFF | 502875 | -0.22 | 1.8 | 0.5 | 48.1 |
| **bull_vb Δ** | ON−OFF | | -0.34 | 1.6 | 0.6 | -1.8 |
| spring_reclaim | ON | 74684 | -0.33 | 4.3 | 1.5 | 48.0 |
| spring_reclaim | OFF | 523998 | -0.27 | 1.7 | 0.5 | 47.8 |
| **spring_reclaim Δ** | ON−OFF | | -0.06 | 2.6 | 1.0 | 0.2 |
| range_exp | ON | 557 | -11.11 | 21.4 | 8.1 | 31.2 |
| range_exp | OFF | 598125 | -0.27 | 2.0 | 0.6 | 47.8 |
| **range_exp Δ** | ON−OFF | | -10.84 | 19.4 | 7.5 | -16.6 |


**P(+50%) monotonicity 0->4:** 0:1.2(n423639) -> 1:3.4(n153694) -> 2:8.7(n19841) -> 3:16.6(n1498) -> 4:10.0(n10)


## nasdaq  (n=415,692 episodes)

### Per-score bucket + HIGH vs LOW

| bucket | n | med fwd% | mean fwd% | P(+25%) | P(+50%) | P(+100%) | RR | win% |
|---|---|---|---|---|---|---|---|---|
| score 0 | 275427 | -0.44 | 0.0 | 7.0 | 1.8 | 0.5 | 0.93 | 46.8 |
| score 1 | 120090 | -0.86 | 0.13 | 13.1 | 4.4 | 1.4 | 0.92 | 45.2 |
| score 2 | 18507 | -2.78 | -0.69 | 23.0 | 9.9 | 3.9 | 0.83 | 40.9 |
| score 3 | 1655 | -7.6 | -2.98 | 32.4 | 16.2 | 6.9 | 0.77 | 33.7 |
| score 4 | 13 | -22.38 | -25.05 | 38.5 | 15.4 | 7.7 | 0.36 | 7.7 |
| | | | | | | | | |
| **LOW (<2)** | 395517 | -0.55 | 0.04 | 8.9 | 2.6 | 0.7 | 0.92 | 46.3 |
| **HIGH (>=2)** | 20175 | -3.13 | -0.9 | 23.8 | 10.4 | 4.2 | 0.82 | 40.3 |


### Per-feature individual lift

| feature | state | n | med fwd% | P(+50%) | P(+100%) | win% |
|---|---|---|---|---|---|---|
| acc_tr | ON | 27537 | -3.61 | 11.1 | 4.4 | 38.6 |
| acc_tr | OFF | 388155 | -0.51 | 2.4 | 0.7 | 46.5 |
| **acc_tr Δ** | ON−OFF | | -3.1 | 8.7 | 3.7 | -7.9 |
| bull_vb | ON | 75735 | -0.88 | 4.4 | 1.5 | 44.8 |
| bull_vb | OFF | 339957 | -0.56 | 2.7 | 0.8 | 46.3 |
| **bull_vb Δ** | ON−OFF | | -0.32 | 1.7 | 0.7 | -1.5 |
| spring_reclaim | ON | 58242 | -1.08 | 5.6 | 2.0 | 45.3 |
| spring_reclaim | OFF | 357450 | -0.56 | 2.5 | 0.7 | 46.1 |
| **spring_reclaim Δ** | ON−OFF | | -0.52 | 3.1 | 1.3 | -0.8 |
| range_exp | ON | 607 | -12.24 | 19.6 | 7.4 | 26.9 |
| range_exp | OFF | 415085 | -0.61 | 3.0 | 0.9 | 46.0 |
| **range_exp Δ** | ON−OFF | | -11.63 | 16.6 | 6.5 | -19.1 |


**P(+50%) monotonicity 0->4:** 0:1.8(n275427) -> 1:4.4(n120090) -> 2:9.9(n18507) -> 3:16.2(n1655) -> 4:15.4(n13)


## sp500  (n=104,847 episodes)

### Per-score bucket + HIGH vs LOW

| bucket | n | med fwd% | mean fwd% | P(+25%) | P(+50%) | P(+100%) | RR | win% |
|---|---|---|---|---|---|---|---|---|
| score 0 | 85826 | 0.32 | 0.32 | 1.1 | 0.1 | 0.0 | 1.06 | 52.5 |
| score 1 | 18059 | 0.52 | 0.62 | 2.0 | 0.3 | 0.0 | 1.12 | 53.9 |
| score 2 | 959 | 0.56 | 1.06 | 4.4 | 0.5 | 0.1 | 1.12 | 53.9 |
| score 3 | 3 | -7.25 | 5.04 | 33.3 | 0.0 | 0.0 | 0.23 | 33.3 |
| score 4 | 0 | — | — | — | — | — | — | — |
| | | | | | | | | |
| **LOW (<2)** | 103885 | 0.35 | 0.37 | 1.2 | 0.1 | 0.0 | 1.07 | 52.7 |
| **HIGH (>=2)** | 962 | 0.56 | 1.08 | 4.5 | 0.5 | 0.1 | 1.12 | 53.8 |


### Per-feature individual lift

| feature | state | n | med fwd% | P(+50%) | P(+100%) | win% |
|---|---|---|---|---|---|---|
| acc_tr | ON | 218 | 0.74 | 5.5 | 1.4 | 51.4 |
| acc_tr | OFF | 104629 | 0.36 | 0.1 | 0.0 | 52.7 |
| **acc_tr Δ** | ON−OFF | | 0.38 | 5.4 | 1.4 | -1.3 |
| bull_vb | ON | 9676 | 0.37 | 0.2 | 0.0 | 52.7 |
| bull_vb | OFF | 95171 | 0.35 | 0.1 | 0.0 | 52.7 |
| **bull_vb Δ** | ON−OFF | | 0.02 | 0.1 | 0.0 | 0.0 |
| spring_reclaim | ON | 10092 | 0.7 | 0.3 | 0.0 | 55.0 |
| spring_reclaim | OFF | 94755 | 0.32 | 0.1 | 0.0 | 52.5 |
| **spring_reclaim Δ** | ON−OFF | | 0.38 | 0.2 | 0.0 | 2.5 |
| range_exp | ON | 0 | — | — | — | — |
| range_exp | OFF | 104847 | 0.36 | 0.1 | 0.0 | 52.7 |


**P(+50%) monotonicity 0->4:** 0:0.1(n85826) -> 1:0.3(n18059) -> 2:0.5(n959) -> 3:0.0(n3) -> 4:n/a(n0)


## OUT-OF-SAMPLE — held out PAVS/WNW/GLOO (n=1,118,811)

Pooled across universes for the held-out check (separation should persist).


| bucket | n | med fwd% | mean fwd% | P(+25%) | P(+50%) | P(+100%) | RR | win% |
|---|---|---|---|---|---|---|---|---|
| score 0 | 784715 | -0.19 | 0.1 | 5.3 | 1.3 | 0.3 | 0.96 | 48.3 |
| score 1 | 291665 | -0.52 | 0.26 | 11.0 | 3.6 | 1.1 | 0.94 | 46.7 |
| score 2 | 39260 | -2.17 | -0.43 | 21.4 | 9.1 | 3.6 | 0.84 | 42.1 |
| score 3 | 3148 | -7.41 | -2.52 | 32.6 | 16.3 | 6.9 | 0.78 | 34.5 |
| score 4 | 23 | -25.26 | -25.93 | 39.1 | 13.0 | 8.7 | 0.36 | 8.7 |
| | | | | | | | | |
| **LOW (<2)** | 1076380 | -0.27 | 0.14 | 6.8 | 1.9 | 0.5 | 0.95 | 47.9 |
| **HIGH (>=2)** | 42431 | -2.45 | -0.6 | 22.3 | 9.6 | 3.9 | 0.83 | 41.6 |


### OOS per-feature lift

| feature | state | n | med fwd% | P(+50%) | P(+100%) | win% |
|---|---|---|---|---|---|---|
| acc_tr | ON | 54528 | -3.44 | 10.7 | 4.3 | 38.8 |
| acc_tr | OFF | 1064283 | -0.23 | 1.8 | 0.5 | 48.1 |
| **acc_tr Δ** | ON−OFF | | -3.21 | 8.9 | 3.8 | -9.3 |
| bull_vb | ON | 181143 | -0.61 | 3.7 | 1.2 | 46.0 |
| bull_vb | OFF | 937668 | -0.26 | 1.9 | 0.6 | 47.9 |
| **bull_vb Δ** | ON−OFF | | -0.35 | 1.8 | 0.6 | -1.9 |
| spring_reclaim | ON | 142898 | -0.49 | 4.5 | 1.6 | 47.4 |
| spring_reclaim | OFF | 975913 | -0.29 | 1.9 | 0.5 | 47.6 |
| **spring_reclaim Δ** | ON−OFF | | -0.2 | 2.6 | 1.1 | -0.2 |
| range_exp | ON | 1152 | -11.54 | 20.4 | 7.6 | 28.9 |
| range_exp | OFF | 1117659 | -0.31 | 2.2 | 0.7 | 47.6 |
| **range_exp Δ** | ON−OFF | | -11.23 | 18.2 | 6.9 | -18.7 |


**OOS P(+50%) monotonicity 0->4:** 0:1.3(n784715) -> 1:3.6(n291665) -> 2:9.1(n39260) -> 3:16.3(n3148) -> 4:13.0(n23)


## Case-study tickers (in-sample, sanity) — PAVS/WNW/GLOO (n=410)

| ticker | univ | date | score | acc | vb | spring | rng | fwd% | mfe% | mae% |
|---|---|---|---|---|---|---|---|---|---|---|
| GLOO | russell2k | 2025-12-12 | 0 | 0 | 0 | 0 | 0 | -16.8 | 1.4 | -22.3 |
| GLOO | nasdaq | 2025-12-12 | 0 | 0 | 0 | 0 | 0 | -16.8 | 1.4 | -22.3 |
| GLOO | russell2k | 2025-12-23 | 0 | 0 | 0 | 0 | 0 | 6.3 | 11.6 | -12.4 |
| GLOO | nasdaq | 2025-12-23 | 0 | 0 | 0 | 0 | 0 | 6.3 | 11.6 | -12.4 |
| GLOO | russell2k | 2026-01-07 | 1 | 0 | 0 | 1 | 0 | 3.8 | 13.8 | -6.6 |
| GLOO | nasdaq | 2026-01-07 | 1 | 0 | 0 | 1 | 0 | 3.8 | 13.8 | -6.6 |
| GLOO | russell2k | 2026-01-15 | 0 | 0 | 0 | 0 | 0 | -9.1 | 6.4 | -10.5 |
| GLOO | nasdaq | 2026-01-15 | 0 | 0 | 0 | 0 | 0 | -9.1 | 6.4 | -10.5 |
| GLOO | russell2k | 2026-01-29 | 0 | 0 | 0 | 0 | 0 | -9.2 | 4.1 | -21.3 |
| GLOO | nasdaq | 2026-01-29 | 0 | 0 | 0 | 0 | 0 | -9.2 | 4.1 | -21.3 |
| GLOO | russell2k | 2026-02-06 | 0 | 0 | 0 | 0 | 0 | 10.8 | 16.7 | -2.0 |
| GLOO | nasdaq | 2026-02-06 | 0 | 0 | 0 | 0 | 0 | 10.8 | 16.7 | -2.0 |
| GLOO | russell2k | 2026-02-17 | 0 | 0 | 0 | 0 | 0 | 7.1 | 29.9 | -3.3 |
| GLOO | nasdaq | 2026-02-17 | 0 | 0 | 0 | 0 | 0 | 7.1 | 29.9 | -3.3 |
| GLOO | russell2k | 2026-02-25 | 0 | 0 | 0 | 0 | 0 | -2.3 | 8.4 | -14.1 |
| GLOO | nasdaq | 2026-02-25 | 0 | 0 | 0 | 0 | 0 | -2.3 | 8.4 | -14.1 |
| GLOO | russell2k | 2026-03-06 | 0 | 0 | 0 | 0 | 0 | -9.7 | 9.9 | -10.8 |
| GLOO | nasdaq | 2026-03-06 | 0 | 0 | 0 | 0 | 0 | -9.7 | 9.9 | -10.8 |
| GLOO | russell2k | 2026-03-18 | 0 | 0 | 0 | 0 | 0 | -14.7 | 6.2 | -17.9 |
| GLOO | nasdaq | 2026-03-18 | 0 | 0 | 0 | 0 | 0 | -14.7 | 6.2 | -17.9 |
| GLOO | russell2k | 2026-03-26 | 1 | 0 | 0 | 1 | 0 | 1.2 | 16.7 | -9.2 |
| GLOO | nasdaq | 2026-03-26 | 1 | 0 | 0 | 1 | 0 | 1.2 | 16.7 | -9.2 |
| GLOO | russell2k | 2026-04-06 | 1 | 0 | 0 | 1 | 0 | 42.3 | 44.0 | -10.3 |
| GLOO | nasdaq | 2026-04-06 | 1 | 0 | 0 | 1 | 0 | 42.3 | 44.0 | -10.3 |
| GLOO | russell2k | 2026-04-16 | 0 | 0 | 0 | 0 | 0 | -14.1 | 9.3 | -18.6 |
| GLOO | nasdaq | 2026-04-16 | 0 | 0 | 0 | 0 | 0 | -14.1 | 9.3 | -18.6 |
| GLOO | russell2k | 2026-04-28 | 0 | 0 | 0 | 0 | 0 | -11.7 | 13.0 | -12.0 |
| GLOO | nasdaq | 2026-04-28 | 0 | 0 | 0 | 0 | 0 | -11.7 | 13.0 | -12.0 |
| GLOO | russell2k | 2026-05-06 | 0 | 0 | 0 | 0 | 0 | -25.2 | 2.4 | -29.3 |
| GLOO | nasdaq | 2026-05-06 | 0 | 0 | 0 | 0 | 0 | -25.2 | 2.4 | -29.3 |
| PAVS | russell2k | 2023-04-06 | 1 | 0 | 1 | 0 | 0 | -14.0 | 0.0 | -22.3 |
| PAVS | nasdaq | 2023-04-06 | 1 | 0 | 1 | 0 | 0 | -14.0 | 0.0 | -22.3 |
| PAVS | russell2k | 2023-04-17 | 1 | 0 | 1 | 0 | 0 | -14.7 | 6.2 | -38.8 |
| PAVS | nasdaq | 2023-04-17 | 1 | 0 | 1 | 0 | 0 | -14.7 | 6.2 | -38.8 |
| PAVS | russell2k | 2023-04-25 | 0 | 0 | 0 | 0 | 0 | -7.1 | 29.0 | -25.6 |
| PAVS | nasdaq | 2023-04-25 | 0 | 0 | 0 | 0 | 0 | -7.1 | 29.0 | -25.6 |
| PAVS | russell2k | 2023-05-04 | 1 | 0 | 0 | 1 | 0 | -24.9 | -0.3 | -32.6 |
| PAVS | nasdaq | 2023-05-04 | 1 | 0 | 0 | 1 | 0 | -24.9 | -0.3 | -32.6 |
| PAVS | russell2k | 2023-05-15 | 1 | 0 | 0 | 1 | 0 | -14.6 | -4.6 | -23.8 |
| PAVS | nasdaq | 2023-05-15 | 1 | 0 | 0 | 1 | 0 | -14.6 | -4.6 | -23.8 |
| PAVS | russell2k | 2023-05-24 | 1 | 0 | 0 | 1 | 0 | -14.6 | 1.4 | -19.8 |
| PAVS | nasdaq | 2023-05-24 | 1 | 0 | 0 | 1 | 0 | -14.6 | 1.4 | -19.8 |
| PAVS | russell2k | 2023-06-02 | 1 | 0 | 0 | 1 | 0 | -5.1 | 10.8 | -11.5 |
| PAVS | nasdaq | 2023-06-02 | 1 | 0 | 0 | 1 | 0 | -5.1 | 10.8 | -11.5 |
| PAVS | russell2k | 2023-06-16 | 1 | 0 | 0 | 1 | 0 | -8.8 | 1.6 | -17.9 |
| PAVS | nasdaq | 2023-06-16 | 1 | 0 | 0 | 1 | 0 | -8.8 | 1.6 | -17.9 |
| PAVS | russell2k | 2023-06-28 | 1 | 0 | 0 | 1 | 0 | -12.0 | -0.6 | -12.4 |
| PAVS | nasdaq | 2023-06-28 | 1 | 0 | 0 | 1 | 0 | -12.0 | -0.6 | -12.4 |
| PAVS | russell2k | 2023-07-10 | 1 | 0 | 0 | 1 | 0 | -10.7 | 1.9 | -22.6 |
| PAVS | nasdaq | 2023-07-10 | 1 | 0 | 0 | 1 | 0 | -10.7 | 1.9 | -22.6 |
| PAVS | russell2k | 2023-07-21 | 1 | 0 | 0 | 1 | 0 | -33.5 | 10.4 | -39.1 |
| PAVS | nasdaq | 2023-07-21 | 1 | 0 | 0 | 1 | 0 | -33.5 | 10.4 | -39.1 |
| PAVS | russell2k | 2023-08-07 | 1 | 0 | 0 | 1 | 0 | 127.7 | 178.3 | -10.9 |
| PAVS | nasdaq | 2023-08-07 | 1 | 0 | 0 | 1 | 0 | 127.7 | 178.3 | -10.9 |
| PAVS | russell2k | 2023-08-15 | 0 | 0 | 0 | 0 | 0 | -3.2 | 18.3 | -9.8 |
| PAVS | nasdaq | 2023-08-15 | 0 | 0 | 0 | 0 | 0 | -3.2 | 18.3 | -9.8 |
| PAVS | russell2k | 2023-08-25 | 1 | 0 | 1 | 0 | 0 | 2.3 | 12.8 | -9.4 |
| PAVS | nasdaq | 2023-08-25 | 1 | 0 | 1 | 0 | 0 | 2.3 | 12.8 | -9.4 |
| PAVS | russell2k | 2023-09-05 | 0 | 0 | 0 | 0 | 0 | -4.1 | 14.2 | -4.7 |
| PAVS | nasdaq | 2023-09-05 | 0 | 0 | 0 | 0 | 0 | -4.1 | 14.2 | -4.7 |
| PAVS | russell2k | 2023-09-13 | 0 | 0 | 0 | 0 | 0 | -16.5 | 2.2 | -16.8 |
| PAVS | nasdaq | 2023-09-13 | 0 | 0 | 0 | 0 | 0 | -16.5 | 2.2 | -16.8 |
| PAVS | russell2k | 2023-09-21 | 0 | 0 | 0 | 0 | 0 | -18.4 | 3.8 | -22.3 |
| PAVS | nasdaq | 2023-09-21 | 0 | 0 | 0 | 0 | 0 | -18.4 | 3.8 | -22.3 |
| PAVS | russell2k | 2023-10-02 | 0 | 0 | 0 | 0 | 0 | -23.7 | 2.1 | -34.0 |
| PAVS | nasdaq | 2023-10-02 | 0 | 0 | 0 | 0 | 0 | -23.7 | 2.1 | -34.0 |
| PAVS | russell2k | 2023-10-13 | 1 | 0 | 0 | 1 | 0 | -18.8 | -3.5 | -28.3 |
| PAVS | nasdaq | 2023-10-13 | 1 | 0 | 0 | 1 | 0 | -18.8 | -3.5 | -28.3 |
| PAVS | russell2k | 2023-10-23 | 1 | 0 | 0 | 1 | 0 | -21.8 | 0.4 | -24.7 |
| PAVS | nasdaq | 2023-10-23 | 1 | 0 | 0 | 1 | 0 | -21.8 | 0.4 | -24.7 |
| PAVS | russell2k | 2023-11-03 | 1 | 0 | 1 | 0 | 0 | 26.7 | 62.4 | -5.8 |
| PAVS | nasdaq | 2023-11-03 | 1 | 0 | 1 | 0 | 0 | 26.7 | 62.4 | -5.8 |
| PAVS | russell2k | 2023-11-14 | 1 | 0 | 1 | 0 | 0 | -9.0 | 40.4 | -15.3 |
| PAVS | nasdaq | 2023-11-14 | 1 | 0 | 1 | 0 | 0 | -9.0 | 40.4 | -15.3 |
| PAVS | russell2k | 2023-11-30 | 0 | 0 | 0 | 0 | 0 | 20.3 | 23.9 | -8.5 |
| PAVS | nasdaq | 2023-11-30 | 0 | 0 | 0 | 0 | 0 | 20.3 | 23.9 | -8.5 |
| PAVS | russell2k | 2023-12-11 | 0 | 0 | 0 | 0 | 0 | 7.2 | 11.0 | -17.4 |
| PAVS | nasdaq | 2023-12-11 | 0 | 0 | 0 | 0 | 0 | 7.2 | 11.0 | -17.4 |
| PAVS | russell2k | 2023-12-19 | 0 | 0 | 0 | 0 | 0 | -10.6 | 8.4 | -20.4 |
| PAVS | nasdaq | 2023-12-19 | 0 | 0 | 0 | 0 | 0 | -10.6 | 8.4 | -20.4 |
| PAVS | russell2k | 2024-01-02 | 0 | 0 | 0 | 0 | 0 | 4.9 | 10.6 | -11.0 |
| PAVS | nasdaq | 2024-01-02 | 0 | 0 | 0 | 0 | 0 | 4.9 | 10.6 | -11.0 |
| PAVS | russell2k | 2024-01-10 | 0 | 0 | 0 | 0 | 0 | 3.7 | 10.6 | -4.3 |
| PAVS | nasdaq | 2024-01-10 | 0 | 0 | 0 | 0 | 0 | 3.7 | 10.6 | -4.3 |
| PAVS | russell2k | 2024-01-19 | 0 | 0 | 0 | 0 | 0 | -16.3 | 0.7 | -19.0 |
| PAVS | nasdaq | 2024-01-19 | 0 | 0 | 0 | 0 | 0 | -16.3 | 0.7 | -19.0 |
| PAVS | russell2k | 2024-01-29 | 0 | 0 | 0 | 0 | 0 | -3.6 | -1.0 | -10.9 |
| PAVS | nasdaq | 2024-01-29 | 0 | 0 | 0 | 0 | 0 | -3.6 | -1.0 | -10.9 |
| PAVS | russell2k | 2024-02-09 | 0 | 0 | 0 | 0 | 0 | -9.8 | 7.1 | -17.9 |
| PAVS | nasdaq | 2024-02-09 | 0 | 0 | 0 | 0 | 0 | -9.8 | 7.1 | -17.9 |
| PAVS | russell2k | 2024-02-22 | 0 | 0 | 0 | 0 | 0 | -20.7 | 1.2 | -22.8 |
| PAVS | nasdaq | 2024-02-22 | 0 | 0 | 0 | 0 | 0 | -20.7 | 1.2 | -22.8 |
| PAVS | russell2k | 2024-03-06 | 1 | 0 | 0 | 1 | 0 | -14.3 | 0.3 | -14.3 |
| PAVS | nasdaq | 2024-03-06 | 1 | 0 | 0 | 1 | 0 | -14.3 | 0.3 | -14.3 |
| PAVS | russell2k | 2024-03-20 | 1 | 0 | 0 | 1 | 0 | -4.5 | 3.7 | -15.0 |
| PAVS | nasdaq | 2024-03-20 | 1 | 0 | 0 | 1 | 0 | -4.5 | 3.7 | -15.0 |
| PAVS | russell2k | 2024-03-28 | 1 | 0 | 0 | 1 | 0 | -26.1 | 2.2 | -31.1 |
| PAVS | nasdaq | 2024-03-28 | 1 | 0 | 0 | 1 | 0 | -26.1 | 2.2 | -31.1 |
| PAVS | russell2k | 2024-04-08 | 1 | 0 | 0 | 1 | 0 | -33.5 | 0.6 | -40.6 |
| PAVS | nasdaq | 2024-04-08 | 1 | 0 | 0 | 1 | 0 | -33.5 | 0.6 | -40.6 |
| PAVS | russell2k | 2024-04-17 | 1 | 0 | 0 | 1 | 0 | -3.5 | 10.5 | -13.2 |
| PAVS | nasdaq | 2024-04-17 | 1 | 0 | 0 | 1 | 0 | -3.5 | 10.5 | -13.2 |
| PAVS | russell2k | 2024-04-29 | 1 | 0 | 0 | 1 | 0 | -4.7 | 18.9 | -5.7 |
| PAVS | nasdaq | 2024-04-29 | 1 | 0 | 0 | 1 | 0 | -4.7 | 18.9 | -5.7 |
| PAVS | russell2k | 2024-05-07 | 0 | 0 | 0 | 0 | 0 | -16.3 | 11.3 | -17.0 |
| PAVS | nasdaq | 2024-05-07 | 0 | 0 | 0 | 0 | 0 | -16.3 | 11.3 | -17.0 |
| PAVS | russell2k | 2024-05-28 | 1 | 0 | 0 | 1 | 0 | -26.4 | -4.6 | -28.3 |
| PAVS | nasdaq | 2024-05-28 | 1 | 0 | 0 | 1 | 0 | -26.4 | -4.6 | -28.3 |
| PAVS | russell2k | 2024-06-05 | 1 | 0 | 0 | 1 | 0 | -4.8 | 8.2 | -8.0 |
| PAVS | nasdaq | 2024-06-05 | 1 | 0 | 0 | 1 | 0 | -4.8 | 8.2 | -8.0 |
| PAVS | russell2k | 2024-06-13 | 0 | 0 | 0 | 0 | 0 | -4.3 | 5.4 | -9.7 |
| PAVS | nasdaq | 2024-06-13 | 0 | 0 | 0 | 0 | 0 | -4.3 | 5.4 | -9.7 |
| PAVS | russell2k | 2024-06-24 | 0 | 0 | 0 | 0 | 0 | 2.2 | 7.9 | -5.6 |
| PAVS | nasdaq | 2024-06-24 | 0 | 0 | 0 | 0 | 0 | 2.2 | 7.9 | -5.6 |
| PAVS | russell2k | 2024-07-02 | 0 | 0 | 0 | 0 | 0 | 1.0 | 25.5 | -10.4 |
| PAVS | nasdaq | 2024-07-02 | 0 | 0 | 0 | 0 | 0 | 1.0 | 25.5 | -10.4 |
| PAVS | russell2k | 2024-07-15 | 0 | 0 | 0 | 0 | 0 | 12.2 | 15.3 | -8.9 |
| PAVS | nasdaq | 2024-07-15 | 0 | 0 | 0 | 0 | 0 | 12.2 | 15.3 | -8.9 |
| PAVS | russell2k | 2024-07-23 | 1 | 0 | 0 | 1 | 0 | 8.4 | 12.6 | -9.3 |
| PAVS | nasdaq | 2024-07-23 | 1 | 0 | 0 | 1 | 0 | 8.4 | 12.6 | -9.3 |
| PAVS | russell2k | 2024-08-05 | 1 | 0 | 1 | 0 | 0 | 4.0 | 9.0 | -11.0 |
| PAVS | nasdaq | 2024-08-05 | 1 | 0 | 1 | 0 | 0 | 4.0 | 9.0 | -11.0 |
| PAVS | russell2k | 2024-08-13 | 0 | 0 | 0 | 0 | 0 | -6.9 | 6.9 | -13.7 |
| PAVS | nasdaq | 2024-08-13 | 0 | 0 | 0 | 0 | 0 | -6.9 | 6.9 | -13.7 |
| PAVS | russell2k | 2024-08-21 | 0 | 0 | 0 | 0 | 0 | -20.4 | 1.0 | -20.4 |
| PAVS | nasdaq | 2024-08-21 | 0 | 0 | 0 | 0 | 0 | -20.4 | 1.0 | -20.4 |
| PAVS | russell2k | 2024-08-30 | 0 | 0 | 0 | 0 | 0 | -5.7 | -0.1 | -11.2 |
| PAVS | nasdaq | 2024-08-30 | 0 | 0 | 0 | 0 | 0 | -5.7 | -0.1 | -11.2 |
| PAVS | russell2k | 2024-09-12 | 0 | 0 | 0 | 0 | 0 | 10.7 | 10.7 | -1.5 |
| PAVS | nasdaq | 2024-09-12 | 0 | 0 | 0 | 0 | 0 | 10.7 | 10.7 | -1.5 |
| PAVS | russell2k | 2024-09-23 | 0 | 0 | 0 | 0 | 0 | -16.6 | 19.3 | -61.7 |
| PAVS | nasdaq | 2024-09-23 | 0 | 0 | 0 | 0 | 0 | -16.6 | 19.3 | -61.7 |
| PAVS | russell2k | 2024-10-01 | 0 | 0 | 0 | 0 | 0 | -22.9 | 2.4 | -30.0 |
| PAVS | nasdaq | 2024-10-01 | 0 | 0 | 0 | 0 | 0 | -22.9 | 2.4 | -30.0 |
| PAVS | russell2k | 2024-10-09 | 1 | 0 | 0 | 1 | 0 | 8.5 | 10.6 | -4.7 |
| PAVS | nasdaq | 2024-10-09 | 1 | 0 | 0 | 1 | 0 | 8.5 | 10.6 | -4.7 |
| PAVS | russell2k | 2024-10-17 | 1 | 0 | 0 | 1 | 0 | 31.4 | 92.8 | 1.6 |
| PAVS | nasdaq | 2024-10-17 | 1 | 0 | 0 | 1 | 0 | 31.4 | 92.8 | 1.6 |
| PAVS | russell2k | 2024-10-28 | 0 | 0 | 0 | 0 | 0 | -2.1 | 5.9 | -10.7 |
| PAVS | nasdaq | 2024-10-28 | 0 | 0 | 0 | 0 | 0 | -2.1 | 5.9 | -10.7 |
| PAVS | russell2k | 2024-11-08 | 0 | 0 | 0 | 0 | 0 | -0.6 | 3.1 | -9.3 |
| PAVS | nasdaq | 2024-11-08 | 0 | 0 | 0 | 0 | 0 | -0.6 | 3.1 | -9.3 |
| PAVS | russell2k | 2024-11-18 | 0 | 0 | 0 | 0 | 0 | -0.9 | 5.3 | -4.1 |
| PAVS | nasdaq | 2024-11-18 | 0 | 0 | 0 | 0 | 0 | -0.9 | 5.3 | -4.1 |
| PAVS | russell2k | 2024-12-02 | 0 | 0 | 0 | 0 | 0 | 19.5 | 41.3 | -1.0 |
| PAVS | nasdaq | 2024-12-02 | 0 | 0 | 0 | 0 | 0 | 19.5 | 41.3 | -1.0 |
| PAVS | russell2k | 2024-12-10 | 0 | 0 | 0 | 0 | 0 | 42.9 | 54.6 | -1.9 |
| PAVS | nasdaq | 2024-12-10 | 0 | 0 | 0 | 0 | 0 | 42.9 | 54.6 | -1.9 |
| PAVS | russell2k | 2024-12-18 | 1 | 1 | 0 | 0 | 0 | -8.6 | 5.0 | -17.1 |
| PAVS | nasdaq | 2024-12-18 | 1 | 1 | 0 | 0 | 0 | -8.6 | 5.0 | -17.1 |
| PAVS | russell2k | 2024-12-31 | 1 | 0 | 1 | 0 | 0 | -3.5 | 7.5 | -10.8 |
| PAVS | nasdaq | 2024-12-31 | 1 | 0 | 1 | 0 | 0 | -3.5 | 7.5 | -10.8 |
| PAVS | russell2k | 2025-01-10 | 1 | 0 | 1 | 0 | 0 | 4.0 | 6.8 | -7.1 |
| PAVS | nasdaq | 2025-01-10 | 1 | 0 | 1 | 0 | 0 | 4.0 | 6.8 | -7.1 |
| PAVS | russell2k | 2025-01-22 | 1 | 0 | 1 | 0 | 0 | -1.5 | 3.6 | -5.1 |
| PAVS | nasdaq | 2025-01-22 | 1 | 0 | 1 | 0 | 0 | -1.5 | 3.6 | -5.1 |
| PAVS | russell2k | 2025-01-30 | 0 | 0 | 0 | 0 | 0 | -1.4 | 3.6 | -6.4 |
| PAVS | nasdaq | 2025-01-30 | 0 | 0 | 0 | 0 | 0 | -1.4 | 3.6 | -6.4 |
| PAVS | russell2k | 2025-02-07 | 0 | 0 | 0 | 0 | 0 | 0.7 | 6.6 | -3.7 |
| PAVS | nasdaq | 2025-02-07 | 0 | 0 | 0 | 0 | 0 | 0.7 | 6.6 | -3.7 |
| PAVS | russell2k | 2025-02-18 | 0 | 0 | 0 | 0 | 0 | 8.8 | 10.3 | -8.1 |
| PAVS | nasdaq | 2025-02-18 | 0 | 0 | 0 | 0 | 0 | 8.8 | 10.3 | -8.1 |
| PAVS | russell2k | 2025-02-26 | 0 | 0 | 0 | 0 | 0 | 1.4 | 7.9 | -10.1 |
| PAVS | nasdaq | 2025-02-26 | 0 | 0 | 0 | 0 | 0 | 1.4 | 7.9 | -10.1 |
| PAVS | russell2k | 2025-03-10 | 1 | 0 | 0 | 1 | 0 | -4.9 | -2.1 | -7.3 |
| PAVS | nasdaq | 2025-03-10 | 1 | 0 | 0 | 1 | 0 | -4.9 | -2.1 | -7.3 |
| PAVS | russell2k | 2025-03-18 | 1 | 0 | 0 | 1 | 0 | -1.5 | 2.2 | -2.9 |
| PAVS | nasdaq | 2025-03-18 | 1 | 0 | 0 | 1 | 0 | -1.5 | 2.2 | -2.9 |
| PAVS | russell2k | 2025-03-28 | 0 | 0 | 0 | 0 | 0 | -1.5 | 3.7 | -4.4 |
| PAVS | nasdaq | 2025-03-28 | 0 | 0 | 0 | 0 | 0 | -1.5 | 3.7 | -4.4 |
| PAVS | russell2k | 2025-04-10 | 0 | 0 | 0 | 0 | 0 | -0.2 | 4.5 | -11.2 |
| PAVS | nasdaq | 2025-04-10 | 0 | 0 | 0 | 0 | 0 | -0.2 | 4.5 | -11.2 |
| PAVS | russell2k | 2025-04-28 | 0 | 0 | 0 | 0 | 0 | -28.4 | 4.5 | -28.4 |
| PAVS | nasdaq | 2025-04-28 | 0 | 0 | 0 | 0 | 0 | -28.4 | 4.5 | -28.4 |
| PAVS | russell2k | 2025-05-15 | 0 | 0 | 0 | 0 | 0 | -1.3 | 12.2 | -4.9 |
| PAVS | nasdaq | 2025-05-15 | 0 | 0 | 0 | 0 | 0 | -1.3 | 12.2 | -4.9 |
| PAVS | russell2k | 2025-05-27 | 0 | 0 | 0 | 0 | 0 | -12.9 | -1.0 | -19.8 |
| PAVS | nasdaq | 2025-05-27 | 0 | 0 | 0 | 0 | 0 | -12.9 | -1.0 | -19.8 |
| PAVS | russell2k | 2025-06-06 | 0 | 0 | 0 | 0 | 0 | -4.4 | 11.1 | -5.6 |
| PAVS | nasdaq | 2025-06-06 | 0 | 0 | 0 | 0 | 0 | -4.4 | 11.1 | -5.6 |
| PAVS | russell2k | 2025-06-16 | 1 | 0 | 0 | 1 | 0 | -1.8 | 5.3 | -12.5 |
| PAVS | nasdaq | 2025-06-16 | 1 | 0 | 0 | 1 | 0 | -1.8 | 5.3 | -12.5 |
| PAVS | russell2k | 2025-07-01 | 0 | 0 | 0 | 0 | 0 | -6.1 | 6.3 | -11.7 |
| PAVS | nasdaq | 2025-07-01 | 0 | 0 | 0 | 0 | 0 | -6.1 | 6.3 | -11.7 |
| PAVS | russell2k | 2025-07-15 | 0 | 0 | 0 | 0 | 0 | -6.6 | 19.7 | -14.9 |
| PAVS | nasdaq | 2025-07-15 | 0 | 0 | 0 | 0 | 0 | -6.6 | 19.7 | -14.9 |
| PAVS | russell2k | 2025-07-23 | 0 | 0 | 0 | 0 | 0 | -20.8 | 5.6 | -27.9 |
| PAVS | nasdaq | 2025-07-23 | 0 | 0 | 0 | 0 | 0 | -20.8 | 5.6 | -27.9 |
| PAVS | russell2k | 2025-08-04 | 1 | 0 | 0 | 1 | 0 | -23.8 | 2.3 | -27.0 |
| PAVS | nasdaq | 2025-08-04 | 1 | 0 | 0 | 1 | 0 | -23.8 | 2.3 | -27.0 |
| PAVS | russell2k | 2025-08-13 | 1 | 0 | 0 | 1 | 0 | -11.0 | -0.6 | -14.4 |
| PAVS | nasdaq | 2025-08-13 | 1 | 0 | 0 | 1 | 0 | -11.0 | -0.6 | -14.4 |
| PAVS | russell2k | 2025-08-21 | 0 | 0 | 0 | 0 | 0 | 15.5 | 54.9 | -7.0 |
| PAVS | nasdaq | 2025-08-21 | 0 | 0 | 0 | 0 | 0 | 15.5 | 54.9 | -7.0 |
| PAVS | russell2k | 2025-09-03 | 1 | 0 | 0 | 1 | 0 | 16.7 | 51.0 | -2.5 |
| PAVS | nasdaq | 2025-09-03 | 1 | 0 | 0 | 1 | 0 | 16.7 | 51.0 | -2.5 |
| PAVS | russell2k | 2025-09-16 | 0 | 0 | 0 | 0 | 0 | 34.2 | 51.0 | -12.3 |
| PAVS | nasdaq | 2025-09-16 | 0 | 0 | 0 | 0 | 0 | 34.2 | 51.0 | -12.3 |
| PAVS | russell2k | 2025-09-24 | 1 | 0 | 1 | 0 | 0 | 5.1 | 19.4 | -12.2 |
| PAVS | nasdaq | 2025-09-24 | 1 | 0 | 1 | 0 | 0 | 5.1 | 19.4 | -12.2 |
| PAVS | russell2k | 2025-10-02 | 1 | 0 | 1 | 0 | 0 | -43.4 | 19.0 | -55.6 |
| PAVS | nasdaq | 2025-10-02 | 1 | 0 | 1 | 0 | 0 | -43.4 | 19.0 | -55.6 |
| PAVS | russell2k | 2025-10-10 | 0 | 0 | 0 | 0 | 0 | -46.4 | 1.8 | -59.5 |
| PAVS | nasdaq | 2025-10-10 | 0 | 0 | 0 | 0 | 0 | -46.4 | 1.8 | -59.5 |
| PAVS | russell2k | 2025-10-20 | 0 | 0 | 0 | 0 | 0 | 6.9 | 28.3 | -6.9 |
| PAVS | nasdaq | 2025-10-20 | 0 | 0 | 0 | 0 | 0 | 6.9 | 28.3 | -6.9 |
| PAVS | russell2k | 2025-10-28 | 0 | 0 | 0 | 0 | 0 | -10.8 | 1.5 | -19.4 |
| PAVS | nasdaq | 2025-10-28 | 0 | 0 | 0 | 0 | 0 | -10.8 | 1.5 | -19.4 |
| PAVS | russell2k | 2025-11-10 | 0 | 0 | 0 | 0 | 0 | -0.9 | 26.3 | -26.6 |
| PAVS | nasdaq | 2025-11-10 | 0 | 0 | 0 | 0 | 0 | -0.9 | 26.3 | -26.6 |
| PAVS | russell2k | 2025-11-18 | 0 | 0 | 0 | 0 | 0 | -94.1 | 107.3 | -94.8 |
| PAVS | nasdaq | 2025-11-18 | 0 | 0 | 0 | 0 | 0 | -94.1 | 107.3 | -94.8 |
| PAVS | russell2k | 2025-11-26 | 2 | 0 | 1 | 1 | 0 | -96.2 | 31.7 | -97.1 |
| PAVS | nasdaq | 2025-11-26 | 2 | 0 | 1 | 1 | 0 | -96.2 | 31.7 | -97.1 |
| PAVS | russell2k | 2025-12-19 | 2 | 0 | 0 | 1 | 1 | 3.1 | 13.5 | -38.1 |
| PAVS | nasdaq | 2025-12-19 | 2 | 0 | 0 | 1 | 1 | 3.1 | 13.5 | -38.1 |
| PAVS | russell2k | 2025-12-31 | 1 | 0 | 0 | 1 | 0 | 16.2 | 75.7 | 0.6 |
| PAVS | nasdaq | 2025-12-31 | 1 | 0 | 0 | 1 | 0 | 16.2 | 75.7 | 0.6 |
| PAVS | russell2k | 2026-01-15 | 1 | 1 | 0 | 0 | 0 | -33.8 | 1.2 | -35.6 |
| PAVS | nasdaq | 2026-01-15 | 1 | 1 | 0 | 0 | 0 | -33.8 | 1.2 | -35.6 |
| PAVS | russell2k | 2026-01-27 | 1 | 1 | 0 | 0 | 0 | -15.1 | 3.8 | -32.1 |
| PAVS | nasdaq | 2026-01-27 | 1 | 1 | 0 | 0 | 0 | -15.1 | 3.8 | -32.1 |
| PAVS | russell2k | 2026-02-05 | 3 | 1 | 1 | 1 | 0 | -1.6 | 14.4 | -7.5 |
| PAVS | nasdaq | 2026-02-05 | 3 | 1 | 1 | 1 | 0 | -1.6 | 14.4 | -7.5 |
| PAVS | russell2k | 2026-02-19 | 3 | 1 | 1 | 1 | 0 | -1.6 | 3.9 | -8.6 |
| PAVS | nasdaq | 2026-02-19 | 3 | 1 | 1 | 1 | 0 | -1.6 | 3.9 | -8.6 |
| PAVS | russell2k | 2026-02-27 | 1 | 1 | 0 | 0 | 0 | 25.6 | 83.7 | -7.0 |
| PAVS | nasdaq | 2026-02-27 | 1 | 1 | 0 | 0 | 0 | 25.6 | 83.7 | -7.0 |
| PAVS | russell2k | 2026-03-10 | 1 | 1 | 0 | 0 | 0 | -73.9 | 71.7 | -82.3 |
| PAVS | nasdaq | 2026-03-10 | 1 | 1 | 0 | 0 | 0 | -73.9 | 71.7 | -82.3 |
| PAVS | russell2k | 2026-03-20 | 2 | 1 | 0 | 0 | 1 | -48.5 | 156.1 | -52.6 |
| PAVS | nasdaq | 2026-03-20 | 2 | 1 | 0 | 0 | 1 | -48.5 | 156.1 | -52.6 |
| PAVS | russell2k | 2026-03-31 | 2 | 1 | 0 | 0 | 1 | -20.7 | -2.3 | -22.5 |
| PAVS | nasdaq | 2026-03-31 | 2 | 1 | 0 | 0 | 1 | -20.7 | -2.3 | -22.5 |
| PAVS | russell2k | 2026-04-09 | 2 | 1 | 0 | 1 | 0 | -25.1 | 7.3 | -26.7 |
| PAVS | nasdaq | 2026-04-09 | 2 | 1 | 0 | 1 | 0 | -25.1 | 7.3 | -26.7 |
| PAVS | russell2k | 2026-04-22 | 2 | 1 | 0 | 1 | 0 | -25.2 | 0.0 | -37.4 |
| PAVS | nasdaq | 2026-04-22 | 2 | 1 | 0 | 1 | 0 | -25.2 | 0.0 | -37.4 |
| PAVS | russell2k | 2026-04-30 | 2 | 1 | 0 | 1 | 0 | -3.4 | 14.7 | -4.3 |
| PAVS | nasdaq | 2026-04-30 | 2 | 1 | 0 | 1 | 0 | -3.4 | 14.7 | -4.3 |
| PAVS | russell2k | 2026-05-19 | 1 | 1 | 0 | 0 | 0 | -7.1 | 13.3 | -9.7 |
| PAVS | nasdaq | 2026-05-19 | 1 | 1 | 0 | 0 | 0 | 0.0 | 13.3 | -9.7 |
| WNW | nasdaq | 2021-06-23 | 0 | 0 | 0 | 0 | 0 | 2.3 | 52.4 | -2.8 |
| WNW | nasdaq | 2021-07-02 | 0 | 0 | 0 | 0 | 0 | -24.8 | -2.9 | -31.9 |
| WNW | nasdaq | 2021-07-19 | 0 | 0 | 0 | 0 | 0 | -13.0 | 6.0 | -15.5 |
| WNW | nasdaq | 2021-07-28 | 0 | 0 | 0 | 0 | 0 | -2.1 | 8.4 | -9.2 |
| WNW | nasdaq | 2021-08-09 | 0 | 0 | 0 | 0 | 0 | -11.4 | 2.1 | -18.5 |
| WNW | nasdaq | 2021-08-20 | 1 | 1 | 0 | 0 | 0 | 15.3 | 21.3 | 0.2 |
| WNW | nasdaq | 2021-08-31 | 1 | 1 | 0 | 0 | 0 | -12.4 | 5.4 | -13.0 |
| WNW | nasdaq | 2021-09-09 | 1 | 1 | 0 | 0 | 0 | -14.6 | -0.5 | -18.3 |
| WNW | nasdaq | 2021-09-27 | 1 | 1 | 0 | 0 | 0 | -17.4 | 1.8 | -22.9 |
| WNW | nasdaq | 2021-10-05 | 1 | 1 | 0 | 0 | 0 | 2.4 | 6.7 | -8.3 |
| WNW | nasdaq | 2021-10-13 | 1 | 0 | 0 | 1 | 0 | -9.1 | 10.7 | -10.9 |
| WNW | nasdaq | 2021-10-25 | 1 | 0 | 0 | 1 | 0 | -8.5 | 1.6 | -9.0 |
| WNW | nasdaq | 2021-11-03 | 0 | 0 | 0 | 0 | 0 | 4.8 | 74.2 | -10.4 |
| WNW | nasdaq | 2021-11-11 | 0 | 0 | 0 | 0 | 0 | -7.1 | 84.5 | -10.1 |
| WNW | nasdaq | 2021-11-24 | 1 | 0 | 1 | 0 | 0 | -19.8 | -2.7 | -28.1 |
| WNW | nasdaq | 2021-12-07 | 1 | 0 | 0 | 1 | 0 | -13.3 | 5.2 | -17.3 |
| WNW | nasdaq | 2021-12-17 | 1 | 0 | 0 | 1 | 0 | -2.0 | 27.6 | -10.4 |
| WNW | nasdaq | 2021-12-28 | 1 | 0 | 0 | 1 | 0 | -10.2 | 30.2 | -15.5 |
| WNW | nasdaq | 2022-01-11 | 0 | 0 | 0 | 0 | 0 | -20.0 | 6.4 | -29.5 |
| WNW | nasdaq | 2022-01-28 | 0 | 0 | 0 | 0 | 0 | 22.3 | 33.6 | -2.0 |
| WNW | nasdaq | 2022-02-07 | 0 | 0 | 0 | 0 | 0 | -17.2 | 60.3 | -20.6 |
| WNW | nasdaq | 2022-02-15 | 0 | 0 | 0 | 0 | 0 | -41.9 | 55.7 | -44.8 |
| WNW | nasdaq | 2022-02-24 | 0 | 0 | 0 | 0 | 0 | -38.1 | 58.9 | -42.2 |
| WNW | nasdaq | 2022-03-09 | 1 | 0 | 0 | 1 | 0 | 11.3 | 26.4 | -22.8 |
| WNW | nasdaq | 2022-03-18 | 1 | 0 | 0 | 1 | 0 | -21.5 | 34.0 | -23.0 |
| WNW | nasdaq | 2022-03-28 | 2 | 0 | 1 | 1 | 0 | -35.0 | 3.4 | -41.0 |
| WNW | nasdaq | 2022-04-06 | 2 | 0 | 1 | 1 | 0 | -13.4 | 5.0 | -18.1 |
| WNW | nasdaq | 2022-04-19 | 2 | 1 | 0 | 1 | 0 | 3.3 | 5.3 | -15.2 |
| WNW | nasdaq | 2022-04-28 | 1 | 1 | 0 | 0 | 0 | -10.1 | 8.6 | -12.6 |
| WNW | nasdaq | 2022-05-12 | 1 | 1 | 0 | 0 | 0 | 3.7 | 12.1 | -3.8 |
| WNW | nasdaq | 2022-05-23 | 1 | 1 | 0 | 0 | 0 | -7.4 | 17.0 | -18.0 |
| WNW | nasdaq | 2022-06-03 | 1 | 1 | 0 | 0 | 0 | -10.4 | 8.3 | -24.1 |
| WNW | nasdaq | 2022-06-13 | 2 | 1 | 0 | 1 | 0 | -1.9 | 8.4 | -12.1 |
| WNW | nasdaq | 2022-06-23 | 1 | 0 | 0 | 1 | 0 | 12.7 | 14.1 | -6.8 |
| WNW | nasdaq | 2022-07-01 | 0 | 0 | 0 | 0 | 0 | 25.0 | 32.8 | -3.9 |
| WNW | nasdaq | 2022-07-12 | 0 | 0 | 0 | 0 | 0 | 2.2 | 17.5 | -3.8 |
| WNW | nasdaq | 2022-07-20 | 1 | 0 | 1 | 0 | 0 | -7.7 | 3.2 | -15.8 |
| WNW | nasdaq | 2022-08-02 | 1 | 0 | 1 | 0 | 0 | 9.5 | 20.1 | -2.4 |
| WNW | nasdaq | 2022-08-10 | 0 | 0 | 0 | 0 | 0 | 0.0 | 5.7 | -5.7 |
| WNW | nasdaq | 2022-08-22 | 1 | 0 | 1 | 0 | 0 | -4.2 | 14.9 | -8.5 |
| WNW | nasdaq | 2022-08-31 | 1 | 0 | 1 | 0 | 0 | -0.9 | 17.6 | -6.3 |
| WNW | nasdaq | 2022-09-09 | 2 | 0 | 1 | 1 | 0 | 1.1 | 10.9 | -7.9 |
| WNW | nasdaq | 2022-09-20 | 2 | 0 | 1 | 1 | 0 | 26.5 | 31.5 | 1.8 |
| WNW | nasdaq | 2022-09-30 | 0 | 0 | 0 | 0 | 0 | 108.5 | 111.0 | -1.0 |
| WNW | nasdaq | 2022-10-11 | 1 | 0 | 1 | 0 | 0 | 9.4 | 63.2 | -12.0 |
| WNW | nasdaq | 2022-10-20 | 1 | 0 | 1 | 0 | 0 | -27.3 | 14.4 | -43.9 |
| WNW | nasdaq | 2022-11-01 | 0 | 0 | 0 | 0 | 0 | 35.8 | 53.4 | -0.4 |
| WNW | nasdaq | 2022-11-09 | 0 | 0 | 0 | 0 | 0 | 19.3 | 35.8 | 2.8 |
| WNW | nasdaq | 2022-11-17 | 0 | 0 | 0 | 0 | 0 | 4.7 | 4.7 | -4.7 |
| WNW | nasdaq | 2022-11-29 | 0 | 0 | 0 | 0 | 0 | 40.8 | 40.8 | -12.3 |
| WNW | nasdaq | 2022-12-07 | 1 | 0 | 1 | 0 | 0 | -77.3 | 91.5 | -77.8 |
| WNW | nasdaq | 2022-12-20 | 1 | 0 | 1 | 0 | 0 | -86.5 | 20.9 | -90.6 |
| WNW | nasdaq | 2022-12-29 | 1 | 0 | 0 | 0 | 1 | 34.2 | 37.0 | -7.9 |
| WNW | nasdaq | 2023-01-09 | 2 | 0 | 0 | 1 | 1 | 10.7 | 20.0 | -6.0 |
| WNW | nasdaq | 2023-01-19 | 0 | 0 | 0 | 0 | 0 | 21.1 | 26.4 | -4.4 |
| WNW | nasdaq | 2023-01-27 | 0 | 0 | 0 | 0 | 0 | -6.6 | 26.1 | -9.8 |
| WNW | nasdaq | 2023-02-13 | 1 | 1 | 0 | 0 | 0 | -10.3 | 6.6 | -13.9 |
| WNW | nasdaq | 2023-02-23 | 1 | 1 | 0 | 0 | 0 | -14.3 | 2.7 | -15.6 |
| WNW | nasdaq | 2023-03-03 | 2 | 1 | 0 | 1 | 0 | -19.6 | 4.6 | -25.7 |
| WNW | nasdaq | 2023-03-14 | 2 | 1 | 0 | 1 | 0 | -0.4 | 8.6 | -8.1 |
| WNW | nasdaq | 2023-03-23 | 1 | 1 | 0 | 0 | 0 | 7.1 | 17.3 | -5.3 |
| WNW | nasdaq | 2023-04-03 | 1 | 1 | 0 | 0 | 0 | 14.0 | 18.4 | -6.3 |
| WNW | nasdaq | 2023-04-12 | 2 | 1 | 1 | 0 | 0 | -7.3 | 7.8 | -13.6 |
| WNW | nasdaq | 2023-04-20 | 2 | 1 | 1 | 0 | 0 | -5.2 | 23.4 | -13.7 |
| WNW | nasdaq | 2023-04-28 | 2 | 1 | 1 | 0 | 0 | -7.6 | 26.9 | -13.7 |
| WNW | nasdaq | 2023-05-10 | 2 | 1 | 1 | 0 | 0 | -3.4 | 4.8 | -8.7 |
| WNW | nasdaq | 2023-05-18 | 2 | 1 | 1 | 0 | 0 | 5.7 | 7.7 | -10.7 |
| WNW | nasdaq | 2023-05-26 | 1 | 1 | 0 | 0 | 0 | 11.2 | 17.7 | -2.9 |
| WNW | nasdaq | 2023-06-07 | 2 | 1 | 1 | 0 | 0 | -3.6 | 1.6 | -8.6 |
| WNW | nasdaq | 2023-06-16 | 2 | 1 | 1 | 0 | 0 | -3.1 | 2.8 | -12.4 |
| WNW | nasdaq | 2023-06-30 | 1 | 1 | 0 | 0 | 0 | 8.2 | 13.6 | -3.2 |
| WNW | nasdaq | 2023-07-11 | 2 | 1 | 1 | 0 | 0 | -0.3 | 11.6 | -7.2 |
| WNW | nasdaq | 2023-07-19 | 1 | 0 | 1 | 0 | 0 | -9.7 | 1.5 | -10.7 |
| WNW | nasdaq | 2023-07-28 | 0 | 0 | 0 | 0 | 0 | -7.6 | 3.4 | -8.6 |
| WNW | nasdaq | 2023-08-11 | 0 | 0 | 0 | 0 | 0 | -24.1 | 1.0 | -30.9 |
| WNW | nasdaq | 2023-08-23 | 0 | 0 | 0 | 0 | 0 | -29.8 | -1.7 | -34.3 |
| WNW | nasdaq | 2023-09-07 | 0 | 0 | 0 | 0 | 0 | 2.0 | 11.2 | -8.8 |
| WNW | nasdaq | 2023-09-25 | 0 | 0 | 0 | 0 | 0 | -12.2 | 71.3 | -22.0 |
| WNW | nasdaq | 2023-10-04 | 1 | 0 | 1 | 0 | 0 | 7.9 | 102.7 | -5.2 |
| WNW | nasdaq | 2023-10-12 | 0 | 0 | 0 | 0 | 0 | -12.7 | 11.3 | -18.8 |
| WNW | nasdaq | 2023-10-24 | 0 | 0 | 0 | 0 | 0 | 13.0 | 40.6 | -9.2 |
| WNW | nasdaq | 2023-11-01 | 0 | 0 | 0 | 0 | 0 | -27.2 | -5.6 | -31.0 |
| WNW | nasdaq | 2023-11-15 | 0 | 0 | 0 | 0 | 0 | -20.0 | 6.4 | -24.0 |
| WNW | nasdaq | 2023-11-24 | 0 | 0 | 0 | 0 | 0 | -12.0 | 92.5 | -21.9 |
| WNW | nasdaq | 2023-12-06 | 1 | 0 | 0 | 1 | 0 | -29.3 | -10.8 | -44.7 |
| WNW | nasdaq | 2023-12-14 | 1 | 0 | 0 | 1 | 0 | 26.7 | 38.7 | -35.5 |
| WNW | nasdaq | 2023-12-22 | 0 | 0 | 0 | 0 | 0 | 37.8 | 72.4 | -0.1 |
| WNW | nasdaq | 2024-01-03 | 1 | 0 | 0 | 1 | 0 | 18.3 | 24.3 | -19.5 |
| WNW | nasdaq | 2024-01-11 | 1 | 0 | 0 | 1 | 0 | 193.4 | 216.5 | -2.0 |
| WNW | nasdaq | 2024-01-22 | 0 | 0 | 0 | 0 | 0 | -84.2 | 75.5 | -85.3 |
| WNW | nasdaq | 2024-02-06 | 2 | 0 | 0 | 1 | 1 | -12.4 | 29.7 | -21.4 |
| WNW | nasdaq | 2024-02-14 | 2 | 0 | 0 | 1 | 1 | -11.9 | 3.7 | -18.5 |
| WNW | nasdaq | 2024-02-26 | 1 | 0 | 1 | 0 | 0 | -2.4 | 4.0 | -10.5 |
| WNW | nasdaq | 2024-03-06 | 0 | 0 | 0 | 0 | 0 | -2.5 | 33.3 | -6.7 |
| WNW | nasdaq | 2024-03-15 | 1 | 1 | 0 | 0 | 0 | -15.0 | 33.3 | -18.3 |
| WNW | nasdaq | 2024-03-28 | 1 | 1 | 0 | 0 | 0 | 10.9 | 98.0 | -4.3 |
| WNW | nasdaq | 2024-04-08 | 1 | 1 | 0 | 0 | 0 | 10.7 | 101.2 | -1.4 |
| WNW | nasdaq | 2024-04-16 | 1 | 1 | 0 | 0 | 0 | -2.9 | 17.1 | -6.7 |
| WNW | nasdaq | 2024-04-26 | 1 | 1 | 0 | 0 | 0 | 7.7 | 25.0 | -4.7 |
| WNW | nasdaq | 2024-05-07 | 1 | 1 | 0 | 0 | 0 | 5.9 | 27.4 | -2.0 |
| WNW | nasdaq | 2024-05-17 | 2 | 1 | 1 | 0 | 0 | -1.9 | 22.9 | -4.8 |
| WNW | nasdaq | 2024-06-03 | 1 | 1 | 0 | 0 | 0 | -7.8 | 8.7 | -8.9 |
| WNW | nasdaq | 2024-06-14 | 1 | 1 | 0 | 0 | 0 | -13.1 | 2.9 | -15.8 |
| WNW | nasdaq | 2024-06-26 | 1 | 1 | 0 | 0 | 0 | -10.9 | 9.4 | -16.5 |
| WNW | nasdaq | 2024-07-10 | 2 | 1 | 0 | 1 | 0 | 29.7 | 108.1 | -1.2 |
| WNW | nasdaq | 2024-07-18 | 2 | 1 | 1 | 0 | 0 | -21.2 | -2.5 | -23.6 |
| WNW | nasdaq | 2024-07-26 | 1 | 0 | 1 | 0 | 0 | -20.7 | -1.7 | -25.1 |
| WNW | nasdaq | 2024-08-07 | 0 | 0 | 0 | 0 | 0 | 4.8 | 7.2 | -13.3 |
| WNW | nasdaq | 2024-08-15 | 0 | 0 | 0 | 0 | 0 | 8.0 | 28.3 | -3.1 |
| WNW | nasdaq | 2024-08-26 | 0 | 0 | 0 | 0 | 0 | -8.6 | -0.0 | -11.5 |
| WNW | nasdaq | 2024-09-04 | 0 | 0 | 0 | 0 | 0 | 3.7 | 18.5 | -3.7 |
| WNW | nasdaq | 2024-09-17 | 1 | 0 | 1 | 0 | 0 | -4.2 | 13.1 | -8.5 |
| WNW | nasdaq | 2024-09-26 | 1 | 0 | 1 | 0 | 0 | -6.6 | 8.2 | -11.2 |
| WNW | nasdaq | 2024-10-04 | 1 | 0 | 1 | 0 | 0 | -9.3 | 0.7 | -10.5 |
| WNW | nasdaq | 2024-10-22 | 0 | 0 | 0 | 0 | 0 | -2.4 | 5.7 | -5.3 |
| WNW | nasdaq | 2024-11-04 | 0 | 0 | 0 | 0 | 0 | 5.1 | 12.7 | -2.6 |
| WNW | nasdaq | 2024-11-13 | 1 | 1 | 0 | 0 | 0 | 25.8 | 32.1 | -3.1 |
| WNW | nasdaq | 2024-11-21 | 2 | 1 | 1 | 0 | 0 | 34.3 | 42.2 | -4.6 |
| WNW | nasdaq | 2024-12-03 | 1 | 0 | 1 | 0 | 0 | 66.3 | 76.8 | -4.4 |
| WNW | nasdaq | 2024-12-12 | 1 | 0 | 1 | 0 | 0 | 59.7 | 66.7 | -3.0 |
| WNW | nasdaq | 2024-12-20 | 0 | 0 | 0 | 0 | 0 | 43.0 | 62.6 | -25.2 |
| WNW | nasdaq | 2024-12-31 | 0 | 0 | 0 | 0 | 0 | -85.2 | 68.3 | -86.5 |
| WNW | nasdaq | 2025-01-16 | 0 | 0 | 0 | 0 | 0 | -40.7 | 21.6 | -49.0 |
| WNW | nasdaq | 2025-01-27 | 1 | 0 | 0 | 1 | 0 | -19.4 | 2.0 | -33.9 |
| WNW | nasdaq | 2025-02-04 | 1 | 0 | 0 | 1 | 0 | 10.4 | 38.0 | -8.1 |
| WNW | nasdaq | 2025-02-13 | 1 | 0 | 0 | 1 | 0 | -11.6 | 67.8 | -18.8 |
| WNW | nasdaq | 2025-02-24 | 3 | 1 | 1 | 1 | 0 | -22.8 | -1.6 | -31.0 |
| WNW | nasdaq | 2025-03-07 | 2 | 1 | 1 | 0 | 0 | -4.7 | 1.2 | -18.8 |
| WNW | nasdaq | 2025-03-19 | 1 | 1 | 0 | 0 | 0 | -38.2 | 22.6 | -43.8 |
| WNW | nasdaq | 2025-03-27 | 1 | 1 | 0 | 0 | 0 | -52.3 | -28.3 | -54.9 |
| WNW | nasdaq | 2025-04-04 | 2 | 1 | 0 | 1 | 0 | -25.7 | 12.0 | -43.1 |
| WNW | nasdaq | 2025-04-17 | 2 | 1 | 0 | 1 | 0 | 106.7 | 173.4 | 1.0 |
| WNW | nasdaq | 2025-04-28 | 3 | 1 | 1 | 1 | 0 | -14.1 | 22.5 | -18.2 |
| WNW | nasdaq | 2025-05-15 | 1 | 1 | 0 | 0 | 0 | -9.7 | 2.7 | -12.5 |
| WNW | nasdaq | 2025-05-23 | 2 | 1 | 0 | 1 | 0 | -2.5 | 8.0 | -8.1 |
| WNW | nasdaq | 2025-06-04 | 2 | 1 | 0 | 1 | 0 | -2.1 | 5.8 | -10.7 |
| WNW | nasdaq | 2025-06-16 | 1 | 1 | 0 | 0 | 0 | 10.8 | 30.7 | -9.6 |
| WNW | nasdaq | 2025-06-25 | 2 | 1 | 1 | 0 | 0 | -2.1 | 8.5 | -9.5 |
| WNW | nasdaq | 2025-07-03 | 2 | 1 | 1 | 0 | 0 | -1.1 | 2.1 | -10.1 |
| WNW | nasdaq | 2025-07-14 | 1 | 1 | 0 | 0 | 0 | 1.6 | 6.0 | -2.8 |
| WNW | nasdaq | 2025-07-22 | 1 | 1 | 0 | 0 | 0 | -6.3 | 1.6 | -13.2 |
| WNW | nasdaq | 2025-08-01 | 1 | 1 | 0 | 0 | 0 | -4.9 | 4.1 | -6.6 |
| WNW | nasdaq | 2025-08-12 | 1 | 1 | 0 | 0 | 0 | 1.2 | 5.2 | -7.2 |
| WNW | nasdaq | 2025-08-21 | 1 | 1 | 0 | 0 | 0 | 8.5 | 10.8 | -3.5 |
| WNW | nasdaq | 2025-08-29 | 1 | 1 | 0 | 0 | 0 | 27.8 | 109.1 | -3.0 |
| WNW | nasdaq | 2025-09-09 | 2 | 1 | 1 | 0 | 0 | -30.1 | -0.1 | -33.7 |
| WNW | nasdaq | 2025-09-17 | 1 | 0 | 1 | 0 | 0 | -6.7 | 1.0 | -13.9 |
| WNW | nasdaq | 2025-09-30 | 0 | 0 | 0 | 0 | 0 | -7.6 | 9.1 | -8.6 |
| WNW | nasdaq | 2025-10-09 | 0 | 0 | 0 | 0 | 0 | -16.2 | 1.1 | -18.6 |
| WNW | nasdaq | 2025-10-17 | 0 | 0 | 0 | 0 | 0 | -12.3 | 2.7 | -18.2 |
| WNW | nasdaq | 2025-10-28 | 1 | 0 | 0 | 1 | 0 | -16.7 | 3.4 | -16.7 |
| WNW | nasdaq | 2025-11-05 | 1 | 0 | 0 | 1 | 0 | -12.3 | 8.5 | -25.8 |
| WNW | nasdaq | 2025-11-14 | 1 | 0 | 0 | 1 | 0 | -30.2 | 3.2 | -37.5 |
| WNW | nasdaq | 2025-11-25 | 0 | 0 | 0 | 0 | 0 | 4.7 | 18.1 | -27.6 |
| WNW | nasdaq | 2025-12-04 | 1 | 0 | 0 | 1 | 0 | -5.9 | 0.0 | -13.0 |
| WNW | nasdaq | 2025-12-12 | 1 | 0 | 0 | 1 | 0 | -11.3 | 3.8 | -12.6 |
| WNW | nasdaq | 2025-12-22 | 1 | 0 | 1 | 0 | 0 | -3.1 | 6.6 | -18.8 |
| WNW | nasdaq | 2025-12-31 | 0 | 0 | 0 | 0 | 0 | 12.9 | 22.7 | -2.2 |
| WNW | nasdaq | 2026-01-12 | 1 | 0 | 1 | 0 | 0 | 5.0 | 11.9 | -16.2 |
| WNW | nasdaq | 2026-01-27 | 1 | 0 | 1 | 0 | 0 | -12.5 | 6.8 | -16.1 |
| WNW | nasdaq | 2026-02-09 | 1 | 0 | 1 | 0 | 0 | -4.0 | 5.3 | -8.3 |
| WNW | nasdaq | 2026-02-25 | 0 | 0 | 0 | 0 | 0 | 18.3 | 22.9 | -1.9 |
| WNW | nasdaq | 2026-03-06 | 0 | 0 | 0 | 0 | 0 | -91.2 | 700.0 | -94.0 |
| WNW | nasdaq | 2026-03-19 | 1 | 0 | 0 | 0 | 1 | -57.9 | 60.8 | -61.7 |
| WNW | nasdaq | 2026-03-31 | 2 | 0 | 0 | 1 | 1 | -61.0 | -16.7 | -61.5 |
| WNW | nasdaq | 2026-04-16 | 2 | 0 | 1 | 1 | 0 | -23.2 | 19.6 | -49.6 |
| WNW | nasdaq | 2026-04-27 | 3 | 1 | 1 | 1 | 0 | -3.6 | 71.7 | -8.7 |
| WNW | nasdaq | 2026-05-06 | 3 | 1 | 1 | 1 | 0 | 2.9 | 73.5 | -40.3 |
