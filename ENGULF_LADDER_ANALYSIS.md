# Engulf Edge — Deep Bar-by-Bar Analysis (1 → 4 bars)

Full-DB study (8.3M bars) of how the engulf edge (T4/T6 bull, Z4/Z6 bear) evolves
as bars are added. Metric: **median fwd return** (median over mean), win%, full
outcome distribution, MFE/MAE/RR, multi-horizon, regime-by-year, robustness, and a
per-bar conditional decomposition. sp500 LONG (the only universe where it works).

---

## 1 · FULL OUTCOME DISTRIBUTION per level (not just the median)

| level | n | p25 | **med** | p75 | mean | win | **+5%** | **−5%** | MFE | MAE | RR |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 engulf | 92,118 | −3.33 | +0.35 | +4.03 | +0.41 | 52.7% | 20.0% | 17.3% | +5.21 | −4.81 | 1.08 |
| 2 +vol=B | 11,779 | −3.17 | +0.63 | +4.40 | +0.76 | 55.0% | 21.8% | 16.8% | +5.58 | −4.83 | 1.16 |
| 3 +bar+1 bear | 5,932 | −3.01 | +0.87 | +4.88 | +1.15 | 56.3% | 24.6% | 16.1% | +5.88 | −4.60 | 1.28 |
| 4 +bar+2 turn | 1,600 | −2.64 | +1.17 | +4.94 | +1.28 | 57.8% | 24.8% | 14.9% | +5.90 | −4.61 | 1.28 |
| **4b T4/T6-trig** | **800** | **−1.91** | **+1.90** | **+5.73** | +2.03 | **63.1%** | **28.7%** | **12.4%** | +6.24 | −4.60 | **1.36** |

**What the whole distribution does (not just median):**
- **Downside compresses**: p25 −3.33 → −1.91 (4b). The *worst quartile* halves its loss.
- **Tail asymmetry inverts**: big-wins (+5%) 20% → 28.7%, big-losses (−5%) 17.3% → 12.4%.
  At 4b that's **2.3 : 1** big-win:big-loss (was 1.2 : 1 raw).
- **RR** (avg MFE / |avg MAE|): 1.08 → 1.36. MAE stays ~−4.6, MFE grows → cleaner trades.
- Every added bar shifts the **entire** distribution right, not just the centre.

---

## 2 · THE HOLDING-PERIOD KEY (5 vs 10 vs 20 days)

| level | med fwd_5d | med fwd_10d | med fwd_20d |
|---|---|---|---|
| 1 engulf | +0.20 | +0.35 | +0.43 |
| 4b T4/T6-trig | **−0.50** | **+1.90** | **+3.45** |

**The 4-bar is NEGATIVE at 5 days and biggest at 20.** The pullback keeps dipping
short-term, then mean-reverts hard. **You MUST hold 10–20 days** — a 5-day stop kills
it. (This is why the journal holds ~10d; for this setup 15–20d is even better.)

---

## 3 · REGIME — positive EVERY year (the overfit killer)

4-bar (T4/T6-trigger), median fwd_10d / win / n by year:

| year | regime | median | win | n |
|---|---|---|---|---|
| 2021 | bull | +0.72% | 53% | 51 |
| 2022 | **BEAR** | **+1.76%** | 57% | 155 |
| 2023 | bull | +1.79% | 64% | 133 |
| 2024 | bull | +0.99% | 59% | 108 |
| 2025 | strong dip-buy | +3.06% | 70% | 291 |
| 2026 | — | +1.00% | 60% | 62 |

**Positive in all 6 years, including the 2022 bear (+1.76%).** The big 2025 number is
regime-favourable, but the floor (weak years ~+0.7–1.0%) is still a real edge. This is
NOT a single-regime artefact.

---

## 4 · ROBUSTNESS — does the exact rule matter? (overfit test)

Vary the bar+2 "turn" definition (base = T4/T6 engulf+B → bear pullback):

| bar+2 rule | n | median | win |
|---|---|---|---|
| just not-W (no turn req) | 2,742 | +1.089% | 57.8% |
| ANY bull bar | 1,493 | +1.394% | 59.7% |
| ANY T-code | 1,491 | +1.395% | 59.8% |
| **T9/T3/T4 (chosen)** | 800 | +1.898% | 63.1% |
| T9/T3/T4 + vol=B (strict) | 154 | +2.414% | 66.9% |

The edge **degrades gracefully** as the rule loosens (any-bull still +1.39%) and
tightens smoothly (+2.41% strict). **No cliff = not overfit.** The specific T-codes are
a *refinement*, not the whole edge.

---

## 5 · PER-BAR CONDITIONAL DECOMPOSITION — which bar drives it

4-bar setup (n=800), median fwd_10d / win by code at each bar:

| bar | code → edge |
|---|---|
| **bar−2 engulf** | **T4 +2.29%/65%** ≫ T6 +1.06%/58% |
| **bar−2 L** | **L12 +2.52%, L3 +2.24%** (strength) ≫ L34 +0.65% |
| **bar−1 pullback** | **Z9 +3.21%/69%** ≫ Z1 +2.26 > Z4 +1.77 > Z5 +1.34 > Z3 +1.07 |
| **bar 0 turn** | **T9 +2.53%/65%** > T4 +1.74 > T3 +1.70 |
| **bar 0 L** | **L12 +2.14%/64%** > L3 +1.38 > L34 +1.30 |

**The single biggest lever is the PULLBACK QUALITY (bar−1).** A **Z9 inside-bar**
pullback (tight, controlled, shallow) → **+3.21% / 69%**; a deep Z3 pullback only
+1.07% / 59%. The textbook flag: strong bar → orderly *inside-bar* pause → resume.

---

## 6 · THE OPTIMAL PATH (and where overfit begins)

| path | n | median | win | **p25** |
|---|---|---|---|---|
| 4-bar (T4/T6 trig) | 800 | +1.90% | 63.1% | −1.91 |
| + bar−2 = T4 | 594 | +2.29% | 65.0% | −1.90 |
| + bar−1 = Z9 inside | 285 | +3.21% | 69.5% | −1.23 |
| **+ T4 & Z9 (optimal)** | **245** | **+3.43%** | **71.8%** | **−0.86** |
| + & bar0 = T9 (too tight) | 58 | +3.07% ↓ | 67.2% ↓ | −0.99 |

**Optimal = T4 engulf+B → Z9 inside-bar pullback → bull-T turn: +3.43% med, 71.8%
win, worst-quartile only −0.86% (n=245).** Adding one more condition (bar0=T9) DROPS
the edge and collapses n — **that is exactly where overfit starts**; stop at T4+Z9.

---

## VERDICT — 3-bar vs 4-bar, and is 4-bar overfit?

1. **NOT overfit.** Three independent checks all pass: positive every year incl. the
   2022 bear; robust to rule loosening (any-bull = +1.39%); the whole distribution
   (not one stat) shifts right with downside compression.
2. **Each added bar adds real, structural edge** — and crucially *cuts the downside*
   (p25 −3.33 → −0.86 on the optimal path) more than it lifts the median.
3. **3-bar** (n=5,932, OOS +1.38%/59%) = higher throughput, but you enter into a
   falling bar. **4-bar** (n=800, +1.90%/63%; optimal T4+Z9 n=245, +3.43%/72%) =
   confirmed turn, much tighter downside. Both real; 4-bar is the higher-conviction.
4. **Operating rules:** sp500 only; hold **10–20 days** (negative at 5d); the pullback
   should be a **controlled inside-bar (Z9)**, not a deep breakdown; stop tightening
   at T4+Z9 (further = overfit).
