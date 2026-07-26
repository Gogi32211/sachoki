# Combo × acc_tr(TEST) factorial — keep the combo or drop it?

_combo = `flip→T1 + −1:l5=PS` ≙ (t_sig=='T1') AND (prev bar_line5=='PS'), the exact screener encoding. Fixed: r2k+nas · TEST bar · $-vol≥$500k · next-open · −15%/+100% · gap-aware · glitch-screened · OOS (ex PAVS/WNW/GLOO). Percent units. No production code._

---

# VERDICT — DROP the combo. Deploy acc_tr(TEST) standalone.

**The combo is not additive, not a booster, and not even neutral — it is antagonistic and over-restricting. It fails the hard n-check and strips out the exact explosive-tail bars that are acc_tr's entire value.**

### The three decisive reads
1. **They are near-orthogonal selectors, not stackable.** Only **1.4–1.5%** of acc_tr(TEST) bars also satisfy the combo; only **4.2–6.5%** of combo bars are acc_tr. Stacking two almost-disjoint filters mechanically collapses the intersection.
2. **BOTH cell n = 17 (r2k) / 19 (nasdaq) — below 30. Per the hard rule, that is itself the answer: the combo over-restricts.** And the (under-powered) BOTH cell is strongly **negative (−4.87 / −4.62)** with **P(+100%) = 0** and **mean winner collapsed to 9–12** vs acc-only's **24** — i.e. adding the combo *removes* acc_tr's tail.
3. **The combo carries no standalone edge — it is ≈ baseline, and of the wrong shape.** combo-only EXP **0.17 / 0.36** ≈ baseline **0.06 / −0.05**; its **P(+50%) 0.2–0.3%, P(+100%) ~0%, mean winner ~8–9** are *baseline-grade*. The combo selects a **higher win% (47%) with tiny winners** — a mild mean-reversion filter — the **opposite** of acc_tr's lose-often/win-huge tail (acc-only P(+100%) 3.2–3.3%, mean winner ~24). It dilutes exactly the property worth keeping.

### Interaction read
**Redundant-and-antagonistic + over-restricting.** The combo neither adds tail (combo-only ≈ baseline) nor survives intersection (BOTH n<30 and negative). There is no horizon (10d or 20d) or universe where BOTH > acc-only.

### Decision
- **DROP `flip→T1 + −1:l5=PS`.** It is decorative at best and tail-destroying at worst.
- **Deploy `acc_tr(TEST)` standalone** (the ALPHA_AND_REGIME-validated selector): r2k/nasdaq · acc_tr · close_pos<0.5 · <50% above 20d-low · entry-day $-vol≥$500k · glitch-screened — as a discretionary explosive-candidate scanner flag.
- Not "optional booster" — an optional booster must be ≥ neutral on the tail; this one zeroes P(+100%) and collapses n. No production code changed.

---

_Step 0 confirmation: the combo fires on PAVS (12), WNW (3), GLOO (2) bars — replication verified._

## Overlap (within TEST bars, $-vol≥$500k)

| universe | TEST n | acc_tr n | combo n | BOTH n | combo⊂acc% | acc⊂combo% |
|---|---|---|---|---|---|---|
| russell2k | 73022 | 1917 | 619 | 26 | 1.4 | 4.2 |
| nasdaq | 45587 | 1864 | 428 | 28 | 1.5 | 6.5 |

## 2×2 factorial — horizon 10d (OOS)


**russell2k**

| cell | n | EXPECTANCY | med | win% | P(+50%) | P(+100%) | mean win | payoff |
|---|---|---|---|---|---|---|---|---|
| (3) BOTH (combo & acc_tr) | 17 | **-4.87** | -4.36 | 29.4 | 0.0 | 0.0 | 9.0 | 0.78 |
| (2) acc_tr only | 1504 | **0.62** | -6.37 | 35.0 | 3.8 | 3.2 | 23.8 | 1.99 |
| (1) combo only | 566 | **0.17** | -0.29 | 47.5 | 0.2 | 0.0 | 8.2 | 1.14 |
| (4) neither (baseline, sampled) | 24045 | **0.06** | -0.11 | 49.1 | 0.3 | 0.1 | 7.2 | 1.04 |

**nasdaq**

| cell | n | EXPECTANCY | med | win% | P(+50%) | P(+100%) | mean win | payoff |
|---|---|---|---|---|---|---|---|---|
| (3) BOTH (combo & acc_tr) | 19 | **-4.62** | -15.0 | 31.6 | 0.0 | 0.0 | 11.6 | 0.88 |
| (2) acc_tr only | 1426 | **-0.83** | -10.41 | 32.5 | 4.1 | 3.3 | 23.7 | 1.85 |
| (1) combo only | 378 | **0.36** | -0.34 | 47.6 | 0.3 | 0.0 | 8.9 | 1.2 |
| (4) neither (baseline, sampled) | 23501 | **-0.05** | -0.49 | 47.2 | 0.6 | 0.2 | 8.5 | 1.09 |

## 2×2 factorial — horizon 20d (OOS)


**russell2k**

| cell | n | EXPECTANCY | med | win% | P(+50%) | P(+100%) | mean win | payoff |
|---|---|---|---|---|---|---|---|---|
| (3) BOTH (combo & acc_tr) | 17 | **-7.66** | -15.0 | 23.5 | 0.0 | 0.0 | 11.6 | 0.85 |
| (2) acc_tr only | 1503 | **0.79** | -15.0 | 31.9 | 6.0 | 4.5 | 31.8 | 2.3 |
| (1) combo only | 566 | **0.25** | -0.46 | 48.4 | 0.5 | 0.4 | 10.5 | 1.12 |
| (4) neither (baseline, sampled) | 24045 | **0.28** | -0.6 | 47.5 | 0.8 | 0.3 | 10.6 | 1.16 |

**nasdaq**

| cell | n | EXPECTANCY | med | win% | P(+50%) | P(+100%) | mean win | payoff |
|---|---|---|---|---|---|---|---|---|
| (3) BOTH (combo & acc_tr) | 19 | **-6.41** | -15.0 | 26.3 | 0.0 | 0.0 | 13.9 | 1.02 |
| (2) acc_tr only | 1424 | **-0.93** | -15.0 | 28.7 | 6.1 | 4.6 | 32.2 | 2.24 |
| (1) combo only | 378 | **-0.15** | -1.09 | 46.8 | 1.1 | 0.5 | 11.4 | 1.1 |
| (4) neither (baseline, sampled) | 23499 | **0.08** | -1.45 | 44.7 | 1.4 | 0.6 | 12.6 | 1.25 |

## BOTH vs acc_tr-only — is any lift beyond noise? (h10, bootstrap)

| universe | acc-only EXP (n) | BOTH EXP (n) | Δ | bootstrap p(Δ>0) | verdict |
|---|---|---|---|---|---|
| russell2k | 0.62 (n1504) | -4.87 (n17) | — | — | ⚠ BOTH n<30 — n IS the answer (over-restricts) |
| nasdaq | -0.83 (n1426) | -4.62 (n19) | — | — | ⚠ BOTH n<30 — n IS the answer (over-restricts) |
