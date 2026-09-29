# TZ_BOOSTERS_V1 — which same-bar signals strengthen / weaken each T/Z signal

**Date** 2026-09-29 · plan user-approved ("ki") and frozen to `tzboost_plan.txt` before any outcome.
**Design**
- **Anchors and candidates.** 25 T/Z states × 638 catalog keys on the SAME bar.
- **Estimand.** Paired same-day contrast within the anchor: ret(TZ∧X) − ret(TZ∧¬X).
  - Returns are the book path-sim: entry open[t+1], trail clip(12·ATR%, 15, 60)%, maxh 60, 15 bps.
  - Returns are per-bar, with no cooldown.
- **Selection.** Made on MINE 2021-23, then read once on VERIFY 2024-26.

## VERDICT: pre-registered result = many PASSes → after the magnitude AUDIT, mostly **one volatility confound**. Only 🕐DR on T6 is a clean booster; the bear side = the known extreme-volume/volatility family.

- **MINE search.** k = **7,430 cells** (n ≥ 200, ≥ 80 paired days).
  - 1,883 have |t| ≥ 1.96, against about 372 expected by chance. The structure is real, but see the audit below.
- **Pre-registered VERIFY.** BULL 22/40 PASS; BEAR 37/40 PASS.

## ⚠️ Magnitude audit (memory feedback-audit-magnitude-first)
The raw effects were +5…+10 pp (bull) and −10…−32 pp (bear) per trade. That is 3-10× the book's best edges, so they were audited before being believed.

1. **The trail width is 12·ATR%.** The exit rule itself depends on volatility, so a contrast that does not match ATR% compares different trades.
   - BEAR X-bars run at ATR% **8-27%**, against 3.3% for ¬X. That is the top 2-0.5% of the ATR distribution (95th pct 8.9%, 99.5th 21%).
2. **Re-stratified within day × ATR% bin** (20 bins + the 98 / 99.5 % tail).

| side | selected | same sign in BOTH windows after fine-ATR matching | typical size after matching |
|---|---|---|---|
| BULL | 40 | **15** | ≈ 0 … +2 pp; medians ≤ +1-3 |
| BEAR | 40 | **27** | −1 … −6 pp (from −10…−30 raw) |

## What survives
**BULL.**
- The list is dominated by **P<50 / P<89**, i.e. the T/Z bar sits *below* EMA50/89. This is the pullback regime, the known law "PULLBACK wins, strength-chase loses", not a T/Z-specific booster.
  - After ATR matching it is ≈ 0 to +2, and it flips in several states.
  - MINE medians are often negative, so the effect lives in the tail.
- **🕐DR (1H-confirmed bottom) on T6** is the one clean booster:

| measure | MINE | VERIFY |
|---|---|---|
| raw Δ | +3.37 | +5.70 |
| fine-ATR Δ | +1.93 | +5.23 |
| median | +4.70 | +3.21 |
| win % | +11.3 | +9.5 |

  n 463 / 422. It is consistent with the 🕐DR family everywhere else in the book.
- **FLY BD / CD on T2/T2G/T4/T6.** Mostly +1-3 after matching, weak and unstable in the MINE medians. This is WATCH at most.

**BEAR** (the long trade is weaker). This is the extreme-volume / extreme-volatility family, universal across T/Z states rather than state-specific:
- V×5 / V×10 volume spikes;
- VOL7 M6 (≥5× median), MR+, M0·σ2 / M1·σ3;
- ⚛ ACC-TR;
- WSH washout on T5/T9 (−2.4…−6.3 after matching, both windows).

After matching it is typically −2…−6 pp. It confirms LADDER_V1 (M6 VETO), ACC_TR (−9…−16 raw) and VB volume. It is a **veto family, not a T/Z-specific finding**. Per the user, nothing is being banned now.

## Weaknesses (flagged, not hidden)
- The selection ranked by raw magnitude. This favoured broad regimes (P<50) and high-variance small-n cells, so the list is not "per T/Z" as intended.
- ⚛ ACC-TR appears twice (`_ph_acc` ≡ `⚛w:ACC-TR`).
- Per-bar returns overlap across consecutive bars of a ticker. The day-clustered CI does not cover ticker serial correlation.
- **Methodological consequence.** Earlier within-T/Z contrasts did not match ATR%: TZPKG Stage 3 Z7 and the forward rules B5-B7 (T5L46ED / Z2GL46ED / Z9L46NUR). They should be re-checked with day × ATR% matching before the 2027-03-01 read.

Artifacts (scratchpad `v4hist/`): `tzboost_plan.txt`, `tzboost.py`, `tzboost.log`, `tzboost_result.json`, `tzboost_mine.parquet`, `tzboost_audit*.py/json/log`. No build, no memory entry.

---
## AMENDMENT_1 — per-T/Z selection with day × ATR% matching (user "ki", frozen before outcome)

**Changes from the pre-registered run**
- **Contrast.** Taken within strata of day × ATR% bin (22 bins incl. the 98 / 99.5 % tail).
- **Candidates.** EMA-location regime keys (P<20/50/89/200, P>…) removed; duplicates removed, leaving **484 candidates**.
- **Selection.** Per T/Z state, top 5 ↑ / 5 ↓ by |t| on MINE, then VERIFY once.

**Search accounting**
- **MINE:** k = **6,143** cells; |t| ≥ 1.96 in 556 (≈ 307 by chance).
- **VERIFY:** k = **229**.
- **PASS:** 20 (9 ↑ · 11 ↓), against roughly 3-6 expected by chance. There is real structure, but each single item is fragile.

### PASS (VERIFY Δ pp, day-bootstrap 95 % CI)

| T/Z | ↑ strengthens (bull) | ↓ weakens the long |
|---|---|---|
| T2 | ⚛ MARKUP +2.05 [1.27, 2.88] | 260308 −2.87 · V×5 −8.52 |
| T4 | — | P2 −1.39 [−2.74, −0.09] |
| T5 | **🕐DR +3.30 [0.85, 5.88]** | — |
| T6 | **🕐DR +8.13 [4.76, 11.61]** · ⚛ MARKUP +2.57 [1.20, 4.13] | ⚛ MKDN −2.58 [−4.22, −1.07] |
| T10 | — | 🔁den −2.85 |
| T1G | — | 260308 −3.41 |
| T2G | — | ⚛ MKDN −2.13 [−3.01, −1.27] |
| Z1 | ⚛ MARKUP +1.80 [0.28, 3.28] | — |
| Z2 | 🔻 structural bottom +1.14 [0.09, 2.15] | RF·D −1.86 |
| Z6 | BE +1.42 [0.06, 2.86] | — |
| Z9 | CON +1.84 [0.08, 3.67] | K1U −1.59 |
| Z1G | — | D50 −2.09 · D2 −2.09 |

**Coherent families**
- **🕐DR** on T5/T6 is the strongest, in line with the rest of the book.
- **⚛ Wyckoff phase:** MARKUP strengthens (T2, T6, Z1; T3 / T1G just miss) and MKDN weakens (T6, T2G; T5 just misses). This is the same axis in two directions.
- **Extreme volume** (V×5) weakens, as in V1.

**Did not replicate:** most MINE picks, e.g. ▲4H REV-trigger, RSI≤35, CD·30/60 and 🌀 on several states. RSI≤35 flips negative on T2 VERIFY (the knife law again).

### RE-CHECK of earlier within-T/Z claims with ATR matching (descriptive)

| claim | MINE | VERIFY | reading |
|---|---|---|---|
| B5 T5·L46·ED within T5 | +0.77 [−0.51, 2.14] | +0.16 [−1.44, 1.64] | ≈ 0: **the TZPKG increment does not survive ATR matching** |
| B6 Z2G·L46·ED within Z2G | +0.20 | −0.35 | ≈ 0 |
| B7 Z9·L46·NUR within Z9 | +0.56 | +0.59 | ≈ 0, wide |
| **Z7 within all Z bars** | **−1.28 [−1.85, −0.72]** | **−1.07 [−1.60, −0.55]** | **holds**; 5/6 yrs negative |
| B3 🔻💪 within T9 | +0.63 | +0.98 | positive 5/5 yrs, CI crosses 0 |
| 🕐DR within T6 | +3.68 [1.14, 6.12] | +8.13 [4.41, 11.72] | strong |

**Consequence.** Forward rules B5-B7 were registered from contrasts that were not ATR-matched, and are now expected to FAIL on 2027-03-01. The registration is not changed (no re-tuning); this note is recorded before any forward data.
