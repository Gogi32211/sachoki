# QR_REL_V1 — Q∧R bar followed by the first ▲ Release

**Date** 2026-09-27 · plan approved by the user ("gaushvi") · frozen to `qr_plan.txt` before any outcome · k = 2.
**Verdict (long): NULL, stopped at the deciding test.** The combination is not a buy. It is **significantly worse** than an ordinary bar in every MINE year. That makes it a **veto candidate**, which needs its own clean test.

## The pattern
Two bars on the chart:
1. **Q∧R bar.** Price broke below the echo zone (BD▼/BDV▼), then came back inside the zone on low volume.
2. **▲1 Release on the next bar.** A green bar on volume above SMA20: the first release of the series.

Entry is at the open after the ▲ bar. The history holds 9,339 events over 1,231 days and 3,320 tickers (≥ $1, ≥ $2M).

## Setup
- **Exit.** Book path-sim: trail with atr_k 12, maxh 60, slip 0.15 %, 5-bar cooldown.
- **Ordinary-bar control.** `control_keys` phase bars on the same entry day. It averages **+0.70 %** per trade over 84,643 trades.
- **Reference control.** Q (no R) → ▲1: the same two-bar picture without the prior breakdown.
- **Cells.** (1) the combo as charted; (2) the fast-reclaim variant, with BD▼/BDV▼ ≤ 5 bars before the Q∧R bar.
- **Windows.** MINE 2021-23. VERIFY 2024-26 was **not opened**.

## Result — MINE 2021-2023

| cell | trades / days | mean · median trade | win % | Δ vs ordinary bars (pp) | years | Δ vs Q(no R)→▲1 |
|---|---|---|---|---|---|---|
| **Combo** | 3,737 / 560 | **−3.52 % · −3.57 %** | 43 | **−1.82** [−3.05, −0.62] | −2.2 / −2.1 / −1.4 | **−1.68** [−3.25, −0.12] |
| Fast reclaim | 953 / 376 | −4.45 % · −3.84 % | 41 | −1.84 [−3.86, +0.52] | −2.7 / −1.6 / −1.7 | −1.70 [−3.97, +0.91] |
| *reference* Q(no R)→▲1 | 2,875 | | | −0.15 [−1.35, +1.05] | | |

**Descriptive, user's 5/15 bracket:** combo +0.17, fast −0.33 vs ordinary. Both are ≈ 0; the bracket truncates the downside at −5 %.

## Reading
1. **Not a long entry.** The deciding test needed ≥ +0.5 pp. The combo is at −1.8 pp, so the long study stops as registered.
2. **The breakdown is what hurts.** The same Q → ▲1 picture without a prior breakdown is flat (−0.15). Adding the breakdown (R) makes it 1.7 pp worse, with a CI that excludes zero.
   - This is the **LPSY reading**, not the Spring reading. After a failed zone, a dry drift back inside followed by one volume up-bar is a rally into supply that tends to fail.
   - The fast-reclaim "Spring" variant is no better (−1.84).
3. **This fits the book.** Buy absorbed weakness *at* a level that holds. Do not buy a rally back into a level that already broke. It is the same knife logic as ABSP_BD_GATE_V1, where a BD▼ anchor also hurt.
4. **Veto candidate, not a veto yet.** Negative in all 3 MINE years with a CI clear of zero is exactly what a veto needs in MINE. The book's rule is still OOS first. **The next step would be a k = 1 pre-registered VETO test on VERIFY 2024-26**, which is untouched for this combo.

Artifacts: session scratchpad `vecho/qr_plan.txt`, `qr_rel.py`, `qr_rel.log`, `qr_rel_result.json`. No UI, score, or memory change.

---

## Addendum 2026-09-27 — the combo alone vs buying anywhere, VERIFY opened (QR_REL_VERIFY, k = 1)
The user asked for the combo alone, compared only with an unsignalled buy on the same day with the same exit. The bar was frozen to `qr_verify_plan.txt` before 2024-26 was opened.
- **Long works** if Δ ≥ +0.5 and CI lower > 0.
- **VETO confirmed** if Δ ≤ −0.5 and CI upper < 0.

| window · exit | combo trades / days | combo mean · median · win | random buy mean · win | Δ vs random buy (pp) | by year |
|---|---|---|---|---|---|
| MINE 2021-23 · book trail | 3,737 / 560 | −3.52 % · −3.57 % · 43 % | −1.69 % · 45 % | −1.82 [−3.05, −0.61] | −2.2 / −2.1 / −1.4 |
| **VERIFY 2024-26 · book trail** | 5,382 / 663 | +1.47 % · −1.26 % · 47 % | +2.59 % · 50 % | **−1.40 [−2.67, −0.05]** | −1.4 / −2.1 / −0.5 |
| MINE · 5/15 bracket | | −0.81 % · 24 % | −0.78 % · 25 % | +0.17 [−0.22, +0.58] | |
| VERIFY · 5/15 bracket | | −0.40 % · 27 % | −0.04 % · 30 % | −0.17 [−0.51, +0.18] | |

**Registered verdict: VETO CONFIRMED (book exit).** The combo underperforms a random buy in all 6 years, 2021-2026.
- **The VERIFY CI only just clears zero** (upper −0.05). The effect is real but modest, about −1.4 to −1.8 pp per trade.
- **Under the 5/15 bracket it is neutral.** A tight stop caps the damage, so the veto matters for trend-style (trailing) holding.
- **Use:** "do not buy the first volume up-bar after a dry return into a broken echo zone". Not applied to any UI or score. No memory written without the user's OK.
