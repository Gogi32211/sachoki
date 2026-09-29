# RAD_STREAK_V1 — ⚛ RA·D repeated on consecutive bars

**Date** 2026-09-28 · user: "check RA·D when it repeats on several consecutive bars" · plan frozen to `rad_plan.txt` before any outcome · k = 2.
**Verdict: 2/2 FAIL as a buy / reversal signal.** RA·D streaks sit around bottoms (×1.5 near true pivots) but carry no forward reversal information. **Longer streaks are progressively worse entries.** That is a veto *candidate*, not a finding yet.

## Frequency
- RA·D is on 12.5 % of eligible bars.
- Streaks of ≥ 2 bars cover 3.3 % of bars, ≥ 3 cover 0.9 % and ≥ 4 cover 0.25 %.
- Near a true 21-bar pivot (±3 bars, hindsight), each is **×1.5** its overall rate: streak ≥ 1 ×1.54, ≥ 2 ×1.53, ≥ 3 ×1.46.

## Forward results
REV lift is adjusted for location × ATR% × bars-since-low. Same-day return Δ is vs the day's control, ATR×12 trail.

| all eligible bars | MINE: REV lift · same-day | VERIFY: REV lift · same-day [CI] · years |
|---|---|---|
| streak = 1 | 1.02 · +0.10 | 1.00 · −0.29 |
| streak ≥ 2 | 1.00 · −0.12 | 1.00 · **−0.84** [−1.26, −0.40] |
| streak ≥ 3 | 0.98 · −0.33 | 0.94 · **−1.59** [−2.44, −0.77] |
| streak ≥ 4 | 0.88 · −0.77 [−2.33, +0.79] | 0.87 · **−4.22** [−5.73, −2.67] · −2.6 / −5.5 / −4.6 |

**Bottom zone only (descriptive):** same pattern. In VERIFY:

| bottom zone, VERIFY | same-day Δ |
|---|---|
| streak ≥ 2 | −1.09 |
| streak ≥ 3 | −2.09 |
| streak ≥ 4 | −4.84 |

## Reading
- **RA·D repeating describes a bottom being built.** It is common around lows. It does not tell whether the low will hold.
- **As an entry it is monotone in the wrong direction.** The longer the RA·D streak, the worse the next 60 bars versus the day's other bars.
  - 2024-26 is clear: −0.8 → −1.6 → −4.2 pp, with ≥ 4 negative in all three years.
  - 2021-23 has the same sign but is not significant (−0.1 → −0.3 → −0.8).
- **Practical reading:** a run of RA·D means the pressing phase is still on. Wait for it to end rather than buying into it.
- **To use it as a VETO**, the direction must be confirmed on its own. The honest next step is a forward registration (the 2021-23 evidence is too weak), for example "RA·D streak ≥ 3 → same-day Δ < 0" read in 2027.

Artifacts (session scratchpad `v4hist/`): `rad_plan.txt`, `rad.py`, `rad.log`. No UI, score or memory change.
