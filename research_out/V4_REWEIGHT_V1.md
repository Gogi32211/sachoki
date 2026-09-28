# V4_REWEIGHT_V1 — can any weighting of the V4 catalog rank S&P stocks?

**Date** 2026-09-27 · plan approved by the user ("ki") · frozen to `reweight_plan.txt` before any outcome.
**Verdict: STOP at the deciding test. No weighting — hand-made, univariate, ridge, lasso or LightGBM — ranks S&P stocks for the next 20 days.** VERIFY 2024-26 was not opened. V4 is unchanged.

## Setup
- **Features.** 472 usable catalog keys (see V4_REWEIGHT_CENSUS.md), evaluated by the app's own catalog code on 792,983 S&P 500 ticker-days.
- **Target.** close[t+20] / open[t+1] − 1, minus the same-day S&P median. Models fit its daily percentile rank.
- **Validation.** MINE 2021-05 … 2023-12, expanding walk-forward, 8 test quarters (2022Q1 … 2023Q4), with a 20-trading-day purge before each test quarter.
- **Metrics.** Daily cross-sectional rank IC, and the mean excess of the top decile by score.

## Result — MINE walk-forward (the deciding test needed a best CV IC ≥ 0.02)

| candidate | CV IC | ICIR | top-decile excess | IC 2022 / 2023 |
|---|---|---|---|---|
| (a) the user's current V4 | **+0.0007** | +0.007 | +0.19 pp | +0.006 / −0.005 |
| (b) univariate shrunk lift | −0.0130 | −0.103 | −0.22 pp | +0.003 / −0.029 |
| (c) ridge α 1e3 / 1e4 / 1e5 | −0.010 / −0.009 / −0.011 | ≈ −0.09 | −0.07 … −0.21 pp | ≈ +0.01 / −0.03 |
| (c) lasso α 1e-5 / 3e-5 / 1e-4 | −0.011 / −0.012 / −0.013 | ≈ −0.13 | −0.05 … −0.12 pp | ≈ +0.003 / −0.026 |
| (d) LightGBM (upper bound) | −0.0167 | −0.199 | −0.10 pp | −0.008 / −0.025 |

The best is the user's own map at +0.0007, **30 times below the 0.02 bar**. The planned integer conversion of the best linear model was not run, because no linear model came close.

## Reading
1. **The catalog carries no stable cross-sectional ranking information at 20 days on the S&P 500.** A hand map, a per-signal average, a regularized joint model and a non-linear model all land at IC ≈ 0.
2. **Every model that learned from the past did worse than no learning.** Each one fit 2021-22 and then pointed the wrong way in 2023 (IC −0.025 … −0.032). Whatever relation a signal has with the next 20 days changes sign with the regime. Learning it faithfully hurts, and more flexibility hurts more (LightGBM worst).
3. **The user's all-5 map is "least bad" because it learns nothing.** It cannot overfit a regime, and it scores ≈ 0.
4. **What this does not say.** It does not say that no signal works in any context. Single conditioned setups can still be real, e.g. the QR_REL_V1 veto. It says that **a single additive score over the whole catalog is not a ranker**, however the weights are chosen.

## Options for the user (none applied)
- **A.** Keep V4 as a *descriptive* "how much is happening on this bar" count, and stop reading it as a buy-strength rank.
- **B.** Redefine the target. Test whether the catalog separates the *bottom* (what to avoid) rather than the top. A veto is where this session's only confirmed result came from.
- **C.** Condition first, score second. Score only inside one state (e.g. an oversold or pullback regime), where relations may be stable. This is a new family with its own k.

Artifacts (session scratchpad `v4hist/`): `reweight_plan.txt`, `reweight.py`, `reweight_mine.log`, `reweight_mine.json`, `user_weights.json`. No UI, score, weight or memory change.
