# V4_HISTORY_V1 — does the user's V4 weight sum rank forward returns, and where does the lift start?

**Date** 2026-09-24 · **Verdict: NULL as a ranker (kit gate: WATCH, deciding gate L2)** · pre-registered plan approved by the user before any code (5 lines, n_trials=12).

## What was measured
- **Score**: the app's own V4 — `frontend/src/lib/v4Weights.js` as of 2026-09-24 (614 keys, **232 non-zero, every one = 5 → 1,160 points**), evaluated by the app's own `v4Score.js` over the real `V4_ALL_GROUPS` catalog (esbuild bundle of `UltraScanPanel.jsx` run in node) — not a re-typed copy.
- **Universe**: S&P 500 (620 tickers incl. past members), 1D, **2021-08-02 → 2026-08-25**, 749,474 ticker-sessions, close ≥ $5.
- **Outcome**: fwd_20d (close-to-close, recomputed from lead(close); stored column agreed to 0.016pp). Day-clustered bootstrap CIs (n_eff ≈ 8-10k of 749k rows — 1,272 sessions).
- **Historical inputs** (each declared): studio DB bars via Ultra's own `_UI_KEY_TO_DB_COL`/`_DB_TO_UI_COL_MAP`; the six display stores through their own `_to_ui()`; `anatomy_signals` (v/rs); `mtf_rev_signals` (rev_4h/rev_1h); pt5/pt9 (`_STRONG`, ≤ 2026-08-20 cutoff); EDGE codes from `edge_replay._frame(64 mo)` — the backtests' own masks; `fly_fresh` re-derived by the main.py rule; `ctx_*` from `context_tokens`.

## Deciding test 1 — coverage: PASS
**91.4 %** of weighted points reproducible (1,060 / 1,160). Not reproducible (20 keys × 5, all live/intraday attachments with no per-bar history): `rs_strong`, `rgti_upup/upupup/green/greencirc`, `mtf_up/upup/upupup`, `x_turn_echo_1/2/3`, `x_mtf_conf_1/2/3`, `x_rev_buy_echo`, `x_brk_buy`, `x_edge_premium_rev`, `x_div_buy`, `x_seq34_gold`, `x_whisper`.
⚠️ Several of these ARE the app's validated intraday-echo lifts (echo≥1 +0.7…+1.05, mtf_conf 1-3 +1.3…+1.6). Live V4 may carry ordering from them that this history cannot see. The 91 % that is measurable is what the verdict covers.

## Deciding test 2 — does V4 order the cross-section? KILLED
| ranker | fwd20 IC | ICIR | hit |
|---|---|---|---|
| **V4** | **+0.0040** | +0.047 | 52.8 % |
| n_weighted (plain count) | +0.0040 | +0.047 | 52.8 % |
| buy_score (incumbent) | −0.0045 | −0.030 | 46.4 % |
| ⚛ phys family alone | **+0.0131** | +0.129 | 56.3 % |
| shape family alone | −0.0075 | −0.103 | 50.1 % |
| placebo ×20 (random 232-key sets) | mean +0.0021 · p95 +0.0064 | | |

Kill bar was |IC| < 0.02 and ICIR < 0.3 — both fail by an order of magnitude. **V4 is arithmetically identical to a signal counter** (every weight is 5 → V4 = 5 × n_weighted; the IC lines are the same number), and a count of fired signals does not rank forward returns.

## Bands (the question asked) — flat
Matched baseline +0.49 % [+0.25, +0.70], 5/6 yr, worst −0.73.

| V4 band | n | median fwd20 | CI | yrs pos | worst yr | Δ base |
|---|---|---|---|---|---|---|
| 0-20 | 29,212 | +0.74 | [+0.36, +1.11] | 6/6 | +0.24 | +0.26 |
| 20-40 | 168,651 | +0.47 | [+0.22, +0.75] | 5/6 | −0.57 | −0.02 |
| 40-60 | 217,872 | +0.44 | [+0.22, +0.64] | 5/6 | −0.77 | −0.05 |
| 60-80 | 175,579 | +0.47 | [+0.23, +0.71] | 5/6 | −0.91 | −0.02 |
| 80-100 | 98,435 | +0.53 | [+0.27, +0.80] | 5/6 | −0.80 | +0.04 |
| 100-130 | 48,602 | +0.56 | [+0.29, +0.87] | 5/6 | −0.98 | +0.07 |
| 130+ | 11,123 | +0.74 | [+0.15, +1.33] | 4/6 | −0.53 | +0.25 |
| **≥45 (screen green)** | 495,739 | **+0.49** | [+0.26, +0.73] | 5/6 | −0.84 | **+0.00** |

No dose-response: 20→130 sits within ±0.07 of baseline. The two ends (0-20 and 130+) tick up +0.25 — the U is not a ranker; 130+ is 4/6 years (−0.29 in 2021, +1.75 in 2024 = regime), CI touches +0.15, n_eff 3,772. Both far below the pre-set L2 bar (+1.0). **The ≥45 green threshold is exactly baseline.** 2022 is negative in every band.

## Controls
- **A · plain count**: n_weighted quintiles +0.51 / +0.44 / +0.45 / +0.47 / +0.55 — same flat shape (it is the same number).
- **B · leave-one-family-out**: removing ⚛ phys collapses IC 0.0040 → 0.0002 — physics is the only family carrying any ordering; removing shape or combo *raises* IC (+0.002 each) — as weighted, those families are mildly anti-predictive.
- **C · placebo**: 20 random equal-sized 5-point key sets give a top-band lift of +0.34 (p5 +0.05, p95 +0.75) vs V4's +0.25 — **V4 beats 45 % of placebos**: the top-band tick is "many signals fired at once", not the user's selection.
- **Price bucket**: $21-89 beats "other" by ≈ +0.2 pp in every band (Fib price-zone law, already known) — the price law is doing that, not V4.
- DSR of the best cell (0-20) = 1.00 against 28 variants — it is real, and it is +0.26.

## Answer to "from which V4 range does the rise start?"
**None.** On 2021-2026 S&P 500 history the user's V4 does not order forward returns anywhere along its axis; the green ≥45 cut is the population median. The only structure inside it is the ⚛ physics family (IC +0.013, still tiny) and the known price-zone law.

## What would change this (not done — would be mining against the same history)
1. Weights that differ — today every key is 5, so "weights" are a count. Any re-weighting chosen from this table must be mined on 2021-23 and frozen before scoring on 2024-26, with the trial count carried into DSR.
2. The 8.6 % of points not measurable here are the validated intraday-echo/turn/mtf_conf families — if live V4 has an edge, it most likely lives there, and it needs its own per-bar history to test.

Artifacts: `scratchpad/v4hist/{deps.json, field_sources.json, meta.json, study.log, study_result.json}` (session scratchpad); runner: `runner.mjs` + `build_rows.py` + `study.py`.
