# RTV_CONTRIBUTION_V1 — does `turbo_engine.py:390`'s unconditional `rtv +3` earn its place?

**Verdict: NO CONTRIBUTION DEMONSTRATED. Removal candidate, not a production removal.**
**No production change was made in this study.** Sealed 2026-09-13.

## The question

```python
# backend/turbo_engine.py:390, inside the Combo / momentum family (cap 14)
if r.get("rtv"):      combo += 3
```

`rtv` receives an unconditional +3 in the served TURBO score. The neighbouring `bb_brk` bonus was
removed on 2026-07-29 once it measured negative (median −1.55 vs a −0.63 baseline, 6 yr, n = 18,514),
so the precedent for measuring a member of this family already exists. `rtv`'s +3 had never been
measured. Meanwhile [[project-rtv-v1]] sealed the RTV family at **4/4 NULL**.

This study asks only the production question: **does that +3 improve the ordering of the same
fires?** It is not new signal research.

## Frozen spec (user-approved before the run, `k = 1`)

| | |
|---|---|
| estimand | same fires, same stored score, only RTV's effective **capped** contribution removed |
| harness | **SCORE_AUDIT_V1 unchanged** — its sealed `X.parquet` sample and its sacred-`_pathsim` `direct_trades.parquet` are *reused, not rebuilt*, so both arms stand on identical rows and identical outcomes |
| treatment authority | **`bars.rtv`** — the engine boolean production actually scores on (`turbo_engine.py:1016` → `_sig(compute_combo(df), "rtv")`). `combo_sig` is a data-quality cross-check **only** |
| delta | `delta_rtv = min(combo,14) − min(combo−3,14)`, `buy_2809` recovered and validated (below) |
| primary | paired WITH vs WITHOUT over the same days, bootstrap **by date** |
| sensitivity | concordant-positive **lower bound**: remove the contribution only where `bars.rtv = 1` **and** the token agrees. Discordant boolean-only rows **keep** their +3 — their stored score really does contain it |
| decision | uplift → keep · null/mixed → unchanged · degradation / no contribution → removal candidate · primary and sensitivity disagreeing → DATA_QUALITY_BOUND, no removal |
| excluded | **no post-hoc weight search** (`+1/+2/+4`). RTV-alone / RTV×RS / liquidity / concentration are diagnostics only and decide nothing. |

## Audit before the run

**The cap does not bind.** `s += min(combo, 14)` raised the worry that the +3 is often inert. It is
not: with `buy_2809` recovered, `delta_rtv` is the full **+3 on 99.48 %** of the 332,733 `rtv` fires
in `bars` (+2 on 0.21 %, +0 on 0.30 %).

**The +3 is not cosmetic.** `turbo_score` is compressed — mean 17.7, median 12, sd 16.4 — so 3 points
is **0.183 SD**. Measured over 2026: removing it drops a name past a **median of 227 competitors** on
a median field of 5,226, about **4.3 % of the day's field**.

**A STOP fired, and was resolved by recovery rather than assumption.** The pre-run safeguard required
three counts. On the first pass `buy_2809` was not stored in `bars`, and because `rocket` almost never
co-fires with `rtv` (768 rows), it decided the cap on nearly every row:

```
rtv = 1 total            332,733
exact-delta rows           2,908   ( 0.87 %)
buy_2809-ambiguous       329,825   (99.13 %)      → STOP. Primary was not run.
```

`bars.combo_sig` turned out to be the raw COMBO token string, in the same table over the full span,
and `ultra_signal_parser.py:242` defines `buy_2809 = "BUY" in tok_Combo`. The recovery was validated
against an **independent producer** (`data/combo_tokens.parquet`):

| BUY: `combo_sig` vs the token store | |
|---|---|
| overlapping rows | 804,194 |
| agreeing | 15,684 |
| disagreeing | **36 (0.004 %)** |

```
exact-delta rows         331,062   (99.50 %)
buy_2809-ambiguous         1,671   ( 0.50 %)      → proceed
```

**A false alarm, retracted.** Mid-audit `sig_ca/sig_cd/sig_cw` appeared to be a wrong mapping because
`combo_sig` never contains CA/CD/CW. The mapping is correct — `main.py:5541` shows
`"sig_cd": int(sig_row["cd"])`. `combo_sig` simply does not emit those tokens, so absence in the text
proves nothing about the boolean. The verification method was wrong, not the mapping.

**Treatment-variable disagreement.** Across all of `bars`, `rtv` and the `RTV` token disagree on
24,788 rows (7.11 % of their union), while `rocket`, `hilo_buy` and `sig_3g` agree 100 %. That is why
the concordant sensitivity exists. Side note: `atr_brk` never fires anywhere in `bars`.

## Mandated run-report counts (sealed sample, 206,503 rows)

```
bars.rtv positive                   8,246
RTV token positive                  8,228
concordant positive                 8,198
discordant boolean-only                48
discordant token-only                  30
combo_sig absent (buy unknown)     24,861
delta_rtv on treated rows   {+3: 8,200, +2: 22, +1: 1, +0: 23}
```

**Self-check:** the percentile reconstruction reproduces the sealed `pct_turbo_score` column exactly
(max |Δ| = 0). Scored trades 206,307 over 1,253 entry days.

## Primary — paired, `WITHOUT − WITH` (positive ⇒ removing the +3 helps)

| window | days | selection differs | mean diff | 95 % CI by date |
|---|---|---|---|---|
| MINE | 834 | 215 (25.8 %) | **+0.0285** | **[+0.0048, +0.0538]** — excludes 0 |
| VERIFY | 419 | 134 (32.0 %) | **+0.0274** | **[−0.0131, +0.0699]** — includes 0 |

Arm levels (day-median of the demeaned edge over the top quintile):

| arm | MINE median / day-win | VERIFY median / day-win |
|---|---|---|
| WITH `rtv +3` | 0.0000 / 47.5 % | 0.0000 / 49.9 % |
| WITHOUT (primary) | 0.0000 / 48.7 % | +0.0226 / 50.1 % |
| WITHOUT (concordant) | 0.0000 / 48.7 % | +0.0226 / 50.1 % |

**Sensitivity** (concordant lower bound): **+0.0287** [+0.0036, +0.0545] MINE · **+0.0274**
[−0.0116, +0.0679] VERIFY — practically identical to the primary. The data-quality disagreement does
**not** drive the result; **DATA_QUALITY_BOUND does not trigger.**

**Not outlier-driven.** Dropping the three largest movers per window: +0.0272 (MINE) / +0.0217
(VERIFY) against full means of +0.0285 / +0.0274.

## Diagnostics — explanatory only, decide nothing

* **RTV alone.** `rtv = 1` rows carry a day-median edge of −0.000 (day-win 47.5 %) in MINE and
  **−0.383 (44.9 %)** in VERIFY. Consistent with the sealed 4/4 NULL.
* **RTV × RS.** RS-intact +0.124 / +0.074, RS-absent −0.180 / −0.148 — the same sign in both windows.
  That is the **RS gate** doing the work, not RTV.
* **Liquidity.** Low dollar-volume flips sign between windows (−1.364 MINE → +1.027 VERIFY) = noise.
* **Concentration.** 8,239 treated rows across 2,256 tickers; top ticker 0.16 %, top-10 1.5 %,
  top-50 6.2 %. No concentration.
* **`bull_score` — NOT ISOLABLE.** `scanner.py:708` gives `rtv` a second, independent production
  surface: `if rtv or preup3 or preup2: bull += 1`, `return min(bull, 10)`. Inside that OR the term
  cannot be isolated and was not evaluated. Stated, not guessed.

## Verdict

**NO CONTRIBUTION DEMONSTRATED — removal candidate, not a production removal.**

* There is **no uplift**. Both windows' point estimates say removing the +3 *helps*, and they are
  nearly identical (+0.0285 / +0.0274).
* MINE's CI excludes zero; **VERIFY's does not**. Directional support for removal, insufficient
  replication for a harm claim.
* The sensitivity is identical, so the 7.11 % representation disagreement is not driving anything.
* The effect is **microscopic — about +0.03 pp per day** — and the score it sits in,
  `turbo_score`, is itself **CONTEXT, not a validated ranker** ([[project-score-audit-v1]]:
  positive IC in MINE only, ≈ 0 in VERIFY).

So the weight's presence in production **is not empirically justified**, and equally it is not proven
harmful. Those are two different statements and this study supports only the first.

## Explicitly not done

**No production change.** `turbo_engine.py:390` is untouched; the +3 still ships. No weight was
searched (`+1`/`+2`/`+4`), no threshold moved, no filter added, no artifact refitted. Whether to
remove an unsupported contribution is a **separate production-cleanup decision**, deliberately kept
apart from this evidence so that evidence and policy do not get mixed.

Code: `backend/rtv_contribution_v1.py` (primary + sensitivity), `backend/rtv_contribution_diag.py`
(diagnostics). Both reuse SCORE_AUDIT_V1 run `SA_20260907T162603Z` (X) and
`OUT_20260907T163751Z_AMENDMENT_1` (sacred outcomes).
