# CROSS_STAR_CONCURRENCE_V1 — PRE-OUTCOME FEATURE AUDIT

**No outcome has been opened and none may be until this section is accepted.** Sealed 2026-09-17.
Code: `backend/cross_star_concurrence_v1.py` · feature table `data/cross_star_features.parquet`.

## 1 · Provenance — traced to producers, not to glyphs

| | TOP STAR | BOTTOM STAR |
|---|---|---|
| producer | `backend/lbal_build.py:157-164` `states(udn, udn_c, colour)` | `backend/studio/bar_physics.py:293` |
| store | `data/lbal_signals.parquet` | `bars.phys_ad` |
| shape | three **independent booleans** | **one mutually-exclusive categorical** |
| ★ | `star` — divergence: `(udn U ∧ candle RED) ∨ (udn D ∧ candle GREEN)` | AD-FRESH: Z1G/Z2G within 12 bars → T4/T6/T2G/T2, position < 0.50 |
| ★★ | `conflict`: `(udn D ∧ udn_c U) ∨ (udn U ∧ udn_c D)` | AD-CLUSTER: 2+ AD-FRESH in an 8-bar window |
| ★★★ / ★A | `half_up`: `(udn U ∧ udn_c N) ∨ (udn N ∧ udn_c U)` | ★A — AD-FRESH **with absorption** at the exhaustion bar (`r_at_a > r_med · R_ABS_MULT`) |
| status | **SAFE / DESCRIPTIVE** — the module says outright "Nothing here is an edge, a score input or a filter with claimed lift" | **SAFE** — "NO LOOKAHEAD ANYWHERE. Every window is backward-looking and every groupby is per ticker" (`bar_physics.py:24`) |
| covered ⇔ | `udn IS NOT NULL` (`nv_lab > 0`) | `phys_ad IS NOT NULL` |

### CD family — `backend/ovd_map_build.py:140-141` → `data/ovdmap_signals.parquet`

Logic 3, "close dominance under a 5-day decline":

```
decline = close < close[5]
cd60 = decline ∧ rvC60  > 1 ∧ c60  / o60  >= 1      (last 60m volume & range vs the first 60m)
cd30 = decline ∧ rvC30B > 1 ∧ c30b / o30a >= 1      (last 30m vs the first 30m)
```

**SAFE** — every input is a same-session intraday aggregate plus a 5-day-back close; nothing forward.
This is a **different upper layer from TOP**: TOP is LBAL/UDN balance, CD is the OVD map. Both are
produced in the same `lbal_build.py` pass from the 15m store.

⚠️ **Coverage trap.** `ovd_map_build.py:181` keeps only rows where *some* OVD token fired
(`any_tok`), so an absent row is ambiguous on its own — "no token" or "not processed". Resolved
empirically rather than assumed: **ovdmap ⊆ lbal with ZERO rows outside it** (0 of 1,353,225). So
LBAL presence defines CD coverage, and an absent ovd row *inside* that coverage means "no token
fired" = False, not UNKNOWN.

**Two corrections to the brief, both material.**

1. **The BOTTOM family is one column, not three flags.** `phys_ad` is
   `np.select([cluster, ad_fresh & absorbed, ad_fresh], ["★★","★A","★"], default="")` — the three
   grades are **mutually exclusive by construction**, so "★A" cannot co-occur with "★", and a row
   can never carry two bottom grades. Any design treating them as independent indicators is wrong.
   The brief's reading of what each glyph *means* is exactly right.
2. **The same concept exists twice in `bars`.** `phys_ad` is `bar_physics`' Python port; `ad_fresh` /
   `ad_cluster` are the Pine originals imported by `studio/importer.py:129`. They are independent
   computations of one rule. `phys_ad` is declared authoritative — it is the layer the bottom pane
   actually renders — and their agreement is measured below rather than assumed.

The TOP columns come in two modes. The **unsuffixed** ones are PRIMARY (`MODE "V"`, volume-confirmed
bars, the user's own TradingView setting); `*_all` is the alternate mode kept for a toggle. Used the
primary set.

## 2 · Coverage — UNKNOWN is never False

5,395,566 ticker-days in `bars` (non-index, deduped by universe), 2021-05-26 … 2026-09-16, 5,719 tickers.

| | rows | share | median $ volume |
|---|---:|---:|---:|
| TOP covered | 3,533,765 | 65.49 % | **$22,689,652** |
| BOTTOM covered | 5,395,561 | 100.00 % | $6,113,933 |
| **BOTH — the eligible universe** | **3,533,764** | **65.49 %** | **$22,689,606** |
| BOTTOM only (TOP missing) | 1,861,797 | 34.51 % | **$235,969** |

⚠️ **Coverage is set entirely by TOP, and the excluded third is 96× less liquid.** This is the LBAL
pathology already on record (`BREAKOUT_SYSTEMS_REPORT.md` §1.1: "their presence is a 100× liquidity
filter"). It is **not** a stop condition — the coverages are *nested*, not disjoint — but it fixes
the study's reach: **the eligible universe is a declared population for BOTH arms, never a filter
applied to one side**, and nothing here will generalise to the missing third.

## 3 · Port parity — `phys_ad` vs the Pine originals

| | |
|---|---:|
| rows compared | 5,395,561 |
| agree (both fire) | 380,639 |
| `phys_ad` only | 3,941 |
| Pine only | 12,439 |
| **disagreement** | **16,380 (0.304 %)** |

Small, one-sided (Pine fires slightly more often), and not disqualifying. Recorded so that if a
future outcome result is close, the concordant subset is the pre-declared sensitivity.

## 4 · Prevalence on the eligible universe

3,533,764 ticker-days · 3,202 tickers · 1,307 sessions.

| | rows | share |
|---|---:|---:|
| **TOP any** | 1,393,255 | **39.427 %** |
| ★ divergence | 942,404 | 26.669 % |
| ★★ conflict | 319,556 | 9.043 % |
| ★★★ half-up | 394,755 | 11.171 % |
| **BOTTOM any** | 253,278 | **7.167 %** |
| ★ AD-FRESH | 92,583 | 2.620 % |
| ★★ AD-CLUSTER | 152,782 | 4.323 % |
| ★A +absorption | 7,913 | 0.224 % |
| **SAME-DAY CROSS** | **104,034** | **2.944 %** |
| TOP only | 1,289,221 | 36.483 % |
| BOTTOM only | 149,244 | 4.223 % |
| neither | 1,991,265 | 56.350 % |

⚠️ **TOP fires on 39.4 % of eligible ticker-days.** At that rate it is a *state*, not an event — the
same base-rate caution `project_signal_cooccurrence_map` already records for UDN/OVD. It does not
block the study; it does mean the only interesting question is incremental value over TOP-only, which
is exactly the frozen primary.

All three arms are well powered: CROSS 104k, TOP-only 1.29M, BOTTOM-only 149k.

## 5 · Redundancy — are the two families the same thing?

| | |
|---|---:|
| P(TOP) | 0.3943 |
| P(BOTTOM) | 0.0717 |
| P(TOP ∧ BOTTOM) observed | 0.02944 |
| expected under independence | 0.02826 |
| **lift over independence** | **1.042** |
| P(BOTTOM \| TOP) | 0.0747 vs base 0.0717 |
| P(TOP \| BOTTOM) | 0.4108 vs base 0.3943 |
| phi | **+0.0094** |
| Jaccard | 0.0674 |

**The two families are statistically near-independent** — a 4 % excess over chance and a correlation
of +0.009. Stable everywhere: liquidity terciles 1.02–1.06, price bands 1.03–1.17, years 1.00–1.12.

**This is the right precondition for an incremental-value test** (the two carry different
information) — but note the prior: `project_signal_cooccurrence_map` mapped 164 marks × 2.8 M
sessions and found **99.2 % of cross-layer pairs within ±10 % of lift 1.0, max 1.193**. At 1.042 this
pair sits squarely inside that already-measured null. Concurrence here is **not** distinguished by
co-occurrence; if it pays, it must be for a reason co-occurrence does not show.

By strength, the only cell above 1.1 is TOP ★★★ half-up → P(BOTTOM) 0.0827, lift **1.154**. With
nine strength cells inspected that is unremarkable and is **not** to be chased.

## 6 · Temporal windows — declared before any outcome

| | rows | share |
|---|---:|---:|
| **same_day_cross — PRIMARY** | 104,034 | 2.944 % |
| near_cross_pm1 | 351,013 | 9.933 % |
| near_cross_pm2 | 490,720 | 13.887 % |
| TOP→BOTTOM within 1d | 65,525 | 1.854 % |
| BOTTOM→TOP within 1d | 70,566 | 1.997 % |

Windows nest correctly (`same_day ⊆ ±1 ⊆ ±2`); ±1 adds 246,979 rows over same-day and ±2 a further
139,707. Distances are counted in **trading sessions**, not calendar days, so a gap stays a gap.

**±1, ±2 and both orderings are SECONDARY, diagnostic only. Same-day is the primary and the only one
that can produce a verdict** — frozen here precisely so the window cannot be picked after seeing
outcomes.

## 7 · Concentration and data quality

* **No concentration.** 104,034 cross rows over 3,178 tickers and 1,307 sessions; top ticker CPSH
  0.07 %, top-10 0.6 %, top-50 2.9 %; busiest day 0.57 %, top-10 days 4.3 %; per-session cross share
  median 2.01 %.
* **No microcap skew.** 15.0 % of cross rows are under $8 against 14.3 % of the eligible universe.
* **No duplicates.** 0 duplicate ticker-days.
* **Eligible share by year** 2021 61 % · 2022 71 % · 2023 69 % · 2024 67 % · 2025 64 % · 2026 59 %.
  Stable; the mild decline tracks universe growth outrunning LBAL coverage.
* Span 2021-07-02 … 2026-09-16.

## 8 · Is it safe to proceed to outcomes?

**Yes — the feature layer is sound, and four things must be settled first because each changes the
design.** None of the brief's stop conditions fired: both producers are verified, coverage is nested
rather than non-overlapping, missing is tri-state throughout, and the reconstruction matches DB
semantics to 0.304 %.

1. **Declare the universe, do not filter it.** All comparisons live inside the 65.5 % both-covered
   population; the excluded third is 96× less liquid and no result reaches it.
2. **`phys_ad` is authoritative** for BOTTOM; the concordant-with-Pine subset is the pre-declared
   sensitivity if a result is close.
3. **State that TOP is a 39 % state, not an event.** The estimand must be incremental value over
   TOP-only — which it already is.
4. ⚠️ **The outcome choice is not free, and the brief's preferred list contains a known trap.**
   `feedback-pathsim-not-mfe-proxy` records that an MFE ≥ target proxy **inflates** against the
   bar-by-bar stop-first path-sim (+3.4 → −2.4 on a measured case). "Probability MFE20_ATR ≥ 3/5/8"
   is exactly that proxy. The honest default is this book's own sacred `edge_replay._pathsim` with
   the day-clustered estimand, as `SCORE_AUDIT_V1` used.

**Still to agree before outcome access** — a new family needs them declared, not improvised: the
outcome and its estimand (per 4), the MINE/VERIFY boundary, **k**, and the matched-control
construction (same date + liquidity bucket + price bucket + universe, no future-derived quantity).

## 9 · CD family — census

⚠️ **`cd_covered` is NOT simply `top_covered`.** The OVD store begins **2021-08-02** while LBAL
begins 2021-07-02, and OVD carries its own warm-up (the HV-event lookback reaches up to 30 sessions
back). Before its first session an absent ovd row means **NOT BUILT**, not "no token fired" —
**46,817 eligible rows would otherwise have carried a false `cd30/cd60 = False`.** Caught and fixed.

* **primary family universe** (`both_covered`) = **3,533,764** — unchanged
* **CD family universe** (`coverage_all_required`) = **3,486,947** (−46,817 to the OVD warm-up)

The primary universe is deliberately **not** shrunk by a secondary family's warm-up.

| | rows | share |
|---|---:|---:|
| CD30 | 726,406 | 20.556 % |
| CD60 | 679,330 | 19.224 % |
| CD30 ∧ CD60 | 620,070 | 17.783 % |
| CD30 only | 106,336 | 3.009 % |
| CD60 only | 59,260 | 1.677 % |
| BOTTOM × CD30 | 54,159 | 1.533 % |
| BOTTOM × CD60 | 50,842 | 1.439 % |
| **BOTTOM × CD_BOTH** | **46,300** | **1.328 %** |
| CD_BOTH without BOTTOM | 573,770 | 16.237 % |

### ⚠️ CD30 and CD60 are very nearly the SAME signal

| | |
|---|---:|
| P(CD60 \| CD30) | 0.8536 |
| P(CD30 \| CD60) | 0.9128 |
| **Jaccard** | **0.7892** |
| **phi** | **+0.8537** |

They share the `decline` gate and differ only in the window (last 30m vs last 60m). `CD_BOTH` is
85 % of CD30 and 91 % of CD60. **The brief's secondary groups 2, 3 and 4 — "CD30 alone", "CD60
alone", "CD30+CD60" — are therefore effectively ONE group, not three**, and reporting them as three
hypotheses would triple-count a single signal. Reported as one family with two near-duplicate
renderings.

### CD vs BOTTOM — the same near-independence as the primary

| | P(cd) | P(BOTTOM\|cd) | lift | phi | Jaccard |
|---|---:|---:|---:|---:|---:|
| CD30 | 0.2056 | 0.0746 | 1.040 | +0.0057 | 0.0585 |
| CD60 | 0.1922 | 0.0748 | 1.044 | +0.0060 | 0.0577 |
| CD_BOTH | 0.1755 | 0.0747 | 1.042 | +0.0054 | 0.0560 |

Base P(BOTTOM) = 0.0717. **Lift 1.040–1.044 — indistinguishable from the TOP × BOTTOM 1.042.** Both
upper layers are near-independent of the lower one to the same degree.

### CD vs TOP — mildly NEGATIVE

| | P(TOP\|cd) | lift | phi |
|---|---:|---:|---:|
| CD30 | 0.3589 | 0.910 | −0.0368 |
| CD60 | 0.3590 | 0.910 | −0.0352 |
| CD_BOTH | 0.3563 | 0.904 | −0.0359 |

Base P(TOP) = 0.3943. The two upper-script layers weakly **avoid** each other — worth knowing,
because it means TOP and CD are not interchangeable stand-ins.

### Concentration of BOTTOM × CD_BOTH

46,300 rows · 3,086 tickers · 1,272 sessions. Ticker concentration is nil (top 0.09 %, top-10
0.8 %, top-50 3.5 %) and it is *less* microcap than the universe (12.4 % under \$8 vs 14.3 %).

⚠️ **But day concentration is materially higher than the primary's:** busiest day 2025-04-09 carries
746 rows (**1.61 %**) and the top-10 days 8.6 %, against 0.57 % / 4.3 % for the same-day cross. That
is structural, not accidental — CD requires a 5-day decline, so it batches onto market-wide selloff
days. **Consequence: day-clustered inference is mandatory for the CD family, and "result after
removing the largest mover days" is a required robustness column, not an optional one.**

## 10 · Frozen estimands

**PRIMARY — one, and it does not move.** Same-day `top_star_any ∧ bottom_star_any`, compared with
TOP-only and BOTTOM-only inside the declared eligible universe, matched on date + liquidity bucket +
price bucket + universe. The verdict of this study is the primary's verdict.

**SECONDARY — pre-declared, diagnostic only.** ±1 and ±2 day windows; both orderings; the CD family
in full (BOTTOM alone, CD, CD+BOTTOM, and the triple); and the strength subgroups.

⚠️ **`CD30+CD60 + BOTTOM` is SECONDARY and stays secondary.** It is registered here, before any
outcome, precisely so that a strong result cannot be promoted over the primary afterwards. If it
comes out well, that is a pre-declared secondary finding and must be reported as one — it does not
overwrite or replace the primary verdict, and it would need its own registration to become a claim.

## 11 · FROZEN OUTCOME SPEC — registered before any outcome access

### 11.1 The authoritative outcome — and why the obvious one was rejected

**`mtm_20` from `opportunities.parquet` is DISQUALIFIED for the primary.** It exists, it is
censor-aware (`bars_priced`, `mtm_exit_bar` — the `072ff0b` contract), but it only holds rows where a
**book edge fired**, and the three arms enter it at *different rates*:

| arm | treated rows | present in `opportunities` | |
|---|---:|---:|---:|
| same-day CROSS | 104,034 | 21,675 | **20.83 %** |
| TOP-only | 1,289,221 | 205,512 | **15.94 %** |
| BOTTOM-only | 149,244 | 35,913 | **24.06 %** |

Comparing arms through a filter that is itself correlated with the treatment is a selection bias, not
an outcome. `MFE20_ATR ≥ 3/5/8` is separately disqualified as a decision metric by
[[feedback-pathsim-not-mfe-proxy]] (+3.4 → −2.4 on a measured case). Both remain **secondary
diagnostics**.

**AUTHORITATIVE: the sacred `edge_replay._pathsim`** via `ovd_outcome_direct_v1.direct_trades(ps,
frames, keys, maxh)`, digest-pinned `PATHSIM_SRC_SHA = 0e74668f554910de` with
`assert_no_local_pathsim` forbidding any local copy. It takes an arbitrary `(ticker, session)`
selection — exactly this design — and is the estimand every sealed family in this book used.

* **PRIMARY: realized `ret` at `maxh = 60`** — the default of every sealed family and the horizon
  production's own ATR×12 exit law runs on.
* **SECONDARY (pre-declared): the same engine at `maxh = 20`** — the 20-bar view asked for, at no
  extra risk: same engine, same rows, one parameter, declared here rather than chosen later.

### 11.2 ⛔ The cooldown, and the registration it requires

`_pathsim` carries a stateful 5-bar same-ticker cooldown (`i - last < 5 → skip`). Measured on these
arms it does **not** thin them comparably:

| arm | n | within 5 sessions of the previous fire |
|---|---:|---:|
| same-day CROSS | 104,034 | **25.9 %** |
| **TOP-only** | 1,289,221 | **89.3 %** |
| BOTTOM-only | 149,244 | 33.5 % |
| BOTTOM × CD_BOTH | 46,300 | 13.7 % |

TOP fires on 39 % of ticker-days, so its rows are dense and the cooldown eats nearly all of them.
**Running each arm as a direct mask would compare the 10.7 % of TOP-only fires that happen to be
isolated against the 74.1 % of CROSS fires that are — different populations, and the difference is a
function of the treatment itself.** That is fatal, and it is why this is settled *before* outcomes.

[[feedback-pathsim-cooldown-estimand]] gives the rule and its one licensed exception: *"if
per-observation outcomes are scientifically wanted, register that estimand (and the cooldown
neutralisation) BEFORE any outcome access."* A k-nearest matched-control design is inherently
observation-level, so:

> **REGISTERED HERE, PRE-OUTCOME:** every eligible row is evaluated exactly once by calling the
> **UNMODIFIED** engine over **five disjoint bar-index-mod-5 masks**, so consecutive taken signals in
> one mask are ≥ 5 bars apart and the cooldown never suppresses a row. Engine, fills, trail, slip and
> horizon untouched. This is the same construction `ovd_outcome_v1.py:8-15` registered for the OVD
> family. For reconciliation the cooldown-thinned direct-mask trade count is reported alongside, as
> descriptive. Row counts are reconciled **by the engine's own drop reasons** — `NO_NEXT_SESSION`
> (`i+1>=n`), `NO_ENTRY_OPEN` (`ep<=0`), `COOLDOWN` (`i-last<5`) — never by interpretation.

### 11.3 Primary estimand and decision quantity

```
Δ_TOP    = CROSS − matched TOP-only
Δ_BOTTOM = CROSS − matched BOTTOM-only
Δ_incremental = min(Δ_TOP, Δ_BOTTOM)
```

The composite counts as confirmation only if it beats **both** components.

### 11.4 Matching — frozen

Control pools, per treated `(ticker, date)`: **same session**, both systems covered, **same
universe/exchange**, **same price bucket**, **same dollar-volume bucket**; TOP-only pool requires
`top_star_any ∧ ¬bottom_star_any`, BOTTOM-only the mirror. Within an admissible pool, the **k = 5
nearest** by

```
distance = |log(dvol_t) − log(dvol_c)| + 0.5 · |log(px_t) − log(px_c)|
```

— deterministic, so no sampling variance. Fewer than 5 admissible: take what exists, minimum 1, and
**report coverage by matched-k**; buckets are never widened or narrowed post hoc. **No RSI,
volatility or decline variable enters the matching** — each is part of the signals' own causal
mechanism and matching on it would match the effect away. No future-derived quantity anywhere.

### 11.5 Time split — frozen

```
MINE    2021-01-01 → 2023-12-31     usable from 2021-07-02 (LBAL store start)
VERIFY  2024-01-01 → 2025-12-31
2026    LOCKED — not opened
```

The boundaries are fixed; only the *usable start* is recorded, and it is not moved for any result.
The CD family's usable start is **2021-08-02** (OVD store).

### 11.6 Secondary decision family — one, not three

`phi = +0.853` and `Jaccard = 0.789` between CD30 and CD60 forbid treating them as independent
hypotheses. **One secondary decision: `BOTTOM + CD_BOTH` vs `BOTTOM-only` and vs `CD_BOTH-only`**,
with the same `min(Δ, Δ)` rule. CD30 and CD60 individually are supporting diagnostics.
**It stays secondary whatever it shows** — registered now precisely so a strong result cannot be
promoted over the primary afterwards.

### 11.7 Inference and robustness — frozen

Matched treated−control mean and median difference; **day-aggregated difference; date-clustered
bootstrap 95 % CI**; yearly sign; MINE and VERIFY reported separately, pooled descriptive only.
**No iid row-level bootstrap.** Robustness columns, all required: exclude the top-3 absolute-mover
dates; ≥ $1M/day slice; ≥ $5 slice; top-50-ticker removal. The CD family's day concentration
(top day 1.61 %, top-10 8.6 %) makes the mover-date column mandatory there, not optional.

### 11.8 Decision rule — frozen

* **INCREMENTAL CONFIRMATION DEMONSTRATED** — in VERIFY both `Δ_TOP > 0` and `Δ_BOTTOM > 0`, both
  directionally consistent with MINE, day-clustered uncertainty not contradicting the effect, and the
  effect not driven by illiquidity, one-day concentration or a few extreme movers.
* **DIRECTIONAL, NOT REPLICATED** — both point estimates positive, uncertainty too wide.
* **NO INCREMENTAL CONFIRMATION** — fails to beat either component.
* **DATA-QUALITY-BOUNDED** · **NOT EVALUABLE** — as defined in the brief.

CD family: the same four, worded for CD.

## 12 · Production

**No production change in this study.** No composite star signal is added, neither script is
altered, nothing is wired in. Evidence collection only.
