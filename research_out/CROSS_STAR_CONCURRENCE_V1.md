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

## 9 · Production

**No production change in this study.** No composite star signal is added, neither script is
altered, nothing is wired in. Evidence collection only.
