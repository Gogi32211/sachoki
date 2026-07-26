# Sachoki — Scoring Systems Reference

> All scoring engines in one place.  
> Source of truth: `backend/canonical_scoring_engine.py`, `backend/turbo_engine.py`,  
> `backend/ultra_score.py`, `backend/beta_engine.py`, `backend/profile_playbook.py`,  
> `backend/gog_engine.py`, `backend/analyzers/tz_wlnbb/signal_extraction.py`

---

## Table of Contents

1. [Score Hierarchy Overview](#1-score-hierarchy-overview)
2. [TURBO Score (primary composite)](#2-turbo-score-primary-composite)
3. [Canonical Sub-Scores](#3-canonical-sub-scores)
4. [FINAL_REGIME Labels](#4-final_regime-labels)
5. [FINAL_SCORE_BUCKET Labels](#5-final_score_bucket-labels)
6. [ULTRA Score v2](#6-ultra-score-v2)
7. [BETA Score v2.1](#7-beta-score-v21)
8. [Profile Score](#8-profile-score)
9. [GOG Score](#9-gog-score)
10. [Prebreak Score (260523)](#10-prebreak-score-260523)
11. [N-Bar Turbo Scores](#11-n-bar-turbo-scores)
12. [Score Interaction Flow](#12-score-interaction-flow)

---

## 1. Score Hierarchy Overview

```
Raw OHLCV
    │
    ├─► T/Z signal engines (signal_engine.py, wlnbb_engine.py, …)
    │
    ├─► turbo_engine._calc_turbo_score()
    │       └─► turbo_score  (0–100+, stored in DB)
    │
    ├─► canonical_scoring_engine.compute_canonical_score()
    │       ├─► FINAL_BULL_SCORE  = turbo_score  (canonical alias)
    │       ├─► ROCKET_SCORE      (0–40)
    │       ├─► CLEAN_ENTRY_SCORE (0–40)
    │       ├─► SHAKEOUT_ABSORB_SCORE (0–30)
    │       ├─► EXTRA_BULL_SCORE  (0–20)
    │       ├─► EXPERIMENTAL_SCORE (0–15)
    │       ├─► REBOUND_SQUEEZE_SCORE (0–20)
    │       ├─► HARD_BEAR_SCORE   (0–40, penalty)
    │       ├─► VOLATILITY_RISK_SCORE (0–20, penalty)
    │       ├─► FINAL_REGIME      (string label)
    │       └─► FINAL_SCORE_BUCKET (string label)
    │
    ├─► profile_playbook.compute_profile_playbook_for_row()
    │       ├─► profile_score     (unbounded additive, typically 0–80+)
    │       ├─► profile_category  (EARLY / WATCH / SWEET_SPOT / LATE / EXTENDED)
    │       ├─► sweet_spot_active (bool)
    │       └─► late_warning      (bool)
    │
    ├─► gog_engine.compute_gog_signals()
    │       ├─► gog_tier  (G1P / G2P / G3P / G1L / … / GOG3)
    │       └─► gog_score (42–100)
    │
    ├─► signal_extraction.compute_prebreak_signals()
    │       ├─► prebreak_score    (0–~80+, approximate)
    │       ├─► prebreak_watch    (bool, score ≥ 18)
    │       ├─► prebreak_ready    (bool, score ≥ 28)
    │       └─► prebreak_prime    (bool, score ≥ 45)
    │
    ├─► ultra_score.compute_ultra_score()
    │       ├─► ultra_score       (0–100)
    │       ├─► ultra_score_band  (A/B/C/D)
    │       ├─► ultra_score_band_v2 (A+/A/B/C/D)
    │       └─► ultra_score_priority (HIGH_PRIORITY / WATCH_A / STRONG_WATCH / …)
    │
    └─► beta_engine.calc_beta_score()
            ├─► beta_score    (0–100, display after non-linear transform)
            ├─► beta_raw      (raw linear composite)
            ├─► beta_setup    (0–60, structure component)
            ├─► beta_momentum (−5–50, regime/momentum component)
            ├─► beta_excess   (≥0, extension penalty)
            ├─► beta_zone     (ELITE / OPTIMAL / BUY / WATCH / BUILDING / NEUTRAL / SHORT_WATCH)
            └─► beta_auto_buy (bool, narrow-window auto-buy gate)
```

---

## 2. TURBO Score (primary composite)

**File:** `backend/turbo_engine.py` → `_calc_turbo_score(r, profile)`  
**Canonical alias:** `FINAL_BULL_SCORE`  
**Range:** 0 – 100 (capped at 100 by `min(100.0, s)`)  
**Stored in DB:** yes, in `turbo_scan` table

### What it measures
The TURBO score is the main multi-engine composite signal strength. It accumulates points from:

| Component | Max pts | Key signals |
|-----------|---------|-------------|
| T signals (bullish bars) | ~20 | T1G, T4, T1, T2G, T3, T6, T10, T12 |
| Z signals (bearish context) | ~8 | Z1, Z2, Z3G — negative or context bonus |
| F signals (F-strength Wyckoff) | ~18 | F1, F7, F8, F11 (profile-weighted) |
| B signals (combo entries) | ~15 | B1–B11 (combo_engine) |
| L signals (volume patterns) | ~12 | L-signals from wlnbb_engine |
| GOG patterns | ~10 | G1P, G2P, GOG tier entries |
| FLY patterns | ~8 | FLY breakaway signals |
| Delta signals | ~12 | dSPR +6, Ab↑ +6, ΔΔ↑ +5, Δ↑ +5 |
| Sequence bonus | ~8 | T/Z sequence continuations |
| ROCKET bonus | ~12 | para launch confirmation |
| Hard bear penalty | −40 | Z4, Z6, strong bearish engulf context |
| Vol risk penalty | −10 | vol-spike risk context |

### N-Bar variants (stored as separate columns)

| Column | Lookback |
|--------|---------|
| `turbo_score` | N=1 (current bar only) |
| `turbo_score_n3` | N=3 bars (any signal in last 3 bars) |
| `turbo_score_n5` | N=5 bars |
| `turbo_score_n10` | N=10 bars |

The N=3/5/10 scores are computed during scan and stored in DB. The UI switches between them client-side using the N-selector without requiring a rescan.

### Score tiers (UI display)

| Range | Label |
|-------|-------|
| 80–100 | 🔥 Elite |
| 60–79 | ✅ Strong |
| 40–59 | 🟡 Actionable |
| 20–39 | 🔵 Early |
| 0–19 | ⬛ Weak |

---

## 3. Canonical Sub-Scores

**File:** `backend/canonical_scoring_engine.py` → `compute_canonical_score(sig_row, profile)`

All sub-scores are breakdowns of `turbo_score` / `FINAL_BULL_SCORE`. They expose which signal family is driving the composite.

| Field | Range | What it measures |
|-------|-------|-----------------|
| `turbo_score` | 0–100 | Primary composite (= FINAL_BULL_SCORE) |
| `FINAL_BULL_SCORE` | 0–100 | Canonical alias for turbo_score |
| `ROCKET_SCORE` | 0–40 | Para-launch explosion signals (rocket, buy_2809, seq_bcont, vol_spike_10x) |
| `CLEAN_ENTRY_SCORE` | 0–40 | F/B entry cluster quality (F1, F7, B-combos, clean bar structure) |
| `SHAKEOUT_ABSORB_SCORE` | 0–30 | Shakeout/absorb entry pattern strength |
| `EXTRA_BULL_SCORE` | 0–20 | L-family and extra bull structure (WLNBB, L-signals) |
| `EXPERIMENTAL_SCORE` | 0–15 | FLY and newer pattern signals |
| `REBOUND_SQUEEZE_SCORE` | 0–20 | TZ_FLIP / rebound-squeeze entries |
| `HARD_BEAR_SCORE` | 0–40 | Hard bear / risk penalty (subtracted from total) |
| `VOLATILITY_RISK_SCORE` | 0–20 | Vol-spike risk context (subtracted from total) |
| `HAS_ELITE_MODEL` | 0/1 | 1 if FBS ≥ 120 OR (rocket AND FBS ≥ 80) |
| `HAS_REBOUND_MODEL` | 0/1 | 1 if REBOUND_SQUEEZE_SCORE ≥ 8 |
| `HAS_STRONG_BULL_MODEL` | 0/1 | 1 if CLEAN_ENTRY_SCORE ≥ 20 OR FBS ≥ 100 |

---

## 4. FINAL_REGIME Labels

Derived from sub-score combination. Shown in Superchart and replay reports.

| Label | Condition |
|-------|-----------|
| `ELITE_CLEAN_BULL` | FBS ≥ 120 AND clean ≥ 20 |
| `A_PLUS_CLEAN_BULL` | FBS ≥ 100 AND (clean ≥ 15 or rocket high) |
| `CONFIRMED_BULL` | FBS ≥ 80 |
| `ROCKET_WATCH` | FBS ≥ 80, rocket dominant |
| `SHAKEOUT_ABSORB` | FBS ≥ 60, shakeout ≥ 15 |
| `CLEAN_ENTRY` | FBS ≥ 60, clean ≥ 15 |
| `REBOUND_SQUEEZE` | FBS ≥ 40, rebound ≥ 8 |
| `RISK_REBOUND` | FBS ≥ 30, rebound present |
| `ACTIONABLE_SETUP` | FBS ≥ 60 |
| `EARLY_WATCH` | FBS 20–59 |
| `BEARISH_PHASE` | HARD_BEAR_SCORE dominant |
| `NEUTRAL_OR_LOW` | FBS < 20 |

---

## 5. FINAL_SCORE_BUCKET Labels

Simple bucket string based solely on `FINAL_BULL_SCORE` value:

| Bucket | FBS threshold |
|--------|--------------|
| `ELITE_140+` | ≥ 140 |
| `STRONG_120+` | ≥ 120 |
| `BULL_100+` | ≥ 100 |
| `CONFIRMED_80+` | ≥ 80 |
| `ACTIONABLE_60+` | ≥ 60 |
| `EARLY_40+` | ≥ 40 |
| `WEAK_20+` | ≥ 20 |
| `NEUTRAL` | < 20 |

---

## 6. ULTRA Score v2

**File:** `backend/ultra_score.py` → `compute_ultra_score(row)`  
**Range:** 0–100  
**No-lookahead guarantee:** never reads `ret_*`, `mfe_*`, `mae_*` forward-return fields

The ULTRA score is a higher-level composite that synthesises TURBO sub-scores, TZ Intelligence, ABR category, profile category, and pullback/reversal miner results into a single prioritisation number for the ULTRA scanner.

### Output fields

| Field | Type | Description |
|-------|------|-------------|
| `ultra_score` | int 0–100 | Final prioritisation score |
| `ultra_score_band` | A/B/C/D | Coarse band (A=80+, B=65+, C=50+, D=<50) |
| `ultra_score_band_v2` | A+/A/B/C/D | Fine band with A+ at ≥90 |
| `ultra_score_priority` | string | Action priority label (see below) |
| `ultra_score_reasons` | list[str] | Up to 12 deduped contribution reasons |
| `ultra_score_flags` | list[str] | Combo/context flags |
| `ultra_score_raw_before_penalty` | int | Sum of A+B+C+D+F before penalty clamped 0–100 |
| `ultra_score_penalty_total` | int | Absolute value of E (penalty component) |
| `ultra_score_regime_bonus` | int | G component (regime alignment bonus) |
| `ultra_score_caps_applied` | list[str] | Which score caps fired |
| `ultra_score_cap_reason` | string | Pipe-separated cap reasons |

### Priority labels

| Label | ultra_score |
|-------|-------------|
| `HIGH_PRIORITY` | ≥ 90 |
| `WATCH_A` | ≥ 80 |
| `STRONG_WATCH` | ≥ 65 |
| `CONTEXT_WATCH` | ≥ 50 |
| `LOW` | < 50 |

### Score caps

- **Momentum-only setup** (no strong regime): capped at 89 if confluence_count < 4  
- **Setup-only** (no good profile + no strong regime): capped at 49  
- **Breakout-only** (no good profile + no strong regime): capped at 59  

---

## 7. BETA Score v2.1

**File:** `backend/beta_engine.py` → `calc_beta_score(row, history, universe, rolling_score_max)`  
**Range:** 0–100 (display, after non-linear transform)  
**Exchange-calibrated:** separate weight formulas for SP500 vs NASDAQ

### Output fields

| Field | Type | Range | Description |
|-------|------|-------|-------------|
| `beta_score` | int | 0–100 | Display score after non-linear transform |
| `beta_raw` | int | any | Raw linear composite before transform |
| `beta_setup` | int | 0–60 | Structure/setup component |
| `beta_momentum` | int | −5–50 | Momentum/regime component |
| `beta_excess` | int | ≥0 | Extension penalty |
| `beta_zone` | string | — | Zone label (see below) |
| `beta_auto_buy` | bool | — | Narrow-window auto-buy gate (75–84 display) |

### Zone labels

| Zone | Display range | Condition |
|------|--------------|-----------|
| `ELITE` | ≥ 80 | — |
| `OPTIMAL` | 75–79 | — |
| `BUY` | 70–74 | — |
| `WATCH` | 60–69 | — |
| `BUILDING` | 40–59 | + RTB phase check |
| `SHORT_WATCH` | < 40 | RTB phase = D |
| `NEUTRAL` | default | — |

### Exchange formulas

| Universe | Formula |
|----------|---------|
| SP500 | `raw = setup × 1.00 + momentum × 1.50 − excess × 0.55` |
| NASDAQ | `raw = setup × 1.40 + momentum × 0.30 − excess × 0.85` |

### Special adjustments

- **Decay Memory Bonus:** If `rolling_score_max ≥ 50` but current display < 20, floor display up (survives momentary score collapse after breakout)
- **Turbo×Beta floor:** If `turbo_score ≥ 40` AND `beta_raw ≥ 30` but display < 15 → floor at 15 (keeps BUILDING signal)
- **P89 gate:** `preup89` + bullish zone → display × 1.1
- **D89 gate:** `predn89` + BUILDING → downgrade to NEUTRAL
- **Auto-buy:** requires display 75–84, setup ≥ 32, momentum 8–28, RTB phase B/C, sweet_spot_active, no LATE profile, no BEARISH regime

---

## 8. Profile Score

**File:** `backend/profile_playbook.py` → `compute_profile_playbook_for_row(row, signals_5bar, universe)`  
**Range:** unbounded additive (typically 0–80+)

Measures signal density relative to the ticker's price bucket (profile). Each profile has its own signal weights and sweet-spot range.

### Output fields

| Field | Type | Description |
|-------|------|-------------|
| `profile_score` | int | Total weighted signal score for the profile |
| `profile_category` | string | EARLY / WATCH / SWEET_SPOT / LATE / EXTENDED |
| `sweet_spot_active` | bool | Score is within the profile's sweet-spot range |
| `late_warning` | bool | Score exceeds the profile's late threshold |
| `base_profile_score_without_btb` | int | Score before Bear-to-Bull bonus |
| `btb_created_sweet_spot` | int(0/1) | 1 if BTB bonus pushed score into sweet-spot |

### Profiles

| Profile | Price range | Universe | Sweet-spot |
|---------|------------|---------|-----------|
| `SP500_LT20` | < $20 | SP500 | 12–36 |
| `SP500_20_50` | $20–$50 | SP500 | 12–40 |
| `SP500_50_150` | $50–$150 | SP500 | 12–42 |
| `SP500_150_300` | $150–$300 | SP500 | 12–42 |
| `SP500_300_PLUS` | > $300 | SP500 | 12–36 |
| `NASDAQ_PENNY` | < $5 | NASDAQ | 10–32 |
| `NASDAQ_REAL` | ≥ $5 | NASDAQ | 12–36 |

---

## 9. GOG Score

**File:** `backend/gog_engine.py` → `compute_gog_signals()`  
**Range:** 0, 42–100

GOG (Grid of Grids) tiers are priority quality labels. The score is fixed per tier (not additive).

| GOG Tier | Score | Description |
|----------|-------|-------------|
| `G1P` | 100 | Grade 1 Prime — highest quality |
| `G2P` | 92 | Grade 2 Prime |
| `G3P` | 88 | Grade 3 Prime |
| `G1L` | 82 | Grade 1 Level |
| `G2L` | 76 | Grade 2 Level |
| `G3L` | 72 | Grade 3 Level |
| `G1C` | 66 | Grade 1 Context |
| `G2C` | 60 | Grade 2 Context |
| `G3C` | 56 | Grade 3 Context |
| `GOG1` | 50 | Generic GOG tier 1 |
| `GOG2` | 46 | Generic GOG tier 2 |
| `GOG3` | 42 | Generic GOG tier 3 |
| *(none)* | 0 / NaN | No GOG pattern |

Only the highest-priority tier that fires is assigned to each bar.  
Both `gog_tier` (string) and `gog_score` (float) are stored in DB and exported to stock_stat CSV/parquet.

---

## 10. Prebreak Score (260523)

**File:** `backend/analyzers/tz_wlnbb/signal_extraction.py` → `compute_prebreak_signals(df, cfg)`  
**Range:** 0–~80+ (clipped to 0 minimum); approximate of Pine 260523_PREBREAK

This score summarises how ready a ticker is for a pre-breakout entry. It is computed bar-by-bar from 260523 event flags.

### Additive contributions

| Contribution | Points |
|-------------|--------|
| `ad_cluster` (AD accumulation cluster) | +15 |
| `ad_fresh` (AD fresh signal) | +8 |
| `pb_stop_cause` (W-Phase stop) | +12 |
| `pb_lvbo` (LVBO reversal) | +10 |
| `pb_pp_rtv` (pivot proximity + RTV) | +10 |
| `wyc_spring` (Wyckoff Spring) | +12 |
| `pb_wvf_confirm` (WVF confirm) | +8 |
| `pb_fly_cd_c` (FLY-CD confirmed) | +6 |
| `wyc_in_tr` (price in trading range) | +4 |
| T-signal present (any) | +3 |
| `wyc_sow` (Sign of Weakness) | −6 |
| `pb_macro_penalty` (EMA20 falling + deep below EMA50) | −15 |

### Score-tier flags

| Flag | Threshold | Chip shown |
|------|----------|-----------|
| `prebreak_watch` | score ≥ 18 (and < 28) | `WATCH` |
| `prebreak_ready` | score ≥ 28 (and < 45) | `READY` |
| `prebreak_prime` | score ≥ 45 | `PRIME★` |

Thresholds are configurable via `PREBREAK_WATCH_THR`, `PREBREAK_READY_THR`, `PREBREAK_PRIME_THR` in cfg.

---

## 11. N-Bar Turbo Scores

The scan stores three additional turbo scores computed at N=3, N=5, N=10:

| Column | Description |
|--------|-------------|
| `turbo_score` | Signal must be on the **current bar** (N=1) |
| `turbo_score_n3` | Any signal fired within last **3 bars** |
| `turbo_score_n5` | Any signal fired within last **5 bars** |
| `turbo_score_n10` | Any signal fired within last **10 bars** |

These allow the client to switch between N= values in the ULTRA scanner without a rescan. The N-selector also controls 260523 event booleans via per-column `_age` fields (e.g. `ad_fresh_age`, `prebreak_prime_age`). An event at age < N counts as active.

---

## 12. Score Interaction Flow

```
turbo_score (0–100)
    │
    ├─ canonical sub-scores ──► FINAL_REGIME ──► Superchart regime label
    │                       └── FINAL_SCORE_BUCKET
    │
    ├─ profile_score ──► profile_category ──► sweet_spot_active / late_warning
    │
    ├─ gog_score ──► gog_tier chip in ULTRA Signals / Superchart
    │
    ├─ prebreak_score ──► PRIME★ / READY / WATCH chips in ULTRA Signals
    │
    ├─ ultra_score (0–100) ──► ultra_score_band_v2 ──► ULTRA grid sort
    │       input: turbo_score + gog_score + profile_score + intel results
    │              + ABR category + pullback/reversal flags + FINAL_REGIME
    │
    └─ beta_score (0–100) ──► beta_zone ──► Superchart beta badge
            input: turbo_score + profile_score + T/Z history (5 bars)
                   + ROCKET_SCORE + rtb_phase + sweet_spot_active
```

---

*Last updated: 2026-05-23. Branch: `feat/ultra-parquet-inmem`.*
