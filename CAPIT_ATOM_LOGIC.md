# Capit → Atom Journal — Logic Reference

## 1. ორი ბაზური სიგნალი

### A. Capitulation (კაპიტ) — `capit_scan.py`

**მიზანი:** Wyckoff selling-climax spring — ფასი ძალიან ღრმად ჩავარდა, ახლა bounce-ს ელოდება.

**SQL ფილტრი (B+ quality):**
```
l_sig IN ('L34', 'L46')   — VSA ვოლუმ-ლაინი (absorption/climax)
RSI_14 >= 15 AND < 30     — ღრმად oversold
CCI_20 < -100             — extreme negative momentum
(close / close_20bar - 1) > -0.25  — knife-guard: max -25% 20d-drawdown
                                      (>-60% collapse → falling knife, exclude)
fri64 = 0 AND absorb = 0  — არ არის FRI64 ან absorption bar
close NOT IN [$1, $2)     — penny knife exclude
dv >= $300k               — ლიქვიდობა
```

**სკორინგი (0-100):**
| atom | ქულა |
|------|------|
| base (L34/L46 + RSI + CCI) | +45 |
| flush -10%..-45% (sweet-spot) | +15 |
| red bar (close ≤ open) | +15 |
| vol 1.5x-5x | +15 |
| BLUE coil | +12 |
| RSI < 20 (deep) | +5 |
| vol ≥7x blowoff | -15 |

**validated edge:** +4.6% cost-adj, 5/6 years (path-sim, stop-first).
**EXIT:** hold ~20 bars, NO tight stop — stop ჭრის bounce-ს.

---

### B. Atomic (weak-close gap-up) — `atomic_scan.py`

**მიზანი:** Bull T-სიგნალი რომელიც gap-ით ზემოთ გაიხსნა, მაგრამ სუსტად დაიხურა (weaknes absorption) — ფასი "შეასახა" gap-ს, ახლა momentum-ი ელოდება.

**ბაზური ფილტრი:**
```
t_sig IN (T1, T1G, T2, T2G, T3, T4, T5, T6, T9, T10, T11, T12)
close_suffix = 'O'          — weak close (close near open / below prior body)
bar_gap_class IN ('G2','G3') — gap-up bar
vol_bucket <> 'VB'          — no blow-off volume
dv >= $500k                 — ლიქვიდობა
close >= $16                — price knife-guard
                            — (exception: rescued by B+ capit — see below)
```

**სკორინგი (0-100):**
| atom | ქულა |
|------|------|
| base (weak-close + gap) | +40 |
| R2L (bar_line5 contains R2L = RSI2/PSAR oversold) | +25 |
| EO (suffix starts with E = escape bar) | +15 |
| vol=B (big volume) | +15 |
| wick=D (lower wick dominant) | +10 |
| G3 (large gap) | +10 |
| 🔥 post-capit (see below) | +20 |

**validated edge (5yr, next-open entry):** positive expectancy, 5/6 years (only 2022 bear negative), -15% stop / +100% target / 20-bar horizon.

---

## 2. Capit → Atom Confluence (🔥 key finding)

**რა არის:** Atomic სიგნალი რომელიც **B+ quality capitulation-ს მოჰყვა** იმავე ticker-ზე **≤ capit_window დღეში** (default: 15 დღე).

**ლოგიკა:**
```python
# B+ capit dates for each (ticker, universe) → dict
capit_dates = capit_signal_dates(since_date=...)

# Atomic candidate-ისთვის:
dpc = days_since_capit(capit_dates, ticker, universe, signal_date)
post_capit = (dpc is not None and dpc <= capit_window)

# Effect:
if post_capit:
    score += 20           # premium confluence boost
    # cheap ($<16) ticker rescued — included even without price gate
    # cheap WITHOUT B+ capit → excluded (med -1.21, random noise)
```

**validated stats (rich+capit ≤10d):**
- win rate: **67%** vs 52% baseline
- median return: **+4.24%** vs +1.41% baseline
- survives: price-control, dedup, cluster-exclusion
- NOTE: only B+ quality capit rescues (raw/penny capit = med -1.21, NO rescue)

**quality filter matters:**
```
B+ capit = L34/L46 + RSI 15-30 + CCI<-100 + drawdown >-25%
           (NOT fri64, NOT absorb, NOT $1-2 penny)

Raw/penny capit → does NOT rescue the following gap-up
```

---

## 3. Trade Mechanics (Replay / Backtest)

```
Entry:    next session open (next bar open after signal)
Stop:     -15% from entry
Target:   +100% from entry
Horizon:  20 bars (~1 month)
Sizing:   equal 4% paper bets per trade
One open: one position per ticker at a time (dedup by ticker)
Path-sim: bar-by-bar stop-first (low hits stop before high hits target)
```

**Exit priority:** stop_first — ბარზე low ჯერ მოწმდება stop, მხოლოდ მერე high = target.

---

## 4. PreBuy Tab (live scanner)

**endpoint:** `GET /api/capit-atom-journal/prebuy?capit_window=15`

```python
result = atomic_scan(max_age_days=5, dv_floor=500_000, capit_window=capit_window)
rows = [r for r in result["rows"] if r["post_capit"]]  # only confluence setups
rows.sort(key=lambda x: (-x["score"], x["age_days"]))
```

**გამოაქვს:** მხოლოდ ის Atomic სიგნალები სადაც `post_capit=True` (B+ capit ≤15 დღეში).

---

## 5. Replay Tab (historical backtest)

**endpoint:** `GET /api/capit-atom-journal/replay?months=12&capit_window=15`

```python
replay(months=12, conf_only=True, ...)  # only post_capit=True trades
```

**გამოაქვს:** by_month breakdown — trades, PnL, hit-rate per month + aggregate KPIs.

---

## 6. ფილტრების summary

| პირობა | Capit | Atomic standalone | Capit→Atom |
|--------|-------|-------------------|------------|
| ფასი | — | ≥$16 | ≥$16 ან post-capit |
| vol | — | ≠VB | ≠VB |
| quality | L34/L46+RSI+CCI+drawdown | T-sig+close=O+gap | ორივე |
| score boost | — | — | +20 |
| win rate | ~55% | ~52% | **~67%** |
| edge | regime-independent | momentum/beta | momentum/beta + absorption |

---

## 7. Regime Gate

```python
regime = compute_regime(as_of)
# label: RISK_ON / NEUTRAL / RISK_OFF
# conv_mult: size multiplier (UI-level hint only — scanner doesn't block)
```

`RISK_OFF` → stand down / reduce size. Atomic edge is beta-dependent (loses in corrections). Capit edge is more regime-independent.

---

## 8. ფაილები

| ფაილი | შინაარსი |
|-------|----------|
| `backend/ai_journal/capit_scan.py` | B+ capit scanner + `capit_signal_dates()` |
| `backend/ai_journal/atomic_scan.py` | Atomic live scanner + confluence flag |
| `backend/ai_journal/atomic_journal.py` | `replay()` — historical backtest |
| `backend/main.py` L697+ | `/api/capit-atom-journal/replay` + `/prebuy` endpoints |
| `frontend/.../CapitAtomJournalPanel.jsx` | UI — PreBuy tab + Replay tab |
