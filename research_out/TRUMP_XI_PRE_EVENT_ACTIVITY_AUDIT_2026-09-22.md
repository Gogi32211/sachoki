# TRUMP–XI PRE-EVENT ACTIVITY AUDIT — 2026-09-22

**Current-state positioning audit. Descriptive only. No forecast, no new signal family, no backtest,
no threshold tuned after looking.** Every number below is read from the existing production stores.

**Data vintage:** the 1D store's last completed session is **2026-09-21**. 2026-09-22 is not in the
data, so "TODAY" throughout means the 2026-09-21 close.

**Windows:** 5d / 10d / 20d ending 2026-09-21. The 20-session window is 2026-08-24 … 2026-09-21.

---

## COVERAGE — what was actually found

**Found: 38 of 42.** All carry the full 1,336-session history except where noted.

**MISSING — 4, all from the China basket:**

| ticker | why |
|---|---|
| **TCEHY** | OTC ADR — the 1D store holds US index constituents only |
| **KWEB** | ETF — not in the store (`universe <> 'index'` excludes them, and no ETF rows exist) |
| **FXI** | ETF — same |
| **MCHI** | ETF — same |

⚠️ **This is material and not a footnote.** The four instruments that would give a *clean, diversified*
read on China exposure are precisely the four that are absent. The China conclusion below therefore
rests on **four single-name ADRs**, each carrying its own company risk. Treat the China basket's
breadth statistics as having N = 4, not as a sector read.

---

## DATA-QUALITY FLAGS (checked before anything was interpreted)

| ticker | flag | effect |
|---|---|---|
| **KLAC** | **corporate-action BASIS SEAM**: 2026-06-11 close 2411.64 → 2026-06-12 close 254.54, ×0.106 | Any window crossing 2026-06-12 is meaningless. Its "−92.4% off the 52-week high" is an artifact, **not** a drawdown. Its 5/10/20d figures are post-seam and usable. |
| **LAC** | ambiguous step 2025-09-23 3.07 → 09-24 6.01, ×1.958 | A basis step would be almost exactly 2.000; 1.958 is more consistent with a real move. Flagged, not excluded. Outside all three windows anyway. |
| **CRML** | median **$8.1M/day** — by far the thinnest name in the audit | Its +44.4% 5d on RVOL 5.79 is a low-liquidity move. It should not be read as basket evidence. |
| **UUUU** | 278 bars (starts 2025-08-13) | 20d fine; no long-run context. |
| **USAR** | 382 bars (starts 2025-03-14) | Same. |
| **MP, LAC** | $76.6M and $35.1M median $/day | Usable but thin relative to the rest. |

This audit was run the same day as `INTRADAY_CORPACTION_BASIS_V1`, which found ~16% of intraday
tickers carrying such seams and left the **1D store's seam status UNRESOLVED**. KLAC is a live
example. Long-window readings on small caps in this audit should be treated with that in mind.

---

## EXECUTIVE SUMMARY

- **Strongest basket: SEMICONDUCTORS**, and it is not close — 10/10 positive over 5d (median
  **+10.0%**), 7/10 over 10d, 9/10 carrying a bullish T-signal on the last bar, 9/10 with a B/VB
  volume bar in the last three sessions.
- **Second: CHINA-LINKED MEGA-CAP**, but narrow — AAPL and TSLA only, and 3 of its 6 names
  (NKE, SBUX, MMM) are in the bottom decile of their own 20-day range.
- **Weakest: AGRICULTURE and AEROSPACE — both 0/6 and 0/5 positive over 10 days.**
- ⭐ **There is NO broad pre-event positioning.** The three baskets a positive Trump–Xi outcome would
  most directly reward — China ADRs, agriculture (purchase commitments), aerospace (aircraft orders)
  — are the **three weakest** in the audit. The one basket that is moving is the one with the
  strongest *independent* explanation: the AI/chip capex cycle.
- ⭐ **Where activity exists, it is EXTENDED, not early.** AMD sits at **99% of its 20-day range,
  RSI 73, 0.2% below its 52-week high**; INTC at 93% / RSI 71. That is a move already made, not
  capital quietly accumulating ahead of an event.

**Answer to the headline question: the data does not support "the market is positioning for the
meeting." It supports "semis are in a strong, late-stage, well-owned move that would have happened
without the summit."**

---

## A NECESSARY WARNING ABOUT THE SIGNAL THAT FIRES MOST

The `PRICE × VOLUME` family (added to the app today) fires heavily in the strongest basket:
**20 fires across all 10 semiconductor names in the last three sessions — and 17 of those 20 are
VETO-class codes** (mostly `UPP`: price up 3 bars on rising volume).

`PV_MULTI_V1` was sealed on 2026-09-21 at k = 9 with **0 BUILD / 4 VETO_CANDIDATE / 5 NULL**, every
cell negative in MINE. `UPP` measured **−0.618 MINE / −0.573 VERIFY, negative in all four MINE
years**. So the shape currently decorating the strongest basket is one the app's own most recent
sealed research found *negative*.

Two things follow, and both matter:
1. **`UPP` clustering is not bullish confirmation.** Counting it as such would be reading a label
   the evidence contradicts.
2. **Nor is it a short signal.** The four VETOes are recorded, not applied; the family is
   descriptive, and inverting a −0.6pp descriptive cell into a trade is exactly the move the
   sealing discipline exists to prevent.

The honest reading: PV activity here confirms **that volume and price are rising together**, which
the raw RVOL and return columns already say. It adds no independent evidence.

**Correlated-family caution applied throughout:** `CD·30` and `CD·60` fire together on almost every
name in this audit and are counted **once**, not twice. OVD tokens fire on 40%+ of all bars
universe-wide (`project_signal_cooccurrence_map`), so their presence is context, not confirmation.
`volB` and `VOL7 M-level` are two readings of the same volume, counted once.

---

## TICKER TABLE

Activity score is descriptive (HIGH / MEDIUM / LOW / NONE) — it is **not** a model output.

| Ticker | Basket | Fresh signals (≤3d) | 5d | 10d | 20d | Volume (RV5) | Breakout state | Data quality | Activity | Comment |
|---|---|---|---|---|---|---|---|---|---|---|
| AMD | SEMI | T2G ×3, volB ×3, PVo:UPP ×3, Σ+ | +24.7% | +28.9% | +30.1% | 1.52 | **pos20 99%, −0.2% off 52wH** | ok | **HIGH** | Cleanest continuation in the audit — and the most extended |
| INTC | SEMI | T1G, volB ×4, PVo:UPP ×3 | +25.3% | +27.1% | +35.2% | 1.50 | pos20 93%, −14.5% off 52wH | ok | **HIGH** | Second leg; still below its own high |
| MRVL | SEMI | T2G ×5 straight, cont6 | +17.6% | +15.1% | +8.6% | 0.84 | pos20 94% | ok | MEDIUM | Price leads, **volume does not confirm** (RV5 0.84) |
| QCOM | SEMI | T1, WRP↑, volVB @ −2d | +7.8% | +15.1% | +20.8% | 1.65 | pos20 97% | ok | MEDIUM | Volume expansion is real (RV5 1.65) |
| MU | SEMI | T2G, volB, PVo:UPP @ −1d | +13.0% | +2.7% | +8.0% | 1.02 | pos20 88% | ok | MEDIUM | 5d move not yet echoed in 10d |
| AMAT | SEMI | T2G, volB, HO·30 | +9.4% | +2.1% | −5.7% | 1.31 | pos20 65% | ok | MEDIUM | Recovering off a lower base |
| LRCX | SEMI | T2G, HO·60 | +10.5% | −1.7% | −3.7% | 1.42 | pos20 65% | ok | MEDIUM | Same shape as AMAT — equipment lagging logic |
| NVDA | SEMI | T2G, cont5 | +7.8% | −1.3% | +5.9% | 0.95 | pos20 73%, −3.9% off 52wH | ok | MEDIUM | **Not leading.** Flat 10d while AMD ran +29% |
| AVGO | SEMI | T2G, HO·30 | +5.2% | +1.3% | −1.6% | 1.08 | pos20 66% | ok | LOW | Participating, not driving |
| KLAC | SEMI | T2G | +8.8% | −0.9% | −0.0% | 1.17 | pos20 72% | ⚠ **basis seam 06-12** | LOW | Long-window fields unusable |
| AAPL | MEGA | T4, volB, PVo:UPP @ −1d | +1.8% | +5.9% | +9.6% | 1.06 | **pos20 98%, −1.6% off 52wH** | ok | MEDIUM | Strong but at the top of its range |
| TSLA | MEGA | T1G, cont5 | +4.5% | +6.0% | +3.4% | 0.96 | pos20 79% | ok | MEDIUM | Steady; volume flat |
| CAT | MEGA | Z5, HO·30, ▼−2 | +4.1% | +0.3% | −1.4% | 1.18 | pos20 72% | ok | LOW | Bounce inside a range |
| MMM | MEGA | Z3, shake5 | +1.8% | −2.1% | −7.8% | 1.13 | pos20 23% | ok | LOW | No structure |
| NKE | MEGA | T9, volB, HO·30+HO·60 | −2.6% | −6.0% | −11.4% | 1.36 | pos20 13% | ok | LOW | Volume on **weakness** |
| SBUX | MEGA | Z2G, HO·60, rev7 | −4.2% | −9.2% | −11.4% | 1.38 | **pos20 8%, RSI 27** | ok | NONE | Actively breaking down |
| BABA | CHINA | T2G, L12, rev6 | +6.0% | +2.2% | −3.0% | 0.82 | pos20 62%, −39.9% off 52wH | ok | LOW | Best of the four — on **falling** volume |
| BIDU | CHINA | T2G, PVo:UPP, PVc:UPR | +0.3% | −7.3% | −1.1% | 0.82 | pos20 35% | ok | LOW | Two-day bounce only |
| JD | CHINA | T2G, OB·60, rev6 | +0.1% | −3.5% | −7.1% | 1.00 | pos20 24% | ok | LOW | No expansion |
| PDD | CHINA | Z5, L12, rev6 | −0.1% | −3.5% | −10.3% | 1.02 | pos20 14% | ok | NONE | Bottom of its range |
| DE | AGRI | Z11, ▼−2 | +0.5% | −1.3% | +5.8% | 0.98 | pos20 76%, −3.0% off 52wH | ok | LOW | Strongest agri name, but on **contracting** volume |
| ADM | AGRI | Z2G, ◆−3 | −3.5% | −1.5% | +3.8% | 1.26 | pos20 58% | ok | LOW | Volume without price |
| BG | AGRI | Z2G, ◆−3, shake4 | −7.1% | −5.1% | −0.4% | 1.49 | pos20 29% | ok | LOW | **Heavy volume on a decline** |
| NTR | AGRI | Z2G, volB, PVo:DIV, CD·30 | −3.2% | −6.2% | −1.0% | 1.01 | pos20 26% | ok | LOW | RVOL 2.11 today on a down bar |
| MOS | AGRI | Z2, volB, PVo:DIV, CD·30 | −3.9% | −6.9% | −1.4% | 1.17 | pos20 26% | ok | LOW | Same shape as NTR |
| CF | AGRI | Z2G, CD·30+60, rev7 | −6.1% | −7.6% | −4.9% | 1.05 | **pos20 6%** | ok | NONE | Weakest agri name |
| HON | AERO | Z11, ▼−2, rev6 | +2.5% | −1.5% | −4.4% | 1.52 | pos20 39% | ok | LOW | Volume up, price down |
| RTX | AERO | T2, ◆−3, rev7 | −0.5% | −3.2% | −7.4% | 1.24 | **pos20 14%, RSI 32** | ok | NONE | — |
| TDG | AERO | T2G, RC·30+RC·60+CD·60 | −0.1% | −4.5% | −7.6% | 1.44 | pos20 25% | ok | LOW | RC tokens are the only bullish note |
| GE | AERO | T2G, ▼−2, rev7 | +0.5% | −5.4% | −8.4% | 1.56 | pos20 25% | ok | LOW | Volume expanding into weakness |
| BA | AERO | T2G, PVo:VUP, rev7 | −4.3% | −5.2% | −6.1% | 1.56 | pos20 29% | ok | LOW | ⭐ The single most narrative-loaded name, and it is **down on all three windows with rising volume** |
| CRML | RARE | T1G, volVB, PVo:UPR, RC·30 | +44.4% | +28.2% | +31.2% | 1.89 | pos20 91% | ⚠ **$8.1M/day** | MEDIUM | Real move, but the thinnest name here |
| USAR | RARE | T1G, volB, PVo:UPP/UPR | +6.8% | −4.7% | −12.9% | 1.06 | pos20 34% | ⚠ 382 bars | LOW | Two-day bounce in a downtrend |
| LAC | RARE | T1G, volB, PVo:UPR | +2.4% | −1.3% | −5.7% | 1.05 | pos20 40% | ⚠ step 2025-09 | LOW | — |
| MP | RARE | T1, OB·30+OB·60+HO·30 | −1.3% | −8.3% | −16.7% | 1.05 | pos20 18% | ok | LOW | The bellwether is **falling** |
| ALB | RARE | Z10, ▼−2, MID | −1.8% | −10.6% | −21.2% | 1.19 | pos20 14% | ok | NONE | Worst 20d in the basket |
| UUUU | RARE | Z7, volB, OB·30+OB·60 | −0.3% | −15.0% | −18.8% | 1.27 | pos20 16% | ⚠ 278 bars | NONE | — |

---

## SECTOR BREADTH TABLE

| Basket | N | Bullish participation (10d>0) | Volume participation (RV5>1.1) | Fresh T-signal on last bar | Breakout participation (pos20>70) | Median 5d / 10d / 20d | Status |
|---|---|---|---|---|---|---|---|
| **SEMI** | 10 | **7/10** | 6/10 | **9/10** | **7/10** | **+10.0% / +2.4% / +6.9%** | **ACTIVE BREAKOUT → EXTENDED at the top** |
| **MEGA** | 6 | 3/6 | 4/6 | 3/6 | 3/6 | +1.8% / −0.9% / −4.6% | **MIXED** — two names carry it |
| **RARE** | 6 | 1/6 | 3/6 | 4/6 | 1/6 | +1.0% / −6.5% / −14.8% | **WEAKENING** (one thin outlier) |
| **CHINA** | 4 | 1/4 | **0/4** | 3/4 | **0/4** | +0.2% / −3.5% / −5.1% | **NO SIGNAL** |
| **AGRI** | 6 | **0/6** | 3/6 | **0/6** | 1/6 | −3.7% / −5.7% / −0.7% | **NO SIGNAL / distribution-shaped** |
| **AERO** | 5 | **0/5** | **5/5** | 4/5 | **0/5** | −0.1% / −4.5% / −7.4% | **NO CONFIRMATION** — volume up, price down |

**Concentration.** SEMI is the only basket whose strength is *broad*: its largest single 10-day
contributor (AMD, +28.9pp) is **31%** of the basket's positive total — the rest is spread across six
more names. Every other basket that is positive at all is **100% concentrated in one name**
(CHINA → BABA; RARE → CRML).

⭐ **Aerospace deserves its own line.** It is the only basket with **5/5 volume expansion** and
**0/5 positive 10-day returns**. Rising volume into falling price is the opposite shape from
accumulation, and it is the basket the "Boeing order" narrative depends on entirely.

---

## SIGNAL CHRONOLOGY — the order of events, not the count

**SEMI — the only basket with a coherent accumulation→expansion sequence.** The pattern repeats
almost identically across AMD / INTC / LRCX / KLAC / AMAT:

```
−9…−6d   Z-signals, L46/L5 weak-side labels, CD·30+CD·60          contraction / supply
−5…−4d   Z11 or Z2 with a WRP↓ / LST↓ shape, shake4-6              shakeout
−3d      T1 / T1G with volB and OB·60 (opening build)              demand appears ON volume
−2…0d    T2G repeating, volB repeating, PVo:UPP                    continuation
```
That is a textbook shakeout → reclaim → expansion order, and it is **5–9 sessions old**, not new.
AMD and INTC are furthest along it; AMAT and LRCX entered it 2–3 sessions ago and are the only
semis still near the middle of their 20-day range (pos20 65%).

**AGRI / AERO — the sequence runs the other way.**
```
−4…−3d   T-signals, volB, OB·60                                    attempted demand
−2…−1d   Z9 / Z1 with volVB, CD·30+CD·60                           supply on the higher volume
 0d      Z2G with V7 ◆−3 (a three-level volume collapse)           effort exhausted
```
ADM, BG and CF all print `V7:◆−3` on the last bar — volume falling three regime levels after a
high-volume down day. That is the closing of an attempt, not the opening of one.

**CHINA — no sequence at all.** BABA is the only one with a recent T-signal chain (T9 → T2G → T2G),
and it runs on **RV5 0.82** — the volume is *contracting* while the price ticks up. The other three
show alternating Z/T with no direction.

**RARE — divergent.** CRML has a genuine `T1G + volVB + RC·30` expansion on the last bar. MP, ALB
and UUUU are in the mirror-image sequence to the semis: Z-signals with CD tokens and falling prices.

---

## FRESHNESS

| Window | What is there |
|---|---|
| **TODAY (2026-09-21)** | SEMI: T2G on AMD, INTC, MRVL, AVGO, MU(Z5), LRCX, KLAC, AMAT + volB on AMD/INTC. MEGA: T4 AAPL, T1G TSLA. AERO: T2G on BA, GE, TDG — **all on down 5d**. AGRI: Z2G on ADM, BG, NTR + `◆−3`. RARE: T1G CRML, T1G USAR, T1G LAC, T1 MP. |
| **Last 3 days** | The semis' `volB + PVo:UPP` cluster (AMD, INTC, MU, AVGO, LRCX, AMAT). AAPL's volB + UPP at −1d. |
| **4–10 days ago** | The shakeout leg for semis (Z11/Z2 + WRP↓ + shake). BIDU/JD's OB·60 cluster. |
| **Stale >10 days** | Everything in the China basket's constructive column. |

---

## TOP 10 EVENT-SENSITIVE NAMES RIGHT NOW

1. **AMD** — the strongest and *most extended*: pos20 99%, RSI 73, 0.2% off its 52-week high, three
   consecutive volB days. **Sector-confirmed** (9/10 semis moving). **Late**, not early.
2. **INTC** — same sequence, less extended (14.5% below its own high). Stock-specific *and*
   sector-confirmed. Fresh (volB on 4 of the last 5 sessions).
3. **QCOM** — the cleanest *volume* confirmation in the audit (RV5 1.65, volVB two sessions ago) with
   pos20 97%. Sector-confirmed.
4. **CRML** — the largest move in the audit (+44.4% 5d) with a real `T1G + volVB` on the last bar.
   **Stock-specific, NOT sector-confirmed** (the rest of the basket is down), and the thinnest name
   here at $8.1M/day. Treat as a single-name event, not a rare-earth signal.
5. **AAPL** — pos20 98%, 1.6% off its high, volB at −1d. The most China-exposed mega-cap, and it is
   strong — but it has been strong throughout, and RV5 1.06 shows no *new* accumulation.
6. **AMAT** and 7. **LRCX** — the two semis still mid-range (pos20 65%) that entered the
   shakeout→reclaim sequence only 2–3 sessions ago. **The only names in the whole audit that look
   early rather than extended.**
8. **MU** — +13.0% over 5d but only +2.7% over 10d: the move is three days old, RV5 1.02.
9. **MRVL** — five consecutive T2G bars, pos20 94%, but **RV5 0.84**. Price without volume.
10. **TDG** — the only aerospace name with a constructive token set (`RC·30+RC·60+CD·60` = a reclaim
    of a prior high-volume level) on the last bar, against a −4.5% 10d. Watch-only; the basket
    around it gives no support.

---

## NO-CONFIRMATION / CONTRARIAN NAMES

Names the geopolitical story requires, where the data shows nothing or the opposite:

- **BA** — the single most narrative-loaded ticker for a China aircraft order. **−4.3% / −5.2% /
  −6.1%** across the three windows, pos20 29%, and RV5 1.56 — *rising volume into falling price*.
  No accumulation shape at any point in the 20-day window.
- **The entire AGRI basket** — 0/6 positive over 10 days, and the last bar carries `◆−3` volume
  collapse on ADM, BG and CF. If purchase commitments were being anticipated, this is where it
  would show first, and it does not.
- **PDD, JD** — bottom quartile of their 20-day range, no volume expansion.
- **BABA** — the one China name rising, and it does so on **contracting** volume (RV5 0.82), which
  is the opposite of accumulation.
- **NKE, SBUX** — the two most China-revenue-sensitive consumer names, both at pos20 ≤ 13% with
  RV5 > 1.35: volume is arriving **on weakness**.
- **NVDA** — flat over 10 days (−1.3%) while AMD ran +28.9%. The export-control bellwether is *not*
  leading the export-control basket.
- **MP** — the rare-earth bellwether, −16.7% over 20 days.

---

## SCENARIO MAPPING

| Scenario | Basket | Does current data show positioning? |
|---|---|---|
| **A · general de-escalation / tariff truce** | China ADRs + AAPL/TSLA + industrials | **NO for China** (0/4 with volume expansion, 0/4 above pos20 70). **PARTIAL for mega-cap** — AAPL and TSLA are strong, but both are long-standing trends with flat relative volume, and the China-consumer names (NKE, SBUX) are among the weakest in the whole audit. |
| **B · agriculture purchase commitments** | ADM BG DE MOS CF NTR | **NO — and the clearest "no" in the audit.** 0/6 positive over 10 days, `◆−3` volume collapse on three names on the last bar. |
| **C · Boeing / aviation orders** | BA + suppliers | **NO.** BA negative on all three windows with expanding volume. The basket is 0/5 on 10d with 5/5 volume expansion — distribution-shaped, not accumulation. |
| **D · chip / export-control relief** | NVDA AMD AMAT LRCX KLAC MU | **YES, but with a large caveat.** This is the only basket with broad participation. However the move is **already extended** (AMD 99% of range, RSI 73), NVDA — the name most directly exposed to export controls — is **flat**, and the sequence started 5–9 sessions ago. That timing and that leadership pattern fit the AI capex cycle at least as well as a summit trade, and nothing in the data distinguishes the two. |
| **E · rare-earth supply normalisation (inverse)** | MP USAR UUUU CRML | **WEAKLY CONSISTENT, NOT EVIDENCE.** MP −8.3%, ALB −10.6%, UUUU −15.0% over 10d — the basket *is* weak, which is the direction a de-escalation would imply. But the weakness is broad, 20 days old, and CRML (+28.2%) moves the opposite way inside the same basket. A pre-existing downtrend is not an inverse positioning signal. |

---

## VERDICT

**1. Is the market already positioning for a positive Trump–Xi outcome?**
**No — not in any way this data can distinguish from ordinary sector rotation.** Three of the four
baskets a positive outcome would most directly reward (China ADRs, agriculture, aerospace) are the
three weakest in the audit, two of them with **zero** names positive over 10 days. Broad pre-event
positioning would look like several narrative-linked baskets turning together; instead exactly one
basket is moving, and it is the one with the strongest independent explanation.

**2. Which sector shows the strongest evidence?**
**Semiconductors**, unambiguously: 10/10 positive 5d, 9/10 fresh T-signals, 9/10 with a B/VB volume
bar in three sessions, and the only basket whose strength is broad rather than one name (top
contributor = 31% of the positive total). **But it is late-stage**: the leaders sit at 93–99% of
their 20-day range and the sequence began 5–9 sessions ago.

**3. Which specific stocks show the cleanest pre-event activity?**
For *cleanliness of structure*: **AMD, INTC, QCOM**. For *earliness* — the more useful question when
an event has not happened yet — **AMAT and LRCX**, the only two names in the entire audit with a
fresh shakeout→reclaim sequence while still mid-range (pos20 65%).

**4. Which narrative baskets are NOT confirmed?**
**Agriculture (0/6), Aerospace (0/5), China ADRs (0/4 on both volume expansion and breakout).**
Aerospace is the sharpest contradiction: 5/5 volume expansion with 0/5 positive returns.

**5. Any inverse trade in rare-earth / critical minerals?**
**Directionally consistent, but it does not qualify as evidence.** MP, ALB and UUUU are all sharply
negative over 10 and 20 days, which is what a de-escalation would imply — but the decline predates
any meeting expectation, CRML runs +28% the other way inside the same basket, and three of the six
names carry data-quality flags. Calling this "the inverse trade appearing" would be fitting a story
to a pre-existing downtrend.

---

### What would change this verdict

Broad pre-event positioning is falsifiable and would look specific: China ADRs expanding volume
(currently 0/4), agriculture turning positive from its current 0/6, and BA reversing its
volume-up/price-down shape. None of the three is present as of 2026-09-21. If they appear together
in the coming sessions, that is the signal — the semiconductor move already in progress is not.

**Nothing in this document is a forecast, a ranking, or a recommendation. No fitted score, no
forward-looking column and no label was used as evidence; the inputs are raw returns, relative
volume, range position, and the existing descriptive signal layers, each correlated family counted
once.**
