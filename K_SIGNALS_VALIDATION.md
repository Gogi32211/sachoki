# „290509 — K Signals (BUY)" — 5-წლიანი ვალიდაცია ჩვენს DB-ზე

_TradingView ინდიკატორის 11 K-სიგნალი გადამოწმებული ჩვენი T/Z სვეტებით (lag-რეკონსტრუქცია +
faithful RSI-rising filter + price anchors), 3 აქცია-universe, 8.07M ბარი, 5 წელი. მეტრიკა:
median EXCESS vs universe + clip25 + per-year + IS/OOS + Wilson LB — არა in-sample NQ annotations._

სკრიპტი: `analysis/validate_k_signals.py` · ერთეული = % (fwd_10d excess) · ANALYSIS ONLY

> ⚠️ caveat: ჩვენი T/Z = Pine „260523"; ინდიკატორი „290509". core T1/T2/… სტანდარტულია და ემთხვევა,
> მაგრამ subtle განსხვავება შესაძლებელია. RSI-rising filter ჩართულია (faithful).

---

## 🔴 მთავარი ვერდიქტი — როგორც დაწერილია, **აქციებზე არ მუშაობს**

| K | n | medL | wLB | per-year | ვერდიქტი |
|---|---|---|---|---|---|
| K1 | 43.5k | −0.15 | 48.8 | **1/6** | ❌ |
| K1G | 33.6k | −0.09 | 48.9 | 3/6 | ❌ |
| K2 | 99.9k | −0.11 | 49.0 | 3/6 | ❌ |
| **K2G** | **389.6k** | −0.19 | 48.7 | 3/6 | ❌ (massively diluted) |
| K3 | 10.4k | +0.10 | 49.6 | 2/6 | ⚠️ marginal |
| K4 | 19.0k | −0.08 | 48.9 | 2/6 | ❌ |
| K5 | **0** | — | — | — | 💀 არასდროს ფიქსირდება |
| **K6** | 11.2k | **+0.33** | 51.0 | 3/6 | ⚠️ საუკეთესო, მაგრამ marginal |
| K9 | 2.0k | −0.24 | 46.8 | 3/6 | ❌ |
| **K10** „premium" | 1.5k | **−0.38** | 45.0 | 3/6 | ❌ („premium"-ი ყველაზე ცუდია) |
| K11 „premium" | 10.1k | +0.02 | 49.2 | 3/6 | ⚠️ ნული |

**0 / 11 K-ს აქვს მდგრადი დადებითი edge.** საუკეთესო (K6 +0.33) marginal-ია და მხ. 3/6 წელი.
**„premium" K10 უარყოფითია** (−0.38). win% ყველგან ~49-52% (ბაზის ფონზე ნული).

## 🧪 OR-dilution დადასტურდა (ზუსტად ნაწინასწარმეტყველი)
**K2G ფიქსირდება 389,598-ჯერ** — ფაქტობრივად ნებისმიერ T2G-ზე T4/T1/T1G-ის შემდეგ (broad anchor-ები
`T4[1]`, `T1[1]`, `T1G[1]` მარტო). edge განზავდა −0.19-მდე. tight pattern + broad anchor = base-rate.

## 🌍 NQ-სტატი აქციებზე **არ გადავიდა** (bragged sub-patterns standalone)

| sub-pattern (NQ annotation) | n | medL | per-year | რეალობა |
|---|---|---|---|---|
| K11① **T1\|T6→T2G** „avg10 **+35.25%**" | 1209 | **−0.07** | 3/6 | ❌ tail-inflated NQ |
| K2① **Z1G\|T4→T2** „avg10 **+16.8%**" | 3344 | **−0.43** | **1/6** | ❌ უარყოფითი! |
| K10① Z5\|T3\|Z3→T4 „w5 76%" | 147 | +0.63 | 2/3 | ⚠️ recent-only, wLB 44 |
| K1① Z1G\|Z2G\|Z2→T1 „w5 76%" | 668 | +0.15 | 2/6 (2022:−3.0) | ⚠️ bear-fragile |

➡️ **„avg10 +35%" = NQ futures-ის tail-inflated mean** (overlapping windows, ერთი instrument).
აქციებზე per-trade median = **−0.07 / −0.43.** ზუსტად ის `mean(clip)` ცდუნება, რაც სესიაში ვამხილეთ.

## 🟡 ერთადერთი ნამდვილი სიგნალი: K3① `Z3|T4|Z9→T3`
| n | win% | medL | m25L | wLB | IS/OOS | per-year |
|---|---|---|---|---|---|---|
| 545 | **74.1** | **+5.27** | +4.34 | **70.3** | +0.4/**+6.4** | 21:+1.1 **22:−2.9** 23:−0.1 24:+0.3 25:+7.0 26:+2.4 |

ეს **მართლა ძლიერია** — Wyckoff supply-test (Z3 supply → T4 demand engulf → Z9 inside test → T3 LPS).
**მაგრამ:** (1) **2022 bear-ში −2.9** (არა bear-robust), (2) **OOS +6.4 ≫ IS +0.4** — ძლიერად
2025-26 regime-skewed. „fail 0%!!" annotation **overstated**, მაგრამ edge **არსებობს** — ღირს ცალკე
ღრმა შესწავლა (per-year robustness + squeeze-tail), არა ბრმად აღება.

## 📌 დასკვნა
1. **როგორც დაწერილია — 0/11 K მუშაობს აქციებზე.** არქიტექტურა repaint-free და სუფთაა, მაგრამ
   **broad-anchor OR-dilution + in-sample NQ-mining** edge-ს ანულებს.
2. **„premium" K10/K11 ყველაზე სუსტია** — discretional „personal picks" ნულოვანი/უარყოფითი.
3. **NQ avg10 +35% ≠ stock edge** — tail-inflated single-instrument mining (იგივე ცდუნება).
4. **ერთადერთი ღირებული თესლი: `Z3|T4|Z9→T3` (და მისი T5/T9 ვარიანტები)** — Wyckoff supply-test,
   median +5.27, win 74%. მაგრამ 2022-fragile + regime-skewed → ცალკე ვალიდაცია სჭირდება.

> ერთი წინადადებით: **ინდიკატორის სტატისტიკა NQ-ზე in-sample-მაინინგია, broad-anchor-ით განზავებული —
> აქციებზე 0/11 K-ს აქვს მდგრადი edge; ერთადერთი გადარჩენილი თესლი Z3|T4|Z9→T3 supply-test-ია,
> ისიც regime-skewed. ზუსტად ის ხაფანგები, რაც სესიაში ვამხილეთ — win%/avg10 ცდუნებს, median+per-year წყვეტს.**

---

## 🟢 DEEP-DIVE: `Z3→T4→Z9→T3` supply-test — ერთადერთი ნამდვილი edge
_`analysis/supply_test_deep.py` — ვარიანტები + per-year + universe + path(MFE/MAE)._

**Canonical Z3→T4→Z9→T3:** median **+5.27**, win **74%**, n=545, Wilson LB 70, clip25 +4.34 →
**არა lottery-არტეფაქტი.** path: MFE+8.5 / MAE−4.4 · მხ. **1.3% კარგავს >25%** · worst −34%.

### Z3 supply ბარი არსებითია (Wyckoff მექანიზმის დადასტურება)
| ვარიანტი | medL | win% |
|---|---|---|
| **Z3**→T4→Z9→T3 | **+5.27** | 74 |
| Z4→T4→Z9→T3 (engulf) | +2.80 | 65 |
| Z5→T4→Z9→T3 (upthrust) | +1.75 | 59 |
| **T4→Z9→T3 (Z3 ამოღებული)** | **+0.58** 💀 | 54 |
| (Z3\|Z4\|Z5)→T4→Z9→T3 [pooled] | +3.32 | 67 · **5/6 yr** · n=1287 |

➡️ Z3-ის ამოღება edge-ს ანულებს → **supply(Z3)→demand-engulf(T4)→quiet-test(Z9)→reversal(T3)** = ნამდვილი absorption, არა უბრალოდ engulf+inside.

### trigger: T3 > T5 > T9; და confirmation ამატებს
→T3 **+5.27** · →T5 +2.63 · →T9 +1.22 · →trigger-ის გარეშე (Z9-ზე) +1.76 (n=5535).

### universe — russell2k ყველაზე მდგრადი
| universe | medL | per-year |
|---|---|---|
| **russell2k** | +5.12 | **5/6** (21:+2.7 22:−2.3 23:+0.2 24:+2.0 25:+7.2 26:+1.8) |
| nasdaq | +4.67 | 2/4 (2022:−3.5) |
| sp500 | +5.69 | მხ. 2025 (n=62) |

### ⚠️ caveat-ები
- **2022 bear უარყოფითი** ყველა ვარიანტში (−2.3…−3.3) — size down risk-off-ში.
- 2025 დიდი წელია (+7.0), მაგრამ 2021/23/24/26-იც დადებითია (არა მხოლოდ regime).
- tail მსუბუქი (reversal-from-absorption, არა falling knife): MAE −4.4.

### 📌 best config
**`Z3→T4→Z9→T3` на russell2k** (5/6 yr, +5.12, 75%) ან pooled **`(Z3|Z4|Z5)→T4→Z9→T3`** (n=1287, 5/6 yr).
ეს არის ერთადერთი productize-ღირსი setup ინდიკატორიდან — repaint-free, ჩვენი `sig_z3/t4/z9/t3` სვეტებით.
