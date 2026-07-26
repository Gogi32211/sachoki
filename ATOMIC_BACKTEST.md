# Atomic bull profile — path-aware backtest

_weak-close gap-up = T-signal AND close=O AND gap∈(G2,G3). Entry next-open, gap-aware s15/t100, $500k floor, glitch-screen, episode-dedup. Percent units._

---

## ვერდიქტი — ეს არის საუკეთესო ედჯი მთელი კვლევიდან
**„weak-close gap-up" (T-signal + close=O + gap G2/G3), s15/t100 exit-ით:**
- **დადებითი expectancy სამივე უნივერსში დიდი n-ით:** sp500 **+0.84** (n19k, win 55%, med +0.74), russell2k **+0.58** (n68k), nasdaq +0.36. **EO+gap ვარიანტი კიდევ უკეთესი** (sp500 +0.83, r2k **+0.70**, nas +0.51).
- **დადებითი median** (acc_tr-ისგან განსხვავებით) — სტაბილური **swing-ედჯია, არა ლატარეა** (P(+50%) დაბალი, win 50-55%).
- **per-year: დადებითი 5/6 წელს** — 2021 +0.23, 2023 +0.28, **2024 +1.15**, 2025 +0.88, 2026 +0.84 — **მხოლოდ 2022 bear-ში უარყოფითი (−0.45).**

### რატომ სჯობს ყველაფერს
| | n | universes+ | positive years | median |
|---|---|---|---|---|
| acc_tr(TEST) | ~6k | regime-dep | 2025 only | უარყოფითი |
| double-gap | ~800 | regime-dep | 2025 only | ~0 |
| **weak-close gap-up** | **12k–68k** | **სამივე +** | **5/6 (only 2022−)** | **დადებითი** |

➡️ **ატომურმა დაშლამ მიგვიყვანა ყველაზე სუფთა, მაღალ-n, რეჟიმ-მდგრად ედჯამდე** — მხოლოდ `close=O` + `gap` (ორი ატომი). ძლებს 2024-საც (სადაც ყველა სხვა ჩავარდა), მხოლოდ 2022 bear-ში აზარალებს. დათქმა: maxloss დიდია micro-ში (gap-through) → მცირე ფრაქციული საიზი + bear-ში stand-down (regime detector უკვე RISK_OFF-ს აჩვენებს).

---

## Exit grid (entry next-open, horizon 10d)
| profile · universe | config | n | EXPECT | med | win% | P(+50%) | maxloss |
|---|---|---|---|---|---|---|---|
| weakclose_gap (close=O & gap) · sp500 | s15/t100 | 18996 | **0.84** | 0.74 | 55.0 | 0.1 | -34.9 |
| weakclose_gap (close=O & gap) · sp500 | s12/t50 | 18996 | **0.8** | 0.7 | 54.7 | 0.2 | -34.9 |
| weakclose_gap (close=O & gap) · nasdaq | s15/t100 | 36039 | **0.36** | 0.0 | 50.0 | 0.6 | -82.4 |
| weakclose_gap (close=O & gap) · nasdaq | s12/t50 | 36039 | **0.37** | -0.14 | 49.1 | 1.1 | -74.4 |
| weakclose_gap (close=O & gap) · russell2k | s15/t100 | 68012 | **0.58** | 0.4 | 52.4 | 0.3 | -84.6 |
| weakclose_gap (close=O & gap) · russell2k | s12/t50 | 68012 | **0.57** | 0.3 | 51.7 | 0.7 | -84.6 |
| EO_gap (escape & O & gap) · sp500 | s15/t100 | 12636 | **0.83** | 0.75 | 55.0 | 0.1 | -34.8 |
| EO_gap (escape & O & gap) · sp500 | s12/t50 | 12636 | **0.79** | 0.71 | 54.7 | 0.2 | -34.8 |
| EO_gap (escape & O & gap) · nasdaq | s15/t100 | 23133 | **0.51** | 0.13 | 50.6 | 0.6 | -82.4 |
| EO_gap (escape & O & gap) · nasdaq | s12/t50 | 23133 | **0.52** | 0.0 | 49.7 | 1.2 | -73.7 |
| EO_gap (escape & O & gap) · russell2k | s15/t100 | 44009 | **0.7** | 0.5 | 52.9 | 0.4 | -84.6 |
| EO_gap (escape & O & gap) · russell2k | s12/t50 | 44009 | **0.68** | 0.4 | 52.3 | 0.7 | -84.6 |

## Per-year (weak-close gap-up, s15/t100, EXPECT(n)) — nasdaq+russell2k
| year | EXPECT | n |
|---|---|---|
| 2021 | 0.23 | 10427 |
| 2022 | -0.45 | 20625 |
| 2023 | 0.28 | 16302 |
| 2024 | 1.15 | 20848 |
| 2025 | 0.88 | 24593 |
| 2026 | 0.84 | 11256 |
