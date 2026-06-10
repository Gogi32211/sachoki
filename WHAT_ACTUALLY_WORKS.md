# რა მუშაობს რეალურად — კონსოლიდირებული დასკვნები (vol-spike zone სამყარო)

_ერთი სესიის ყველა რიგორული ვალიდაცია ერთ ადგილას: long-vs-short, SMX case, confluence,
signal-edges. ყველაფერი 5-წლიან 8M-ბარიან DB-ზე, universe-drift მოჭრილი, median + clip25 +
per-year + IS/OOS. ერთეული = % (fwd_10d excess vs universe median). ANALYSIS ONLY._

წყაროები: [EXIT_SEQ_LONG_VS_SHORT.md](EXIT_SEQ_LONG_VS_SHORT.md) · [SMX_CASE_STUDY_2025.md](SMX_CASE_STUDY_2025.md) ·
`analysis/exit_seq_*.py`, `delta_leads_*.py`, `fri34_load_abs_eb.py`, `confluence_*.py`

---

## 0. 🧭 ერთი სტრუქტურული ჭეშმარიტება
**vol-spike zone-exit-ის შემდეგ ფასი mean-reverts ქვევით.** universe baselines (median fwd_10d):
sp500 +0.34 · russell2k −0.21 · nasdaq −0.50. zone-exit-ის long base ყველგან უარყოფითი,
short base ყველგან დადებითი.
→ **წესი: fade strength, buy absorbed weakness.** (ძალის დადევნება აგებს; შთანთქმული სისუსტის ყიდვა იგებს.)

## 1. ⚖️ როგორ ვსაჯავთ (honest filter — ეს ყველაზე მნიშვნელოვანია)
- ❌ **win% ცდუნებს** — breakout median უარყოფითია, ამიტომ მაღალი win + უარყ. expectancy ნორმაა.
- ❌ **Wilson-LB-მაც კი ვერ გაფილტრა** (L34→VBO↑→GAP↑ "OOS 83%" → expectancy −2.5%).
- ❌ **pivot სიგნალები lookahead-ია** (Williams centered pivot მომავალ ბარებს სჭირდება — არა-tradeable).
- ❌ **mean clip(+500) ლატარიის კუდით იბერება** — long-ს აზვიადებს, short-ს tail-რისკს მალავს.
- ✅ **ნამდვილი ფილტრი = median LIFT + clip25-mean + per-year bear-survival + IS≈OOS.**

## 2. 🟢 რა მუშაობს LONG
| setup | n | clip25-lift | per-year |
|---|---|---|---|
| **Δ↑→PARA·p→CONSO** (spike↑) 🥇 | 42 | +8.3 | **5/5** (2022 bear-შიც) |
| **ABS→EB↑→{LVBO/PSAR/CONSO}** (spike↑) | ~40 | +6.5…+7.0 | 4/4 (2022 სუსტი) |
| **L34→Ab→R2L / c=O** (spike↓ spring) | ~40 | +6.8…+7.2 | →100% (2025) უმჯობესდება |
| **SQ→PARA·p→PSAR** (VB↓ spring) | 39 | +5.0 | 24/25/26 მტკიცე |

core ატომი: **R2L (oversold) + c=O (weak-close) + structure** = capitulation reclaim.

## 3. 🔴 რა მუშაობს SHORT (უფრო ადვილი — სტრუქტურულად)
short base **დადებითი** ყველგან → tailwind. უფრო დიდი-n, 6/6-წლიანი sequence-ები ვიდრე long-ში:

| short setup | n | clip25-lift | per-year |
|---|---|---|---|
| **c=O → V×5/V×10** (blow-off fade) 🥇 | 100–180 | +10.8…+13.4 | **6/6** |
| **R2L → V×5** | 109 | +12.7 | 6/6 |
| **PSAR→Ab→V×10** (VB↓) | 32 | +11.2 | 3/3 |

მექანიზმი: **vol-spike breakout რომელიც სუსტად იხურება მოცულობის კლიმაქსზე = exhaustion → fade.**
⚠️ **tail-რისკი:** ~1 trade 20-დან >50% squeeze (−350%). unclipped mean მაინც დადებითი (edge ნამდვილია),
მაგრამ **size + hard stop სავალდებულო.**

## 4. 🧩 Confluence — count ≠ ხარისხი
- ❌ **naive count anti-predictive** (cnt 0→+0.04, cnt 4→−0.21) — ღერძები ეწინააღმდეგებიან.
- ❌ **chase stack უარესდება დაგროვებით** (thrust −0.08 → thrust+flow+tz −0.17).
- ✅ **ერთადერთი კოჰერენტული tradeable წყვილი: oversold (R2L) + absorption (LOAD/FRI34) = +0.32 median, 5/6 წელი, IS≈OOS.**
- ⚠️ `flow` (delta surge/flip) **აზიანებს** oversold-ს (+0.13 → −0.07) — დელტა-ფლიპი ხმაურია.

**წესი:** confluence ეხმარება მხოლოდ **კოჰერენტული** (ერთი ლოგიკა) + **reversion-aligned** სიგნალებით.

## 5. 📇 Signal cheat-sheet (განმარტება წყაროდან + 5-yr edge)
| სიგნალი | რა არის | medL | ვერდიქტი |
|---|---|---|---|
| **LOAD** | დიდი vol + პატარა move + ვიწრო spread + ძლ. დახურვა = Wyckoff loading | **+0.17** | მცირე **მდგრადი** tilt (5/6, OOS>IS) |
| **FRI34** | BLUE (vol z↑ + RSI ბრტყელი) & L34 (mზარდი vol up-close bullish body) | **+0.14** | მცირე **მდგრადი** (5/6) |
| FRI34&LOAD | ორივე ერთ ბარზე | +0.21 | საუკეთესო კომბო |
| **ABS** | vol-bucket ≥2 საფეხ. ნახტომი | +0.02 | ნული |
| **EB↑** | დიდი სხეული + პატარა კუდი + close>open (marubozu thrust) | −0.10 | მარტო **chase** |
| **ABS→EB↑** (raw) | absorption მერე thrust | −0.01 | ნული (edge **zone-ზეა**) |
| oversold (R2L) | RSI2 ძირში | +0.13 | სუსტი მარტო |
| **oversold+absorption** | R2L + LOAD/FRI34 | **+0.32** | ✅ best coherent tilt |

## 6. ⚠️ ხაფანგები (კარგად გამოიყურება, არ მუშაობს)
1. **delta leads price (SMX ნარატივი)** — `d_flip_bull` მარტო ხმაური (1/6); crash-ის შემდეგ ყიდვა **−20% (0/6)** falling knife; `d_div_bear` short run-up-ზე **−22%** squeeze. **survivorship trap.**
2. **win%-ის „✅ holds"** — `T1G→EB→VBO↑` (2022:14%), `c=O→LVBO→CONSO` (მხ. 2024-25), `atomic→atomic→GAP↑` (2023:42%) — bear-ში ინგრევიან.
3. **2025-regime artifacts** — ~75% composites / ~57% sequences მხოლოდ 2025 risk-on-ში დადებითი.
4. **VB "is king" შებრუნდა** — 5 წელზე VB ყველაზე ცუდი bucket.
5. **naive confluence / EB chase** — მეტი აღგზნება = უარესი forward.

## 7. 🎯 პრაქტიკული decision rules
1. **მიმართულება:** vol-spike exit-ი default-ად **short-ს უჭერს მხარს** (base +). long მხოლოდ 2 ვიწრო ოჯახში (Δ↑→PARA·p→CONSO; spring-reclaim).
2. **Long entry:** absorbed weakness — **R2L oversold + LOAD/FRI34** (+0.32 tilt), არა thrust/breakout chase.
3. **Short entry:** **blow-off fade** — vol climax (V×5/10) + weak close (c=O) ტოპზე. size პატარა, stop მკაცრი (squeeze tail).
4. **არასდროს:** crash-ის შემდეგ delta-flip-ის ყიდვა (falling knife); run-up-ის divergence-ის short (squeeze); pivot-ზე დაყრდნობა (lookahead); confluence-count-ის მაქსიმიზაცია.
5. **სიგნალების როლი:** LOAD/FRI34/ABS→EB↑ = **confluence/entry-quality tilt-ები** broader სისტემაში, **არა standalone trigger-ები.**

---
> **TL;DR:** ამ სამყაროში edge მცირეა და ასიმეტრიული — **fade strength / buy absorbed weakness.**
> short უფრო ადვილი (base +, 6/6 sequences), long მხოლოდ ვიწროდ. confluence ეხმარება მხოლოდ
> კოჰერენტულად (oversold+absorption). win% ცდუნებს; median+per-year+IS/OOS წყვეტს. SMX-ნაირი
> survivor-ი ანატომიისთვის შესანიშნავია, predict-ისთვის — საფრთხე.
