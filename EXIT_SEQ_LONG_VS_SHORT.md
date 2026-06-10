# Exit-sequence miner — LONG vs SHORT, ღრმა ვალიდაცია (6 პანელი)

_3 volume class (spike ≥5× · spike 2–5× · VB) × 2 მიმართულება (exit↑ · exit↓), 5-წლიანი
ისტორია, first-exit ნიმუში. მეტრიკა: **median lift + clip25-mean lift + per-year**, არა win%/LB.
ერთეული = პროცენტი (fwd_10d). ANALYSIS ONLY — production კოდი არ შეცვლილა._

სკრიპტები: `analysis/exit_seq_allpanels.py`, `exit_seq_robust.py`, `exit_seq_short.py`, `exit_seq_short_tail.py`

---

## 0. მეთოდი — რატომ არა win% და LB

- პანელი `first_only`-ს იყენებს (ზონის **პირველი** exit) → ეს ციფრები განსხვავდება ძველი
  `EXIT_SEQ_DEEP.md`-ისგან (იქ ყველა გამეორებული exit იყო).
- **mean +18/+20% ხელოვნურია** — `+500%` clip-ის ლატარიის კუდი ბერავს. ამიტომ headline =
  **median lift** + **clip25-mean** (კუდი მოჭრილი ±25%). edge clip25-შიც გადარჩა → ცენტრიც იწევს.
- win% და Wilson-LB-მაც კი ვერ გაფილტრა (L34→VBO↑→GAP↑ "OOS 83%" → expectancy −2.5%).
  **clip25-lift + per-year bear-survival = ერთადერთი ჭეშმარიტი ფილტრი.**

---

## 1. 🔑 მთავარი სტრუქტურული აღმოჩენა — long baseline ყველგან წააგებს, short ყველგან მოგებს

clip25-mean **base** (სიგნალის გარეშე, მთელი exit-population):

| volume class | exit↑ **LONG** base | exit↑ **SHORT** base | short win% |
|---|---|---|---|
| **spike ≥5×** | **−2.14** ❌ | **+2.13** ✅ | 56.0% |
| **spike 2–5×** | +0.06 | +0.51 ✅ | 52.2% |
| **VB class** | −0.81 ❌ | +0.81 ✅ | 53.0% |

**vol-spike zone-exit-ის შემდეგ ფასი საშუალოდ ქვევით დრიფტავს.** ანუ:
- breakout-ის **ყიდვა** (long) იწყება **მინუსიდან** — sequence-მა ჯერ ეს ხვრელი უნდა ამოავსოს.
- breakout-ის **fade/short** იწყება **პლუსიდან** — sequence-ს tailwind აქვს.

> ეს ზუსტად ის არის, რაც შენ შენიშნე: **short-ის პოვნა უფრო ადვილია** — არა შემთხვევით, არამედ
> იმიტომ რომ ამ ზონებიდან exit-ი სტრუქტურულად mean-reversion-ია (ექსჰაუსტი), არა გაგრძელება.

---

## 2. ⚠️ Terminology-ის ხაფანგი (აუცილებლად)

პანელის **exit↓**-ის "win = price UP after breakdown" → ეს **failed-breakdown / spring = LONG**
პოზიციაა (ფასი ბრუნდება ზევით), **არა short.** ამიტომ ეკრანზე ნანახი ✅-ები exit↓-ში
spring-**ლონგებია**, არა shorts. ნამდვილ short-ს ცალკე ვზომავთ (§4): short P&L = −fwd_10d.

---

## 3. LONG მხარის გადარჩენილები (clip25-lift + per-year)

base უარყოფითია → ცოტა sequence ღირს. ის რაც ნამდვილად ძლებს:

| sequence | zone/dir | n | win% | clip25-lift | per-year win% |
|---|---|---|---|---|---|
| **Δ↑→PARA·p→CONSO** 🥇 | spike↑ | 42 | 71 | **+8.32** | 22:83 23:57 24:70 25:67 26:71 — **5/5 ✅** |
| **ABS→EB→LVBO** | spike↑ | 43 | 67 | +7.01 | 22:40 23:69 24:83 25:70 — 4/4 (22 სუსტი) |
| **ABS→EB→PSAR** | spike↑ | 39 | 72 | +6.50 | 22:50 23:78 24:83 25:64 — 4/4 |
| **L34→Ab→R2L** (spring) | spike↓ | 39 | 72 | +7.16 | 22:50 23:62 24:80 25:**100** — უმჯობესდება |
| **L34→Ab→c=O** (spring) | spike↓ | 43 | 70 | +6.76 | →100 (2025) |
| **SQ→PARA·p→PSAR** (spring) | VB↓ | 39 | 74 | +4.97 | 24:82 25:75 26:88 |

**ერთადერთი exit↑ long რომელსაც სრულად ვენდობი: `Δ↑→PARA·p→CONSO`** — 5/5 წელი დადებითი, 2022 bear-შიც.
spring-ლონგების core ატომი: **R2L (oversold) + c=O (weak-close gap) + L34/SQ (structure)** = capitulation reclaim.

---

## 4. 🎯 SHORT მხარე — შენი ჰიპოთეზა დადასტურდა (და უფრო ძლიერად)

short P&L = −fwd_10d. რჩება გაცილებით **მეტი, უფრო დიდი-n, 6/6-წლიანი** sequence ვიდრე long-ში:

| short sequence | zone/dir | n | short-win% | clip25-lift | per-year (short-win%) |
|---|---|---|---|---|---|
| **c=O → V×5 → exit** 🥇 | spike2–5×↑ | 179 | **78.2** | +10.82 | 21:64 22:85 23:79 24:80 25:71 26:90 — **6/6 ✅** |
| **V×5 → V×10** | VB↑ | 124 | 82.3 | +13.35 | 21:67 22:89 23:94 24:76 25:82 26:87 — **6/6 ✅** |
| **R2L → V×5 → exit** | spike2–5×↑ | 109 | 81.7 | +12.68 | 22:91 23:85 24:90 25:71 26:85 — **6/6 ✅** |
| **c=O → V×5 → exit** | VB↑ | 91 | 83.5 | +13.60 | 5/5 ✅ |
| **PSAR→Ab→V×10** | VB↓ | 32 | 87.5 | +11.23 | 23:86 24:80 25:100 — 3/3 |

**long-vs-short კონტრასტი (საუკეთესოები):**

| | LONG best | SHORT best |
|---|---|---|
| sequence | Δ↑→PARA·p→CONSO | c=O→V×5→… |
| n | 42 | **179** |
| per-year | 5/5 | **6/6** |
| win% | 71 | **78** |
| clip25-lift | +8.3 | **+10.8** |

short იგებს **ოთხივე ღერძზე**: n, დაფარვა, win%, lift.

### 🔬 მექანიზმი — blow-off-top fade
ყველა მტკიცე short-ის core: **vol-spike breakout-UP რომელიც სუსტად იხურება (c=O) მოცულობის
კლიმაქსზე (V×10)** = ექსჰაუსტი / blow-off → fade. კლასიკური exhaustion-short.
base უკვე ქვევით დრიფტავს (mean-reversion), სიგნალი (weak close + volume climax) ამახვილებს.

---

## 5. ⚠️⚠️ SHORT-ის tail-რისკი — გულახდილი caveat (clip25 მალავს!)

clip25 short-ისთვის **საშიშია**: squeeze (+500% mover) = −500 short-ზე, რომელიც −25-მდე იჭრება.
ამიტომ unclipped-ი გავამოწმე (`exit_seq_short_tail.py`):

| short setup | win% | clip25 | **UNCLIPPED** | worst single | >25% ზარალი | >50% ზარალი |
|---|---|---|---|---|---|---|
| spike2–5↑ c=O→V×5 | 62 | +4.5 | **+4.4** ✅ | **−348%** | 6.5% | 2.4% |
| VB↑ V×5→V×10 | 74 | +10.5 | **+11.9** ✅ | −403% | 7.5% | 4.7% |
| spike↑ c=O→V×5 | 66 | +6.8 | **+7.8** ✅ | −215% | 6.4% | 3.8% |

**კარგი ამბავი:** unclipped short mean **მაინც დადებითია** (+4…+12) — squeeze-ების მიუხედავად.
ანუ edge **არა clip-ის არტეფაქტი.** ზოგჯერ unclipped > clip25, რადგან down-tail (კრახები −50/−90% →
short +50/+90%) აჭარბებს squeeze-ებს.

**ცუდი ამბავი:** ~**1 trade 20-დან** არის >50% squeeze (−348%, −403% ერთეული).
short-ის distribution = **ბევრი პატარა მოგება + იშვიათი კატასტროფული ზარალი** (positive-skew **შებრუნებული**).
→ short-ისთვის **დივერსიფიკაცია + hard stop + პატარა per-trade size** სავალდებულოა.
ერთი კონცენტრირებული short-ი ერთ squeeze-ზე წიგნს კლავს.

---

## 6. volume-class დასკვნა

- **spike ≥5×** — long base ყველაზე ცუდი (−2.14), მაგრამ lift ყველაზე დიდი (+7…+8). short base +2.13.
  მაღალი რისკი / მაღალი ჯილდო; სიგნალი აქ ყველაზე გადამწყვეტია.
- **spike 2–5×** — base ~ნული; lift +4…+6; **short-ში ყველაზე დიდი-n (179, 109) 6/6-წლიანი** sequence-ები.
  ყველაზე საიმედო short-ბაზა.
- **VB class** — base მსუბუქი; long lift +3.5…+5; short lift +13 (V×5→V×10). სტაბილური.

---

## 7. 📌 პრაქტიკული დასკვნა

1. **short-ის პოვნა მართლა უფრო ადვილია** — სტრუქტურულად (base +), რაოდენობრივად (179 vs 42 n),
   დროულად (6/6 vs 5/5 წელი). მაგრამ short = **მაღალი win% + იშვიათი კატასტროფა** → size/stop კრიტიკულია.
2. **საუკეთესო short:** `c=O → V×5/V×10` blow-off fade (spike2–5× / VB exit↑) — 6/6 წელი, n>100.
3. **საუკეთესო long:** `Δ↑→PARA·p→CONSO` (spike↑) — 5/5 წელი, ერთადერთი breakout-long რომელსაც ვენდობი.
4. **საუკეთესო spring-long:** `L34→Ab→R2L` (spike↓) — წლიდან წლამდე უმჯობესდება.
5. პანელის "✅ holds" (win/LB) **არ ნიშნავს edge-ს** — `T1G→EB→VBO↑` (2022:14%), `c=O→LVBO→CONSO`
   (მხოლოდ 2024–25), `atomic→atomic→GAP↑` (2023:42%) ჯერ კიდევ ✅-ს აჩვენებენ, მაგრამ bear-ში ინგრევიან.

> ერთი წინადადებით: **vol-spike exit-ები mean-revert ქვევით → short-ს structural tailwind აქვს და
> blow-off-fade (c=O + volume climax) 6/6 წელი მუშაობს; long მხოლოდ ორ ვიწრო ოჯახში ღირს
> (Δ↑→PARA·p→CONSO და spring-reclaim). win%-ი ცდუნებს — clip25-lift + per-year-ი წყვეტს.**
