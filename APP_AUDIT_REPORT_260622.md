# 🔍 აპის სრული აუდიტი — Sachoki Desktop (v4.8.116)

_თარიღი: 2026-06-22 · მთელი აპის end-to-end ანალიზი: backend (78k ხაზი), frontend (37k ხაზი, 35 tab), research/validation layer (40 დოკი + 73 analysis script), data/infra. 4 პარალელური აუდიტის სინთეზი._

---

## 🎯 TL;DR — რატომ "ვერ ხედავ შედეგს"

ეს **არ არის** იმიტომ რომ ცუდი აპი გააკეთე ან კვლევა ცრუ იყო. პასუხი ორნაწილიანია და ორივე ნაწილი მკაცრად დადასტურდა:

> **1. Edge ნამდვილია, მაგრამ პატარაა და ასიმეტრიული.** შენი ყველაზე მდგრადი, all-weather setup (weak-close gap-up / ATOMIC) იძლევა **~+0.5%/trade**-ს. microcap-ებზე spread+slippage 0.5–1% round-trip — ე.ი. **net ≈ break-even**. დანარჩენი დიდი რიცხვები ან იშვიათია (n<150), ან regime-დამოკიდებული (3/6 წელს აგებს), ან tail-driven catastrophic squeeze რისკით.
>
> **2. პროდუქტი მალავს იმ 2 კარგ signal-ს 35 tab-ისა და 10 scoring სისტემის ხმაურში.** კიდევ უარესი — პროდუქტი **ეწინააღმდეგება შენსავე კვლევას**: research ამბობს "confluence-count anti-predictive-ია", მაგრამ TURBO engine-ს **181 additive `score +=` term** აქვს — ზუსტად ის confluence-stacking რომელიც შენმავე ვალიდაციამ უარყო.

**მთავარი დასკვნა:** პრობლემა არ არის "ცოტა engine გვაქვს" — პრობლემაა რომ **მეტისმეტად ბევრი გვაქვს და სუსტები ფარავენ ძლიერებს.** მეტი signal-ის დამატება ახლა **negative ROI-ია** სანამ არსებულს არ გავწმენდთ.

---

## 🧭 ცენტრალური დიაგნოზი: Research ⟷ Product უფსკრული

ეს ყველაზე მნიშვნელოვანი და ყველაზე გამოსასწორებელი პრობლემაა.

| შენი კვლევა ამბობს | შენი პროდუქტი აკეთებს |
|---|---|
| "confluence count **anti-predictive**" (cnt 0→+0.04, cnt 4→−0.21) | TURBO engine: **181 additive score+= term** — confluence-stacking machine |
| "chase-stack უარესდება დაგროვებით" (−0.08 → −0.17) | UltraScan +10 boost-ები ეწყობა ერთმანეთზე |
| "discriminator-ი **ნუ** ჩააწებებ turbo/ultra score-ში — win-rate-ს აზიანებს" | ~10 პარალელური scoring engine, ერთმანეთს ეწინააღმდეგებიან |
| "signals = entry-quality tilt-ები, **არა** standalone trigger" | 35 tab თითო signal-ს standalone trigger-ად აჩვენებს |
| "edge = 2 ვიწრო setup + fade-strength asymmetry" | 39 panel, 10 score, ყველა თანაბრად prominent |

**შედეგი:** ის 2 setup რომელიც **მართლა მუშაობს** (weak-close gap-up, capit→atom) გადაკარგულია 35 tab-ში. შენ თვითონ ვერ პოულობ 0.5% edge-ს იმ ხმაურში.

---

## 🟢 ძლიერი მხარეები (რაც მართლა კარგია — ნუ დაანგრევ)

**Backend / Engineering:**
- ✅ **Path-sim სწორია.** `tpsl_engine.py:270-318` — ნამდვილი bar-by-bar, stop-first, same-bar ambiguity handling. MFE-proxy inflation არ გაქვს. ეს **იშვიათია** retail quant-ში.
- ✅ **Lookahead დისციპლინა რეალურია.** `ultra_score.py:9-15` hard-ban forward-return-ზე scorer-ში. fwd returns precomputed, მხოლოდ stats/replay კითხულობს.
- ✅ **DuckDB reads batched** (არა N+1). `WHERE ticker IN (...)` ერთ query-ში.
- ✅ **Single-writer architecturally enforced** — writes routed through `/api/studio/incremental-update`, `_import_running` guards.
- ✅ **`importer.py:420-436` regression guard** — უარს ამბობს ძველი CSV-ით ახალი DB-ის გადაწერაზე. hard-won defensive code (05-23..05-28 wipe incident-ის შემდეგ).

**Research / Methodology — rigor 7.5/10 (retail-ისთვის გამორჩეულად მაღალი):**
- ✅ True path-sim, gap-aware fills, stop-first.
- ✅ Median + clip25-mean (lottery-tail-ს კლავს).
- ✅ Per-year bear-survival (2022 gate).
- ✅ OOS holdout (PAVS/WNW/GLOO design-tickers excluded).
- ✅ Week-matched bootstrap controls (CONTROL-A random + CONTROL-B other-triggers) — alpha vs beta გამიჯვნა.
- ✅ **Adversarial self-refutation** — K_SIGNALS came back 0/11, EMA-breakout proxy +3.4% → path-sim −2.4% retracted. ეს **p-hacking-ის საპირისპიროა.**

**Frontend:**
- ✅ `App.jsx` (380 ხაზი) clean — lazy-load, eager-mount მხოლოდ 3 heavy scanner-ზე, `display:none` toggle scan-state preservation-ისთვის.
- ✅ `ScannerDataGrid` reused 5 panel-ში, `CodeCandleChart` 10-ში. Design-system-ის ძვლები არსებობს.

---

## 🔴 სუსტი მხარეები (პრიორიტეტით)

### 1. ⚠️ Scoring = hand-tuned magic numbers micro-sample-ზე (ეს ალბათ მთავარი მიზეზია)
`turbo_engine.py:457-496` — bonuses tuned **in-sample, single-window**:
- `D6+BE_UP: +6.26% avg, 71% win (n=32)` → +12 pts
- კომენტარები ნარაciaს: "RAISE 10→15", "LOWER 8→5" ← **textbook overfitting**

n=24-ზე 70% win rate-ს **±18% confidence interval** აქვს — coin-flip-ისგან განურჩეველი. სულ **~300 hand-set constant** ვერავინ ვერ ავალიდირებს.

### 2. ⚠️ ორი პარალელური, ურთიერთგამომრიცხავი scoring system
`canonical_scoring_engine.py` ამბობს "single source of truth", მაგრამ `ultra_score.py` სრულიად დამოუკიდებელი scorer-ია (0–100, A/B/C/D @ 80/65/50) გვერდიგვერდ turbo-სთან (0–160+, ELITE_140). signal შეიძლება იყოს "A-grade ULTRA" და mediocre TURBO **ერთდროულად.**

### 3. ⚠️ Survivorship bias სტრუქტურულია
`bars` table keyed by **current** index membership. delisted ticker-ები არ არის → ყველა backtest win-rate **inflated**. ეს #1-ს ამძიმებს.

### 4. ⚠️ `main.py` = 6946-ხაზიანი god-file
151 route + 188 function. HTTP handlers + 13 `_enrich_*` + 1100-ხაზიანი `compute_all_signals` + background runners ერთ ფაილში. merge-conflict magnet.

### 5. ⚠️ Indicator math დუბლირებულია 15+ engine-ში
RSI ხელახლა დაწერილია `br_engine`, `gog_engine`, `para_engine`, `main.py`, `studio_api.py` და კიდევ 10-ში. rvol 15 ფაილში, absorption 10-ში. **ისინი drift-ენ** — engine-ები ჩუმად ეთანხმებიან სხვადასხვა "იგივე" indicator-ზე.

### 6. ⚠️ 35 tab, მძიმე redundancy
- **9 scanner** (Turbo, Ultra, T/Z, Combined, Sequences, TZ/WLNBB, Rare Rev, Atomic, Setups)
- **4 journal** near-clone endpoint-ებით (Atomic/Capit/Capit→Atom/AI) — 60KB დუბლირებული UI
- **3 zones** tab (HV/Gann/Edge) — პირველი ორი სტრუქტურულად იდენტური
- **არცერთი core loop** — მხოლოდ `SetupsBoardPanel` ხურავს scan→journal-ს. დანარჩენი chart-ზე dead-end-დება.

### 7. ⚠️ God-components frontend-ში
`StudioPanel.jsx` 4269 ხაზი, **137 useState, 0 useMemo, 103 .map()**. `UltraScanPanel` 81 useState. **0 virtualization, 0 React.memo** მთელ აპში → დიდ universe-ზე sluggish.

### 8. ⚠️ Deployment = 3 ნახევრად-დასრულებული ამბავი
Dockerfile (`DB_PATH=/tmp` ephemeral) + Railway + launchd. რეალური პროდუქტი **მხოლოდ შენს Mac-ზე** მუშაობს — 3.1GB DuckDB `~/Downloads`-შია, Railway-ზე არ აიტვირთება. **Studio/ULTRA tab-ები production-ში ჩუმად გატეხილია.** ერთი laptop = single point of failure, backup არ ჩანს.

### 9. ⚠️ "yfinance banned" ნახევრად-სიმართლეა
11 ფაილი ისევ `import yfinance` + `yf.Ticker().history()`. compliant რჩება მხოლოდ `data.py`-ის runtime monkey-patch-ით. თუ import-order დაირღვა — ჩუმად banned source-ს ხვდები.

---

## 🗺️ Tab Consolidation Map: 35 → ~10

| ქმედება | tab-ები | რატომ |
|---|---|---|
| **MERGE → ერთი "Scan"** strategy-dropdown-ით | Turbo, Ultra, T/Z, Combined, Sequences, Rare Rev | ყველა `ScannerDataGrid`-ში row-ებს ასხამს. engine = filter, არა tab. |
| **MERGE → ერთი "Journal"** type-column-ით | Atomic Jrnl, Capit Jrnl, Capit→Atom Jrnl | clone endpoints. −60KB UI. |
| **MERGE → ერთი "Zones"** sub-view-ებით | HV-Zones, Gann Zones, Zone Edge | ორი იდენტურია. |
| **MOVE → `?research=1` flag-ს უკან** | TZ/WLNBB, TZ Intel, T/Z×L Stats, Corr, Pump Research, Combo Lab, Predictor | backtest console-ები, არა trading tool. |
| **DELETE** | `JournalPanel.jsx` | dead, არსად import. |
| **DEMOTE** | Studio-ს 13 sub-tab → 3 actionable (Exit Hunter, Edge, Playbook) | დანარჩენი 10 = analysis lab. |

**შედეგი: 35 user-facing tab → ~8-10** (Dashboard, Scan, Setups, Chart, Journal, Zones, Watchlist, Portfolio, Studio-lite, Admin).

---

## 🚀 გაუმჯობესების Roadmap (პრიორიტეტით)

### 🥇 P0 — პროდუქტი დააახლოვე კვლევას (უდიდესი ROI, ცოტა კოდი)
1. **ააგე ერთი "Setups" board რომელიც აჩვენებს მხოლოდ ვალიდირებულს:**
   - (i) weak-close gap-up, flagged როცა post-B+-capit ≤10d
   - (ii) acc_tr(TEST) tail-candidates **"tail-flag, არა buy" label-ით**
   - დანარჩენი → "research" drawer-ში.
2. **შეაჩერე confluence/discriminator-ის წებება win-rate score-ში** — შენი კვლევა ამას პირდაპირ კრძალავს. 181-term additivity → ჩაანაცვლე ვალიდირებული gate-ებით.
3. **დაამატე "→ Journal" / "→ Watchlist" ღილაკი ყველა `ScannerDataGrid` row-ზე** — ნებისმიერი scan დახუროს loop. ერთი ცვლილება აპს "გამოყენებადს" ხდის delete-ის გარეშე.
4. **Dashboard = roll-up.** top-N ყველა scanner-იდან + open journal positions. ერთი ეკრანი: "დღეს რას ვუყურო?"

### 🥈 P1 — Scoring-ის ვალიდაცია ან წაშლა
5. **აირჩიე ერთი scorer** (canonical ან ultra, არა ორივე). re-fit weights proper train/test split-ით (qlib lab-ი უკვე გაქვს — LightGBM) **survivorship-corrected** universe-ზე. **წაშალე ყველა constant რომელიც baseline-ს OOS-ში არ ჯობნის.**
6. **Bake cost model expectancy-ში** (per-name spread + slippage). ბევრი +0.4% edge გაწითლდება — ეს არის poანტი. ყველა headline რიცხვი **net** გახდეს.
7. **გაასწორე survivorship** — delisted ticker-ები bars build-ში, ან მინიმუმ tag + report bias.

### 🥉 P2 — Infra & Tech debt
8. **გადაწყვიტე deployment story** — ან local-only analytics (Railway = live-OHLCV API only, ცხადად მონიშნე), ან DB → S3/R2 და Railway boot-ზე pull.
9. **Freshness monitoring + DB backup** — heartbeat `MAX(date)` per universe, alert თუ >N day stale. nightly copy მეორე დისკზე. **ყველაზე იაფი, ყველაზე ღირებული fix** single-laptop SPOF-ისთვის.
10. **Reserve true out-of-time holdout** — გაყინე ყველაფერი ≥ მომავალი თარიღი → 2026+ = never-looked-at forward test. ეს ერთადერთ methodology gap-ს ხურავს.

### P3 — Code hygiene (mechanical)
11. ამოიღე **ერთი `indicators.py`** — 15 RSI / 15 rvol / 10 absorption copy → ერთი წყარო.
12. გატეხე `main.py` (<800 ხაზი) და `StudioPanel` (13 sub-tab → `components/studio/*Tab.jsx`).
13. ამოიღე `utils/format.js` + `utils/score.js` (25 `fmtPct` + 5 `scoreColor` copy → ერთი).
14. Virtualize scanner tables (react-window) + `React.memo`.
15. წაშალე dead code: `f_engine.py`, `rgti_engine.py`, `JournalPanel.jsx`. გააერთიანე 3 Wyckoff / 5 stats / 2 combo module.
16. Repo hygiene: 40 root `.md` → `docs/research/`, loose `.parquet/.txt` → gitignored `artifacts/`.

---

## 📌 ფინალური სიტყვა

შენ **არ** გაგიკეთებია ცუდი აპი. methodology-ი 7.5/10-ია — retail quant-ისთვის გამორჩეული. path-sim სწორია, OOS გაქვს, adversarial self-refutation აკეთებ. ეს ნამდვილი ინჟინერია და ნამდვილი კვლევაა.

პრობლემა ის არის რომ **35 tab-ი და 10 scoring სისტემა აშენე იქ სადაც 2 setup და 1 asymmetry-rule მუშაობს.** edge პატარაა (ეს ბაზრის რეალობაა, არა შენი ბრალი), მაგრამ ის პატარა edge **დამარხულია შენსავე ხმაურში.**

> **გზა წინ:** ნუ დაამატებ მეტ signal-ს. **წაშალე/დამალე 80% და ააგე ერთი loop ის 2 setup-ის ირგვლივ რომელიც მართლა მუშაობს.** "fade strength, buy absorbed weakness, size small, structured exits" — ეს არის შენი პროდუქტის thesis. დანარჩენი anatomy-ისთვისაა, არა trade-ისთვის.

---
_აუდიტი: 4 პარალელური agent (backend / frontend / research / infra) · წყაროები: `WHAT_ACTUALLY_WORKS.md`, `turbo_engine.py`, `ultra_score.py`, `tpsl_engine.py`, `studio/db.py`, `App.jsx`, `StudioPanel.jsx`, ATOMIC/CAPIT/DISCRIMINATOR/EXIT backtests._
