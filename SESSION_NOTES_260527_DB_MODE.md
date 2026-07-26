# Sachoki Screener — 2026-05-27 სესიის სრული შემაჯამება

> ვერსია: v4.8.116 · TZ_WLNBB Pine 260523 v3.5
> ფაილი: ეს დოკუმენტი ჩატის გასაგრძელებლად. ახალ ჩატში ჩასვი ბმული:
> `/Users/sachoki/Desktop/sachoki-desktop/SESSION_NOTES_260527_DB_MODE.md`

---

## 1. რა გავაკეთეთ ამ სესიაში (მაღალდონეზე)

ულტრა სკანერი ადრე ორ რეჟიმში იყო:
- **Live (slow)** — Massive API-დან ფაცხავდა + Pine-ის ექვივალენტურ ანალიზს ატარებდა inline (~30-60 წუთი)
- **DB (instant)** — Studio DuckDB-დან კითხულობდა ცე ლი მე ბ ას (~1-2 წამი) მაგრამ DB არ ხდებოდა განახლება არც სიგნალები არ ჯდებოდა

ამ სესიის შემდეგ:
- **DB-instant რეჟიმი ახლა ცოცხალია** — დღევანდელი ბარები შემოდის და ყველა Pine-ის სიგნალი თვითონ გამოითვლება
- **ღილაკი "🔄 Update DB"** დაემატა Ultra Scan პანელში — ერთი დაჭერით ბაზრის დახურვის შემდეგ DB განახლდება (~20-30 წუთი)
- **3 universe-ი სრულად განახლდა** SP500/NASDAQ/Russell2K → ყველა 2026-05-26 თარიღამდე
- **სრული Pine სიგნალების ნაკრები** — L88, 260308, ULTRA v2 (eb_bull/fbo_bull/best_long...), PARA, FLY, Delta order-flow (d_strong_bull, d_surge_bull და ა.შ.) — ყველაფერი enricher-ში დაემატა

---

## 2. ფაილის ცვლილებები (რა შეიცვალა და სად)

### Backend

#### `studio/incremental_delta.py` (ახალი ფაილი)
- **ფუნქცია:** `incremental_delta_refresh(universes)` — ყოველდღიური "delta append" ლოგიკა
- რას აკეთებს:
  1. ყოველი ticker-ისთვის DB-დან ვიღებთ MAX(date)
  2. `api_bar_signals(ticker, '1d', 200, universe)` — 200 ბარი fetch (Pine warm-up)
  3. ვტოვებთ მხოლოდ ბარებს `date > last_date`-ით
  4. ვაკონვერტებთ DB-row dict-ში `bar_to_row()` + `_COL_MAP`-ით (იგივე გზა რასაც bulk_export CSV → import აკეთებს)
  5. `INSERT INTO bars` ავტომატური ID-ით, DELETE+INSERT idempotency-ისთვის
  6. ბოლოს `enrich_universe(universe)` ეშვება — RSI, ATR, avg_vol_20d, PARA, FLY, Delta, ULTRA v2, 260308/L88
- **დადასტურებული**: AAPL 2026-05-22 ბარი ახალი გზით აპროდუცირებს იდენტურ მნიშვნელობებს რასაც CSV-import-ი ადგენდა (470/470 cells = 100% match)
- გასაღები პარამეტრი: `_WARMUP_BARS = 200` — Pine სიგნალები 150-ზე ნაკლები კონტექსტით ვერ კონვერგდება სწორად

#### `studio_api.py`
- `_run_incremental()` → ახლა იძახებს `incremental_delta_refresh()` (და არა ძველი `_incremental_refresh`-ს)
- `/api/studio/incremental-update/status` ახლა delta progress-ს კითხულობს `/tmp/studio_incremental_delta_progress.json`-დან

#### `studio/ultra_db_scan.py` — DB-instant scanner
- **`max_age_days=7` filter დაემატა** (ფიქსი) — ადრე ეს პარამეტრი იყო მაგრამ არ გამოიყენებოდა, ამიტომ delisted ticker-ები 6-თვის ძველი ფასებით ჩნდებოდნენ
- **SQL aggregation** sig_ages გამოთვლისთვის — 3000+ ticker × 50 სიგნალი ხდება ~2 წამში (ადრე pandas loop-ით 18 წამი იყო)
- **`_UI_KEY_TO_DB_COL` alias map დაემატა** — DB-ში `sig_best` შენახულია, ხოლო frontend filter checks `best_sig` → alias map ხდის aliased copy:
  - `best_sig ← sig_best`, `strong_sig ← sig_strong`, `buy_2809 ← sig_buy`, `sig3g ← sig_3g`
  - `cd/ca/cw/g1/g2/g4/g6/g11 ← sig_cd/.../sig_g11`
  - `preup66...preup50 ← sig_p66...sig_p50`, `predn* ← sig_d*`
  - `gog_g1p/g2p... ← g1p/g2p...` (DB-ში bare cols)
  - 80+ alias სულ
- **`tz_t1g, tz_t2g, ..., tz_any_t, tz_any_z, tz_bull_flip` aliases sig_ages-ში** — frontend N=lookback filter ამიტომ მუშაობს
- **String aliases**: `tz_wlnbb_volume_bucket ← vol_bucket`, `tz_wlnbb_l_signal ← l_sig`, `tz_wlnbb_ne_suffix/wick_suffix/bar_body_wick/bar_gap_range/bar_line5`
- **swing_type ← swing_type_3** alias (DB-ში `swing_type` ცარიელია, რეალური ღირებულება `swing_type_3`-შია — Williams 3-3)
- **`_EMIT_AGE_FIELDS`** — `ad_fresh_age`, `wyc_in_tr_age`, `prebreak_ready_age` etc. direct row fields რათა `evHit()` JS filter იმუშაოს

#### `studio/importer.py`
- `_COL_MAP`-ში დაემატა `"L88": "sig_l88"`, `"SIG_260308": "sig_260308"`
- `UNIVERSE_CSV_MAP`-ში `russell2k` უკვე იყენებს `russell2k_signals_5y.csv` (ნაცვლად ძველი `russell2k_full_signals_v3.csv`-ისა)

#### `studio/enricher.py` — ახალი Pine-equivalent computation
- **ახალი ფუნქცია `_compute_pine_engines(df)`** გამოიძახება `enrich_ticker_df()`-დან
- იხდის 5 self-contained ენჯინს:
  1. **`compute_260308_l88(df)`** → `sig_260308`, `sig_l88`
  2. **`compute_ultra_v2(df)`** → `eb_bull/bear`, `fbo_bull/bear`, `bf_buy/sell`, `ultra_3up/3dn`, `best_long/short` (10 ცვლადი)
  3. **`compute_para_series(df)`** → `para_prep/start/plus/retest` (4)
  4. **`compute_fly_series(df)`** → `fly_abcd/cd/bd/ad` (4)
  5. **`compute_delta(df)`** → 23 დელტა სიგნალი (`d_strong_bull/bear`, `d_absorb_bull/bear`, `d_div_bull/bear`, `d_cd_bull/bear`, `d_surge_bull/bear`, `d_blast_bull/bear`, `d_vd_div_bull/bear`, `d_spring`, `d_upthrust`, `d_flip_bull/bear`, `d_orange_bull`, `d_blast_bull_red`, `d_blast_bear_grn`, `d_surge_bull_red`, `d_surge_bear_grn`)
- ყველაფერი silent fallback to 0 თუ ენჯინი ვერ მუშაობს
- `ENRICH_COLUMNS`-ში დაემატა ~37 ახალი ცვლადი

#### `ultra_signal_parser.py` და `ultra_score.py` — `pd.NA` fix
- ორივე ფაილში `_truthy()` ფუნქცია გადაიწერა — ადრე `v == ""` რეიზდებოდა `TypeError: boolean value of NA is ambiguous` როდესაც DB row-ში `pd.NA` მნიშვნელობა გვქონდა
- `_is_nan()`-მა ახლა `pd.isna(v)` ცდილობს ჯერ
- ეს არღვევდა `compute_ultra_score()`-ს ყველა ULTRA score = None ხდებოდა

#### DB schema (DuckDB ALTER TABLE)
- დაემატა 35 ახალი column:
  - `sig_l88 SMALLINT`
  - `sig_260308 SMALLINT`
  - ULTRA v2 (10): eb_bull/bear, fbo_bull/bear, bf_buy/sell, ultra_3up/3dn, best_long/short
  - Delta (23): d_strong_bull/bear, d_absorb_bull/bear, d_div_bull/bear, d_cd_bull/bear, d_surge_bull/bear, d_blast_bull/bear, d_vd_div_bull/bear, d_spring, d_upthrust, d_flip_bull/bear, d_orange_bull, d_blast_bull_red, d_blast_bear_grn, d_surge_bull_red, d_surge_bear_grn

### Frontend

#### `frontend/src/components/UltraScanPanel.jsx`
- **`KEEP_ALWAYS`-ში დაემატა 22 ცვლადი** — localStorage cache slim row-ს არ უნდა ჩაშალოს ULTRA/Beta/Bull/scan_date/etc.
- **`Update DB` ღილაკი** — DB instant mode-ში მარჯვნივ ჩანს, ერთ დაჭერით ეშვება `incremental_delta_refresh`, progress real-time გამოჩნდება
- **`dbInfo` state** — DB-ის ბოლო თარიღი + bars რაოდენობა Source-row-ში ჩანს ("📊 8.21M bars, last updated 2026-05-26")
- **EMA filters fix** — `_gt_ema50` etc. ახლა იყენებს `r.price_gt_50` flag-ს თუ `r.ema50` სრულდება (DB-ში raw EMA-ები არ ინახება, მხოლოდ flag-ები)

### Configuration

#### `incremental_delta` defaults
- გაშვება ხდება `api_bar_signals(bars=200)`-ით — Pine warm-up
- enricher ეშვება ყოველი universe-ის ჩაშვების შემდეგ ერთხელ
- `today_str` hard cap — არ ჩაშვება ბარები რომელიც დღევანდელი თარიღის შემდეგაა

---

## 3. რა მუშაობს / რა არ მუშაობს DB-instant რეჟიმში

### ✅ მუშაობს (~80+ filter)

| ჯგუფი | სიგნალები |
|---|---|
| TZ engine | T1...T12, Z1...Z12, T1G/T2G/Z1G/Z2G, ANY T, ANY Z, TZ→3 (`tz_bull_flip`), TZ→2 (`tz_attempt`), W (`tz_weak_bull`) |
| Score bands | 0-20, 21-40, 41-60, 61-80, 81-100 |
| Direction | BULL / BEAR / ALL |
| VABS | BEST★, STRONG, V×20, V×10, V×5, VBO↑, ABS, CLB, LD |
| Wyckoff (VABS legacy) | NS, SQ, SC, ND |
| Combo 2809 | BUY, 🚀, 3G, HILO↑, VA, ↑BIAS, SVS, CON |
| F/G | CD, CA, CW, G1, G2, G4, G6, G11, SBC |
| GOG context | G1P, G2P, G3P, G1L, G2L, G1C, G2C, G3C, ★GOG+ |
| WLNBB | FRI34/43/64, L34/43/22/64, L555, L1L2, BL, B, N, NE |
| WLNBB extended | CCI, CCI0R, CCIB, BO↑/↓, BX↑/↓, BE↑/↓, RH, RL, PP |
| L-signal panel | ANY L, L1...L6, L22, L34, L43, L64, FRI34, FRI43, L555, L1L2 |
| Vol bucket (TZ/WLNBB) | VB, B, N |
| Suffix | NE (E), WK↑/↓/↕ |
| BW (body/wick) | X, M, S, J, TB, BB, F, XF, MF |
| GR (gap/range) | G1, G2, G3, V, C, N |
| L5 (VIX/PSAR/RSI2) | L5∗, VX, PB, VR, R2L, R2H, R2X |
| Wick X | X1G, X1, X2, X3 (X2G მხოლოდ CSV-ში, არ ღირს) |
| ULTRA v2 | BEST↑, FBO↑/↓, EB↑/↓, 4BF/4BF↓, 3↑ (enricher-დან ახლა) |
| 260308 + L88 | სიგ_260308, L88 (UPDATE-ით backfilled + enricher) |
| Delta | ΔΔ↑, Δ↑, B/S↑, Ab↑, dSPR, T↓, NS (delta), cd↑, FLP↑, ORG↑, ΔΔ↑R, Δ↑R, Δ↓G, ΔΔ↓G, ND (delta) — enricher-დან |
| PARA | PREP, PARA, PARA+, RETEST |
| FLY | ABCD, CD, BD, AD |
| PREUP/PREDN | P66, P55, P89, P50, P3, P2, ANY P / D66, D55, D89, D50, D3, D2, ANY D |
| Price vs EMA | P>200, P>89, P>50, P>20, P<200, P<89, P<50, P<20 |
| RSI | RSI≤35, RSI≥70 |
| RTB Phase | A-Build, B-Turn, C-Ready, D-Late |
| 260523 | AD-FRESH ★, AD-CLUSTER ★★ |
| WYC Phase | All, SPRING, UTAD, SOS, ACC_TR, DIST_TR, MARKUP, MKDN |
| Swing | HL, LL, HH, LH, Any pivot |
| PREBREAK | PRIME★, READY, WATCH, LVBO, W-PHASE, WVF |
| WYC additional | In TR (`wyc_in_tr`), SOW (`wyc_sow`) |
| Macro | No penalty / Penalty (`pb_macro_penalty`) |
| Profile | Sweet Spot, Building, Watch |
| B-family | B1...B11, ANY B |
| F-family | F1...F11, ANY F |
| Volume | <100K, 100K+, 500K+, 1M+, 5M+ |
| N= lookback | 1d, 3d, 5d, 10d (sig_ages-ით) |

### ⚠️ არ მუშაობს DB-instant რეჟიმში

| ჯგუფი | მიზეზი |
|---|---|
| **RGTI** (LL, UP, ↑↑, ↑↑↑, ORG, GRN, GC) + **SMX** | **LIVE-ში ისედაც disabled** — `turbo_engine.py` line 1260: "extra API calls slow scan". ეს ფილტრები არასოდეს მუშაობდა არსად. |
| **AKAN (A), NNN (N), MX, GOG_sig** | `gog_engine.compute_gog_signals()` ფუნქცია იყენებს 7 პრერექიზიტ DataFrame-ს (sig_df, wlnbb, f_sigs, vabs, ultra260, ultraV2, combo). Live scan ერთად აპროდუცირებს ყველაფერს. DB-ში გადასატანი — ნახევარი დღის სამუშაოა. |
| **CTX_*** (LDS, LDC, LDP, LRC, LRP, WRC, SQB, BCT) | იგივე — `gog_engine`-ის output-ია. |
| **Sectors** (XLC, XLY, XLP...) | DB-ში `sector` სვეტი NULL-ია. Frontend lazy-fetch აკეთებს `/api/ticker-info-batch`-ით (top 200 by score). რამდენიმე წამში ლოდინი საკმარისია. |
| **Cross-engine ⚡×2+/×3+/×4+** | სხვა ფილტრებზე გადახედვა — გადასაბანაო |

---

## 4. ბაზის სტრუქტურა (DB-instant pipeline-ის შემდეგ)

### Studio DuckDB
- ფაილი: `/Users/sachoki/Downloads/studio_analytics.duckdb`
- ზომა: ~2.4 GB
- ცხრილი: `bars` (320+35 = 355 columns ახლა)
- universe-ები:
  - **sp500**: ~610 ticker, ~741,900 rows (2021-05-26 → 2026-05-26)
  - **nasdaq**: ~3,297 ticker, ~3,052,782 rows
  - **russell2k**: ~5,000 ticker, ~4,419,564 rows
- სულ: ~8.2M bars, 3,713 unique tickers

### დღიური ციკლი
```
T+0 16:00 ET   ბაზრის დახურვა
T+0 17:00 ET   APScheduler (main.py lifespan) ცდილობს autorun-ს Mon-Fri
              ↓
              _run_incremental(['sp500','nasdaq'])
              ↓
              incremental_delta_refresh()
              ↓
              ფარგლავს api_bar_signals(bars=200)-ით
              ↓
              INSERT + enrich_universe → 30 ახალი სიგნალი დაითვლება
              ↓
T+0 17:30 ET   DB ახლა 2026-05-26-მდე
```

⚠️ **შენიშვნა**: APScheduler მუშაობს მხოლოდ თუ uvicorn server up-ია 17:00 ET-ზე. ლოკალურ macOS-ზე ეს ნიშნავს რომ ლეპტოპი უნდა იყოს ჩართული + server დახურული არ უნდა იყოს. ხელით განახლება: ულტრა პანელში "🔄 Update DB" ღილაკი.

---

## 5. ცნობილი bug-ები რომელიც გადავწყვიტეთ

| Bug | მიზეზი | ფიქსი |
|---|---|---|
| Score = ULTRA ერთი და იგივე მნიშვნელობით | `ultra_db_scan.py` fallback იყენებდა `final_bull_score`-ს. Score = turbo_score = bull_score = ერთი. | `compute_ultra_score()` fallback-ი — ULTRA ცალკე გამოითვლება |
| SPLIT universe აჩვენებდა მთელ NASDAQ-ს | ძველი production build `backend/static/`-ში | npm run build + cp dist/* static/ |
| PM% სვეტი არ ჩანდა | სრულად დანამატი | `premarket_cache.py` + `/api/premarket` endpoint + `pmData` state + 15-min interval refresh |
| stale 6-თვის ძველი ფასები DB-instant-ში (e.g. MCTA $29.36 from 2025-11-11) | `max_age_days=7` parameter არსად არ გამოიყენებოდა | filter დაემატა `latest = latest[date >= cutoff]` |
| ULTRA და Beta ცარიელი ("—") localStorage cache-დან გადატვირთვაზე | slim cache მხოლოდ `=== 1` მნიშვნელობებს ინახავდა, ULTRA=42 დაბრუნდებოდა undefined | `KEEP_ALWAYS` set-ში დაემატა 22 cvlavi |
| sig_ages computation 18 wami (slow) | pandas groupby loop 50 signal × 3300 ticker × 20 bars | SQL conditional MIN aggregation → 2.3 wami |
| `pd.NA` boolean ambiguous error → ULTRA = None | `_truthy()` ცდილობდა `v == ""`-ს pd.NA-ზე | `pd.isna(v)` ჯერ შემოწმდება |
| Filter "BEST★", "STRONG", "BUY" etc. იძახდა 0 ticker-ს | DB-ში `sig_best`, frontend filter `best_sig` — name mismatch | `_UI_KEY_TO_DB_COL` alias map (80+ entries) |
| TZ filters (T2G, T6 etc.) ცარიელი | sig_ages dict-ში `tz_*` key-ები არ იყო | `_filter_key()` mapping `sig_t2g → tz_t2g` ემიტს ორივე ფორმატს |
| EMA filter P>50 etc. ცარიელი | DB-ში `ema50` სვეტი არ არსებობს, frontend `r.ema50 > 0` ცდილობდა | SIG_GROUPS custom function ახლა `r.price_gt_50` flag-ს ცდილობს ჯერ |
| AD-FRESH, PREBREAK, WYC filters ცარიელი | `evHit(col)` ცდილობდა `r.col_age` — ეს field არ ემიტდებოდა DB-mode-ში | `_EMIT_AGE_FIELDS` set + direct `<col>_age` field-ები row-ში |
| `incremental.py` INSERT-ი `id=NULL` rows-ით | _compose_row მხოლოდ 30 column-ს წერდა + ID-ი არ ენიჭებოდა | `incremental_delta.py` ახალი მოდული — bar_to_row + _COL_MAP იგივეს რასაც CSV import |
| L88 + 260308 ფილტრები ცარიელი | CSV-ში იყო, მაგრამ importer-ის _COL_MAP-ში არ ემუშავებოდა | `"L88": "sig_l88"`, `"SIG_260308": "sig_260308"` დაემატა + DB schema ALTER + UPDATE-ით backfill |
| Delta signals (d_strong_bull, d_surge_bull etc.) ცარიელი | CSV-ში არც იყო — turbo_engine-ის live-only | enricher-ში `_compute_pine_engines()` ემატავს 23 დელტა + 10 ULTRA v2 + 4 PARA + 4 FLY column-ს |

---

## 6. გასაშვები პროცედურა (როცა აპლიკაცია გადატვირთვა გჭირდება)

### უვიკორნ server-ის გაშვება
```bash
cd /Users/sachoki/Desktop/sachoki-desktop/backend
.venv/bin/uvicorn main:app --host 127.0.0.1 --port 8080
```
ან ბექგრაუნდში:
```bash
cd /Users/sachoki/Desktop/sachoki-desktop/backend
nohup .venv/bin/uvicorn main:app --host 127.0.0.1 --port 8080 > /tmp/uvicorn.log 2>&1 &
```

### Frontend-ის ხელახალი ბილდი
```bash
cd /Users/sachoki/Desktop/sachoki-desktop/frontend
npm run build
cp -r dist/* /Users/sachoki/Desktop/sachoki-desktop/backend/static/
```

### DB განახლება ხელით
```bash
# ვარიანტი 1: UI ღილაკით — Ultra Scan → DB (instant) → "🔄 Update DB"
# ვარიანტი 2: API
curl -X POST http://127.0.0.1:8080/api/studio/incremental-update \
  -H "Content-Type: application/json" \
  -d '{"universes":["sp500","nasdaq","russell2k"]}'

# Status
curl -s http://127.0.0.1:8080/api/studio/incremental-update/status | python3 -m json.tool
```

### URL-ები
- **Production app**: http://127.0.0.1:8080 (FastAPI serves built frontend from backend/static/)
- **Vite dev server**: http://localhost:5173 (`npm run dev` — ცოცხალი reload, მაგრამ source-ის ფაილებიდან წაიკითხავს ე.ი. შეიძლება ვერ ნახო ბოლო ცვლილებები თუ build-ი არ გაგიკეთებია)

---

## 7. ღია სამუშაო (TODO ახალ ჩატში)

1. **Sectors enrichment** — `bars.sector` სვეტი არ ივსება. `/api/ticker-info-batch` endpoint მუშაობს, მაგრამ frontend lazy-fetch ერთჯერ ეშვება მხოლოდ top 200 ticker-ისთვის. სრული 3700+ ticker-ის sectors-ის გადატანა DB-ში (yfinance) — ერთ-ჯერ ცალკე ბექგრაუნდ ჯობი.

2. **AKAN/NNN/MX/GOG_sig + CTX_* enrichment** — `gog_engine.compute_gog_signals()` უნდა გადავიყვანოთ DB-instant-ში. ცოტა რთულია, 7 dependency DataFrame-ი სჭირდება — sig_df, wlnbb, f_sigs, vabs, ultra260, ultraV2, combo. შესაძლებელია მაგრამ ~3-4 საათის სამუშაო.

3. **RGTI/SMX** — Live-შიც disabled-ია (`turbo_engine.py` 1260). Pine script-ი არც კი წერს მათ. თუ ნამდვილად გვინდა — საჭიროა Pine-ის ცალკე ენდპოინტი (Massive-ი ცალკე API call-ი მოითხოვს RGTI-სთვის). ალბათ არ ღირს ძალისხმევა.

4. **APScheduler ავტომატური 17:00 ET refresh** — კოდი არსებობს main.py lifespan-ში, მაგრამ მუშაობს მხოლოდ თუ uvicorn server up-ია. ლოკალურ macOS-ზე ეს ნიშნავს რომ ლეპტოპი უნდა იყოს ჩართული. LaunchAgent macOS-ისთვის ვცადეთ, მაგრამ Desktop folder TCC protection-ის გამო ვერ მუშაობს (გადასატანი მთელი პროექტი /Users/sachoki/-ში პირდაპირ — დიდი ცვლილება).

5. **Wick X2G filter** — DB-ში `sig_x2g` სვეტი არ არსებობს (CSV-ში არც წერია). Live-შიც წინასწარ disabled-ი იყო. თუ მნიშვნელოვანია, საჭიროა Pine-ის გადახედვა.

---

## 8. გასაცნობი ფაილები (კოდის ჰიერარქია)

### Backend pipeline
- `backend/main.py` — FastAPI app + APScheduler + `api_bar_signals()` (Pine bridge)
- `backend/studio/incremental_delta.py` ← **ახალი**, daily delta
- `backend/studio/enricher.py` ← `_compute_pine_engines()` დაემატა
- `backend/studio/ultra_db_scan.py` ← UI key aliases + sig_ages SQL + max_age filter
- `backend/studio/importer.py` ← L88/260308 mapping
- `backend/turbo_engine.py` — LIVE scan column reference
- `backend/delta_engine.py`, `backend/ultra_engine.py`, `backend/para_engine.py`, `backend/fly_engine.py` — self-contained signal computations

### Frontend
- `frontend/src/components/UltraScanPanel.jsx` ← KEEP_ALWAYS + dbInfo + Update DB button + SIG_GROUPS filter defs
- `frontend/src/components/ScannerDataGrid.jsx` ← ULTRA + Beta render + PM% column + sortable
- `frontend/src/api.js` ← `studioIncremental`, `studioStats`, `premarket`, etc.

### Docs (ეს ფაილი + სხვები)
- `ARCHITECTURE.md` — ფართო Architecture & Signal Reference
- `ANALYTIC_STUDIO_ARCHITECTURE.md` — Studio-სპეციფიკური
- `SCORING_SYSTEMS.md` — 8 scoring engine
- `TURBO_SCORE_REFERENCE.md` — Turbo score breakdown
- **`SESSION_NOTES_260527_DB_MODE.md`** — ეს ფაილი

---

## 9. წინა სესიების ლინკები (კონტექსტი)

- `local_581ab95d-75e9-41de-b070-d08100b7ce64` — "Desktop website version" (2026-05-25): საწყისი DB incremental refresh, APScheduler setup
- `local_140035f0` — "NASDAQ scan complete" (2026-05-24)
- `local_a033d637` — "Fix duplicate signal aliases in pattern miner" (2026-05-24)

JSONL-ფაილები:
- `/Users/sachoki/.claude/projects/-Users-sachoki-Desktop-sachoki-desktop/52d95f78-3e46-4ae9-83eb-ad22ec0bb68c.jsonl` — Desktop website version რეფერენსი (~27 MB)
- `/Users/sachoki/.claude/projects/-Users-sachoki-Desktop-sachoki-desktop/75badf79-875a-49e8-a333-4c3ccfef2ab0.jsonl` — ეს მიმდინარე სესია

---

## 10. შემოწმების შემოკლებული checklist

ახალ ჩატში გადააფირე ეს ფაილი + დაბრუნდი ბრძანებებზე:

```bash
# 1. სერვერი ცოცხალია?
curl -s http://127.0.0.1:8080/api/health

# 2. DB-ში რა თარიღია?
curl -s http://127.0.0.1:8080/api/studio/stats | python3 -m json.tool

# 3. ULTRA scan სამუშაოა?
curl -s -X POST http://127.0.0.1:8080/api/studio/ultra-from-db \
  -H "Content-Type: application/json" \
  -d '{"universes":["sp500"]}' | python3 -c "import sys,json;d=json.load(sys.stdin);print(len(d.get('results',[])))"
# უნდა გადაიხედოს ~586

# 4. ფილტრები მუშაობდა?
# UI-ში Cmd+Shift+R, Ultra Scan-ში დაჭერი T2G, BEST★, BUY და სხვა
# ჩვენ უნდა გამოჩნდეს ticker-ები
```
