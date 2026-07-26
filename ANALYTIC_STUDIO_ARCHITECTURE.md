# Sachoki — Analytic Studio Architecture

> Version 1.0 · Designed: 2026-05-24  
> Goal: поймать логику рынка через сигналы, комбо, последовательности — и понять что мы упустили и почему.

---

## СОДЕРЖАНИЕ

1. [Концепция и цели](#1-концепция-и-цели)
2. [Общая схема системы](#2-общая-схема-системы)
3. [Data Foundation Layer](#3-data-foundation-layer)
4. [Event Detection Engine](#4-event-detection-engine)
5. [Pattern Miner Engine](#5-pattern-miner-engine)
6. [Miss & False-Positive Analyzer](#6-miss--false-positive-analyzer)
7. [Scoring Lab](#7-scoring-lab)
8. [Bar Description Generator](#8-bar-description-generator)
9. [API Layer](#9-api-layer)
10. [Frontend Studio UI](#10-frontend-studio-ui)
11. [Database Schema](#11-database-schema)
12. [Файловая структура](#12-файловая-структура)
13. [Этапы разработки](#13-этапы-разработки)

---

## 1. КОНЦЕПЦИЯ И ЦЕЛИ

### Что мы строим

**Analytic Studio** — внутренний аналитический инструмент для исследования сигналов.
Не сканер в реальном времени, а **ретроспективная лаборатория**: берём исторические данные
со всеми сигналами и ищем что работало, что не работало и почему.

### Ключевые вопросы на которые должна отвечать студия

| Вопрос | Тип анализа |
|--------|-------------|
| Какие комбо сигналов предшествовали росту x2 в 14-30 дней? | Pattern Mining |
| Что мы пропустили — тикеры которые выросли но сигнала не было? | Miss Analysis |
| Тикеры которые сигнал поймал но они упали — почему? | False Positive Analysis |
| Какие последовательности баров (T/Z + L + Volume) надёжны? | Sequence Analysis |
| Если изменить веса скоринга — стало бы лучше? | Scoring Lab |
| Опиши мне словами что происходило с акцией перед ростом | Bar Description |

---

## 2. ОБЩАЯ СХЕМА СИСТЕМЫ

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA FOUNDATION                                 │
│                                                                         │
│  bulk_export CSVs (SP500/NASDAQ/Russell2k)                             │
│  + Long forward returns (20d/30d/60d/90d computed)                     │
│  + Bar narrative text (generated once, stored)                         │
│  + Event labels (x2, +50%, -30%, etc.)                                 │
│                            │                                            │
│                     DuckDB (analytics.db)                              │
│                     ┌──────┴──────┐                                    │
│                     │ bars table  │  534K+ rows, 400+ cols             │
│                     │ events table│  tagged outcome events             │
│                     │ bar_desc    │  text descriptions per bar         │
│                     └─────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│  EVENT DETECTOR │ │  PATTERN MINER   │ │   SCORING LAB        │
│                 │ │                  │ │                      │
│ Find tickers    │ │ Before an event: │ │ Recompute scores     │
│ that moved x2   │ │ what signals     │ │ with custom weights  │
│ in N days       │ │ appeared in 14-  │ │ compare to actual    │
│                 │ │ 30 bar window?   │ │ turbo_score          │
└────────┬────────┘ └────────┬─────────┘ └──────────┬───────────┘
         │                   │                        │
         └──────────┬────────┘                        │
                    ▼                                 │
         ┌─────────────────────┐                     │
         │  MISS & FP ANALYZER │                     │
         │                     │                     │
         │ Miss: big move but  │                     │
         │ no signal fired     │                     │
         │                     │                     │
         │ FP: signal fired    │                     │
         │ but price dropped   │                     │
         └──────────┬──────────┘                     │
                    │                                 │
                    └──────────┬──────────────────────┘
                               ▼
               ┌────────────────────────────┐
               │      FastAPI Backend       │
               │   /api/studio/*            │
               └──────────────┬─────────────┘
                               │
               ┌────────────────────────────┐
               │   Analytic Studio UI       │
               │   React — новая вкладка    │
               └────────────────────────────┘
```

---

## 3. DATA FOUNDATION LAYER

### 3.1 Источник данных

Входные данные: **bulk_export CSVs** (уже есть):
- `sp500_full_signals_v3.csv` — 88K rows
- `nasdaq_full_signals_v3.csv` — 464K rows
- `russell2k_full_signals_v3.csv` — ~300K rows (в процессе)

### 3.2 Что добавляем к существующим данным

```python
# Дополнительные forward return горизонты (не было в bulk_export)
fwd_20d   # +20 days forward return %
fwd_30d   # +30 days
fwd_60d   # +60 days (для x2 за квартал)
fwd_90d   # +90 days

# Максимальный рост за окно (MFE — Maximum Favorable Excursion)
mfe_20d   # max(high[i:i+20]) / close[i] - 1  (лучший выход за 20 дней)
mfe_30d
mfe_60d

# Максимальная просадка за окно (MAE — Maximum Adverse Excursion)
mae_20d   # min(low[i:i+20]) / close[i] - 1
mae_30d

# Событие-флаги (bool)
hit_50pct_20d    # mfe_20d >= 50%
hit_2x_60d       # mfe_60d >= 100%
hit_3x_90d       # mfe_90d >= 200%
drop_20pct_10d   # mae_10d <= -20%
drop_30pct_20d   # mae_20d <= -30%

# Нарратив бара (text, генерируется один раз)
bar_description  # "T4 engulf on 3× volume, L34 coiling, RSI 38, below EMA200..."
```

### 3.3 DuckDB как аналитическое хранилище

**Почему DuckDB, а не SQLite:**
- Колоночное хранение → запросы по 400 колоннам без scan penalty
- In-process Python (нет отдельного сервера)
- SQL с оконными функциями, ASOF JOIN, UNNEST
- 534K строк × 400 колонн → запрос за < 200ms

```python
# backend/studio/db.py
import duckdb

STUDIO_DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"

def get_conn():
    return duckdb.connect(STUDIO_DB)
```

### 3.4 Загрузка данных (одноразовый импорт)

```
POST /api/studio/import
  body: { universes: ["sp500","nasdaq","russell2k"], recompute_fwd: true }

→ Reads bulk_export CSVs
→ Computes fwd_20d, fwd_30d, fwd_60d, mfe_*, mae_*, hit_*, drop_*
→ Generates bar_description text (batch)
→ Writes to DuckDB tables: bars, bar_desc
→ Returns: { rows_imported: 870000, duration_sec: 45 }
```

---

## 4. EVENT DETECTION ENGINE

### 4.1 Концепция "события"

Событие = конкретный бар конкретного тикера, после которого произошло
определённое ценовое движение (рост или падение).

```python
# Структура события
{
  "ticker": "NVDA",
  "date": "2025-11-14",
  "event_type": "BULL_2X",     # тип события
  "close_at_event": 142.50,
  "mfe_60d": 112.4,            # +112% за 60 дней
  "fwd_30d": 68.2,
  "universe": "SP500",
  "pre_window_start": "2025-10-01",  # начало окна до события
  "pre_window_bars": 20
}
```

### 4.2 Стандартные типы событий

| event_type | Условие | Описание |
|------------|---------|----------|
| `BULL_50PCT_20D` | mfe_20d >= 50% | Рост 50%+ за 20 торговых дней |
| `BULL_2X_60D` | mfe_60d >= 100% | Удвоение за 60 дней |
| `BULL_3X_90D` | mfe_90d >= 200% | Утроение за квартал |
| `BULL_30PCT_10D` | mfe_10d >= 30% | Быстрый рост 30% за 2 недели |
| `BULL_20PCT_5D` | mfe_5d >= 20% | Взрывной рост за неделю |
| `BEAR_DROP_20D` | mae_20d <= -20% | Падение 20%+ за месяц |
| `BEAR_DROP_30D` | mae_30d <= -30% | Серьёзное падение за 6 недель |
| `SIGNAL_CATCH` | turbo_score >= 60 AND fwd_5d > 10% | Поймали и выросло |
| `SIGNAL_MISS` | turbo_score == 0 AND mfe_20d >= 40% | Сигнала не было, но выросло |
| `FALSE_POS` | turbo_score >= 60 AND fwd_10d <= -10% | Сигнал был, упало |

### 4.3 Настраиваемые события (пользовательские)

```python
# Пример пользовательского события из UI
custom_event = {
  "name": "MY_2X_NASDAQ",
  "universe": ["nasdaq"],
  "condition": {
    "mfe_60d": {"gte": 100},      # выросло в 2 раза
    "close": {"gte": 5},           # цена > $5
    "volume": {"gte": 500000}      # объём > 500K
  },
  "date_range": ["2024-01-01", "2025-12-31"]
}
```

### 4.4 API

```
POST /api/studio/events/detect
  body: {
    event_type: "BULL_2X_60D" | "custom",
    universe: ["nasdaq"] | ["sp500"] | ["all"],
    date_range: ["2024-01-01", "2025-12-31"],
    custom_condition: {...}  # только если event_type == "custom"
  }
→ Returns: List[Event] + summary stats

GET /api/studio/events/summary
→ { total_events: 847, by_type: {...}, by_universe: {...} }
```

---

## 5. PATTERN MINER ENGINE

### 5.1 Задача

Для каждого события — найти **что происходило на барах ДО** (в окне 14–30 дней).
Не просто "какой сигнал был", а:
- Частота появления каждого сигнала в pre-window
- Комбинации сигналов (2, 3, N-way)
- Порядок сигналов (sequences)
- Сравнение с random baseline (те же тикеры без события)

### 5.2 Pre-window анализ

```python
def analyze_pre_window(
    events: List[Event],
    pre_window_bars: int = 20,   # смотрим 20 баров назад
    min_signal_freq: float = 0.3, # сигнал должен появляться в 30%+ событий
    min_n: int = 20,              # минимум событий для вывода
) -> PatternReport
```

**Алгоритм:**
1. Для каждого события берём `pre_window_bars` баров до него
2. Для каждого сигнала считаем: в скольких % событий он появлялся в окне
3. Сравниваем с базовой частотой этого сигнала по всему датасету
4. **Lift = event_freq / base_freq** — насколько сигнал "предвещает" событие

```
СИГНАЛ         ЧАСТОТА В СОБЫТИЯХ   БАЗОВАЯ ЧАСТОТА   LIFT
L34            72.3%                 3.9%              18.5×  ← очень предиктивен
ad_cluster     58.1%                 4.2%              13.8×
SIG_PARA_RETEST 45.6%               1.0%              45.6×  ← самый сильный лифт
prebreak_prime  38.2%               0.01%             3820×  ← редкий но важный
turbo≥40        61.4%              10.4%               5.9×
G1C             29.8%               0.97%             30.7×
```

### 5.3 Combo Mining

```python
# 2-way combo
combos_2way = [
    { "signals": ["L34", "ad_cluster"], "freq": 0.42, "lift": 8.3 },
    { "signals": ["SIG_PARA_RETEST", "G1C"], "freq": 0.31, "lift": 12.1 },
    ...
]

# 3-way combo
combos_3way = [
    { "signals": ["SIG_FRI34", "SIG_PARA_PREP", "turbo≥40"], "freq": 0.28, "lift": 15.2 },
    ...
]
```

### 5.4 Sequence Mining (порядок сигналов)

Не просто "все три сигнала были в окне", а **в каком порядке**:

```
Последовательность #1 (встречалась в 34% событий x2):
  Day -18: L34 + ad_fresh
  Day -12: SIG_PARA_PREP
  Day -7:  turbo_score 40-65
  Day -3:  G1C
  Day 0:   СОБЫТИЕ (начало роста)

Последовательность #2 (23% событий x2):
  Day -25: wyc_spring (!)   ← да, сейчас bearish, но ДО роста x2 он появляется!
  Day -15: ad_cluster
  Day -8:  SIG_FRI34
  Day 0:   СОБЫТИЕ
```

### 5.5 API

```
POST /api/studio/patterns/mine
  body: {
    event_ids: [1,2,3...] | "all_BULL_2X_60D",
    pre_window_bars: 20,
    min_lift: 3.0,
    combo_depth: 3,          # до 3-way комбо
    include_sequences: true,
    sequence_max_gap: 5      # макс 5 дней между сигналами в seq
  }
→ Returns: {
    single_signals: [...],   # сигналы с lift-ом
    combos_2way: [...],
    combos_3way: [...],
    sequences: [...],
    baseline_comparison: {...}
  }
```

---

## 6. MISS & FALSE-POSITIVE ANALYZER

### 6.1 Miss Analysis (что мы пропустили)

**Цель:** найти тикеры которые выросли x2 но сигнала НЕ БЫЛО.

```python
miss_analysis = {
  "total_missed_events": 234,
  "pct_of_all_2x_moves": 28.4,  # 28% роста x2 мы пропустили

  # Что было на барах этих тикеров КОГДА они росли:
  "pre_move_bar_patterns": [
    { "pattern": "Low turbo (1-20) + ad_cluster", "freq": 34% },
    { "pattern": "No signal at all + high volume",  "freq": 28% },
    { "pattern": "SIG_BIAS_DN active (was in bear trend)", "freq": 22% },
  ],

  # Bar descriptions для топ 10 пропущенных
  "example_misses": [
    {
      "ticker": "SOUN",
      "event_date": "2025-01-15",
      "pre_bars_summary": "20 баров: низкий турбо (max 8), L34 раз появлялся...",
      "why_missed": "turbo_score был 0-8, no major signal. Volume был 1.2x normal."
    }
  ]
}
```

### 6.2 False Positive Analysis (поймали — упало)

**Цель:** понять почему сигнал был, но цена пошла вниз.

```python
fp_analysis = {
  "total_fp_events": 156,   # turbo >= 60, fwd_10d <= -10%

  # Чем FP отличаются от успешных?
  "discriminators": [
    { "feature": "ALREADY_EXTENDED_FLAG = 1", "in_FP": 67%, "in_winners": 12%, "power": "HIGH" },
    { "feature": "SIG_BC = 1 (buying climax)",  "in_FP": 45%, "in_winners": 8%,  "power": "HIGH" },
    { "feature": "turbo_score >= 80",           "in_FP": 38%, "in_winners": 3%,  "power": "HIGH" },
    { "feature": "RSI_GE_70 = 1",              "in_FP": 52%, "in_winners": 18%, "power": "MEDIUM" },
    { "feature": "PRICE_GT_200 = 0 (below EMA200)", "in_FP": 71%, "in_winners": 39%, "power": "MEDIUM" },
    { "feature": "ad_cluster = 0",             "in_FP": 78%, "in_winners": 42%, "power": "MEDIUM" },
  ],

  # Лучший FP-killer (combo признаков которые предсказывают провал)
  "fp_killer_combos": [
    { "combo": "SIG_BC + ALREADY_EXTENDED_FLAG", "precision": 0.82, "n": 45 },
    { "combo": "turbo>=80 + RSI_GE_70",          "precision": 0.79, "n": 31 },
  ]
}
```

### 6.3 API

```
POST /api/studio/analysis/miss
  body: { event_type: "BULL_2X_60D", date_range: [...], universe: [...] }
→ MissReport

POST /api/studio/analysis/false-positives
  body: { min_turbo: 60, max_fwd_10d: -10, date_range: [...] }
→ FPReport

GET /api/studio/analysis/compare
  ?event_type=BULL_2X_60D&vs=false_positive
→ SideBySideComparison
```

---

## 7. SCORING LAB

### 7.1 Задача

Протестировать: если бы мы изменили веса в TURBO Score — поймали бы мы больше
событий BULL_2X_60D и меньше FP?

### 7.2 Custom Score Definition

```python
# Пользователь задаёт в UI:
custom_score_def = {
  "name": "MyScore_v1",
  "weights": {
    # Можно переопределить любой вес
    "ad_cluster":    +15,   # было +3 в turbo
    "L34":           +8,    # было +5
    "SIG_BC":        -10,   # было -3 (penalty)
    "ALREADY_EXTENDED_FLAG": -20,  # было -6
    "SIG_PARA_RETEST": +6,  # не было вообще!
    "prebreak_prime": +12,  # не было вообще!
    "wyc_spring":    -8,    # было 0 (нейтральный)
  },
  "hard_filters": [
    # Обнулить score если выполнено условие
    { "if": "ALREADY_EXTENDED_FLAG == 1", "then": "score = 0" },
    { "if": "SIG_BC == 1 AND ad_cluster == 0", "then": "score *= 0.3" },
  ],
  "threshold": 45   # считать сигналом если score >= 45
}
```

### 7.3 Backtesting кастомного скора

```python
# Применяем к историческим данным и сравниваем:
backtest_result = {
  "score_name": "MyScore_v1",
  "vs": "turbo_score",
  "date_range": "2024-01-01 — 2025-12-31",

  # Precision/Recall против события BULL_2X_60D
  "precision":  { "turbo": 0.234,  "my_score": 0.318 },  # % поднятых тикеров что реально выросли
  "recall":     { "turbo": 0.412,  "my_score": 0.589 },  # % x2 тикеров что мы поймали
  "f1":         { "turbo": 0.298,  "my_score": 0.407 },

  # Forward returns для тикеров с score >= threshold
  "avg_fwd_20d":  { "turbo": 3.2,   "my_score": 6.8  },
  "avg_fwd_60d":  { "turbo": 8.1,   "my_score": 16.4 },
  "win_rate_20d": { "turbo": 48.3,  "my_score": 57.1 },

  # FP rate
  "fp_rate":  { "turbo": 0.31,  "my_score": 0.19 },

  # Missed events
  "missed_2x": { "turbo": 234, "my_score": 156 },
}
```

### 7.4 API

```
POST /api/studio/scoring-lab/define
  body: CustomScoreDef
→ { score_id: "my_score_v1_abc123" }

POST /api/studio/scoring-lab/backtest
  body: { score_id: ..., event_type: ..., date_range: ..., universe: ... }
→ BacktestResult

GET /api/studio/scoring-lab/compare
  ?score_a=turbo_score&score_b=my_score_v1_abc123&event=BULL_2X_60D
→ ComparisonReport
```

---

## 8. BAR DESCRIPTION GENERATOR

### 8.1 Задача

Для каждого бара — текстовое описание на человеческом языке. Это нужно чтобы:
- Понять "пропущенные" тикеры
- Изучить последовательность событий перед ростом
- Дать контекст при анализе FP

### 8.2 Структура описания

```python
def generate_bar_description(bar: dict) -> str:
    """Генерирует ~3-5 предложений описания бара."""

    # Пример выхода:
    # "T4 engulf на 3.2× среднем объёме. Закрытие в верхних 85% диапазона.
    #  L34 (coiling): цена выше EMA50, RSI=38 (перепродан). PARA retest активен.
    #  Turbo score=47, G1C tier. 20 дней назад был wyc_spring."
    pass
```

**Компоненты описания:**

```
1. T/Z сигнал: "T4 full engulf" / "Z6 engulf bear" / "-"
2. Объём: "3.2× avg (high absorption)" / "0.4× (drying up)"
3. L сигнал: "L34 coiling (close in top 60%)" / "L22 double supply"
4. EMA позиция: "above all EMAs" / "below EMA200" / "reclaiming EMA89"
5. RSI: "oversold 32" / "neutral 52" / "overbought 74"
6. VABS: "ABS absorption spike" / "NS narrow space"
7. PARA/CISD: "PARA retest" / "CISD CPLUS"
8. Delta: "d_spring (delta spring)" / "d_blast_bull"
9. GOG tier: "G1C" / "G1P (top tier)"
10. Turbo score: "turbo=52"
11. Контекст: "3 дня назад L34, 7 дней назад ad_cluster"
```

### 8.3 Batch генерация (один раз при импорте)

```python
# Без LLM — чистая rule-based генерация на Python
# Быстро, детерминированно, воспроизводимо

def batch_generate_descriptions(df: pd.DataFrame) -> pd.Series:
    return df.apply(generate_bar_description, axis=1)

# ~534K баров × 5ms = ~45 минут (один раз)
# Результат сохраняется в DuckDB: bar_desc table
```

### 8.4 Pre-move narrative (20 баров до события)

```python
def generate_pre_move_narrative(ticker: str, event_date: str) -> str:
    """
    Генерирует связный рассказ о 20 барах до события.

    Выход:
    "За 20 дней до взрыва:
     День -20: neutral bar, объём 0.8×, turbo=4
     День -17: L34 появился впервые. Объём 2.1×, закрытие на хаях
     День -14: ad_fresh + T4. Turbo вырос до 38. RSI пробивает 50
     День -10: SIG_PARA_PREP активен. Цена выше EMA89 впервые за 6 нед
     День -7:  G1C tier! turbo=51. Объём 3.8× — накопление
     День -3:  SIG_FRI34 + PARA_RETEST. turbo=58
     День 0:   СОБЫТИЕ — открытие на +8%, gap up на объёме 7×"
    """
    pass
```

---

## 9. API LAYER

### 9.1 Роутер: `backend/studio_api.py`

```
Префикс: /api/studio/

--- Data Management ---
POST   /import              Импортировать CSVs в DuckDB
GET    /import/status       Прогресс импорта
GET    /stats               Общая статистика (тикеров, баров, дат)

--- Events ---
POST   /events/detect       Найти события по критерию
GET    /events/list         Список всех событий (пагинация)
GET    /events/{id}         Один event + контекст
POST   /events/custom       Создать кастомный event type
GET    /events/summary      Агрегированная статистика событий

--- Pattern Mining ---
POST   /patterns/mine       Майнить паттерны для events
GET    /patterns/result/{id} Получить результат майнинга
POST   /patterns/compare    Сравнить два набора событий (winners vs FP)

--- Miss & FP Analysis ---
POST   /analysis/miss       Анализ пропущенных
POST   /analysis/false-pos  Анализ ложных срабатываний
GET    /analysis/compare    Сравнение: winner vs FP vs miss

--- Scoring Lab ---
POST   /scoring-lab/define  Создать кастомный скор
GET    /scoring-lab/list    Все кастомные скоры
POST   /scoring-lab/backtest Бектест скора
GET    /scoring-lab/compare Сравнение скоров

--- Bar Data ---
GET    /bars/{ticker}       Все бары тикера + forward returns
GET    /bars/{ticker}/{date} Один бар с описанием
POST   /bars/search         Найти бары по условию

--- Narratives ---
GET    /narrative/{ticker}/{date}/pre   Pre-move narrative (20 баров до)
GET    /narrative/{ticker}/{date}/bar   Описание одного бара
POST   /narrative/batch     Batch описание для списка тикеров

--- Export ---
POST   /export/csv          Экспорт результатов анализа в CSV
POST   /export/report       Генерировать PDF/MD отчёт
```

---

## 10. FRONTEND STUDIO UI

### 10.1 Новая вкладка в App.jsx

```jsx
// Новая вкладка: 🔬 Studio
<StudioPanel />
```

### 10.2 Структура UI (6 экранов)

```
┌─────────────────────────────────────────────────────────┐
│  🔬 ANALYTIC STUDIO                              [?] [⚙]│
├─────────────────────────────────────────────────────────┤
│  [📊 Events] [🔍 Patterns] [👁 Miss] [⚠ FP] [🧪 Lab] [📋 Report] │
└─────────────────────────────────────────────────────────┘
```

---

#### Экран 1: 📊 EVENTS (Setup)

```
┌────────────────────────────────────────────────────────────┐
│  DEFINE WHAT YOU'RE LOOKING FOR                            │
│                                                            │
│  Universe:    [x] SP500  [x] NASDAQ  [ ] Russell2k        │
│  Date range:  [2024-01-01] → [2025-12-31]                 │
│                                                            │
│  Event type:  ● Preset    ○ Custom                        │
│  ┌──────────────────────────────────────────────────┐     │
│  │ [x2 in 60d] [+50% in 20d] [+30% in 10d]         │     │
│  │ [Drop -20%] [Signal→Up] [Signal→Down]             │     │
│  └──────────────────────────────────────────────────┘     │
│                                                            │
│  Custom filter:                                            │
│  Price range: [$5] to [$500]   Volume: [>500K]            │
│  Min turbo at event: [0]   Max turbo: [100]               │
│                                                            │
│         [🔍 DETECT EVENTS]                                │
│                                                            │
│  Results: 847 events found                                 │
│  SP500: 312  NASDAQ: 535                                   │
│  Top movers: NVDA +218%, SMCI +189%, SOUN +156%           │
└────────────────────────────────────────────────────────────┘
```

---

#### Экран 2: 🔍 PATTERNS (Pre-move analysis)

```
┌────────────────────────────────────────────────────────────┐
│  PRE-MOVE PATTERN ANALYSIS                                 │
│                                                            │
│  Analyzing: 847 BULL_2X_60D events                        │
│  Pre-window: [20] bars  Combo depth: [3]  Min lift: [3×]  │
│                                                            │
│  [⚡ MINE PATTERNS]                                        │
│                                                            │
│  SINGLE SIGNALS (sorted by lift)                          │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Signal           │ In events │ Baseline │ Lift     │   │
│  │ SIG_PARA_RETEST  │  45.6%    │  1.0%    │ 45.6×🔥  │   │
│  │ ad_cluster       │  58.1%    │  4.2%    │ 13.8×    │   │
│  │ L34              │  72.3%    │  3.9%    │ 18.5×    │   │
│  │ G1C              │  29.8%    │  0.97%   │ 30.7×    │   │
│  └────────────────────────────────────────────────────┘   │
│                                                            │
│  TOP COMBOS                                                │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Combo                     │ Freq  │ Lift  │ FWD30d │   │
│  │ SIG_PARA_RETEST + L34     │ 38.2% │ 22.1× │ +18.4% │   │
│  │ ad_cluster + G1C          │ 24.7% │ 15.8× │ +14.2% │   │
│  └────────────────────────────────────────────────────┘   │
│                                                            │
│  SEQUENCES                  [▶ Show timeline view]        │
│  Seq #1 (34% of events): L34 → ad_cluster → G1C → EVENT  │
│  Seq #2 (23% of events): wyc_spring → FRI34 → PARA → EVENT│
└────────────────────────────────────────────────────────────┘
```

---

#### Экран 3: 👁 MISS ANALYSIS

```
┌────────────────────────────────────────────────────────────┐
│  WHAT DID WE MISS?                                         │
│                                                            │
│  234 tickers grew x2 — we had NO signal on them           │
│  That's 28.4% of all x2 moves missed                      │
│                                                            │
│  WHY WE MISSED THEM:                                       │
│  ████████████████████  34%  Low turbo (0-15) + ad_cluster  │
│  ████████████████      28%  Zero signal + vol spike        │
│  ████████              22%  SIG_BIAS_DN active (bear trend)│
│  ██████                16%  prebreak_watch only (too weak) │
│                                                            │
│  MISSED TICKERS LIST                    [📥 Export CSV]   │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ Ticker │ Date      │ Move   │ Pre-window summary     │ │
│  │ SOUN   │ 2025-01-15│ +224%  │ Low turbo, L34 once..  │ │
│  │ CLSK   │ 2024-11-03│ +189%  │ BIAS_DN, then flip...  │ │
│  │ [click for full bar narrative]                        │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  BAR NARRATIVE for SOUN pre-move:                         │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ Day -20: Neutral, vol 0.8×, turbo=4                  │ │
│  │ Day -17: L34 appeared. 2.1× vol. RSI 38              │ │
│  │ Day -10: ad_fresh + T4. turbo=38. Above EMA89        │ │
│  │ Day -3:  SIG_FRI34. turbo=51 (just below threshold)  │ │
│  │ Day 0:   Gap up +8%. MISSED — turbo was 51, not 60!  │ │
│  └──────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────┘
```

---

#### Экран 4: ⚠ FALSE POSITIVE

```
┌────────────────────────────────────────────────────────────┐
│  WHY DID WE GET BURNED?                                    │
│                                                            │
│  156 events: turbo≥60 but price dropped -10%+ in 10 days  │
│                                                            │
│  FP vs WINNERS — KEY DIFFERENCES:                         │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Feature              │ In FP │ In Winners │ Power  │   │
│  │ ALREADY_EXTENDED=1   │  67%  │   12%      │ 🔥HIGH │   │
│  │ SIG_BC (buy climax)  │  45%  │    8%      │ 🔥HIGH │   │
│  │ turbo_score >= 80    │  38%  │    3%      │ 🔥HIGH │   │
│  │ RSI_GE_70            │  52%  │   18%      │ ⚡MED  │   │
│  │ ad_cluster = 0       │  78%  │   42%      │ ⚡MED  │   │
│  └────────────────────────────────────────────────────┘   │
│                                                            │
│  FP KILLER COMBOS (avoid when all present):               │
│  • SIG_BC + ALREADY_EXTENDED  → 82% chance of drop       │
│  • turbo≥80 + RSI_GE_70       → 79% chance of drop       │
│                                                            │
│  RECOMMENDATION:                                           │
│  Add penalty: if ALREADY_EXTENDED + SIG_BC → score = 0    │
│  Estimated impact: -156 FP, -12 true positives (net +)   │
└────────────────────────────────────────────────────────────┘
```

---

#### Экран 5: 🧪 SCORING LAB

```
┌────────────────────────────────────────────────────────────┐
│  SCORING LAB — Test Custom Signal Weights                  │
│                                                            │
│  Base: turbo_score    New: [MyScore_v1 ▼]  [+ New Score]  │
│                                                            │
│  WEIGHT EDITOR                                             │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Signal            │ Current │ New  │ Change        │   │
│  │ ad_cluster        │    +3   │ [15] │ ▲ +12         │   │
│  │ L34               │    +5   │ [8]  │ ▲ +3          │   │
│  │ SIG_BC (penalty)  │    -3   │[-10] │ ▼ -7          │   │
│  │ ALREADY_EXTENDED  │    -6   │[-20] │ ▼ -14         │   │
│  │ SIG_PARA_RETEST   │    +3   │ [8]  │ ▲ +5 (new)    │   │
│  └────────────────────────────────────────────────────┘   │
│                                                            │
│  HARD FILTERS  [+ Add rule]                               │
│  [x] if ALREADY_EXTENDED + SIG_BC → score = 0            │
│  [x] if turbo >= 80 + RSI_GE_70 → score *= 0.3           │
│                                                            │
│  THRESHOLD: [45] points to fire signal                    │
│                                                            │
│  [▶ RUN BACKTEST against BULL_2X_60D]                     │
│                                                            │
│  RESULTS:                                                  │
│  ┌────────────────────────────────────────────────┐       │
│  │ Metric        │ turbo_score │ MyScore_v1 │ Δ   │       │
│  │ Precision     │   23.4%     │   31.8%    │ ▲   │       │
│  │ Recall        │   41.2%     │   58.9%    │ ▲   │       │
│  │ Avg FWD 60d   │   +8.1%     │  +16.4%    │ ▲▲  │       │
│  │ FP rate       │   31%       │   19%      │ ▼   │       │
│  │ Missed x2     │   234       │   156      │ ▼   │       │
│  └────────────────────────────────────────────────┘       │
└────────────────────────────────────────────────────────────┘
```

---

#### Экран 6: 📋 REPORT

Автогенерация Markdown/PDF отчёта:
```
# Analysis Report: NASDAQ x2 Movers 2024-2025

## Summary
847 events found. 312 caught (36.8%), 234 missed (27.6%), 156 FP (18.4%)

## Top Pre-Move Patterns
...

## Miss Analysis
...

## Scoring Lab Results
...
```

---

## 11. DATABASE SCHEMA

### DuckDB Tables

```sql
-- Главная таблица: все бары со всеми сигналами
CREATE TABLE bars (
  id          INTEGER PRIMARY KEY,
  ticker      VARCHAR NOT NULL,
  date        DATE NOT NULL,
  universe    VARCHAR,       -- 'sp500' | 'nasdaq' | 'russell2k'

  -- OHLCV
  open        DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT,

  -- Вычисленные forward returns (computed from close)
  fwd_1d      DOUBLE, fwd_3d   DOUBLE, fwd_5d   DOUBLE, fwd_10d  DOUBLE,
  fwd_20d     DOUBLE, fwd_30d  DOUBLE, fwd_60d  DOUBLE, fwd_90d  DOUBLE,

  -- MFE / MAE
  mfe_5d      DOUBLE, mfe_10d  DOUBLE, mfe_20d  DOUBLE, mfe_60d  DOUBLE,
  mae_5d      DOUBLE, mae_10d  DOUBLE, mae_20d  DOUBLE,

  -- Event flags (bool)
  hit_20pct_5d   BOOLEAN, hit_50pct_20d  BOOLEAN,
  hit_2x_60d     BOOLEAN, hit_3x_90d     BOOLEAN,
  drop_20pct_10d BOOLEAN, drop_30pct_20d BOOLEAN,

  -- Все сигналы из bulk_export (344 колонны)
  turbo_score    DOUBLE, turbo_score_n3 DOUBLE,
  -- ... все SIG_* колонны
  -- ... все остальные сигналы

  UNIQUE(ticker, date)
);

-- События
CREATE TABLE events (
  id           INTEGER PRIMARY KEY,
  ticker       VARCHAR,
  event_date   DATE,
  event_type   VARCHAR,     -- 'BULL_2X_60D', 'FALSE_POS', 'MISS', etc.
  close_price  DOUBLE,
  mfe_60d      DOUBLE,
  fwd_30d      DOUBLE,
  universe     VARCHAR,
  tags         VARCHAR[],   -- ['caught', 'missed', 'fp', 'custom']
  notes        VARCHAR,
  created_at   TIMESTAMP DEFAULT now()
);

-- Текстовые описания баров
CREATE TABLE bar_descriptions (
  ticker        VARCHAR,
  date          DATE,
  bar_desc      TEXT,        -- короткое описание (~200 chars)
  pre_narrative TEXT,        -- нарратив 20 баров до события
  PRIMARY KEY (ticker, date)
);

-- Кастомные скоры (определения)
CREATE TABLE custom_scores (
  score_id     VARCHAR PRIMARY KEY,
  name         VARCHAR,
  weights      JSON,         -- { "L34": 8, "ad_cluster": 15, ... }
  hard_filters JSON,
  threshold    INTEGER,
  created_at   TIMESTAMP DEFAULT now()
);

-- Результаты бектестов
CREATE TABLE backtest_results (
  id           INTEGER PRIMARY KEY,
  score_id     VARCHAR,
  event_type   VARCHAR,
  date_range   VARCHAR,
  universe     VARCHAR,
  precision_   DOUBLE,
  recall       DOUBLE,
  f1           DOUBLE,
  avg_fwd_20d  DOUBLE,
  avg_fwd_60d  DOUBLE,
  fp_rate      DOUBLE,
  missed_count INTEGER,
  result_json  JSON,         -- полные данные
  created_at   TIMESTAMP DEFAULT now()
);

-- Паттерны (результаты майнинга)
CREATE TABLE mined_patterns (
  id           INTEGER PRIMARY KEY,
  event_type   VARCHAR,
  pre_window   INTEGER,
  pattern_type VARCHAR,      -- 'single' | 'combo_2' | 'combo_3' | 'sequence'
  signals      JSON,         -- ["L34", "ad_cluster"]
  freq_in_events DOUBLE,
  base_freq    DOUBLE,
  lift         DOUBLE,
  avg_fwd_30d  DOUBLE,
  n            INTEGER,
  created_at   TIMESTAMP DEFAULT now()
);
```

---

## 12. ФАЙЛОВАЯ СТРУКТУРА

```
backend/
├── studio/
│   ├── __init__.py
│   ├── db.py               DuckDB connection + schema creation
│   ├── importer.py         CSV → DuckDB import + fwd return computation
│   ├── event_detector.py   Find events by criteria
│   ├── pattern_miner.py    Mine pre-window patterns + lift calculation
│   ├── miss_analyzer.py    Miss analysis + FP analysis
│   ├── scoring_lab.py      Custom score definition + backtest
│   ├── bar_describer.py    Rule-based bar description generator
│   └── narrative.py        Pre-move narrative generator
├── studio_api.py           FastAPI router (/api/studio/*)

frontend/src/components/
└── StudioPanel/
    ├── StudioPanel.jsx      Main shell + tab routing
    ├── EventsScreen.jsx     Screen 1: event setup
    ├── PatternsScreen.jsx   Screen 2: pattern mining results
    ├── MissScreen.jsx       Screen 3: miss analysis
    ├── FPScreen.jsx         Screen 4: false positive analysis
    ├── ScoringLabScreen.jsx Screen 5: scoring lab
    ├── ReportScreen.jsx     Screen 6: report generator
    ├── BarNarrative.jsx     Reusable bar description component
    ├── PatternTable.jsx     Signal lift table
    ├── LiftBadge.jsx        Visual lift indicator (3×, 18×, 45×)
    └── ScoreCompare.jsx     Side-by-side score comparison

tests/
└── test_studio/
    ├── test_importer.py
    ├── test_event_detector.py
    ├── test_pattern_miner.py
    ├── test_miss_analyzer.py
    ├── test_scoring_lab.py
    └── test_bar_describer.py
```

---

## 13. ЭТАПЫ РАЗРАБОТКИ

### Фаза 1 — Data Foundation (1-2 дня)
- [ ] `studio/db.py` — DuckDB schema
- [ ] `studio/importer.py` — CSV import + fwd_20/30/60/90d + mfe + event flags
- [ ] `POST /api/studio/import` + status endpoint
- [ ] Verify: 870K rows imported, all forward returns correct

### Фаза 2 — Event Detection (1 день)
- [ ] `studio/event_detector.py` — все preset types + custom
- [ ] `POST /api/studio/events/detect`
- [ ] UI: Экран 1 (EventsScreen)

### Фаза 3 — Pattern Mining (2 дня)
- [ ] `studio/pattern_miner.py` — lift calc + combo mining + sequences
- [ ] `POST /api/studio/patterns/mine`
- [ ] UI: Экран 2 (PatternsScreen) — таблица с lift + timeline

### Фаза 4 — Miss & FP Analysis (1 день)
- [ ] `studio/miss_analyzer.py`
- [ ] `POST /api/studio/analysis/miss` + `/false-pos`
- [ ] UI: Экраны 3 и 4

### Фаза 5 — Scoring Lab (2 дня)
- [ ] `studio/scoring_lab.py` — custom score + backtest
- [ ] `POST /api/studio/scoring-lab/define` + `/backtest`
- [ ] UI: Экран 5 (ScoringLabScreen) — weight editor + compare table

### Фаза 6 — Bar Descriptions (1 день)
- [ ] `studio/bar_describer.py` — rule-based, детерминированный
- [ ] `studio/narrative.py` — pre-move narrative
- [ ] Batch generation при импорте
- [ ] UI: BarNarrative компонент, интеграция в Screens 3 + 4

### Фаза 7 — Reports + Polish (1 день)
- [ ] `POST /api/studio/export/report`
- [ ] UI: Экран 6 + финальный polish
- [ ] Tests: минимум 50 тестов

---

## ТЕХНИЧЕСКИЕ ЗАВИСИМОСТИ

```python
# Новые пакеты (добавить в requirements.txt)
duckdb>=0.10.0          # аналитическая БД
scipy>=1.12.0           # stats для lift calculation
mlxtend>=0.23.0         # association rule mining (опционально для apriori)
reportlab>=4.1.0        # PDF генерация (для Report screen)
```

---

## КЛЮЧЕВЫЕ АРХИТЕКТУРНЫЕ РЕШЕНИЯ

| Решение | Почему |
|---------|--------|
| **DuckDB не SQLite** | Колоночный движок. 400 колонн × 870K строк — DuckDB в 50× быстрее SQLite для аналитических запросов |
| **Rule-based описания, не LLM** | Детерминированно, мгновенно, воспроизводимо, работает offline |
| **Lift как основная метрика паттернов** | Не просто "частота сигнала в событиях" а насколько он специфичен для события vs random |
| **Кастомный скор в JSON** | Не меняем turbo_engine.py. Lab хранит веса отдельно, применяет поверх bulk_export данных |
| **Одноразовый импорт** | Bulk_export CSVs → DuckDB один раз. Обновление по кнопке |
| **Event-driven дизайн** | Всё строится вокруг "события" (движение цены). Паттерны, миссы, FP — всё привязано к event |

---

*Следующий шаг: `Фаза 1 — studio/db.py + importer.py + POST /api/studio/import`*

