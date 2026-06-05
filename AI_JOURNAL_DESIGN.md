# AI Journal — дизайн (v1)

> Дисциплинированный LLM-агент бумажной торговли с **обучением, привязанным к статистике, а не к нарративу**.
> Каркас не зависит от движка сигналов: движок даёт сигналы → Journal превращает их в дисциплинированные сделки и накапливает *проверенные* уроки.

---

## 0. Решения (приняты, обоснованы)

| Параметр | Решение | Почему |
|---|---|---|
| Роль | **Автономный paper-агент** (реальные сделки — никогда автоматически) | Закрытый P&L — единственный честный сигнал для обучения. Financial-safety: деньги не двигаем. |
| Горизонт | **Multi-day swing (3–10 дней)**, ATR-стопы, грейдинг по fwd_5d + HH-breakout | Мы эмпирически выяснили: edge у наших сигналов — multi-day structural continuation. Премаркет/intraday — news-шум (corr≈0, кроме V×20). |
| Старт | **v0 = схема + bootstrap из истории + grading**, потом decision-loop | Сначала замыкаем петлю обратной связи на готовых данных, без риска. |
| Хранилище | **Отдельный `journal.duckdb`** | Single-writer лок основной `studio_analytics.duckdb` конфликтует с app/обновлениями (прочувствовано на практике). |
| Модели | Sonnet — решения; Haiku — черновики уроков | Дорогое суждение отделено от дешёвой рефлексии. |

---

## 1. Главный принцип: трёхуровневая память (firewall против суеверий)

Это центральное отличие от «наивного» журнала, где LLM учится из n=1 анекдотов.

```
Tier 1 — HARD STATS  (signal_outcomes)   ← ground truth, считает КОД, не LLM
         форвард 3/7/14d КАЖДОГО сигнала/комбо, as-of-date.
         Bootstrap из истории на day-1. Это наши проверенные приоры (V×20=12.8×, …).

Tier 2 — PATTERN MEMORY (own closed trades)
         агрегатный win-rate / avg-ret по fingerprint собственных закрытых сделок,
         as-of-date. Это "мой личный опыт".

Tier 3 — LLM LESSONS  (narrative)         ← LLM пишет, КОД валидирует
         текстовый урок (what_worked/what_failed). НЕ влияет на решения,
         пока Tier-1/Tier-2 не подтвердят его: n ≥ N_min И lift vs baseline.
         Иначе статус "provisional" + TTL-распад.
```

**Правило промоушена урока:** LLM-агент B предлагает урок → код ищет его `scope` (fingerprint) в Tier-1/Tier-2 → если `n ≥ N_min` (напр. 20) и `win_rate − base ≥ порог` (или `avg_ret` значимо > 0) → `status='active'` (попадает в промпт решений). Иначе `status='provisional'` (виден агенту как «гипотеза, мало данных», на сайзинг не влияет). Урок, который при ре-валидации больше не подтверждается → `status='retired'`.

> Так агент **не может выучить примету**: любое убеждение, влияющее на деньги, обязано иметь статистическую опору, посчитанную кодом из реальных исходов.

---

## 2. Инварианты данных (вшиты в код, не в промпт)

- **No look-ahead.** Решение на дату `D` видит только бары/исходы с датой `< D`. Tier-1/Tier-2 статистики строятся `as-of D`. Любая протечка будущего = фейковая метрика → запрещено.
- **Outcome ≠ feature.** fwd_3/7/14d сигнала считаются ПОСЛЕ факта и никогда не подаются в решение по текущему бару.
- **Честный baseline.** Каждая статистика сравнивается с base rate соответствующего среза (как мы делали для V3/V×20). «Хорошо» = значимо лучше базы, а не «>50%».
- **Детерминизм рельсов.** Сайзинг, стоп/таргет, блэклист, исполнение — чистый Python, воспроизводимо. LLM на них не влияет.

---

## 3. Детерминированные рельсы (код решает «можно ли» и «сколько»)

- **Entry-фильтр кандидатов:** по *валидированному* edge (V3 + V×20/V×10 + structural HH-контекст), а не по сырому ultra/turbo (они форвард не предсказывали). Топ-N (12) на сессию.
- **Сайзинг:** база × множитель от conviction × множитель от Tier-1 lift сетапа. Жёсткие кэпы: макс % капитала на позицию, макс число открытых, макс на сектор.
- **Выходы (multi-day):** ATR-стоп (напр. −1.5·ATR от входа) и таргет (+2.5·ATR) ИЛИ time-stop (N дней). Проверяются по **дневным** барам. Закрытие — в коде, без LLM.
- **Исполнение-реализм:** вход/выход по **open следующего дня** + слиппедж (bps от цены/ликвидности). Никаких фантомных филлов по close сигнального бара.
- **Refusal:** код может отклонить BUY (нет кэша, блэклист, дубликат тикера, превышен кэп) — намерение LLM ≠ сделка.
- **Правило времени входа (нельзя торговать при закрытой бирже):** журнал принимает решения **за ~30 мин до закрытия** (в сессии). Если сессия ОТКРЫТА → вход `AT_DECISION` по цене решения. Если ЗАКРЫТА (пре/афтермаркет, выходные) → позиция `PENDING_OPEN`, вход по **open следующей сессии** (`NEXT_OPEN`); стоп/таргет считаются от реализованного open + ATR-снимок с момента решения. Look-ahead-safe: open берётся из первого дневного бара строго ПОСЛЕ даты решения. Реализация: `decide.py` (ветвление по `premarket_cache._regular_session_open()`) + `fills.py` (доливка).

---

## 4. Два агента + промпт

**Agent A — Decision (Sonnet), после скана.**
Вход (один JSON, system-промпт закэширован `cache_control: ephemeral`):
`account · open_positions(+live P&L, дни) · auto_closed(сегодня) · recent_closed · market_regime · active_lessons · pattern_memory(as-of) · signal_outcomes(as-of) · blacklist · scan_candidates(top-12 с fingerprint)`.
Выход (tool use / structured output, валидируется): по каждому кандидату `action(BUY/WATCH/SKIP) · conviction(0-100) · thesis · expected_horizon · key_evidence_refs` + `pattern_insights[]` + `lesson_proposals[]` + `blacklist_proposals[]`.

**Agent B — Lesson (Haiku), после каждого закрытия.**
Вход: закрытая сделка + её fingerprint + Tier-1/2 статистика этого fingerprint.
Выход: `what_worked / what_failed / lesson / scope(fingerprint) / tags`. Это **черновик** → проходит промоушен-гейт §1.

> LLM возвращает только намерения и гипотезы. Деньги двигает код; убеждения активирует статистика.

---

## 5. Цикл сессии — `run_journal_session(scan_results)`

1. **Load state** — капитал, открытые/закрытые позиции, последние записи.
2. **Mechanical auto-close** — по дневным барам: стоп/таргет/time-stop → закрытие в коде → триггер Agent B (урок-черновик).
3. **Enrich open** — live-цена (Massive), текущий P&L, дней в позиции.
4. **Load memory (as-of сегодня)** — Tier-1 signal_outcomes, Tier-2 pattern_memory, active lessons, blacklist, market_regime.
5. **Candidates** — entry-фильтр движка → top-12 + fingerprint каждого.
6. **Build prompt** — единый JSON (см. §4).
7. **Agent A call** (стрим, кэш).
8. **Parse** — устойчивый extract JSON (markdown-fences/thinking-safe) + schema-валидация.
9. **Execute BUY/WATCH** детерминированно (рельсы §3, refusal).
10. **Persist** — позиции, journal_entry, lesson_proposals→гейт, blacklist_proposals, новый капитал.
11. **Shadow** — «что если бы» по исключённому тиру/сетапу (валидация: не слишком ли строги правила).

---

## 6. Циклы обучения

- **Grading (cron, БЕЗ LLM):** PENDING-сделки с прошедшим горизонтом → fwd/HH из баров → verdict (WIN/LOSS/FLAT). Дёшево, объективно.
- **Lesson (Agent B):** черновик при закрытии → §1 промоушен-гейт.
- **Validation/re-promote (cron):** периодически пересчитывает Tier-1/2 as-of, ре-валидирует active-уроки, ретайрит неподтверждённые. **Защита от нарративного дрейфа и self-reinforcing петель.**
- **Calibration (мета):** строит кривую conviction→реальный win-rate. Если 80 не выигрывает чаще 50 — conviction шум, сайзинг по нему урезается; сам факт становится уроком.

---

## 7. Bootstrap из истории (day-1 не вслепую)

При инициализации `signal_outcomes` **засевается из `studio_analytics.duckdb`**: для каждого сигнала/комбо считаем исторические fwd_3/7/14d + HH-rate + base rate (as-of-корректно). Так агент с первого дня знает реальные приоры (V×20≈12.8×, LVBO, structural HH≈+10pp), а не учит их 200 сделок с нуля. Наше ключевое преимущество — годы размеченных баров.

---

## 8. Схема (journal.duckdb)

```
journal_state(id, capital, start_capital, updated_at, config_json)
journal_position(id, ticker, opened_at, decision_date, action, conviction,
                 fingerprint, entry_px, size, stop_px, target_px, horizon_days,
                 status(OPEN/CLOSED), closed_at, exit_px, exit_reason,
                 pnl, pnl_pct, verdict, evidence_json, thesis)
signal_outcomes(fingerprint, as_of_date, n, fwd3_avg, fwd7_avg, fwd14_avg,
                hh_rate, win_rate, base_win_rate, lift, updated_at)      -- Tier 1
pattern_memory(fingerprint, as_of_date, n_trades, win_rate, avg_ret, last_seen) -- Tier 2
trade_lesson(id, created_at, scope_fingerprint, what_worked, what_failed,
             lesson, tags, confidence, status(provisional/active/retired),
             evidence_n, evidence_lift, source_position_ids)            -- Tier 3
signal_blacklist(pattern, reason, created_at, ttl_days, source)
calibration(bucket, conviction_lo, conviction_hi, n, realized_win_rate, updated_at)
shadow_position(... зеркало journal_position для исключённого среза ...)
journal_session_log(ts, candidates_n, decisions_json, capital_before/after, notes)
```

`fingerprint` = детерминированный ключ из сигналов на дату решения: `{V3-бакет, tz, rtb_phase, отсортированные reasons-теги, RSI-бакет, vol_bucket, price-бакет}`. Это и ключ retrieval, и `scope` уроков — общий словарь между всеми тремя ярусами памяти.

---

## 9. Интеграция

```
backend/ai_journal/
  db.py          # journal.duckdb, схема, as-of запросы
  bootstrap.py   # засев signal_outcomes из истории
  memory.py      # сборка трёх ярусов + retrieval по fingerprint
  rails.py       # entry-фильтр, сайзинг, стоп/таргет, исполнение-реализм, refusal
  grading.py     # авто-грейдинг закрытых (без LLM)
  lessons.py     # Agent B + промоушен-гейт + re-validation
  calibration.py # conviction→winrate
  decide.py      # Agent A: сборка промпта, вызов, парсинг, исполнение
  session.py     # run_journal_session() — оркестратор §5
  llm.py         # Anthropic клиент: tool use, prompt caching, стрим
FastAPI:  /api/journal/session  /grade  /lessons  /calibration  /positions  /state
React:    вкладка "AI Journal" — лента решений · открытые/закрытые · уроки(active/provisional) · кривая калибровки · shadow vs real
```

API-ключ — на бэке (`ANTHROPIC_API_KEY` в `.env`, как `MASSIVE_API_KEY`).

---

## 10. Фазы

- **v0** — схема + `bootstrap` из истории + `grading` + ручной прогон одной сессии на топ-10 (без авто-расписания). Цель: убедиться, что петля «решение→исход→оценка» физически замыкается и данные честные.
- **v1** — retrieval по fingerprint + Agent A/B + промоушен-гейт уроков.
- **v2** — calibration + shadow + расписание (cron) + paper-P&L дашборд.
- **v3** — A/B самих правил (разные пороги фильтра/сайзинга как «стратегии», сравнение по shadow).

---

## 10b. UI — вкладка "AI Journal"

Принцип: **прозрачность firewall'а**. На каждом решении и уроке видно, на какую статистику (Tier-1/2) он опёрся — пользователь должен видеть *почему*, а не получать чёрный ящик.

```
┌ KPI-полоса (sticky) ───────────────────────────────────────────────────────┐
│ Equity $10,840 (+8.4%)  ▁▂▃▅▇ │ Open 4 │ Today +1.2% │ WinRate 58% (base 48%)│
│ Calibrated ✓ │ [▶ Run session]  [Grade now]  Last: 2026-06-05 15:58          │
└──────────────────────────────────────────────────────────────────────────────┘
 [ Decisions ] [ Positions ] [ Lessons ] [ Calibration ] [ Knowledge ] [ Shadow ]
```

**1. Decisions** — лента последней сессии. Карточка на кандидата:
```
 NVDA   BUY ▮▮▮▮▯ conv 78   horizon 5d            entry≈ next-open
 thesis: "T2G+V×20 в Phase D, RSI 58 — accumulation breakout setup"
 evidence ▸  V×20 → P(big)=9.6% (12.8× base, n=412)   ← Tier-1, кликабельно
            pattern T2G|D|V×20 → own win 64% (n=22)    ← Tier-2
            lesson #14 (active): "не входить если RSI>72"
 [принято кодом ✓ size 4% ]   |   SKIP'ы и WATCH'и — свёрнутым списком ниже
```
Карточки, отклонённые рельсами (блэклист/кэп/нет кэша), помечены серым с причиной.

**2. Positions** — открытые сверху: ticker, entry/now, P&L%, дней, стоп/таргет (визуальные риски), мини-свечной (переиспользуем CodeCandleChart). Закрытые ниже: verdict-чип (WIN/LOSS/FLAT), realized P&L, exit_reason (stop/target/time), и привязанный урок.

**3. Lessons** — три секции с явным статусом:
   - **Active** (зелёные) — `lesson · scope · n=NN · winrate XX% vs base YY% · lift Z×`.
   - **Provisional** (серые, «гипотеза — мало данных n<N») — НЕ влияют на сайзинг, видно что ждут подтверждения.
   - **Retired** (зачёркнуты) — перестали подтверждаться.
   Это и есть визуализация firewall'а: видно, что в деньги идёт только статистически опёртое.

**4. Calibration** — бар-чарт conviction-бакетов → realized win-rate (с диагональю идеальной калибровки). Сразу видно, если conviction = шум.

**5. Knowledge (Tier-1)** — explorer засеянных из истории `signal_outcomes`: таблица/хитмап `fingerprint → fwd3/7/14, HH-rate, lift vs base, n`, сортировка по lift. Это «база знаний» агента (где живёт V×20=12.8× и т.п.) — её можно смотреть и без единого вызова API.

**6. Shadow** — две equity-кривые наложением (Real vs исключённый срез) + дельта. Ответ на вопрос «не слишком ли строги правила».

Источник данных — `/api/journal/*`. Live-цены открытых позиций — через premarket_cache/Massive (session-gated, как RT%/PM%).

---

## 11. Чек-лист «сделано правильно»
- [ ] Ни один урок не влияет на деньги без статистической опоры (Tier-1/2, n≥N, lift vs base).
- [ ] As-of-date везде; нет look-ahead.
- [ ] Исходы считает код, не LLM.
- [ ] Исполнение по next-open + слиппедж.
- [ ] Отдельный journal.duckdb (нет лок-конфликта).
- [ ] Bootstrap из истории на старте.
- [ ] Реальные сделки — никогда автоматически.
- [ ] Calibration и shadow включены до того, как доверять conviction/правилам.
