# Intraday writer — acceptance gate before any backfill · FROZEN 2026-09-13

**The last operational gate on the writer.** Restoring 147k lost ticker-days into a job that is
still destroying a session a night would only feed the damage back, so **one real scheduled run must
pass before any Massive backfill is authorised.**

Fix under test: `797faaf`. Root cause and history: `MTF_ZERO_RECONSTRUCTION.md` §8,
`backend/tests/test_intraday_update_overlap.py`.

## ✅ CYCLE 2 — PASS (2026-09-20)

**7/7, exit 0. The writer is safe to backfill into.**

The run that mattered was **Sat 2026-09-19**: `old_max 2026-09-17 (Thu) − OVERLAP 3d = **2026-09-14
(Mon)**`, a real trading day and exactly the case the old code destroyed. Monday **survived intact —
3,104 tickers in both 1H and 4H**, unchanged. Thu 09-17 and Fri 09-18 had weekend cutoffs and put
nothing at risk, as predicted.

| check | result |
|---|---|
| both timeframes exit 0 | ✅ |
| invariant printed each run | ✅ +6,206 · +21,697 · +6,208 · +21,686 · +6,207 · +4,984 |
| `key_difference` negative on neither | ✅ none negative |
| **per-ticker check** | ✅ **"no ticker lost rows" on every run** — the guard added after APGE/HLX |
| cutoff session survived | ✅ 2026-09-14 3,104 → 3,104 |
| completeness vs the reference calendar | ✅ 1H 1,286 → 1,289 · 4H 1,279 → 1,282 |
| zero new missing sessions | ✅ none |
| `(ticker, date)` key sets | ✅ 10/10 sessions, zero tickers lost |

Both fixes held in production: `797faaf` (the overlap off-by-one) and `f75aaee` (the clamp that
stopped stale tickers being eaten).

⚠️ **A defect in this gate was found and fixed while reading the result** (`bugfix in this commit`).
`--verify` derived ONE cutoff from the *baseline's* max_date — the cutoff of a run that may never
have happened — and so reported "2026-09-12 is not a trading day, nothing at risk" while the
genuinely at-risk session, Mon 09-14, was covered only incidentally by the key-set table. It now
walks **every** cutoff between the baseline and the current max. The PASS was correct; the reasoning
line was not, and it would have missed a destroyed session falling outside the last-10 key-set window.

**Next: the Massive backfill is unblocked and awaits its own authorization** (vendor calls).

---

## Cycle 2 — the run to watch (as registered)

**Cycle 1 FAILED on 2026-09-16 and was right to.** The `797faaf` overlap fix held — the cutoff
session 2026-09-08 survived 3,104 → 3,104, no session went newly missing, and complete sessions rose
— but the gate's own per-ticker instrumentation caught a second, different defect: the delete window
was derived from `old_max` while the restore window was bounded by `FETCH_DAYS`, so **stale tickers**
lost days nothing could give back (APGE and HLX lost 2026-08-31). Fixed by clamping the cutoff to
the fetched frame's minimum date (`f75aaee`). Two gate bugs were fixed alongside it: the invariant
was a net sum that hid per-ticker losses, and `capture()` slid its key-set window forward so intact
sessions read as total losses.

The clamp has **not yet been exercised by a live run**, so cycle 2 begins with a fresh baseline
(taken 2026-09-16 with `--force`): 1H max 2026-09-15, 1,297 sessions, 20 missing; 4H 1,295 / 27.

**Verify after Saturday 2026-09-19**, not before, because the two defects surface on different runs:

| run | `old_max` | cutoff | exercises |
|---|---|---|---|
| Thu 09-17 | 09-15 Tue | 09-12 **Sat** | the **clamp** only — APGE/HLX run every night |
| Fri 09-18 | 09-16 Wed | 09-13 **Sun** | the clamp only |
| **Sat 09-19** | 09-17 Thu | **09-14 Mon** | **both** — the first trading-day cutoff since the fix |

A cutoff that lands on a weekend puts nothing at risk, so only 09-19 re-tests the original
destruction case.

## Frozen acceptance — all seven, no partial credit

1. `update_intraday_db.py --tf 1h` and `--tf 4h` both finish **exit 0**.
2. Both print `overlap invariant: deleted N · re-inserted M · key_difference D`.
3. **`D < 0` on neither.**
4. The **cutoff-day session survives intact** in the store.
5. Session completeness against the 15m / 1D reference calendar **does not regress**.
6. **Zero new missing sessions** on 1H or 4H.
7. The **`(ticker, date)` key set** of the recent sessions is compared — not only aggregate counts.
   A count can match while membership silently rotates.

## How to run it

```bash
backend/.venv/bin/python backend/intraday_acceptance.py --verify
```

The **cycle-2 baseline was taken 2026-09-16** and is at `research_out/intraday_acceptance_snapshot.json`:

| | 1H | 4H |
|---|---|---|
| max date | 2026-09-15 | 2026-09-15 |
| sessions | 1,297 | 1,295 |
| missing sessions | 20 | 27 |
| key sets captured | 10 | 10 |

`--verify` compares against it and prints **PASS** or **FAIL** with the specific breach. Do not
re-snapshot before the run — the snapshot *is* the baseline.

⚠️ The baseline is kept **in the repo**, not under `data/`. `data/` is a symlink to the external
QUANT_RESEARCH SSD: nothing there is tracked, and nothing there is readable when the volume is
unmounted. The first version of this document claimed the snapshot had been committed under
`data/`; it had not, and could not be. Corrected rather than quietly moved.

**If it passes**, the writer is safe to backfill into. **If it fails**, the backfill does not start
and the writer goes back for another pass.

Three exit codes, because "the run never happened" is not a failure and is certainly not a pass:

| exit | meaning |
|---:|---|
| `0` | **PASS** — all seven hold; the writer is safe to backfill into |
| `1` | **FAIL** — a named criterion was breached; do not backfill |
| `2` | **NOT TESTED** — no timeframe advanced past the baseline, so the nightly run did not happen. A scheduler / runtime finding, not an acceptance result. Re-run after a real night. |

Both guards are in code rather than in this document, which is the point: `--snapshot` refuses to
overwrite an existing baseline without `--force` (overwriting it would make `--verify` compare the
post-run state against itself and report a meaningless PASS), and `--verify` refuses to call an
untested gate a pass. Pinned by `backend/tests/test_intraday_acceptance_guard.py`.

## Sequence after a PASS

one live-night acceptance → **Massive backfill authorization** → staging 1H/4H rebuild → completeness
against the 15m/1D calendar + parity on healthy overlap → canonical replacement → rebuild
`mtf_rev_signals.parquet` → re-run census and parity.

Pre-roll for the backfill: enough history before the earliest damage (2026-06-29) for RSI maturity —
~44 sessions on 4H, so a fetch starting mid-April.

## The two anomalies, audited 2026-09-16

### 1 · Saturday's 0.12 ratio — **RESOLVED, and it was my error**

There is no Saturday anomaly. `update_all.sh:59` adds a **third** block on Saturdays — `1w`, the
weekly store — and my log parser tracked only `(1h|4h)` without resetting its current-timeframe
variable, so the **weekly run's numbers overwrote the 4H ones**. What I reported as "4H writes half
what it should" was the 1w run all along; 5,190 tickers should have given it away immediately, since
the intraday universe is 3,203 while 5,190 is the weekly one.

The real Saturday 2026-09-12 run:

```
1h  +86,849 · 3,108 tickers
4h  +24,837 · 3,108 tickers   ← ratio 0.286, identical to every other night
1w  +10,166 · 5,190 tickers   ← what I was misreading as 4H
```

This matters for cycle 2: **the Sat 2026-09-19 run is entirely ordinary**, and nothing about it
should be treated as suspect.

### 2 · Why the damage starts 2026-06-29 — **NARROWED, not proven**

**Established.** Across the stores' full history — 1,306 sessions from 2021-07-02 — there are
**zero** damaged sessions before each store's onset. And the onsets **differ per store**: 4H
2026-06-29, 1H 2026-07-20 (both Mondays). A single global cause — a code change, a schedule change,
a vendor change — would have hit both stores on the same date. This is a **per-store** event.

**Strongly indicated, not proven.** Each onset is that store's last **full rebuild**
(`build_intraday_db --all`): the rebuild rewrote all prior history correctly, and from then on the
nightly incremental was the only writer, eating one cutoff day at a time. That is the only mechanism
consistent with both "no damage at all before date X" and "a different date X per store".

**Why it cannot be settled.** A full rebuild is run by hand, not from `update_all.sh`, so it never
reaches `~/Library/Logs/sachoki_update.log`; the shell history holds no `build_intraday_db` command.
Left as a narrowed finding rather than an explained one. It does not block the backfill — the writer
is fixed either way, and the backfill will itself rewrite the affected span.

