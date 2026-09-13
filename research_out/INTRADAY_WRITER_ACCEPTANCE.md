# Intraday writer — acceptance gate before any backfill · FROZEN 2026-09-13

**The last operational gate on the writer.** Restoring 147k lost ticker-days into a job that is
still destroying a session a night would only feed the damage back, so **one real scheduled run must
pass before any Massive backfill is authorised.**

Fix under test: `797faaf`. Root cause and history: `MTF_ZERO_RECONSTRUCTION.md` §8,
`backend/tests/test_intraday_update_overlap.py`.

## The run to watch

The next scheduled `com.sachoki.dbupdate` is **Tuesday 2026-09-15, 03:00** (launchd runs Tue–Sat),
covering the Monday 2026-09-14 session.

Its cutoff is `old_max 2026-09-11 (Fri) − OVERLAP 3d = **2026-09-08 (Tue)**` — **a real trading
day**, so this is precisely the case the old code destroyed. A natural test, with nothing to stage.

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

The **pre-run snapshot was taken 2026-09-13** and is at `research_out/intraday_acceptance_snapshot.json`:

| | 1H | 4H |
|---|---|---|
| max date | 2026-09-11 | 2026-09-11 |
| sessions | 1,295 | 1,293 |
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

## ⚠️ Two open anomalies — findings, NOT closed

Neither blocks the backfill if the corrected writer's live invariant comes back clean, and neither
may be written off as explained.

1. **Why does the historical damage only start 2026-06-29?** The overlap bug is structural and would
   have been destroying the cutoff day for as long as this code path has run. Something used to
   repair those sessions, or the schedule changed. The plist was last modified 2026-08-01, which
   does not explain the date. Unresolved.
2. **Why is the Saturday 4H/1H row ratio 0.12?** Every other night it is 0.29, which is exactly the
   2-bars-to-7-bars grid. Saturday runs write less than half the 4H rows they should:
   2026-08-15, 08-22, 08-29, 09-05 and 09-12 all read 0.12. Unresolved.
