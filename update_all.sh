#!/bin/bash
# update_all.sh — hands-off FULL daily refresh (no manual intervention):
#     1D bars + enrichment  +  ULTRA screener rescan      [via update_db.sh]
#     1h + 4h + 1w intraday/weekly DBs                     [update_intraday_db.py]
#
# Built for launchd (com.sachoki.dbupdate) — runs Tue–Sat 01:00 local, i.e. just
# after each US market close (00:00–01:00 GET) + settle. MASSIVE only; never yfinance.
# Server-side single-writer endpoints → no DB lock conflicts with the live backend.
#
#   Manual run:        ./update_all.sh
#   Skip intraday:     NO_INTRADAY=1 ./update_all.sh
#   Skip ULTRA rescan: NO_RESCAN=1   ./update_all.sh   (passed through to update_db.sh)
#   Log:               ~/Library/Logs/sachoki_update.log
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
PORT="${BACKEND_PORT:-8080}"
LOG="$HOME/Library/Logs/sachoki_update.log"
LOCK="/tmp/sachoki_update.lock"
mkdir -p "$(dirname "$LOG")"
exec >> "$LOG" 2>&1

# ── single-instance guard (skip if a prior run is still going) ─────────────────
if [ -e "$LOCK" ] && kill -0 "$(cat "$LOCK" 2>/dev/null)" 2>/dev/null; then
  echo "$(date '+%F %T')  ⏭  update already running (pid $(cat "$LOCK")) — skip"; exit 0
fi
echo $$ > "$LOCK"
trap 'rm -f "$LOCK"' EXIT

echo ""
echo "════════════════ $(date '+%F %T %Z') START full daily update ════════════════"

# ── wait for backend (separate launchd agent; allow time after a reboot) ───────
up=0
for i in $(seq 1 30); do
  if curl -sf --max-time 5 "http://127.0.0.1:$PORT/api/studio/incremental-update/status" >/dev/null; then up=1; break; fi
  echo "  waiting for backend :$PORT … ($i/30)"; sleep 10
done
if [ "$up" != "1" ]; then echo "❌ backend :$PORT not responding — aborting"; exit 1; fi

# ── [1/2] 1D bars + ULTRA screener rescan (all 3 universes) ────────────────────
echo "──── [1/2] 1D + ULTRA  (update_db.sh) ────"
BACKEND_PORT="$PORT" ./update_db.sh || echo "  ⚠ update_db.sh returned non-zero"

# MOVED AHEAD OF [2/2] (2026-07-28): options have NO history API, so a day missed here is
# gone FOREVER — and that is not hypothetical: the 07-27 snapshot was lost when the nightly
# hung in the ULTRA re-scan, which used to sit between the delta and this call. The logger
# needs the FRESH bars (latest_edges_map(build=True) reads today's fires), so it cannot move
# to the top — but it can run the moment the 1D delta is in, ahead of the slower intraday
# phase. Non-fatal and isolated either way.
# reconciled. derive_intraday still builds 30m/2h and the 15m-enriched top-up below.
if [ "${NO_INTRADAY:-0}" != "1" ]; then
  cd "$ROOT/backend"
  # 1w gating (2026-07-07): the weekly bar only COMPLETES at the Fri US close, so a mid-week
  # 1w refresh just re-fetches a still-forming bar (~30-67min, the slowest step, for no gain).
  # The launchd job runs Tue-Sat local (after each US close); the Sat-local run = Fri US close
  # = week done → run 1w only then. Override any day with FORCE_1W=1.
  # NIGHTLY INTRADAY V2 (2026-09-21): ONE 30m fetch per ticker at 90 days builds BOTH 1h and 4h.
  # The old loop ran update_intraday_db once per timeframe and each run did its own fetch, so it
  # made 6,406 vendor calls a night at FETCH_DAYS = 15 — a window far too short for a Wilder-14
  # (4H got ~22 bars against the ~81 the seed needs, leaving stored rsi_14 off by a median of 9.6
  # points). --dual satisfies both timeframes with one 90-day window and HALVES the call count to
  # 3,203. The old per-TF invocations are REMOVED, not left alongside: running both paths would
  # re-fetch and re-write the same rows twice. Halved vendor calls in the log is the sanity check.
  echo "──── [2/2] 1h+4h dual update  ($(date '+%T')) ────"
  .venv/bin/python update_intraday_db.py --dual --workers 8 || echo "  ⚠ dual 1h/4h update failed"
  for tf in 1h 4h; do
    # forward labels (fwd/mfe/mae = N BARS ahead) for Studio analytics — idempotent,
    # fills only rows where fwd_5d IS NULL (new bars from the update above)
    .venv/bin/python backfill_intraday_fwd.py "$tf" || echo "  ⚠ $tf fwd backfill failed"
  done
  # 1w still runs on its own path — the weekly bar only COMPLETES at the Fri US close, so a
  # mid-week refresh just re-fetches a still-forming bar. Sat-local run = Fri close = week done.
  if [ "$(date +%u)" = "6" ] || [ "${FORCE_1W:-0}" = "1" ]; then
    echo "──── [2/2] 1w update  ($(date '+%T')) ────"
    .venv/bin/python update_intraday_db.py --tf 1w --workers 8 || echo "  ⚠ 1w update failed"
  else
    echo "  (1w skipped — weekly bar completes Fri; 1w runs on the Sat-local run or with FORCE_1W=1)"
  fi
  # 15m base delta top-up (source layer for derive_intraday full rebuilds; PK dedups).
  # LOCK-GUARD (2026-07-04): skip if a full derive_intraday is running — it holds a READ lock
  # on studio_15m_base.duckdb while this delta needs a WRITE lock → DuckDB conflict drops the
  # derive's in-flight tickers (612 lost once). Derive is one-off; skipping one nightly delta
  # is harmless (the base is only a source layer, re-topped the next night).
  echo "──── [2/2] 15m base delta  ($(date '+%T')) ────"
  if pgrep -f "derive_intraday.py --tf 15m" >/dev/null 2>&1; then
    echo "  ⏭  a 15m derive is active (holds the base read-lock) — skipping base delta this run"
  else
    .venv/bin/python build_15m_base.py --delta --days 4 --workers 6 || echo "  ⚠ 15m base delta failed"
    # 15m ENRICHED top-up (2026-07-09): the base above is only OHLCV; the enriched
    # studio_15m.duckdb (394 signal cols, served by Studio 15M) used to be a manual
    # one-off. Incremental mode appends ONLY bars newer than each ticker's last
    # enriched bar, computing signals over a 45-day warmup window — validated
    # bit-for-bit identical to a full re-derive, and runs in minutes not ~1.5h.
    echo "──── [2/2] 15m enriched top-up  ($(date '+%T')) ────"
    # nice + modest worker count: the 6-worker derive saturates every core; `nice`
    # keeps the live backend responsive if a user is active when this runs.
    nice -n 10 .venv/bin/python derive_intraday.py --tf 15m --all --incremental --workers 4 \
      || echo "  ⚠ 15m enriched top-up failed"
    # refresh the High-Base scanner's per-day MIN-15m-RSI cache from the new enriched bars
    .venv/bin/python build_m15_dayrsi.py 2>/dev/null || echo "  (m15_dayrsi rebuild skipped)"
    # ★ L-BAL + L-VX + OVD map + VOL7 (2026-09-06/07): the TradingView display scripts ported —
    # per-session 15m L-label balance (UDN / UDN+ / ★ marks) → data/lbal_signals.parquet, daily
    # L34/L46 graded V·VL·VH·VX → data/lvx_signals.parquet, OVD tokens OB/RC/CD/HO/NM? →
    # data/ovdmap_signals.parquet, 7-level volume regime (M·σ, jumps, VB2, SHIFT) →
    # data/vol7_signals.parquet; read by the Ultra chips, the chart lines and the Superchart rows.
    # READ-ONLY on studio_15m / studio_1h / studio_analytics (no lock conflict once the derive
    # above is done), full rebuild ~8 min, atomic replace so the live backend never sees a
    # half-written file. Descriptive only — never a ranking input. Non-fatal.
    echo "──── ★ L-BAL / L-VX / OVD rebuild  ($(date '+%T')) ────"
    nice -n 10 .venv/bin/python lbal_build.py || echo "  ⚠ L-BAL rebuild failed"

    # ▽△ BOTTOM-ANATOMY history (2026-09-11): the per-session verdict + 0-8 score that /api/day1h
    # draws on the chart, for the whole universe and the whole history → data/anatomy_signals.parquet.
    # The endpoint computes it live one ticker at a time, so without this there is nothing to measure
    # the row against. 1D + 1H + 15m, read-only, ~4 min, atomic replace, non-fatal — hence INSIDE the
    # intraday branch, after the derive.
    #   MUST STAY IN PARITY WITH main.py::api_day1h — backend/tests/test_anatomy_parity.py. A moving
    #   threshold in `key` once put the 0-8 score at 54.7 % agreement with the chart and voided two
    #   sealed families. If that endpoint's definition changes, this file changes with it.
    #   DESCRIPTIVE ONLY: a DETECTOR (1.37x lift, 76 % recall, 33 % precision). Never a ranking input.
    echo "──── ▽△ BOTTOM-ANATOMY rebuild  ($(date '+%T')) ────"
    nice -n 10 .venv/bin/python anatomy_build.py || echo "  ⚠ anatomy rebuild failed"
  fi
else
  echo "  (intraday skipped — NO_INTRADAY=1)"
fi

# ── 🔷 SHAPE × CONTEXT (2026-09-10) ───────────────────────────────────────────
# The TradingView "260910_SHAPE_CTX" script ported for display: the seven body-nest shapes under
# the Pine display priority (MTH > CL4 > MID > EXP > CON > LST > WRP), the ↑↓ arrow on the swallow
# shapes, the EFFORT grade 0-2, the 📍FLOOR / 🧱KEY / 🏆RS legs, the ⛔KNIFE veto and the two
# cluster axes 🎯 diversity / 🔁 density → data/shapectx_signals.parquet. Read by the Ultra chips,
# the Superchart SHAPE row and the Superchart CSV.
#   1D ONLY — reads studio_analytics `bars`, so it does NOT depend on the intraday derive above and
#   lives outside that branch. ~20 s, atomic replace, non-fatal.
#   DESCRIPTIVE ONLY: four sealed families (MOTHER_V1, SHAPE_CLUSTER_V1, SHAPE_GATE_V1,
#   SWALLOW_DIR_V1), k = 21, 0 BUILD — clustering measured monotonically WORSE, not better. The one
#   cell with two-window evidence is LST↑ and it is a VETO. Never a ranking input.
echo "──── 🔷 SHAPE × CONTEXT rebuild  ($(date '+%T')) ────"
( cd "$ROOT/backend" && nice -n 10 .venv/bin/python shape_ctx_build.py ) || echo "  ⚠ SHAPE_CTX rebuild failed"

# ── 💠 GEX edge-context forward log (2026-07-22) ──────────────────────────────
# Options have NO historical snapshot, so GEX-confluence can only be validated by
# capturing the LIVE GEX context at each edge-fire day and joining forward returns
# months later. Runs at nightly (post-close) = EOD gamma levels. Non-fatal, isolated.
echo "──── 💠 GEX edge-context log  ($(date '+%T')) ────"
( cd "$ROOT/backend" && .venv/bin/python gex_edge_logger.py ) || echo "  ⚠ GEX log skipped (options plan off?)"

# ── [2/2] intraday + weekly DBs ────────────────────────────────────────────────
# NOTE (2026-07-24): evaluated deriving 1h/4h from the 15m base (single source) instead of the
# separate MASSIVE fetch below. Validation: OHLC came out bit-identical (1h & 4h, AAPL+NVDA,
# same 13:30/17:30 anchors) and derived 4h was even MORE complete — BUT 15m-summed VOLUME
# differs from MASSIVE's native intraday volume on a minority of bars (~9% on some 1h bars).
# Since the VSA/WLNBB/VABS signals are volume-classified, that would subtly shift them app-wide,
# so 1h/4h stay FETCHED (native volume authoritative). Revisit only if the volume source is
echo "════════════════ $(date '+%F %T %Z') DONE ════════════════"
