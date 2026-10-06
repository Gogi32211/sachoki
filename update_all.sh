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

# ── PIPELINE STATUS MODEL (2026-09-22) ────────────────────────────────────────────────────────
# The 09-22 run showed the cost of `critical_command || echo "⚠ failed"`: the dual writer's trust
# gate returned NOT PASS and exited 2, the `||` turned that into one line of text, and EIGHT
# downstream stages then built a derived layer on a store the gate had just refused to certify.
# That night the NOT PASS was a false alarm, so nothing was actually corrupted — but on a real
# key_difference < 0 the same path would have propagated the damage everywhere.
#
# "Stop everything" is the wrong fix too. The options snapshot has NO history API, so killing the
# run on an unrelated 1H/4H fault would turn one failure into two, and the second one permanent.
#
# So: DEPENDENCY-AWARE FAIL-CLOSED. Stages that READ the 1H/4H canonical are skipped when the
# writer's gate fails; stages that do not are unaffected and still run. The dependency list is
# MEASURED, not assumed — lbal_build ATTACHes studio_1h for its L-VX 60m counts and anatomy_build
# opens it for the 1H session shape, so both are dependents even though they look like 15m jobs.
#
#   PASS      everything required ran
#   DEGRADED  the 1H/4H trust gate failed, its dependents were skipped, independents all ran
#   FAILED    orchestration itself broke, or a time-critical independent capture failed
INTRADAY_TRUSTED=1        # until the dual writer's gate says otherwise
DEGRADED=0
FAILED=0
SKIPPED=""

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
  # made 6,406 ticker FETCHES a night at FETCH_DAYS = 15 — a window far too short for a Wilder-14
  # (4H got ~22 bars against the ~81 the seed needs, leaving stored rsi_14 off by a median of 9.6
  # points). --dual satisfies both timeframes with one 90-day window and HALVES the ticker fetches
  # to 3,203. The old per-TF invocations are REMOVED, not left alongside: running both paths would
  # re-fetch and re-write the same rows twice.
  #   MEASURED ON THE FIRST LIVE RUN (2026-09-22): 3,203 ticker fetches -> 3,227 HTTP requests and
  #   346.8 MB. Only ~24 tickers needed a second cursor page, so HTTP requests track ticker fetches
  #   almost exactly and they halve too (the old path's ~6,406 one-page fetches -> 3,227).
  #   An earlier note here predicted ~6,400 requests. That was extrapolated from AAPL alone, which
  #   is atypical: its extended-hours activity gives 1,990 rows over 90 days and needs two pages,
  #   while most tickers stay under the page cap. n = 1 is not a fleet measurement.
  #   Bytes DO rise (~64k -> ~108k per ticker): that is the 90-day warm-up's price, paid on purpose.
  #   The sanity check that the old path is really gone stays TICKER FETCHES ~3,203 and ONE dual
  #   block in this log instead of two per-TF ones.
  echo "──── [2/2] 1h+4h dual update  ($(date '+%T')) ────"
  # THE TRUST GATE. `if` instead of `||` on purpose: the writer's exit code has to be able to
  # change what happens next, not merely leave a sentence in the log.
  if .venv/bin/python update_intraday_db.py --dual --workers 8; then
    INTRADAY_TRUSTED=1
  else
    rc=$?
    INTRADAY_TRUSTED=0
    DEGRADED=1
    echo "  ⛔ 1H/4H TRUST GATE FAILED (exit $rc) — every stage that READS the 1H/4H canonical is"
    echo "     SKIPPED this run. Independent and time-critical stages continue below."
  fi
  if [ "$INTRADAY_TRUSTED" = "1" ]; then
    for tf in 1h 4h; do
      # forward labels (fwd/mfe/mae = N BARS ahead) for Studio analytics — idempotent,
      # fills only rows where fwd_5d IS NULL (new bars from the update above)
      .venv/bin/python backfill_intraday_fwd.py "$tf" || echo "  ⚠ $tf fwd backfill failed"
    done
  else
    echo "  ⏭  skipping 1H/4H forward labels — they read the untrusted canonical"
    SKIPPED="$SKIPPED fwd-labels"
  fi
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
    #   ⚠️ 1H DEPENDENT (verified 2026-09-22): lbal_build ATTACHes studio_1h READ_ONLY for the
    #   L-VX 60m family counts. It reads as a 15m job and is not one. VOL7 inside it is daily-only
    #   and could in principle still be built — splitting the module is a separate change.
    if [ "$INTRADAY_TRUSTED" = "1" ]; then
      echo "──── ★ L-BAL / L-VX / OVD rebuild  ($(date '+%T')) ────"
      nice -n 10 .venv/bin/python lbal_build.py || echo "  ⚠ L-BAL rebuild failed"
    else
      echo "  ⏭  skipping ★ L-BAL / L-VX / OVD — lbal_build reads studio_1h"
      SKIPPED="$SKIPPED lbal"
    fi

    # ▽△ BOTTOM-ANATOMY history (2026-09-11): the per-session verdict + 0-8 score that /api/day1h
    # draws on the chart, for the whole universe and the whole history → data/anatomy_signals.parquet.
    # The endpoint computes it live one ticker at a time, so without this there is nothing to measure
    # the row against. 1D + 1H + 15m, read-only, ~4 min, atomic replace, non-fatal — hence INSIDE the
    # intraday branch, after the derive.
    #   MUST STAY IN PARITY WITH main.py::api_day1h — backend/tests/test_anatomy_parity.py. A moving
    #   threshold in `key` once put the 0-8 score at 54.7 % agreement with the chart and voided two
    #   sealed families. If that endpoint's definition changes, this file changes with it.
    #   DESCRIPTIVE ONLY: a DETECTOR (1.37x lift, 76 % recall, 33 % precision). Never a ranking input.
    #   ⚠️ 1H DEPENDENT (verified 2026-09-22): anatomy_build opens studio_1h for the 1H session
    #   shape (low_early, z_first/t_last, the late hi-vol T-reversal).
    if [ "$INTRADAY_TRUSTED" = "1" ]; then
      echo "──── ▽△ BOTTOM-ANATOMY rebuild  ($(date '+%T')) ────"
      nice -n 10 .venv/bin/python anatomy_build.py || echo "  ⚠ anatomy rebuild failed"
    else
      echo "  ⏭  skipping ▽△ BOTTOM-ANATOMY — anatomy_build reads studio_1h"
      SKIPPED="$SKIPPED anatomy"
    fi
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
#   INDEPENDENT of the 1H/4H trust gate (verified 2026-09-22: no studio_1h / studio_4h reference).
echo "──── 🔷 SHAPE × CONTEXT rebuild  ($(date '+%T')) ────"
( cd "$ROOT/backend" && nice -n 10 .venv/bin/python shape_ctx_build.py ) || { echo "  ⚠ SHAPE_CTX rebuild failed"; DEGRADED=1; }

# ── ⚖️ PRICE × VOLUME MULTI (2026-09-22) ──────────────────────────────────────
# The TradingView "260921_PV_MULTI" script ported for display: the nine ordinal price×volume shapes
# (DIV UPP UPR REV RUP VUP TURN UP4 RE2) computed on BOTH price sources — close and ohlc4 — since
# the two disagree on 47% of hits → data/pv_multi_signals.parquet. Read by the Ultra chips and
# filters, the two Superchart PV rows and the Superchart CSV.
#   1D ONLY — reads studio_analytics `bars`, so it is INDEPENDENT of the 1H/4H trust gate and lives
#   outside that branch, beside SHAPE_CTX. ~10 s, atomic replace, non-fatal.
#   DESCRIPTIVE ONLY: PV_MULTI_V1 sealed k = 9 → 0 BUILD / 4 VETO_CANDIDATE / 5 NULL, every cell
#   negative in MINE. The four VETOes (REV RE2 UPP RUP) are RECORDED, NOT APPLIED. Never a ranking
#   input. Parity with the sealed research is pinned by tests/test_pv_multi_display_parity.py.
echo "──── ⚖️ PRICE × VOLUME rebuild  ($(date '+%T')) ────"
( cd "$ROOT/backend" && nice -n 10 .venv/bin/python pv_multi_build.py ) || { echo "  ⚠ PV_MULTI rebuild failed"; DEGRADED=1; }

# ── 📣 VOL ECHO (2026-09-27) ──────────────────────────────────────────────────
# The TradingView "260925_VOL_ECHO" script ported at its defaults: SPIKE · SPK · VE echo · Q quiet ·
# R after breakdown · ▲/▼ release · BO▲ BD▼ BOV▲ BDV▼ → data/vol_echo_signals.parquet. Read by the
# Ultra chips and filters, the Superchart ECHO row and the Superchart CSV.
#   1D ONLY — reads studio_analytics `bars`; INDEPENDENT of the 1H/4H trust gate. ~50 s, atomic
#   replace, non-fatal.
#   DESCRIPTIVE ONLY: every long study NULL; QR_REL_V1 veto (Q∧R then first ▲) CONFIRMED and
#   RECORDED, NOT APPLIED. Never a ranking input. Port pinned by tests/test_vol_echo_port.py.
echo "──── 📣 VOL ECHO rebuild  ($(date '+%T')) ────"
( cd "$ROOT/backend" && nice -n 10 .venv/bin/python vol_echo_build.py ) || { echo "  ⚠ VOL_ECHO rebuild failed"; DEGRADED=1; }

# ── ⟲ TURN·58 + ⟲ROW for Ultra (2026-09-28) ─────────────────────────────────────
# Last 15 sessions per ticker of the two Superchart turn-zone gauges (TURN·58 count, ⟲ROW per-row
# tiers) → data/turn_rowseq_signals.parquet, read by the Ultra filter chips. Runs AFTER the display
# stores above (it reads lbal/lvx/ovd/vol7/shape/pv/vol_echo + anatomy + studio_4h/1h + EDGE masks) and
# evaluates them with the Superchart's own JS (esbuild bundle of the catalog + lib/turnCount.js +
# lib/rowSeq.js). Atomic replace, non-fatal.
#   DESCRIPTIVE ONLY (TURN_SET_V1, ROWSEQ_V1): turn-zone identification, never a ranking input.
echo "──── ⟲ TURN·58 / ⟲ROW rebuild  ($(date '+%T')) ────"
( cd "$ROOT/backend" && nice -n 10 .venv/bin/python turn_rowseq_build.py ) || { echo "  ⚠ TURN/ROW rebuild failed"; DEGRADED=1; }

# ── V4 history increment (2026-10-05, user: "V4 mtlian DB bazashi" + "ki daamate gamis damatebebi") ──
# The Superchart V4 per bar (fired catalog keys) for every ticker → data/v4_signals.parquet, read by
# /api/studio/v4-marks (score re-summed with the CURRENT v4Weights.js). Recomputes the last 14 calendar
# days and merges them into the store, so a night skipped by the trust gate is healed by the next one.
# Same sources as TURN/ROW above (display stores + anatomy + EDGE frame) PLUS studio_4h/1h/15m for the
# REV / MTF-echo / turn-echo / mtf_conf fields → gated on the 1H/4H trust gate. ~5-10 min, atomic
# replace, non-fatal. DESCRIPTIVE ONLY: V4 as a ranker was measured NULL (research_out/V4_HISTORY_V1.md).
if [ "$INTRADAY_TRUSTED" = "1" ]; then
  echo "──── V4 history increment  ($(date '+%T')) ────"
  V4_SINCE=$(date -v-14d '+%Y-%m-%d')
  ( cd "$ROOT/backend" && nice -n 10 .venv/bin/python -W ignore v4_history_build.py --since "$V4_SINCE" --nightly ) \
    || { echo "  ⚠ V4 history increment failed"; DEGRADED=1; }
else
  echo "  ⏭  skipping V4 history increment — it reads studio_4h/1h/15m (next night heals the 14-day window)"
  SKIPPED="$SKIPPED v4hist"
fi

# ── 💠 GEX edge-context forward log (2026-07-22) ──────────────────────────────
# Options have NO historical snapshot, so GEX-confluence can only be validated by
# capturing the LIVE GEX context at each edge-fire day and joining forward returns
# months later. Runs at nightly (post-close) = EOD gamma levels. Non-fatal, isolated.
#   TIME-CRITICAL AND INDEPENDENT. There is no history API: a day missed here is gone forever, and
#   that is not hypothetical — 2026-07-27 was lost to a hang, and 2026-09-22 to an aborted run.
#   It must NEVER be gated on the 1H/4H writer (it reads neither), and its own failure is the one
#   independent failure that makes the whole night FAILED rather than merely DEGRADED.
echo "──── 💠 GEX edge-context log  ($(date '+%T')) ────"
if ( cd "$ROOT/backend" && .venv/bin/python gex_edge_logger.py ); then
  :
else
  echo "  ⛔ GEX log FAILED — today's options context cannot be recaptured (options plan off?)"
  FAILED=1
fi

# ── [2/2] intraday + weekly DBs ────────────────────────────────────────────────
# NOTE (2026-07-24): evaluated deriving 1h/4h from the 15m base (single source) instead of the
# separate MASSIVE fetch below. Validation: OHLC came out bit-identical (1h & 4h, AAPL+NVDA,
# same 13:30/17:30 anchors) and derived 4h was even MORE complete — BUT 15m-summed VOLUME
# differs from MASSIVE's native intraday volume on a minority of bars (~9% on some 1h bars).
# Since the VSA/WLNBB/VABS signals are volume-classified, that would subtly shift them app-wide,
# so 1h/4h stay FETCHED (native volume authoritative). Revisit only if the volume source is
# ── FINAL STATUS ──────────────────────────────────────────────────────────────────────────────
# One unambiguous word, because "DONE" next to a skipped critical branch is how 2026-09-22 read.
if [ "$FAILED" = "1" ]; then
  STATUS="FAILED"; CODE=1
elif [ "$DEGRADED" = "1" ]; then
  STATUS="DEGRADED"; CODE=3
else
  STATUS="PASS"; CODE=0
fi
echo "════════════════ $(date '+%F %T %Z') DONE — STATUS: $STATUS ════════════════"
if [ "$INTRADAY_TRUSTED" != "1" ]; then
  echo "   1H/4H trust gate FAILED; skipped:${SKIPPED:- (none)}"
  echo "   The 1H/4H canonical and everything derived from it are LAST NIGHT'S. Independent"
  echo "   stages (15m chain, SHAPE_CTX, GEX) ran normally and are current."
fi
exit $CODE
