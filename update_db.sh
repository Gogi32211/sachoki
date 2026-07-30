#!/usr/bin/env bash
# update_db.sh — ერთი ბრძანება: Studio DB-ის განახლება ბოლო დახურულ ბარზე,
# ოთხივე universe-ზე (sp500 / nasdaq / russell2k / index).
#
# იყენებს გაშვებული backend-ის incremental-delta endpoint-ს — DB-write სერვერს
# ეკუთვნის (single-writer კონფლიქტი არ ხდება). yfinance არასდროს; MASSIVE API.
#
#   გამოყენება:   ./update_db.sh
#   სხვა პორტი:   BACKEND_PORT=8000 ./update_db.sh
set -uo pipefail
PORT="${BACKEND_PORT:-8080}"
BASE="http://127.0.0.1:$PORT/api/studio"
# 2026-07-22: "index" (16 sector/market ETF) added — this list is the ACTUAL
# nightly trigger (launchd com.sachoki.dbupdate → update_all.sh → here), separate
# from main.py's in-app APScheduler cron which already included "index" — this
# script's list was the one actually running each night and had been missing it,
# so the index universe silently lagged a day behind every refresh.
UNIVERSES='["sp500","nasdaq","russell2k","index"]'
# 2026-07-29: this pointed at ~/Downloads/studio_analytics.duckdb — a file that does not
# exist. Together with `python3` (system 3.11, no duckdb module) and a `2>/dev/null`, the
# max-date verification below had been silently dead, which is why a nightly could report
# "✅ მზადაა" on a run whose delta worker was SIGKILLed and inserted nothing.
ROOT="$(cd "$(dirname "$0")" && pwd)"
DB="${STUDIO_DB:-$ROOT/data/studio_analytics.duckdb}"
PY="$ROOT/backend/.venv/bin/python"          # system python3 has no duckdb — never use it here

# 1) სერვერი მუშაობს?
if ! curl -sf --max-time 5 "$BASE/incremental-update/status" >/dev/null; then
  echo "❌ backend არ პასუხობს :$PORT-ზე — სერვერი გაშვებულია?"; exit 1
fi

# 1b) max(date) BEFORE the run — the only honest way to prove the delta actually landed.
_maxdate() { "$PY" -c "
import sys, duckdb
try:
    c = duckdb.connect('$DB', read_only=True)
    print(c.execute('SELECT max(date) FROM bars').fetchone()[0]); c.close()
except Exception as e:
    print('ERR:' + str(e)[:90])
" 2>/dev/null || echo "ERR:python-failed"; }
DATE_BEFORE="$(_maxdate)"
echo "  ბაზის ბოლო თარიღი განახლებამდე: $DATE_BEFORE"
# UTC stamp for the ULTRA re-scan freshness assertion at the end (last_scan is UTC ISO)
RUN_STARTED="$(date -u '+%Y-%m-%dT%H:%M:%S')"

# 2) უკვე მუშაობს?
running=$(curl -s "$BASE/incremental-update/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['running'])" 2>/dev/null)
if [ "$running" = "True" ]; then
  echo "⏳ განახლება უკვე მიდის — სტატუსს ვადევნებ თვალს…"
else
  echo "🚀 ვუშვებ განახლებას (sp500 + nasdaq + russell2k)…"
  curl -s -X POST "$BASE/incremental-update" -H "Content-Type: application/json" \
       -d "{\"universes\":$UNIVERSES}" >/dev/null
  sleep 2
fi

# 3) ვადევნებ თვალს დასრულებამდე
while true; do
  s=$(curl -s "$BASE/incremental-update/status")
  fin=$(echo "$s" | python3 -c "import sys,json;print(not json.load(sys.stdin)['running'])" 2>/dev/null)
  echo "$s" | python3 -c "import sys,json;p=json.load(sys.stdin)['progress'];print(f\"  [{p['stage']}] {p['done']}/{p['total']} ({p['pct']}%)  +{p['new_rows']} rows · {p['errors']} err\")" 2>/dev/null
  [ "$fin" = "True" ] && break
  sleep 15
done

# 4) summary — AND fail loudly if the worker died. `running:false` alone means "not running",
#    NOT "succeeded": on a crash the backend stores results={"error": ...} and the old summary
#    silently printed nothing but "duration: Nones" and carried on to print "✅ მზადაა".
echo "── summary ─────────────────────────────"
STATUS_JSON="$(curl -s "$BASE/incremental-update/status")"
echo "$STATUS_JSON" | "$PY" -c "
import sys, json
r = json.load(sys.stdin).get('results', {}) or {}
for u, d in (r.get('universes') or {}).items():
    print(f\"  {u:10}+{d['new_rows_inserted']:>5} rows · {d['errors']} err · {d['affected_tickers']} tickers\")
print(f\"  duration: {r.get('duration_sec')}s\")
" 2>/dev/null

# 5) ვამოწმებ ბოლო თარიღს — და ვადარებ განახლებამდელს
DATE_AFTER="$(_maxdate)"
echo "── ბოლო თარიღი DB-ში ────────────────────"
"$PY" - "$DB" <<'PY'
import sys, duckdb
try:
    c = duckdb.connect(sys.argv[1], read_only=True)
    for u, d, n in c.execute("SELECT universe,max(date),count(distinct ticker) "
                             "FROM bars GROUP BY universe ORDER BY universe").fetchall():
        print(f"  {u:10}{d}  ({n} tickers)")
    c.close()
except Exception as e:
    print("  ⚠ max-date შემოწმება ჩავარდა:", e)
PY

DELTA_ERR="$(echo "$STATUS_JSON" | "$PY" -c \
  "import sys,json; print(((json.load(sys.stdin).get('results') or {}).get('error') or '').strip())" 2>/dev/null)"
DELTA_OK=1
if [ -n "$DELTA_ERR" ]; then
  DELTA_OK=0
  echo ""
  echo "❌❌ 1D DELTA FAILED — ბაზა არ განახლდა ❌❌"
  echo "$DELTA_ERR" | head -20 | sed 's/^/   /'
elif [ "$DATE_AFTER" = "$DATE_BEFORE" ]; then
  echo ""
  echo "⚠️  max(date) არ დაიძრა: $DATE_BEFORE → $DATE_AFTER"
  echo "   (ნორმალურია შაბ/კვირას ან დღესასწაულზე — სამუშაო დღეს კი ჩავარდნაა)"
else
  echo "  ✔ $DATE_BEFORE → $DATE_AFTER"
fi

# 6) ULTRA re-scan — the screener snapshot is a SEPARATE cached job from the bars DB.
#    Updating bars does NOT refresh it, so without this the screener shows yesterday's
#    RSI/CCI/TZ/score even though the DB is current. Re-scan each universe (heavy: full
#    Turbo pass). Skip with NO_RESCAN=1 ./update_db.sh
API="http://127.0.0.1:$PORT/api"
if [ "${NO_RESCAN:-0}" != "1" ]; then
  echo "── ULTRA re-scan (screener snapshot) ───────"
  # NOTE (2026-07-28): the `|| echo True` is load-bearing. When /ultra-scan/status 500s the
  # python parse fails, 2>/dev/null eats it and the function returned an EMPTY string — so
  # `until [ "$(_running)" = "False" ]` spun forever. That is exactly how a nightly run hung
  # for 5h and never reached the intraday phase or the GEX logger. Empty now means "still
  # running", and every wait below is bounded so a broken endpoint costs minutes, not a night.
  _running() { curl -s --max-time 8 "$API/ultra-scan/status" | python3 -c "import sys,json;print(json.load(sys.stdin).get('running',True))" 2>/dev/null || echo True; }
  _snap() { curl -s --max-time 8 "$API/ultra-scan/results?universe=$1&tf=1d" | python3 -c "import sys,json;print(json.load(sys.stdin).get('last_scan',''))" 2>/dev/null; }
  for u in sp500 nasdaq russell2k; do
    printf "  🔄 %-10s" "$u"
    # 1) wait for any in-flight scan to clear so our trigger isn't rejected (409)
    for i in $(seq 1 100); do [ "$(_running)" = "False" ] && break; sleep 3; done
    old=$(_snap "$u")
    # 2) trigger (retry once if it 409s because a scan slipped in)
    resp=$(curl -s -X POST "$API/ultra-scan/trigger?universe=$u&tf=1d")
    echo "$resp" | grep -q "already running" && { for i in $(seq 1 100); do [ "$(_running)" = "False" ] && break; sleep 3; done; curl -s -X POST "$API/ultra-scan/trigger?universe=$u&tf=1d" >/dev/null; }
    sleep 3
    # 3) wait for THIS scan to finish (default-True on status hiccup → never break early)
    for i in $(seq 1 180); do
      [ "$(_running)" = "False" ] && break
      sleep 5
    done
    # 4) VERIFY the snapshot actually advanced.
    #
    # 2026-07-30: comparing `new != old` the instant `running` flips to False is a RACE, and
    # it was wrong in BOTH directions on the 07-30 nightly — it reported nasdaq as "NOT
    # refreshed" when it had refreshed, and russell2k as "done ✓" when its scan had died in
    # the merge phase and left a day-old snapshot. The false POSITIVE is the dangerous one:
    # a genuinely stale screener passes silently.
    # Fixed by polling for the snapshot to actually appear (it is published a moment after
    # the running flag clears) and then asserting it is NEWER THAN THIS RUN, rather than
    # merely different from what we happened to read before triggering.
    new=""
    for i in $(seq 1 20); do
      new=$(_snap "$u")
      [ -n "$new" ] && [ "$new" != "$old" ] && break
      sleep 3
    done
    if [ -z "$new" ]; then
      echo "⚠ NOT refreshed (no snapshot returned)"
    elif [ "$new" = "$old" ]; then
      echo "⚠ NOT refreshed (still $old)"
    elif [ "$new" \< "$RUN_STARTED" ]; then
      # advanced, but to a timestamp older than this run began — not our scan
      echo "⚠ STALE ($new predates this run)"
    else
      echo "done ✓ ($new)"
    fi
  done
else
  echo "  (ULTRA re-scan გამოტოვდა — NO_RESCAN=1)"
fi
if [ "$DELTA_OK" = "1" ]; then
  echo "✅ მზადაა."
else
  # Non-zero so update_all.sh's `|| echo "⚠ update_db.sh returned non-zero"` fires and the
  # nightly log carries a searchable failure marker instead of a green "✅ მზადაა".
  echo "❌ დასრულდა შეცდომით — 1D ბარები არ ჩაიწერა."
  exit 1
fi
