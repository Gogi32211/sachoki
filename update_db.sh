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
DB="${STUDIO_DB:-/Users/sachoki/Downloads/studio_analytics.duckdb}"

# 1) სერვერი მუშაობს?
if ! curl -sf --max-time 5 "$BASE/incremental-update/status" >/dev/null; then
  echo "❌ backend არ პასუხობს :$PORT-ზე — სერვერი გაშვებულია?"; exit 1
fi

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

# 4) summary
echo "── summary ─────────────────────────────"
curl -s "$BASE/incremental-update/status" | python3 -c "
import sys,json
r=json.load(sys.stdin).get('results',{})
for u,d in r.get('universes',{}).items():
    print(f\"  {u:10}+{d['new_rows_inserted']:>5} rows · {d['errors']} err · {d['affected_tickers']} tickers\")
print(f\"  duration: {r.get('duration_sec')}s\")
" 2>/dev/null

# 5) ვამოწმებ ბოლო თარიღს (read-only — write-ს არ ეხება)
python3 - "$DB" <<'PY' 2>/dev/null
import sys, duckdb
try:
    c=duckdb.connect(sys.argv[1], read_only=True)
    print("── ბოლო თარიღი DB-ში ────────────────────")
    for u,d,n in c.execute("SELECT universe,max(date),count(distinct ticker) FROM bars GROUP BY universe ORDER BY universe").fetchall():
        print(f"  {u:10}{d}  ({n} tickers)")
    c.close()
except Exception as e:
    print("  (max-date შემოწმება გამოტოვდა:", e, ")")
PY

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
    # 4) VERIFY the snapshot timestamp actually advanced
    new=$(_snap "$u")
    if [ -n "$new" ] && [ "$new" != "$old" ]; then echo "done ✓"; else echo "⚠ NOT refreshed (still $old)"; fi
  done
else
  echo "  (ULTRA re-scan გამოტოვდა — NO_RESCAN=1)"
fi
echo "✅ მზადაა."
