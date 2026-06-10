#!/usr/bin/env bash
# update_db.sh — ერთი ბრძანება: Studio DB-ის განახლება ბოლო დახურულ ბარზე,
# სამივე ინდექსზე (sp500 / nasdaq / russell2k).
#
# იყენებს გაშვებული backend-ის incremental-delta endpoint-ს — DB-write სერვერს
# ეკუთვნის (single-writer კონფლიქტი არ ხდება). yfinance არასდროს; MASSIVE API.
#
#   გამოყენება:   ./update_db.sh
#   სხვა პორტი:   BACKEND_PORT=8000 ./update_db.sh
set -uo pipefail
PORT="${BACKEND_PORT:-8080}"
BASE="http://127.0.0.1:$PORT/api/studio"
UNIVERSES='["sp500","nasdaq","russell2k"]'
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
echo "✅ მზადაა."
