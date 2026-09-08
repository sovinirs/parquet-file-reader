#!/usr/bin/env bash
# Full verification: API + filter semantics, then the UI driven in headless Chrome.
#   ./tests/run.sh
set -uo pipefail
cd "$(dirname "$0")/.."

PORT="${TEST_PORT:-8901}"
BASE="http://127.0.0.1:${PORT}"
PY=.venv/bin/python
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
SAMPLE="$(pwd)/sample/orders.parquet"
ASSETS="$(pwd)/sample/assets.parquet"

[ -x "$PY" ] || { echo "Run ./run.sh once first to create .venv"; exit 1; }
[ -f "$SAMPLE" ] || $PY tests/make_sample.py
[ -f "$ASSETS" ] || $PY tests/make_assets.py

echo "Starting test server on ${PORT}…"
$PY -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" >/tmp/pqs-test.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null; rm -f app/static/__selftest.html app/static/__diffselftest.html' EXIT

for _ in $(seq 1 40); do
  curl -sf "${BASE}/api/recents" >/dev/null 2>&1 && break
  sleep 0.25
done

echo
echo "═══ API + filter semantics ═══"
$PY tests/test_api.py "$BASE" "$SAMPLE"
API_STATUS=$?

echo
echo "═══ UI workflow (headless Chrome) ═══"
if [ ! -x "$CHROME" ]; then
  echo "  skipped — Chrome not found at $CHROME (set CHROME=/path/to/chrome)"
  UI_STATUS=0
  DIFF_STATUS=0
else
  # The harness must be same-origin to fetch the app's own HTML.
  cp tests/ui_selftest.html app/static/__selftest.html
  ENCODED=$($PY -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$SAMPLE")
  "$CHROME" --headless --disable-gpu --no-sandbox --virtual-time-budget=120000 \
    --dump-dom "${BASE}/static/__selftest.html?path=${ENCODED}" 2>/dev/null > /tmp/pqs-ui.html
  rm -f app/static/__selftest.html
  $PY - <<'PY'
import html, re, sys
dom = open('/tmp/pqs-ui.html').read()
match = re.search(r'<div id="results"[^>]*>(.*?)</div>', dom, re.S)
if not match:
    print("  FAIL — the harness produced no results (is the page erroring?)"); sys.exit(1)
lines = [l for l in html.unescape(match.group(1)).splitlines() if '|' in l]
for line in lines:
    print("  " + line.replace('PASS |', 'ok  ').replace('FAIL |', 'FAIL'))
bad = sum(1 for l in lines if l.startswith('FAIL'))
print("\n  {} passed, {} failed".format(len(lines) - bad, bad))
sys.exit(1 if bad else 0)
PY
  UI_STATUS=$?

  echo
  echo "═══ Difference analysis UI (headless Chrome) ═══"
  cp tests/diff_selftest.html app/static/__diffselftest.html
  ENCODED=$($PY -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$ASSETS")
  "$CHROME" --headless --disable-gpu --no-sandbox --virtual-time-budget=120000 \
    --dump-dom "${BASE}/static/__diffselftest.html?path=${ENCODED}" 2>/dev/null > /tmp/pqs-diff.html
  rm -f app/static/__diffselftest.html
  $PY - <<'DIFFPY'
import html, re, sys
dom = open('/tmp/pqs-diff.html').read()
match = re.search(r'<div id="results"[^>]*>(.*?)</div>', dom, re.S)
if not match:
    print("  FAIL — the harness produced no results"); sys.exit(1)
lines = [l for l in html.unescape(match.group(1)).splitlines() if '|' in l]
for line in lines:
    print("  " + line.replace('PASS |', 'ok  ').replace('FAIL |', 'FAIL'))
bad = sum(1 for l in lines if l.startswith('FAIL'))
print("\n  {} passed, {} failed".format(len(lines) - bad, bad))
sys.exit(1 if bad else 0)
DIFFPY
  DIFF_STATUS=$?
fi

echo
if [ $API_STATUS -eq 0 ] && [ $UI_STATUS -eq 0 ] && [ $DIFF_STATUS -eq 0 ]; then
  echo "All checks passed."
else
  echo "Some checks failed (api=$API_STATUS ui=$UI_STATUS diff=$DIFF_STATUS)"; exit 1
fi
