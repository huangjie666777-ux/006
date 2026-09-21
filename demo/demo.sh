#!/usr/bin/env bash
# EventRelay demo: publish, replay and live-subscribe.
# Usage: ./demo/demo.sh [port]
set -euo pipefail

PORT="${1:-5080}"
BASE="http://127.0.0.1:${PORT}"
STREAM="demo-$$"

cd "$(dirname "$0")/.."
export ASPNETCORE_URLS="$BASE"
export Logging__LogLevel__Default=Warning

echo "==> Starting EventRelay on $BASE"
dotnet build src/EventRelay.Api -v q --nologo
dotnet src/EventRelay.Api/bin/Debug/net8.0/EventRelay.Api.dll &
APP_PID=$!
trap 'kill $APP_PID 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  curl -sf "$BASE/" >/dev/null 2>&1 && break
  sleep 0.5
done

echo "==> 1. Publish three events to stream '$STREAM'"
for n in 1 2 3; do
  curl -sf -X POST "$BASE/streams/$STREAM/events" \
    -H 'Content-Type: application/json' \
    -d "{\"type\":\"tick\",\"data\":{\"n\":$n}}" | cat
  echo
done

echo "==> 2. Replay history from the beginning (after=0), first 3 frames"
timeout 3 curl -sN "$BASE/streams/$STREAM/events?after=0" | head -n 12 || true

echo "==> 3. Replay using Last-Event-ID: 2"
timeout 3 curl -sN -H 'Last-Event-ID: 2' "$BASE/streams/$STREAM/events" | head -n 8 || true

echo "==> 4. Live subscription: subscribe first, then publish a new event"
curl -sN "$BASE/streams/$STREAM/events?after=3" > /tmp/eventrelay-live.txt &
CURL_PID=$!
sleep 1
curl -sf -X POST "$BASE/streams/$STREAM/events" \
  -H 'Content-Type: application/json' \
  -d '{"type":"tick","data":{"n":4}}' | cat
echo
sleep 1
kill $CURL_PID 2>/dev/null || true
echo "---- live frames received ----"
cat /tmp/eventrelay-live.txt

echo "==> 5. Validation error (missing type) returns 4xx with error body"
curl -s -o /tmp/eventrelay-err.txt -w "HTTP %{http_code}\n" -X POST "$BASE/streams/$STREAM/events" \
  -H 'Content-Type: application/json' -d '{"data":{}}'
cat /tmp/eventrelay-err.txt
echo

echo "==> Demo finished"
