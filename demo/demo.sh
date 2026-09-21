#!/usr/bin/env bash
# Demo: publish, replay, and live-subscribe against the in-memory event relay.
# Usage: ./demo/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=5080
BASE="http://127.0.0.1:${PORT}"
STREAM="orders"

echo "==> Starting EventRelay.Api on ${BASE}"
ASPNETCORE_URLS="${BASE}" dotnet run --project src/EventRelay.Api --no-launch-profile &
APP_PID=$!
trap 'kill ${APP_PID} 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  curl -sf "${BASE}/" >/dev/null && break
  sleep 0.2
done
echo "==> Service is up"

echo
echo "==> Publishing 3 events to stream '${STREAM}'"
for n in 1 2 3; do
  curl -sf -X POST "${BASE}/streams/${STREAM}/events" \
  -H 'Content-Type: application/json' \
  -d "{\"type\":\"order-created\",\"data\":{\"order\":${n}}}"
  echo
done

echo
echo "==> Replaying history after id=1 (GET /streams/${STREAM}/events?after=1)"
curl -sfN --max-time 2 "${BASE}/streams/${STREAM}/events?after=1" || true

echo
echo "==> Replaying with Last-Event-ID: 2 header"
curl -sfN --max-time 2 -H 'Last-Event-ID: 2' "${BASE}/streams/${STREAM}/events" || true

echo
echo "==> Live subscription (background) + publishing a new event"
curl -sfN --max-time 3 "${BASE}/streams/${STREAM}/events?after=3" > /tmp/eventrelay-live.txt &
LIVE_PID=$!
sleep 0.5
curl -sf -X POST "${BASE}/streams/${STREAM}/events" \
  -H 'Content-Type: application/json' \
  -d '{"type":"order-shipped","data":{"order":1}}'
echo
wait ${LIVE_PID} || true
echo "==> Live subscriber received:"
cat /tmp/eventrelay-live.txt

echo
echo "==> Validation error example (missing 'data')"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST "${BASE}/streams/${STREAM}/events" \
  -H 'Content-Type: application/json' -d '{"type":"broken"}'

echo
echo "==> Demo finished; stopping service"
