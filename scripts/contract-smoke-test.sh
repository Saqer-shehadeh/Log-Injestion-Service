#!/usr/bin/env bash
set -euo pipefail

# Contract smoke test.
#
# Brings the full stack up with `docker compose up` and zero extra
# configuration (no .env file, no flags) — exactly the "Default Posture:
# Zero Configuration" the spec grades against — then exercises all four
# required endpoints as an external client would.
#
# No optional features (AUTH_ENABLED, rate limiting, multi-tenancy) are
# implemented in this project, so per the spec ("If you implement no
# optional features, only the first [unauthenticated] configuration
# applies") this is the only configuration that needs to be verified here.
#
# Exits non-zero on the first unexpected status code or missing/malformed
# response field, which fails the CI job.

cd "$(dirname "$0")/.."

COMPOSE="docker compose"
BASE_URL="http://localhost:8080"
HEALTH_TIMEOUT_SECONDS=90

cleanup() {
  echo "--- Tearing down docker compose stack ---"
  $COMPOSE down -v --remove-orphans || true
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

echo "--- docker compose up -d --build (zero config, as graded) ---"
$COMPOSE up -d --build

echo "--- Waiting for GET /health to return 200 (up to ${HEALTH_TIMEOUT_SECONDS}s) ---"
deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
until curl -sf -o /dev/null "$BASE_URL/health"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "FAIL: /health did not become healthy within ${HEALTH_TIMEOUT_SECONDS}s" >&2
    $COMPOSE logs
    exit 1
  fi
  sleep 1
done
echo "OK: GET /health -> 200"

# --- POST /logs: one valid entry + one invalid entry in the same batch ---
echo "--- POST /logs ---"
NOW="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
INGEST_BODY=$(cat <<EOF
{
  "logs": [
    {"timestamp": "$NOW", "level": "error", "service": "smoke-test", "message": "contract smoke test", "attributes": {"region": "eu-west", "retries": 3}},
    {"timestamp": "$NOW", "level": "not-a-real-level", "service": "smoke-test", "message": "should be rejected"}
  ]
}
EOF
)
INGEST_RES=$(curl -s -w '\n%{http_code}' -X POST "$BASE_URL/logs" -H 'content-type: application/json' -d "$INGEST_BODY")
INGEST_STATUS=$(echo "$INGEST_RES" | tail -n1)
INGEST_JSON=$(echo "$INGEST_RES" | sed '$d')

[ "$INGEST_STATUS" = "200" ] || fail "POST /logs expected 200, got $INGEST_STATUS: $INGEST_JSON"
echo "$INGEST_JSON" | grep -q '"accepted":1' || fail "POST /logs expected accepted:1, got: $INGEST_JSON"
echo "$INGEST_JSON" | grep -q '"index":1' || fail "POST /logs expected a rejected entry at index 1, got: $INGEST_JSON"
echo "OK: POST /logs -> 200, 1 accepted, 1 rejected with index+reason"

# --- POST /logs: malformed JSON -> 400 ---
echo "--- POST /logs (malformed JSON) ---"
BAD_JSON_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/logs" -H 'content-type: application/json' -d '{ "logs": [ not valid')
[ "$BAD_JSON_STATUS" = "400" ] || fail "POST /logs with malformed JSON expected 400, got $BAD_JSON_STATUS"
echo "OK: POST /logs malformed JSON -> 400"

# Let the async flush loop COPY the accepted row before querying for it.
# Freshness target is 20s; the flush interval is ~50ms, a few seconds is
# a comfortable margin.
sleep 3

# --- GET /logs ---
echo "--- GET /logs ---"
QUERY_RES=$(curl -s -w '\n%{http_code}' "$BASE_URL/logs?service=smoke-test&limit=5")
QUERY_STATUS=$(echo "$QUERY_RES" | tail -n1)
QUERY_JSON=$(echo "$QUERY_RES" | sed '$d')
[ "$QUERY_STATUS" = "200" ] || fail "GET /logs expected 200, got $QUERY_STATUS: $QUERY_JSON"
echo "$QUERY_JSON" | grep -q '"logs"' || fail "GET /logs response missing 'logs' field: $QUERY_JSON"
echo "$QUERY_JSON" | grep -q '"next_cursor"' || fail "GET /logs response missing 'next_cursor' field: $QUERY_JSON"
echo "OK: GET /logs -> 200 with logs[] and next_cursor"

# --- GET /logs with an invalid parameter -> 400 {"error": "..."} ---
echo "--- GET /logs (invalid level) ---"
BAD_QUERY_RES=$(curl -s -w '\n%{http_code}' "$BASE_URL/logs?level=bogus")
BAD_QUERY_STATUS=$(echo "$BAD_QUERY_RES" | tail -n1)
BAD_QUERY_JSON=$(echo "$BAD_QUERY_RES" | sed '$d')
[ "$BAD_QUERY_STATUS" = "400" ] || fail "GET /logs?level=bogus expected 400, got $BAD_QUERY_STATUS: $BAD_QUERY_JSON"
echo "$BAD_QUERY_JSON" | grep -q '"error"' || fail "GET /logs?level=bogus response missing 'error' field: $BAD_QUERY_JSON"
echo "OK: GET /logs invalid parameter -> 400 {error}"

# --- GET /logs/aggregate ---
echo "--- GET /logs/aggregate ---"
SINCE=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)
UNTIL=$(date -u -d '1 hour' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+1H +%Y-%m-%dT%H:%M:%SZ)
AGG_RES=$(curl -s -w '\n%{http_code}' "$BASE_URL/logs/aggregate?since=$SINCE&until=$UNTIL&bucket=1h&group_by=service")
AGG_STATUS=$(echo "$AGG_RES" | tail -n1)
AGG_JSON=$(echo "$AGG_RES" | sed '$d')
[ "$AGG_STATUS" = "200" ] || fail "GET /logs/aggregate expected 200, got $AGG_STATUS: $AGG_JSON"
echo "$AGG_JSON" | grep -q '"buckets"' || fail "GET /logs/aggregate response missing 'buckets' field: $AGG_JSON"
echo "OK: GET /logs/aggregate -> 200 with buckets[]"

# --- GET /logs/aggregate missing required parameter -> 400 ---
echo "--- GET /logs/aggregate (missing bucket) ---"
BAD_AGG_STATUS=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/logs/aggregate?since=$SINCE&until=$UNTIL")
[ "$BAD_AGG_STATUS" = "400" ] || fail "GET /logs/aggregate without bucket expected 400, got $BAD_AGG_STATUS"
echo "OK: GET /logs/aggregate missing required param -> 400"

echo ""
echo "=== All contract smoke tests passed ==="
