#!/usr/bin/env sh
# Smoke-test every task REST operation and its documented status codes.
# Start `make run` first. For the forwarding node, pass http://localhost:8081.
# Usage: ./etc/demo.sh [base-url]     default: http://localhost:8080
set -eu

B=${1:-http://localhost:8080}
B=${B%/}
JSON='Content-Type: application/json'
FAIL=0

# expect <status> <method> <path> [curl arguments...]
expect() {
  want=$1
  method=$2
  path=$3
  shift 3

  if ! got=$(curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$B$path" "$@"); then
    printf 'FAIL curl %s %s %s\n' "$method" "$B$path" "$want"
    FAIL=1
    return
  fi
  if [ "$got" = "$want" ]; then
    printf 'ok   %s %-6s %s\n' "$got" "$method" "$path"
  else
    printf 'FAIL %s %-6s %s (expected %s)\n' "$got" "$method" "$path" "$want"
    FAIL=1
  fi
}

echo "Testing $B"

# GET /v1/tasks: 200, 400
expect 200 GET '/v1/tasks?task=alpha&participant=alice&priority=1&limit=2'
expect 400 GET '/v1/tasks?priority=6'

# GET /v1/tasks/{task}: 200, 404
expect 200 GET '/v1/tasks/alpha?consistency=linearizable'
expect 404 GET '/v1/tasks/missing'

# POST /v1/tasks: 200, 400, 412
expect 200 POST '/v1/tasks' -H "$JSON" \
  -d '{"entries":[{"task":"alpha","entryBody":{"name":"Demo","priority":2}}]}'
expect 400 POST '/v1/tasks' -H "$JSON" -d '{bad json'
expect 412 POST '/v1/tasks' -H "$JSON" \
  -d '{"entries":[{"task":"alpha","entryBody":{"name":"Demo","priority":2},"ifVersion":99}]}'

# PATCH /v1/tasks: 200, 400, 412
expect 200 PATCH '/v1/tasks' -H "$JSON" \
  -d '{"ops":[{"task":"alpha","entryMutables":{"incrementVersion":true}}]}'
expect 400 PATCH '/v1/tasks' -H "$JSON" \
  -d '{"ops":[{"task":"alpha","entryMutables":{}}]}'
expect 412 PATCH '/v1/tasks' -H "$JSON" \
  -d '{"ops":[{"task":"alpha","entryMutables":{"incrementVersion":true},"ifVersion":99}]}'

# DELETE /v1/tasks: 200, 400
expect 200 DELETE '/v1/tasks?task=alpha&task=missing'
expect 400 DELETE '/v1/tasks'

# POST /v1/tasks/{task}: 200, 201, 400, 412
expect 200 POST '/v1/tasks/alpha' -H "$JSON" \
  -d '{"entryBody":{"name":"Demo","description":"Example task","participants":["alice"],"priority":2,"completed":false}}'
expect 201 POST '/v1/tasks/demo-new' -H "$JSON" \
  -d '{"entryBody":{"name":"New demo task","priority":2}}'
expect 400 POST '/v1/tasks/alpha' -H "$JSON" \
  -d '{"entryBody":{"name":"Invalid priority","priority":6}}'
expect 412 POST '/v1/tasks/alpha' -H "$JSON" -H 'If-Match: "99"' \
  -d '{"entryBody":{"name":"Stale write","priority":2}}'

# PATCH /v1/tasks/{task}: 200, 400, 412
expect 200 PATCH '/v1/tasks/alpha' -H "$JSON" \
  -d '{"entryMutables":{"incrementVersion":true}}'
expect 400 PATCH '/v1/tasks/alpha' -H "$JSON" -d '{}'
expect 412 PATCH '/v1/tasks/alpha' -H "$JSON" -H 'If-Match: "99"' \
  -d '{"entryMutables":{"incrementVersion":true}}'

# DELETE /v1/tasks/{task}: 204, 400, 412
expect 204 DELETE '/v1/tasks/alpha'
expect 400 DELETE '/v1/tasks/alpha' -H 'If-Match: not-a-version'
expect 412 DELETE '/v1/tasks/alpha' -H 'If-Match: "99"'

if [ "$FAIL" -ne 0 ]; then
  echo "Some checks failed."
  exit 1
fi

echo "All documented status checks passed for $B."
