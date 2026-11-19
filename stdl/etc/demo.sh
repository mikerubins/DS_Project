#!/usr/bin/env sh
# Build both nodes, run every documented status check against each in parallel,
# then stop only the server processes started by this script.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

LOG_DIR="${TMPDIR:-/tmp}/stdl-demo-$$"
mkdir "$LOG_DIR"
N1_LOG="$LOG_DIR/n1.log"
N2_LOG="$LOG_DIR/n2.log"
TEST_8080_LOG="$LOG_DIR/http-8080.log"
TEST_8081_LOG="$LOG_DIR/http-8081.log"
P1=
P2=
T1=
T2=

stop_process() {
  process_id=$1
  if [ -n "$process_id" ]; then
    kill "$process_id" 2>/dev/null || true
    wait "$process_id" 2>/dev/null || true
  fi
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  stop_process "$T2"
  stop_process "$T1"
  stop_process "$P2"
  stop_process "$P1"
  if [ "$status" -eq 0 ]; then
    rm -f "$N1_LOG" "$N2_LOG" "$TEST_8080_LOG" "$TEST_8081_LOG"
    rmdir "$LOG_DIR" 2>/dev/null || true
  else
    echo "Demo logs kept in $LOG_DIR"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_server() {
  name=$1
  process_id=$2
  url=$3
  log_file=$4

  attempt=0
  while [ "$attempt" -lt 50 ]; do
    if ! kill -0 "$process_id" 2>/dev/null; then
      echo "$name exited before becoming ready:"
      cat "$log_file"
      return 1
    fi
    if curl -fsS -o /dev/null "$url/openapi.yaml"; then
      echo "$name ready at $url"
      return 0
    fi
    sleep 0.2
    attempt=$((attempt + 1))
  done

  echo "Timed out waiting for $name at $url:"
  cat "$log_file"
  return 1
}

echo "Building the server..."
make -f Makefile build

echo "Starting n1 on :8080 and n2 on :8081 (forwarding to n1)..."
./bin/server -id n1 -http :8080 -grpc :50051 >"$N1_LOG" 2>&1 &
P1=$!
wait_for_server n1 "$P1" http://localhost:8080 "$N1_LOG"

./bin/server -id n2 -http :8081 -grpc :50052 -target localhost:50051 >"$N2_LOG" 2>&1 &
P2=$!
wait_for_server n2 "$P2" http://localhost:8081 "$N2_LOG"

JSON='Content-Type: application/json'

# expect <base-url> <route-label> <status> <method> <path> [curl arguments...]
expect() {
  base=$1
  route=$2
  want=$3
  method=$4
  path=$5
  shift 5

  if ! got=$(curl -sS -o /dev/null -w '%{http_code}' -X "$method" "$base$path" "$@"); then
    printf 'FAIL curl %s %s %s\n' "$method" "$base$path" "$want"
    FAIL=1
    return
  fi
  if [ "$got" = "$want" ]; then
    printf 'ok   %s [%s] %-6s %s\n' "$got" "$route" "$method" "$path"
  else
    printf 'FAIL %s [%s] %-6s %s (expected status %s)\n' "$got" "$route" "$method" "$path" "$want"
    FAIL=1
  fi
}

run_suite() {
  B=$1
  REST_NODE=$2
  GRPC_NODE=$3
  ROUTE="HTTP $REST_NODE"
  FAIL=0
  echo "Testing $B (response from $ROUTE; RPC target $GRPC_NODE)"

  # GET /v1/tasks: 200, 400
  expect "$B" "$ROUTE" 200 GET '/v1/tasks?task=alpha&participant=alice&priority=1&limit=2'
  expect "$B" "$ROUTE" 400 GET '/v1/tasks?priority=6'

  # GET /v1/tasks/{task}: 200, 404
  expect "$B" "$ROUTE" 200 GET '/v1/tasks/alpha?consistency=linearizable'
  expect "$B" "$ROUTE" 404 GET '/v1/tasks/missing'

  # POST /v1/tasks: 200, 400, 412
  expect "$B" "$ROUTE" 200 POST '/v1/tasks' -H "$JSON" \
    -d '{"entries":[{"task":"alpha","entryBody":{"name":"Demo","priority":2}}]}'
  expect "$B" "$ROUTE" 400 POST '/v1/tasks' -H "$JSON" -d '{bad json'
  expect "$B" "$ROUTE" 412 POST '/v1/tasks' -H "$JSON" \
    -d '{"entries":[{"task":"alpha","entryBody":{"name":"Demo","priority":2},"ifVersion":99}]}'

  # PATCH /v1/tasks: 200, 400, 412
  expect "$B" "$ROUTE" 200 PATCH '/v1/tasks' -H "$JSON" \
    -d '{"ops":[{"task":"alpha","entryMutables":{"incrementVersion":true}}]}'
  expect "$B" "$ROUTE" 400 PATCH '/v1/tasks' -H "$JSON" \
    -d '{"ops":[{"task":"alpha","entryMutables":{}}]}'
  expect "$B" "$ROUTE" 412 PATCH '/v1/tasks' -H "$JSON" \
    -d '{"ops":[{"task":"alpha","entryMutables":{"incrementVersion":true},"ifVersion":99}]}'

  # DELETE /v1/tasks: 200, 400
  expect "$B" "$ROUTE" 200 DELETE '/v1/tasks?task=alpha&task=missing'
  expect "$B" "$ROUTE" 400 DELETE '/v1/tasks'

  # POST /v1/tasks/{task}: 200, 201, 400, 412
  expect "$B" "$ROUTE" 200 POST '/v1/tasks/alpha' -H "$JSON" \
    -d '{"entryBody":{"name":"Demo","description":"Example task","participants":["alice"],"priority":2,"completed":false}}'
  expect "$B" "$ROUTE" 201 POST '/v1/tasks/demo-new' -H "$JSON" \
    -d '{"entryBody":{"name":"New demo task","priority":2}}'
  expect "$B" "$ROUTE" 400 POST '/v1/tasks/alpha' -H "$JSON" \
    -d '{"entryBody":{"name":"Invalid priority","priority":6}}'
  expect "$B" "$ROUTE" 412 POST '/v1/tasks/alpha' -H "$JSON" -H 'If-Match: "99"' \
    -d '{"entryBody":{"name":"Stale write","priority":2}}'

  # PATCH /v1/tasks/{task}: 200, 400, 412
  expect "$B" "$ROUTE" 200 PATCH '/v1/tasks/alpha' -H "$JSON" \
    -d '{"entryMutables":{"incrementVersion":true}}'
  expect "$B" "$ROUTE" 400 PATCH '/v1/tasks/alpha' -H "$JSON" -d '{}'
  expect "$B" "$ROUTE" 412 PATCH '/v1/tasks/alpha' -H "$JSON" -H 'If-Match: "99"' \
    -d '{"entryMutables":{"incrementVersion":true}}'

  # DELETE /v1/tasks/{task}: 204, 400, 412
  expect "$B" "$ROUTE" 204 DELETE '/v1/tasks/alpha'
  expect "$B" "$ROUTE" 400 DELETE '/v1/tasks/alpha' -H 'If-Match: not-a-version'
  expect "$B" "$ROUTE" 412 DELETE '/v1/tasks/alpha' -H 'If-Match: "99"'

  if [ "$FAIL" -ne 0 ]; then
    echo "Some checks failed for $B."
    return 1
  fi
  printf 'Sample dummy response (HTTP %s, gRPC reply from %s):\n' "$REST_NODE" "$GRPC_NODE"
  curl -sS -w '\n' "$B/v1/tasks/alpha"
  echo "All documented status checks passed for $B."
}

echo "Running the same checks against both nodes in parallel..."
run_suite http://localhost:8080 n1 n1 >"$TEST_8080_LOG" 2>&1 &
T1=$!
run_suite http://localhost:8081 n2 n1 >"$TEST_8081_LOG" 2>&1 &
T2=$!

FAIL=0
if ! wait "$T1"; then
  FAIL=1
fi
T1=
if ! wait "$T2"; then
  FAIL=1
fi
T2=

cat "$TEST_8080_LOG"
cat "$TEST_8081_LOG"
if [ "$FAIL" -ne 0 ]; then
  echo "Some checks failed; server logs are in $LOG_DIR."
  exit 1
fi

echo "All checks passed on :8080 and :8081."
