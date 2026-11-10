#!/bin/sh
# Build graftd, start it on a fresh database, run the SDK e2e tests against it,
# then check that a run survives a restart of the service. Stops everything at the end.
# For machines without Docker; CI runs the same tests against `docker compose up`.
#
# Needs GRAFT_TEST_DATABASE_URL (postgres://user@host:port/db, no query string) and psql.
set -eu
cd "$(dirname "$0")/.."
PORT="${PORT:-18080}"
TOKEN=local-e2e-token-0000
BIN="$(mktemp -d)/graftd"
PY="${PY:-sdk/python/.venv/bin/python}"
[ -x "$PY" ] || PY=python3
ADMIN="${GRAFT_TEST_DATABASE_URL:?set GRAFT_TEST_DATABASE_URL (make pg-dev)}"
NAME="graft_e2e_$$"
DBURL="${ADMIN%/*}/$NAME"

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $PORT is already in use; the tests would hit the wrong server. Stop it or set PORT." >&2
  exit 1
fi
psql "$ADMIN" -qc "CREATE DATABASE $NAME"
PID=""
cleanup() {
  [ -n "$PID" ] && { kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null || true; }
  psql "$ADMIN" -qc "DROP DATABASE IF EXISTS $NAME WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT

(cd service && go build -o "$BIN" ./cmd/graftd)
start() {
  GRAFT_TOKEN=$TOKEN GRAFT_DATABASE_URL="$DBURL" GRAFT_ADDR="127.0.0.1:$PORT" "$BIN" >>"$BIN.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 25); do
    GRAFT_ADDR="127.0.0.1:$PORT" "$BIN" -healthcheck >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "graftd did not become healthy; log:" >&2; cat "$BIN.log" >&2; return 1
}
start

(cd sdk/python && GRAFT_E2E=1 GRAFT_URL="http://127.0.0.1:$PORT" GRAFT_TOKEN=$TOKEN "../../$PY" -m pytest tests/e2e tests/chaos -v -s "$@")

echo "--- restart check: a finished run must still be there after graftd restarts"
api() { curl -sf -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"; }
field() { "$PY" -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }
BR=$(api -X POST "http://127.0.0.1:$PORT/v1/projects/demo/branches" -d '{"name":"restart-check"}' | field id)
RUN=$(api -X POST "http://127.0.0.1:$PORT/v1/branches/$BR/runs" -d '{"steps":[{"id":"a"}]}' | field id)
for _ in $(seq 1 50); do
  S=$(api "http://127.0.0.1:$PORT/v1/runs/$RUN" | field status)
  [ "$S" = succeeded ] && break
  sleep 0.1
done
echo "before restart: $RUN $S (graftd pid $PID)"
kill "$PID"; wait "$PID" 2>/dev/null || true
start
AFTER=$(api "http://127.0.0.1:$PORT/v1/runs/$RUN" | field status)
echo "after restart:  $RUN $AFTER (graftd pid $PID)"
[ "$S" = succeeded ] && [ "$AFTER" = succeeded ] || { echo "run did not survive the restart" >&2; exit 1; }
