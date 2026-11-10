#!/bin/sh
# A throwaway local Postgres for tests: TCP only, trust auth, data under $TMPDIR.
#   scripts/pg-dev.sh start   -> prints the GRAFT_TEST_DATABASE_URL to export
#   scripts/pg-dev.sh stop
# Needs initdb/pg_ctl on PATH (e.g. brew install postgresql@17). Never points at your own databases.
set -eu
PORT="${PGPORT_DEV:-56543}"
DIR="${PGDATA_DEV:-${TMPDIR:-/tmp}/graft-pg-dev}"
export LC_ALL="${LC_ALL:-en_US.UTF-8}"   # macOS: postgres refuses to start without a locale
case "${1:-start}" in
  start)
    [ -f "$DIR/PG_VERSION" ] || initdb -D "$DIR" -U graft --auth=trust -E UTF8 >/dev/null
    if ! pg_ctl -D "$DIR" status >/dev/null 2>&1; then
      pg_ctl -D "$DIR" -l "$DIR/log.txt" -w start \
        -o "-p $PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories= -c max_connections=200" >/dev/null
    fi
    echo "export GRAFT_TEST_DATABASE_URL=postgres://graft@127.0.0.1:$PORT/postgres"
    ;;
  stop) pg_ctl -D "$DIR" -w stop >/dev/null && echo stopped ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
