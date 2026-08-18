#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
initial_snapshot_id="${SCHEMA_INITIAL_SNAPSHOT_ID:?SCHEMA_INITIAL_SNAPSHOT_ID is required}"
evolved_snapshot_id="${SCHEMA_EVOLVED_SNAPSHOT_ID:?SCHEMA_EVOLVED_SNAPSHOT_ID is required}"

query() {
  trino \
    --server "$server" \
    --user lakehouse-ci \
    --output-format CSV_UNQUOTED \
    --execute "$1" \
    | tr -d '\r'
}

assert_result() {
  local name="$1"
  local sql="$2"
  local expected="$3"
  local actual
  actual="$(query "$sql")"
  if [[ "$actual" != "$expected" ]]; then
    printf '%s: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
}

assert_result current_columns \
  "SELECT array_join(array_agg(column_name ORDER BY ordinal_position), ',') FROM lakehouse.information_schema.columns WHERE table_schema = 'ops' AND table_name = 'schema_evolution_fixture'" \
  "event_id,message,severity"
assert_result current_compatibility \
  "SELECT count(*) FROM lakehouse.ops.schema_evolution_fixture WHERE (event_id = 1 AND message = 'stable' AND severity IS NULL) OR (event_id = 2 AND message = 'regression' AND severity = 'warning')" \
  "2"
assert_result initial_snapshot_schema \
  "SELECT format('%s|%s', event_id, payload) FROM lakehouse.ops.schema_evolution_fixture FOR VERSION AS OF ${initial_snapshot_id} ORDER BY event_id" \
  "1|stable"
assert_result evolved_snapshot_schema \
  "SELECT format('%s|%s|%s', event_id, payload, coalesce(severity, 'NULL')) FROM lakehouse.ops.schema_evolution_fixture FOR VERSION AS OF ${evolved_snapshot_id} ORDER BY event_id" \
  $'1|stable|NULL\n2|regression|warning'

printf '{"status":"ready","current_columns":"event_id,message,severity","current_rows":2,"initial_snapshot_id":%s,"evolved_snapshot_id":%s}\n' \
  "$initial_snapshot_id" "$evolved_snapshot_id"
