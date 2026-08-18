#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
initial_snapshot_id="${PARTITION_INITIAL_SNAPSHOT_ID:?PARTITION_INITIAL_SNAPSHOT_ID is required}"

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

assert_result current_rows \
  "SELECT format('%s|%s|%s', event_id, format_datetime(event_ts, 'yyyy-MM-dd HH:mm:ss'), payload) FROM lakehouse.ops.partition_evolution_fixture ORDER BY event_id" \
  $'1|2026-08-01 10:00:00|before-a\n2|2026-08-01 11:00:00|before-b\n3|2026-08-02 09:00:00|after-a\n4|2026-08-03 09:00:00|after-b'
assert_result initial_snapshot_rows \
  "SELECT format('%s|%s|%s', event_id, format_datetime(event_ts, 'yyyy-MM-dd HH:mm:ss'), payload) FROM lakehouse.ops.partition_evolution_fixture FOR VERSION AS OF ${initial_snapshot_id} ORDER BY event_id" \
  $'1|2026-08-01 10:00:00|before-a\n2|2026-08-01 11:00:00|before-b'
assert_result manifest_spec_ids \
  "SELECT array_join(array_sort(array_distinct(array_agg(CAST(partition_spec_id AS varchar)))), ',') FROM lakehouse.ops.\"partition_evolution_fixture\$manifests\" WHERE content = 0" \
  "0,1"

printf '{"status":"ready","current_rows":4,"manifest_spec_ids":[0,1],"initial_snapshot_id":%s}\n' \
  "$initial_snapshot_id"
