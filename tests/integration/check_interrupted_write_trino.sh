#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
expected_snapshot_id="${INTERRUPTED_WRITE_SNAPSHOT_ID:?INTERRUPTED_WRITE_SNAPSHOT_ID is required}"
expected_file_count="${INTERRUPTED_WRITE_FILE_COUNT:?INTERRUPTED_WRITE_FILE_COUNT is required}"

query() {
  trino \
    --server "$server" \
    --user lakehouse-recovery-drill \
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

assert_result current_snapshot \
  'SELECT snapshot_id FROM lakehouse.ops."interrupted_write_fixture$refs" WHERE name = '\''main'\''' \
  "$expected_snapshot_id"
assert_result committed_rows \
  "SELECT format('%s|%s', event_id, payload) FROM lakehouse.ops.interrupted_write_fixture ORDER BY event_id" \
  $'1|committed-a\n2|committed-b\n3|committed-c'
assert_result referenced_files \
  'SELECT CAST(count(*) AS varchar) FROM lakehouse.ops."interrupted_write_fixture$files" WHERE content = 0' \
  "$expected_file_count"

printf '{"status":"ready","snapshot_id":%s,"rows":3,"referenced_files":%s}\n' \
  "$expected_snapshot_id" "$expected_file_count"
