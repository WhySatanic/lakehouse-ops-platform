#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
abandoned_snapshot_id="${RECOVERY_ABANDONED_SNAPSHOT_ID:?RECOVERY_ABANDONED_SNAPSHOT_ID is required}"

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

assert_result restored_current_rows \
  "SELECT count(*) FROM lakehouse.ops.snapshot_recovery_fixture" \
  "1"
assert_result abandoned_snapshot_rows \
  "SELECT count(*) FROM lakehouse.ops.snapshot_recovery_fixture FOR VERSION AS OF ${abandoned_snapshot_id}" \
  "2"
assert_result abandoned_lineage \
  "SELECT count(*) FROM lakehouse.ops.\"snapshot_recovery_fixture\$history\" WHERE snapshot_id = ${abandoned_snapshot_id} AND is_current_ancestor = false" \
  "1"

printf '{"status":"ready","restored_current_rows":1,"abandoned_snapshot_id":%s,"abandoned_snapshot_rows":2}\n' \
  "$abandoned_snapshot_id"
