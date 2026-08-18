#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"

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

assert_result workers \
  "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false" \
  "1"
assert_result bronze_rows \
  "SELECT count(*) FROM lakehouse.bronze.weather_hourly" \
  "4"
assert_result silver_rows \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly" \
  "2"
assert_result reject_rows \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly_rejects" \
  "1"
assert_result duplicate_keys \
  "SELECT count(*) FROM (SELECT location_name, observed_at FROM lakehouse.silver.weather_hourly GROUP BY 1, 2 HAVING count(*) > 1)" \
  "0"
assert_result latest_survivor \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly WHERE location_name = 'moscow' AND observed_at = TIMESTAMP '2026-08-06 00:00:00' AND object_checksum = '0500bb2b50ec417801db2bce49ee65ed3f835ad6271792b3a3a083ecc44c572b' AND temperature_2m = 19.0" \
  "1"
assert_result humidity_reject \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly_rejects WHERE contains(quality_errors, 'humidity_out_of_range')" \
  "1"
assert_result silver_snapshots \
  "SELECT count(*) > 0 FROM lakehouse.silver.\"weather_hourly\$snapshots\"" \
  "true"

printf '{"status":"ready","workers":1,"bronze_rows":4,"silver_rows":2,"reject_rows":1,"duplicate_keys":0}\n'
