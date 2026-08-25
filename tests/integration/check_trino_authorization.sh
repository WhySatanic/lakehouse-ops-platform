#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
report_path="${TRINO_AUTHORIZATION_REPORT_PATH:?TRINO_AUTHORIZATION_REPORT_PATH is required}"

query() {
  local user="$1"
  local sql="$2"
  trino \
    --server "$server" \
    --user "$user" \
    --output-format CSV_UNQUOTED \
    --execute "$sql" \
    2>&1 \
    | tr -d '\r'
}

expect_allowed() {
  local case_id="$1"
  local user="$2"
  local sql="$3"
  local expected="$4"
  local actual
  if ! actual="$(query "$user" "$sql")"; then
    printf '%s: expected access for %s, query failed: %s\n' "$case_id" "$user" "$actual" >&2
    exit 1
  fi
  if [[ "$actual" != "$expected" ]]; then
    printf '%s: expected %s, got %s\n' "$case_id" "$expected" "$actual" >&2
    exit 1
  fi
}

expect_denied() {
  local case_id="$1"
  local user="$2"
  local sql="$3"
  local actual
  if actual="$(query "$user" "$sql")"; then
    printf '%s: expected access denial for %s, query returned: %s\n' "$case_id" "$user" "$actual" >&2
    exit 1
  fi
  if [[ "$actual" != *"Access Denied"* ]]; then
    printf '%s: expected an access-control failure, got: %s\n' "$case_id" "$actual" >&2
    exit 1
  fi
}

expect_allowed platform_admin_reads_bronze platform_admin \
  "SELECT count(*) FROM lakehouse.bronze.weather_hourly" 4
expect_allowed data_engineer_reads_bronze data_engineer \
  "SELECT count(*) FROM lakehouse.bronze.weather_hourly" 4
expect_allowed analytics_engineer_reads_silver analytics_engineer \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly" 2
expect_allowed operator_reads_system lakehouse-operator \
  "SELECT count(*) FROM system.runtime.nodes" 3

expect_denied analytics_engineer_cannot_read_bronze analytics_engineer \
  "SELECT count(*) FROM lakehouse.bronze.weather_hourly"
expect_denied analyst_cannot_read_silver analyst \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly"
expect_denied service_ingest_cannot_read_silver service_ingest \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly"
expect_denied unknown_user_cannot_read_lakehouse untrusted_user \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly"
expect_denied unknown_user_cannot_read_system untrusted_user \
  "SELECT count(*) FROM system.runtime.nodes"
expect_denied data_engineer_cannot_create_schema data_engineer \
  "CREATE SCHEMA lakehouse.authorization_test"

cat >"$report_path" <<'EOF'
{
  "schema_version": "1.0",
  "status": "succeeded",
  "policy": {
    "engine": "trino",
    "mode": "file",
    "default": "deny",
    "authentication_enforced": false
  },
  "cases": [
    {"id": "platform_admin_reads_bronze", "user": "platform_admin", "expectation": "allow", "result": "allowed"},
    {"id": "data_engineer_reads_bronze", "user": "data_engineer", "expectation": "allow", "result": "allowed"},
    {"id": "analytics_engineer_reads_silver", "user": "analytics_engineer", "expectation": "allow", "result": "allowed"},
    {"id": "operator_reads_system", "user": "lakehouse-operator", "expectation": "allow", "result": "allowed"},
    {"id": "analytics_engineer_cannot_read_bronze", "user": "analytics_engineer", "expectation": "deny", "result": "denied"},
    {"id": "analyst_cannot_read_silver", "user": "analyst", "expectation": "deny", "result": "denied"},
    {"id": "service_ingest_cannot_read_silver", "user": "service_ingest", "expectation": "deny", "result": "denied"},
    {"id": "unknown_user_cannot_read_lakehouse", "user": "untrusted_user", "expectation": "deny", "result": "denied"},
    {"id": "unknown_user_cannot_read_system", "user": "untrusted_user", "expectation": "deny", "result": "denied"},
    {"id": "data_engineer_cannot_create_schema", "user": "data_engineer", "expectation": "deny", "result": "denied"}
  ]
}
EOF

cat "$report_path"
