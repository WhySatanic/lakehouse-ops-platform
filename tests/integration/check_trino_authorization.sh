#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
report_path="${TRINO_AUTHORIZATION_REPORT_PATH:?TRINO_AUTHORIZATION_REPORT_PATH is required}"
authorization_mode="${TRINO_AUTHORIZATION_MODE:-file}"
authentication_enforced="${TRINO_AUTHENTICATION_ENFORCED:-false}"
authentication_password="${TRINO_AUTH_PASSWORD:-}"
truststore_path="${TRINO_TRUSTSTORE_PATH:-}"
truststore_password="${TRINO_TRUSTSTORE_PASSWORD:-}"
ca_cert_path="${TRINO_CA_CERT_PATH:-}"
expected_node_count="${TRINO_EXPECTED_NODE_COUNT:-3}"
expected_silver_rows=2
expected_checksum_count=2
if [[ "$authorization_mode" == "ranger" ]]; then
  expected_silver_rows=1
  expected_checksum_count=0
fi

query() {
  local user="$1"
  local sql="$2"
  local -a command=(
    trino --server "$server" --user "$user" --output-format CSV_UNQUOTED
  )
  if [[ "$authentication_enforced" == "true" ]]; then
    : "${authentication_password:?TRINO_AUTH_PASSWORD is required}"
    : "${truststore_path:?TRINO_TRUSTSTORE_PATH is required}"
    : "${truststore_password:?TRINO_TRUSTSTORE_PASSWORD is required}"
    command+=(--password --truststore-path "$truststore_path" --truststore-type PKCS12)
    command+=(--truststore-password "$truststore_password")
    TRINO_PASSWORD="$authentication_password" "${command[@]}" --execute "$sql" 2>&1 \
      | tr -d '\r'
    return
  fi
  "${command[@]}" --execute "$sql" 2>&1 | tr -d '\r'
}

if [[ "$authentication_enforced" == "true" ]]; then
  : "${ca_cert_path:?TRINO_CA_CERT_PATH is required}"
  anonymous_status="$(curl --silent --show-error --output /dev/null \
    --write-out '%{http_code}' --cacert "$ca_cert_path" "$server/v1/info")"
  if [[ "$anonymous_status" != "401" ]]; then
    printf 'anonymous request returned HTTP %s, expected 401\n' "$anonymous_status" >&2
    exit 1
  fi

  if TRINO_PASSWORD=incorrect-password trino --server "$server" --user platform_admin \
    --password --truststore-path "$truststore_path" \
    --truststore-type PKCS12 \
    --truststore-password "$truststore_password" --execute "SELECT 1" >/dev/null 2>&1; then
    printf 'incorrect password unexpectedly authenticated\n' >&2
    exit 1
  fi
fi

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
expect_allowed analytics_engineer_silver_row_visibility analytics_engineer \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly" "$expected_silver_rows"
expect_allowed analytics_engineer_checksum_visibility analytics_engineer \
  "SELECT count(object_checksum) FROM lakehouse.silver.weather_hourly" "$expected_checksum_count"
expect_allowed platform_admin_checksum_is_visible platform_admin \
  "SELECT count(object_checksum) FROM lakehouse.silver.weather_hourly" 2
expect_allowed operator_reads_system lakehouse-operator \
  "SELECT count(*) FROM system.runtime.nodes" "$expected_node_count"

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

analytics_visible_rows="$(query analytics_engineer \
  "SELECT count(*) FROM lakehouse.silver.weather_hourly")"
analytics_visible_checksums="$(query analytics_engineer \
  "SELECT count(object_checksum) FROM lakehouse.silver.weather_hourly")"
admin_visible_checksums="$(query platform_admin \
  "SELECT count(object_checksum) FROM lakehouse.silver.weather_hourly")"

cat >"$report_path" <<EOF
{
  "schema_version": "1.0",
  "status": "succeeded",
  "policy": {
    "engine": "trino",
    "mode": "$authorization_mode",
    "default": "deny",
    "authentication_enforced": $authentication_enforced
  },
  "authentication": {
    "anonymous_request": "$(if [[ "$authentication_enforced" == "true" ]]; then printf denied; else printf not_tested; fi)",
    "incorrect_password": "$(if [[ "$authentication_enforced" == "true" ]]; then printf denied; else printf not_tested; fi)"
  },
  "transformations": {
    "analytics_engineer_visible_rows": $analytics_visible_rows,
    "analytics_engineer_visible_checksums": $analytics_visible_checksums,
    "platform_admin_visible_checksums": $admin_visible_checksums
  },
  "cases": [
    {"id": "platform_admin_reads_bronze", "user": "platform_admin", "expectation": "allow", "result": "allowed"},
    {"id": "data_engineer_reads_bronze", "user": "data_engineer", "expectation": "allow", "result": "allowed"},
    {"id": "analytics_engineer_silver_row_visibility", "user": "analytics_engineer", "expectation": "allow", "result": "allowed"},
    {"id": "analytics_engineer_checksum_visibility", "user": "analytics_engineer", "expectation": "allow", "result": "allowed"},
    {"id": "platform_admin_checksum_is_visible", "user": "platform_admin", "expectation": "allow", "result": "allowed"},
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
