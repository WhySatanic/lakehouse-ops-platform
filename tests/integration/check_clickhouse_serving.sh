#!/usr/bin/env bash
set -euo pipefail

server="${CLICKHOUSE_SERVER:-clickhouse-server}"
user="${CLICKHOUSE_USER:-lakeops-serving}"
password="${CLICKHOUSE_PASSWORD:-serving-development-only}"
bucket="${LAKEHOUSE_BUCKET:-lakehouse}"
access_key="${MINIO_CLICKHOUSE_USER:-lakeops-clickhouse}"
secret_key="${MINIO_CLICKHOUSE_PASSWORD:-clickhouse-development-only}"
report_path="${CLICKHOUSE_SERVING_REPORT_PATH:-/artifacts/clickhouse-serving.json}"
table_url="http://minio:9000/${bucket}/warehouse/silver/weather_hourly"
reject_table_url="http://minio:9000/${bucket}/warehouse/silver/weather_hourly_rejects"

query() {
  clickhouse-client \
    --host "$server" \
    --user "$user" \
    --password "$password" \
    --format TabSeparatedRaw \
    --query "$1" \
    | tr -d '\r'
}

iceberg() {
  local url="$1"
  printf "icebergS3('%s', '%s', '%s')" "$url" "$access_key" "$secret_key"
}

silver_table="$(iceberg "$table_url")"
reject_table="$(iceberg "$reject_table_url")"
version="$(query 'SELECT version()')"
silver_rows="$(query "SELECT count() FROM ${silver_table}")"
reject_rows="$(query "SELECT count() FROM ${reject_table}")"
duplicate_keys="$(query "SELECT count() FROM (SELECT location_name, observed_at FROM ${silver_table} GROUP BY location_name, observed_at HAVING count() > 1)")"
latest_survivor="$(query "SELECT count() FROM ${silver_table} WHERE location_name = 'moscow' AND observed_at = toDateTime('2026-08-06 00:00:00') AND object_checksum = '0500bb2b50ec417801db2bce49ee65ed3f835ad6271792b3a3a083ecc44c572b' AND temperature_2m = 19.0")"

test "$silver_rows" = "2"
test "$reject_rows" = "1"
test "$duplicate_keys" = "0"
test "$latest_survivor" = "1"

printf '{"schema_version":"1.0","status":"ready","engine":"clickhouse","mode":"direct_iceberg_s3","clickhouse_version":"%s","table_url":"%s","silver_rows":%s,"reject_rows":%s,"duplicate_keys":%s,"latest_survivor":%s}\n' \
  "$version" "$table_url" "$silver_rows" "$reject_rows" "$duplicate_keys" "$latest_survivor" \
  | tee "$report_path"
