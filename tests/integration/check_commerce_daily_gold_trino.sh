#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
batch_id="${COMMERCE_BATCH_ID:?COMMERCE_BATCH_ID is required}"
expected_orders="${EXPECTED_COMMERCE_GOLD_ORDERS:-60}"
expected_rejects="${EXPECTED_COMMERCE_GOLD_REJECTS:-7}"
if [[ ! "$batch_id" =~ ^[0-9a-f]{16}$ ]]; then
  printf 'invalid commerce batch ID: expected 16 lowercase hexadecimal characters\n' >&2
  exit 1
fi

query() {
  trino --server "$server" --user lakehouse-ci --output-format CSV_UNQUOTED \
    --execute "$1" | tr -d '\r'
}

actual="$(query "SELECT count(*), sum(order_count), sum(rejected_payment_count) FROM lakehouse.gold.commerce_daily WHERE source_batch_id = '${batch_id}'")"
days="${actual%%,*}"
rest="${actual#*,}"
orders="${rest%%,*}"
rejects="${rest##*,}"
if [[ "$days" -le 0 || "$orders" != "$expected_orders" || "$rejects" != "$expected_rejects" ]]; then
  printf 'commerce gold Trino contract failed: days=%s orders=%s rejects=%s\n' \
    "$days" "$orders" "$rejects" >&2
  exit 1
fi
printf '{"status":"ready","days":%s,"orders":%s,"rejected_payments":%s}\n' \
  "$days" "$orders" "$rejects"
