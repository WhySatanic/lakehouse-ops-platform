#!/bin/sh
set -eu

batch_id="${COMMERCE_BATCH_ID:-}"
source="local/${LAKEHOUSE_BUCKET}/landing"
target=/input
scope=all_landing
case "${COMMERCE_SYNC_SCOPE:-all}" in
all) ;;
selected)
  test "${#batch_id}" -eq 16
  case "$batch_id" in *[!0-9a-f]*) exit 2 ;; esac
  suffix="source=commerce/batch_id=${batch_id}"
  source="${source}/${suffix}"
  target="${target}/${suffix}"
  scope=selected_commerce_batch
  ;;
*) exit 2 ;;
esac

mc alias set local http://minio:9000 "${MINIO_SPARK_USER}" "${MINIO_SPARK_PASSWORD}"
object_count="$(mc find "$source" --name '*.json' | wc -l | tr -d ' ')"
test "$object_count" -gt 0
mkdir -p "$target"
mc mirror --overwrite "$source" "$target"
printf '{"status":"ready","landing_objects":%s,"scope":"%s"}\n' "$object_count" "$scope"
