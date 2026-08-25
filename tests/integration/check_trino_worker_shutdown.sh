#!/usr/bin/env bash
set -euo pipefail

server="${TRINO_SERVER:-http://trino-coordinator:8080}"
worker="${TRINO_SHUTDOWN_WORKER:-http://trino-worker-2:8080}"
report_path="${TRINO_SHUTDOWN_REPORT_PATH:?TRINO_SHUTDOWN_REPORT_PATH is required}"
operator="lakehouse-operator"

query() {
  trino \
    --server "$server" \
    --user "$operator" \
    --output-format CSV_UNQUOTED \
    --execute "$1" \
    | tr -d '\r'
}

expect() {
  local name="$1"
  local actual="$2"
  local expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    printf '%s: expected %s, got %s\n' "$name" "$expected" "$actual" >&2
    exit 1
  fi
}

nodes_before="$(query "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")"
workers_before="$(query "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active' AND coordinator = false")"
target_before="$(query "SELECT count(*) FROM system.runtime.nodes WHERE node_id = 'lakehouse-worker-2' AND state = 'active' AND coordinator = false")"
state_before="$(curl --fail --silent --show-error -H "X-Trino-User: $operator" "$worker/v1/info/state" | tr -d '"\r\n')"

expect active_nodes_before "$nodes_before" 3
expect active_workers_before "$workers_before" 2
expect target_worker_before "$target_before" 1
expect target_state_before "$state_before" ACTIVE

curl --fail --silent --show-error \
  -X PUT \
  -H 'Content-Type: application/json' \
  -H "X-Trino-User: $operator" \
  --data '"SHUTTING_DOWN"' \
  "$worker/v1/info/state"

state_after_request="$(curl --fail --silent --show-error -H "X-Trino-User: $operator" "$worker/v1/info/state" | tr -d '"\r\n')"
expect target_state_after_request "$state_after_request" SHUTTING_DOWN

workers_after=""
target_after=""
for _ in $(seq 1 30); do
  workers_after="$(query "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active' AND coordinator = false")"
  target_after="$(query "SELECT count(*) FROM system.runtime.nodes WHERE node_id = 'lakehouse-worker-2' AND state = 'active'")"
  if [[ "$workers_after" == "1" && "$target_after" == "0" ]]; then
    break
  fi
  sleep 1
done

expect active_workers_after "$workers_after" 1
expect target_worker_after "$target_after" 0
nodes_after="$(query "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")"
expect active_nodes_after "$nodes_after" 2

silver_rows="$(query "SELECT count(*) FROM lakehouse.silver.weather_hourly")"
metadata_rows="$(query "SELECT count(*) > 0 FROM lakehouse.silver.\"weather_hourly\$snapshots\"")"
expect silver_rows_after_shutdown "$silver_rows" 2
expect metadata_query_after_shutdown "$metadata_rows" true

endpoint_stopped=false
for _ in $(seq 1 20); do
  if ! curl --fail --silent --show-error -H "X-Trino-User: $operator" "$worker/v1/info/state" >/dev/null 2>&1; then
    endpoint_stopped=true
    break
  fi
  sleep 1
done
expect target_endpoint_stopped "$endpoint_stopped" true

cat >"$report_path" <<EOF
{"schema_version":"1.0","status":"succeeded","topology":{"active_nodes_before":$nodes_before,"active_workers_before":$workers_before,"active_nodes_after":$nodes_after,"active_workers_after":$workers_after},"shutdown":{"target_node_id":"lakehouse-worker-2","state_before":"$state_before","state_after_request":"$state_after_request","target_registered_after":false,"endpoint_stopped":true,"grace_period_seconds":5},"continuity":{"silver_rows":$silver_rows,"snapshot_metadata_readable":$metadata_rows}}
EOF

cat "$report_path"
