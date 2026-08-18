# Iceberg metadata collection

`lakeops collect-iceberg-metadata` collects a bounded table-health snapshot through
Trino. It reads the Iceberg `$snapshots`, `$files`, `$manifests`, and `$partitions`
metadata tables and emits one normalized JSON document. The report is an observation;
it does not change table or warehouse state.

## Prerequisites

Start the query profile and ensure the target table has at least one snapshot:

```bash
docker compose --env-file .env --profile query up -d --wait \
  trino-coordinator trino-worker
```

Follow the [Trino query runbook](trino-iceberg-query.md) if the catalog or worker is not
ready.

## Collect a report

```bash
uv run lakeops collect-iceberg-metadata \
  --server http://localhost:8080 \
  --catalog lakehouse \
  --schema silver \
  --table weather_hourly \
  > weather-hourly-metadata.json
```

The command uses the Trino HTTP client protocol, consumes every result page, and exits
non-zero for transport, query, protocol, or metadata-contract failures. Catalog,
schema, and table identifiers are quoted before they are placed in SQL.

## Report contract

`schema_version` is `1.0`. Consumers should reject unknown major versions.

| Object | Meaning |
|---|---|
| `snapshots` | Snapshot count and current snapshot identity, timestamp, and operation |
| `files` | Current data/delete file count, records, and byte-size distribution |
| `manifests` | Current manifest count, bytes, and added/existing/deleted file entries |
| `partitions` | Current partition count, records, files, and total bytes |

The `table` field is the requested fully qualified name and `collected_at` is the real
UTC observation time. Snapshot IDs remain strings so downstream JSON consumers do not
lose 64-bit integer precision.

## Interpretation

- A growing file count with a stable record count is a small-file signal for the
  planner, not an automatic instruction to compact.
- Manifest growth can increase scan-planning work even when data-file size is healthy.
- Delete files above zero are expected for some merge-on-read tables and need separate
  thresholds from data files.
- Partition totals should reconcile with current file totals. A mismatch is a failed
  observation and must not be used for a maintenance decision.

The collector deliberately reports facts only. The next roadmap increment will turn
these observations into explainable recommendations with explicit thresholds.

## Upgrade from 0.7.0

No table, metastore, or configuration migration is required. The new CLI command uses
the existing `httpx` dependency and the published Trino port. Scripts consuming its
output should pin `schema_version` before relying on individual fields.

## Current limits

Collection is scoped to one table per invocation and reports aggregates for the current
snapshot. It does not retain history, calculate query latency, or execute maintenance.
Authentication follows the current local Trino profile and is not yet an authorization
boundary.
