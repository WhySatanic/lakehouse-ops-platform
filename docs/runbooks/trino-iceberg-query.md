# Trino Iceberg query path

The `query` profile runs Trino 483 as one coordinator and two workers. All nodes use a
read-only Iceberg catalog named `lakehouse`, Hive Metastore for table discovery, and
the native Trino S3 filesystem for MinIO warehouse access. The coordinator does not
execute queries, so successful table reads also prove worker participation.

## Prerequisites

Copy `.env.example` to `.env` and complete the bronze and silver paths described in the
[bronze](spark-iceberg-bronze.md) and [silver](spark-iceberg-silver.md) runbooks. The
acceptance check expects their deterministic fixtures: four bronze rows, two accepted
silver rows, and one rejected row.

## Start and inspect the cluster

```bash
docker compose --env-file .env --profile query up -d --wait \
  trino-coordinator trino-worker trino-worker-2
docker compose --env-file .env exec trino-coordinator \
  trino --server http://localhost:8080 --catalog lakehouse --schema silver
```

Useful queries in the Trino shell:

```sql
SELECT node_id, coordinator, state FROM system.runtime.nodes;
SELECT count(*) FROM lakehouse.bronze.weather_hourly;
SELECT count(*) FROM lakehouse.silver.weather_hourly;
SELECT count(*) FROM lakehouse.silver.weather_hourly_rejects;
SELECT committed_at, operation
FROM lakehouse.silver."weather_hourly$snapshots"
ORDER BY committed_at DESC;
```

## Run the acceptance check

Run this after loading the deterministic integration fixtures:

```bash
docker compose --env-file .env --profile query run --rm trino-query-check
```

The check requires two workers, verifies bronze/silver/reject row counts, proves the
deduplication winner and validation error, and reads the Iceberg snapshot metadata
table. It exits non-zero at the first failed post-condition.

## Failure handling

Inspect node and metastore connectivity first:

```bash
docker compose --env-file .env --profile query ps
docker compose --env-file .env --profile query logs \
  trino-coordinator trino-worker trino-worker-2 hive-metastore
```

- An empty `system.runtime.nodes` worker set means the worker has not joined discovery.
- A catalog error usually indicates Hive Metastore is unavailable or the table is not
  registered.
- An S3 error usually indicates MinIO is unavailable or its credentials differ from
  `.env`.

After correcting the dependency, restart only the query profile and repeat the
acceptance check. Table data and metastore state remain in their existing volumes.

## Current limits

This profile is a local query-path proof, not a production deployment. It has two
workers, no TLS or user authentication, and no centralized policy engine. The
coordinator applies file-backed resource groups, and every Trino node loads the shared
deny-by-default file policy described in the
[authorization runbook](trino-authorization.md). Because the HTTP profile trusts the
claimed username, these rules are testable authorization evidence rather than a secure
external boundary. The Iceberg catalog is read-only; Spark remains the table writer.

## Upgrade from 0.6.0

No data or metastore migration is required. Copy `TRINO_HTTP_PORT` from the updated
`.env.example` if port 8080 is unavailable, then start the new opt-in `query` profile.
Existing catalog and warehouse volumes are reused without modification.
