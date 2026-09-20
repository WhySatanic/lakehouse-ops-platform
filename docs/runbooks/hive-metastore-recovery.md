# Hive Metastore outage recovery

This drill proves a bounded catalog-service recovery path while the PostgreSQL metadata
database and MinIO warehouse remain available. It uses Trino's cache-disabled Iceberg
catalog so a cached table handle cannot hide the outage.

## Preconditions

Start the catalog, compute, and query profiles, initialize MinIO, and create the bronze
and silver fixtures described in the Spark and Trino runbooks. The drill reads
`lakehouse_cache_disabled.silver.weather_hourly` and does not mutate the table.

## Exercise

Run the host-side orchestrator from the repository root:

```bash
uv run python tests/integration/exercise_hive_metastore_recovery.py \
  http://localhost:8080 artifacts/hive-metastore-recovery.json
```

The orchestrator performs these ordered phases:

1. Read the silver row count and current Iceberg snapshot through Trino.
2. Record the Hive Metastore and PostgreSQL container identities.
3. Stop only `hive-metastore` and confirm `metastore-db` remains running.
4. Require the cache-disabled Trino query to fail.
5. Start Hive Metastore with its existing PostgreSQL backend.
6. Read the same table and require an unchanged row count and snapshot ID.

The recovery call is in a `finally` path. If the outage assertion or report generation
fails, the orchestrator still attempts to start Hive Metastore before returning the
error.

Validate the evidence and the database schema independently:

```bash
uv run python tests/integration/check_hive_metastore_recovery.py \
  artifacts/hive-metastore-recovery.json
docker compose --profile catalog run --rm metastore-schema-check
```

## Evidence contract

Schema `1.0` records:

- baseline and recovered Trino query IDs, row counts, and snapshot IDs;
- the expected query failure type and message during the outage;
- Hive Metastore and PostgreSQL container IDs and running states for each phase;
- measured outage duration;
- explicit row, snapshot, and metadata-database identity invariants.

The report is accepted only when PostgreSQL stays running with the same container ID,
the cache-disabled query fails while Hive Metastore is stopped, and the post-recovery
query returns the original rows and snapshot.

## Authenticated Ranger drill

The secure variant runs the same cache-disabled failure injection through the HTTPS
coordinator as `platform_admin`. The client verifies the generated Trino certificate,
uses password authentication, and then reaches Ranger before catalog access. The report
records that exact security boundary while retaining the same service-topology and
Iceberg-state invariants as the default drill.

After the Ranger, catalog, and secure-query profiles are ready:

```bash
docker compose --profile security --profile catalog --profile secure-query \
  cp trino-secure-coordinator:/etc/trino/security/trino.crt \
  artifacts/trino-secure-ca.crt
uv run python tests/integration/exercise_hive_metastore_recovery.py \
  https://localhost:8443 artifacts/trino-authenticated-metastore-recovery.json \
  --mode authenticated-ranger --password "$TRINO_AUTH_PASSWORD" \
  --ca-cert artifacts/trino-secure-ca.crt
uv run python tests/integration/check_hive_metastore_recovery.py \
  artifacts/trino-authenticated-metastore-recovery.json
```

CI retains this report beside the authenticated Ranger matrix and audit export. It does
not infer authenticated recovery from the default HTTP report. The default command and
schema remain compatible; the secure report adds an exact `security` object.

## Recovery boundary

This is a deliberate Hive Metastore process outage, not metadata-database disaster
recovery. It does not delete or corrupt PostgreSQL data, restore a backup, rebuild table
registrations from object storage, or prove an RPO/RTO. PostgreSQL backup and restore is
covered by the separate metadata-database recovery drill.
