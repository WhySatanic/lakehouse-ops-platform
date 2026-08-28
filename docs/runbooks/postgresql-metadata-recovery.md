# PostgreSQL metadata recovery

This drill proves that Hive Metastore metadata can be restored after destructive catalog
loss without replacing the PostgreSQL container or changing Iceberg data in MinIO. It is
intended for the repository's isolated local and CI stack. Do not point it at a shared or
production metastore.

## Recovery contract

The exercise performs these phases in order:

1. query the silver fixture through the cache-disabled Trino catalog;
2. capture a deterministic PostgreSQL catalog manifest;
3. stop Hive Metastore and create a custom-format `pg_dump` backup;
4. verify that the backup table of contents includes `VERSION`, `DBS`, `TBLS`, `SDS`, and
   `SERDES`;
5. drop and recreate only the PostgreSQL `public` schema, then require all five core tables
   to be absent;
6. verify the backup checksum immediately before `pg_restore`;
7. restore the database, start Hive Metastore, and compare the catalog manifest;
8. query Trino again and require the same row count and current Iceberg snapshot.

The PostgreSQL and Hive Metastore container IDs are recorded in every phase. The database
container must remain running and unchanged. The recovery path is registered before loss
injection, so a failure during deletion or validation still attempts `pg_restore` and then
starts Hive Metastore.

## Run the drill

Start a populated core stack first. The standard CI ports are shown here:

```bash
docker compose --profile catalog --profile compute --profile query up -d --wait
uv run python tests/integration/exercise_metadata_db_recovery.py \
  http://localhost:8080 \
  artifacts/metadata-db-recovery.json \
  artifacts/metastore-backup.dump
```

Validate the evidence and the restored query path:

```bash
uv run python tests/integration/check_metadata_db_recovery.py \
  artifacts/metadata-db-recovery.json
docker compose --profile catalog run --rm metastore-schema-check
docker compose --profile query run --rm trino-query-check
```

The JSON evidence includes the backup SHA-256 and size, backup TOC count, required-table
coverage, catalog manifest checksum, service topology, Trino query IDs, row counts, current
snapshot IDs, and total recovery duration. The binary dump is deliberately excluded from
Git and must be protected according to the operator's backup policy.

## Failure handling

If the command fails after the backup was accepted, inspect the command output before doing
anything else. The runner has already attempted an idempotent restore and Hive Metastore
restart. Confirm the resulting state with:

```bash
docker compose ps metastore-db hive-metastore
docker compose --profile catalog run --rm metastore-schema-check
docker compose --profile query run --rm trino-query-check
```

If schema validation still fails, retain the dump and its expected SHA-256 from the error
context. Do not rerun Spark writers or initialize a fresh metastore while reconciliation is
incomplete.

## Recovery boundary

This is a logical, single-database restore under a stopped Hive Metastore. It does not cover
point-in-time recovery, WAL archiving, encrypted or off-site backup custody, PostgreSQL
major-version migration, concurrent writers, partial object-store loss, or multi-region
failover. Production recovery still requires independent MinIO protection because the
database backup contains catalog metadata, not Iceberg metadata and data files.

## Upgrade notes

No database schema, warehouse, or Compose migration is required. Version 0.41.0 adds a
host-side recovery runner and CI evidence only. Existing operators can adopt it after
confirming that the target Compose project is an isolated recovery environment.
