# PostgreSQL-backed Hive Metastore

## Start

Copy `.env.example` to `.env`, then start the catalog profile:

```bash
docker compose --env-file .env --profile catalog up -d --build --wait \
  metastore-db hive-metastore
```

The PostgreSQL service stays on the internal Compose network. Hive Metastore exposes
its Thrift endpoint on `thrift://localhost:9083` by default. Change
`HIVE_METASTORE_PORT` in `.env` if the host port is occupied.

## Verify schema bootstrap

```bash
docker compose --env-file .env --profile catalog run --rm metastore-schema-check
```

The check connects directly to PostgreSQL and requires the `VERSION`, `DBS`, `TBLS`,
`SDS`, and `SERDES` tables plus a non-empty schema version. A successful result is a
single JSON document with `"status": "ready"`.

## Diagnose

```bash
docker compose --env-file .env --profile catalog ps
docker compose --env-file .env --profile catalog logs hive-metastore
docker compose --env-file .env --profile catalog logs metastore-db
```

The metastore image is built from Apache Hive 4.0.0 and adds pgJDBC 42.7.13 plus the
Hadoop 3.3.6 S3A client with checksum verification. Metadata is retained in the
PostgreSQL volume; table and namespace locations are validated against MinIO before
catalog changes are committed.

Hive 4.0.0 is pinned intentionally. Hive 4.0.1 removed deprecated Thrift methods still
used by Iceberg HiveCatalog, including `get_table`; the incompatibility is tracked in
[Apache Iceberg issue #12878](https://github.com/apache/iceberg/issues/12878).

The S3A client version matches Hive 4.0.0's Hadoop 3.3.6 dependency. MinIO development
credentials are passed only through environment and Hadoop properties; production
deployments must replace them with scoped credentials from a secret manager.

## Stop

```bash
docker compose --env-file .env --profile catalog down
```

Use `--volumes` only for an intentional clean bootstrap. Removing the database volume
deletes metastore metadata and is not part of the normal stop procedure.
