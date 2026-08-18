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

The metastore image is built from Apache Hive 4.0.0 and adds pgJDBC 42.7.13 with a
pinned SHA-256. The database and warehouse use named volumes so normal restarts retain
their state.

Hive 4.0.0 is pinned intentionally. Hive 4.0.1 removed deprecated Thrift methods still
used by Iceberg HiveCatalog, including `get_table`; the incompatibility is tracked in
[Apache Iceberg issue #12878](https://github.com/apache/iceberg/issues/12878).

## Stop

```bash
docker compose --env-file .env --profile catalog down
```

Use `--volumes` only for an intentional clean bootstrap. Removing the database volume
deletes metastore metadata and is not part of the normal stop procedure.
