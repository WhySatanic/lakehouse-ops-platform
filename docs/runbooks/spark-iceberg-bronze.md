# Spark to Iceberg bronze pipeline

## Scope

The opt-in `compute` profile converts immutable Open-Meteo landing documents into the
typed `lakehouse.bronze.weather_hourly` Iceberg table. Spark registers the table in Hive
Metastore and stores Iceberg metadata and Parquet data under `s3a://lakehouse/warehouse`.
The metastore receives a dedicated S3A configuration because Hive validates namespace
and table locations before committing catalog records. Spark still reads and writes
Iceberg objects through S3FileIO against the same MinIO bucket.

The first implementation deliberately runs Spark in local mode. The catalog and object
store boundaries are the same ones a distributed Spark deployment would use.

## Start the platform

Create `.env` from `.env.example`, then start MinIO, PostgreSQL, and Hive Metastore:

```bash
docker compose --env-file .env --profile catalog --profile compute build hive-metastore spark-bronze
docker compose --env-file .env --profile catalog --profile compute up -d --wait minio metastore-db hive-metastore
docker compose --env-file .env run --rm minio-init
```

Land at least one payload through the S3 adapter:

```bash
uv run --env-file .env lakeops ingest-weather \
  --name moscow \
  --latitude 55.7558 \
  --longitude 37.6173 \
  --forecast-days 1 \
  --backend s3
```

For an external-API-independent smoke test, use the canonical checked-in fixture:

```bash
docker compose --env-file .env --profile compute run --rm landing-fixture
```

## Write bronze

Sync immutable landing objects into the bounded Spark input volume and submit the job:

```bash
docker compose --env-file .env --profile compute run --rm bronze-input-sync
docker compose --env-file .env --profile catalog --profile compute run --rm spark-bronze
```

The final line is a JSON report. A successful first fixture run reports two source rows,
two rows after the merge, and one or more snapshots. Repeating the Spark command reports
zero inserted rows because the merge key is `(object_checksum, observed_at)`.

## Verify storage and catalog state

Run both post-condition checks rather than relying only on Spark's exit code:

```bash
docker compose --env-file .env --profile catalog run --rm metastore-schema-check
docker compose --env-file .env --profile catalog --profile compute run --rm bronze-catalog-check
docker compose --env-file .env --profile compute run --rm bronze-storage-check
```

The catalog check requires one `bronze.weather_hourly` table whose location is inside the
configured S3A warehouse. The storage check requires both Iceberg metadata JSON and
Parquet data files in MinIO.

## Upgrade from 0.4.0

Back up PostgreSQL before replacing the catalog image, and do not remove the named
volumes. Version 0.5.0 pins Hive Metastore from 4.0.1 to 4.0.0 for Iceberg HiveCatalog
compatibility and adds the S3A client required to validate MinIO table locations.

After updating the checkout, rebuild `hive-metastore` and `spark-bronze`, start the core
services, and run the metastore schema check before submitting Spark. Existing landing
objects are unchanged; the new bronze table is created only when the compute profile is
run. Reusing a catalog already modified outside the documented 0.4.0 path has not been
tested, so retain the database backup until the schema and table checks both pass.

## Failure handling

1. If input sync reports no objects, verify that the landing prefix is
   `s3://lakehouse/landing` and rerun the S3 readiness check.
2. If Spark cannot resolve the catalog, check Hive Metastore health and the Thrift port.
3. If Spark receives S3 access errors, verify MinIO credentials, endpoint, and bucket.
4. If Spark exits after writing files but before committing, do not manually register
   them. Preserve the logs and leave orphan cleanup to a later reconciler milestone.

Collect evidence with:

```bash
docker compose --profile catalog --profile compute ps -a
docker compose --profile catalog --profile compute logs minio hive-metastore metastore-db
```

## Stop

```bash
docker compose --profile catalog --profile compute down
```

Named volumes preserve MinIO and metastore state. Add `--volumes` only when the local
catalog and object data are intentionally being discarded.
