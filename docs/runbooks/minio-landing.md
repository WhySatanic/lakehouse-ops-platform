# MinIO landing zone

## Start

Create a local environment file once:

```powershell
Copy-Item .env.example .env
```

Start MinIO and wait for its health endpoint, then create the private lakehouse bucket:

```bash
docker compose --env-file .env up -d --wait minio
docker compose --env-file .env run --rm minio-init
```

MinIO exposes its S3 endpoint on `http://localhost:9000` and its console on
`http://localhost:9001`. If either port is already in use, change `MINIO_API_PORT`,
`MINIO_CONSOLE_PORT`, and `LAKEOPS_S3_ENDPOINT_URL` in `.env` before starting the stack.

## Smoke test

Verify credentials, bucket access, and the recovery-oriented versioning invariant before
writing data:

```bash
uv run --env-file .env lakeops doctor --backend s3 --require-versioning
```

The report must contain `"status": "ready"`. A failed check exits with status code 1,
which makes the command suitable for deployment and scheduled-job preflight checks.

Land a real forecast through the S3 adapter:

```bash
uv run --env-file .env lakeops ingest-weather \
  --name moscow \
  --latitude 55.7558 \
  --longitude 37.6173 \
  --forecast-days 1 \
  --backend s3
```

Run the same command twice. The first response must contain `"created": true`; the
second must return the same checksum with `"created": false`.

Audit every retained JSON version before a recovery and inventory delete markers:

```bash
uv run --env-file .env lakeops audit-landing --backend s3 --include-versions
```

The command reads version history with the scoped ingestion identity. It fails when the
history is empty or any retained JSON version has an invalid payload, path, or metadata
checksum. Delete markers are reported separately because they are recovery evidence,
not payload corruption. Run the regular `audit-landing --backend s3` command as well
when the current visible landing state must be verified.

## Diagnose

```bash
docker compose ps
docker compose logs minio
docker compose exec minio sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls --recursive local/lakehouse'
```

The bootstrap command is safe to repeat because bucket creation uses
`mc mb --ignore-existing`. It enables versioning on both new and existing buckets and
reconciles the development identities and their versioned policies from `config/s3`:

| Identity | Allowed | Boundary |
| --- | --- | --- |
| ingestion | read and write `landing/*` | no warehouse access |
| Spark | read `landing/*`, manage `warehouse/*` | no landing writes |
| Trino | read `warehouse/*` | no landing access or writes |
| Hive Metastore | manage namespace paths in `warehouse/*` | no landing access |

Run `docker compose --env-file .env run --rm minio-access-check` to verify permitted and
denied operations. The data checks authenticate as the scoped identities, not as root.
The landing fixtures authenticate as ingestion, Spark jobs authenticate as Spark, and
Trino nodes authenticate as Trino. Hive Metastore authenticates as its warehouse-only
identity because namespace creation validates and creates external warehouse paths.

## Upgrade from 0.33

Add `MINIO_HMS_USER` and `MINIO_HMS_PASSWORD` from `.env.example`, run `minio-init`,
then run `minio-access-check`. Recreate Hive Metastore so it receives the dedicated
identity. Existing bucket and catalog data are unchanged. Roll back by restoring the
0.33 Compose file and recreating Hive Metastore with the previous bootstrap credentials.

## Upgrade from 1.1.0

Run `minio-init` once to enable versioning and grant the ingestion identity permission to
read the versioning state. Existing objects remain current objects and are not duplicated.
Verify the change with `lakeops doctor --backend s3 --require-versioning`. Suspending
versioning is the rollback, but it reduces recovery options and makes the required check
fail by design.

## Upgrade from 1.2.0

Version-history audit requires `s3:ListBucketVersions` and `s3:GetObjectVersion` on the
existing landing boundary. Rerun `minio-init` to apply those scoped permissions. No
object migration is required. Roll back by restoring the previous ingestion policy; the
current-object audit and ingestion path remain available, but history inspection stops.

## Stop

```bash
docker compose down
```

The named volume is preserved. Use `docker compose down --volumes` only when local
object data is intentionally being discarded.
