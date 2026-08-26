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

Verify credentials and bucket access before writing data:

```bash
uv run --env-file .env lakeops doctor --backend s3
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

## Diagnose

```bash
docker compose ps
docker compose logs minio
docker compose exec minio sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls --recursive local/lakehouse'
```

The bootstrap command is safe to repeat because bucket creation uses
`mc mb --ignore-existing`. It also reconciles three development identities and their
versioned policies from `config/s3`:

| Identity | Allowed | Boundary |
| --- | --- | --- |
| ingestion | read and write `landing/*` | no warehouse access |
| Spark | read `landing/*`, manage `warehouse/*` | no landing writes |
| Trino | read `warehouse/*` | no landing access or writes |

Run `docker compose --env-file .env run --rm minio-access-check` to verify permitted and
denied operations. The data checks authenticate as the scoped identities, not as root.
The landing fixtures authenticate as ingestion, Spark jobs authenticate as Spark, and
Trino nodes authenticate as Trino. Hive Metastore still uses the bootstrap identity until
its object-store responsibilities and dedicated policy are proven separately.

## Upgrade from 0.32

Keep the six service-account variables from `.env.example`, run `minio-init`, and recreate
the ingestion, Spark, and Trino containers so they receive the scoped credentials. Run
`minio-access-check` before the end-to-end workflow. Existing bucket data is unchanged.
Roll back by restoring the 0.32 Compose file and recreating the affected containers.

## Stop

```bash
docker compose down
```

The named volume is preserved. Use `docker compose down --volumes` only when local
object data is intentionally being discarded.
