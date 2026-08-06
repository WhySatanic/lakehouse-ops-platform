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
`mc mb --ignore-existing`.

## Stop

```bash
docker compose down
```

The named volume is preserved. Use `docker compose down --volumes` only when local
object data is intentionally being discarded.
