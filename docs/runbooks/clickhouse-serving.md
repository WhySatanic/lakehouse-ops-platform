# ClickHouse Iceberg/S3 serving experiment

This opt-in experiment proves that ClickHouse can query the same Spark-written Iceberg
tables in MinIO without copying them into ClickHouse native storage. It uses the pinned
open-source ClickHouse 26.3 LTS image and a dedicated warehouse-read-only MinIO identity.

## Acceptance path

Prepare the deterministic bronze and silver tables with the normal Spark workflow, then
start the serving engine and run its checker:

```bash
docker compose --profile catalog --profile compute up -d --wait \
  minio metastore-db hive-metastore
docker compose run --rm minio-init
docker compose --profile compute run --rm landing-fixture
docker compose --profile compute run --rm bronze-input-sync
docker compose --profile catalog --profile compute run --rm spark-bronze
docker compose --profile compute run --rm silver-landing-fixture
docker compose --profile compute run --rm bronze-input-sync
docker compose --profile catalog --profile compute run --rm spark-bronze
docker compose --profile catalog --profile compute run --rm spark-silver
mkdir -p artifacts && touch artifacts/clickhouse-serving.json
chmod 0666 artifacts/clickhouse-serving.json
docker compose --profile serving up -d --wait clickhouse-server
docker compose --profile serving run --rm clickhouse-iceberg-check
uv run python tests/integration/check_clickhouse_serving.py \
  artifacts/clickhouse-serving.json
```

The checker queries the Iceberg table roots through `icebergS3`, then records schema
version `1.0` evidence with the exact ClickHouse version, integration mode, source URL,
silver and reject row counts, duplicate-key count, and the expected survivor row. A pass
requires two silver rows, one reject, no duplicate business keys, and the same newest
survivor selected by Spark and Trino acceptance.

The shared `minio-access-check` also proves that `lakeops-clickhouse` can read warehouse
objects but cannot write warehouse objects or read landing data.

## Stop the profile

```bash
docker compose --profile catalog --profile compute --profile serving down
```

The named `clickhouse-data` volume is retained by default. Add `--volumes` only when the
local serving state is intentionally disposable.

## Current limits

- This experiment uses direct Iceberg table paths. It does not claim Hive Metastore
  discovery or ClickHouse catalog synchronization.
- It proves query-in-place interoperability and row-level reconciliation, not a latency or
  concurrency advantage over Trino.
- ClickHouse has a dedicated MinIO identity, but its SQL endpoint is not integrated with
  Ranger in this profile.
- The deterministic dataset is deliberately tiny and is unsuitable for capacity sizing.

## Upgrade from 0.43.0

No warehouse or metastore migration is required. Version 0.44.0 adds the optional
`serving` profile, the persistent `clickhouse-data` volume, ports 8123 and 9002, and the
`MINIO_CLICKHOUSE_*` and `CLICKHOUSE_*` environment variables shown in `.env.example`.
Existing core, query, security, and observability profiles are unchanged.
