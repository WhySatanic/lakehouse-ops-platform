# Local two-volume commerce measurement

This opt-in experiment runs the actual eight-stage commerce runner twice against a
dedicated, fresh MinIO/Hive Metastore/Trino stack, with seven Spark memory observations
per batch. It depends on the [Spark memory overlay](spark-container-memory.md).
It does not start or stop services automatically and never deletes volumes.

Use a new Compose project name and unused host ports. These public development
credentials are only for the local reference stack. Run from the repository root:

```powershell
$env:COMPOSE_PROJECT_NAME = 'lakehouse-ops-scale-demo'
$env:COMPOSE_FILE = 'compose.yaml;compose.spark-memory.yaml'
$env:MINIO_API_PORT = '19100'
$env:MINIO_CONSOLE_PORT = '19101'
$env:HIVE_METASTORE_PORT = '19183'
$env:TRINO_HTTP_PORT = '18100'
$env:AWS_ACCESS_KEY_ID = 'lakeops-ingest'
$env:AWS_SECRET_ACCESS_KEY = 'ingest-development-only'
docker compose --profile catalog --profile compute --profile query up -d --wait `
  minio metastore-db hive-metastore trino-coordinator trino-worker
uv run python -m lakehouse_ops.commerce_pipeline_profile `
  --output artifacts/commerce-pipeline-scale-demo --orders 100 1000 `
  --s3-endpoint-url http://localhost:19100 --server http://localhost:18100
```

The bucket is `lakehouse`; keep the reference stack's default bucket configuration.
The dedicated catalog must contain no commerce tables before measurement. The output
directory must be empty. A repeated experiment needs a new project/output directory,
not deletion of previous evidence or resetting a live catalog.

Each chronological batch has 10 customers, 5 products, and one null email, duplicate
order, late order and invalid payment. Only the order volume increases. The second
sample uses the same warm stack and existing table history, not an isolated cold run.
The retained `profile.json` contains runner stage/verification/checkpoint timings,
seven container memory reports, current Iceberg file counts/bytes by table, and a
Trino revenue query ID/result/statistics. File inventories are cumulative current
files across batches, not per-batch physical bytes or orphan-file accounting.

Each successful sample is followed by an idle runner invocation; its file inventory
must remain unchanged. This demonstrates checkpoint replay only, not interruption,
partial-stage recovery, compaction or exactly-once behavior under crashes. All child
stdout/stderr logs are retained, including failures. Failed experiments preserve a
`failed` report and completed samples; they must not be labelled capacity evidence.
The child invocation has a 30-minute outer timeout; it is not an owner-death cleanup
mechanism. Existing per-stage timeout handling remains authoritative. Do not launch
other writers against the measurement stack.

Memory values cover individual Spark containers, not total-stack memory; see the
memory runbook for cgroup accounting and missing-exit limitations. One run at two
small volumes is reproducibility evidence, not a production capacity claim.

After the run, stop only this project's services if no further measurements need them:

```powershell
docker compose --profile catalog --profile compute --profile query stop `
  trino-worker trino-coordinator hive-metastore metastore-db minio
```

Volumes and evidence remain available. Clear the task-specific environment variables
before working on another stack. No public CLI contract or data migration changes.

## Local evidence, 2026-10-12

Dedicated `lakehouse-ops-scale-v3`, Spark 3.5.9/Iceberg 1.11.0, HMS 4.0.0,
Trino 483, open-source MinIO, Windows Docker/WSL2. Spark uses `local[2]`,
not a declared cluster capacity or container CPU limit. Both volumes completed all
eight stages and Trino gold verification, emitted seven observed Spark memory records,
then returned idle with unchanged current files.

| Orders | Runner seconds | Largest Spark container peak bytes | Cumulative files | Cumulative file bytes | Revenue cents |
| --- | --- | --- | --- | --- | --- |
| 100 | 106.772 | 1,001,807,872 | 11 | 62,248 | 278,410 |
| 1,000 | 106.683 | 1,088,000,000 | 22 | 276,044 | 2,733,643 |

Revenue queries `20261012_043049_00016_yujfs` and `20261012_043241_00046_yujfs`
finished in 91 and 66 ms reported elapsed time. They read 31-row daily aggregates,
not the whole source dataset. Similar runner durations at these small volumes do not
prove scalability; startup overhead and warm state are confounders. The maximum
per-container peak is not a sum or total-stack RAM usage.

Local full evidence: `artifacts/commerce-pipeline-scale-20261012-v3/profile.json`,
SHA-256 `7dd127737b33676bf77c23d303eff7310d00be4bb27fe60b8af671ba1e622834`.
Adjacent logs and fixtures are retained locally, not in a release evidence bundle.
Earlier failed measurements remain separate: one missed bucket forwarding; another
completed processing but failed on Windows-default decoding of Docker UTF-8 logs.
Its checkpoint was not reset or relabelled as failed processing. The completed
experiment used a fresh project.
