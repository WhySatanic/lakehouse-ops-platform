# Recovery after gold publication, before checkpoint

Opt-in bounded local acceptance, using a caught injected exception after the last
real Compose stage succeeds. This is not SIGKILL, OOM or interruption inside an
Iceberg transaction. It does not implement distributed exactly-once execution.

Start a fresh dedicated reference stack from the repository root. Choose unused ports
and a new project name for repeat experiments. No existing data is reset or deleted.

```powershell
$env:COMPOSE_PROJECT_NAME = 'lakehouse-ops-recovery-demo'
$env:MINIO_API_PORT = '19300'
$env:MINIO_CONSOLE_PORT = '19301'
$env:HIVE_METASTORE_PORT = '19383'
$env:TRINO_HTTP_PORT = '18300'
$env:AWS_ACCESS_KEY_ID = 'lakeops-ingest'
$env:AWS_SECRET_ACCESS_KEY = 'ingest-development-only'
docker compose --profile catalog --profile compute --profile query up -d --wait `
  minio metastore-db hive-metastore trino-coordinator trino-worker
uv run python -m lakehouse_ops.commerce_recovery `
  --output artifacts/commerce-recovery-demo `
  --s3-endpoint-url http://localhost:19300 --server http://localhost:18300
```

Use the default `lakehouse` bucket and default Compose file, without a profiling
overlay. The scenario requires an empty output directory, fresh commerce catalog,
one source batch and no competing writer. It creates a deterministic 100-order
fixture, conditionally lands it, and runs the actual eight production stages twice.

After the first attempt writes gold, the injected exception must leave the checkpoint
absent and the same hash-bound batch pending. All commerce table rows are read through
Trino and hashed in stable order, including `gold.dim_customers_scd2`; Iceberg file
inventories are hashed and snapshot counts captured. The retry
must complete gold verification and checkpoint, preserving every table's row count,
content hash and physical data/delete file inventory. Snapshot history must not shrink.
Empty MERGE operations can add metadata-only snapshots even with zero inserted rows;
snapshot count is recorded, not required to remain identical. An additional idle
attempt must start no compute. This proves no duplicate rows or changed data files
for this specific failure boundary,
not every crash boundary or historical customer correction.

`recovery.json` retains the before/after audit and retry result. `stages.log` retains
stage stdout and Python stderr; native Docker/Spark stderr also appears in the invoking
terminal. Preserve that terminal output separately when investigating failures. A failed
report is not a recovery success; checkpoints already committed before an audit failure
remain authoritative and must not be reset to rerun the experiment.

Afterwards stop only the experiment's services if no further tests need them:

```powershell
docker compose --profile catalog --profile query stop `
  trino-worker trino-coordinator hive-metastore metastore-db minio
```

Volumes and evidence remain retained. Clear task-specific environment variables before
another stack. No data migration, default pipeline change or public CLI contract change.

## Retained local acceptance, 2026-10-12

Dedicated `lakehouse-ops-recovery-v2` reference stack, Spark 3.5.9 and Iceberg 1.11.0:
all eight actual stages ran twice. The interrupted attempt left no checkpoint. Retry
verified 100 orders, 31 daily rows and 278410 captured revenue cents, then committed
the checkpoint; the next run was idle. All 14 tables retained identical row hashes
and physical file inventories (12 files). Retry took 90.20 seconds on this warm,
small fixture; this is not a throughput or capacity benchmark.

Snapshot count increased from 15 to 29 through empty MERGE commits. An initial
experiment incorrectly required unchanged snapshot counts and failed; that evidence
and its checkpoint were retained, not reset or relabelled successful. The acceptance
now separates unchanged data/files from observable metadata-only snapshots, with
negative tests for changed row hashes, changed files and lost snapshot history.

Retained report: `artifacts/commerce-recovery-20261012-v2/recovery.json`, SHA256
`36c1d87719a9267283bd3f0cffa8dcb4e48a377dec771b0e6a7f63aac6ea1994`.
Artifacts and Docker volumes are local evidence, not shipped source or release assets.
