# Forced commerce runner-owner termination

This opt-in local acceptance complements the caught-exception recovery drill. A spawned
OS process holds the normal runner lock, retains its SQLite execution journal, and runs
all eight commerce stages. After the gold Docker invocation has returned successfully,
it sends its PID and a completed-publication marker over a private pipe and pauses.
The orchestrator forcibly kills only that exact process. On Windows this uses process
termination; on POSIX it uses SIGKILL. It does not rely on Python finally/exception hooks.

Use a fresh isolated catalog and empty output directory. Choose unused ports. Standard
public development credentials are only for this local reference stack:

```powershell
$env:COMPOSE_PROJECT_NAME = 'lakehouse-ops-owner-exit-demo'
$env:MINIO_API_PORT = '19500'
$env:MINIO_CONSOLE_PORT = '19501'
$env:HIVE_METASTORE_PORT = '19583'
$env:TRINO_HTTP_PORT = '18500'
$env:AWS_ACCESS_KEY_ID = 'lakeops-ingest'
$env:AWS_SECRET_ACCESS_KEY = 'ingest-development-only'
docker compose --profile catalog --profile compute --profile query up -d --wait `
  minio metastore-db hive-metastore trino-coordinator trino-worker
uv run --frozen python -m lakehouse_ops.commerce_owner_exit `
  --output artifacts/commerce-owner-exit-demo `
  --s3-endpoint-url http://localhost:19500 --server http://localhost:18500
```

The parent must reacquire the OS lock without deleting its file, observe an absent
checkpoint, and find an incomplete last-stage journal event. It audits all commerce
tables and SCD2 rows/files before retry. The orchestrator (a different OS process from
the killed owner) starts a new journal attempt and reruns the same pending batch.
Published row and file hashes must remain identical. Empty MERGE snapshots may be added;
snapshot counts are observations, not the no-duplicates invariant. A final runner call
must be idle and start no compute.

The retained `owner-exit.json`, `journal.sqlite3`, `owner.log` and `retry.log` are local
diagnostic evidence, not a release bundle. Native Docker stderr remains in the terminal.
The journal is not an Iceberg transaction coordinator or an automatic restart plan.
Failure preserves evidence and never resets a checkpoint or warehouse.

The boundary wait is bounded to 30 minutes. EOF, invalid markers or timeout cause failure,
not replay. Cleanup targets only the spawned owner PID. A failure *before* the completed
marker may leave an in-flight Docker stage: inspect that project's containers and ensure
there are no active writers before further work. This driver deliberately does not claim
or implement cleanup/recovery inside an active Spark stage or an Iceberg commit, OOM,
distributed fencing, multi-writer exactly-once, or automatic production crash recovery.
The default scheduled runner and stable CLI contract are unchanged. No version/release.

## Local evidence, 2026-10-12

Fresh `lakehouse-ops-owner-exit-v1` on Windows Docker/WSL2 completed the real acceptance.
The forcibly terminated owner returned exit code `-15` as reported by multiprocessing;
no Python failure event or checkpoint was written by that owner. Its 17 journal events
end at `spark-commerce-daily-gold/started`, despite the completed gold publication.
The orchestrator reacquired the OS lock and retried with a distinct run ID.

All 14 audited commerce/SCD2 tables retained identical row and current-file hashes.
Retry verified 100 orders, 31 days and 278410 captured cents, committed the checkpoint,
then returned idle without compute. Total snapshots grew from 15 to 29: this does not
violate the asserted row/file invariants and is not labelled snapshot-stable recovery.

Report: `artifacts/commerce-owner-exit-20261012/owner-exit.json`, SHA256
`9468ac0f7b118b5c3f2079cede87f1176b8501d4204b505922771738820a47e8`.
Local quality gates: 897 passed, nine skipped, 90.59% coverage.

After successful acceptance, stop only this project's services, retaining all volumes:

```powershell
docker compose --profile catalog --profile query stop `
  trino-worker trino-coordinator hive-metastore metastore-db minio
```
