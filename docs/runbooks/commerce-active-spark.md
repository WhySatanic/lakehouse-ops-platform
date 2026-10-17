# Experimental active Spark interruption acceptance

This opt-in acceptance driver prepares REC-02/REC-03. Its real-stack result is
**unverified until executed**. Offline tests establish refusal and orchestration
behavior, not Spark/Iceberg recovery. It adds no public CLI or production kill policy.

The required serving CI runs the real driver on its own fresh project after the
existing recovery checks. Its step has a 15-minute limit and retains the report,
stage stderr and infrastructure logs in `serving-evidence`. Cleanup targets only
that run's Compose project and keeps its volumes. Passing unrelated jobs does not
verify this experiment; inspect this exact step and its artifacts on the PR head.

The driver reuses the three linked full-snapshot source batches and literal SQL
oracles from the normal commerce acceptance. It completes the baseline batch, then
starts the correction batch through the production runner. A separate observer
waits for that attempt's journal-recorded SCD2 container, verifies all ownership
labels and probes the Spark UI inside its immutable Docker ID. A RUNNING job with
positive `numActiveTasks` is required; a running container alone is insufficient.
The probe uses the official [Spark REST API](https://spark.apache.org/docs/3.5.7/monitoring.html#rest-api).

After rechecking the same immutable ID and running state, the observer sends KILL
only to that container. The production stage must fail as well. A missed boundary,
changed ownership, failed kill or successful stage is a failed experiment, never a
successful injection. The observer stops when the stage exits; no later stage is
targeted. No broad container enumeration or removal is used.

The driver requires byte-identical checkpoint state after the failed correction,
captures warehouse rows/files/snapshots and retries from a new planner/journal
attempt with retained manifest, active intent and history. Literal current revenue,
orders and customer history are checked before checkpoint, followed by the third
source batch and an idle call that must start no compute.

## Run on a dedicated fresh stack

Run from the repository root. Keep the same checkout and output paths for diagnosis.
Choose unused ports and a fresh project with the required prefix. Never point this
experiment at a shared catalog or reuse an existing output directory.

```powershell
$env:COMPOSE_PROJECT_NAME = 'lakehouse-ops-active-spark-local1'
$env:MINIO_API_PORT = '19600'
$env:MINIO_CONSOLE_PORT = '19601'
$env:HIVE_METASTORE_PORT = '19683'
$env:TRINO_HTTP_PORT = '18600'
$env:AWS_ACCESS_KEY_ID = 'lakeops-ingest'
$env:AWS_SECRET_ACCESS_KEY = 'ingest-development-only'
docker compose --profile compute build spark-commerce-customer-scd2
docker compose --profile catalog --profile compute --profile query up -d --wait minio metastore-db hive-metastore trino-coordinator trino-worker
docker compose run --rm minio-init
uv run --frozen python tests/integration/exercise_commerce_active_spark.py --output artifacts/active-spark-local1 --s3-endpoint-url http://localhost:19600 --server http://localhost:18600 2> artifacts/active-spark-local1-stages.log
```

Create the `artifacts` parent directory first if needed. The driver creates the
empty experiment output itself. The shown development credentials apply only to
this local reference stack. No container or volume cleanup occurs automatically;
preserve the catalog, landed objects, journal and logs after failure. Observe any
remaining owned stage before retrying. A failed experiment is not permission to
reset the checkpoint or warehouse.

`active-spark.json` is written on success and failure after argument validation.
It retains source SHA, project, attempted run IDs, batch/manifest/container identity,
Spark job observation, UTC observation/kill times, checkpoint hashes, audits and
literal oracle results for completed attempts. `status=ready` requires all phases.
Stage errors and native Docker/Spark output are retained by the stderr redirection.

The interruption report also retains bounded `observer_diagnostics` on success and
failure: attempt count, outcome counts, elapsed observation time, whether the stage
failed, and the last probe. A failed UI probe includes its return code and at most
512 stderr characters. A successful UI response retains at most 20 job summaries
(status, active task count and application ID), with the full job count separately.
These observations distinguish UI startup/probe failures from responses with no
active tasks; they do not make a missed interruption a successful recovery test.
No unbounded polling history or job payloads are retained.

The [diagnostic compatibility evidence](../evidence/active-spark-observer-diagnostics-2026-10-17.json)
records the original main-CI miss and a separate local run with an observed active
kill, three verified batches and idle replay. It does not claim sampling misses are fixed.

The observer polls for up to 180 seconds, with five-second inspection/probe and
ten-second kill calls. If no active task is observed, it refuses a verified retry;
the normal stage timeout still bounds the executing Compose stage. The small
scenario may finish too quickly for observation. Such a miss is an unsuccessful
experiment, not evidence that active interruption recovered. The observer is not a
general cancellation service and must not be used against an unrelated runner.

## Boundaries

Active Spark work may be validation/counting before a write. The kill and commit
can race. This is **not** an injection inside Iceberg commit, a proof of rollback,
host/power-loss recovery or distributed exactly-once. Inspect committed snapshots
instead of inferring commit outcome from owner/container exit. The current driver
tests SCD2 forward correction, not late rebuild, every stage, OOM or network failure.

Local independent checks:

```powershell
uv run --frozen pytest tests/test_commerce_active_spark.py tests/test_commerce_linked_acceptance.py tests/test_commerce_run_journal.py --no-cov -q
uv run --frozen python -m lakehouse_ops.local_quality
```

These checks deliberately use controlled substitutes for the new experiment. A
real retained `active-spark.json` with matching source and logs is still required
before marking REC-02/REC-03 verified or claiming this recovery in release notes.
