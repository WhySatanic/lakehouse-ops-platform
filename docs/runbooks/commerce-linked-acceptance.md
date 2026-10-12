# Three linked commerce snapshots through the reference stack

This opt-in acceptance uses the existing deterministic change scenario and its literal
revenue/order/customer-period oracle. It executes real S3 landing, eight production
commerce stages, optional current-snapshot gold publication, and Trino queries for
each of three chronological full snapshots. Revenue must be 1000, 1500 and 2200 cents,
not their sum of 4700. The order correction and late order must appear in current gold.

## Run locally

Choose unused ports and a new isolated project name for another experiment. The catalog
and output directory must be fresh. Do not reset existing warehouses or checkpoints.

```powershell
$env:COMPOSE_PROJECT_NAME = 'lakehouse-ops-linked-demo'
$env:MINIO_API_PORT = '19400'
$env:MINIO_CONSOLE_PORT = '19401'
$env:HIVE_METASTORE_PORT = '19483'
$env:TRINO_HTTP_PORT = '18400'
$env:AWS_ACCESS_KEY_ID = 'lakeops-ingest'
$env:AWS_SECRET_ACCESS_KEY = 'ingest-development-only'
docker compose --profile catalog --profile compute --profile query up -d --wait `
  minio metastore-db hive-metastore trino-coordinator trino-worker
uv run python -m lakehouse_ops.commerce_linked_acceptance `
  --output artifacts/commerce-linked-demo `
  --s3-endpoint-url http://localhost:19400 --server http://localhost:18400
```

The driver uses the existing `lakehouse-ingestion-ci` read-only Trino identity and
the standard Spark/S3 identities. It does not expand the role policy. Conditional
landing replay must create zero new objects. Planning must select exactly the three
expected chronological batches, with hash-bound checkpoints.

For each batch, the wrapper publishes `spark-commerce-current-gold` after normal
batch gold, verifies current revenue and customer periods through Trino, then lets
the normal runner verify batch gold and commit the checkpoint. An optional-mart or
oracle failure therefore prevents that batch's checkpoint, although its Iceberg
tables may already have been written. A separate replay of the last current mart
must add no snapshot. An additional checkpoint-driven idle run must start no compute.

`linked.json` retains per-batch progress, pipeline results and oracle observations,
including completed samples on failure. `run-journal.sqlite3` records stage events;
it is diagnostic, not an Iceberg transaction coordinator. `stages.log` retains stage
stdout/Python stderr. Native Docker/Spark stderr remains in the invoking terminal.
Retain it separately for diagnostics. Previously committed checkpoints remain
authoritative on later acceptance failure; do not reset them to obtain a green report.

All eight production stages and the optional current mart now receive their batch's
`CommerceRunJournal`. Before launching each Compose container, the standard executor
persists its unique name, service, project hint, run ID and hash-bound batch identity.
The same identity is attached as Docker labels; completion or scoped timeout cleanup
is appended to `commerce_container_events`. This closes the acceptance wrapper's
previous gap: it retained pipeline events but did not pass ownership to its executor.

The final current-mart replay has a separate run ID in the same journal, including
verification and a terminal ready/failed event. It never writes a checkpoint and
never appends pipeline events to an already completed batch run. A successful linked
scenario therefore has four ready runs but only three completed checkpoints, with
28 launch intents and 28 completed container events. A replay failure preserves the
three existing checkpoints and is retained as its own failed attempt. The compute-free
idle check starts no container and adds no journal attempt.

These records are diagnostic ownership evidence, not automatic orphan removal,
crash recovery inside an active Spark stage, or distributed coordination. A forced
owner exit can still leave a running container; inspect its exact recorded identity
before any manual cleanup. Existing stage timeouts and scoped cleanup are unchanged.

After a fresh successful run, verify retained ownership evidence without modifying
the journal, checkpoint or report:

```powershell
$env:LAKEOPS_RUN_LINKED_OWNERSHIP_SMOKE = '1'
$env:LAKEOPS_LINKED_OWNERSHIP_DIRECTORY = 'artifacts/commerce-linked-demo'
uv run --frozen pytest tests/integration/test_commerce_linked_ownership.py --no-cov
```

The retained-evidence test checks distinct batch/replay run IDs, all 28 unique
containers' intent/completion pairs, manifest identity and unchanged evidence hashes.
Old acceptance journals without ownership events do not satisfy this smoke test.

Only this acceptance wrapper inserts the optional current-mart/oracle step before
checkpoint. The default scheduled pipeline remains unchanged. This is a bounded,
single-writer full-snapshot scenario, not general CDC, deletions, multi-writer
exactly-once, or retroactive customer corrections at an earlier effective timestamp.
The separate history-rebuild work is not invoked or required by this driver.
No version or release change.

## Local evidence, 2026-10-12

The fresh `lakehouse-ops-linked-ownership-20261012` stack completed the corrected
driver in 377.24 seconds including startup (before service stop). Revenue/history
oracles passed for all three snapshots. SQLite retained four ready runs, exactly
three checkpoints, and 28 unique containers with 28 launch intents and 28 completions.
A live Spark container's run/batch/manifest/project labels matched its prelaunch row.
The retained-evidence smoke passed and preserved all three evidence-file hashes.
Scoped timeout/label Docker tests also passed. Only this experiment's services were
stopped; volumes, checkpoints and logs were retained, with no running project container.

Report: `artifacts/linked-ownership-20261012/linked.json`, SHA256
`c8ef0e80f400e065e32f9445632ad7816cac2f539d63df7c8a54caccf2b03ee9`.
The earlier unowned journal was rejected by the new retained-evidence test, as expected.

The isolated `lakehouse-ops-linked-v3` reference stack completed all three batches
with `status=ready`. Trino observed 1000, 1500 and 2200 cents, the exact customer
periods, and the corrected/current order set. The journal contains three completed
checkpoints, each after its current-mart oracle. Conditional landing replay created
no new objects, final current-mart replay was snapshot-stable, and the final runner
was idle without compute.

Retained report: `artifacts/commerce-linked-20261012-v3/linked.json`, SHA256
`55655779dab9c95698428b0732a8971d028f5a6177d22b4b1fd16abde3eab83c`.
Fresh local quality gates passed: 865 tests, nine skipped, 91.21% coverage.

Earlier attempts remain failed evidence, not successful runs. The first used an
unmapped Trino identity and stopped before landing. The second encountered Trino's
`timestamp(6) with time zone` text ending in ` UTC`; its first batch had published
tables but no checkpoint. The timestamp parser now has a regression test for that
actual response, including retained fractional seconds. The successful experiment
used a new isolated catalog; no failed checkpoint or warehouse was reset.

Stop only the experiment's services when finished; volumes and evidence stay retained:

```powershell
docker compose --profile catalog --profile query stop `
  trino-worker trino-coordinator hive-metastore metastore-db minio
```
