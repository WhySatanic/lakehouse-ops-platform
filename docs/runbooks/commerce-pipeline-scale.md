# Local two-volume commerce measurement

This opt-in experiment runs the actual nine-stage full-snapshot commerce runner twice
against a dedicated, fresh MinIO/Hive Metastore/Trino stack, including current-gold
publication and eight Spark memory observations per batch. It depends on the
[Spark memory overlay](spark-container-memory.md). Historical evidence below describes
the older eight-stage, batch-scoped path separately; it is not relabelled current proof.
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
eight container memory reports, whole-project sampled memory, current Iceberg file
counts/bytes by table, and Trino revenue query IDs/results/statistics. The generated
JSONL independently supplies expected order count and captured revenue for both
batch and current-gold reconciliation. File inventories are cumulative current
files across batches, not per-batch physical bytes or orphan-file accounting.

## Optional retained warehouse inventory

The existing profile accepts `--warehouse-inventory`. After each verified batch
and idle replay, it saves `sample-N.warehouse.json` and the same observation in
`profile.json` under `samples[].warehouse_storage`. Without the flag the report
explicitly records `warehouse_inventory: not_requested`. Existing commands and
the automatic profile demo keep their previous credential requirements.

Provision a separate observer in the **dedicated local experiment project** before
running the profile. The supplied
[`warehouse-inventory-policy.json`](../../config/s3/warehouse-inventory-policy.json)
grants only bucket versioning status and version listings under `warehouse/`.
It grants no object reads, writes or deletes. Do not attach it to ingest or runtime
identities. With the scoped project and ports above, provision once using its
administrator bootstrap service (the password below is public development data):

```powershell
$bootstrap = @'
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
sed "s/__BUCKET__/$LAKEHOUSE_BUCKET/g" /policies/warehouse-inventory-policy.json > /tmp/inventory.json
mc admin policy create local lakehouse-warehouse-inventory /tmp/inventory.json
mc admin user add local lakeops-inventory inventory-development-only
mc admin policy attach local lakehouse-warehouse-inventory --user lakeops-inventory
'@
docker compose run --rm --no-deps --entrypoint /bin/sh minio-init -ec $bootstrap
$env:WAREHOUSE_INVENTORY_ACCESS_KEY_ID = 'lakeops-inventory'
$env:WAREHOUSE_INVENTORY_SECRET_ACCESS_KEY = 'inventory-development-only'
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile `
  --output artifacts/commerce-pipeline-scale-with-storage --orders 100 1000 `
  --s3-endpoint-url http://localhost:19100 --server http://localhost:18100 `
  --warehouse-inventory
```

Use an empty commerce catalog, so choose this command instead of the first profile
command above, or start a new isolated stack. The runner still uses the ingest AWS
environment credentials; only the observer uses the two separate variables.
Missing observer credentials, denied listing or a failure on a later page fails
the requested profile instead of reporting a partial total as complete.

The collector uses the SDK's
[ListObjectVersions paginator](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/paginator/ListObjectVersions.html),
including both key and version continuation markers. Every listed object version
contributes its `Size` once, including noncurrent versions of deleted keys and
`null` versions. `versions.latest` is the S3 latest object version, **not** an
Iceberg current-file classification. Latest delete markers hide older payload;
markers are counted separately and contribute no invented payload bytes.
`layout` divides payload by `metadata`/`data` path segments and `other`, not
by inferred file suffix or reachability. Raw key/version/size records are retained
for subsequent reconciliation.

`total_payload_bytes = versions.latest.bytes + versions.noncurrent.bytes` covers
the whole warehouse prefix, including non-commerce tables in that prefix. It
excludes landing, incomplete multipart uploads, S3/MinIO implementation overhead,
replication, filesystem allocation and billing. No writers or maintenance may run
concurrently: a paginated listing is not an atomic snapshot. The profile invokes
it outside the timed runner/query/idle measurements after its stages have stopped.

This increment supplies the object-store side of storage accounting. It does not
yet join Iceberg retained-snapshot references, classify unreferenced objects, run
retention, or prove a measured physical saving. `iceberg_reachability` explicitly
remains `not_classified`. SDK pagination and failure handling have offline tests;
a real versioned MinIO measurement remains required before completing PERF-02.

### Real MinIO inventory and permissions gate

The mandatory `serving-integration` CI job provisions the listing-only observer
above in its initialized reference MinIO and runs
[`test_warehouse_inventory_minio.py`](../../tests/integration/test_warehouse_inventory_minio.py)
before Spark table creation. Its writers and maintenance are idle during this step.
The test writes only a new UUID prefix under `warehouse/`: a three-byte object
overwritten with five bytes, a seven-byte object hidden by a delete marker, and
two bytes of synthetic metadata. These are synthetic payload, not valid Iceberg
table files or performance evidence. No existing objects are modified or removed;
test versions and marker are retained in the scoped CI stack.

The production collector runs through the real SDK paginator with a two-entry
page size. The complete listing must cover at least three pages and reconcile
each returned version ID and size against the writes. The expected payload delta
is 17 bytes: 7 latest and 10 noncurrent; there is exactly one additional delete
marker. Existing warehouse objects contribute to both baseline and final totals.
Concurrent writes fail this comparison instead of becoming an invented saving.

The observer must receive `AccessDenied`/403 for listing landing or bucket root,
reading an existing warehouse object, writing a new object and deleting its live
test object. A successful inventory alone does not prove these restrictions.
The CI step uses the existing administrator bootstrap container only to provision
the observer; the observer and Spark writer use separate runtime credentials.

To run on the fresh isolated stack above, provision the observer with the bootstrap
commands, export its two credentials, then run in PowerShell:

```powershell
$env:LAKEOPS_RUN_WAREHOUSE_INVENTORY_SMOKE = '1'
$env:LAKEOPS_S3_ENDPOINT_URL = 'http://localhost:19100'
$env:WAREHOUSE_INVENTORY_REPORT = 'artifacts/warehouse-inventory-minio-local.json'
uv run --frozen pytest tests/integration/test_warehouse_inventory_minio.py -q -o addopts=''
```

Use a new evidence path on each run. `serving-evidence` retains the JSON baseline,
final listing, owned prefix, written version IDs, page count and permission result.
Failures retain a `failed` JSON report where setup permits; pytest/CI logs identify
the failing assertion or API error. Default local quality skips the real test and
checks its assertions offline with SDK Stubber, including wrong payload, latest
flags, marker response, excessive permissions and setup failure. Until the real
gate has run successfully on the exact PR head, these offline tests are not a
confirmed MinIO result. A successful synthetic gate still does not finish PERF-02's
commerce retained-reference/physical-storage reconciliation.

## Query and replay measurements

Each new sample additionally retains `query_costs`: separate IDs, results and Trino
statistics for a detail orders/payments aggregation and the materialized daily mart.
Payments are grouped by order before the left join so split payments do not multiply
the order count. Both inputs are restricted to the selected batch. Order count and
captured revenue must reconcile, and the count must equal the selected fixture volume.
The old `query_id`, `query_stats` and `query_rows` fields remain available unchanged.

The detail query runs first, then daily gold, once per volume. There are no repetitions,
cache controls, percentile estimates or latency thresholds. Startup/JIT, caches, the
shared warm stack and query order can dominate elapsed time. Processed rows/bytes and
physical input bytes are engine query statistics, not source size or total storage.
Query peak memory is distinct from the Spark cgroup/container memory observations.
The current full profile also compares these SQL results with the generated input;
older historical reports below did not contain that independent reconciliation.

Each successful sample is followed by an idle runner invocation; its file inventory
must remain unchanged. This demonstrates checkpoint replay only, not interruption,
partial-stage recovery, compaction or exactly-once behavior under crashes. All child
stdout/stderr logs are retained, including failures. Failed experiments preserve a
`failed` report and completed samples; they must not be labelled capacity evidence.
The runner child wait uses the configured run budget plus cleanup grace; it is not
an owner-death cleanup
mechanism. Existing per-stage timeout handling remains authoritative. Do not launch
other writers against the measurement stack.

Spark records cover individual containers. The separate `stack_memory` observation
includes only the dedicated project's running services and Spark one-offs, excluding
host/VM overhead and inactive Linux file cache. Sampling can miss short peaks; see
the memory runbook for cgroup accounting and missing-exit limitations. One run at
two small volumes is reproducibility evidence, not a production capacity claim.

A whole-project memory observation marked `partial`, including no samples or a
known sampling error, now fails the measurement command instead of returning a
top-level `ready` profile. The diagnostic sample and raw memory report are retained
after SQL reconciliation and idle verification; an incomplete first sample does
not start the second load. The completed ingestion checkpoint and warehouse are
not rolled back or deleted. Retry the measurement with a new empty output and
fresh owned catalog, not by clearing completed state. Historical ready profiles
with nested partial memory remain incomplete cost evidence; do not relabel them.
An `observed` result is still discrete sampling, not continuous peak coverage.

Local validation on 2026-10-15 injected a partial observer result only after real
Spark/Iceberg stages, independent Trino reconciliation, idle and Docker samples.
The failed measurement retained one completed checkpoint and diagnostic sample;
it did not start load two. A fresh isolated project then passed the unchanged
default demo child with two 100-order snapshots and observed memory. Both projects
were stopped, their networks removed and their named volumes retained. This is
failure/retry correctness evidence, not a physical Docker-daemon fault test or a
performance comparison. Six regressions cover empty/partial first and partial
second observations at equal and increasing volumes. The shared local checker
passed all ten steps: 1431 tests, 34 expected skips and four Docker smoke tests.

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

## Detail versus daily query evidence, 2026-10-12

A fresh `lakehouse-ops-scale-query-v1` stack completed both full runner volumes,
seven observed Spark memory records per batch, and idle replay with unchanged files.
The new query pairs returned identical counts (100 / 1000) and captured revenue
(278410 / 2733643 cents), with zero reported spilled bytes.

| Orders | Detail processed rows | Detail physical input bytes | Daily processed rows | Daily physical input bytes | Detail / daily elapsed ms |
| --- | --- | --- | --- | --- | --- |
| 100 | 298 | 15817 | 31 | 4248 | 292 / 69 |
| 1000 | 2998 | 91579 | 31 | 4257 | 117 / 52 |

Detail IDs: `20261012_070451_00017_t4irp`, `20261012_070635_00049_t4irp`.
Daily IDs: `20261012_070451_00018_t4irp`, `20261012_070635_00050_t4irp`.
The smaller elapsed time at the larger volume is not evidence of improved scaling;
these are single sequential observations, not a controlled latency benchmark.

Retained local report: `artifacts/commerce-query-scale-20261012/profile.json`, SHA256
`8a182899f918bc0c56baa3c96e60938d2e14a684979ff37fcf6de635258ab683`.
The earlier scale report remains unchanged. No product version or release change.

## Timed idle replay with snapshot audit

New full profiles retain `idle_replay_observation` beside the existing
`idle_replay: verified` field. It measures the wall time of the idle CLI child,
including Python startup, S3 planning and local journal writes. The before/after
Trino audits are outside this timer. Each sample requires an idle result with no
selected batch or completed stage, unchanged current file counts/bytes and the
same complete retained snapshot-ID lists for every commerce table.

The unchanged file counts/bytes are aggregate evidence, not file-content hashes.
Snapshot inventory is observational and assumes the existing single-writer profile.
Concurrent writers or maintenance can invalidate the observation. This is not a
crash-recovery measurement, a latency percentile or production throughput claim.
Old reports remain readable but do not acquire measurements retroactively.

A short opt-in acceptance can use a completed local scale checkpoint. It copies
the checkpoint into a new evidence directory, runs three idle children against
the same retained stack and compares the source/copy checkpoint bytes afterward.
It does not rerun Spark or edit the previous scale report. Set local development
S3 credentials as for the original scale run, then:

```powershell
$env:LAKEOPS_RUN_COMMERCE_IDLE_PROFILE_SMOKE = "1"
$env:LAKEOPS_IDLE_PROFILE_CHECKPOINT = "artifacts/commerce-query-scale-20261012/checkpoint.json"
$env:LAKEOPS_IDLE_PROFILE_OUTPUT = "artifacts/commerce-idle-profile-20261012-v2"
$env:LAKEOPS_IDLE_PROFILE_SERVER = "http://localhost:18300"
$env:LAKEOPS_IDLE_PROFILE_ENDPOINT = "http://localhost:19300"
uv run --frozen pytest tests/integration/test_commerce_idle_measurement.py -q --no-cov
Remove-Item Env:LAKEOPS_RUN_COMMERCE_IDLE_PROFILE_SMOKE
uv run --frozen python -m lakehouse_ops.local_quality
```

The optional smoke expects the 13-table scale fixture, not the linked acceptance's
extra current-gold table. No public CLI options, data migration or release change.
Rollback restores the prior report shape without changing warehouse data.

Retained local evidence on 2026-10-12, `lakehouse-ops-scale-query-v1`: three
idle child wall times were 0.9127, 0.9235 and 0.9084 seconds. Each retained the same
13 tables, 26 snapshot IDs and 22 current files; source/copied checkpoint bytes
were unchanged. The complete acceptance including catalog audits took 16.06 seconds.
Report `artifacts/commerce-idle-profile-20261012-v2/idle-profile.json`, SHA-256:
`233ba3e0eaae32a23d0e3e0bbeaa915924b8fe84405bbdc0094cec5fb8ce85d0`.
The first attempt's JUnit/logs remain separate: its observations were idle but an
incorrect 14-table test expectation failed against the actual 13-table scale fixture.
No timings were inserted into the historical two-volume reports.
Fresh local quality passed all seven gates: 899 tests passed, ten opt-in skips,
90.75% coverage. Full-profile wiring is covered by two-volume unit acceptance;
this short live check exercised the retained stack's idle path, not a new full
two-volume Spark execution. Mandatory remote CI remains a separate merge gate.

## Growth of completed full-snapshot history, 2026-10-14

Three fresh dedicated projects each processed ten chronological 1000-order full
snapshots through the existing CLI runner. This is actual completed Iceberg history,
not a pending-manifest queue or a fabricated checkpoint. Immutable measurement
source: `ba58455de33322eac8726debb70bc3bdc2dbd481`. Main `ca0c259` was integrated
only after all measurements finished; its recovery-observer change does not alter
this nine-stage path. No product command, module, flag, dependency or image was added.

Each round used automatic free ports, the same pinned images, ten customers, five
products and fixed anomaly counts, one stack at a time. Snapshot dates were January
1 through 10, 2026. Each source had 1000 distinct orders and 2733643 captured cents.
Literal JSONL independently reconciled detail, batch daily and current daily SQL in
all 30 runs. Current gold still contains **1000 orders**, not the sum of ten snapshots.
Each round ended with ten actual hash-bound checkpoint entries. All 270 stage records,
240 observed Spark memory records, observed stack samples without errors, and 30
compute-free idle results were checked. Idle preserved file inventories and complete
snapshot-ID lists. This is replay evidence, not an injected crash-recovery experiment.

Fresh means the first catalog state; subsequent samples use warm services and prior
completed batches. OS/image/VM caches, host CPU load and startup/JIT are not controlled.
Every Spark stage starts a new process. The following three-round medians retain
observed ranges, not percentiles or a latency/capacity guarantee:

| Completed snapshots | Runner, s (range) | Planning, s | Idle child, s | Sampled stack peak, MiB | Largest Spark peak, MiB | Current files | Current file bytes |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1, fresh catalog | 134.935 (133.296 to 144.951) | 0.026113 | 2.180733 | 3697.45 | 1045.42 | 13 | 224114 |
| 5, warm history | 132.694 (127.398 to 136.512) | 0.038937 | 0.966316 | 3830.66 | 1052.95 | 57 | 1079412 |
| 10, warm history | 132.976 (126.823 to 139.007) | 0.045813 | 1.039534 | 3934.29 | 1053.65 | 112 | 2148565 |

Stage medians at those positions include Compose launch and cleanup:

| Stage | First snapshot, s | Fifth, s | Tenth, s |
|---|---:|---:|---:|
| Selected source sync | 2.024 | 1.177 | 1.313 |
| Bronze | 19.565 | 18.126 | 18.609 |
| Payment silver | 15.919 | 15.625 | 16.044 |
| Order silver | 16.738 | 16.952 | 17.108 |
| Product silver | 14.653 | 14.349 | 14.481 |
| Customer silver | 14.718 | 15.072 | 14.843 |
| Customer SCD2 | 13.069 | 12.562 | 12.633 |
| Batch daily gold | 18.521 | 18.944 | 18.680 |
| Current daily gold | 18.276 | 18.109 | 18.071 |

All 15 table file-count/byte inventories agree across the three rounds at each
position. Eleven batch-history tables add one file per snapshot; current daily and
customer SCD2 each retain one current file, and two reject tables remain empty.
These are current data/delete file inventories, not all retained snapshot files,
MinIO versions, metadata, orphan files or total disk usage. Physical names differ
between fresh catalogs; aggregate equality is not a content-hash audit.

Single detail/daily queries alternated order between neighboring snapshots. Each
position has one query pair per round, with IDs and engine statistics retained.
Detail processed 2998 rows and daily 31 at every position, with zero reported spill.
This is selected-batch filtering, not an unfiltered historical aggregation:

| Snapshot | Detail / daily median elapsed, ms | Detail physical input bytes | Daily physical input bytes |
|---:|---:|---:|---:|
| 1 | 431 / 100 | 91544 | 4249 |
| 5 | 128 / 47 | 91613 | 4250 |
| 10 | 108 / 52 | 91641 | 4250 |

Query caches and service warm-up confound elapsed comparisons; no SQL speedup from
growing history is claimed. In this bounded workload file count grows while planning
remains small compared with repeated Spark launches. This does not establish scaling
to hundreds of completed snapshots, large customer cardinality, concurrent writers,
maintenance capacity or production throughput. No compaction was performed.

To reproduce the workload on a new dedicated stack, use the existing
`generate_commerce_fixture` API with `CommerceFixtureConfig(customers=10, products=5,
orders=1000, null_customer_emails=1, duplicate_orders=1, late_orders=1,
invalid_payments=1, batch_at=<chronological UTC date>)` and explicit
`source_semantics="full_snapshot"`. The `generate-commerce-fixture` CLI's ordinary
input defaults are not a substitute for that declaration. For each fixture call
`land-commerce-fixture`, then `run-commerce-batch` with one retained checkpoint and
`--user lakehouse-performance`, followed by an idle call. Use a new stack/checkpoint
for each of three rounds; do not mark unprocessed manifests as completed or reset
state to manufacture repeats. The existing two-volume profile remains available.

Raw reports: `artifacts/completed-history-v2-20261014/round-N/history.json`, with
adjacent fixture files, runner/idle logs and checkpoint. Their SHA256 digests:

- Round 1: `39e9c386220c3964506364ef35ddada15f7ed17855118594b84319aca544abb0`.
- Round 2: `84bfae762f8d0d362dbac8e941df6f664cd466eeb94bc4bdef25e1894a78d358`.
- Round 3: `2e933b08643c9864964c23996d10263fb73dad3438d65e3cfd2866de81d36f1c`.

Only the three experiment stacks/networks were removed; all 18 named volumes remain.
Earlier failed startup evidence is retained separately: an unlisted diagnostic Trino
identity was correctly denied before any load. Retrying with the existing supported
performance identity verified both negative/positive access cases without weakening
policy. An owned normal worker restarted during that failed startup, was verified and
stopped before successful rounds; its cause is unknown. No competing containers were
observed during accepted samples. Study/marketplace data and containers were untouched.
These local reports are not RC assets or signed source attestations. Existing CI,
release policy and capability limits remain applicable; there is no version change.
