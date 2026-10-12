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
Equality here is reconciliation against materialized data, not an independent revenue
oracle; the linked acceptance uses a separate literal oracle.

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
