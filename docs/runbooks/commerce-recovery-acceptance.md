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

Use the default `lakehouse` bucket. The optional `compose.spark-memory.yaml` overlay
can record Spark process memory in `stages.log`; defaults remain unchanged.
Selecting this canonical overlay in `COMPOSE_FILE` also enables whole-project memory
observations in the existing recovery command. Use the platform path separator or
`COMPOSE_PATH_SEPARATOR` for the base/overlay list. No new command or flag is required.
The existing `StackMemorySampler` is shared with pipeline profiling and discovers only
the dedicated recovery project's containers, including active Spark one-offs. It rejects
the study stack, marketplace and normal `lakehouse-ops` project before any Docker call.

With the overlay, `recovery.json` additionally retains `stack_memory` across interruption,
before/after audits, retry and idle. The observer closes on failed loads too. Its `partial`
status and errors mean incomplete memory evidence, not zero usage or a valid capacity
measurement. Recovery correctness and observation status are separate: a sampling error
does not fabricate a failed load. These are sampled Docker container usage values, not
host RSS or exact simultaneous cgroup peaks. Without the overlay, previous recovery
output and behavior remain unchanged.

The scenario requires an empty output directory, fresh commerce catalog,
one source batch and no competing writer. It creates a deterministic 100-order
fixture, conditionally lands it, and runs the actual eight production stages twice.
The private module accepts `--orders 1000` to choose 100 through 10000 orders;
customers, products and anomaly counts remain fixed. Use a fresh dedicated project
and empty artifact directory for each volume, not a reset checkpoint or reused catalog.

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

`fixture_orders` records the requested volume. `phase_elapsed_seconds` separates
the interrupted attempt, before-retry audit, retry (including verification and
checkpoint), after-retry audit, and idle replay. Timings use a monotonic wall clock;
even a failed phase retains its elapsed time, while unstarted phases are absent.
Initial catalog checks, fixture generation, landing and planning outside these
phases are excluded. The audit queries are outside the retry timer, not hidden
inside it. These single local observations are not a capacity benchmark or a
hard execution deadline. The caught exception boundary is unchanged.

For memory observations set `$env:COMPOSE_FILE = 'compose.yaml;compose.spark-memory.yaml'`
before starting the dedicated stack and keep it set for the drill. Each attempt
emits seven `spark_container_memory` records in `stages.log`: bronze plus the six
Spark silver/gold jobs. The eighth production stage is the non-Spark current-mart
publication. Compare individual container peaks, not the sum of sequential peaks.
These Spark-only records exclude MinIO, Hive Metastore, Trino and remote executors; they
are neither total host memory nor capacity measurements. Do not treat missing
or unavailable memory observations as zero usage.

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

## Two-volume recovery observation, 2026-10-12

Two new projects, `lakehouse-ops-recovery-scale-100-v1` and
`lakehouse-ops-recovery-scale-1000-v1`, each used a fresh catalog and the memory
overlay. Spark processing was sequential, not concurrent between the experiments.
Each volume ran all eight production stages twice, then an idle replay.

| Orders | Interrupted attempt, s | Retry, s | Audit before/after, s | Idle, s | Largest Spark container peak, bytes |
| --- | --- | --- | --- | --- | --- |
| 100 | 91.802493 | 96.342994 | 6.038373 / 5.277949 | 0.006328 | 1012850688 |
| 1000 | 97.351807 | 88.859424 | 6.137104 / 4.109319 | 0.010338 | 1115258880 |

Both retained identical content hashes and physical file inventories across all
14 tables (12 files), no checkpoint before retry, and a compute-free idle replay.
Each log contains 14 observed Spark memory reports, seven per attempt. Daily gold
verified 100/1000 orders and 278410/2733643 captured revenue cents respectively.
The hosts, image caches and previously running services were not reset or controlled;
one observation per volume does not establish a scaling curve. In particular, the
shorter 1000-order retry is not evidence that larger workloads execute faster.
SIGKILL, in-transaction failures, maintenance and production capacity remain unmeasured
by this experiment. Only the two experiment stacks were stopped; volumes were retained.

Retained local reports:

- `artifacts/commerce-recovery-scale-100-20261012-v1/recovery.json`, SHA256
  `da645cbecb3b0ef278535db0e307b7ce2b107e9a37a9ad32a445dcca4378fae2`.
- `artifacts/commerce-recovery-scale-1000-20261012-v1/recovery.json`, SHA256
  `6fbc69c7cecdacb58045a063904d63044fe12e2303ed4b7602ac0c14ed5d2338`.

## Three-repeat 1k/10k recovery measurement, 2026-10-14

Six sequential fresh-catalog experiments used the existing recovery command and
canonical memory overlay, source `f22828098dfb41ab4fbc533caf658e21e5c4b410`.
That local commit contains the observer integration on base `5764863`; the published
change subsequently integrates main `ba58455` without changing the recovery boundary.
Each experiment has a unique `lakehouse-ops-recovery-measured-<orders>-r<repeat>-20261014`
project and automatic free ports. Docker reported 16637534208 bytes available.
Order was 1k/10k, 10k/1k, 1k/10k, with one stack at a time, the same pinned images,
10 customers, 5 products and fixed anomaly counts. A previously owned normal worker
restarted during the first stack's startup; its ownership and absent coordinator
were verified and it was stopped before the measured recovery phase. No competing
containers were observed during the measured phases. Its restart cause is unknown.

The first attempt starts with a fresh commerce catalog; the retry uses that same
published dataset and warm services. Neither is a controlled cold OS/image/VM run.
Each Spark stage starts a new process, including on retry. Startup, initial checks,
fixture landing and observer shutdown are excluded from the phase timers. Stack
memory spans both attempts and audits, not startup; no per-attempt stack peak is
inferred from the combined observation. Values below are three-sample medians.

| Orders | Fresh interrupted attempt, s | Warm retry, s (range) | Audit before/after, s | Idle, s | Stack sampled peak, MiB | Largest Spark peak, MiB |
|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 115.468 | 109.817 (108.965 to 112.846) | 6.523 / 4.449 | 0.007930 | 3649.77 | 1021.51 |
| 10000 | 116.771 | 111.191 (110.217 to 113.795) | 7.066 / 4.900 | 0.008388 | 3609.08 | 1078.39 |

Retry stage medians, including Compose launch/cleanup, are retained separately:

| Stage | 1k, s | 10k, s |
|---|---:|---:|
| Selected source sync | 1.509 | 1.518 |
| Bronze | 16.713 | 17.924 |
| Payment silver | 15.391 | 15.734 |
| Order silver | 16.388 | 15.891 |
| Product silver | 13.917 | 14.000 |
| Customer silver | 14.347 | 14.336 |
| Customer SCD2 | 12.359 | 12.319 |
| Batch daily gold | 18.935 | 19.141 |

All six reports were independently checked against the generated JSONL: exactly
1000/10000 distinct orders and 2733643/27461573 captured revenue cents, matching
Trino verification. Each retained no checkpoint before retry, one hash-bound entry
after all eight stages and verification, and a compute-free idle. All 14 table row
hashes and physical inventories stayed unchanged across retry, totaling 12 files.
Within each volume the row hashes/counts and file counts also agree across repeats;
physical file names differ between fresh catalogs. Snapshot totals increased from
14 to 27 through metadata-only replay commits, not duplicate data. Each log has
14 observed Spark reports, and every whole-project observer reported `observed`
with no errors. SQL audit timing is separate from the retry timer.

Reports remain under `artifacts/recovery-measured-20261014/<orders>-r<repeat>/acceptance/`.
Each adjacent `experiment.json` records source SHA, project, ports and startup,
drill and scoped cleanup results. SHA256 of the six `recovery.json` files:

| Sample | SHA256 |
|---|---|
| 1000-r1 | `5ba59e198abc4d61debe09346bfc59ceec193b178e16f598aecab24da4eab265` |
| 1000-r2 | `3911a39f572773d63ab18473c3bc8ce3d54522227185c09fe10c379eaf5ee33e` |
| 1000-r3 | `4353cb1cb8f80f8afe5f0cc0a2f3cf524582d4ee141f5ad6f85fd05bbfa60168` |
| 10000-r1 | `f80863f75e16f7eeed6dfeafad8c044501baeda1413e32ff62c0fcd2592bafb6` |
| 10000-r2 | `8c12c3c4aca28fc182ec4164fe1b9b0d2210b1dd54e66b913149afcc246de452` |
| 10000-r3 | `717d54f2cb6725882ca00cf59cbd4f3deb4b60e0cc5be651cc7fd7ef1679c509` |

Only the six experiment stacks/networks were removed afterwards; all 36 named
volumes and local evidence remain. No global prune or unrelated project shutdown.
This measures the caught post-gold boundary with ordinary batch-scoped input, not
the full-snapshot current-mart/late-correction path, SIGKILL, in-commit recovery,
100k recovery, maintenance or completed multi-batch Iceberg history growth.
The nearby timings do not establish capacity or throughput improvement. Full-stack
sampling is optional and has overhead; this is not an A/B observer-cost experiment.
There is no new CLI, wrapper, dependency, image or release scheme. Omit the memory
overlay to retain the previous output; incomplete observations remain `partial`,
never zero. Existing integration CI and release policy still apply.
