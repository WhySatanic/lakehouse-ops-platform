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

## Volume and phase observations

The private driver accepts `--orders 1000`; the default stays 100 and the supported
local diagnostic range is 100 through 10000. Customers (10), products (5) and anomaly
counts (one each) remain fixed. Use a new dedicated project, fresh catalog and empty
artifact directory for each volume. Never reset a checkpoint or reuse an accepted
catalog to create another measurement. This option does not change the crash boundary.

`fixture_orders` records the requested size. `phase_elapsed_seconds` retains monotonic
wall-clock observations for `interrupted_attempt`, `audit_before_retry`, `retry`,
`audit_after_retry` and `idle_replay`. The interrupted timer includes child spawn,
the normal OS-lock acquisition, all stages, waiting for the completed-gold marker and
confirmed process termination. Retry includes gold verification and checkpoint; audits
are measured separately. Initial catalog checks, fixture generation/S3 landing and
parent lock/journal-boundary checks are outside these phases. Failed operations retain
their elapsed time; unstarted phases remain absent. Timings are not execution deadlines.

For per-container Spark memory, set the overlay before starting the new project and
retain it for the driver, then add the selected volume to the original command:

```powershell
$env:COMPOSE_FILE = 'compose.yaml;compose.spark-memory.yaml'
# Start the fresh dedicated reference stack as above, then:
uv run --frozen python -m lakehouse_ops.commerce_owner_exit `
  --orders 1000 --output artifacts/commerce-owner-exit-1000 `
  --s3-endpoint-url http://localhost:19500 --server http://localhost:18500
```

Seven Spark memory observations per attempt are retained in `owner.log` and `retry.log`;
also redirect/retain the invoking terminal because native Docker stderr is not captured
by those Python logs. Compare individual cgroup-v2 container peaks, never add sequential
peaks. Observations exclude MinIO/HMS/Trino, remote executors and total host memory.
Missing/unavailable records are not zero usage. Run volumes sequentially on separate
fresh catalogs; warm images and other running host services are uncontrolled. One
observation per volume does not establish throughput, a scaling curve or production
capacity. Forced termination here is still after completed gold, not within a commit.

Verify a retained measured run without starting compute or modifying its checkpoint:

```powershell
$env:LAKEOPS_OWNER_EVIDENCE_DIRECTORY = (Resolve-Path artifacts/commerce-owner-exit-1000).Path
$env:LAKEOPS_RUN_OWNER_VOLUME_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_owner_volume.py --no-cov
```

This opt-in check requires a successful report produced with the memory overlay, all
five phase timings, seven observed memory records in each attempt log, the requested
verified order count and distinct interrupted/successful journal attempts. Missing or
unavailable memory evidence fails the check; it is not silently excluded from the result.

Both the original owner and its retry pass their pipeline journal to every Docker
stage. The same attempt ID therefore links stage events to durable container launch
intent and completion records, including all eight original containers after forced
owner termination. The retry has its own attempt ID and eight different container
names. These ownership records do not advance the checkpoint or change the interrupted
pipeline's last-stage `started` event. Completed stages use auto-removal; absence of
their containers later is expected, not proof of an orphan or permission to clean up.

Inspect fresh retained acceptance evidence without starting compute:

```powershell
$env:LAKEOPS_OWNER_EVIDENCE_DIRECTORY = (Resolve-Path artifacts/commerce-owner-exit-demo).Path
$env:LAKEOPS_RUN_JOURNAL_INSPECTION_SMOKE = '1'
$env:LAKEOPS_RUN_OWNER_OWNERSHIP_SMOKE = '1'
uv run --frozen pytest tests/integration/test_commerce_retained_journal.py --no-cov
```

The ownership check requires evidence produced after this journal propagation fix;
older retained journals remain usable by the original journal-inspection check alone.

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

### Two-volume forced-owner observation

Fresh projects `lakehouse-ops-owner-exit-scale-100-v1` and
`lakehouse-ops-owner-exit-scale-1000-v1` each ran all eight actual production stages
twice, using the memory overlay and confirmed forced Windows process termination
(`-15`) after completed gold. The experiments were sequential on separate catalogs.

| Orders | Interrupted, s | Retry, s | Audit before/after, s | Idle, s | Largest Spark container peak, bytes |
| --- | --- | --- | --- | --- | --- |
| 100 | 92.678972 | 86.497441 | 5.633347 / 4.050281 | 0.005643 | 966565888 |
| 1000 | 92.420380 | 96.969648 | 5.817973 / 4.169335 | 0.009833 | 1008676864 |

Both retained 17 incomplete owner events and 23 successful retry events with different
attempt IDs, no checkpoint before retry and a compute-free idle call. Row/current-file
hashes stayed identical across all 14 tables (12 files). Gold verified 100/1000 orders,
31 days and 278410/2733643 captured cents respectively. Each pair of owner/retry logs
contains 14 observed memory records. Both read-only volume/memory acceptance tests passed
without modifying the journal or checkpoint. No experiment containers remained running;
the projects' volumes were retained.

These are one observation per volume, with warm images, other host services and concurrent
local quality/Docker smoke checks uncontrolled. Similar first-attempt times do not prove
constant cost or scalable throughput. The memory peak is for one Spark container, not
the combined stack. In-transaction failure, OOM, maintenance and production capacity
remain unmeasured. This extends the real forced-exit drill, not the caught-exception drill.

Reports:

- `artifacts/owner-exit-scale-100-20261012-v1/owner-exit.json`, SHA256
  `1bb73314d00a0a70a84140123427625a088e09824e0df3492974bc51124c40d7`.
- `artifacts/owner-exit-scale-1000-20261012-v1/owner-exit.json`, SHA256
  `189193cb72216fbe1c8e0a615763a2498e8ba9f57c9d9f1ec5e48168be3007dd`.

Fresh local main-based quality: 1031 passed, 18 expected opt-in skips, 91.53% coverage;
three isolated Docker smokes and all policy/contract/image-lock gates passed. No version
bump or release. Container ownership propagation is a separate increment; the volume
driver remains compatible with it without claiming that these older runs retained it.

### Container ownership propagation evidence

Fresh `lakehouse-ops-owner-exit-ownership-20261012` repeated the real forced termination
with journal propagation enabled. The owner exited with `-15`, retained 17 pipeline
events and eight completed container launches, and wrote no checkpoint. The retry
retained 23 pipeline events, eight different completed launches and one completed
checkpoint. The container journal contains 16 launch intents and 16 completions across
two distinct attempt IDs, with matching batch, manifest, service and project identities.
Both read-only retained-evidence tests passed without changing the journal/checkpoint.

All 14 tables again kept identical row/current-file hashes; retry verified 100 orders,
31 days and 278410 captured cents, then the runner returned compute-free idle. Only
this reference project's five persistent services were stopped; its volumes remain.
The failure boundary is still after completed gold, not inside a Spark transaction.

Report: `artifacts/commerce-owner-ownership-20261012/owner-exit.json`, SHA256
`2d655e7ea8662ff66e67519b05af8ae41f6c41dc5983d6cd3ed30e3c1011229a`.
Local one-command quality check on main including merged #205: 990 passed, 17 skipped,
91.59% coverage, plus three
real Docker smoke tests. The ownership smoke is opt-in and adds no CI stack startup.

After successful acceptance, stop only this project's services, retaining all volumes:

```powershell
docker compose --profile catalog --profile query stop `
  trino-worker trino-coordinator hive-metastore metastore-db minio
```
