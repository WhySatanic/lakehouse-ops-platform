# One-command full commerce pipeline measurement

With Docker running and the reference images already available, from the checkout:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile_demo `
  --output artifacts/commerce-pipeline-profile-demo --orders 100 1000
```

Use a new empty output directory. The optional volumes must be ascending integers
between 100 and 100000. The default is 100 and 1000. Invalid volumes, a missing
memory overlay or an unrelated checkout are rejected before services start.

The opt-in `--orders 10000 100000` comparison uses the same runner, stage and
whole-run budgets, Spark limits and scoped teardown. It does not add services or
raise container memory limits. Check available Docker VM memory first and run
only one measurement stack at a time; a permitted input size is not a capacity
guarantee. Failed or partial runs remain diagnostic evidence, not measurements
to include in a successful comparison. Keep the default small run for setup.

This private module wraps the existing [full pipeline measurement](commerce-pipeline-scale.md)
in the same scoped lifecycle as the linked demo. It creates a unique
`lakehouse-ops-scale-demo-<uuid>` project, selects four free ports, enables only the
checked-in Spark memory Compose overlay, and starts its own MinIO, metastore database,
Hive Metastore, Trino coordinator and worker. It neither pulls nor builds images.
The port probe cannot reserve ports until Docker starts; startup failures are retained.
The shared demo teardown uses scoped Compose `down`, so completed tests leave no
service/initializer containers in Docker Desktop. Named data volumes and evidence
remain; no `--volumes`, image removal or global prune is used.

The child explicitly declares two chronological complete snapshots and runs the
normal commerce runner through all nine production stages, including the current
daily mart. Each retains eight observed Spark container memory reports, stage,
verification and checkpoint timings, cumulative current Iceberg files/bytes, and
batch-scoped Trino detail/daily query statistics. Query order count and revenue must
reconcile. An independent oracle reads the synthetic order/payment JSONL inputs;
detail, batch daily and current daily SQL must match its order count and captured
revenue. This input oracle is specific to the generator's valid canonical orders,
identical duplicate rows and null invalid-payment amounts, not arbitrary commerce
input validation. Missing stages, incomplete memory evidence or SQL disagreement
fail the profile. Each batch then runs an idle replay with unchanged inventories.
These additional profile oracles run after the normal runner's verified checkpoint;
a failed measurement does not roll back that checkpoint or Iceberg writes. They
do not replace the runner's processing gate or prove crash-recovery semantics.

The default fixture generator and its CLI keep legacy manifests and identities.
Only an explicit Python `source_semantics="full_snapshot"` generation request
adds this declaration to the identity-bearing config. No manifest is modified
after publication or reused under a different meaning. Unknown semantics are
rejected before writing a fixture.

Both inventories include the `commerce_*` tables in bronze, silver and gold and
`gold.dim_customers_scd2`. Customer history participates in the fresh-catalog
guard and idle replay checks, even when no other table changes. File bytes count
only current data files, not retained obsolete files or Iceberg metadata storage.
Reports produced before this correction omitted the customer-history table;
their totals and idle assertions must not be treated as complete history evidence.

Each new sample additionally retains `stack_memory` and a separate
`sample-N.stack-memory.json`. The observer polls running containers selected by the
exact Compose project label, including core services and active one-off Spark jobs.
It never reads all-host statistics when that project has no containers. Failed or
incomplete observations retain their errors and `status: partial`, not a zero-byte
success. The observer stops on both successful and failed pipeline execution.

`peak_observed_bytes` is the largest **sum at one sampling time**, not the sum of
unrelated per-container peaks. These are discrete, rounded Docker CLI observations;
short peaks can be missed. Docker CLI subtracts inactive file cache on Linux
([Docker stats documentation](https://docs.docker.com/reference/cli/docker/container/stats/)).
Host/WSL VM overhead, unlabelled remote workers and other projects are excluded.
Observer subprocesses have ten-second limits and a bounded shutdown. A failed
sampling round cannot be used as proof of complete-stack memory coverage.

If Docker reports exactly one requested container no longer exists, or returns
only `EOF` as a job exits during an in-flight statistics request, the observer
rediscovers running IDs using the same exact project label. Only when exactly one
previously requested ID is absent and owned containers remain does it retry once.
A named missing-container error must identify that same retired ID. An `EOF`
with unchanged IDs, an empty project, multiple retired IDs or additional error
text remains a failed observation; `EOF` alone is not evidence of retirement. The accepted
sample records `resampled_after_container_exit`; it measures the new instant, not
the vanished container's earlier peak. The failed round is discarded, never
combined with the retry or interpreted as zero memory. Empty rediscovery, daemon
errors, timeouts, malformed/foreign/duplicate IDs and a failed retry remain errors
and produce `partial` evidence. No all-host fallback or unbounded retry is used.
The race path has at most four ten-second subprocess calls; observer shutdown
allows 45 seconds. Even successfully resampled evidence remains discrete samples,
not continuous whole-stack coverage or host/VM memory.

The real container-exit regression is opt-in:

```powershell
$env:RUN_DOCKER_STACK_MEMORY_TEST = '1'
uv run --frozen pytest --no-cov -q -s tests/test_commerce_stack_memory.py
Remove-Item Env:RUN_DOCKER_STACK_MEMORY_TEST
```

On 2026-10-13 this passed all 13 cases in 6.65 seconds. The Docker case starts two
disposable, network-isolated containers from the existing local Spark image,
removes only its stage after discovery, and exercises the actual failing Docker
statistics call plus scoped retry. Project
`lakehouse-ops-scale-memory-43af33e9a41f41adbe35d843c59897ca` retained core ID
`f087efa4497d`, retired stage ID `4d6795c654a4` and 4198498 bytes for one accepted
sample. Both disposable test containers were removed. No warehouse volumes,
image pulls/builds or full Spark pipeline were involved. This proves handling of
the discovery/exit race, not stack-memory cost. Historical `partial` reports are
not rewritten or promoted; repeated 1k/10k measurements are still required.

The post-cleanup 1k/10k run on 2026-10-14 (Europe/Moscow) completed all nine
stages, input/SQL reconciliation and idle replay, but memory reports were `partial`.
An independent live probe captured Docker's exact `EOF` error; short-lived
network-isolated containers reproduced it during a statistics request. These
partial reports are not complete-stack cost evidence.

The extended opt-in regression passed 20 cases in 12.20 seconds. Besides retirement
before discovery completes, it removes only its disposable stage while the real
statistics request is active and requires the actual `EOF` failure. Project
`lakehouse-ops-scale-memory-48b4777887684b738883192ef7c7bba1` then accepted a
fresh sample for remaining core `f80e03d74f9d`, recorded retired stage
`c5c224b8d6d0`, and observed 626688 bytes. The failed stream is not reused. Both
containers were removed without warehouse volumes, builds, pulls or study-stack
changes. This verifies the bounded race recovery, not continuous memory coverage
or a completed three-repeat performance baseline.

`demo.json` records startup, child and shutdown command outcomes and elapsed times.
`acceptance/profile.json` retains completed samples and measurement details, while
per-sample stdout/stderr retain the actual stage outputs. A child failure, malformed
or missing ready report, or failed scoped shutdown yields a failed demo and exit 2.
Only this project's five services are stopped; volumes, tables and logs are retained.
Parent environment variables and unrelated projects are not modified.

The two samples share one newly created stack. The second sample is warm and has
prior table history. File totals are cumulative. Historical reports cover Spark
containers only; new reports separately identify sampled project-container memory.
Single detail, batch-daily and current-daily queries do not establish throughput,
latency percentiles or production capacity. This is not the generator-only profile
and not forced-exit recovery; idle checkpoint replay is the only recovery observation.
Use the separate recovery/owner-exit drills for interruption evidence.

Startup has a 240-second bound, the measurement child 3600 seconds, and scoped stop
120 seconds. Stage execution retains the demo's 300-second stage limit. Outer child
termination is not whole-process-tree cleanup or an automatic orphan-stage recovery
mechanism; inspect retained logs and this project's stage containers after a timeout.

Within the measurement child, each `run-commerce-batch` wait uses the runner's
validated `COMMERCE_RUN_TIMEOUT_SECONDS` plus the same 35-second cleanup/exit
reserve as the scheduled cycle. The default wait is 3635 seconds, not an
independent 1800-second cutoff that could kill a runner before its own deadline.
Fixture landing retains its separate 1800-second wait. This reserve does not
extend the runner's processing or checkpoint budget. The parent demo still has
its separate 3600-second limit for the entire measurement, including both volumes;
it can terminate the measurement sooner and does not guarantee child-tree or
container cleanup. Its timeout remains failed evidence requiring inspection.
Tests cover 10/3600/86400-second budgets, invalid configuration before spawning
and unchanged landing behavior; they do not reproduce a half-hour pipeline stall.

## Three bounded 10k/100k repeats, 2026-10-14

The existing command with `--orders 10000 100000` completed three serial rounds
at local source `b50ca8b403f4553ac9877ecf1f745d727b08cdfd`, based on main
`3aab1fa`. Only the demo's input bound changed; the runner, SQL/input oracles,
budgets, memory overlay and scoped teardown remained unchanged. The default is
still 100/1000. This source does **not** include PR #234's compact current writer,
so these are not its after measurements or evidence for its effect at 100k.

Images were already local. Each 10k run used fresh services/catalog; 100k used
those warmed services and one previous historical batch. The study SQL stack
remained active (about 0.87 GiB). Docker VM total was 15867 MiB, available memory
before rounds 14090/14071/14023 MiB, Windows free physical memory 3899/4116/4141
MiB. Other desktop applications remained active. There were no concurrent local
tests, probes, image pulls/builds or second own measurement stack. A successful
local sample on this shared host is not a production capacity guarantee.

| Round | Orders | Runner seconds | Project peak MiB | Largest Spark peak MiB | Cumulative files | Cumulative file bytes |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 10000 | 134.735 | 3633.85 | 1046.30 | 40 | 1899816 |
| 2 | 10000 | 133.958 | 3506.55 | 934.98 | 40 | 1899816 |
| 3 | 10000 | 135.857 | 3683.65 | 1034.13 | 40 | 1899816 |
| 1 | 100000 | 137.900 | 3928.32 | 1187.71 | 54 | 19573253 |
| 2 | 100000 | 138.449 | 3836.39 | 1099.62 | 54 | 19573253 |
| 3 | 100000 | 138.351 | 3875.64 | 1097.04 | 54 | 19573253 |

Median runner times were 134.735 / 138.351 seconds; project sampled peaks
3633.85 / 3875.64 MiB and largest Spark lifetime peaks 1034.13 / 1099.62 MiB.
Project peaks are simultaneous sampled sums, not sums of individual peaks or
continuous maxima. Inventories include customer history and current files only,
not retained obsolete files, metadata or disk usage. Counts/bytes were identical
within each volume. Current mart had 28 files / 130503 bytes at 10k and 31 files /
143488 bytes at 100k in this pre-compaction source.

| Stage | 10k median seconds | 100k median seconds |
|---|---:|---:|
| Selected input sync | 1.654 | 1.709 |
| Bronze | 19.938 | 20.895 |
| Payment silver | 15.800 | 16.763 |
| Order silver | 16.204 | 17.870 |
| Product silver | 13.802 | 13.917 |
| Customer silver | 14.232 | 14.525 |
| Customer SCD2 | 13.231 | 11.929 |
| Batch daily gold | 18.889 | 19.340 |
| Current daily gold | 19.430 | 20.485 |

All six runs completed nine stages, eight observed Spark memory reports and
observed project memory. Independent input, detail, batch-daily and current-daily
SQL agreed on 10000 / 100000 orders and 27461573 / 273446528 captured revenue
cents. Inputs still have ten customers and five products. Batch IDs are
`18ae3bdf50f64d6c` / `83279a7dc07032dc`; the first batch has a different timestamp
from the warmed 10k sample in the earlier 1k/10k comparison. Do not combine their
timings or layouts as a controlled same-volume cold/warm comparison.

At 100k, detail reads processed 299998 rows / 5377684 bytes with 776978 physical
bytes; batch daily processed 32 rows / 4386 bytes with 4316 physical bytes;
current daily processed 32 rows / 145658 bytes with 143488 physical bytes.
Spilled bytes were zero. Detail wall times were 630/625/760 ms, daily 8/8/7 ms,
current 229/238/251 ms. These are single ordered detail/daily/current reads, not
cache-controlled SQL percentiles, a throughput benchmark or an SLA. Physical
scan bytes are observations, not a linear function of order count.

100k detail / daily / current query IDs:

- Round 1: `20261013_233704_00104_e4xci` / `20261013_233705_00105_e4xci` / `20261013_233705_00106_e4xci`.
- Round 2: `20261013_234321_00104_hf4vq` / `20261013_234321_00105_hf4vq` / `20261013_234321_00106_hf4vq`.
- Round 3: `20261013_234940_00104_e34q3` / `20261013_234940_00105_e34q3` / `20261013_234940_00106_e34q3`.

Every idle replay preserved all files/snapshots. Median idle-child times were
1.760 / 1.819 seconds including startup. This measures idle recovery only, not
an injected crash. Every round left zero own containers/networks and retained
six named volumes. No user study service or marketplace container was changed.

Raw reports remain under `artifacts/profile-100k-20261014/round-N/acceptance`,
with `profile.json` SHA256 values:

- Round 1: `933abc82a5ecf1578b0536573a28f6d37c2893bfad46d42878fcf19e97f623f4`.
- Round 2: `e82e124e1314cf19941699eb63b7e5ab3e6a7a7d391387828aa5e45af713dd99`.
- Round 3: `e85c8212777aa16bd63a01aded42c3a118e0ec021c916c9d1779f86e34b5ea78`.

Separate same-volume fresh/warm runs, longer-history cost, injected failure and
retry at these sizes, and complete release-candidate path evidence remain open.
No release or audit-phase completion follows from this bounded measurement.

## Compatibility

The linked and owner-exit demonstrations keep their existing commands, reports,
Compose configuration and default behavior. The new measurement is opt-in through
its own module, not a public CLI contract change. No migration, version bump,
release or mandatory CI gate removal is included. Rollback removes only this wrapper
and its scenario wiring; retained warehouse data is not rewritten.

## Local execution, 2026-10-12

Fresh project `lakehouse-ops-scale-demo-0da7690657f3435bbbb68a11d3044f6c`
completed both volumes and stopped only its five services. All eight stages ran
per sample, seven Spark memory reports were observed, detail/daily query counts
and revenue reconciled, and both idle replays retained files and snapshot IDs.

| Orders | Runner seconds | Largest Spark container peak bytes | Cumulative files | Cumulative file bytes | Revenue cents |
| --- | --- | --- | --- | --- | --- |
| 100 | 95.007 | 1182531584 | 11 | 62248 | 278410 |
| 1000 | 95.123 | 1098194944 | 22 | 276044 | 2733643 |

These are single local observations with shared warm history, not a volume scaling
law. Local quality checks also ran concurrently on the same host. They do not
prove that ten times the data has equal cost or lower memory in general.

Retained report: `artifacts/pipeline-profile-demo-20261012-v1/acceptance/profile.json`.
SHA256: `eb944ad4aab009aad606ea1a8da9f2821d2b36da0e86631cfcc5c799fa2fe9b8`.
The parent `demo.json`, stage logs and all volumes remain local diagnostic evidence,
not shipped release assets. Fresh one-command quality verification completed all
eight commands: 1060 passing default tests, 18 expected opt-in skips, 91.70%
coverage and three actual Docker ownership/timeout/Spark-memory smokes. Evidence:
`artifacts/local-pipeline-profile-demo-20261012-v2/`.

## Read-only inventory regression, 2026-10-13

On the retained `lakehouse-ops-scale-query-v1` stack, the old query omitted one
customer-history data file (4850 bytes) and three snapshot IDs. The corrected
query returns all 14 scenario tables, including that history. The other 13
tables' file inventories and snapshot IDs were unchanged. No pipeline stage or
warehouse write was executed for this check. Local before/after evidence:
`artifacts/history-inventory/before-live.log` and `after-live.log`.
This validates inventory completeness, not a new multi-volume performance run
or completion of the audit's repeated benchmarking requirements.

## Full-snapshot execution, 2026-10-13

One fresh scoped project processed 1k then 10k orders through the normal runner's
nine stages. Each sample retained eight observed Spark memory reports; detail,
batch-daily and current-daily SQL matched the independent input oracle, followed
by a verified idle replay.

| Orders | Runner seconds | Captured revenue cents | Idle replay |
| --- | --- | --- | --- |
| 1000 | 166.019 | 2733643 | verified |
| 10000 | 162.358 | 27461573 | verified |

This is one functional acceptance series, not the audit's three comparable
benchmark repeats. The second sample reused the stack and prior history; local
quality checks ran concurrently during the first. Neither timing establishes a
scaling law. Both whole-stack memory observations were `partial`: a short-lived
stage container could disappear between project discovery and Docker statistics.
They are not accepted as complete-stack memory evidence. This retained run also
predates the history-inventory correction, so its file totals exclude
`gold.dim_customers_scd2` and cannot establish complete warehouse storage cost.
Repeat measurements with corrected inventory and reliable observation before
making optimization or capacity claims. No crash-recovery drill was performed.

Retained report:
`artifacts/full-snapshot-profile-20261013-v1/acceptance/profile.json`.
SHA256: `a1187bc24205e7b047d146a3611dba493a95def710dcec27b721afc66d985011`.
The scoped project was
`lakehouse-ops-scale-demo-6e22395be4c64603a68e5d84570a2c65`; its parent lifecycle
report and stage logs remain local evidence, not release assets. Production-path
proof ran against base `c35e63e` plus this increment, before later inventory,
scheduled-cycle and empty-network cleanup changes.
