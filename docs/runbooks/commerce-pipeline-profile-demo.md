# One-command full commerce pipeline measurement

With Docker running and the reference images already available, from the checkout:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile_demo `
  --output artifacts/commerce-pipeline-profile-demo --orders 100 1000
```

Use a new empty output directory. The optional volumes must be ascending or equal integers
between 100 and 100000. The default is 100 and 1000. Invalid volumes, a missing
memory overlay or an unrelated checkout are rejected before services start.

The opt-in `--orders 10000 100000` comparison uses the same runner, stage and
whole-run budgets, Spark limits and scoped teardown. It does not add services or
raise container memory limits. Check available Docker VM memory first and run
only one measurement stack at a time; a permitted input size is not a capacity
guarantee. Failed or partial runs remain diagnostic evidence, not measurements
to include in a successful comparison. Keep the default small run for setup.

## Select customer cardinality and snapshot history

The same isolated lifecycle can pass the existing profile's workload options to its
single measurement child. For a bounded changing-attribute experiment:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile_demo `
  --output artifacts/customer-history-10000-round-1 --orders 100 100 `
  --customers 10000 --batches 4 --customer-revisions 0 1 1 2
```

`--customers` accepts 2..100000, `--batches` accepts 2..100, and an explicit revision
schedule requires one integer 0..10000 per batch. Validation runs before creating
output or contacting Docker. Recovery demos reject these measurement-only options.
The first snapshot uses the first order volume; later snapshots repeat the second.
All snapshots have chronological source times. Omitting the revision schedule keeps
customer attributes stable; batch history alone is not changing-attribute SCD2 growth.

With the example schedule the child verifies expected SCD2 row totals
10000/20000/20000/30000 and 10000 current rows after each snapshot, including the
repeated unchanged revision. These are expected oracles, not measured results from
this documentation. The child retains actual SQL, results, query IDs/statistics,
runner timings, memory, checkpoint and idle checks in `acceptance/profile.json` and
adjacent evidence. It still runs the normal nine stages and full-snapshot rules.

An explicit workload is recorded in `demo.json`; changing attributes and multiple
snapshots use distinct scopes from the default fresh/warm pair. With no new options,
the original two-volume command, report shape and scope remain unchanged. Startup,
child and teardown are still separately timed. The whole-child timeout stays 3600
seconds, with the same stage budgets and resource limits; accepting 100 batches
does not promise they fit in one hour or memory. A timeout or failed history oracle
keeps failed/partial child files and still attempts only the owned stack's teardown.

This wrapper does not enable the standalone profile's warehouse inventory/reference
flags or provision an observer identity. Use the existing standalone profile and
its documented observer setup for retained-storage accounting. A successful run of
the example alone is not complete retained warehouse cost evidence.

## Default fresh/warm comparison

For a matched fresh/warm pair, pass the same volume twice:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile_demo `
  --output artifacts/profile-1k-matched-round-1 --orders 1000 1000
```

Repeat with new output directories for at least three independent pairs at each
volume, then do the same at 10000 and, with sufficient resources, 100000.
Each invocation starts fresh owned services and
an empty commerce catalog; its second sample keeps those services and exactly one
completed prior batch. Both samples run all nine normal runner stages, rather than
timing an idle replay as a warm load. Two chronological full-snapshot manifests have
distinct batch identities. At equal volumes, IDs, amounts, reference dimensions,
quality cases and row counts match; order/payment timestamps shift forward one day
with the second snapshot. Source JSONL is therefore not byte-identical. Row/revenue
oracles, file/snapshot-stable idle checks and project-only memory sampling are unchanged.

Runner `durations_seconds.planning` includes queue selection, persisted active-batch
intent and execution-option resolution, including their S3 rediscovery and the
selected-batch journal event. The first stage starts after that work. Before this timing
correction (including v1.28.1), intent/resolution time was incorrectly charged to
`bronze-input-sync`; planning included only the initial queue selection. Historical
total-run measurements remain valid, but do not directly compare their planning or
first-stage split with corrected reports. This changes attribution, not throughput,
stage order, execution budget or checkpoint semantics; no speedup is claimed.

The private profile report records `catalog_state` and `prior_completed_batches`
for every sample. Equal volumes use `same_volume_fresh_then_warm_catalog` in the
child report and `same_volume_fresh_then_warm_reference_stack` in the demo report;
ascending volumes retain the previous scopes and default behavior. The standalone
profile requires an empty commerce catalog but does not itself start services.

Here "fresh" (or cold) means fresh scoped services/catalog with locally available
images, not a cold image download or a flushed host/OS cache. "Warm" means those
same services with one prior batch, not a reused Spark session: each Spark stage
still starts a new process. Runner times exclude stack startup and the additional
post-run SQL/inventory checks; startup is separately timed in `demo.json`. The
first/second order is fixed and retained history is a deliberate difference. These
pairs do not isolate JVM/cache effects from one-batch history, measure arbitrary
history growth, or establish production capacity. Keep study/foreign projects
unchanged, run pairs serially, and record competing host load rather than claiming
an otherwise idle machine. Warehouse volumes remain retained after scoped teardown.

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

## Three post-cleanup baseline repeats, 2026-10-14

Local source `935e1d61a33cd487e6bbc43b47190a94721415c5` combines main
`a9818d0` with the finished memory-observer fix from PR #233. It is not a release
revision. Three serial invocations of the same command used `--orders 1000 10000`
and fresh output directories under `artifacts/audit-repeat-eof-fixed-20261014`.
All six batches completed the same nine stages, eight Spark memory observations,
independent input/SQL oracles and snapshot/file-stable idle replay. Every project
memory report is `observed`; completed projects have no remaining containers or
network, and their named volumes remain. Earlier partial reports stay partial.

Docker/WSL2 reports 15867 MiB total VM memory. The study SQL stack stayed running
throughout, approximately 0.88 GiB of container memory. Windows free physical
memory before rounds was 7772, 6286 and 6173 MiB; VM available memory was 14149,
14051 and 14047 MiB. Other desktop applications remained active. No concurrent
local tests, probes, second measurement stack, builds or image pulls ran during
these three repeats. This is a shared-host baseline, not isolated capacity.

Each 1k sample uses fresh services/catalog with already-local warm images; each
10k sample uses those warmed services and one earlier historical batch. These
are distinct startup states, not separately controlled cold/warm runs of each
volume. Timing differences between volumes must not be interpreted as scaling.

| Round | Orders | Runner seconds | Project peak MiB | Largest Spark peak MiB | Current files | Current file bytes |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 1000 | 132.962 | 3654.05 | 1050.56 | 29 | 134375 |
| 2 | 1000 | 131.545 | 3612.32 | 961.30 | 29 | 134375 |
| 3 | 1000 | 130.753 | 3599.70 | 962.11 | 29 | 134375 |
| 1 | 10000 | 130.146 | 3761.86 | 1020.06 | 31 | 143158 |
| 2 | 10000 | 128.857 | 3805.69 | 1056.23 | 31 | 143158 |
| 3 | 10000 | 126.983 | 3765.00 | 1056.38 | 31 | 143158 |

Median runner times are 131.545 / 128.857 seconds; medians of observed project
peaks are 3612.32 / 3765.00 MiB for 1k / 10k. Project peak is a sampled simultaneous
sum, not a continuous maximum or host/VM RSS. Spark peaks describe individual
container lifetimes and must not be summed as a platform peak.

| Stage | 1k median seconds | 10k median seconds |
|---|---:|---:|
| Selected input sync | 1.370 | 1.274 |
| Bronze | 19.240 | 18.092 |
| Payment silver | 15.616 | 15.024 |
| Order silver | 16.058 | 16.087 |
| Product silver | 13.463 | 13.653 |
| Customer silver | 14.031 | 14.688 |
| Customer SCD2 | 12.507 | 12.079 |
| Batch daily gold | 18.712 | 18.537 |
| Current daily gold | 18.911 | 19.168 |

Stage times include their Docker/Spark startup and execution, not an attribution
of JVM startup alone. Similar total times support investigating fixed overhead,
but do not prove session reuse is safe or identify its exact achievable saving.

Current inventories are identical within each volume: 41 files / 353034 bytes
after 1k; 54 files / 2124936 bytes after 10k, including customer history. These
are cumulative current files, not obsolete files, metadata or total disk usage.
Input, detail, batch-daily and current-daily results agree on 1000 / 10000 orders
and 2733643 / 27461573 captured revenue cents. Idle-child medians are 1.298 / 1.313
seconds with unchanged snapshots/files; this is not injected crash recovery.

The current mart has only 31 daily rows but 29 / 31 files. Its Trino query reads
134375 / 143158 physical bytes and processes 136405 / 145328 bytes in every round.
Current-query IDs for 1k / 10k:

- Round 1: `20261013_221713_00021_axspn` / `20261013_221943_00106_axspn`.
- Round 2: `20261013_222315_00021_ec8cx` / `20261013_222541_00106_ec8cx`.
- Round 3: `20261013_222908_00021_ddi6m` / `20261013_223135_00106_ddi6m`.

Each query is one ordered observation, not a latency percentile, cache-controlled
SQL benchmark or SLA. The measured file fan-out is the next narrow optimization
target: consolidate the already validated daily aggregate at publication, without
changing source joins, validation, snapshot identity or the single atomic commit.
Before production changes, the extended real Spark/Iceberg oracle failed with
`small current mart has 2 data files` after forcing eight input partitions.
After consolidating the writer input, the same unchanged business/history oracles
passed create/replace, one-file row accounting, snapshot-stable replay/stale input,
removed-day replacement and atomic SCD2 failure boundaries. The matched full-stack
after measurements below establish the bounded file-layout improvement.

Raw profile SHA256 values, one per round's `acceptance/profile.json`:

- Round 1: `6f19153d798e17dff991c8767f550ff9defe01a19f71c7563f9c29d50c9a9ed5`.
- Round 2: `762518cb8844c3f14fae36ba0eb92235b94729bef126d595c2725ffcc54fe237`.
- Round 3: `29ee9836971221714c49576d745d38e4dd14ebd205ff2ed9569f98ea215ae2e7`.

No 100k capacity, longer-history cost, forced-exit recovery at these volumes,
complete per-volume cold/warm comparison or release readiness is established.

## Matched compact-current-mart measurements, 2026-10-14

Three serial after rounds used source `3f5e6a4863458fa128a937a4a9f053e0e80a5eae`
and the same command, volumes, local images, input identities, runner, memory
observer and SQL oracles as the baseline above. The only production change is
`coalesce(1)` on the already cached and validated daily candidate at create and
replacement writes. Raw input, joins and aggregation remain parallel. A single
Iceberg commit still publishes each state; replay and stale input do not rewrite it.

The same study SQL services stayed active, approximately 0.87 GiB combined.
Windows free physical memory before rounds was 7150, 5143 and 5330 MiB; VM
available memory was 14175, 14081 and 14110 MiB. No concurrent local tests, probes,
builds or own stacks ran. The host was not isolated and its available memory
varied, so small timing/memory differences are not controlled causal estimates.

| Round | Orders | Runner seconds | Project peak MiB | Largest Spark peak MiB | Current files | Current file bytes |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 1000 | 135.200 | 3536.12 | 953.52 | 1 | 5467 |
| 2 | 1000 | 141.998 | 3627.09 | 1001.85 | 1 | 5467 |
| 3 | 1000 | 128.137 | 3606.53 | 941.32 | 1 | 5467 |
| 1 | 10000 | 130.942 | 3793.16 | 1016.57 | 1 | 5494 |
| 2 | 10000 | 128.509 | 3705.48 | 1042.32 | 1 | 5494 |
| 3 | 10000 | 127.359 | 3926.16 | 1128.31 | 1 | 5494 |

| Metric | 1k before / after | 10k before / after |
|---|---:|---:|
| Current mart files | 29 / 1 | 31 / 1 |
| Current physical query bytes | 134375 / 5467 | 143158 / 5494 |
| Current processed query bytes | 136405 / 5537 | 145328 / 5564 |
| Cumulative current files | 41 / 13 | 54 / 24 |
| Cumulative current file bytes | 353034 / 224126 | 2124936 / 1987272 |
| Median runner seconds | 131.545 / 135.200 | 128.857 / 128.509 |
| Median current-gold stage seconds | 18.911 / 17.859 | 19.168 / 17.776 |
| Median project peak MiB | 3612.32 / 3606.53 | 3765.00 / 3793.16 |
| Median largest Spark peak MiB | 962.11 / 953.52 | 1056.23 / 1042.32 |
| Median idle-child seconds | 1.298 / 1.653 | 1.313 / 1.573 |

All three rounds reduced current-mart physical bytes by 95.932% / 96.162% at
unchanged 31 daily rows, input counts and revenue. Every non-current-mart file
inventory entry is identical to its baseline counterpart. All nine stages,
eight Spark reports, project memory observations and stable idle replay passed.
After rounds leave zero project containers/networks and retain six named volumes
each. This is measured elimination of tiny-file fan-out, not total disk reclamation.

After-stage medians, in the baseline table's order, are 1.414, 19.791, 15.777,
16.808, 14.272, 14.735, 13.248, 19.184, 17.859 seconds for 1k, and 1.399,
18.929, 15.380, 16.716, 13.732, 14.062, 11.600, 18.059, 17.776 for 10k.
Total runner ranges overlap: 1k before 130.753-132.962 / after 128.137-141.998;
10k before 126.983-130.146 / after 127.359-130.942 seconds. No consistent
whole-pipeline speedup or memory saving is established. Current-query wall times
were 259/461/309 ms before and 10/16/13 ms after at 1k, 247/306/237 ms before
and 7/7/8 ms after at 10k. These single ordered observations remain explicitly
not a cache-controlled latency benchmark, percentile or SLA.

After current-query IDs for 1k / 10k:

- Round 1: `20261013_230017_00021_p4j9q` / `20261013_230247_00106_p4j9q`.
- Round 2: `20261013_230638_00021_p9f69` / `20261013_230905_00106_p9f69`.
- Round 3: `20261013_231232_00021_bsqxs` / `20261013_231459_00106_bsqxs`.

Raw reports remain under `artifacts/current-mart-layout-after-20261014`, with
SHA256 values for each round's `acceptance/profile.json`:

- Round 1: `e1cb823fae6dfaa96fa4aa7fcf4b4abe385788add623cf63816a90b97214265a`.
- Round 2: `898f92bf4d7da0bb846c62c7b355e0a8f8b73ee289fd614ef9e5a0e02ead4d9a`.
- Round 3: `ada28230c1dadc47c534804acf6102acdab536feab56bbe1513cb7823b5d2c2a`.

Trade-off: final publication uses one Spark task for the small daily aggregate,
not an arbitrary large raw dataset. This is tested for the supported scenario,
not a large-mart throughput guarantee; Iceberg can still roll files by size.
No schema, configuration, CLI or checkpoint migration is required. Existing
fragmented snapshots are not rewritten by replay. A subsequent newer full snapshot
gets the compact layout. Rollback restores the prior writer distribution for
future publications without deleting existing files or warehouse volumes. The
100k, longer-history, independent cold/warm and crash-recovery gaps remain open.

## Matched fresh/warm pairs at 1k and 10k

On 2026-10-14 (Europe/Moscow), three serial pairs per volume ran at source
`8f6266b348934b8b2d1c2a3701c6a0769fcb652d`, based on main `04dd322` with the compact
current-mart writer. Each pair used the original command above with `--orders 1000
1000` or `--orders 10000 10000`, a fresh output directory and a fresh owned stack.
The later rebase onto `7f24a1b` changes only the demo's already-reviewed upper bound
from 10000 to 100000; the measured runner, profile child, fixture generator, Spark
jobs and Compose configuration are unchanged. These are not matched 100k results.

All twelve loads completed the normal nine stages and eight observed Spark memory
records. Every project memory report was `observed`, with no sampling errors. Detail,
batch daily and current daily SQL matched the independent input and literal totals:
1000 orders / 2733643 cents, or 10000 orders / 27461573 cents. Each load's idle rerun
changed no files or snapshots. Fresh/warm batch IDs are `84cdb288a85b0a3f` /
`9c14d7c692dba5ba` at 1k and `18ae3bdf50f64d6c` / `944f877f4c6a3e18` at 10k.
Full-snapshot dates are February 1/2, with the timestamp difference described above.

Raw runner seconds (excluding stack startup and post-run profile queries):

| Pair | 1k fresh | 1k warm | 10k fresh | 10k warm |
|---|---:|---:|---:|---:|
| 1 | 133.485372 | 127.994144 | 132.262903 | 135.913095 |
| 2 | 128.583505 | 129.116945 | 134.630255 | 132.065582 |
| 3 | 137.428028 | 131.270202 | 134.254008 | 129.480372 |
| Median | 133.485372 | 129.116945 | 134.254008 | 132.065582 |

Medians across the three comparable observations per state:

| Observation | 1k fresh | 1k warm | 10k fresh | 10k warm |
|---|---:|---:|---:|---:|
| Observed project peak MiB | 3576.56 | 3672.09 | 3686.01 | 3770.93 |
| Largest individual Spark peak MiB | 998.58 | 1012.85 | 1051.62 | 1024.65 |
| Idle child seconds | 2.058343 | 1.956578 | 1.917178 | 1.946245 |
| Current data files | 13 | 24 | 13 | 24 |
| Current data-file bytes | 224126 | 437910 | 1774799 | 3537926 |

"Current data files" here means the full current inventory across the fifteen
commerce/SCD2 tables, not just the current mart or retained obsolete/metadata files.
The current mart remains one file: fresh/warm 5467/5464 bytes at 1k, 5486/5494 at 10k.
File counts/bytes above were identical in all three observations for each state.
History adds batch-scoped files, not cumulative current revenue; two batches do not
establish longer-history growth or maintenance cost. Spark values use `peak_bytes`
from cgroup reports; project values use the maximum sampled simultaneous sum.

Per-stage median seconds include each Compose/Spark process startup:

| Stage | 1k fresh | 1k warm | 10k fresh | 10k warm |
|---|---:|---:|---:|---:|
| Input sync | 1.960 | 1.913 | 2.003 | 2.030 |
| Bronze | 18.883 | 17.892 | 20.012 | 18.371 |
| Payment silver | 15.389 | 15.419 | 16.033 | 16.284 |
| Order silver | 16.644 | 16.171 | 16.345 | 16.924 |
| Product silver | 13.671 | 14.419 | 14.138 | 14.339 |
| Customer silver | 14.772 | 14.695 | 14.656 | 15.032 |
| Customer SCD2 | 13.638 | 12.474 | 12.821 | 12.600 |
| Batch daily gold | 19.440 | 18.965 | 18.824 | 19.838 |
| Current daily gold | 16.889 | 17.417 | 17.207 | 18.040 |

Single fixed-order detail/daily/current SQL wall-time medians were 138/11/10 ms
(1k fresh), 41/7/7 (1k warm), 245/11/10 (10k fresh), and 67/8/7 (10k warm).
Detail physical input bytes were respectively 91584, 91579, 866314 and 865517,
identical across repeats within each state. Queries run after runner verification
and inventory reads, not against a guaranteed cold query cache. These are ordered
observations with retained query IDs/stats, not a latency SLA or isolated cache test.
Small shifted-date byte differences are not evidence of a pruning improvement.

Raw reports remain in `artifacts/profile-matched-1k-20261014/round-N/acceptance`
and `artifacts/profile-matched-10k-20261014/round-N/acceptance`; `demo.json` beside
each acceptance directory separately retains startup/teardown timings and ports.
SHA256 for `profile.json` in round order:

- 1k/1: `4ca31206985d4f381822790cd36a748a1f8ad97a71f3419f064b9b95219fd4d5`.
- 1k/2: `6edf5069992029ee2c2915594ee20464adc9ff29687580a6f0cda486114baa27`.
- 1k/3: `e24cde50849b6cc2cafe8267ffc50d5c5ab9f8b47262dcd9cfeaaaf7db3d22b0`.
- 10k/1: `775ebc82eb77baad59b3b61a7bf79c92a6918ad959637b1ab884a8a374dca182`.
- 10k/2: `50600bac41b621e7437493293b8ede5ddab38d073109f364a3be312251a64d00`.
- 10k/3: `72b442aece4f8252ff0853044b6b354e0271a35a1b56d0fa9ef0048872b22cc0`.

Current-mart query IDs, fresh then warm, in round order:

- 1k/1: `20261014_001736_00021_hmzei` / `20261014_002002_00106_hmzei`.
- 1k/2: `20261014_002331_00021_2r87a` / `20261014_002558_00106_2r87a`.
- 1k/3: `20261014_002934_00021_uvgts` / `20261014_003206_00106_uvgts`.
- 10k/1: `20261014_003748_00021_s6e3k` / `20261014_004023_00106_s6e3k`.
- 10k/2: `20261014_004401_00021_wnj2s` / `20261014_004632_00106_wnj2s`.
- 10k/3: `20261014_005008_00021_3uhmt` / `20261014_005235_00106_3uhmt`.

Project suffixes (all `lakehouse-ops-scale-demo-`) were
`8343eae03022460ead04d122b55dfd62`, `df914b2e831743089347e9f0f1bb0d19`,
`ff82332ec72a4a648189e73a9de62b33` at 1k, then
`e621348db8754608bc6a2e5d5dd1b795`, `e52912b87bb74032b28c48eca53588d9`,
`b7c3639b341e4709a3cd7de25d2b9cc3` at 10k. Fresh inspection after every pair
confirmed zero owned containers/networks and six retained warehouse volumes.
The three running study SQL containers kept the same IDs; other projects were not
started/stopped. No global prune, image pull/build or cache flush was performed.

The Docker VM had about 15.5 GiB total memory. Free Windows physical RAM before
the three 1k pairs was 7690388/6228644/6074940 KiB; before 10k it was
6941180/5478384/5314436 KiB. VM available memory before 10k was
14405004/14356560/14338708 KiB. Study SQL services and ordinary host applications
remained competing load; no local tests or second lakehouse measurement ran in
parallel. Images/OS caches were uncontrolled. Warming is not reliably faster
(1k pair 2 and 10k pair 1 were slower warm), and all Spark jobs remain new processes.
This section closes the same-volume comparison gap for 1k/10k under the declared
conditions. The separate 100k results below do not establish arbitrary history
growth, crash recovery, host memory or capacity.
No public CLI/schema/configuration migration is required; ascending/default commands
are unchanged. Rollback can omit equal volumes and use the prior ascending profile.

## Matched fresh/warm pairs at 100k

Three serial pairs on 2026-10-14 used the same command with `--orders 100000 100000`
and new output directories `artifacts/profile-matched-100k-20261014/round-N`.
Measured source was `e3c063a98904cbc5715e281c63ae7145203c1591`; its tracked tree is
identical to merged main `708998b4c7c60d908406e59026ed0316b5befa63`. This is a
new same-volume comparison, not the earlier 10k-fresh/100k-warm baseline or an A/B
claim for the compact writer. No runtime, defaults, schema or release version changes.

All six loads completed nine ordinary runner stages, eight observed Spark peaks,
and error-free project memory sampling. Input, runner gate and detail/daily/current
SQL all agreed on 100000 orders and 273446528 captured cents. The ten customers,
five products, seed and configured anomalies stayed fixed. Each pair retained exactly
two completed batches: `d460d0619585ebc8` then `83279a7dc07032dc`, dated February 1/2.
Only the timestamp shifts; inputs are equivalent business states, not identical bytes.
Each compute-free idle replay retained the fifteen-table file inventory and snapshots.

Runner wall seconds exclude service startup and post-run profiling:

| Pair | Fresh catalog | Warm catalog with one prior batch |
|---|---:|---:|
| 1 | 146.617208 | 140.142985 |
| 2 | 145.308181 | 139.451173 |
| 3 | 146.324542 | 144.727765 |
| Median | 146.324542 | 140.142985 |

Medians across the three observations per state:

| Observation | Fresh | Warm |
|---|---:|---:|
| Sampled whole-project peak MiB | 3700.37 | 3877.02 |
| Largest individual Spark cgroup peak MiB | 1160.06 | 1188.79 |
| Idle child seconds, including startup | 2.247608 | 2.006407 |
| Current data files, all fifteen tables | 13 | 24 |
| Current data-file bytes, all fifteen tables | 17674850 | 35335297 |
| Current-mart files / bytes | 1 / 5536 | 1 / 5531 |

File counts/bytes were identical across all three repeats within each state. The
current mart contains 32 daily rows. These inventories exclude obsolete data and
metadata files. Project memory is the maximum sampled simultaneous container sum,
not the sum of independent lifetime peaks; Docker excludes inactive Linux file
cache. Host applications, Python orchestration and the study stack are not included.

Per-stage median wall seconds include each Compose/Spark process startup:

| Stage | Fresh | Warm |
|---|---:|---:|
| Input sync | 2.149 | 2.147 |
| Bronze | 23.041 | 21.455 |
| Payment silver | 17.471 | 17.633 |
| Order silver | 19.239 | 17.915 |
| Product silver | 14.739 | 14.421 |
| Customer silver | 14.914 | 14.711 |
| Customer SCD2 | 13.436 | 12.735 |
| Batch daily gold | 20.067 | 19.958 |
| Current daily gold | 19.384 | 19.399 |

Single fixed-order detail/daily/current SQL wall medians were 802/12/9 ms fresh,
270/7/8 ms warm. Detail physical input was 776978 bytes in every observation.
Queries follow runner verification and inventory reads; neither a cold query cache,
a repeated query corpus, percentile/SLA nor an isolated cache speedup is established.
The modest runner-time increase from the preceding 1k/10k observations is consistent
with substantial per-stage fixed cost, not proof that eight JVM starts are the sole
bottleneck or that a shared-session implementation has been measured.

Raw `acceptance/profile.json` SHA256 in round order:

- 1: `a473c35505ce52a62fac34547bc5f3a175e4171ce08c9302d9013d883ded4d1c`.
- 2: `67a7aeafaac7511d6c5a97708afcb217e0548740131a5ffe225b9363f5053121`.
- 3: `f2aff26493a9802cc764e561d23754bc3929935e7cbfa65cdce43bed5c18d27a`.

Current-query IDs, fresh / warm:

- 1: `20261014_011032_00021_2daqf` / `20261014_011319_00106_2daqf`.
- 2: `20261014_011705_00021_r27yz` / `20261014_011948_00106_r27yz`.
- 3: `20261014_012340_00021_9mxfu` / `20261014_012629_00106_9mxfu`.

Project suffixes (all `lakehouse-ops-scale-demo-`) were
`17d7904686de46f78182423a0f718aa1`, `ac49ad393b894be8a6bd45166e034099`, and
`16e77190cd8242eb8db0e41c80cb46a9`. Each retained parent `demo.json` records ready
startup/profile/stop, with an absent network. Independent inspection confirmed zero
project containers/networks and six retained named volumes per pair. The three study
SQL containers kept their IDs; no unrelated container, global prune or cache flush
was used. The roughly 15.5 GiB Docker VM, study SQL and normal host applications
remained the environment; no second lakehouse load or local test ran concurrently.

Fresh means new catalog and services, not cold OS/images/VM caches. Warm reuses the
five core services and one completed prior batch, but still starts eight new Spark
processes. This closes the declared matched-volume load measurement at 100k, not
long historical queues, large-load failure/retry cost, full-path release-candidate
proof or production capacity. Keep those remaining audit requirements open.

## Rejected bronze validation aggregation, 2026-10-15

A bounded study at main `27fcd22cbab23aef849cae77b6db3caad406672b` tested replacing
the separate JSONL row-count and required-NULL counts in `prepare_source` with one
aggregation returning both counts. Three serial `--orders 1000 1000` pairs per
implementation reused this exact runner, settings, fixed oracles and stage boundaries.
Fresh still means new services/catalog, not cold OS/images; warm has one prior batch.
All before pairs preceded all after pairs, so this is not randomized/interleaved AB.

| Observation, seconds | Before fresh | Candidate fresh | Before warm | Candidate warm |
|---|---:|---:|---:|---:|
| Bronze round 1 | 17.309 | 16.729 | 15.212 | 14.761 |
| Bronze round 2 | 16.060 | 16.407 | 15.046 | 13.882 |
| Bronze round 3 | 15.666 | 14.495 | 14.600 | 14.032 |
| Bronze median | 16.060 | 16.407 | 15.046 | 14.032 |
| Runner wall median | 114.142 | 107.881 | 110.684 | 105.406 |

The fresh bronze median was 2.16% higher; warm was 6.74% lower with overlapping
ranges. Unchanged stages also improved: fresh daily gold median 15.50 to 14.67 s
and warm payment silver 12.97 to 12.37 s. The total wall-time change therefore
does not establish an attributable pipeline speedup from combining counts. The
candidate was rejected and production code restored. Fewer Python action calls
did not prove fewer physical Spark jobs or a consistent stage-time benefit.

All twelve loads completed nine stages, observed project/eight Spark memory reports,
literal detail/daily/current SQL of 1000 orders and 2733643 cents, identical input
manifest hashes and current-file inventories, two final completed checkpoints and
stable compute-free idle. Fresh/warm files were 13/24, bytes 224126/437910 throughout.
Sampled project peak medians were 3719.70/3773.80 MiB before and 3600.60/3632.68 MiB
after; largest individual Spark lifetime peak medians 1095.07/1047.29 and
951.48/970.91 MiB. These observations are not attributed memory savings or capacity.
The six projects retained six volumes each and zero containers/networks after teardown.
Docker VM total was 16637542400 bytes with 12 CPUs and local reference images;
ordinary host activity/caches were uncontrolled, with no concurrent second stack,
build or heavy local test suite. This adds idle replay evidence, not injected recovery.

Local raw evidence is `artifacts/bronze-validation-scan/{before,after}-N/`, with
per-stage timing, startup/shutdown, SQL IDs, memory, files, checkpoints and journals.
The asserted `comparison.json` SHA256 is
`3895bfab830d30c82bdc9c2a357a75cc1e0f5de1157551bb15ba6fdcc3996b11`;
the retained rejected four-file `candidate-complete.patch` SHA256 is
`a5cad51091c7fa82e401a36dfb1ab1a171e58fbb344628553b78e8423e65c97c`.
All final checkpoint hashes are
`2612e8b2cc5a5d42e9dfe0066d125d1def0c8b3f2c547eb2f110f9882f3737e8`.
This small-volume result does not replace the existing 10k/100k/history/recovery
measurements or prove a shared-session alternative. Only the semantic source-validation
cases are retained in the existing Spark smoke; no action-count requirement,
production optimization, new command/profile, migration or release version is added.
