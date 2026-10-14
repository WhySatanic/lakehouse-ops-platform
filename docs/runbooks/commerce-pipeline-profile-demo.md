# One-command full commerce pipeline measurement

With Docker running and the reference images already available, from the checkout:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile_demo `
  --output artifacts/commerce-pipeline-profile-demo --orders 100 1000
```

Use a new empty output directory. The optional volumes must be ascending integers
between 100 and 10000. The default is 100 and 1000. Invalid volumes, a missing
memory overlay or an unrelated checkout are rejected before services start.

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
