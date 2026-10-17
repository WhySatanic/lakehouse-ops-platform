# Bounded customer history and retained warehouse experiment

The separately [audited 50-snapshot observation](customer-history-50-observation.md)
completed on October 16. Its source, resources, 500000 history versions and full
retained payload are recorded separately from the four-snapshot baseline below.

A separate [local 100000-customer observation](customer-dimension-100k-observation.md)
completed two changing-attribute snapshots with 100 orders each. It measures the
dimension-cardinality axis, independently of the history-depth workflow below.

The separate `Customer history experiment` workflow uses one fresh GitHub runner
and one uniquely named `lakehouse-ops-scale-history-*` stack. It runs for pull requests
changing its workflow or experiment tests, and can be dispatched manually once the
workflow is present on the default branch. Manual dispatch accepts `snapshots=4`
(default), `snapshots=50` or `snapshots=100`; pull requests retain the four-snapshot baseline.
Ordinary CI and its mandatory gates
are unchanged. This experiment is not a release candidate or capacity certification.

Inputs are fixed: 100 canonical orders, 10000 customers per chronological full snapshot,
four February 1..4 snapshots, customer revisions `0,1,1,2`. The existing production
profile runs all nine normal stages and verifies independent order/revenue input
oracles, every customer's SCD2 periods/names/current flags and idle checkpoint replay.
Expected history totals are 10000, 20000, 20000 and 30000, with 10000 current each time.
These are acceptance oracles; a queued workflow is not an observed result.

The profile enables both existing warehouse inventory and retained-reference observers.
The test bootstraps the existing listing-only identity inside its own MinIO before
the profile starts. Writers keep their existing credentials; observer policy is not
expanded. Trino current/retained references are reconciled with versioned S3 payloads
after every sample, with reference changes around listing rejected by the profile.
Metadata and unselected references are not classified as orphan, and payload bytes
do not establish physical disk usage or billing. Writers and maintenance are quiescent
during each observation; multiple requests are not one atomic distributed snapshot.

Images are built/preloaded before the isolated experiment. Runner CPU, memory, kernel,
Docker information and checkout SHA are saved separately. This is fresh services/catalog
with locally available images, not a cold image download or cleared host cache.
The default child has a 1800-second total limit and the existing 300-second stage budgets.
The default job is bounded to 60 minutes including build and upload. Timeout, memory failure,
partial history or missing observations are failed evidence, not successful samples.

## Audit downloaded extended evidence

The read-only [depth auditor](../evidence/audit-customer-history.py) accepts the
two explicitly admitted experiment identities: 50 snapshots at source
`d53976d2378ed578e7fb19df18de10c8436070a7` (run `38922050656`), and 100 at source
`6459ed9595b2a387c7ff60f36727484160efedaa` (run `38969869281`). Use the extracted
artifact directory containing `customer-history-ci/` and `customer-history-runner/`:

```powershell
uv run --frozen python docs/evidence/audit-customer-history.py artifacts/downloaded-history
uv run --frozen python docs/evidence/audit-customer-history.py artifacts/downloaded-history --snapshots 100
```

The default remains the actual 50-snapshot identity. Other depths and a different
source are refused, including internally matching source files with an unapproved
SHA. Run without Python's `-O` option because verification uses assertions.
The original hash-bound `audit-customer-history-50.py` remains unchanged.

Successful verification requires every snapshot, all manifest/input hashes, literal
customer IDs/names/periods/current flags, detail/daily/current revenue, all nine
stages, completed checkpoint entries and no active intent. The same storage-group,
referenced-object/size, memory-observation and idle file/snapshot checks apply at
both depths. A partial or still-running report fails verification; retain its raw
diagnostics separately instead of treating its completed prefix as a full run.

The new auditor rechecked all 50 actual samples and left all 665 raw files unchanged.
Its 100-snapshot tests use synthetic reports and input files. Corruption tests reach
the last snapshot and also reject missing checkpoint state and unapproved source.
This is tool compatibility evidence, **not an observed 100-snapshot Spark run**.
Confirm the Actions run/artifact identity and immutable checkout SHA independently
before interpreting an audit result; JSON alone does not authenticate its origin.

## Extended workload budgets

| Snapshots | Customer revisions | Profile child limit | CI measurement step | Whole job |
|---|---|---:|---:|---:|
| 4 | 0,1,1,2 | 30 min | 38 min | 60 min |
| 50 | 0 through 49 | 180 min | 190 min | 220 min |
| 100 | 0 through 99 | 325 min | 335 min | 360 min |

The 100-snapshot option is an admission with a fixed resource budget, not a measured
success. The [admission/compatibility evidence](../evidence/customer-history-100-budget-2026-10-16.json)
records test-first boundary checks and an actual four-snapshot regression run, not
an actual 100-snapshot result. The observed 50-snapshot profile took 162.495 minutes on one four-CPU hosted
runner. Simply doubling that observation gives 324.990 minutes, almost the entire
325-minute child limit; history growth, runner variation and inventory overhead can
make the 100-snapshot attempt exceed it. Do not present that extrapolation as a forecast
or increase limits indefinitely to force a green result. A budget refusal is a useful
result when the retained partial profile says exactly which batches completed.

The measurement step has ten minutes beyond the profile child for service startup,
observer setup, logs and owned cleanup. The whole-job limit additionally reserves
25 minutes for image setup (bounded build15/pull5), runner setup and upload. Existing
per-stage 300-second limits are unchanged. Child timeout is caught outside the profile:
the experiment writes failed experiment.json, preserves the partial profile/checkpoint,
attempts scoped cleanup and re-raises the timeout. It never summarizes partial batches
as a successful 100-snapshot result. A hard workflow/runner termination may prevent
finally/upload; the timeout hierarchy reduces that risk and is not a guarantee.

After the workflow version has merged, dispatch a single attempt when no experiment
is active on main:

```bash
gh workflow run customer-history-experiment.yml \
  --repo WhySatanic/lakehouse-ops-platform --ref main -f snapshots=100
```

Inspect its status before dispatching again. The concurrency group queues rather than
cancels an existing run; it is not duplicate suppression. Ordinary PR validation still
uses four snapshots. Do not pass a 100-snapshot artifact to the source-bound
`audit-customer-history-50.py`: that audit deliberately verifies only the recorded
50-snapshot run. Audit an actual 100-snapshot result separately against its own source,
one million expected versions, 10000 current customers, checkpoint and raw inventories.

Download the `customer-history-experiment` artifact from the workflow run. It retains
`customer-history-ci/experiment.json`, raw `profile/profile.json` with its SHA256,
source fixtures/manifests, SQL/results/query IDs, runner and memory reports, retained
object/reference inventories, checkpoint and service logs. The outer report is ready
only after all requested sample and storage checks and successful owned cleanup. Partial
reports and logs upload even on failure. Verify report source SHA against the workflow
checkout and PR head/merge parents before citing results.

The test uses scoped Compose down and validates ownership of the empty network.
It does not delete volumes, images, unrelated containers or host data. GitHub runner
disposal remains external: raw JSON artifacts are evidence, not a backup of warehouse
volumes. Repeat on independent fresh stacks for comparisons; one run does not prove
performance improvement, general capacity at 50/100 snapshots, commit-time crash recovery or
distributed exactly-once processing.

## Observed baseline, 2026-10-16

[CI run 38859196975](https://github.com/WhySatanic/lakehouse-ops-platform/actions/runs/38859196975)
and three sequential fresh local stacks completed the workload above. The
[recorded evidence](../evidence/customer-history-baseline-2026-10-16.json) contains
source/report hashes, input manifest hashes, per-stage times, query IDs/statistics,
memory observations and reconciled storage groups. CI checkout ecce85a8727443a45ac95bc532bbde1142fe8bdb
has verified parents main d3462a2 and experiment head a977142; all local rounds used
the clean a977142 checkout. These observations belong to those sources.

All four runs verified history totals 10000/20000/20000/30000 and 10000 current
customers per sample. Every customer had periods [Feb1,Feb2), [Feb2,Feb4),
[Feb4,open). SCD2 snapshot counts were 1/2/2/3; the repeated revision preserved the
same snapshot set between samples two and three. All manifests were byte-identical
across runs. Independent raw checks repeated fixture hashes, literal period/count
SQL, revenue/order inputs, checkpoint hashes, compute-free idle and S3/reference
reconciliation. Each local cleanup preserved six volumes and all seven preexisting
foreign container IDs, with no owned containers/network remaining.

| Snapshot | History rows | Local median runner, s | Local min to max runner, s | Local min to max warehouse payload, B |
|---|---:|---:|---:|---:|
| 1 | 10000 | 113.182 | 105.218 to 117.870 | 1964833 to 1964853 |
| 2 | 20000 | 104.159 | 103.283 to 116.951 | 4745077 to 4745116 |
| 3 | 20000 | 105.192 | 102.878 to 109.459 | 5924745 to 5924811 |
| 4 | 30000 | 108.609 | 103.781 to 110.594 | 8961378 to 8961432 |

Local sum of four runner totals: 415.161, 437.395, 448.621 s; median 437.395 s.

The final CI warehouse payload was 8961468 B: current data 6126036 B,
retained-only data 1846021 B and metadata 989411 B. Delete-reference and S3 noncurrent
payload bytes were zero. Current-only accounting would miss 2835432 B. Current and
retained data sizes matched all local rounds; metadata bytes varied. Matching file
sizes and SQL results are not a content-hash audit of Parquet payloads.

Local Docker Linux 29.8.2/cgroup2 had 12 CPU and 16637530112 B RAM under Windows;
seven other containers were active. Images/source were the same for all three
rounds; caches were not cleared. CI had 4 CPU and 16766414848 B RAM. Exclude CI from
local medians. Runner timing includes stage launch/SQL/IO and excludes subsequent
profile observers. Memory is sampled, not an exact continuous host peak. This is
n=3 local baseline, not an optimization result, isolated capacity test or proof of
50/100-batch behavior. A bounded history-depth experiment remains the next step.

To reproduce, run the integration experiment above on a fresh owned stack, retain
the raw artifact, compare its source/manifest/report hashes and independently check
the expected rows, checkpoint and storage sums. Local raw files are operator evidence;
the linked CI artifact is the published raw run. Successful measurement does not
replace the five mandatory exact-head checks required for merging.

## Manual 50-snapshot CI experiment

Dispatch the existing workflow with the bounded depth option:

```text
gh workflow run customer-history-experiment.yml --ref main -f snapshots=50
```

This uses 100 canonical orders and 10000 customers per chronological full snapshot,
beginning February 1, 2026, with revisions 0 through 49. Production profile oracles
check every customer's exact daily periods/names/current flags, order/revenue
inputs, checkpoint and idle replay; storage/reference observations run after every
sample. Expected history is 10000 times the snapshot number, ending at 500000 rows
with 10000 current customers. It uses the same owned setup and cleanup as the
four-snapshot baseline, a 10800-second profile limit, a 190-minute measurement-step
limit and a 220-minute whole-job limit. Startup/log/cleanup budgets remain bounded.
The long job is opt-in and does not replace or expand ordinary mandatory CI.

Only 4 and 50 are accepted, before output creation or Docker startup. The 50 mode
requires all 50 ready samples; the 32-snapshot prefix below, a running report,
wrong final row counts or failed cleanup cannot produce ready outer evidence.
Upload still runs after failure while the runner is available. Forced host/process
termination can prevent finalization/upload and is not made recoverable by this
wrapper. Record an actually completed artifact before claiming 50-snapshot support
is measured; offline wrapper tests do not satisfy that acceptance criterion.

## Interrupted 50-snapshot attempt, 2026-10-16

A local attempt requested 10000 customers, 100 canonical orders, 50 chronological
full snapshots beginning 2026-02-01 and revisions 0 through 49. Source was
`4902dc1aa8f9b0a1db51d45a2294b30844cde543`; pinned image inspections and operator/report hashes are
preserved with the [partial evidence](../evidence/customer-history-depth-interrupted-2026-10-16.json).
It used a fresh owned five-service stack with listing-only inventory credentials,
the production nine-stage runner, both storage observers and a 10800-second child
budget. The four-snapshot CI workflow above still has its original fixed inputs.

The attempt did **not** complete. Only 32 full observations and 32 checkpoint entries
were independently verified. The last observed state had 320000 history rows,
10000 current customers and 32 SCD2 snapshots. Every completed observation verified
all customer IDs/names, chronological half-open daily periods, row/current counts,
100 orders, captured revenue 277225 cents, compute-free idle and reconciled storage.
The independently checked stored SQL results do not establish current live state
after interruption. Raw fixtures/manifests, SQL/results, logs and inventories remain
local operator evidence, not an uploaded CI artifact or a volume backup.

| Completed snapshot | History rows | Warehouse payload, B | Runner, s |
|---|---:|---:|---:|
| 1 | 10000 | 1964834 | 110.366 |
| 4 | 40000 | 10950517 | 105.380 |
| 8 | 80000 | 23869816 | 111.020 |
| 16 | 160000 | 50806948 | 107.777 |
| 24 | 240000 | 78825393 | 114.272 |
| 32 | 320000 | 107959234 | 121.238 |

At observation 32, current referenced data was 55092023 B, retained-only data
36799870 B and metadata 16067341 B, totaling 107959234 B. Delete-reference and S3
noncurrent payloads were zero. Current-only accounting would omit 52867211 B.
These are versioned object payload bytes, not physical disk consumption/billing;
metadata and objects outside selected file references are not proven orphans.
Timings include process/JVM/SQL/IO; this single prefix is neither a matched A/B
comparison nor a capacity limit or evidence of optimization.

Sample 33 has fixtures and a Spark log ending with executor rejection at
08:06:32 UTC, but no completed sample result. The operator session returned
1073807364. At inspection the operator/profile processes were absent, the Docker
Linux engine pipe was absent and the Docker Windows service was stopped. The
cause of interruption remains unknown: these observations do not establish OOM,
an application deadline, or failure inside an Iceberg commit. Reports still saying
`running` are stale; no recovery/replay of sample 33 has been verified.

Owned teardown and preservation of the seven original foreign container IDs could
not be verified while Docker was unavailable. No broad Docker restart, prune or
volume deletion was attempted. Before another local experiment, inspect this exact
project after Docker is available and confirm that no writer is active:
`lakehouse-ops-scale-depth-history-8756af0fa1094b1dbf30f1ada8bc8ea0`.
Collect its service/task/exit-state logs before any owned cleanup. Scope cleanup
only to that confirmed project and an empty owned network; preserve volumes and
all foreign services. Then repeat the entire workload on a fresh owned project,
with the memory overlay and listing observer used above, retaining provenance:

```text
uv run --frozen python -m lakehouse_ops.commerce_pipeline_profile --output artifacts/history-depth-repeat --orders 100 100 --customers 10000 --batches 50 --customer-revisions 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 --warehouse-inventory --retained-references --s3-endpoint-url <owned-minio-url> --server <owned-trino-url>
```

This command requires the existing isolated profile setup, owned Compose project
environment and credentials; it is not a standalone stack launcher. Require all
50 completed observations, exact checkpoint manifests, final 500000 history rows
and 10000 current customers, literal periods/names for every snapshot, storage
reconciliation, idle replay and verified owned cleanup before marking it ready.
Do not restart against the partial catalog by bypassing the empty-catalog guard.
The 50/100-snapshot acceptance criteria remain open.

## Measured validation candidate

The [SCD2 validation experiment](scd2-validation-experiment.md) compares a fused
invariant check with the existing job in three matched replay pairs. Fewer Spark
jobs did not meet the end-to-end adoption threshold; the production job is unchanged.


## Profile controller deadline on POSIX

The experiment starts its profile controller in a new POSIX session. On the
existing profile deadline it sends SIGKILL to that owned process group before
reaping its leader, with a separate maximum 10-second reap wait. Inherited CLI
children cannot continue launching subsequent stages after a successful group
signal. The failed profile step records `process_group`, including signal/reap
errors, while preserving the original `TimeoutExpired` and cleanup path.

This covers the controller and descendants that remain in its process group.
Docker daemon tasks and deliberately detached processes are outside this boundary.
A signal request and reaped leader do not prove every descendant has exited, a
Docker stage has stopped, or whether its Iceberg commit succeeded. Inspect the
owned stage journal/container evidence before deciding recovery. A hard workflow
termination can prevent the wrapper's final report. Windows retains the previous
subprocess timeout behavior without this process-tree guarantee.

A [short Linux regression](../evidence/history-profile-process-group-2026-10-17.json)
records a child continuing after the original parent-only timeout, then the same
child being prevented from writing its continuation marker with group cancellation.
Reproduce in Linux with `uv run --frozen pytest tests/test_history_profile_process.py`
or run `python tests/integration/exercise_history_profile_process.py --output <new-directory>`.
The regression performs no Spark job, Docker stage launch or catalog write.
