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
