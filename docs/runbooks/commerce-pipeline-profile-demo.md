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

The child runs two chronological commerce batches through all eight production
stages. Each retains seven observed Spark container memory reports, stage,
verification and checkpoint timings, cumulative current Iceberg files/bytes, and
batch-scoped Trino detail/daily query statistics. Query order count and revenue must
reconcile. Each batch then runs an idle replay with unchanged file/snapshot inventories.

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
One detail query followed by one daily query does not establish throughput,
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
