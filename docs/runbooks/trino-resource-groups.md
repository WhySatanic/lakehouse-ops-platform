# Trino resource groups

The query profile loads a file-backed resource group policy on the coordinator. Every
query enters the `global` root and one workload leaf. The root permits four concurrent
queries. Leaf limits reserve different concurrency and queue budgets:

| Workload | Selector | Hard concurrency | Maximum queued | Scheduling weight |
|---|---|---:|---:|---:|
| Ingestion | user `lakehouse-ingestion` or `lakehouse-ingestion-*` | 1 | 4 | 2 |
| BI | user `lakehouse-bi` or `lakehouse-bi-*` | 2 | 10 | 4 |
| Ad-hoc | catch-all | 1 | 3 | 1 |

The root uses `weighted_fair` scheduling. BI receives more opportunity when multiple
leaves are eligible, while every workload keeps a hard upper bound. The percentages are
soft distributed-memory limits, not reservations: ingestion and BI each use 40%, and
ad-hoc uses 20%.

This follows Trino's
[file resource group manager](https://trino.io/docs/current/admin/resource-groups.html#file-resource-group-manager)
and selector rules. The policy is versioned at
`infra/trino/coordinator/resource-groups.json` and only needs to be mounted on the
coordinator.

## Run the queueing drill

Load the deterministic silver fixture and start the coordinator plus both workers. Then
run the protocol-level exercise from the host:

```bash
mkdir -p artifacts
uv run python tests/integration/exercise_trino_resource_groups.py \
  http://localhost:8080 artifacts/trino-resource-groups-report.json
uv run python tests/integration/check_trino_resource_groups.py \
  artifacts/trino-resource-groups-report.json
```

The exercise submits bounded, cancellable queries with four identities. Ingestion, BI,
and the first ad-hoc query occupy three leaf slots. A second ad-hoc query is submitted
while the leaf hard concurrency is one. A BI inspector reads `system.runtime.queries`
and the check fails unless it observes:

- `global.ingestion` with a running ingestion query;
- `global.bi` with a running BI query;
- `global.adhoc` with one running query and one `QUEUED` query;
- successful cancellation acknowledgement for all four drill queries;
- a successful silver-table query after cleanup.

The report uses schema version `1.0`. It records the effective policy, selector
assignments, observed states, cleanup result, and continuity row count. The strict host
validator rejects missing or inconsistent evidence.

## Tuning and rollback

Queue depth, concurrency, memory percentages, and weights are policy decisions. Change
one variable at a time, repeat the baseline corpus under comparable conditions, and
record both throughput and queue latency. Avoid increasing concurrency merely to remove
queueing; worker CPU, memory, and object-store pressure must support the additional work.

To roll back, restore the previous coordinator mounts and recreate only the coordinator.
Workers and Iceberg data do not require migration. Without a resource group manager,
Trino returns to its default unpartitioned workload behavior.

## Current limits

This policy selects by local test usernames because authentication and the centralized
role model are not implemented yet. It proves deterministic assignment and queueing, not
production capacity. The exercise deliberately cancels synthetic result-heavy queries
after observing policy state and does not retain latency measurements.

## Upgrade from 0.20.0

No warehouse, metastore, or worker migration is required. Recreate the coordinator after
pulling the release so it loads `resource-groups.properties` and
`resource-groups.json`. Existing clients that do not use an ingestion or BI identity
enter the catch-all ad-hoc group and are limited to one running plus three queued queries.
