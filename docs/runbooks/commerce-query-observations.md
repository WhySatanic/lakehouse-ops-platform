# Repeated commerce query observations

The full two-volume profile retains one detail aggregation and one daily-mart
query per volume. This private, read-only follow-up adds repeated observations
without rerunning Spark, changing a checkpoint or rewriting an earlier profile.
It reuses exactly the profile's batch-scoped order/revenue SQL.

## Run

Use a retained local scale catalog with its Trino endpoint, existing processing
checkpoint, fixture batch ID and expected order count from `profile.json`:

```powershell
uv run --frozen python -m lakehouse_ops.commerce_query_observations --output artifacts/commerce-query-observations/100 --state artifacts/commerce-query-scale-20261012/checkpoint.json --server http://localhost:18300 --batch-id 271bf513fa34b667 --orders 100 --repetitions 4
uv run --frozen python -m lakehouse_ops.commerce_query_observations --output artifacts/commerce-query-observations/1000 --state artifacts/commerce-query-scale-20261012/checkpoint.json --server http://localhost:18300 --batch-id 61a84a7ff2915394 --orders 1000 --repetitions 4
```

Those IDs/ports are an example from a retained local stack, not portable defaults.
Set them from your own profile. The command does not start or stop Docker services.
Only catalog reads and local evidence writes are performed. Keep a single catalog
writer; the snapshot audit detects changes but does not lock external writers.

The first detail/daily pair is retained as warm-up and excluded from summaries.
Each subsequent pair alternates detail-first and daily-first. An even 2..10
repetitions gives equal numbers of each order; the default four pairs execute
eight measured queries plus two warm-up queries. Each query has a 60-second budget.
Order count must be 100..10000, matching the bounded fixture observations.

Each pair must reconcile orders/revenue. All pairs must match the warm-up result.
Commerce snapshot inventory and checkpoint bytes must remain unchanged. Otherwise
the report fails and retains completed observations and the error type; partial
samples are not reported as a successful comparison. No query is retried or replaced
with a more favorable timing. Reuse requires a new empty evidence directory.

`queries.json` contains every successful pair's query IDs, returned rows and all
eight Trino metrics, explicit execution order, warm-up results and snapshot audits.
It reports median/min/max separately for detail and daily queries. The existing
full profile's default single detail-then-daily query behavior remains unchanged.

## Retained local observation, 2026-10-12

Four pairs per batch on the already running `lakehouse-ops-scale-query-v1` catalog:

| Orders | Revenue cents | Detail / daily median elapsed ms | Detail / daily median CPU ms | Detail / daily physical input bytes |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 278410 | 89 / 43 | 16.5 / 5.5 | 15817 / 4248 |
| 1000 | 2733643 | 115.5 / 41 | 25 / 5 | 91579 / 4257 |

Both reports were ready, with unchanged checkpoint bytes and snapshot inventories
for all 13 commerce tables. All measured queries reconciled to their selected
batch. The already running services were left running; no unrelated service was
changed and no warehouse data was written.

Ignored local evidence under `artifacts/commerce-query-repetitions-20261012-v1`:

- `100/queries.json`, SHA256
  `d59448b77eb8ee692ab4fbeaf8a35a5289e0edaa333d22039da0d1a27c3313ef`.
- `1000/queries.json`, SHA256
  `340bbabca936ffcb9f93c06a9fc60e68673173109e43409e5ae14b768f141abd`.

The 100-order detail warm-up was 1037 ms and is not included in its 89 ms median.
The 1000-order measured detail range was 76..567 ms, despite a 115.5 ms median.
This variability is why raw observations and ranges remain necessary.

These are small warm-catalog reads on a shared host, sequential volumes and
deterministically alternating query order. There is no cold-cache guarantee,
randomized workload, statistical confidence interval, concurrent throughput,
full-pipeline rerun or production-capacity claim. Separate timing/CPU medians are
not additive and do not prove a general speedup. This diagnostic remains outside
the stable public CLI/JSON contract; no version or release change is made.
