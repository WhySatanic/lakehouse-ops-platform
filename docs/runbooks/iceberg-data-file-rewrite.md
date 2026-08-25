# Iceberg data-file rewrite

The Spark maintenance job consumes plan schema `1.0` and supports
`rewrite_data_files`. It checks the live snapshot before any procedure call and records
file count, manifest count, row count, snapshot, and procedure output before and after
execution.

## Prepare the plan

Start the catalog, compute, and query profiles, then collect and plan against a table as
described in the [maintenance planning runbook](iceberg-maintenance-planning.md). Store
the plan at `artifacts/maintenance-plan.json`. On Linux, create the report file with
write permission for the unprivileged container user:

```bash
mkdir -p artifacts
touch artifacts/maintenance-report.json
chmod 0666 artifacts/maintenance-report.json
```

Select one `rewrite_data_files` action and export its identifiers:

```bash
export MAINTENANCE_ACTION_ID=action-...
export MAINTENANCE_APPROVED_PLAN_ID=plan-...
export MAINTENANCE_APPROVED_SNAPSHOT_ID=...
```

## Dry-run

Dry-run is the default and performs live snapshot and table-state checks without calling
the Iceberg procedure:

```bash
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

The report is written to `artifacts/maintenance-report.json` with `applied: false` and
`status: dry_run`. Preserve this report as review evidence before execution.

## Apply

Set all three approval values to the exact values in the reviewed plan, then run:

```bash
MAINTENANCE_APPLY=true \
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

Execution is rejected before mutation if the plan, action, or current snapshot differs.
The procedure is restricted to one concurrent file group, disables partial progress,
and rewrites no more than 1,000 files. A successful report requires preserved row count,
a new snapshot, fewer data files, and zero failed files.

The [Trino compaction experiment](trino-compaction-experiment.md) captures the same
read-only workload before and after this operation. Its comparison report rejects
snapshot or file-count evidence that does not match the applied maintenance report.

## Failure handling

- `current snapshot does not match`: discard the stale plan, recollect metadata, and
  review a new plan.
- `reconciliation_failed`: stop automation and retain both reports and Spark logs. Do
  not repeat the action until the row count, file count, snapshot chain, and procedure
  result have been investigated.
- missing or mismatched approvals: repeat dry-run and copy identifiers from the same
  reviewed plan. Never bypass the guards.

This executor does not expire snapshots or delete orphan files. Existing snapshots keep
the pre-rewrite files reachable until a separately reviewed retention operation runs.
