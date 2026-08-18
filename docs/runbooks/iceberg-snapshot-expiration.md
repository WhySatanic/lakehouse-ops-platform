# Iceberg snapshot expiration

Snapshot expiration is destructive: it removes historical table states and can delete
files used only by those states. The control plane therefore plans and applies exact
snapshot IDs rather than an open-ended timestamp predicate.

## Retention plan

The default policy retains snapshots newer than seven days, always preserves at least
the latest three, and selects no more than 50 snapshots per action. Metadata collection
includes complete snapshot history and Iceberg references. Any branch or tag other than
`main` defers expiration until an operator reviews its retention intent.

Create and review a plan:

```bash
uv run lakeops collect-iceberg-metadata \
  --server http://localhost:8080 \
  --catalog lakehouse --schema silver --table weather_hourly \
  > artifacts/metadata.json

uv run lakeops plan-iceberg-maintenance \
  --input artifacts/metadata.json \
  > artifacts/maintenance-plan.json
```

The `expire_snapshots` action records both its target IDs and the complete history seen
by the planner. Preserve the plan for audit and inspect every target before approval.

## Dry-run and apply

Select the expiration action and export its action ID, plan ID, and expected current
snapshot. Dry-run is the default and performs all live-history checks without calling
Iceberg:

```bash
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

Apply only the exact reviewed plan:

```bash
MAINTENANCE_APPLY=true \
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

Execution stops before mutation if the current snapshot or any snapshot in history has
changed, a target is missing, the current snapshot is targeted, or the batch exceeds
its limit. Deletes use one thread, stream results to avoid collecting large file lists
on the driver, and do not clean schemas or partition specs in this capability.

## Evidence and recovery boundary

Success requires the same current snapshot, row count, current data files, and current
manifests, plus the exact removal of every approved historical ID. The integration drill
also reads the retained historical snapshot and proves an expired ID is no longer
available.

Snapshot expiration cannot be undone from Iceberg metadata. Restore requires an
external metadata/object-store backup, which is outside this capability and remains a
required recovery drill before 1.0.
