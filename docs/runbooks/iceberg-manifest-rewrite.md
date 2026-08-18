# Iceberg manifest rewrite

The Spark maintenance job applies planner-generated `rewrite_manifests` actions through
Iceberg's system procedure. The same plan schema, dry-run default, snapshot guard, and
explicit approval handshake used for data-file rewrites apply here.

## Prepare and review

Collect current metadata and create a plan. If manifest density crosses the configured
policy, select its `rewrite_manifests` action and retain the plan for review. The action
contains `max_manifests_to_rewrite: 1000`; execution stops before the procedure call if
the live manifest count exceeds that bound.

Set the selected action, plan, and snapshot identifiers:

```bash
export MAINTENANCE_PLAN_PATH=/opt/lakehouse/artifacts/manifest-plan.json
export MAINTENANCE_ACTION_ID=action-...
export MAINTENANCE_APPROVED_PLAN_ID=plan-...
export MAINTENANCE_APPROVED_SNAPSHOT_ID=...
```

## Dry-run and apply

Run without `MAINTENANCE_APPLY` first. This reads the live snapshot, data files,
manifests, and row counts but does not call the procedure:

```bash
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

After reviewing and preserving the dry-run report, apply the exact plan:

```bash
MAINTENANCE_APPLY=true \
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

A successful reconciliation requires a new snapshot, fewer manifests, unchanged data
file and row counts, and fewer added manifests than rewritten manifests. A zero-rewrite
procedure result is accepted only when every observed state field is unchanged.

## Failure handling

- Recollect and re-plan if the current snapshot differs from the approved snapshot.
- Split or inspect the table if its manifest count exceeds the action bound; do not
  increase the bound without reviewing memory and commit risk.
- Treat `reconciliation_failed` as an incident. Preserve the plan, both reports, and
  Spark logs before investigating the snapshot chain and manifest metadata.

Manifest rewrite does not compact data files or remove historical files. Run those
operations as separate reviewed actions with their own evidence.
