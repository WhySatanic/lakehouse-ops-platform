# Iceberg orphan-file inventory

Files left behind by failed or interrupted writes can consume storage without being
referenced by Iceberg metadata. Discovery must be treated as a destructive-operation
prerequisite: a file that is still being written can look orphaned before its commit
becomes visible.

## Create the inspection plan

Collect fresh table metadata and explicitly enable the inspection action:

```bash
uv run lakeops collect-iceberg-metadata \
  --server http://localhost:8080 \
  --catalog lakehouse --schema silver --table weather_hourly \
  > artifacts/metadata.json

uv run lakeops plan-iceberg-maintenance \
  --input artifacts/metadata.json \
  --enable-orphan-inspection \
  --orphan-retention-hours 168 \
  --max-orphan-files 1000 \
  > artifacts/orphan-plan.json
```

The planner refuses an age window below 72 hours. The action pins the current snapshot,
uses one job, and records the cutoff and maximum reviewable candidate count.

## Run the non-deleting inspection

Set `MAINTENANCE_PLAN_PATH` and `MAINTENANCE_ACTION_ID` for the generated
`inspect_orphan_files` action, then run the existing maintenance service without
`MAINTENANCE_APPLY`:

```bash
docker compose --profile catalog --profile compute run --rm spark-maintenance
```

The executor verifies that the current snapshot still matches the plan and calls
Iceberg `remove_orphan_files` with `dry_run => true`. Prefix mismatches use `ERROR`, so
scheme or authority ambiguity stops the inspection instead of classifying a file as
deletable. Object listing uses Iceberg S3FileIO prefix operations rather than Hadoop
S3A. Applying this action is rejected because this release cannot delete files.

## Review evidence

The execution report contains:

- `candidate_files`: sorted paths reported by Iceberg;
- `candidate_set_id`: a deterministic digest of the table, cutoff, and candidate paths;
- `procedure_result.orphan_file_count`: the complete candidate count;
- identical `before` and `after` table state.

Store the plan and report together. If the candidate count exceeds the plan's safety
bound, inspection fails without producing an approval set.

## Current boundary

This capability inventories files but never removes them. It collects complete dry-run
results on the Spark driver, so operators must use a conservative candidate bound and
split very large tables operationally. A later increment must require exact
candidate-set approval, delete only that set, and reconcile object-store state before
the roadmap's orphan-removal item can be closed.

The procedure behavior follows the
[Apache Iceberg Spark procedure contract](https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files).
