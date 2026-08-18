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
S3A.

## Review evidence

The execution report contains:

- `candidate_files`: sorted paths reported by Iceberg;
- `candidate_set_id`: a deterministic digest of the table, cutoff, and candidate paths;
- `procedure_result.orphan_file_count`: the complete candidate count;
- identical `before` and `after` table state.

Store the plan and report together. If the candidate count exceeds the plan's safety
bound, inspection fails without producing an approval set.

## Remove the approved set

Review every path in the inspection report. Set all four approvals and point the job at
the unchanged report:

```bash
export MAINTENANCE_APPLY=true
export MAINTENANCE_APPROVED_PLAN_ID=plan-...
export MAINTENANCE_APPROVED_SNAPSHOT_ID=...
export MAINTENANCE_APPROVED_CANDIDATE_SET_ID=orphans-...
export MAINTENANCE_CANDIDATE_REPORT_PATH=/opt/lakehouse/artifacts/orphan-inspection-report.json

docker compose --profile catalog --profile compute run --rm spark-maintenance
```

The executor rejects a changed plan, snapshot, table state, report, candidate count, or
candidate digest. It creates a temporary Iceberg `file_list_view` containing only the
approved paths and runs a second dry-run against current table metadata. Deletion starts
only if every approved path is still orphaned. Prefix mismatch handling remains
fail-closed. The procedure requests one delete worker, but Iceberg S3FileIO uses bulk
deletes and ignores that setting; the candidate-count bound therefore remains the
effective batch-size control for S3.

The execution report must have `status: succeeded`, `applied: true`, identical table
state before and after, and matching `orphan_file_count` and
`deleted_orphan_file_count`. Independently verify that every approved object is absent
from S3 before accepting the operation; the integration gate performs this check with
`HeadObject`.

## Recovery boundary

Object deletion is irreversible. Keep the original inspection report and execution
report as audit evidence. If reconciliation fails, stop maintenance on the table,
compare the approved paths with object-store audit logs, and restore missing objects
from an independently managed backup. The platform does not yet automate that restore.

Inspection and exact-list results are collected on the Spark driver, so operators must
use a conservative candidate bound and split very large tables operationally. An empty
approved set returns `noop` without invoking deletion.

The procedure behavior follows the
[Apache Iceberg Spark procedure contract](https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files).
