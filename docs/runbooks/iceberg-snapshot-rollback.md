# Iceberg snapshot rollback drill

This drill proves a bounded recovery path for an incorrect append. It uses the dedicated
`lakehouse.ops.snapshot_recovery_fixture` table and does not operate on application tables.

The scenario creates two snapshots:

1. the recovery target contains `(1, "stable")`;
2. a later snapshot adds `(2, "regression")`;
3. Spark reads the target by exact snapshot ID without changing current state;
4. `rollback_to_snapshot` moves the table back to the target;
5. Spark and Trino verify the restored current state and confirm that the abandoned
   snapshot is still readable through time travel.

The procedure and query forms follow the official
[Iceberg Spark procedure](https://iceberg.apache.org/docs/latest/spark-procedures/#rollback_to_snapshot),
[Iceberg Spark time-travel](https://iceberg.apache.org/docs/latest/spark-queries/#time-travel-queries-with-dataframe),
and [Trino Iceberg time-travel](https://trino.io/docs/current/connector/iceberg.html#time-travel-queries)
contracts.

## Run the drill

Start the catalog, compute, and query services using the existing Hive Metastore, MinIO,
Spark, and Trino runbooks. Then prepare a writable report and run the Spark drill:

```bash
mkdir -p artifacts
touch artifacts/snapshot-rollback-report.json
chmod 0666 artifacts/snapshot-rollback-report.json
docker compose --profile catalog --profile compute run --rm spark-snapshot-rollback-drill
uv run python tests/integration/check_snapshot_rollback.py \
  artifacts/snapshot-rollback-report.json
```

Export the abandoned snapshot ID and verify both table states through Trino:

```bash
export RECOVERY_ABANDONED_SNAPSHOT_ID="$(uv run python -c \
  'import json; print(json.load(open("artifacts/snapshot-rollback-report.json"))["abandoned_snapshot"]["snapshot_id"])')"
docker compose --profile query run --rm trino-snapshot-rollback-check
```

## Required evidence

The schema 1.0 report records:

- distinct target and pre-rollback snapshot IDs;
- two rows before rollback and one row in the historical target;
- the procedure's exact previous and current snapshot IDs;
- the restored one-row current state;
- the still-readable two-row abandoned snapshot;
- current-ancestor and abandoned lineage after rollback.

The host validator rejects incomplete or inconsistent reports. The Trino check separately
requires one current row, two rows through `FOR VERSION AS OF`, and a non-ancestor history
entry for the abandoned snapshot.

## Recovery boundary

`rollback_to_snapshot` only accepts a snapshot that is an ancestor of the current state.
It changes table metadata; it does not delete the abandoned snapshot or its data files.
That makes forward investigation possible until snapshot expiration removes the recovery
window. Operators must capture the target ID, verify it with a time-travel read, stop
writers for the drill, and avoid snapshot expiration until the restored state is accepted.

This is a reproducible single-table recovery exercise, not automated incident orchestration.
It does not coordinate concurrent writers, select a target from business evidence, restore
deleted objects, or replace the full platform recovery drill required for 1.0.0.
