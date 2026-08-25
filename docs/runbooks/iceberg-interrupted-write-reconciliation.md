# Interrupted-write reconciliation drill

This drill models a writer that uploads a valid Parquet object and stops before the
Iceberg metadata commit. It proves that the table remains on the committed snapshot,
the uploaded object is not referenced, and an exact-object cleanup does not change
table contents.

## Failure boundary

The scenario uses an isolated `lakehouse.ops.interrupted_write_fixture` table. Spark
creates one committed snapshot and records its rows plus referenced data files. The
injector then copies one committed Parquet file to a new object under the table's data
prefix and raises a deliberate interruption before any catalog or table metadata update.

This is a deterministic model of the `data uploaded, metadata not committed` boundary.
It does not kill a live Spark driver, simulate a partial multipart upload, or discover
unknown old objects. The bounded orphan inventory and removal workflow remains the
production path for aged, unreferenced objects.

## Run the drill

Start the catalog, object store, and Trino topology, then initialize MinIO:

```bash
docker compose --profile catalog --profile compute up -d --wait \
  minio metastore-db hive-metastore
docker compose run --rm minio-init
docker compose --profile query up -d --wait trino-coordinator trino-worker
mkdir -p artifacts
touch artifacts/interrupted-write-report.json
chmod 0666 artifacts/interrupted-write-report.json
```

Create the baseline, inject the failure, and reconcile it:

```bash
docker compose --profile catalog --profile compute run --rm \
  spark-interrupted-write-fixture
uv run python tests/integration/exercise_interrupted_write_reconciliation.py \
  artifacts/interrupted-write-report.json
uv run python tests/integration/check_interrupted_write_reconciliation.py \
  artifacts/interrupted-write-report.json
```

The injector refuses to overwrite an existing candidate. Before deletion it verifies
the candidate's ETag, confirms the object is absent from Iceberg's referenced files,
and confirms that the current snapshot, ordered rows, and referenced files still match
the baseline. It deletes only the recorded key and repeats the table checks afterward.

## Evidence contract

The JSON report records:

- the baseline snapshot, ordered rows, referenced files, and copy source;
- the exact injection point and expected injected error;
- the candidate location, ETag, size, existence, and unreferenced status;
- the interrupted and reconciled table states;
- explicit post-conditions for snapshot, rows, references, and cleanup.

A `recovered` status is valid only when every interrupted and reconciled table field is
identical to the baseline and the exact candidate no longer exists.

## Recovery boundary

If the drill stops after object creation, do not delete by prefix. Read the candidate
location and ETag from the report or object-store audit trail, prove the key is absent
from the table's current `$files` metadata table, then use the guarded orphan workflow.
For untracked or aged artifacts, follow the
[orphan inventory runbook](iceberg-orphan-inventory.md) and retain its approval report.
