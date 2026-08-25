# Trino partition pruning experiment

This experiment compares two isolated Iceberg tables containing the same 65,536 rows.
`lakehouse.ops.pruning_unpartitioned` has no partition transform. The matching
`lakehouse.ops.pruning_partitioned` table uses `days(event_ts)` across 32 days. A
single-day Trino predicate must return the same 2,048 rows from both tables while the
partitioned scan processes fewer rows and fewer physical input bytes.

The experiment measures pruning, not just wall-clock noise. Latency is reported as an
observation but is not a correctness gate.

## Evidence contract

The fixture and Trino reports use schema version `1.0`. The final report records:

- the current snapshot ID, data-file count, partition count, record count, and size of
  each table;
- an identical aggregate result for the target day;
- three alternating `EXPLAIN ANALYZE` runs per table;
- median elapsed time, wall time, CPU, processed rows and bytes, physical input bytes,
  peak memory, and spill;
- query and plan digests;
- the percentage reduction in processed rows and physical input bytes.

The validator rejects evidence unless both tables contain 65,536 rows, the partitioned
table exposes 32 partitions, the filtered results match, and both scan-volume metrics
decrease. It does not require latency to decrease.

## Run

Start the catalog, compute, and query profiles, then create the fixture:

```bash
mkdir -p artifacts
touch artifacts/partition-pruning-fixture.json
chmod 0666 artifacts/partition-pruning-fixture.json
docker compose --profile catalog --profile compute run --rm \
  spark-partition-pruning-fixture
uv run python tests/integration/check_partition_pruning_fixture.py \
  artifacts/partition-pruning-fixture.json
```

Capture and verify Trino evidence:

```bash
uv run lakeops capture-trino-partition-pruning \
  --server http://localhost:8080 \
  --target-day 2026-01-16 \
  --repetitions 3 \
  > artifacts/trino-partition-pruning.json
uv run python tests/integration/check_trino_partition_pruning.py \
  artifacts/trino-partition-pruning.json
```

## Interpretation

`processed_rows_reduction_percent` shows how much less table input reached Trino
operators. `physical_input_bytes_reduction_percent` shows the corresponding reduction
in object-store reads. Both must be positive. Wall time can still regress because JVM
state, host contention, metadata caching, and small-file overhead are not isolated by
this fixture.

The snapshot IDs prove which table states were measured. The SQL-template and predicate
digests make incompatible reruns visible without embedding full plans in the report.

## Rollback and cleanup

The fixture does not modify application tables. Remove it with Spark after retaining any
required evidence:

```sql
DROP TABLE IF EXISTS lakehouse.ops.pruning_unpartitioned;
DROP TABLE IF EXISTS lakehouse.ops.pruning_partitioned;
```

Dropping the catalog entries does not replace an object-store retention policy. Remove
the isolated prefixes only through an approved storage cleanup procedure.

## Limits

- The dataset is synthetic and runs on one local two-worker Trino topology.
- Three repetitions are enough for reproducibility checks, not capacity planning.
- The experiment proves day-partition pruning only. It does not evaluate sort orders,
  partition evolution, skew, delete files, or production cardinalities.
- Cold and warm metadata-cache behavior remains a separate experiment.

The table transform follows the official
[Iceberg Spark DDL partitioning](https://iceberg.apache.org/docs/latest/spark-ddl/#partitioned-by)
contract. Trino scan evidence uses the official
[`EXPLAIN ANALYZE`](https://trino.io/docs/current/sql/explain-analyze.html) statement and
[Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) metadata tables.
