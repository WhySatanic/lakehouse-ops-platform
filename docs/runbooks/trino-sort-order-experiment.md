# Trino sort-order experiment

This experiment compares two unpartitioned Iceberg tables containing the same 65,536
rows. `lakehouse.ops.sort_baseline` writes 16 hash-scattered files without a sort order.
`lakehouse.ops.sort_ordered` declares `event_id` as its global write order, so Spark uses
range distribution. A Trino predicate selecting 128 adjacent IDs must return the same
result while the ordered layout processes fewer rows and fewer physical input bytes.

Latency is recorded but is not a correctness gate. The experiment isolates file-level
data skipping from partition pruning because both tables expose one Iceberg partition.

## Evidence contract

The fixture and Trino reports use schema version `1.0`. The final report records:

- current snapshot, file count, record count, partition count, and byte size;
- a digest of each table's Trino `SHOW CREATE TABLE` output;
- proof that only the ordered table exposes `sorted_by = ARRAY['event_id']`;
- an identical aggregate result for event IDs 30,000 through 30,127;
- three alternating `EXPLAIN ANALYZE` runs per layout;
- medians for elapsed time, CPU, processed rows and bytes, physical input bytes, peak
  memory, and spill;
- query and plan digests plus scan-volume reduction percentages.

The validator rejects the report unless both scan-volume metrics decrease. It does not
require wall time to improve on the small local fixture.

## Run

Start the catalog and query profiles, then create and verify the Spark fixture:

```bash
mkdir -p artifacts
touch artifacts/sort-order-fixture.json
chmod 0666 artifacts/sort-order-fixture.json
docker compose --profile catalog --profile compute run --rm spark-sort-order-fixture
uv run python tests/integration/check_sort_order_fixture.py \
  artifacts/sort-order-fixture.json
```

Capture and verify Trino evidence:

```bash
uv run lakeops capture-trino-sort-order \
  --server http://localhost:8080 \
  --range-start 30000 --range-size 128 --repetitions 3 \
  > artifacts/trino-sort-order.json
uv run python tests/integration/check_trino_sort_order.py \
  artifacts/trino-sort-order.json
```

## Interpretation

`processed_rows_reduction_percent` measures how much less table input reached Trino
operators. `physical_input_bytes_reduction_percent` measures the reduction in object-store
reads. Both must be positive. File counts can differ slightly because Spark write planning
uses task sizing, so identical data and scan evidence are the gates, not exact file parity.

The table snapshots and create-statement digests bind measurements to exact layouts.
Alternating execution order reduces systematic warm-cache bias, but does not turn three
local repetitions into a production capacity benchmark.

## Rollback and cleanup

The fixture is isolated from application tables. Remove its catalog entries with Spark
after retaining required evidence:

```sql
DROP TABLE IF EXISTS lakehouse.ops.sort_baseline;
DROP TABLE IF EXISTS lakehouse.ops.sort_ordered;
```

Dropping catalog entries does not replace an object-store retention policy. Remove the
isolated prefixes only through an approved storage cleanup procedure.

## Limits

- The dataset is synthetic and measured on the local two-worker Trino topology.
- The experiment covers one ascending high-cardinality sort key and one range predicate.
- It does not test compound keys, descending order, null placement, delete files, or skew.
- Cold and warm metadata-cache behavior remains a separate controlled experiment.

The write layout follows the official
[Iceberg Spark write distribution](https://iceberg.apache.org/docs/latest/spark-writes/#writing-distribution-modes)
and [Spark sort-order DDL](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table-write-ordered-by)
contracts. Trino verification uses the official
[Iceberg sorted table](https://trino.io/docs/current/connector/iceberg.html#sorted-tables)
and [`EXPLAIN ANALYZE`](https://trino.io/docs/current/sql/explain-analyze.html) contracts.
