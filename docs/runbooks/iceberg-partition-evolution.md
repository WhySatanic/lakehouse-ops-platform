# Iceberg partition evolution drill

This drill proves that an Iceberg table remains readable while its physical layout moves
from unpartitioned files to daily partitions. It uses the isolated
`lakehouse.ops.partition_evolution_fixture` table and does not modify application tables.

The scenario:

1. writes two rows under the unpartitioned spec and records the snapshot;
2. adds `day(event_ts) AS event_day` without rewriting data;
3. proves the metadata-only change does not create a data snapshot;
4. writes two more rows into separate daily partitions;
5. proves the current snapshot contains files from spec IDs 0 and 1;
6. reads all four rows and the original snapshot through both Spark and Trino.

The contract follows the official [Iceberg Spark partition evolution](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table-add-partition-field),
[Spark metadata table](https://iceberg.apache.org/docs/latest/spark-queries/#partitions), and
[Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html#schema-evolution)
documentation.

## Run the drill

Start the catalog, compute, and query services from the existing platform runbooks. Then
prepare the report and run the Spark exercise:

```bash
mkdir -p artifacts
touch artifacts/partition-evolution-report.json
chmod 0666 artifacts/partition-evolution-report.json
docker compose --profile catalog --profile compute run --rm spark-partition-evolution-drill
uv run python tests/integration/check_partition_evolution.py \
  artifacts/partition-evolution-report.json
```

Export the initial snapshot ID and validate the mixed layout through Trino:

```bash
export PARTITION_INITIAL_SNAPSHOT_ID="$(uv run python -c \
  'import json; print(json.load(open("artifacts/partition-evolution-report.json"))["before_evolution"]["snapshot_id"])')"
docker compose --profile query run --rm trino-partition-evolution-check
```

## Required evidence

The schema 1.0 report proves:

- the partition-spec update leaves the current snapshot ID unchanged;
- the old data files remain on spec 0 with a `NULL` value for `event_day`;
- new data files use spec 1 and the expected daily values;
- the current table reads all four rows across both layouts;
- exact snapshot time travel still reads the two pre-evolution rows.

The host validator rejects inconsistent snapshots, rows, file layouts, record counts, or
compatibility flags. The Trino check independently validates current rows, initial-snapshot
rows, and manifest spec IDs 0 and 1.

## Operational boundary

Adding a partition field does not repartition existing files. Query engines must plan
against every live spec until maintenance rewrites old files. Partition pruning can be less
effective during that transition, so compare scanned bytes and latency before declaring a
layout migration complete.

Avoid dynamic partition overwrite while a spec is changing: its replacement boundary is
derived from the current spec and can leave older layouts untouched. Prefer explicit
row-level operations or an approved rewrite plan. This drill does not cover partition-field
replacement, removal, concurrent writers, compaction into the new spec, or performance
measurement.
