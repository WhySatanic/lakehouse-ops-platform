# Iceberg schema evolution drill

This drill proves compatible add-and-rename evolution on the isolated
`lakehouse.ops.schema_evolution_fixture` table. It does not modify application tables.

The scenario:

1. creates a two-column table and records its first snapshot;
2. appends nullable `severity` and writes a second row;
3. records the evolved snapshot with the original `payload` name;
4. renames `payload` to `message` without rewriting data;
5. verifies the current schema and both historical snapshot schemas through Spark and Trino.

Iceberg assigns a new field ID when a column is added and preserves the existing field ID
when a column is renamed. Exact snapshot time travel uses the schema stored with that
snapshot. The scenario follows the official [Iceberg schema evolution specification](https://iceberg.apache.org/spec/#schema-evolution),
[Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table),
[Spark time-travel schema selection](https://iceberg.apache.org/docs/latest/spark-queries/#schema-selection-in-time-travel-queries),
and [Trino Iceberg time travel](https://trino.io/docs/current/connector/iceberg.html#time-travel-queries)
contracts.

## Run the drill

Start the catalog, compute, and query services from the existing platform runbooks. Then
prepare a writable report and run the Spark exercise:

```bash
mkdir -p artifacts
touch artifacts/schema-evolution-report.json
chmod 0666 artifacts/schema-evolution-report.json
docker compose --profile catalog --profile compute run --rm spark-schema-evolution-drill
uv run python tests/integration/check_schema_evolution.py \
  artifacts/schema-evolution-report.json
```

Export the two snapshot IDs and validate compatibility through Trino:

```bash
export SCHEMA_INITIAL_SNAPSHOT_ID="$(uv run python -c \
  'import json; print(json.load(open("artifacts/schema-evolution-report.json"))["initial"]["snapshot_id"])')"
export SCHEMA_EVOLVED_SNAPSHOT_ID="$(uv run python -c \
  'import json; print(json.load(open("artifacts/schema-evolution-report.json"))["after_add"]["snapshot_id"])')"
docker compose --profile query run --rm trino-schema-evolution-check
```

## Required evidence

The schema 1.0 report proves:

- the initial snapshot exposes `event_id,payload` and one row;
- the evolved snapshot exposes `event_id,payload,severity`, including `NULL` for the old row;
- the current table exposes `event_id,message,severity` with both values preserved;
- renaming the column does not create a data snapshot;
- both historical schemas remain available after the rename.

The host validator rejects inconsistent columns, rows, IDs, or compatibility flags. The
Trino check independently validates current information-schema metadata, current values,
and exact-version queries against both historical schemas.

The historical checks materialize rows without filtering on the renamed field. Trino 483
can read that field from the snapshot schema, but predicate pushdown on its historical name
fails when the selected snapshot contains files written before and after the nullable-column
add. Keep historical filters on fields whose names are stable, or materialize the snapshot
before filtering, until that connector limitation is removed.

## Operational boundary

Adding an optional column and renaming an existing field are metadata changes; existing
Parquet files are not rewritten. This drill deliberately avoids positional reordering and
non-last-column deletion because Hive Metastore validates schema changes positionally by
default. It also avoids unsafe type changes and nullable-to-required conversion.

Snapshot time travel protects historical reads, but it is not a general schema rollback.
Operators must assess every consumer before changing names and must keep old snapshots
until compatibility is accepted. This drill does not cover nested fields, type widening,
partition evolution, concurrent writers, or multi-table migration orchestration.
