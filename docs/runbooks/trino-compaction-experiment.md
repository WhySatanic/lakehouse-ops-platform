# Trino compaction experiment

This experiment measures one controlled file-layout change instead of treating a faster
single query as proof. The maintenance fixture contains 100,000 deterministic rows in
four Iceberg data files. Trino executes the same aggregate with `EXPLAIN ANALYZE` three
times before and after Spark applies the approved `rewrite_data_files` action.

Each phase records:

- the current Iceberg snapshot and data-file layout;
- the workload digest and three Trino query IDs;
- each plan digest and final protocol metrics;
- medians for wall time, CPU, processed and physical bytes, peak memory, and spill.

The comparison is accepted only when its snapshots and file counts match the applied
maintenance report, the record count remains 100,000, and the data-file count falls.
Latency is reported as `improved`, `unchanged`, or `regressed`; none of those outcomes is
forced by CI.

## Run the experiment

Create the maintenance fixture after starting the catalog, compute, and query profiles:

```bash
docker compose --profile catalog --profile compute run --rm \
  spark-maintenance-fixture
uv run lakeops capture-trino-compaction \
  --server http://localhost:8080 --phase before --repetitions 3 \
  > artifacts/trino-compaction-before.json
```

Plan, review, dry-run, and apply data-file maintenance as described in the
[data-file rewrite runbook](iceberg-data-file-rewrite.md). Preserve its execution report,
then capture and compare the second phase:

```bash
uv run lakeops capture-trino-compaction \
  --server http://localhost:8080 --phase after --repetitions 3 \
  > artifacts/trino-compaction-after.json
uv run lakeops compare-trino-compaction \
  --before artifacts/trino-compaction-before.json \
  --after artifacts/trino-compaction-after.json \
  --execution artifacts/maintenance-report.json \
  > artifacts/trino-compaction-experiment.json
uv run python tests/integration/check_trino_compaction_experiment.py \
  artifacts/trino-compaction-experiment.json
```

## Interpretation

File-count reduction is the deterministic result of this fixture. Wall time and CPU are
observations from one local topology. A regression is valid evidence, not a failing test:
JVM state, object-store latency, and the small dataset can dominate the file-layout
effect. Use more repetitions and a representative dataset before making a capacity or
cost decision.

Processed bytes should remain comparable because compaction changes layout, not logical
rows. Physical bytes can vary with Parquet encoding and rewritten file boundaries. Plan
digests may change with snapshot metadata even when the SQL digest stays fixed.

## Rollback and limits

The rewrite creates a new Iceberg snapshot and does not immediately delete the previous
files. Use the existing snapshot rollback workflow if the new layout must be abandoned,
and do not expire the pre-compaction snapshot until the comparison has been reviewed.

This is a single-node-runner experiment with three repetitions per phase. It does not
isolate network noise, compare partition or sort orders, exercise metadata-cache toggles,
or establish a performance SLO. Those remain separate roadmap increments.

The metric source follows Trino's
[client protocol](https://trino.io/docs/current/develop/client-protocol.html), while the
table layout is read from Iceberg metadata tables exposed by the
[Iceberg connector](https://trino.io/docs/current/connector/iceberg.html#metadata-tables).
