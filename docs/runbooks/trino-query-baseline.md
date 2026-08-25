# Trino query baseline

The baseline command runs a versioned, read-only query corpus through `EXPLAIN ANALYZE`
and records final coordinator statistics from the Trino client protocol. It creates the
evidence needed to compare partitioning, file layout, caching, and engine configuration
changes without treating one noisy wall-clock number as a conclusion.

## Corpus contract

The default corpus is `config/trino/query-corpus.json`. Schema version `1.0` requires:

- a non-empty corpus name;
- between 1 and 25 uniquely named queries;
- lowercase query IDs suitable for stable report keys;
- one read-only `SELECT` or `WITH` statement per entry;
- no statement separators.

The checked-in silver-weather corpus covers a full table scan, a grouped aggregation,
and an event-time filter with ordering. Query text is hashed into the report so two
baselines can prove that they ran the same workload.

## Capture

Run the core data path and start both Trino nodes, then execute:

```bash
uv run lakeops capture-trino-baseline \
  --server http://localhost:8080 \
  --corpus config/trino/query-corpus.json \
  > artifacts/trino-baseline.json
uv run python tests/integration/check_trino_baseline.py \
  artifacts/trino-baseline.json
```

Each query is wrapped in `EXPLAIN ANALYZE`, which executes the statement and returns its
distributed plan. The report stores a plan digest and line count rather than embedding a
large engine-specific plan.

## Metrics

The final `FINISHED` response supplies:

- elapsed and execution wall time in milliseconds;
- cluster CPU time;
- processed rows and logical bytes;
- physical input bytes;
- peak memory and spilled bytes.

The command rejects missing, negative, incomplete, or non-final statistics. A baseline
is an observation, not a performance claim. Compare repeated runs under the same image,
topology, dataset snapshot, and corpus before choosing an optimization.

## Comparison boundary

Cold and warm runs can differ because of JVM startup, metadata caches, object-store
caches, and host contention. Future experiments should record at least three repetitions,
state whether caches were warm, compare medians, and keep the query and plan digests.
This first baseline deliberately does not enforce latency thresholds.

Protocol behavior follows the official
[Trino client REST API reference](https://trino.io/docs/current/develop/client-protocol.html).
